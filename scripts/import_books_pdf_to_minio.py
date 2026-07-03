#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import tempfile
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
from minio import Minio
from pypdf import PdfReader


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _safe_filename(title: str, max_len: int = 140) -> str:
    text = str(title or "").strip()
    if not text:
        text = "untitled"
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r'[\\/:*?"<>|]+', " ", text)
    text = re.sub(r"\s+", " ", text).strip().rstrip(".")
    return (text[:max_len].strip() or "untitled")


def _slug(text: str, max_len: int = 120) -> str:
    value = unicodedata.normalize("NFKD", str(text or ""))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    return value[:max_len] or "book"


def _source_slug(value: str) -> str:
    return _slug(value, max_len=40)


def _year_to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    txt = str(value).strip()
    if not txt:
        return None
    m = re.search(r"(19|20)\d{2}", txt)
    return int(m.group(0)) if m else None


def _url_sha8(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]


def _book_id(title: str, author: str, year: Optional[int]) -> str:
    base = f"{title}|{author}|{year or ''}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


@dataclass
class ImportResult:
    index: int
    status: str
    reason: str
    book_id: str
    object_key: Optional[str]
    pdf_url: str
    title: str
    bytes_size: int = 0
    pages: int = 0
    text_chars: int = 0
    source: str = ""
    year: Optional[int] = None
    author: str = ""


class BookPdfImporter:
    def __init__(
        self,
        excel_path: str,
        bucket: str,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool,
        verify_ssl: bool,
        timeout: int,
        max_pages_check: int,
        min_text_chars: int,
        object_prefix: str,
        force_reupload: bool,
        dry_run: bool,
        workers: int,
        progress_every: int,
    ) -> None:
        self.excel_path = excel_path
        self.bucket = bucket
        self.object_prefix = object_prefix.strip("/")
        self.timeout = timeout
        self.max_pages_check = max_pages_check
        self.min_text_chars = min_text_chars
        self.force_reupload = force_reupload
        self.dry_run = dry_run
        self.workers = max(1, int(workers))
        self.progress_every = max(1, int(progress_every))

        self.session = requests.Session()
        self.session.verify = verify_ssl
        self.session.headers.update(
            {
                "User-Agent": "OER-Book-Importer/1.0",
                "Accept": "application/pdf,*/*;q=0.8",
            }
        )

        self.minio = Minio(
            endpoint=endpoint.replace("http://", "").replace("https://", ""),
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        if not self.minio.bucket_exists(bucket):
            self.minio.make_bucket(bucket)

    def _build_object_key(self, row: Dict[str, Any]) -> Tuple[str, str]:
        title = str(row.get("ten_sach_goc") or row.get("ten_sach_chuan_hoa") or "").strip()
        source = str(row.get("nguon") or "unknown")
        year = _year_to_int(row.get("year"))
        url = str(row.get("pdf_url") or "").strip()
        author = str(row.get("author") or "").strip()

        safe_title = _safe_filename(title)
        source_folder = _source_slug(source)
        year_folder = str(year) if year else "unknown-year"
        suffix = _url_sha8(url)
        filename = f"{safe_title}_{suffix}.pdf"
        key = f"{self.object_prefix}/{source_folder}/{year_folder}/{filename}"
        book_id = _book_id(title=safe_title, author=author, year=year)
        return key, book_id

    def _check_pdf_has_content(self, local_pdf_path: str) -> Tuple[bool, int, int]:
        reader = PdfReader(local_pdf_path)
        total_pages = len(reader.pages)
        checked = min(total_pages, max(1, self.max_pages_check))
        text_chars = 0
        for idx in range(checked):
            try:
                text = reader.pages[idx].extract_text() or ""
            except Exception:
                text = ""
            text_chars += len(re.sub(r"\s+", "", text))
            if text_chars >= self.min_text_chars:
                return True, total_pages, text_chars
        return text_chars >= self.min_text_chars, total_pages, text_chars

    def _download_pdf(self, url: str, target_file: str) -> int:
        timeout = (5, self.timeout)
        with self.session.get(url, stream=True, timeout=timeout, allow_redirects=True) as resp:
            resp.raise_for_status()
            content_type = str(resp.headers.get("Content-Type") or "").lower()
            if "pdf" not in content_type and not url.lower().endswith(".pdf"):
                raise RuntimeError(f"URL does not look like direct PDF (content-type={content_type})")
            total = 0
            with open(target_file, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 128):
                    if not chunk:
                        continue
                    f.write(chunk)
                    total += len(chunk)
        return total

    def _import_one(self, i: int, row: Dict[str, Any]) -> ImportResult:
            title = str(row.get("ten_sach_goc") or row.get("ten_sach_chuan_hoa") or "").strip()
            pdf_url = str(row.get("pdf_url") or "").strip()
            source = str(row.get("nguon") or "")
            author = str(row.get("author") or "")
            year = _year_to_int(row.get("year"))
            if not title or not pdf_url:
                return ImportResult(
                    index=i,
                    status="skipped",
                    reason="missing_title_or_pdf_url",
                    book_id="",
                    object_key=None,
                    pdf_url=pdf_url,
                    title=title,
                )
            if not urlparse(pdf_url).scheme.startswith("http"):
                return ImportResult(
                    index=i,
                    status="skipped",
                    reason="invalid_pdf_url",
                    book_id="",
                    object_key=None,
                    pdf_url=pdf_url,
                    title=title,
                )

            object_key, book_id = self._build_object_key(row)
            try:
                if not self.force_reupload:
                    try:
                        self.minio.stat_object(self.bucket, object_key)
                        return ImportResult(
                            index=i,
                            status="exists",
                            reason="already_uploaded",
                            book_id=book_id,
                            object_key=object_key,
                            pdf_url=pdf_url,
                            title=title,
                            source=source,
                            year=year,
                            author=author,
                        )
                    except Exception:
                        pass

                if self.dry_run:
                    return ImportResult(
                        index=i,
                        status="dry_run",
                        reason="not_uploaded",
                        book_id=book_id,
                        object_key=object_key,
                        pdf_url=pdf_url,
                        title=title,
                        source=source,
                        year=year,
                        author=author,
                    )

                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
                    file_size = self._download_pdf(pdf_url, tmp.name)
                    has_content, pages, text_chars = self._check_pdf_has_content(tmp.name)
                    if not has_content:
                        return ImportResult(
                            index=i,
                            status="skipped",
                            reason="pdf_has_low_text_content",
                            book_id=book_id,
                            object_key=None,
                            pdf_url=pdf_url,
                            title=title,
                            bytes_size=file_size,
                            pages=pages,
                            text_chars=text_chars,
                            source=source,
                            year=year,
                            author=author,
                        )
                    self.minio.fput_object(
                        bucket_name=self.bucket,
                        object_name=object_key,
                        file_path=tmp.name,
                        content_type="application/pdf",
                    )
                    return ImportResult(
                        index=i,
                        status="uploaded",
                        reason="ok",
                        book_id=book_id,
                        object_key=object_key,
                        pdf_url=pdf_url,
                        title=title,
                        bytes_size=file_size,
                        pages=pages,
                        text_chars=text_chars,
                        source=source,
                        year=year,
                        author=author,
                    )
            except Exception as exc:
                return ImportResult(
                    index=i,
                    status="error",
                    reason=str(exc),
                    book_id=book_id,
                    object_key=object_key,
                    pdf_url=pdf_url,
                    title=title,
                    source=source,
                    year=year,
                    author=author,
                )

    def import_rows(self, rows: List[Dict[str, Any]]) -> List[ImportResult]:
        indexed_rows = list(enumerate(rows, start=1))
        out: List[ImportResult] = []
        if self.workers <= 1:
            for i, row in indexed_rows:
                out.append(self._import_one(i, row))
                if len(out) % self.progress_every == 0:
                    print(f"[import] processed {len(out)}/{len(indexed_rows)}", flush=True)
            return out

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = [ex.submit(self._import_one, i, row) for i, row in indexed_rows]
            total = len(futures)
            done = 0
            for fut in as_completed(futures):
                out.append(fut.result())
                done += 1
                if done % self.progress_every == 0 or done == total:
                    print(f"[import] processed {done}/{total}", flush=True)
        out.sort(key=lambda x: x.index)
        return out


def _select_columns(df: pd.DataFrame) -> pd.DataFrame:
    expected = ["ten_sach_goc", "ten_sach_chuan_hoa", "nguon", "author", "year", "pdf_url", "page_url", "notes"]
    for col in expected:
        if col not in df.columns:
            df[col] = None
    return df[expected].copy()


def _summarize(results: List[ImportResult]) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    for item in results:
        counts[item.status] = counts.get(item.status, 0) + 1
    return {"total": len(results), "counts": counts}


def main() -> None:
    parser = argparse.ArgumentParser(description="Import PDF books from Excel into MinIO")
    parser.add_argument("--excel", default="data/exports/final_book.xlsx", help="Path to Excel file")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of rows (0 = all)")
    parser.add_argument("--offset", type=int, default=0, help="Start row offset")
    parser.add_argument("--dry-run", action="store_true", help="Do not download/upload, only show planned keys")
    parser.add_argument("--force-reupload", action="store_true", help="Upload again even if object exists")
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "oer-lakehouse"))
    parser.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT", "localhost:19000"))
    parser.add_argument("--access-key", default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--secret-key", default=os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    parser.add_argument("--secure", action="store_true", default=_env_bool("MINIO_SECURE", False))
    parser.add_argument("--insecure-ssl", action="store_true", help="Disable TLS cert verification")
    parser.add_argument("--timeout", type=int, default=int(os.getenv("PDF_IMPORT_TIMEOUT", "45")))
    parser.add_argument("--max-pages-check", type=int, default=int(os.getenv("PDF_IMPORT_MAX_PAGES_CHECK", "5")))
    parser.add_argument("--min-text-chars", type=int, default=int(os.getenv("PDF_IMPORT_MIN_TEXT_CHARS", "120")))
    parser.add_argument(
        "--object-prefix",
        default=os.getenv("PDF_IMPORT_PREFIX", "bronze/library_books/pdfs"),
        help="MinIO object key prefix",
    )
    parser.add_argument(
        "--manifest-prefix",
        default=os.getenv("PDF_IMPORT_MANIFEST_PREFIX", "bronze/library_books/manifest"),
        help="MinIO object key prefix for import manifest logs",
    )
    parser.add_argument("--workers", type=int, default=int(os.getenv("PDF_IMPORT_WORKERS", "8")))
    parser.add_argument("--progress-every", type=int, default=int(os.getenv("PDF_IMPORT_PROGRESS_EVERY", "10")))
    args = parser.parse_args()

    if not os.path.exists(args.excel):
        raise SystemExit(f"Excel file not found: {args.excel}")

    try:
        df = pd.read_excel(args.excel)
    except Exception as exc:
        raise SystemExit(f"Cannot read Excel file: {exc}") from exc
    df = _select_columns(df)
    if args.offset > 0:
        df = df.iloc[args.offset :]
    if args.limit and args.limit > 0:
        df = df.iloc[: args.limit]

    records = df.to_dict(orient="records")
    importer = BookPdfImporter(
        excel_path=args.excel,
        bucket=args.bucket,
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=args.secure,
        verify_ssl=not args.insecure_ssl,
        timeout=args.timeout,
        max_pages_check=args.max_pages_check,
        min_text_chars=args.min_text_chars,
        object_prefix=args.object_prefix,
        force_reupload=args.force_reupload,
        dry_run=args.dry_run,
        workers=args.workers,
        progress_every=args.progress_every,
    )
    results = importer.import_rows(records)
    summary = _summarize(results)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "excel": args.excel,
        "offset": args.offset,
        "limit": args.limit,
        "dry_run": args.dry_run,
        "bucket": args.bucket,
        "object_prefix": args.object_prefix,
        "summary": summary,
        "results": [item.__dict__ for item in results],
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest_key = f"{args.manifest_prefix.strip('/')}/import_result_{stamp}.json"

    if not args.dry_run:
        raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        importer.minio.put_object(
            bucket_name=args.bucket,
            object_name=manifest_key,
            data=io.BytesIO(raw),  # type: ignore[name-defined]
            length=len(raw),
            content_type="application/json",
        )

    print(json.dumps({"summary": summary, "manifest_key": manifest_key}, ensure_ascii=False))


if __name__ == "__main__":
    main()
