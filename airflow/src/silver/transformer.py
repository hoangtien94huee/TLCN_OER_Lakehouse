#!/usr/bin/env python3
"""
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
import unicodedata
from io import BytesIO
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from uuid import uuid4

try:
    from minio import Minio
    MINIO_AVAILABLE = True
except ImportError:
    Minio = None  # type: ignore
    MINIO_AVAILABLE = False

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_AVAILABLE = True
except ImportError:
    TfidfVectorizer = None  # type: ignore
    cosine_similarity = None  # type: ignore
    SKLEARN_AVAILABLE = False

try:
    import PyPDF2
    PYPDF_AVAILABLE = True
except ImportError:
    PyPDF2 = None  # type: ignore
    PYPDF_AVAILABLE = False

try:
    from src.hierarchical import MultilingualExtractiveSummarizer, TOCExtractor

    HIERARCHICAL_AVAILABLE = True
except ImportError:
    try:
        from hierarchical import MultilingualExtractiveSummarizer, TOCExtractor

        HIERARCHICAL_AVAILABLE = True
    except ImportError:
        MultilingualExtractiveSummarizer = None  # type: ignore
        TOCExtractor = None  # type: ignore
        HIERARCHICAL_AVAILABLE = False

try:
    from src.silver.benchmark import StageBenchmarkLogger
except ImportError:
    try:
        from silver.benchmark import StageBenchmarkLogger  # type: ignore
    except ImportError:
        from benchmark import StageBenchmarkLogger  # type: ignore

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# -----------------------------
# Pure-python helper functions
# -----------------------------

def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def clean_scalar(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple, set)):
        return None
    text = str(value).strip()
    return text or None


def clean_string_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, Iterable):
        items = list(value)
    else:
        return []

    out: List[str] = []
    seen = set()
    for item in items:
        text = clean_scalar(item)
        if not text:
            continue
        if text not in seen:
            seen.add(text)
            out.append(text)
    return out


def ensure_language_code(value: Any) -> str:
    text = (clean_scalar(value) or "en").lower()
    aliases = {
        "eng": "en",
        "english": "en",
        "vie": "vi",
        "vietnamese": "vi",
    }
    if text in aliases:
        return aliases[text]
    if len(text) >= 2:
        return text[:2]
    return "en"


def derive_source_system(record: Dict[str, Any]) -> str:
    for key in ("source_system", "source", "provider", "scraper"):
        value = clean_scalar(record.get(key))
        if value:
            return value.lower()
    url = clean_scalar(record.get("url") or record.get("link"))
    if url:
        u = url.lower()
        if "ocw.mit.edu" in u:
            return "mit_ocw"
        if "openstax" in u:
            return "openstax"
        if "open.umn.edu" in u:
            return "otl"
        if "oercommons" in u:
            return "oer_commons"
    bronze_path = clean_scalar(record.get("bronze_source_path")) or ""
    for known in ("mit_ocw", "openstax", "otl", "oer_commons"):
        if known in bronze_path.lower():
            return known
    return "unknown"


def derive_publisher(record: Dict[str, Any], source_system: str) -> str:
    publisher = clean_scalar(record.get("publisher"))
    if publisher:
        return publisher
    mapping = {
        "mit_ocw": "MIT OpenCourseWare",
        "openstax": "OpenStax",
        "otl": "Open Textbook Library",
        "oer_commons": "OER Commons",
    }
    return mapping.get(source_system, "Unknown")


def derive_license(record: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    name = clean_scalar(record.get("license") or record.get("rights"))
    url = clean_scalar(record.get("license_url") or record.get("rights_url"))
    if name and name.lower().startswith("http") and not url:
        url = name
        name = "License"
    return name, url


def parse_datetime_string(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = clean_scalar(value)
    if not text:
        return None
    if text.isdigit() and len(text) == 4:
        try:
            return datetime(int(text), 1, 1)
        except Exception:
            return None
    text = text.replace("Z", "+00:00")
    for fmt in (
        None,
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            if fmt is None:
                return datetime.fromisoformat(text)
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def select_title(record: Dict[str, Any]) -> Optional[str]:
    for key in ("title", "course_title", "book_title", "resource_title"):
        text = clean_scalar(record.get(key))
        if text:
            return text
    return None


def select_identifier(record: Dict[str, Any], source_system: str) -> Optional[str]:
    for key in ("resource_id", "course_id", "id", "uid"):
        value = clean_scalar(record.get(key))
        if value:
            return f"{source_system}_{value}"
    url = clean_scalar(record.get("url") or record.get("link"))
    if url:
        return f"{source_system}_{hashlib.sha1(url.encode('utf-8')).hexdigest()[:24]}"
    title = select_title(record)
    if title:
        slug = re.sub(r"[^a-z0-9]+", "_", normalize_text(title))[:80].strip("_")
        if slug:
            return f"{source_system}_{slug}"
    return None


def deterministic_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def compute_record_fingerprint(
    *,
    resource_id: str,
    source_system: str,
    source_url: Optional[str],
    title: Optional[str],
    description: Optional[str],
    creators: List[str],
    publisher_name: Optional[str],
    language: Optional[str],
    license_name: Optional[str],
    license_url: Optional[str],
    pdf_paths: List[str],
) -> str:
    canonical_payload = {
        "resource_id": resource_id,
        "source_system": source_system,
        "source_url": source_url or "",
        "title": title or "",
        "description": description or "",
        "creators": sorted([c for c in creators if c]),
        "publisher_name": publisher_name or "",
        "language": language or "",
        "license_name": license_name or "",
        "license_url": license_url or "",
        "pdf_paths": sorted([p for p in pdf_paths if p]),
    }
    return deterministic_hash(json.dumps(canonical_payload, ensure_ascii=False, sort_keys=True))


def compute_quality_score(
    *,
    title: Optional[str],
    description: Optional[str],
    creators: List[str],
    publisher_name: Optional[str],
    language: Optional[str],
    license_name: Optional[str],
    source_url: Optional[str],
    pdf_count: int,
) -> float:
    score = 0.0
    if title:
        score += 0.18
    if description and len(description) >= 80:
        score += 0.22
    if creators:
        score += 0.12
    if publisher_name and publisher_name != "Unknown":
        score += 0.08
    if language:
        score += 0.05
    if license_name:
        score += 0.12
    if source_url:
        score += 0.13
    if pdf_count > 0:
        score += 0.10
    return round(min(score, 1.0), 3)


class SubjectMatcher:
    """Moderate model-based matcher (TF-IDF cosine) with lexical fallback."""

    def __init__(self, subjects: List[Dict[str, Any]], programs_by_subject: Dict[int, List[int]], threshold: float = 0.55) -> None:
        self.threshold = threshold
        self.programs_by_subject = programs_by_subject
        self.subjects = []
        self.subject_corpus: List[str] = []
        self.vectorizer = None
        self.subject_matrix = None
        for subj in subjects:
            subject_id = subj.get("subject_id")
            if subject_id is None:
                continue
            name = clean_scalar(subj.get("subject_name"))
            name_en = clean_scalar(subj.get("subject_name_en"))
            code = clean_scalar(subj.get("subject_code"))
            tokens = set(normalize_text(" ".join([x for x in [name, name_en, code] if x])).split())
            self.subjects.append(
                {
                    "subject_id": int(subject_id),
                    "subject_name": name,
                    "subject_name_en": name_en,
                    "subject_code": code,
                    "tokens": tokens,
                    "norm_name": normalize_text(name),
                    "norm_name_en": normalize_text(name_en),
                    "norm_code": normalize_text(code),
                }
            )
            self.subject_corpus.append(normalize_text(" ".join([x for x in [name, name_en, code] if x])))

        if SKLEARN_AVAILABLE and self.subject_corpus:
            try:
                self.vectorizer = TfidfVectorizer(
                    analyzer="word",
                    ngram_range=(1, 2),
                    min_df=1,
                    stop_words="english",
                )
                self.subject_matrix = self.vectorizer.fit_transform(self.subject_corpus)
            except Exception:
                self.vectorizer = None
                self.subject_matrix = None

    def match(self, title: Optional[str], description: Optional[str], top_k: int = 5) -> List[Dict[str, Any]]:
        haystack = normalize_text(" ".join([x for x in [title, description] if x]))
        if not haystack:
            return []
        hay_tokens = set(haystack.split()) if haystack else set()
        matches: List[Dict[str, Any]] = []
        similarity_by_subject: Dict[int, float] = {}

        if self.vectorizer is not None and self.subject_matrix is not None and SKLEARN_AVAILABLE:
            try:
                query_vec = self.vectorizer.transform([haystack])
                cosine_scores = cosine_similarity(query_vec, self.subject_matrix).flatten()
                for idx, subj in enumerate(self.subjects):
                    similarity_by_subject[int(subj["subject_id"])] = float(round(float(cosine_scores[idx]), 4))
            except Exception:
                similarity_by_subject = {}

        for subj in self.subjects:
            score = 0.0
            matched_text = None

            if subj["norm_code"] and subj["norm_code"] in haystack:
                score = max(score, 0.99)
                matched_text = subj["subject_code"]
            if subj["norm_name_en"] and subj["norm_name_en"] in haystack:
                score = max(score, 0.94)
                matched_text = subj["subject_name_en"]
            if subj["norm_name"] and subj["norm_name"] in haystack:
                score = max(score, 0.92)
                matched_text = subj["subject_name"]

            subj_tokens = subj["tokens"]
            if subj_tokens:
                overlap = len(subj_tokens & hay_tokens)
                denom = max(1, min(len(subj_tokens), 6))
                token_score = overlap / denom
                if overlap >= 2:
                    score = max(score, round(token_score, 3))
                    if not matched_text:
                        matched_text = subj["subject_name_en"] or subj["subject_name"]

            model_score = similarity_by_subject.get(int(subj["subject_id"]), 0.0)
            if model_score > 0:
                score = max(score, model_score)
                if not matched_text:
                    matched_text = subj["subject_name_en"] or subj["subject_name"]

            if score >= self.threshold:
                matches.append(
                    {
                        "subject_id": subj["subject_id"],
                        "subject_name": subj["subject_name"],
                        "subject_name_en": subj["subject_name_en"],
                        "subject_code": subj["subject_code"],
                        "similarity": float(round(score, 4)),
                        "matched_text": matched_text,
                    }
                )

        matches.sort(key=lambda x: (-x["similarity"], x.get("subject_id") or 0))
        return matches[:top_k]


def _normalize_record_with_matcher(
    row: Dict[str, Any],
    subject_matcher: SubjectMatcher,
    programs_by_subject: Dict[int, List[int]],
) -> Optional[Dict[str, Any]]:
    source_system = derive_source_system(row)
    resource_id = select_identifier(row, source_system)
    if not resource_id:
        return None

    title = select_title(row)
    description = clean_scalar(row.get("description"))
    source_url = clean_scalar(row.get("url") or row.get("link"))
    creators = clean_string_list(row.get("instructors") or row.get("authors") or row.get("creators"))
    publisher_name = derive_publisher(row, source_system)
    language = ensure_language_code(row.get("language"))
    license_name, license_url = derive_license(row)
    publication_date = parse_datetime_string(row.get("publication_date") or row.get("year"))
    publication_year = publication_date.year if publication_date else None
    scraped_at = parse_datetime_string(row.get("scraped_at"))
    last_updated_at = parse_datetime_string(row.get("last_updated_at") or row.get("updated_at") or row.get("scraped_at"))
    bronze_source_path = clean_scalar(row.get("bronze_source_path"))
    pdf_paths = clean_string_list(row.get("pdf_paths"))
    pdf_types_found = clean_string_list(row.get("pdf_types_found"))
    pdf_count_declared = len(pdf_paths)
    has_assets = pdf_count_declared > 0
    resource_uid = deterministic_hash(resource_id)

    matched_subjects = subject_matcher.match(title, description, top_k=5)
    program_ids = sorted(
        {
            pid
            for subj in matched_subjects
            for pid in programs_by_subject.get(int(subj["subject_id"]), [])
        }
    )

    data_quality_score = compute_quality_score(
        title=title,
        description=description,
        creators=creators,
        publisher_name=publisher_name,
        language=language,
        license_name=license_name,
        source_url=source_url,
        pdf_count=pdf_count_declared,
    )
    record_fingerprint = compute_record_fingerprint(
        resource_id=resource_id,
        source_system=source_system,
        source_url=source_url,
        title=title,
        description=description,
        creators=creators,
        publisher_name=publisher_name,
        language=language,
        license_name=license_name,
        license_url=license_url,
        pdf_paths=pdf_paths,
    )

    now = datetime.utcnow()
    return {
        "resource_uid": resource_uid,
        "resource_id": resource_id,
        "source_system": source_system,
        "source_url": source_url,
        "title": title,
        "description": description,
        "creator_names": creators,
        "publisher_name": publisher_name,
        "language": language,
        "license_name": license_name,
        "license_url": license_url,
        "publication_date": publication_date,
        "publication_year": publication_year,
        "scraped_at": scraped_at,
        "last_updated_at": last_updated_at,
        "bronze_source_path": bronze_source_path,
        "pdf_paths": pdf_paths,
        "pdf_types_found": pdf_types_found,
        "pdf_count_declared": pdf_count_declared,
        "has_assets": has_assets,
        "matched_subjects": matched_subjects,
        "program_ids": program_ids,
        "record_fingerprint": record_fingerprint,
        "data_quality_score": data_quality_score,
        "ingested_at": now,
    }


def _enrich_document_with_client(row: Dict[str, Any], minio_client: Optional[Minio], bucket: str) -> Dict[str, Any]:
    now = datetime.utcnow()
    asset_path = clean_scalar(row.get("asset_path")) or ""
    mime_type = mimetypes.guess_type(asset_path)[0] or "application/pdf"
    etag = None
    size_bytes = None
    last_modified = None

    if minio_client and asset_path:
        try:
            stat = minio_client.stat_object(bucket, asset_path)
            etag = clean_scalar(getattr(stat, "etag", None))
            size_bytes = int(getattr(stat, "size", 0) or 0)
            last_modified = getattr(stat, "last_modified", None)
        except Exception:
            pass

    return {
        "asset_uid": row.get("asset_uid"),
        "resource_uid": row.get("resource_uid"),
        "resource_id": row.get("resource_id"),
        "source_system": row.get("source_system"),
        "source_url": row.get("source_url"),
        "title": row.get("title"),
        "asset_order": row.get("asset_order"),
        "asset_path": asset_path,
        "file_name": row.get("file_name"),
        "asset_extension": row.get("asset_extension"),
        "etag": etag,
        "size_bytes": size_bytes,
        "mime_type": mime_type,
        "last_modified": last_modified,
        "language": row.get("language"),
        "license_name": row.get("license_name"),
        "license_url": row.get("license_url"),
        "scraped_at": row.get("scraped_at"),
        "updated_at": now,
    }


def _create_partition_minio_client(
    *,
    endpoint: str,
    access_key: str,
    secret_key: str,
    secure: bool,
) -> Optional[Minio]:
    if not MINIO_AVAILABLE:
        return None
    try:
        return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    except Exception:
        return None


class _PartitionChunker:
    def __init__(
        self,
        *,
        minio_client: Optional[Minio],
        bucket: str,
        max_pages_per_pdf: int,
        chunk_max_chars: int,
        chunk_min_chars: int,
        chunk_overlap_chars: int,
        section_chunk_max_chars: int,
        toc_fallback_chapter_size: int,
        toc_min_confidence: float,
        toc_enabled: bool,
        doc_summary_max_chars: int,
        chapter_summary_max_chars: int,
        toc_extractor: Optional[Any] = None,
        summarizer: Optional[Any] = None,
    ) -> None:
        self.minio_client = minio_client
        self.bucket = bucket
        self.max_pages_per_pdf = max_pages_per_pdf
        self.chunk_max_chars = chunk_max_chars
        self.chunk_min_chars = chunk_min_chars
        self.chunk_overlap_chars = chunk_overlap_chars
        self.section_chunk_max_chars = section_chunk_max_chars
        self.toc_fallback_chapter_size = toc_fallback_chapter_size
        self.toc_min_confidence = toc_min_confidence
        self.toc_enabled = toc_enabled
        self.doc_summary_max_chars = doc_summary_max_chars
        self.chapter_summary_max_chars = chapter_summary_max_chars
        self.toc_extractor = toc_extractor
        self.summarizer = summarizer

    def _get_pdf_bytes(self, asset_path: str) -> Optional[bytes]:
        if not self.minio_client or not asset_path:
            return None
        response = None
        try:
            response = self.minio_client.get_object(self.bucket, asset_path)
            return response.read()
        except Exception:
            return None
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def _normalize_pdf_text(self, text: str) -> str:
        text = re.sub(r"-\s*\n\s*", "", text)
        text = text.replace("\r", "\n")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _extract_pdf_page_texts(self, pdf_bytes: bytes) -> Tuple[Dict[int, str], int]:
        if not PYPDF_AVAILABLE:
            return {}, 0
        try:
            reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
            max_pages = min(len(reader.pages), self.max_pages_per_pdf)
            page_texts: Dict[int, str] = {}
            for idx in range(max_pages):
                raw = clean_scalar(reader.pages[idx].extract_text()) or ""
                page_texts[idx + 1] = self._normalize_pdf_text(raw)
            return page_texts, max_pages
        except Exception:
            return {}, 0

    def _split_long_segment(
        self,
        text: str,
        max_chars: Optional[int] = None,
        min_chars: Optional[int] = None,
        overlap_chars: Optional[int] = None,
    ) -> List[str]:
        chunks: List[str] = []
        text = text.strip()
        if not text:
            return chunks

        max_chars = int(max_chars or self.chunk_max_chars)
        min_chars = int(min_chars or self.chunk_min_chars)
        overlap_chars = int(overlap_chars if overlap_chars is not None else self.chunk_overlap_chars)
        overlap = min(max(overlap_chars, 0), max(max_chars // 2, 0))
        step = max(1, max_chars - overlap)
        start = 0
        while start < len(text):
            end = min(start + max_chars, len(text))
            if end < len(text):
                window_start = min(end, start + max(min_chars, max_chars // 2))
                split_pos = max(
                    text.rfind("\n", window_start, end),
                    text.rfind(". ", window_start, end),
                    text.rfind("? ", window_start, end),
                    text.rfind("! ", window_start, end),
                )
                if split_pos > start:
                    end = split_pos + 1

            chunk = text[start:end].strip()
            if chunk and (len(chunk) >= min_chars or end == len(text)):
                chunks.append(chunk)
            if end >= len(text):
                break
            start = start + step if end <= start else max(start + 1, end - overlap)
        return chunks

    def _chunk_text_smart(
        self,
        text: str,
        max_chars: Optional[int] = None,
        min_chars: Optional[int] = None,
        overlap_chars: Optional[int] = None,
    ) -> List[str]:
        max_chars = int(max_chars or self.chunk_max_chars)
        min_chars = int(min_chars or self.chunk_min_chars)
        overlap_chars = int(overlap_chars if overlap_chars is not None else self.chunk_overlap_chars)
        paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p and p.strip()]
        if not paragraphs:
            return self._split_long_segment(text, max_chars=max_chars, min_chars=min_chars, overlap_chars=overlap_chars)

        chunks: List[str] = []
        current_parts: List[str] = []
        current_len = 0

        def flush_current() -> None:
            nonlocal current_parts, current_len
            if current_parts:
                chunk = "\n\n".join(current_parts).strip()
                if chunk:
                    chunks.append(chunk)
            current_parts = []
            current_len = 0

        for para in paragraphs:
            if len(para) > max_chars:
                flush_current()
                chunks.extend(self._split_long_segment(para, max_chars=max_chars, min_chars=min_chars, overlap_chars=overlap_chars))
                continue

            projected = current_len + (2 if current_parts else 0) + len(para)
            if projected <= max_chars:
                current_parts.append(para)
                current_len = projected
            else:
                flush_current()
                current_parts.append(para)
                current_len = len(para)

        flush_current()
        return chunks if chunks else self._split_long_segment(text, max_chars=max_chars, min_chars=min_chars, overlap_chars=overlap_chars)

    def _extract_pdf_pages(self, asset_path: str) -> List[Tuple[int, int, str]]:
        pdf_bytes = self._get_pdf_bytes(asset_path)
        if not pdf_bytes:
            return []
        page_texts, _ = self._extract_pdf_page_texts(pdf_bytes)
        pages: List[Tuple[int, int, str]] = []
        for page_no in sorted(page_texts.keys()):
            page_text = page_texts[page_no]
            if not page_text:
                continue
            for chunk_index, chunk in enumerate(self._chunk_text_smart(page_text), start=1):
                if chunk:
                    pages.append((page_no, chunk_index, chunk))
        return pages

    def _build_flat_toc(self, total_pages: int) -> List[Dict[str, Any]]:
        chapter_size = max(10, self.toc_fallback_chapter_size)
        toc: List[Dict[str, Any]] = []
        chapter_num = 0
        for start_page in range(1, total_pages + 1, chapter_size):
            chapter_num += 1
            end_page = min(start_page + chapter_size - 1, total_pages)
            toc.append(
                {
                    "chapter_id": f"ch{chapter_num:02d}",
                    "chapter_number": chapter_num,
                    "chapter_title": f"Part {chapter_num}",
                    "page_start": start_page,
                    "page_end": end_page,
                    "sections": [],
                }
            )
        return toc

    def chunk_document_record(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        asset_path = clean_scalar(row.get("asset_path")) or ""
        if not asset_path.lower().endswith(".pdf"):
            return []
        resource_uid = clean_scalar(row.get("resource_uid"))
        asset_uid = clean_scalar(row.get("asset_uid"))
        if not resource_uid or not asset_uid:
            return []

        records: List[Dict[str, Any]] = []
        now = datetime.utcnow()
        for page_no, chunk_order, chunk_text in self._extract_pdf_pages(asset_path):
            token_count = len(re.findall(r"\w+", chunk_text))
            chunk_id = deterministic_hash(f"{asset_uid}::{page_no}::{chunk_order}::{chunk_text[:128]}")
            records.append(
                {
                    "chunk_id": chunk_id,
                    "resource_uid": resource_uid,
                    "asset_uid": asset_uid,
                    "page_no": int(page_no),
                    "chunk_order": int(chunk_order),
                    "chunk_text": chunk_text,
                    "token_count": int(token_count),
                    "lang": ensure_language_code(row.get("language")),
                    "chunk_type": "section_detail",
                    "chunk_tier": 3,
                    "chapter_id": None,
                    "chapter_title": None,
                    "chapter_number": None,
                    "chapter_page_start": None,
                    "chapter_page_end": None,
                    "section_id": None,
                    "section_title": None,
                    "section_number": None,
                    "section_page_start": int(page_no),
                    "section_page_end": int(page_no),
                    "parent_chunk_id": None,
                    "has_children": False,
                    "is_summary": False,
                    "summary_method": None,
                    "updated_at": now,
                }
            )
        return records

    def chunk_document_hierarchical(self, row: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        asset_path = clean_scalar(row.get("asset_path")) or ""
        if not asset_path.lower().endswith(".pdf"):
            return [], None
        resource_uid = clean_scalar(row.get("resource_uid"))
        asset_uid = clean_scalar(row.get("asset_uid"))
        if not resource_uid or not asset_uid:
            return [], None

        pdf_bytes = self._get_pdf_bytes(asset_path)
        if not pdf_bytes:
            return self.chunk_document_record(row), None

        page_texts, total_pages = self._extract_pdf_page_texts(pdf_bytes)
        if total_pages <= 0:
            return self.chunk_document_record(row), None

        toc_result = {"method": "flat", "confidence": 0.5, "toc": [], "total_pages": total_pages, "structure_valid": False}
        if self.toc_enabled and self.toc_extractor:
            toc_result = self.toc_extractor.extract(pdf_bytes, max_pages=total_pages)

        toc = toc_result.get("toc") or []
        confidence = float(toc_result.get("confidence") or 0.0)
        method = str(toc_result.get("method") or "flat")
        structure_valid = bool(toc_result.get("structure_valid"))

        if not toc:
            toc = self._build_flat_toc(total_pages)
            method = "flat"
            confidence = 0.5

        if confidence < self.toc_min_confidence:
            method = "flat"
            toc = self._build_flat_toc(total_pages)

        now = datetime.utcnow()
        lang = ensure_language_code(row.get("language"))
        chunk_records: List[Dict[str, Any]] = []
        section_global_order = 0

        doc_summary = ""
        if self.summarizer:
            doc_summary = self.summarizer.generate_document_summary(row, toc, max_chars=self.doc_summary_max_chars)
        if doc_summary:
            doc_chunk_id = deterministic_hash(f"{asset_uid}::tier1::doc_summary")
            chunk_records.append(
                {
                    "chunk_id": doc_chunk_id,
                    "resource_uid": resource_uid,
                    "asset_uid": asset_uid,
                    "page_no": 1,
                    "chunk_order": 1,
                    "chunk_text": doc_summary,
                    "token_count": int(len(re.findall(r"\w+", doc_summary))),
                    "lang": lang,
                    "chunk_type": "doc_summary",
                    "chunk_tier": 1,
                    "chapter_id": None,
                    "chapter_title": None,
                    "chapter_number": None,
                    "chapter_page_start": None,
                    "chapter_page_end": None,
                    "section_id": None,
                    "section_title": None,
                    "section_number": None,
                    "section_page_start": None,
                    "section_page_end": None,
                    "parent_chunk_id": None,
                    "has_children": True,
                    "is_summary": True,
                    "summary_method": "extractive",
                    "updated_at": now,
                }
            )

        for chapter_idx, chapter in enumerate(toc, start=1):
            chapter_id = clean_scalar(chapter.get("chapter_id")) or f"ch{chapter_idx:02d}"
            chapter_title = clean_scalar(chapter.get("chapter_title")) or f"Chapter {chapter_idx}"
            chapter_number = int(chapter.get("chapter_number") or chapter_idx)
            chapter_start = max(1, min(int(chapter.get("page_start") or 1), total_pages))
            chapter_end = max(chapter_start, min(int(chapter.get("page_end") or chapter_start), total_pages))

            chapter_text = "\n\n".join(
                [page_texts.get(page_no, "") for page_no in range(chapter_start, chapter_end + 1) if page_texts.get(page_no, "").strip()]
            ).strip()
            if not chapter_text:
                continue

            chapter_summary = chapter_title
            if self.summarizer:
                chapter_summary = self.summarizer.generate_chapter_summary(
                    chapter_text,
                    chapter_title,
                    max_chars=self.chapter_summary_max_chars,
                )
            chapter_chunk_id = deterministic_hash(f"{asset_uid}::tier2::{chapter_id}")
            chunk_records.append(
                {
                    "chunk_id": chapter_chunk_id,
                    "resource_uid": resource_uid,
                    "asset_uid": asset_uid,
                    "page_no": chapter_start,
                    "chunk_order": chapter_idx,
                    "chunk_text": chapter_summary,
                    "token_count": int(len(re.findall(r"\w+", chapter_summary))),
                    "lang": lang,
                    "chunk_type": "chapter_summary",
                    "chunk_tier": 2,
                    "chapter_id": chapter_id,
                    "chapter_title": chapter_title,
                    "chapter_number": chapter_number,
                    "chapter_page_start": chapter_start,
                    "chapter_page_end": chapter_end,
                    "section_id": None,
                    "section_title": None,
                    "section_number": None,
                    "section_page_start": None,
                    "section_page_end": None,
                    "parent_chunk_id": None,
                    "has_children": True,
                    "is_summary": True,
                    "summary_method": "extractive",
                    "updated_at": now,
                }
            )

            sections = chapter.get("sections") or []
            if not sections:
                sections = [
                    {
                        "section_id": f"{chapter_id}_sec01",
                        "section_number": f"{chapter_number}.1",
                        "section_title": chapter_title,
                        "page_start": chapter_start,
                        "page_end": chapter_end,
                    }
                ]

            for section_idx, section in enumerate(sections, start=1):
                section_id = clean_scalar(section.get("section_id")) or f"{chapter_id}_sec{section_idx:02d}"
                section_number = clean_scalar(section.get("section_number")) or f"{chapter_number}.{section_idx}"
                section_title = clean_scalar(section.get("section_title")) or chapter_title
                section_start = max(chapter_start, min(int(section.get("page_start") or chapter_start), chapter_end))
                section_end = max(section_start, min(int(section.get("page_end") or chapter_end), chapter_end))

                section_text = "\n\n".join(
                    [page_texts.get(page_no, "") for page_no in range(section_start, section_end + 1) if page_texts.get(page_no, "").strip()]
                ).strip()
                if not section_text:
                    continue

                detail_chunks = self._chunk_text_smart(
                    section_text,
                    max_chars=self.section_chunk_max_chars,
                    min_chars=max(self.chunk_min_chars, 220),
                    overlap_chars=self.chunk_overlap_chars,
                )

                for local_idx, detail in enumerate(detail_chunks, start=1):
                    if not detail:
                        continue
                    section_global_order += 1
                    detail_id = deterministic_hash(f"{asset_uid}::tier3::{section_id}::{local_idx}::{detail[:128]}")
                    chunk_records.append(
                        {
                            "chunk_id": detail_id,
                            "resource_uid": resource_uid,
                            "asset_uid": asset_uid,
                            "page_no": section_start,
                            "chunk_order": section_global_order,
                            "chunk_text": detail,
                            "token_count": int(len(re.findall(r"\w+", detail))),
                            "lang": lang,
                            "chunk_type": "section_detail",
                            "chunk_tier": 3,
                            "chapter_id": chapter_id,
                            "chapter_title": chapter_title,
                            "chapter_number": chapter_number,
                            "chapter_page_start": chapter_start,
                            "chapter_page_end": chapter_end,
                            "section_id": section_id,
                            "section_title": section_title,
                            "section_number": section_number,
                            "section_page_start": section_start,
                            "section_page_end": section_end,
                            "parent_chunk_id": chapter_chunk_id,
                            "has_children": False,
                            "is_summary": False,
                            "summary_method": None,
                            "updated_at": now,
                        }
                    )

        structure_record = {
            "structure_id": deterministic_hash(asset_uid),
            "asset_uid": asset_uid,
            "resource_uid": resource_uid,
            "source_system": clean_scalar(row.get("source_system")),
            "has_toc": method != "flat",
            "toc_extracted_at": now,
            "toc_method": method,
            "toc_confidence": confidence,
            "total_pages": total_pages,
            "total_chapters": len(toc),
            "total_sections": int(sum(len(ch.get("sections") or []) for ch in toc)),
            "table_of_contents_json": json.dumps(toc, ensure_ascii=False),
            "structure_valid": structure_valid,
            "created_at": now,
            "updated_at": now,
        }
        return chunk_records, structure_record


class SilverTransformer:
    def __init__(self) -> None:
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.catalog_name = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.database_name = os.getenv("SILVER_DATABASE", "default")
        self.bronze_input = os.getenv("BRONZE_INPUT", f"s3a://{self.bucket}/bronze/")

        self.resources_curated_table = f"{self.catalog_name}.{self.database_name}.oer_resources_curated"
        self.documents_table = f"{self.catalog_name}.{self.database_name}.oer_documents"
        self.chunks_table = f"{self.catalog_name}.{self.database_name}.oer_chunks"
        self.document_structure_table = f"{self.catalog_name}.{self.database_name}.oer_document_structure"
        # Backward compatibility for downstream jobs still expecting this name.
        self.resources_table = f"{self.catalog_name}.{self.database_name}.oer_resources"

        self.reference_subjects_table = f"{self.catalog_name}.{self.database_name}.reference_subjects"
        self.reference_faculties_table = f"{self.catalog_name}.{self.database_name}.reference_faculties"
        self.reference_programs_table = f"{self.catalog_name}.{self.database_name}.reference_programs"
        self.reference_program_subject_links_table = f"{self.catalog_name}.{self.database_name}.reference_program_subject_links"
        self.pipeline_state_table = f"{self.catalog_name}.{self.database_name}.pipeline_state"
        self.reference_state_key = "reference_bootstrap"

        self.run_reference_bootstrap_enabled = os.getenv("RUN_REFERENCE_BOOTSTRAP", "0").lower() in {"1", "true", "yes"}
        self.force_reference_refresh = os.getenv("FORCE_REFERENCE_REFRESH", "0").lower() in {"1", "true", "yes"}
        self.chunk_max_chars = int(os.getenv("SILVER_CHUNK_MAX_CHARS", "1800"))
        overlap_chars = (
            os.getenv("SILVER_CHUNK_OVERLAP_CHARS")
            or os.getenv("SILVER_CHUNK_OVERLAP")
            or "240"
        )
        self.chunk_overlap_chars = int(overlap_chars)
        self.chunk_min_chars = int(os.getenv("SILVER_CHUNK_MIN_CHARS", "220"))
        self.max_pages_per_pdf = int(os.getenv("SILVER_MAX_PDF_PAGES", "200"))
        self.enable_hierarchical = os.getenv("ENABLE_HIERARCHICAL_CHUNKING", "0").lower() in {"1", "true", "yes"}
        self.toc_enabled = os.getenv("TOC_EXTRACTION_ENABLED", "1").lower() in {"1", "true", "yes"}
        self.toc_min_confidence = float(os.getenv("TOC_MIN_CONFIDENCE", "0.60"))
        self.toc_fallback_chapter_size = int(os.getenv("TOC_FALLBACK_CHAPTER_SIZE", "50"))
        self.doc_summary_max_chars = int(os.getenv("DOC_SUMMARY_MAX_CHARS", "1000"))
        self.chapter_summary_max_chars = int(os.getenv("CHAPTER_SUMMARY_MAX_CHARS", "800"))
        self.section_chunk_max_chars = int(os.getenv("SECTION_CHUNK_MAX_CHARS", str(self.chunk_max_chars)))

        self.spark = self._create_spark_session()
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.database_name}")

        self.reference_storage = "local"
        self.reference_bucket: Optional[str] = None
        self.reference_prefix: str = ""
        self.reference_path: Path = Path(os.getenv("REFERENCE_DATA_URI", "/opt/airflow/reference"))
        self.minio_client: Optional[Minio] = self._create_minio_client() if MINIO_AVAILABLE else None

        reference_uri = os.getenv("REFERENCE_DATA_URI")
        if reference_uri and reference_uri.startswith(("s3://", "s3a://")) and MINIO_AVAILABLE:
            self.reference_storage = "minio"
            self.reference_bucket, self.reference_prefix = self._parse_s3_uri(reference_uri)

        self.reference_subject_records: List[Dict[str, Any]] = []
        self.reference_faculty_records: List[Dict[str, Any]] = []
        self.reference_program_records: List[Dict[str, Any]] = []
        self.reference_program_subject_link_records: List[Dict[str, Any]] = []
        self.programs_by_subject: Dict[int, List[int]] = {}
        self._load_reference_data()

        self.subject_matcher = SubjectMatcher(
            self.reference_subject_records,
            self.programs_by_subject,
            threshold=float(os.getenv("SUBJECT_MATCH_THRESHOLD", "0.55")),
        )

        self.toc_extractor = None
        self.summarizer = None
        if self.enable_hierarchical and HIERARCHICAL_AVAILABLE:
            self.toc_extractor = TOCExtractor(
                fallback_chapter_size=self.toc_fallback_chapter_size,
            )
            self.summarizer = MultilingualExtractiveSummarizer()

    # -----------------------------
    # Spark + reference setup
    # -----------------------------
    def _create_spark_session(self) -> SparkSession:
        java_home = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
        os.environ.setdefault("JAVA_HOME", java_home)
        os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        # Avoid JVM bootstrap conflict with inherited options (seen in Airflow runtime).
        os.environ.pop("JAVA_TOOL_OPTIONS", None)

        spark_master = os.getenv("SPARK_MASTER", os.getenv("SPARK_MASTER_URL", "local[*]"))
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        s3a_endpoint = minio_endpoint if minio_endpoint.startswith(("http://", "https://")) else f"http://{minio_endpoint}"
        builder = SparkSession.builder.appName("silver_oer_transformer").master(spark_master)

        spark_jars = os.getenv("SPARK_JARS")
        use_local_jars = False
        if spark_jars:
            jar_paths = [p.strip() for p in spark_jars.split(",") if p.strip()]
            use_local_jars = bool(jar_paths) and all(Path(p).exists() for p in jar_paths)
            if use_local_jars:
                builder = (
                    builder
                    .config("spark.jars", spark_jars)
                    .config("spark.driver.extraClassPath", spark_jars)
                    .config("spark.executor.extraClassPath", spark_jars)
                )
        if not use_local_jars:
            builder = (
                builder
                .config(
                    "spark.jars.packages",
                    ",".join(
                        [
                            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
                            "org.apache.hadoop:hadoop-aws:3.3.4",
                            "com.amazonaws:aws-java-sdk-bundle:1.12.565",
                        ]
                    ),
                )
                .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2"))
            )
        builder = (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.catalog_name}.warehouse", f"s3a://{self.bucket}/silver/")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.endpoint", s3a_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "oer-airflow-scraper"))
            .config("spark.driver.bindAddress", os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"))
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        )
        return builder.getOrCreate()

    def _parse_s3_uri(self, uri: str) -> Tuple[str, str]:
        no_scheme = uri.split("://", 1)[1]
        bucket, _, prefix = no_scheme.partition("/")
        return bucket, prefix.rstrip("/")

    def _create_minio_client(self) -> Optional[Minio]:
        if not MINIO_AVAILABLE:
            return None
        endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

    def _load_reference_json(self, name: str) -> List[Dict[str, Any]]:
        if self.reference_storage == "minio" and self.minio_client and self.reference_bucket:
            obj_name = f"{self.reference_prefix}/{name}" if self.reference_prefix else name
            response = self.minio_client.get_object(self.reference_bucket, obj_name)
            try:
                data = json.loads(response.read().decode("utf-8"))
                return data if isinstance(data, list) else []
            finally:
                response.close()
                response.release_conn()
        path = self.reference_path / name
        if not path.exists():
            return []
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_reference_data(self) -> None:
        self.reference_subject_records = self._load_reference_json("subjects.json")
        self.reference_faculty_records = self._load_reference_json("faculties.json")
        self.reference_program_records = self._load_reference_json("programs.json")
        self.reference_program_subject_link_records = self._load_reference_json("program_subject_links.json")

        programs_by_subject: Dict[int, List[int]] = {}
        for row in self.reference_program_subject_link_records:
            try:
                subject_id = int(row["subject_id"])
                program_id = int(row["program_id"])
            except Exception:
                continue
            programs_by_subject.setdefault(subject_id, [])
            if program_id not in programs_by_subject[subject_id]:
                programs_by_subject[subject_id].append(program_id)
        for subject_id, program_ids in programs_by_subject.items():
            program_ids.sort()
        self.programs_by_subject = programs_by_subject

    def _compute_reference_hash(self) -> str:
        payload = {
            "subjects": self.reference_subject_records,
            "faculties": self.reference_faculty_records,
            "programs": self.reference_program_records,
            "program_subject_links": self.reference_program_subject_link_records,
        }
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return deterministic_hash(canonical)

    def _ensure_pipeline_state_table(self) -> None:
        if self._table_exists(self.pipeline_state_table):
            return
        self.spark.sql(
            f"""
            CREATE TABLE {self.pipeline_state_table} (
                state_key STRING,
                state_hash STRING,
                updated_at TIMESTAMP
            ) USING iceberg
            """
        )

    def _reference_needs_refresh(self, current_hash: str) -> bool:
        if self.force_reference_refresh:
            return True
        if not self._table_exists(self.pipeline_state_table):
            return True
        state_df = (
            self.spark.table(self.pipeline_state_table)
            .filter(F.col("state_key") == self.reference_state_key)
            .orderBy(F.col("updated_at").desc_nulls_last())
            .limit(1)
        )
        if state_df.rdd.isEmpty():
            return True
        last_hash = state_df.select("state_hash").collect()[0]["state_hash"]
        return last_hash != current_hash

    def _update_reference_state(self, current_hash: str) -> None:
        self._ensure_pipeline_state_table()
        state_schema = T.StructType(
            [
                T.StructField("state_key", T.StringType(), False),
                T.StructField("state_hash", T.StringType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        state_df = self.spark.createDataFrame(
            [(self.reference_state_key, current_hash, datetime.utcnow())],
            schema=state_schema,
        )
        self._merge_into(state_df, self.pipeline_state_table, ["state_key"])

    def _write_reference_tables(self) -> None:
        def create_df(records: List[Dict[str, Any]], integer_cols: Optional[List[str]] = None) -> Optional[DataFrame]:
            if not records:
                return None

            integer_cols_set = set(integer_cols or [])
            all_cols = sorted({k for row in records for k in row.keys()})

            def normalize_cell(col: str, value: Any) -> Any:
                if value is None:
                    return None
                if col in integer_cols_set:
                    try:
                        return int(value)
                    except Exception:
                        return None
                if isinstance(value, (dict, list, tuple, set)):
                    return json.dumps(value, ensure_ascii=False)
                if isinstance(value, (bool, int, float)):
                    return value
                return str(value)

            normalized_rows = []
            for row in records:
                normalized_rows.append({col: normalize_cell(col, row.get(col)) for col in all_cols})

            fields: List[T.StructField] = []
            for col in all_cols:
                if col in integer_cols_set:
                    dtype: T.DataType = T.IntegerType()
                else:
                    sample = next((r.get(col) for r in normalized_rows if r.get(col) is not None), None)
                    if isinstance(sample, bool):
                        dtype = T.BooleanType()
                    elif isinstance(sample, int):
                        dtype = T.IntegerType()
                    elif isinstance(sample, float):
                        dtype = T.DoubleType()
                    else:
                        dtype = T.StringType()
                fields.append(T.StructField(col, dtype, True))

            schema = T.StructType(fields)
            data = [tuple(r.get(col) for col in all_cols) for r in normalized_rows]
            return self.spark.createDataFrame(data, schema=schema)

        subjects_df = create_df(self.reference_subject_records, integer_cols=["subject_id"])
        faculties_df = create_df(self.reference_faculty_records, integer_cols=["faculty_id"])
        programs_df = create_df(self.reference_program_records, integer_cols=["program_id", "faculty_id"])
        links_df = create_df(self.reference_program_subject_link_records, integer_cols=["program_id", "subject_id"])

        if subjects_df is not None:
            subjects_df = subjects_df.dropDuplicates(["subject_id"])
            subjects_df.writeTo(self.reference_subjects_table).createOrReplace()
        if faculties_df is not None:
            faculties_df = faculties_df.dropDuplicates(["faculty_id"])
            faculties_df.writeTo(self.reference_faculties_table).createOrReplace()
        if programs_df is not None:
            programs_df = programs_df.dropDuplicates(["program_id"])
            programs_df.writeTo(self.reference_programs_table).createOrReplace()
        if links_df is not None:
            links_df = links_df.dropDuplicates(["program_id", "subject_id"])
            links_df.writeTo(self.reference_program_subject_links_table).createOrReplace()

    def run_reference_bootstrap(self) -> None:
        reference_hash = self._compute_reference_hash()
        if not self._reference_needs_refresh(reference_hash):
            print("Reference tables unchanged; skipping bootstrap.")
            return
        print("Writing reference tables (bootstrap/refresh)...")
        self._write_reference_tables()
        self._update_reference_state(reference_hash)

    # -----------------------------
    # Bronze read + normalization
    # -----------------------------
    def _read_bronze(self) -> DataFrame:
        df = self.spark.read.option("multiline", True).json(self.bronze_input)
        if "bronze_source_path" not in df.columns:
            df = df.withColumn("bronze_source_path", F.input_file_name())
        return df

    def _normalize_record_python(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _normalize_record_with_matcher(row, self.subject_matcher, self.programs_by_subject)

    def _base_schema(self) -> T.StructType:
        matched_struct = T.StructType(
            [
                T.StructField("subject_id", T.IntegerType(), True),
                T.StructField("subject_name", T.StringType(), True),
                T.StructField("subject_name_en", T.StringType(), True),
                T.StructField("subject_code", T.StringType(), True),
                T.StructField("similarity", T.DoubleType(), True),
                T.StructField("matched_text", T.StringType(), True),
            ]
        )
        return T.StructType(
            [
                T.StructField("resource_uid", T.StringType(), False),
                T.StructField("resource_id", T.StringType(), False),
                T.StructField("source_system", T.StringType(), True),
                T.StructField("source_url", T.StringType(), True),
                T.StructField("title", T.StringType(), True),
                T.StructField("description", T.StringType(), True),
                T.StructField("creator_names", T.ArrayType(T.StringType()), True),
                T.StructField("publisher_name", T.StringType(), True),
                T.StructField("language", T.StringType(), True),
                T.StructField("license_name", T.StringType(), True),
                T.StructField("license_url", T.StringType(), True),
                T.StructField("publication_date", T.TimestampType(), True),
                T.StructField("publication_year", T.IntegerType(), True),
                T.StructField("scraped_at", T.TimestampType(), True),
                T.StructField("last_updated_at", T.TimestampType(), True),
                T.StructField("bronze_source_path", T.StringType(), True),
                T.StructField("pdf_paths", T.ArrayType(T.StringType()), True),
                T.StructField("pdf_types_found", T.ArrayType(T.StringType()), True),
                T.StructField("pdf_count_declared", T.IntegerType(), True),
                T.StructField("has_assets", T.BooleanType(), True),
                T.StructField("matched_subjects", T.ArrayType(matched_struct), True),
                T.StructField("program_ids", T.ArrayType(T.IntegerType()), True),
                T.StructField("record_fingerprint", T.StringType(), False),
                T.StructField("data_quality_score", T.DoubleType(), True),
                T.StructField("ingested_at", T.TimestampType(), False),
            ]
        )

    def _build_base_df(self, bronze_df: DataFrame) -> DataFrame:
        schema = self._base_schema()
        subject_records_bc = self.spark.sparkContext.broadcast(self.reference_subject_records)
        programs_by_subject_bc = self.spark.sparkContext.broadcast(self.programs_by_subject)
        match_threshold = float(self.subject_matcher.threshold)

        def normalize_partition(rows: Iterator[Any]) -> Iterator[Dict[str, Any]]:
            local_subjects = subject_records_bc.value
            local_programs = programs_by_subject_bc.value
            matcher = SubjectMatcher(local_subjects, local_programs, threshold=match_threshold)
            for row in rows:
                normalized = _normalize_record_with_matcher(row.asDict(recursive=True), matcher, local_programs)
                if normalized is not None:
                    yield normalized

        normalized_rdd = bronze_df.rdd.mapPartitions(normalize_partition)
        base_df = self.spark.createDataFrame(normalized_rdd, schema=schema)

        # Latest record wins if same resource appears multiple times.
        window = Window.partitionBy("resource_uid").orderBy(
            F.col("scraped_at").desc_nulls_last(),
            F.col("ingested_at").desc_nulls_last(),
            F.col("last_updated_at").desc_nulls_last(),
        )
        return base_df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")

    # -----------------------------
    # Silver table builders
    # -----------------------------
    def _filter_incremental_resources(self, base_df: DataFrame) -> DataFrame:
        if not self._table_exists(self.resources_curated_table):
            return base_df

        existing_table_df = self.spark.table(self.resources_curated_table)
        existing_has_fingerprint = "record_fingerprint" in existing_table_df.columns
        existing_df = existing_table_df.select(
            "resource_uid",
            F.col("scraped_at").alias("existing_scraped_at"),
            F.col("last_updated_at").alias("existing_last_updated_at"),
            F.col("ingested_at").alias("existing_ingested_at"),
            (
                F.col("record_fingerprint")
                if existing_has_fingerprint
                else F.lit(None).cast("string")
            ).alias("existing_record_fingerprint"),
        )
        filtered = (
            base_df.alias("n")
            .join(existing_df.alias("e"), on="resource_uid", how="left")
            .filter(
                F.col("e.resource_uid").isNull()
                | (
                    F.coalesce(F.col("n.scraped_at"), F.col("n.ingested_at"))
                    > F.coalesce(F.col("e.existing_scraped_at"), F.col("e.existing_ingested_at"))
                )
                | (
                    F.coalesce(F.col("n.last_updated_at"), F.col("n.ingested_at"))
                    > F.coalesce(F.col("e.existing_last_updated_at"), F.col("e.existing_ingested_at"))
                )
                | (
                    F.coalesce(F.col("n.record_fingerprint"), F.lit(""))
                    != F.coalesce(F.col("e.existing_record_fingerprint"), F.lit(""))
                )
            )
            .select("n.*")
        )
        return filtered

    def _build_resources_curated_df(self, base_df: DataFrame) -> DataFrame:
        return base_df.select(
            "resource_uid",
            "resource_id",
            "source_system",
            "source_url",
            "title",
            "description",
            "creator_names",
            "publisher_name",
            "language",
            "license_name",
            "license_url",
            "publication_date",
            "publication_year",
            "scraped_at",
            "last_updated_at",
            "bronze_source_path",
            "pdf_count_declared",
            "pdf_types_found",
            "has_assets",
            "matched_subjects",
            "program_ids",
            "record_fingerprint",
            "data_quality_score",
            "ingested_at",
        )

    def _build_documents_df(self, base_df: DataFrame) -> DataFrame:
        docs_base = base_df.select(
            "resource_uid",
            "resource_id",
            "source_system",
            "source_url",
            "title",
            "language",
            "license_name",
            "license_url",
            "scraped_at",
            "ingested_at",
            F.posexplode_outer("pdf_paths").alias("asset_order", "asset_path"),
        ).filter(F.col("asset_path").isNotNull())

        docs_base = (
            docs_base
            .withColumn("asset_order", F.col("asset_order") + F.lit(1))
            .withColumn("file_name", F.element_at(F.split(F.col("asset_path"), "/"), -1))
            .withColumn("asset_extension", F.lower(F.regexp_extract(F.col("file_name"), r"\.([^.]+)$", 1)))
            .withColumn("asset_uid", F.sha2(F.concat_ws("||", F.col("resource_uid"), F.col("asset_path")), 256))
        )

        docs_schema = T.StructType(
            [
                T.StructField("asset_uid", T.StringType(), False),
                T.StructField("resource_uid", T.StringType(), False),
                T.StructField("resource_id", T.StringType(), True),
                T.StructField("source_system", T.StringType(), True),
                T.StructField("source_url", T.StringType(), True),
                T.StructField("title", T.StringType(), True),
                T.StructField("asset_order", T.IntegerType(), True),
                T.StructField("asset_path", T.StringType(), True),
                T.StructField("file_name", T.StringType(), True),
                T.StructField("asset_extension", T.StringType(), True),
                T.StructField("etag", T.StringType(), True),
                T.StructField("size_bytes", T.LongType(), True),
                T.StructField("mime_type", T.StringType(), True),
                T.StructField("last_modified", T.TimestampType(), True),
                T.StructField("language", T.StringType(), True),
                T.StructField("license_name", T.StringType(), True),
                T.StructField("license_url", T.StringType(), True),
                T.StructField("scraped_at", T.TimestampType(), True),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )

        bucket = self.bucket
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

        def enrich_partition(rows: Iterator[Any]) -> Iterator[Dict[str, Any]]:
            client = _create_partition_minio_client(
                endpoint=minio_endpoint,
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                secure=minio_secure,
            )
            for row in rows:
                yield _enrich_document_with_client(row.asDict(recursive=True), client, bucket)

        enriched_rdd = docs_base.rdd.mapPartitions(enrich_partition)
        documents_df = self.spark.createDataFrame(enriched_rdd, schema=docs_schema)
        return documents_df.dropDuplicates(["asset_uid"])

    def _empty_asset_uid_df(self) -> DataFrame:
        return self.spark.createDataFrame([], schema=T.StructType([T.StructField("asset_uid", T.StringType(), False)]))

    def _has_rows(self, df: DataFrame) -> bool:
        return len(df.take(1)) > 0

    def _filter_changed_documents(self, documents_df: DataFrame) -> DataFrame:
        if not self._has_rows(documents_df):
            return self._empty_asset_uid_df()
        if not self._table_exists(self.documents_table):
            return documents_df.select("asset_uid").dropDuplicates(["asset_uid"])

        existing_df = self.spark.table(self.documents_table).select(
            "asset_uid",
            F.col("etag").alias("existing_etag"),
            F.col("size_bytes").alias("existing_size_bytes"),
            F.col("last_modified").alias("existing_last_modified"),
        )
        changed_df = (
            documents_df.alias("n")
            .join(existing_df.alias("e"), on="asset_uid", how="left")
            .filter(
                F.col("e.asset_uid").isNull()
                | (F.coalesce(F.col("n.etag"), F.lit("")) != F.coalesce(F.col("e.existing_etag"), F.lit("")))
                | (F.coalesce(F.col("n.size_bytes"), F.lit(-1)) != F.coalesce(F.col("e.existing_size_bytes"), F.lit(-1)))
                | (
                    F.coalesce(F.col("n.last_modified").cast("string"), F.lit(""))
                    != F.coalesce(F.col("e.existing_last_modified").cast("string"), F.lit(""))
                )
            )
            .select("asset_uid")
            .dropDuplicates(["asset_uid"])
        )
        return changed_df

    def _find_deleted_assets(self, incremental_base_df: DataFrame, documents_df: DataFrame) -> DataFrame:
        if not self._table_exists(self.documents_table):
            return self._empty_asset_uid_df()

        changed_resources = incremental_base_df.select("resource_uid").dropDuplicates(["resource_uid"])
        existing_assets = (
            self.spark.table(self.documents_table)
            .select("resource_uid", "asset_uid")
            .join(changed_resources, on="resource_uid", how="inner")
            .select("asset_uid")
            .dropDuplicates(["asset_uid"])
        )
        current_assets = documents_df.select("asset_uid").dropDuplicates(["asset_uid"]) if self._has_rows(documents_df) else self._empty_asset_uid_df()
        return existing_assets.join(current_assets, on="asset_uid", how="left_anti")

    def _enrich_document_record(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return _enrich_document_with_client(row, self.minio_client, self.bucket)

    def _get_pdf_bytes(self, asset_path: str) -> Optional[bytes]:
        if not self.minio_client or not asset_path:
            return None
        response = None
        try:
            response = self.minio_client.get_object(self.bucket, asset_path)
            return response.read()
        except Exception:
            return None
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def _extract_pdf_page_texts(self, pdf_bytes: bytes) -> Tuple[Dict[int, str], int]:
        if not PYPDF_AVAILABLE:
            return {}, 0
        try:
            reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
            max_pages = min(len(reader.pages), self.max_pages_per_pdf)
            page_texts: Dict[int, str] = {}
            for idx in range(max_pages):
                raw = clean_scalar(reader.pages[idx].extract_text()) or ""
                page_texts[idx + 1] = self._normalize_pdf_text(raw)
            return page_texts, max_pages
        except Exception:
            return {}, 0

    def _extract_pdf_pages(self, asset_path: str) -> List[Tuple[int, int, str]]:
        pdf_bytes = self._get_pdf_bytes(asset_path)
        if not pdf_bytes:
            return []
        page_texts, _ = self._extract_pdf_page_texts(pdf_bytes)
        pages: List[Tuple[int, int, str]] = []
        for page_no in sorted(page_texts.keys()):
            page_text = page_texts[page_no]
            if not page_text:
                continue
            for chunk_index, chunk in enumerate(self._chunk_text_smart(page_text), start=1):
                if chunk:
                    pages.append((page_no, chunk_index, chunk))
        return pages

    def _normalize_pdf_text(self, text: str) -> str:
        text = re.sub(r"-\s*\n\s*", "", text)
        text = text.replace("\r", "\n")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _split_long_segment(
        self,
        text: str,
        max_chars: Optional[int] = None,
        min_chars: Optional[int] = None,
        overlap_chars: Optional[int] = None,
    ) -> List[str]:
        chunks: List[str] = []
        text = text.strip()
        if not text:
            return chunks

        max_chars = int(max_chars or self.chunk_max_chars)
        min_chars = int(min_chars or self.chunk_min_chars)
        overlap_chars = int(overlap_chars if overlap_chars is not None else self.chunk_overlap_chars)
        overlap = min(max(overlap_chars, 0), max(max_chars // 2, 0))
        step = max(1, max_chars - overlap)
        start = 0
        while start < len(text):
            end = min(start + max_chars, len(text))
            if end < len(text):
                window_start = min(end, start + max(min_chars, max_chars // 2))
                split_pos = max(
                    text.rfind("\n", window_start, end),
                    text.rfind(". ", window_start, end),
                    text.rfind("? ", window_start, end),
                    text.rfind("! ", window_start, end),
                )
                if split_pos > start:
                    end = split_pos + 1

            chunk = text[start:end].strip()
            if chunk and (len(chunk) >= min_chars or end == len(text)):
                chunks.append(chunk)
            if end >= len(text):
                break
            start = start + step if end <= start else max(start + 1, end - overlap)
        return chunks

    def _chunk_text_smart(
        self,
        text: str,
        max_chars: Optional[int] = None,
        min_chars: Optional[int] = None,
        overlap_chars: Optional[int] = None,
    ) -> List[str]:
        max_chars = int(max_chars or self.chunk_max_chars)
        min_chars = int(min_chars or self.chunk_min_chars)
        overlap_chars = int(overlap_chars if overlap_chars is not None else self.chunk_overlap_chars)
        paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p and p.strip()]
        if not paragraphs:
            return self._split_long_segment(text, max_chars=max_chars, min_chars=min_chars, overlap_chars=overlap_chars)

        chunks: List[str] = []
        current_parts: List[str] = []
        current_len = 0

        def flush_current() -> None:
            nonlocal current_parts, current_len
            if current_parts:
                chunk = "\n\n".join(current_parts).strip()
                if chunk:
                    chunks.append(chunk)
            current_parts = []
            current_len = 0

        for para in paragraphs:
            if len(para) > max_chars:
                flush_current()
                chunks.extend(self._split_long_segment(para, max_chars=max_chars, min_chars=min_chars, overlap_chars=overlap_chars))
                continue

            projected = current_len + (2 if current_parts else 0) + len(para)
            if projected <= max_chars:
                current_parts.append(para)
                current_len = projected
            else:
                flush_current()
                current_parts.append(para)
                current_len = len(para)

        flush_current()
        return chunks if chunks else self._split_long_segment(text, max_chars=max_chars, min_chars=min_chars, overlap_chars=overlap_chars)

    def _build_chunk_schema(self) -> T.StructType:
        chunk_schema = T.StructType(
            [
                T.StructField("chunk_id", T.StringType(), False),
                T.StructField("resource_uid", T.StringType(), False),
                T.StructField("asset_uid", T.StringType(), False),
                T.StructField("page_no", T.IntegerType(), False),
                T.StructField("chunk_order", T.IntegerType(), False),
                T.StructField("chunk_text", T.StringType(), True),
                T.StructField("token_count", T.IntegerType(), True),
                T.StructField("lang", T.StringType(), True),
                T.StructField("chunk_type", T.StringType(), True),
                T.StructField("chunk_tier", T.IntegerType(), True),
                T.StructField("chapter_id", T.StringType(), True),
                T.StructField("chapter_title", T.StringType(), True),
                T.StructField("chapter_number", T.IntegerType(), True),
                T.StructField("chapter_page_start", T.IntegerType(), True),
                T.StructField("chapter_page_end", T.IntegerType(), True),
                T.StructField("section_id", T.StringType(), True),
                T.StructField("section_title", T.StringType(), True),
                T.StructField("section_number", T.StringType(), True),
                T.StructField("section_page_start", T.IntegerType(), True),
                T.StructField("section_page_end", T.IntegerType(), True),
                T.StructField("parent_chunk_id", T.StringType(), True),
                T.StructField("has_children", T.BooleanType(), True),
                T.StructField("is_summary", T.BooleanType(), True),
                T.StructField("summary_method", T.StringType(), True),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        return chunk_schema

    def _build_structure_schema(self) -> T.StructType:
        return T.StructType(
            [
                T.StructField("structure_id", T.StringType(), False),
                T.StructField("asset_uid", T.StringType(), False),
                T.StructField("resource_uid", T.StringType(), False),
                T.StructField("source_system", T.StringType(), True),
                T.StructField("has_toc", T.BooleanType(), True),
                T.StructField("toc_extracted_at", T.TimestampType(), True),
                T.StructField("toc_method", T.StringType(), True),
                T.StructField("toc_confidence", T.DoubleType(), True),
                T.StructField("total_pages", T.IntegerType(), True),
                T.StructField("total_chapters", T.IntegerType(), True),
                T.StructField("total_sections", T.IntegerType(), True),
                T.StructField("table_of_contents_json", T.StringType(), True),
                T.StructField("structure_valid", T.BooleanType(), True),
                T.StructField("created_at", T.TimestampType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )

    def _build_chunks_and_structure_df(self, documents_df: DataFrame) -> Tuple[DataFrame, DataFrame, Any]:
        chunk_schema = self._build_chunk_schema()
        structure_schema = self._build_structure_schema()
        enable_hierarchical = bool(self.enable_hierarchical and HIERARCHICAL_AVAILABLE)
        bucket = self.bucket

        minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

        max_pages_per_pdf = int(self.max_pages_per_pdf)
        chunk_max_chars = int(self.chunk_max_chars)
        chunk_min_chars = int(self.chunk_min_chars)
        chunk_overlap_chars = int(self.chunk_overlap_chars)
        section_chunk_max_chars = int(self.section_chunk_max_chars)
        toc_fallback_chapter_size = int(self.toc_fallback_chapter_size)
        toc_min_confidence = float(self.toc_min_confidence)
        toc_enabled = bool(self.toc_enabled)
        doc_summary_max_chars = int(self.doc_summary_max_chars)
        chapter_summary_max_chars = int(self.chapter_summary_max_chars)

        def chunk_partition(rows: Iterator[Any]) -> Iterator[Tuple[str, Dict[str, Any]]]:
            client = _create_partition_minio_client(
                endpoint=minio_endpoint,
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                secure=minio_secure,
            )
            toc_extractor = TOCExtractor(fallback_chapter_size=toc_fallback_chapter_size) if enable_hierarchical else None
            summarizer = MultilingualExtractiveSummarizer() if enable_hierarchical else None
            worker = _PartitionChunker(
                minio_client=client,
                bucket=bucket,
                max_pages_per_pdf=max_pages_per_pdf,
                chunk_max_chars=chunk_max_chars,
                chunk_min_chars=chunk_min_chars,
                chunk_overlap_chars=chunk_overlap_chars,
                section_chunk_max_chars=section_chunk_max_chars,
                toc_fallback_chapter_size=toc_fallback_chapter_size,
                toc_min_confidence=toc_min_confidence,
                toc_enabled=toc_enabled,
                doc_summary_max_chars=doc_summary_max_chars,
                chapter_summary_max_chars=chapter_summary_max_chars,
                toc_extractor=toc_extractor,
                summarizer=summarizer,
            )

            hierarchical_ready = bool(enable_hierarchical and toc_extractor and summarizer)
            for row in rows:
                record = row.asDict(recursive=True)
                if hierarchical_ready:
                    chunks, structure_record = worker.chunk_document_hierarchical(record)
                    for chunk in chunks:
                        yield ("chunk", chunk)
                    if structure_record:
                        yield ("structure", structure_record)
                else:
                    for chunk in worker.chunk_document_record(record):
                        yield ("chunk", chunk)

        tagged_rdd = documents_df.rdd.mapPartitions(chunk_partition).persist(StorageLevel.MEMORY_AND_DISK)
        chunk_rdd = tagged_rdd.filter(lambda item: item[0] == "chunk").map(lambda item: item[1])
        structure_rdd = tagged_rdd.filter(lambda item: item[0] == "structure").map(lambda item: item[1])

        chunks_df = self.spark.createDataFrame(chunk_rdd, schema=chunk_schema).dropDuplicates(["chunk_id"])
        structures_df = self.spark.createDataFrame(structure_rdd, schema=structure_schema).dropDuplicates(["structure_id"])
        return chunks_df, structures_df, tagged_rdd

    def _chunk_document_record(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        asset_path = clean_scalar(row.get("asset_path")) or ""
        if not asset_path.lower().endswith(".pdf"):
            return []
        resource_uid = clean_scalar(row.get("resource_uid"))
        asset_uid = clean_scalar(row.get("asset_uid"))
        if not resource_uid or not asset_uid:
            return []

        records: List[Dict[str, Any]] = []
        now = datetime.utcnow()
        for page_no, chunk_order, chunk_text in self._extract_pdf_pages(asset_path):
            token_count = len(re.findall(r"\w+", chunk_text))
            chunk_id = deterministic_hash(f"{asset_uid}::{page_no}::{chunk_order}::{chunk_text[:128]}")
            records.append(
                {
                    "chunk_id": chunk_id,
                    "resource_uid": resource_uid,
                    "asset_uid": asset_uid,
                    "page_no": int(page_no),
                    "chunk_order": int(chunk_order),
                    "chunk_text": chunk_text,
                    "token_count": int(token_count),
                    "lang": ensure_language_code(row.get("language")),
                    "chunk_type": "section_detail",
                    "chunk_tier": 3,
                    "chapter_id": None,
                    "chapter_title": None,
                    "chapter_number": None,
                    "chapter_page_start": None,
                    "chapter_page_end": None,
                    "section_id": None,
                    "section_title": None,
                    "section_number": None,
                    "section_page_start": int(page_no),
                    "section_page_end": int(page_no),
                    "parent_chunk_id": None,
                    "has_children": False,
                    "is_summary": False,
                    "summary_method": None,
                    "updated_at": now,
                }
            )
        return records

    def _build_flat_toc(self, total_pages: int) -> List[Dict[str, Any]]:
        chapter_size = max(10, self.toc_fallback_chapter_size)
        toc: List[Dict[str, Any]] = []
        chapter_num = 0
        for start_page in range(1, total_pages + 1, chapter_size):
            chapter_num += 1
            end_page = min(start_page + chapter_size - 1, total_pages)
            toc.append(
                {
                    "chapter_id": f"ch{chapter_num:02d}",
                    "chapter_number": chapter_num,
                    "chapter_title": f"Part {chapter_num}",
                    "page_start": start_page,
                    "page_end": end_page,
                    "sections": [],
                }
            )
        return toc

    def _chunk_document_hierarchical(self, row: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        asset_path = clean_scalar(row.get("asset_path")) or ""
        if not asset_path.lower().endswith(".pdf"):
            return [], None
        resource_uid = clean_scalar(row.get("resource_uid"))
        asset_uid = clean_scalar(row.get("asset_uid"))
        if not resource_uid or not asset_uid:
            return [], None

        pdf_bytes = self._get_pdf_bytes(asset_path)
        if not pdf_bytes:
            return self._chunk_document_record(row), None

        page_texts, total_pages = self._extract_pdf_page_texts(pdf_bytes)
        if total_pages <= 0:
            return self._chunk_document_record(row), None

        toc_result = {"method": "flat", "confidence": 0.5, "toc": [], "total_pages": total_pages, "structure_valid": False}
        if self.toc_enabled and self.toc_extractor:
            toc_result = self.toc_extractor.extract(pdf_bytes, max_pages=total_pages)

        toc = toc_result.get("toc") or []
        confidence = float(toc_result.get("confidence") or 0.0)
        method = str(toc_result.get("method") or "flat")
        structure_valid = bool(toc_result.get("structure_valid"))

        # Keep tier navigation stable even when TOC confidence is low.
        if not toc:
            toc = self._build_flat_toc(total_pages)
            method = "flat"
            confidence = 0.5

        if confidence < self.toc_min_confidence:
            method = "flat"
            toc = self._build_flat_toc(total_pages)

        now = datetime.utcnow()
        lang = ensure_language_code(row.get("language"))
        chunk_records: List[Dict[str, Any]] = []
        section_global_order = 0

        # Tier 1: Document summary
        doc_summary = ""
        if self.summarizer:
            doc_summary = self.summarizer.generate_document_summary(row, toc, max_chars=self.doc_summary_max_chars)
        if doc_summary:
            doc_chunk_id = deterministic_hash(f"{asset_uid}::tier1::doc_summary")
            chunk_records.append(
                {
                    "chunk_id": doc_chunk_id,
                    "resource_uid": resource_uid,
                    "asset_uid": asset_uid,
                    "page_no": 1,
                    "chunk_order": 1,
                    "chunk_text": doc_summary,
                    "token_count": int(len(re.findall(r"\w+", doc_summary))),
                    "lang": lang,
                    "chunk_type": "doc_summary",
                    "chunk_tier": 1,
                    "chapter_id": None,
                    "chapter_title": None,
                    "chapter_number": None,
                    "chapter_page_start": None,
                    "chapter_page_end": None,
                    "section_id": None,
                    "section_title": None,
                    "section_number": None,
                    "section_page_start": None,
                    "section_page_end": None,
                    "parent_chunk_id": None,
                    "has_children": True,
                    "is_summary": True,
                    "summary_method": "extractive",
                    "updated_at": now,
                }
            )

        for chapter_idx, chapter in enumerate(toc, start=1):
            chapter_id = clean_scalar(chapter.get("chapter_id")) or f"ch{chapter_idx:02d}"
            chapter_title = clean_scalar(chapter.get("chapter_title")) or f"Chapter {chapter_idx}"
            chapter_number = int(chapter.get("chapter_number") or chapter_idx)
            chapter_start = max(1, min(int(chapter.get("page_start") or 1), total_pages))
            chapter_end = max(chapter_start, min(int(chapter.get("page_end") or chapter_start), total_pages))

            chapter_text = "\n\n".join(
                [page_texts.get(page_no, "") for page_no in range(chapter_start, chapter_end + 1) if page_texts.get(page_no, "").strip()]
            ).strip()
            if not chapter_text:
                continue

            chapter_summary = chapter_title
            if self.summarizer:
                chapter_summary = self.summarizer.generate_chapter_summary(
                    chapter_text,
                    chapter_title,
                    max_chars=self.chapter_summary_max_chars,
                )
            chapter_chunk_id = deterministic_hash(f"{asset_uid}::tier2::{chapter_id}")
            chunk_records.append(
                {
                    "chunk_id": chapter_chunk_id,
                    "resource_uid": resource_uid,
                    "asset_uid": asset_uid,
                    "page_no": chapter_start,
                    "chunk_order": chapter_idx,
                    "chunk_text": chapter_summary,
                    "token_count": int(len(re.findall(r"\w+", chapter_summary))),
                    "lang": lang,
                    "chunk_type": "chapter_summary",
                    "chunk_tier": 2,
                    "chapter_id": chapter_id,
                    "chapter_title": chapter_title,
                    "chapter_number": chapter_number,
                    "chapter_page_start": chapter_start,
                    "chapter_page_end": chapter_end,
                    "section_id": None,
                    "section_title": None,
                    "section_number": None,
                    "section_page_start": None,
                    "section_page_end": None,
                    "parent_chunk_id": None,
                    "has_children": True,
                    "is_summary": True,
                    "summary_method": "extractive",
                    "updated_at": now,
                }
            )

            sections = chapter.get("sections") or []
            if not sections:
                sections = [
                    {
                        "section_id": f"{chapter_id}_sec01",
                        "section_number": f"{chapter_number}.1",
                        "section_title": chapter_title,
                        "page_start": chapter_start,
                        "page_end": chapter_end,
                    }
                ]

            for section_idx, section in enumerate(sections, start=1):
                section_id = clean_scalar(section.get("section_id")) or f"{chapter_id}_sec{section_idx:02d}"
                section_number = clean_scalar(section.get("section_number")) or f"{chapter_number}.{section_idx}"
                section_title = clean_scalar(section.get("section_title")) or chapter_title
                section_start = max(chapter_start, min(int(section.get("page_start") or chapter_start), chapter_end))
                section_end = max(section_start, min(int(section.get("page_end") or chapter_end), chapter_end))

                section_text = "\n\n".join(
                    [page_texts.get(page_no, "") for page_no in range(section_start, section_end + 1) if page_texts.get(page_no, "").strip()]
                ).strip()
                if not section_text:
                    continue

                detail_chunks = self._chunk_text_smart(
                    section_text,
                    max_chars=self.section_chunk_max_chars,
                    min_chars=max(self.chunk_min_chars, 220),
                    overlap_chars=self.chunk_overlap_chars,
                )

                for local_idx, detail in enumerate(detail_chunks, start=1):
                    if not detail:
                        continue
                    section_global_order += 1
                    detail_id = deterministic_hash(f"{asset_uid}::tier3::{section_id}::{local_idx}::{detail[:128]}")
                    chunk_records.append(
                        {
                            "chunk_id": detail_id,
                            "resource_uid": resource_uid,
                            "asset_uid": asset_uid,
                            "page_no": section_start,
                            "chunk_order": section_global_order,
                            "chunk_text": detail,
                            "token_count": int(len(re.findall(r"\w+", detail))),
                            "lang": lang,
                            "chunk_type": "section_detail",
                            "chunk_tier": 3,
                            "chapter_id": chapter_id,
                            "chapter_title": chapter_title,
                            "chapter_number": chapter_number,
                            "chapter_page_start": chapter_start,
                            "chapter_page_end": chapter_end,
                            "section_id": section_id,
                            "section_title": section_title,
                            "section_number": section_number,
                            "section_page_start": section_start,
                            "section_page_end": section_end,
                            "parent_chunk_id": chapter_chunk_id,
                            "has_children": False,
                            "is_summary": False,
                            "summary_method": None,
                            "updated_at": now,
                        }
                    )

        structure_record = {
            "structure_id": deterministic_hash(asset_uid),
            "asset_uid": asset_uid,
            "resource_uid": resource_uid,
            "source_system": clean_scalar(row.get("source_system")),
            "has_toc": method != "flat",
            "toc_extracted_at": now,
            "toc_method": method,
            "toc_confidence": confidence,
            "total_pages": total_pages,
            "total_chapters": len(toc),
            "total_sections": int(sum(len(ch.get("sections") or []) for ch in toc)),
            "table_of_contents_json": json.dumps(toc, ensure_ascii=False),
            "structure_valid": structure_valid,
            "created_at": now,
            "updated_at": now,
        }
        return chunk_records, structure_record

    # -----------------------------
    # Iceberg write helpers
    # -----------------------------
    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.table(table_name)
            return True
        except Exception:
            return False

    def _ensure_columns_exist(self, table_name: str, df: DataFrame) -> None:
        existing_cols = {c.lower(): t for c, t in self.spark.table(table_name).dtypes}
        for col_name, col_type in df.dtypes:
            if col_name.lower() not in existing_cols:
                self.spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")

    def _merge_into(self, df: DataFrame, table_name: str, merge_keys: List[str], partition_columns: Optional[List[str]] = None) -> None:
        if not self._table_exists(table_name):
            temp_view = f"tmp_{uuid4().hex}"
            df.createOrReplaceTempView(temp_view)
            partition_clause = ""
            if partition_columns:
                partition_clause = f"PARTITIONED BY ({', '.join(partition_columns)})"
            self.spark.sql(
                f"""
                CREATE TABLE {table_name}
                USING iceberg
                {partition_clause}
                AS SELECT * FROM {temp_view}
                """
            )
            self.spark.catalog.dropTempView(temp_view)
            return

        self._ensure_columns_exist(table_name, df)
        temp_view = f"tmp_{uuid4().hex}"
        df.createOrReplaceTempView(temp_view)
        on_clause = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        self.spark.sql(
            f"""
            MERGE INTO {table_name} t
            USING {temp_view} s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        self.spark.catalog.dropTempView(temp_view)

    def _delete_by_asset_uid(self, table_name: str, asset_uid_df: DataFrame) -> None:
        if not self._table_exists(table_name):
            return
        delete_keys_df = (
            asset_uid_df
            .select("asset_uid")
            .where(F.col("asset_uid").isNotNull())
            .dropDuplicates(["asset_uid"])
        )
        if not self._has_rows(delete_keys_df):
            return

        temp_view = f"tmp_delete_{uuid4().hex}"
        delete_keys_df.createOrReplaceTempView(temp_view)
        try:
            self.spark.sql(
                f"""
                MERGE INTO {table_name} t
                USING {temp_view} d
                ON t.asset_uid = d.asset_uid
                WHEN MATCHED THEN DELETE
                """
            )
        finally:
            self.spark.catalog.dropTempView(temp_view)

    # -----------------------------
    # Orchestration
    # -----------------------------
    def run_reference_bootstrap_only(self) -> None:
        self.run_reference_bootstrap()
        print("Reference bootstrap completed.")

    def run(self) -> None:
        benchmark = StageBenchmarkLogger(run_id=uuid4().hex[:12])
        if self.run_reference_bootstrap_enabled:
            self.run_reference_bootstrap()

        print(f"Reading bronze input from: {self.bronze_input}")
        bronze_df = self._read_bronze().persist(StorageLevel.MEMORY_AND_DISK)
        bronze_count = int(bronze_df.count())
        if bronze_count == 0:
            print("No bronze records found; exiting.")
            bronze_df.unpersist(False)
            return

        normalize_started = benchmark.start()
        print("Building normalized silver base dataset...")
        base_df = self._build_base_df(bronze_df).persist(StorageLevel.MEMORY_AND_DISK)
        base_count = int(base_df.count())
        benchmark.finish(
            stage="normalize",
            started_at=normalize_started,
            input_records=bronze_count,
            output_records=base_count,
        )
        bronze_df.unpersist(False)

        print("Applying incremental resource filter...")
        incremental_base_df = self._filter_incremental_resources(base_df).persist()
        incremental_count = int(incremental_base_df.count())
        base_df.unpersist(False)
        if incremental_count == 0:
            print("No new or updated resources after incremental filter; exiting.")
            incremental_base_df.unpersist(False)
            return

        document_started = benchmark.start()
        print("Building silver resources_curated...")
        resources_curated_df = self._build_resources_curated_df(incremental_base_df).persist()
        print("Building silver documents...")
        documents_df = self._build_documents_df(incremental_base_df).persist()
        documents_count = int(documents_df.count())
        benchmark.finish(
            stage="document",
            started_at=document_started,
            input_records=incremental_count,
            output_records=documents_count,
        )
        print("Detecting changed/deleted assets for incremental chunking...")
        changed_assets_df = self._filter_changed_documents(documents_df).persist()
        deleted_assets_df = self._find_deleted_assets(incremental_base_df, documents_df).persist()
        affected_assets_df = changed_assets_df.unionByName(deleted_assets_df).dropDuplicates(["asset_uid"]).persist()
        changed_assets_count = int(changed_assets_df.count())
        has_changed_assets = changed_assets_count > 0

        changed_documents_df = (
            documents_df.join(changed_assets_df, on="asset_uid", how="inner").persist()
            if has_changed_assets
            else documents_df.limit(0).persist()
        )
        changed_documents_count = int(changed_documents_df.count())
        chunk_started = benchmark.start()
        print("Building silver chunks...")
        chunks_df, structure_df, chunk_tagged_rdd = self._build_chunks_and_structure_df(changed_documents_df)
        chunks_df = chunks_df.persist(StorageLevel.MEMORY_AND_DISK)
        structure_df = structure_df.persist(StorageLevel.MEMORY_AND_DISK)
        chunk_count = int(chunks_df.count())
        structure_count = int(structure_df.count())
        benchmark.finish(
            stage="chunk",
            started_at=chunk_started,
            input_records=changed_documents_count,
            output_records=chunk_count,
            extra={"structure_records": structure_count},
        )
        has_chunk_rows = chunk_count > 0
        has_structure_rows = structure_count > 0

        print("Writing Iceberg tables...")
        self._merge_into(resources_curated_df, self.resources_curated_table, ["resource_uid"], partition_columns=["source_system", "days(ingested_at)"])
        # Keep legacy table updated for current Gold compatibility.
        self._merge_into(resources_curated_df, self.resources_table, ["resource_uid"], partition_columns=["source_system", "days(ingested_at)"])
        self._delete_by_asset_uid(self.chunks_table, affected_assets_df)
        self._delete_by_asset_uid(self.document_structure_table, affected_assets_df)
        self._delete_by_asset_uid(self.documents_table, deleted_assets_df)
        self._merge_into(documents_df, self.documents_table, ["asset_uid"], partition_columns=["source_system", "days(updated_at)"])
        if has_chunk_rows:
            self._merge_into(chunks_df, self.chunks_table, ["chunk_id"], partition_columns=["chunk_tier", "days(updated_at)"])
        if has_structure_rows:
            self._merge_into(structure_df, self.document_structure_table, ["structure_id"], partition_columns=["source_system", "days(updated_at)"])

        chunk_tagged_rdd.unpersist(False)
        for df in [structure_df, chunks_df, changed_documents_df, affected_assets_df, deleted_assets_df, changed_assets_df, documents_df, resources_curated_df, incremental_base_df]:
            df.unpersist(False)

        print("Silver transformation completed successfully.")


def main() -> None:
    mode = os.getenv("SILVER_MODE", "transform").strip().lower()
    transformer = SilverTransformer()
    if mode == "reference_bootstrap":
        transformer.run_reference_bootstrap_only()
    else:
        transformer.run()


if __name__ == "__main__":
    main()
