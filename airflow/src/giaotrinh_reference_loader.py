#!/usr/bin/env python3
"""
Giaotrinh SQL Reference Loader
==============================

Parse giaotrinh.sql (MySQL dump) without requiring a MySQL server and expose
structured datasets for faculties, departments, subjects, programs, textbook
catalogue, and Dewey classifications.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False


try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.messages import HumanMessage
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False


@dataclass(frozen=True)
class Faculty:
    faculty_id: int
    faculty_name: str


@dataclass(frozen=True)
class Department:
    department_id: int
    department_name: str
    faculty_id: Optional[int]


@dataclass(frozen=True)
class Subject:
    subject_id: int
    subject_name: str
    subject_name_en: Optional[str]  # English translation
    subject_code: Optional[str]
    faculty_id: Optional[int]
    department_id: Optional[int]
    subject_type_id: Optional[int]
    status_code: Optional[str]


@dataclass(frozen=True)
class Program:
    program_id: int
    program_name: str
    program_code: Optional[str]
    published_year: Optional[str]
    faculty_id: Optional[int]


@dataclass(frozen=True)
class ProgramSubjectLink:
    subject_id: int
    program_id: int


@dataclass(frozen=True)
class Textbook:
    textbook_id: int
    title: Optional[str]
    metadata: Optional[str]
    publisher: Optional[str]
    publish_year: Optional[str]
    textbook_type_id: Optional[int]
    quantity: Optional[int]
    source_id: Optional[int]


@dataclass(frozen=True)
class DeweyClass:
    code: str
    meaning: str


class SqlInsertParser:
    """Parse INSERT INTO blocks without being confused by semicolons inside strings."""

    HEADER_PATTERN = re.compile(r"INSERT INTO\s+`(?P<table>\w+)`.*?VALUES", re.IGNORECASE | re.S)

    def __init__(self, sql_text: str) -> None:
        self.sql_text = sql_text
        self._cache: Dict[str, List[Tuple]] = {}
        self._blocks = list(self._iterate_blocks())

    def _iterate_blocks(self) -> Iterator[Tuple[str, str]]:
        idx = 0
        length = len(self.sql_text)
        while idx < length:
            header_match = self.HEADER_PATTERN.search(self.sql_text, idx)
            if not header_match:
                break
            table = header_match.group("table")
            block_start = header_match.end()
            i = block_start
            depth = 0
            in_string = False
            escape = False
            while i < length:
                ch = self.sql_text[i]
                if in_string:
                    if escape:
                        escape = False
                    elif ch == "\\":
                        escape = True
                    elif ch == "'":
                        if i + 1 < length and self.sql_text[i + 1] == "'":
                            i += 1  # escaped quote via double single-quote
                        else:
                            in_string = False
                    i += 1
                    continue

                if ch == "'":
                    in_string = True
                elif ch == "(":
                    depth += 1
                elif ch == ")":
                    if depth > 0:
                        depth -= 1
                elif ch == ";" and depth == 0:
                    block = self.sql_text[block_start:i]
                    yield table, block
                    idx = i + 1
                    break
                i += 1
            else:
                raise ValueError(f"INSERT statement for table {table} missing terminating semicolon")

    def get_rows(self, table: str) -> List[Tuple]:
        if table in self._cache:
            return self._cache[table]

        rows: List[Tuple] = []
        for tbl, block in self._blocks:
            if tbl != table:
                continue
            rows.extend(list(self._parse_values_block(block)))
        self._cache[table] = rows
        return rows

    def _parse_values_block(self, block: str) -> Iterator[Tuple]:
        idx = 0
        length = len(block)
        while idx < length:
            ch = block[idx]
            if ch == "(":
                idx += 1
                record, idx = self._parse_record(block, idx)
                yield tuple(record)
            else:
                idx += 1

    def _parse_record(self, text: str, start_idx: int) -> Tuple[List[Optional[str]], int]:
        values: List[Optional[str]] = []
        current = ""
        raw_type: Optional[str] = None
        idx = start_idx
        length = len(text)
        in_string = False
        escape = False

        while idx < length:
            ch = text[idx]
            if in_string:
                if escape:
                    current += ch
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == "'":
                    if idx + 1 < length and text[idx + 1] == "'":
                        current += "'"
                        idx += 1
                    else:
                        in_string = False
                else:
                    current += ch
                idx += 1
                continue

            if ch == "'":
                in_string = True
                raw_type = "string"
                current = ""
            elif ch == ",":
                values.append(self._convert_value(current, raw_type))
                current = ""
                raw_type = None
            elif ch == ")":
                values.append(self._convert_value(current, raw_type))
                return values, idx + 1
            elif ch in " \n\r\t":
                pass
            else:
                if raw_type is None:
                    raw_type = "bare"
                current += ch
            idx += 1

        snippet = text[start_idx:start_idx + 120].replace("\n", " ")
        raise ValueError(f"Unterminated record near: {snippet}")

    def _convert_value(self, raw: str, raw_type: Optional[str]):
        if raw_type == "string":
            return raw
        token = (raw or "").strip()
        if not token or token.upper() == "NULL":
            return None
        if token.isdigit() or (token.startswith("-") and token[1:].isdigit()):
            try:
                return int(token)
            except ValueError:
                pass
        try:
            return float(token)
        except ValueError:
            return token


class GiaotrinhReferenceExtractor:
    """Expose structured data from giaotrinh.sql using the manual INSERT parser."""
    
    # Class variable for translation cache file
    TRANSLATION_CACHE_FILE = None

    def __init__(self, sql_path: Path, cache_file: Optional[Path] = None) -> None:
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file does not exist: {sql_path}")
        sql_text = sql_path.read_text(encoding="utf-8", errors="ignore")
        self.parser = SqlInsertParser(sql_text)
        
        # Set up translation cache
        self.cache_file = cache_file or Path(sql_path.parent) / "subjects_translation_cache.json"
        self.translation_cache: Dict[str, str] = {}
        self._load_translation_cache()
        
        # Initialize translator if available (prefer deep_translator for speed)
        self.translator = None
        
        if LANGCHAIN_AVAILABLE:
            try:
                api_key = os.getenv("GOOGLE_API_KEY")
                if not api_key:
                    print("GOOGLE_API_KEY not set, translation disabled")
                else:
                    self.translator = ChatGoogleGenerativeAI(
                        model="gemini-2.5-flash-lite",
                        google_api_key=api_key
                    )
                    print("LangChain + Gemini API initialized (with caching)")
            except Exception as exc:
                print(f"Translation disabled: {exc}")

    def _load_translation_cache(self) -> None:
        """Load existing translations from cache file."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.translation_cache = json.load(f)
                print(f"✓ Loaded {len(self.translation_cache)} cached translations")
            except Exception as exc:
                print(f"⚠ Failed to load cache: {exc}")

    def _save_translation_cache(self) -> None:
        """Save translations to cache file."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.translation_cache, f, ensure_ascii=False, indent=2)
            print(f"✓ Saved {len(self.translation_cache)} translations to cache")
        except Exception as exc:
            print(f"⚠ Failed to save cache: {exc}")

    def _translate_batch(self, texts: List[str]) -> Dict[str, str]:
        """Translate multiple texts in one API call for efficiency."""
        results = {}
        to_translate = []
        
        # Filter out already cached items
        for text in texts:
            text = text.strip()
            if not text:
                continue
            if text in self.translation_cache:
                cached = self.translation_cache[text]
                # Validate cached value is string, not coroutine
                if isinstance(cached, str):
                    results[text] = cached
                else:
                    print(f"  ⚠ Invalid cache for '{text}': {type(cached).__name__}")
                    to_translate.append(text)
            elif text not in to_translate:
                to_translate.append(text)
        
        if not to_translate or not self.translator:
            return results
        
        print(f"  Translating {len(to_translate)} new subjects...")
        
        # Batch in chunks of 20 to avoid token limit
        batch_size = 500
        for i in range(0, len(to_translate), batch_size):
            batch = to_translate[i:i+batch_size]
            try:
                batch_text = "\n".join([f"{j+1}. {t}" for j, t in enumerate(batch)])
                prompt = f"""Translate these Vietnamese texts to English. Output ONLY the translations numbered 1-{len(batch)}, one per line:
{batch_text}"""
                
                response = self.translator.invoke([HumanMessage(content=prompt)])
                
                # Validate response is an AIMessage
                if not hasattr(response, 'content'):
                    raise ValueError(f"Response is {type(response).__name__}, not AIMessage")
                
                response_text = str(response.content).strip()
                
                if not response_text:
                    raise ValueError("Empty response content")
                
                translated_lines = response_text.split("\n")
                
                # Parse numbered output
                for idx, original in enumerate(batch):
                    if idx < len(translated_lines):
                        line = translated_lines[idx].strip()
                        # Remove number prefix if present (e.g., "1. Translation" -> "Translation")
                        if line and line[0].isdigit():
                            line = line.split(".", 1)[1].strip() if "." in line else line
                        
                        if line and isinstance(line, str):
                            self.translation_cache[original] = line
                            results[original] = line
                            print(f"    ✓ {original} → {line}")
            except Exception as exc:
                print(f"  ⚠ Batch {i//batch_size + 1} failed: {str(exc)[:100]}")
        
        return results
    
    def faculties(self) -> List[Faculty]:
        rows = self.parser.get_rows("khoa")
        return [Faculty(int(row[0]), row[1] or "") for row in rows]

    def departments(self) -> List[Department]:
        rows = self.parser.get_rows("bomon")
        return [
            Department(
                department_id=int(row[0]),
                department_name=row[1] or "",
                faculty_id=int(row[2]) if row[2] is not None else None,
            )
            for row in rows
        ]

    def subjects(self) -> List[Subject]:
        rows = self.parser.get_rows("mon")
        
        # Extract all subject names for batch translation
        subject_names = [row[1] or "" for row in rows if row[1]]
        
        # Batch translate all at once
        if subject_names and self.translator:
            print(f"Batch translating {len(subject_names)} subjects...")
            translations = self._translate_batch(subject_names)
        else:
            translations = {}
        
        # Build results with translations
        result: List[Subject] = []
        for row in rows:
            subject_id, name, code, _, faculty_id, department_id, subject_type, status = row
            subject_name = name or ""
            subject_name_en = translations.get(subject_name) if subject_name else None
            
            result.append(
                Subject(
                    subject_id=int(subject_id),
                    subject_name=subject_name,
                    subject_name_en=subject_name_en,
                    subject_code=code,
                    faculty_id=int(faculty_id) if faculty_id is not None else None,
                    department_id=int(department_id) if department_id is not None else None,
                    subject_type_id=int(subject_type) if subject_type is not None else None,
                    status_code=status,
                )
            )
        
        # Save updated cache after processing all subjects
        self._save_translation_cache()
        return result

    def programs(self) -> List[Program]:
        rows = self.parser.get_rows("chuongtrinhdaotao")
        return [
            Program(
                program_id=int(row[0]),
                program_name=row[1] or "",
                program_code=row[2],
                published_year=row[3],
                faculty_id=int(row[4]) if row[4] is not None else None,
            )
            for row in rows
        ]

    def program_subject_links(self) -> List[ProgramSubjectLink]:
        rows = self.parser.get_rows("mon_ctdt")
        return [
            ProgramSubjectLink(subject_id=int(row[0]), program_id=int(row[1]))
            for row in rows
        ]

    def textbooks(self) -> List[Textbook]:
        rows = self.parser.get_rows("giaotrinh")
        textbooks: List[Textbook] = []
        for row in rows:
            (
                textbook_id,
                title,
                info,
                publisher,
                year,
                type_id,
                quantity,
                source_id,
                *_,
            ) = row + (None,) * (12 - len(row))
            textbooks.append(
                Textbook(
                    textbook_id=int(textbook_id),
                    title=title,
                    metadata=info,
                    publisher=publisher,
                    publish_year=year,
                    textbook_type_id=int(type_id) if type_id is not None else None,
                    quantity=int(quantity) if quantity is not None else None,
                    source_id=int(source_id) if source_id is not None else None,
                )
            )
        return textbooks

    def dewey_classes(self) -> List[DeweyClass]:
        rows = self.parser.get_rows("ddc")
        return [DeweyClass(code=row[1], meaning=row[2]) for row in rows]


def export_json(records: Iterable, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump([asdict(record) for record in records], fh, ensure_ascii=False, indent=2)


def upload_files_to_minio(file_paths: List[Path], prefix: str) -> None:
    if not MINIO_AVAILABLE:
        raise RuntimeError("MinIO library not available; cannot upload reference datasets")

    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure = os.getenv("MINIO_SECURE", "0").lower() in {"1", "true", "yes"}
    bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    normalized_prefix = prefix.strip().lstrip("/")
    for file_path in file_paths:
        object_name = f"{normalized_prefix}/{file_path.name}" if normalized_prefix else file_path.name
        client.fput_object(
            bucket,
            object_name,
            str(file_path),
            content_type="application/json; charset=utf-8",
        )
        print(f"Uploaded {file_path.name} to s3://{bucket}/{object_name}")


def main(sql_path: str, output_dir: str, upload_to_minio: bool = False, minio_prefix: Optional[str] = None) -> None:
    extractor = GiaotrinhReferenceExtractor(Path(sql_path))
    out_dir = Path(output_dir)

    generated_files: List[Path] = []
    export_json(extractor.faculties(), out_dir / "faculties.json"); generated_files.append(out_dir / "faculties.json")
    export_json(extractor.departments(), out_dir / "departments.json"); generated_files.append(out_dir / "departments.json")
    export_json(extractor.subjects(), out_dir / "subjects.json"); generated_files.append(out_dir / "subjects.json")
    export_json(extractor.programs(), out_dir / "programs.json"); generated_files.append(out_dir / "programs.json")
    export_json(extractor.program_subject_links(), out_dir / "program_subject_links.json"); generated_files.append(out_dir / "program_subject_links.json")
    export_json(extractor.textbooks(), out_dir / "textbooks.json"); generated_files.append(out_dir / "textbooks.json")
    export_json(extractor.dewey_classes(), out_dir / "dewey_classes.json"); generated_files.append(out_dir / "dewey_classes.json")

    if upload_to_minio:
        if not minio_prefix:
            minio_prefix = "bronze/reference/giaotrinh"
        upload_files_to_minio(generated_files, minio_prefix)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract giaotrinh.sql into JSON reference datasets")
    parser.add_argument("--sql-path", default="giaotrinh.sql", help="Path to giaotrinh.sql dump")
    parser.add_argument("--output-dir", default="data/reference", help="Directory to store JSON outputs")
    parser.add_argument("--upload-to-minio", action="store_true", help="Upload generated JSON files to MinIO")
    parser.add_argument(
        "--minio-prefix",
        default="bronze/reference/giaotrinh",
        help="MinIO object prefix for reference datasets (within MINIO_BUCKET)",
    )
    args = parser.parse_args()
    main(args.sql_path, args.output_dir, upload_to_minio=args.upload_to_minio, minio_prefix=args.minio_prefix)
