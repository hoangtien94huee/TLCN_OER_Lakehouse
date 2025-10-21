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

    def __init__(self, sql_path: Path) -> None:
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file does not exist: {sql_path}")
        sql_text = sql_path.read_text(encoding="utf-8", errors="ignore")
        self.parser = SqlInsertParser(sql_text)

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
        result: List[Subject] = []
        for row in rows:
            subject_id, name, code, _, faculty_id, department_id, subject_type, status = row
            result.append(
                Subject(
                    subject_id=int(subject_id),
                    subject_name=name or "",
                    subject_code=code,
                    faculty_id=int(faculty_id) if faculty_id is not None else None,
                    department_id=int(department_id) if department_id is not None else None,
                    subject_type_id=int(subject_type) if subject_type is not None else None,
                    status_code=status,
                )
            )
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
    parser.add_argument("--output-dir", default="lakehouse/data/reference", help="Directory to store JSON outputs")
    parser.add_argument("--upload-to-minio", action="store_true", help="Upload generated JSON files to MinIO")
    parser.add_argument(
        "--minio-prefix",
        default="bronze/reference/giaotrinh",
        help="MinIO object prefix for reference datasets (within MINIO_BUCKET)",
    )
    args = parser.parse_args()
    main(args.sql_path, args.output_dir, upload_to_minio=args.upload_to_minio, minio_prefix=args.minio_prefix)
