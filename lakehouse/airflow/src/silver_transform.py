#!/usr/bin/env python3
"""
Silver Layer Dublin Core Transformer
====================================

This module normalizes bronze layer OER scrapes into a consistent Dublin Core
representation stored in Iceberg. Each record includes both structured fields
for analytics and an XML payload that follows the Dublin Core standard so that
search and downstream systems can rely on a uniform contract.
"""

from __future__ import annotations

import hashlib
import os
import sys
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import unicodedata
import re
import xml.etree.ElementTree as ET
from uuid import uuid4

try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

# Spark imports
try:
    from pyspark.sql import Row, SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    Row = SparkSession = DataFrame = None  # type: ignore


@dataclass(frozen=True)
class SubjectCategoryRule:
    pattern: str
    name: str
    code: str


class SilverDublinCoreTransformer:
    """Transforms bronze JSON payloads into normalized Dublin Core XML records."""

    SUBJECT_CATEGORY_RULES: Tuple[SubjectCategoryRule, ...] = (
        SubjectCategoryRule(r"\b(computer|software|program|data|ai|machine|cs)\b", "Computer Science", "CS"),
        SubjectCategoryRule(r"\b(math|algebra|calculus|geometry|statistics)\b", "Mathematics", "MATH"),
        SubjectCategoryRule(r"\b(physics|chemistry|biology|science|astronomy)\b", "Natural Sciences", "SCI"),
        SubjectCategoryRule(r"\b(engineering|mechanical|electrical|civil|aerospace)\b", "Engineering", "ENG"),
        SubjectCategoryRule(r"\b(history|politic|sociology|psychology|anthropology|philosophy)\b", "Social Scie nces", "SOC"),
        SubjectCategoryRule(r"\b(business|finance|management|marketing|economics)\b", "Business and Management", "BUS"),
        SubjectCategoryRule(r"\b(language|literature|writing|art|music)\b", "Arts and Humanities", "ART"),
        SubjectCategoryRule(r"\b(education|teaching|learning|pedagogy)\b", "Education", "EDU"),
        SubjectCategoryRule(r"\b(health|medicine|nursing|biology|biomedical)\b", "Health Sciences", "HEALTH"),
    )

    PUBLISHER_TYPE_RULES: Tuple[Tuple[str, str], ...] = (
        ("university", "University"),
        ("institute", "University"),
        ("college", "University"),
        ("openstax", "Non-profit"),
        ("opentextbook", "Non-profit"),
        ("mit", "University"),
        ("ocw", "University"),
        ("library", "Library"),
    )

    FORMAT_CATEGORY_RULES: Tuple[Tuple[str, str], ...] = (
        ("pdf", "Document"),
        ("html", "Document"),
        ("ebook", "Document"),
        ("video", "Media"),
        ("audio", "Media"),
        ("interactive", "Interactive"),
        ("notebook", "Interactive"),
    )

    def __init__(self) -> None:
        if not SPARK_AVAILABLE:
            raise RuntimeError("PySpark is required to run the silver transformer")

        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.catalog_name = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.database_name = os.getenv("SILVER_DATABASE", "default")
        self.table_name = os.getenv("SILVER_TABLE", "oer_resources")
        self.full_table_name = f"{self.catalog_name}.{self.database_name}.{self.table_name}"
        self.legacy_table_name = f"{self.catalog_name}.{self.database_name}.oer_dc_documents"
        self.subjects_bridge_table = f"{self.catalog_name}.{self.database_name}.oer_resource_subjects"
        self.keywords_bridge_table = f"{self.catalog_name}.{self.database_name}.oer_resource_keywords"
        self.creators_bridge_table = f"{self.catalog_name}.{self.database_name}.oer_resource_creators"
        self.reference_subjects_table = f"{self.catalog_name}.{self.database_name}.reference_subjects"
        self.reference_faculties_table = f"{self.catalog_name}.{self.database_name}.reference_faculties"
        self.reference_departments_table = f"{self.catalog_name}.{self.database_name}.reference_departments"
        self.reference_programs_table = f"{self.catalog_name}.{self.database_name}.reference_programs"
        self.reference_program_subject_links_table = f"{self.catalog_name}.{self.database_name}.reference_program_subject_links"
        self.reference_textbooks_table = f"{self.catalog_name}.{self.database_name}.reference_textbooks"
        self.reference_dewey_table = f"{self.catalog_name}.{self.database_name}.reference_dewey_classes"

        self.spark = self._create_spark_session()
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.database_name}")

        bronze_hint = os.getenv("BRONZE_INPUT")
        self.bronze_root = bronze_hint or f"s3a://{self.bucket}/bronze/"
        print(f"Silver transformer targeting {self.full_table_name} from {self.bronze_root}")

        # Reference datasets (faculties, subjects) for canonical mapping
        self.reference_subjects_by_code: Dict[str, Dict[str, Any]] = {}
        self.reference_subjects_by_name: Dict[str, Dict[str, Any]] = {}
        self.reference_faculties: Dict[int, str] = {}
        self.reference_departments: Dict[int, str] = {}
        self.reference_enabled = False
        self.reference_subject_records: List[Dict[str, Any]] = []
        self.reference_faculty_records: List[Dict[str, Any]] = []
        self.reference_department_records: List[Dict[str, Any]] = []
        self.reference_program_records: List[Dict[str, Any]] = []
        self.reference_program_subject_links_records: List[Dict[str, Any]] = []
        self.reference_textbook_records: List[Dict[str, Any]] = []
        self.reference_dewey_records: List[Dict[str, Any]] = []
        self.reference_storage: str = "local"
        self.reference_path: Optional[Path] = None
        self.reference_bucket: Optional[str] = None
        self.reference_prefix: str = ""
        self.minio_client: Optional[Minio] = None

        reference_uri = os.getenv("REFERENCE_DATA_URI")
        if reference_uri and reference_uri.startswith(("s3://", "s3a://")):
            if MINIO_AVAILABLE:
                bucket, prefix = self._parse_s3_uri(reference_uri)
                self.reference_storage = "minio"
                self.reference_bucket = bucket
                self.reference_prefix = prefix
                self.minio_client = self._create_minio_client()
                if not self.minio_client:
                    print("Warning: unable to initialize MinIO client; falling back to local reference files")
                    self.reference_storage = "local"
                    self.reference_path = Path(__file__).resolve().parents[2] / "data" / "reference"
            else:
                print("Warning: REFERENCE_DATA_URI points to MinIO but minio package not installed; using local fallback")
                self.reference_path = Path(__file__).resolve().parents[2] / "data" / "reference"
        elif reference_uri:
            self.reference_path = Path(reference_uri)
        else:
            self.reference_path = Path(__file__).resolve().parents[2] / "data" / "reference"

        self._load_reference_data()
        self._write_reference_dimensions()

        self.output_schema = self._build_output_schema()

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("spark", None)
        state.pop("minio_client", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self.spark = None
        self.minio_client = None
        self.output_schema = self._build_output_schema()

    def _build_output_schema(self) -> T.StructType:
        return T.StructType(
            [
                T.StructField("resource_id", T.StringType(), False),
                T.StructField("resource_uid", T.StringType(), False),
                T.StructField("source_system", T.StringType(), True),
                T.StructField("source_url", T.StringType(), True),
                T.StructField("catalog_provider", T.StringType(), True),
                T.StructField("title", T.StringType(), True),
                T.StructField("description", T.StringType(), True),
                T.StructField("keywords", T.ArrayType(T.StringType()), True),
                T.StructField("subjects", T.ArrayType(T.StringType()), True),
                T.StructField("primary_subject", T.StringType(), True),
                T.StructField("primary_subject_code", T.StringType(), True),
                T.StructField("subject_category_name", T.StringType(), True),
                T.StructField("subject_category_code", T.StringType(), True),
                T.StructField("faculty_id", T.IntegerType(), True),
                T.StructField("faculty_name", T.StringType(), True),
                T.StructField("creator_names", T.ArrayType(T.StringType()), True),
                T.StructField("publisher_name", T.StringType(), True),
                T.StructField("publisher_type", T.StringType(), True),
                T.StructField("language", T.StringType(), True),
                T.StructField("license_name", T.StringType(), True),
                T.StructField("license_url", T.StringType(), True),
                T.StructField("resource_format", T.StringType(), True),
                T.StructField("resource_format_category", T.StringType(), True),
                T.StructField("resource_type", T.StringType(), True),
                T.StructField("difficulty_level", T.StringType(), True),
                T.StructField("target_audience", T.StringType(), True),
                T.StructField("education_level", T.StringType(), True),
                T.StructField("course_id", T.StringType(), True),
                T.StructField("course_code", T.StringType(), True),
                T.StructField("course_name", T.StringType(), True),
                T.StructField("department_id", T.IntegerType(), True),
                T.StructField("department_name", T.StringType(), True),
                T.StructField("institution_name", T.StringType(), True),
                T.StructField("publication_date", T.TimestampType(), True),
                T.StructField("last_updated_at", T.TimestampType(), True),
                T.StructField("scraped_at", T.TimestampType(), True),
                T.StructField("bronze_source_path", T.StringType(), True),
                T.StructField("data_quality_score", T.DoubleType(), True),
                T.StructField("dc_xml", T.StringType(), True),
                T.StructField("ingested_at", T.TimestampType(), False),
            ]
        )

    def _load_reference_data(self) -> None:
        """Load faculties and subjects reference data for canonical mapping."""
        try:
            subjects_data = self._load_reference_json("subjects.json")
            if subjects_data:
                self.reference_subject_records = subjects_data
                for record in subjects_data:
                    name_key = self._normalize_text(record.get("subject_name"))
                    code_key = (record.get("subject_code") or "").strip().lower()
                    if code_key:
                        self.reference_subjects_by_code[code_key] = record
                    if name_key:
                        existing = self.reference_subjects_by_name.get(name_key)
                        if not existing or existing.get("subject_code") in (None, ""):
                            self.reference_subjects_by_name[name_key] = record
                print(f"Loaded {len(self.reference_subjects_by_name)} reference subjects from {self._reference_location_desc('subjects.json')}")
            else:
                print("Reference subjects not found; subject normalization will use heuristics")

            faculties_data = self._load_reference_json("faculties.json")
            if faculties_data:
                self.reference_faculty_records = faculties_data
                for record in faculties_data:
                    faculty_id = record.get("faculty_id")
                    faculty_name = record.get("faculty_name") or ""
                    if faculty_id is not None:
                        self.reference_faculties[int(faculty_id)] = faculty_name
                print(f"Loaded {len(self.reference_faculties)} faculties from {self._reference_location_desc('faculties.json')}")

            departments_data = self._load_reference_json("departments.json")
            if departments_data:
                self.reference_department_records = departments_data
                for record in departments_data:
                    department_id = record.get("department_id")
                    department_name = record.get("department_name") or ""
                    if department_id is not None:
                        self.reference_departments[int(department_id)] = department_name
                if self.reference_departments:
                    print(f"Loaded {len(self.reference_departments)} departments from {self._reference_location_desc('departments.json')}")

            programs_data = self._load_reference_json("programs.json")
            if programs_data:
                self.reference_program_records = programs_data

            program_subject_links = self._load_reference_json("program_subject_links.json")
            if program_subject_links:
                self.reference_program_subject_links_records = program_subject_links

            textbooks_data = self._load_reference_json("textbooks.json")
            if textbooks_data:
                self.reference_textbook_records = textbooks_data

            dewey_data = self._load_reference_json("dewey_classes.json")
            if dewey_data:
                self.reference_dewey_records = dewey_data

            self.reference_enabled = bool(self.reference_subjects_by_name or self.reference_subjects_by_code)
            if not self.reference_enabled:
                print("Reference mapping disabled: no subjects found")
        except Exception as exc:
            print(f"Warning loading reference data: {exc}")
            self.reference_enabled = False
        finally:
            if self.reference_storage == "minio":
                self.minio_client = None

    def _reference_location_desc(self, filename: str) -> str:
        if self.reference_storage == "minio" and self.reference_bucket:
            prefix = self.reference_prefix.rstrip("/")
            object_name = f"{prefix}/{filename}" if prefix else filename
            return f"s3://{self.reference_bucket}/{object_name}"
        if self.reference_path:
            return str(self.reference_path / filename)
        return filename

    def _load_reference_json(self, filename: str) -> Optional[Any]:
        try:
            if self.reference_storage == "minio" and self.minio_client and self.reference_bucket:
                object_name = filename if not self.reference_prefix else f"{self.reference_prefix.rstrip('/')}/{filename}"
                try:
                    response = self.minio_client.get_object(self.reference_bucket, object_name)
                    try:
                        data = json.loads(response.read().decode("utf-8"))
                    finally:
                        response.close()
                        response.release_conn()
                    return data
                except S3Error as s3_err:
                    print(f"Reference object missing in MinIO ({object_name}): {s3_err}")
                    return None
            if self.reference_path:
                file_path = self.reference_path / filename
                if not file_path.exists():
                    return None
                with file_path.open("r", encoding="utf-8") as fh:
                    return json.load(fh)
            return None
        except Exception as exc:
            print(f"Warning reading reference file {filename}: {exc}")
            return None

    def _create_minio_client(self) -> Optional[Minio]:
        try:
            endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
            access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
            secure = os.getenv("MINIO_SECURE", "0").lower() in {"1", "true", "yes"}
            client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
            return client
        except Exception as exc:
            print(f"Warning: unable to create MinIO client ({exc})")
            return None

    def _parse_s3_uri(self, uri: str) -> Tuple[str, str]:
        cleaned = uri.replace("s3a://", "").replace("s3://", "")
        if "/" not in cleaned:
            return cleaned, ""
        bucket, prefix = cleaned.split("/", 1)
        return bucket, prefix.rstrip("/")

class SilverTransformStandalone(SilverDublinCoreTransformer):
    """Backward-compatible alias for existing DAG imports."""

    def __init__(self) -> None:
        super().__init__()

    def _create_spark_session(self) -> SparkSession:
        session = (
            SparkSession.builder.appName("OER-Silver-Dublin-Core")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.%s" % self.catalog_name, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.%s.type" % self.catalog_name, "hadoop")
            .config("spark.sql.catalog.%s.warehouse" % self.catalog_name, f"s3a://{self.bucket}/silver/")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "hadoop")
            .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{self.bucket}/warehouse/")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("WARN")
        return session

    def run(self) -> None:
        bronze_df = self._load_bronze_payloads()
        if bronze_df is None or bronze_df.rdd.isEmpty():
            print("No bronze payloads found for silver transformation")
            return

        normalized_df = self._normalize_to_dublin_core(bronze_df)
        if normalized_df is None or normalized_df.rdd.isEmpty():
            print("Bronze payloads could not be normalized, skipping write")
            return

        self._write_tables(normalized_df)

    def _load_bronze_payloads(self) -> Optional[DataFrame]:
        candidate_paths = self._resolve_bronze_paths()
        if not candidate_paths:
            print("No bronze input paths were resolved")
            return None

        try:
            df = (
                self.spark.read.option("recursiveFileLookup", "true")
                .option("multiLine", "true")
                .json(candidate_paths)
                .withColumn("bronze_source_path", F.input_file_name())
            )
            print(f"Loaded bronze dataset with {df.count()} raw records")
            return df
        except Exception as exc:
            print(f"Failed to load bronze payloads: {exc}")
            return None

    def _resolve_bronze_paths(self) -> List[str]:
        hint = os.getenv("BRONZE_INPUT")
        if hint:
            return [value.strip() for value in hint.split(",") if value.strip()]

        local_candidates: Iterable[Path] = (
            Path(self.bronze_root) if Path(self.bronze_root).exists() else None,
            Path.cwd() / "lakehouse" / "data" / "scraped",
            Path(__file__).resolve().parents[2] / "data" / "scraped",
        )

        resolved: List[str] = []
        for candidate in local_candidates:
            if candidate and candidate.exists():
                resolved.append(str(candidate))

        if resolved:
            return resolved

        return [self.bronze_root]

    def _normalize_to_dublin_core(self, bronze_df: DataFrame) -> Optional[DataFrame]:
        def transform_row(row_dict: Dict[str, Any]) -> Optional[Row]:
            trimmed = {k: row_dict.get(k) for k in row_dict}
            metadata = self._normalize_record(trimmed)
            if not metadata:
                return None
            return Row(**metadata)

        normalized_rdd = bronze_df.rdd.map(lambda row: transform_row(row.asDict(recursive=True))).filter(
            lambda item: item is not None
        )

        if normalized_rdd.isEmpty():
            return None

        return self.spark.createDataFrame(normalized_rdd, schema=self.output_schema)

    def _normalize_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        source_system = self._detect_source_system(record)
        resource_id = self._select_identifier(record, source_system)
        if not resource_id:
            return None

        now = datetime.utcnow()
        source_url = self._ensure_str(record.get("url") or record.get("link"))
        title = self._ensure_title(record)
        description = self._ensure_str(record.get("description"))
        creators = self._clean_string_list(record.get("instructors") or record.get("authors") or record.get("creators"))
        subjects = self._derive_subjects(record)
        primary_subject = subjects[0] if subjects else self._ensure_str(record.get("subject"))
        subject_category_name, subject_category_code = self._derive_subject_category(primary_subject)

        keywords = self._derive_keywords(record, subjects, title)
        language = self._ensure_language(record.get("language"))
        publisher_name = self._derive_publisher(record, source_system)
        publisher_type = self._derive_publisher_type(publisher_name)
        license_name, license_url = self._derive_license(record)
        resource_format = self._derive_format(record)
        resource_format_category = self._derive_format_category(resource_format)
        resource_type = self._derive_resource_type(record, source_system)
        difficulty_level, target_audience = self._derive_level(record)
        education_level = target_audience

        primary_subject_code: Optional[str] = None
        faculty_id: Optional[int] = None
        faculty_name: Optional[str] = None
        department_id: Optional[int] = None

        course_id = self._ensure_str(record.get("course_id") or record.get("identifier"))
        course_code = self._ensure_str(record.get("course_number") or record.get("code"))
        course_name = self._ensure_str(record.get("course_title") or record.get("course_name") or title)
        department_name = self._ensure_str(record.get("department"))
        institution_name = self._derive_institution(source_system)

        reference_subject = self._match_reference_subject(primary_subject, subjects, course_code, course_id, course_name)
        if reference_subject:
            if reference_subject.get("subject_name"):
                primary_subject = reference_subject["subject_name"]
                if primary_subject and primary_subject not in subjects:
                    subjects.insert(0, primary_subject)
            primary_subject_code = reference_subject.get("subject_code")
            ref_faculty_id = reference_subject.get("faculty_id")
            if ref_faculty_id is not None:
                faculty_id = int(ref_faculty_id)
                faculty_name = self.reference_faculties.get(faculty_id)
                if faculty_name:
                    subject_category_name = faculty_name
                    subject_category_code = f"FAC_{faculty_id:02d}"
            ref_department_id = reference_subject.get("department_id")
            if ref_department_id is not None:
                department_id = int(ref_department_id)
                department_name = self.reference_departments.get(department_id, department_name)

        publication_date = self._parse_datetime(record.get("publication_date") or record.get("year"))
        scraped_at = self._parse_datetime(record.get("scraped_at"))
        last_updated_at = self._parse_datetime(record.get("last_updated_at") or record.get("updated_at") or scraped_at)
        bronze_source_path = self._ensure_str(record.get("bronze_source_path"))

        data_quality_score = self._compute_quality_score(
            title=title,
            description=description,
            subjects=subjects,
            keywords=keywords,
            creators=creators,
            publisher_name=publisher_name,
            language=language,
            license_name=license_name,
            source_url=source_url,
        )

        dc_xml = self._build_dublin_core_xml(
            identifier=resource_id,
            title=title,
            description=description,
            creators=creators,
            subjects=subjects or keywords,
            publisher=publisher_name,
            language=language,
            rights=license_name,
            source=source_system,
            resource_type=resource_type,
            resource_format=resource_format,
            url=source_url,
            audience=target_audience,
            relation=course_id or course_code,
            coverage=institution_name or department_name,
            date=publication_date or scraped_at,
        )

        metadata: Dict[str, Any] = {
            "resource_id": resource_id,
            "resource_uid": self._hash_identifier(resource_id),
            "source_system": source_system,
            "source_url": source_url,
            "catalog_provider": institution_name or publisher_name,
            "title": title,
            "description": description,
            "keywords": keywords,
            "subjects": subjects,
            "primary_subject": primary_subject,
            "primary_subject_code": primary_subject_code,
            "subject_category_name": subject_category_name,
            "subject_category_code": subject_category_code,
            "faculty_id": faculty_id,
            "faculty_name": faculty_name,
            "creator_names": creators,
            "publisher_name": publisher_name,
            "publisher_type": publisher_type,
            "language": language,
            "license_name": license_name,
            "license_url": license_url,
            "resource_format": resource_format,
            "resource_format_category": resource_format_category,
            "resource_type": resource_type,
            "difficulty_level": difficulty_level,
            "target_audience": target_audience,
            "education_level": education_level,
            "course_id": course_id,
            "course_code": course_code,
            "course_name": course_name,
            "department_id": department_id,
            "department_name": department_name,
            "institution_name": institution_name,
            "publication_date": publication_date,
            "last_updated_at": last_updated_at,
            "scraped_at": scraped_at,
            "bronze_source_path": bronze_source_path,
            "data_quality_score": data_quality_score,
            "dc_xml": dc_xml,
            "ingested_at": now,
        }
        return metadata

    def _write_tables(self, df: DataFrame) -> None:
        if not self.spark:
            return

        deduped_df = df.dropDuplicates(["resource_uid"]).persist()

        fact_df = (
            deduped_df.withColumn("subjects_count", F.coalesce(F.size("subjects"), F.lit(0)))
            .withColumn("keywords_count", F.coalesce(F.size("keywords"), F.lit(0)))
            .withColumn("creator_count", F.coalesce(F.size("creator_names"), F.lit(0)))
            .select(
                "resource_uid",
                "resource_id",
                "source_system",
                "source_url",
                "catalog_provider",
                "title",
                "description",
                "primary_subject",
                "primary_subject_code",
                "subject_category_name",
                "subject_category_code",
                "faculty_id",
                "department_id",
                "institution_name",
                "publisher_name",
                "publisher_type",
                "language",
                "license_name",
                "license_url",
                "resource_format",
                "resource_format_category",
                "resource_type",
                "difficulty_level",
                "target_audience",
                "education_level",
                "course_id",
                "course_code",
                "course_name",
                "subjects_count",
                "keywords_count",
                "creator_count",
                "bronze_source_path",
                "scraped_at",
                "last_updated_at",
                "publication_date",
                "data_quality_score",
                "dc_xml",
                "ingested_at",
            )
        )

        subjects_df = (
            deduped_df.select(
                "resource_uid",
                "source_system",
                "primary_subject_code",
                "ingested_at",
                F.posexplode_outer("subjects").alias("subject_position", "subject_name"),
            )
            .where(F.col("subject_name").isNotNull())
            .withColumn("is_primary", F.col("subject_position") == 0)
            .withColumn(
                "subject_code",
                F.when(F.col("is_primary"), F.col("primary_subject_code")),
            )
            .select(
                "resource_uid",
                F.col("subject_name").alias("subject_name"),
                "subject_code",
                "is_primary",
                "source_system",
                "ingested_at",
            )
        )

        keywords_df = (
            deduped_df.select(
                "resource_uid",
                "source_system",
                "ingested_at",
                F.explode_outer("keywords").alias("keyword"),
            )
            .where(F.col("keyword").isNotNull())
        )

        creators_df = (
            deduped_df.select(
                "resource_uid",
                "source_system",
                "ingested_at",
                F.explode_outer("creator_names").alias("creator_name"),
            )
            .where(F.col("creator_name").isNotNull())
        )

        self._replace_table(
            fact_df,
            self.full_table_name,
            partition_columns=["subject_category_code", "DATE(ingested_at)"],
        )
        if self.legacy_table_name != self.full_table_name:
            self._replace_table(
                deduped_df,
                self.legacy_table_name,
                partition_columns=["subject_category_code", "DATE(ingested_at)"],
            )
        self._replace_table(subjects_df, self.subjects_bridge_table, partition_columns=["DATE(ingested_at)"])
        self._replace_table(keywords_df, self.keywords_bridge_table, partition_columns=["DATE(ingested_at)"])
        self._replace_table(creators_df, self.creators_bridge_table, partition_columns=["DATE(ingested_at)"])

        deduped_df.unpersist(False)

    def _replace_table(self, df: DataFrame, table_name: str, partition_columns: Optional[List[str]] = None) -> None:
        temp_view = f"temp_{uuid4().hex}"
        row_count = df.count()
        df.createOrReplaceTempView(temp_view)

        partition_clause = ""
        if partition_columns:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_columns)})"

        self.spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_name}
            USING iceberg
            {partition_clause}
            AS SELECT * FROM {temp_view}
            """
        )
        self.spark.catalog.dropTempView(temp_view)
        print(f"Wrote {row_count} records to {table_name}")

    def _write_reference_dimensions(self) -> None:
        if not self.spark:
            return

        def create_df(records: List[Dict[str, Any]], schema: Optional[T.StructType] = None) -> Optional[DataFrame]:
            if not records:
                return None
            if schema:
                return self.spark.createDataFrame(records, schema=schema)
            return self.spark.createDataFrame(records)

        subjects_schema = T.StructType(
            [
                T.StructField("subject_id", T.IntegerType(), True),
                T.StructField("subject_name", T.StringType(), True),
                T.StructField("subject_code", T.StringType(), True),
                T.StructField("faculty_id", T.IntegerType(), True),
                T.StructField("department_id", T.IntegerType(), True),
                T.StructField("subject_type_id", T.IntegerType(), True),
                T.StructField("status_code", T.StringType(), True),
            ]
        )
        faculties_schema = T.StructType(
            [
                T.StructField("faculty_id", T.IntegerType(), True),
                T.StructField("faculty_name", T.StringType(), True),
            ]
        )
        departments_schema = T.StructType(
            [
                T.StructField("department_id", T.IntegerType(), True),
                T.StructField("department_name", T.StringType(), True),
                T.StructField("faculty_id", T.IntegerType(), True),
            ]
        )

        subjects_df = create_df(self.reference_subject_records, subjects_schema)
        faculties_df = create_df(self.reference_faculty_records, faculties_schema)
        departments_df = create_df(self.reference_department_records, departments_schema)
        programs_df = create_df(self.reference_program_records)
        program_subject_links_df = create_df(self.reference_program_subject_links_records)
        textbooks_df = create_df(self.reference_textbook_records)
        dewey_df = create_df(self.reference_dewey_records)

        if subjects_df is not None:
            self._replace_table(subjects_df, self.reference_subjects_table)
        if faculties_df is not None:
            self._replace_table(faculties_df, self.reference_faculties_table)
        if departments_df is not None:
            self._replace_table(departments_df, self.reference_departments_table)
        if programs_df is not None:
            self._replace_table(programs_df, self.reference_programs_table)
        if program_subject_links_df is not None:
            self._replace_table(program_subject_links_df, self.reference_program_subject_links_table)
        if textbooks_df is not None:
            self._replace_table(textbooks_df, self.reference_textbooks_table)
        if dewey_df is not None:
            self._replace_table(dewey_df, self.reference_dewey_table)

    def _detect_source_system(self, record: Dict[str, Any]) -> str:
        candidates = [
            record.get("source"),
            record.get("source_system"),
            record.get("provider"),
            record.get("scraper"),
        ]
        for candidate in candidates:
            if not candidate:
                continue
            value = str(candidate).strip().lower()
            if value:
                return value
        url_str = self._ensure_str(record.get("url") or record.get("link"))
        if url_str:
            hostname = url_str.lower()
            if "openstax" in hostname:
                return "openstax"
            if "ocw.mit.edu" in hostname or "mit" in hostname:
                return "mit_ocw"
            if "open.umn.edu" in hostname:
                return "otl"
        return "unknown"

    def _select_identifier(self, record: Dict[str, Any], source_system: str) -> Optional[str]:
        for key in ("resource_id", "id", "identifier", "url", "course_id"):
            candidate = record.get(key)
            if candidate:
                value = self._ensure_str(candidate)
                if value:
                    return value
        if source_system and record.get("title"):
            raw = f"{source_system}:{record.get('title')}"
            return self._hash_identifier(raw)
        return None

    def _ensure_title(self, record: Dict[str, Any]) -> Optional[str]:
        for key in ("title", "name", "course_title"):
            candidate = self._ensure_str(record.get(key))
            if candidate:
                return candidate
        return None

    def _ensure_str(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (list, dict)):
            return None
        text = str(value).strip()
        return text or None

    def _clean_string_list(self, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            raw_items: Iterable[str] = (item.strip() for item in value.split(","))
        elif isinstance(value, Iterable):
            raw_items = (self._ensure_str(item) or "" for item in value)
        else:
            raw_items = ()
        cleaned: List[str] = []
        for item in raw_items:
            item = (item or "").strip()
            if item and item not in cleaned:
                cleaned.append(item)
        return cleaned

    def _normalize_text(self, value: Optional[str]) -> str:
        if not value:
            return ""
        normalized = unicodedata.normalize("NFKD", value)
        normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = normalized.encode("ascii", "ignore").decode("ascii")
        normalized = re.sub(r"[^a-z0-9]+", " ", normalized.lower())
        return normalized.strip()

    def _derive_subjects(self, record: Dict[str, Any]) -> List[str]:
        candidates = [
            record.get("subjects"),
            record.get("subject"),
            record.get("categories"),
            record.get("tags"),
            record.get("keywords"),
        ]
        subjects: List[str] = []
        for candidate in candidates:
            subjects.extend(self._clean_string_list(candidate))
        return subjects[:10]

    def _derive_subject_category(self, primary_subject: Optional[str]) -> Tuple[str, str]:
        if not primary_subject:
            return "General Studies", "GEN"
        normalized = primary_subject.lower()
        for rule in self.SUBJECT_CATEGORY_RULES:
            if rule.pattern and self._regex_match(rule.pattern, normalized):
                return rule.name, rule.code
        return "General Studies", "GEN"

    def _match_reference_subject(
        self,
        primary_subject: Optional[str],
        subjects: List[str],
        course_code: Optional[str],
        course_id: Optional[str],
        course_name: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        if not self.reference_enabled:
            return None

        candidate_codes = []
        for code in (course_code, course_id):
            if code:
                candidate_codes.append(code.strip().lower())
        for code in candidate_codes:
            match = self.reference_subjects_by_code.get(code)
            if match:
                return match

        candidate_names: List[str] = []
        for value in (primary_subject, course_name):
            if value:
                candidate_names.append(value)
        candidate_names.extend(subjects)

        seen = set()
        for name in candidate_names:
            key = self._normalize_text(name)
            if key and key not in seen:
                seen.add(key)
                match = self.reference_subjects_by_name.get(key)
                if match:
                    return match
        return None

    def _derive_keywords(self, record: Dict[str, Any], subjects: List[str], title: Optional[str]) -> List[str]:
        keywords = set(subjects)
        keywords.update(self._clean_string_list(record.get("keywords")))
        keywords.update(self._clean_string_list(record.get("tags")))
        if title:
            title_tokens = [token for token in re_split_words(title) if len(token) > 3]
            keywords.update(title_tokens[:10])
        return sorted(keyword for keyword in keywords if keyword)

    def _ensure_language(self, value: Any) -> str:
        language = self._ensure_str(value) or "en"
        if len(language) == 2:
            return language.lower()
        return language[:5].lower()

    def _derive_publisher(self, record: Dict[str, Any], source_system: str) -> str:
        publisher = self._ensure_str(record.get("publisher"))
        if publisher:
            return publisher
        if source_system == "mit_ocw":
            return "MIT OpenCourseWare"
        if source_system == "openstax":
            return "OpenStax"
        if source_system == "otl":
            return "Open Textbook Library"
        return "Unknown Publisher"

    def _derive_publisher_type(self, publisher_name: str) -> str:
        name = (publisher_name or "").lower()
        for keyword, publisher_type in self.PUBLISHER_TYPE_RULES:
            if keyword in name:
                return publisher_type
        return "Platform"

    def _derive_license(self, record: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        name = self._ensure_str(record.get("license") or record.get("rights"))
        url = self._ensure_str(record.get("license_url") or record.get("rights_url"))
        if not name and record.get("legal_notice"):
            legal = self._ensure_str(record["legal_notice"])
            if legal:
                name = legal[:120]
        return name, url

    def _derive_format(self, record: Dict[str, Any]) -> Optional[str]:
        candidates = [
            record.get("format"),
            record.get("content_format"),
            record.get("resource_format"),
        ]
        for candidate in candidates:
            value = self._ensure_str(candidate)
            if value:
                return value.lower()
        if record.get("has_videos"):
            return "video"
        if record.get("has_pdfs"):
            return "pdf"
        return None

    def _derive_format_category(self, resource_format: Optional[str]) -> Optional[str]:
        if not resource_format:
            return None
        fmt = resource_format.lower()
        for keyword, category in self.FORMAT_CATEGORY_RULES:
            if keyword in fmt:
                return category
        return "Document"

    def _derive_resource_type(self, record: Dict[str, Any], source_system: str) -> str:
        explicit = self._ensure_str(record.get("resource_type"))
        if explicit:
            return explicit.title()
        if source_system == "mit_ocw":
            return "Course"
        if source_system == "openstax":
            return "Textbook"
        if source_system == "otl":
            return "Textbook"
        return "Learning Resource"

    def _derive_level(self, record: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        difficulty = self._ensure_str(record.get("difficulty") or record.get("level"))
        audience = self._ensure_str(record.get("target_audience"))
        if not audience and difficulty:
            audience = {
                "introductory": "Undergraduate",
                "beginner": "Undergraduate",
                "intermediate": "Undergraduate",
                "advanced": "Graduate",
                "graduate": "Graduate",
            }.get(difficulty.lower())
        return difficulty, audience

    def _derive_institution(self, source_system: str) -> Optional[str]:
        if source_system == "mit_ocw":
            return "Massachusetts Institute of Technology"
        if source_system == "openstax":
            return "Rice University / OpenStax"
        if source_system == "otl":
            return "University of Minnesota"
        return None

    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        text = self._ensure_str(value)
        if not text:
            return None
        try:
            if text.isdigit() and len(text) == 4:
                return datetime(int(text), 1, 1)
            text = text.replace("Z", "+00:00")
            return datetime.fromisoformat(text)
        except Exception:
            return None

    def _compute_quality_score(
        self,
        *,
        title: Optional[str],
        description: Optional[str],
        subjects: List[str],
        keywords: List[str],
        creators: List[str],
        publisher_name: Optional[str],
        language: Optional[str],
        license_name: Optional[str],
        source_url: Optional[str],
    ) -> float:
        checks = [
            bool(title),
            bool(description),
            bool(subjects),
            bool(keywords),
            bool(creators),
            bool(publisher_name),
            bool(language),
            bool(license_name),
            bool(source_url),
        ]
        score = sum(1 for flag in checks if flag) / len(checks)
        return round(score, 3)

    def _hash_identifier(self, identifier: str) -> str:
        return hashlib.sha256(identifier.encode("utf-8")).hexdigest()

    def _build_dublin_core_xml(
        self,
        *,
        identifier: str,
        title: Optional[str],
        description: Optional[str],
        creators: List[str],
        subjects: List[str],
        publisher: Optional[str],
        language: Optional[str],
        rights: Optional[str],
        source: Optional[str],
        resource_type: Optional[str],
        resource_format: Optional[str],
        url: Optional[str],
        audience: Optional[str],
        relation: Optional[str],
        coverage: Optional[str],
        date: Optional[datetime],
    ) -> str:
        ns = {
            "xmlns:oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
            "xmlns:dc": "http://purl.org/dc/elements/1.1/",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xsi:schemaLocation": "http://www.openarchives.org/OAI/2.0/oai_dc/ "
            "http://www.openarchives.org/OAI/2.0/oai_dc.xsd",
        }
        root = ET.Element("oai_dc:dc", ns)

        def add_element(tag: str, value: Optional[str]) -> None:
            if value:
                ET.SubElement(root, f"dc:{tag}").text = value

        add_element("identifier", identifier)
        if url and url != identifier:
            add_element("identifier", url)
        add_element("title", title)
        add_element("description", description)
        for creator in creators:
            add_element("creator", creator)
        for subject in subjects:
            add_element("subject", subject)
        add_element("publisher", publisher)
        add_element("language", language)
        add_element("rights", rights)
        add_element("source", source)
        add_element("type", resource_type)
        add_element("format", resource_format)
        add_element("relation", relation)
        add_element("coverage", coverage)
        add_element("audience", audience)
        if date:
            add_element("date", date.strftime("%Y-%m-%dT%H:%M:%SZ"))

        return ET.tostring(root, encoding="utf-8").decode("utf-8")

    def _regex_match(self, pattern: str, value: str) -> bool:
        import re

        return re.search(pattern, value, re.IGNORECASE) is not None


def re_split_words(text: str) -> List[str]:
    import re

    return [token for token in re.split(r"[^A-Za-z0-9]+", text) if token]


def main() -> None:
    transformer = SilverDublinCoreTransformer()
    transformer.run()


if __name__ == "__main__":
    main()
