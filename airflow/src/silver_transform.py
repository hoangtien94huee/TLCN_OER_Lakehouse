#!/usr/bin/env python3
"""
Silver Layer Resource Transformer
==================================

This module normalizes bronze layer OER scrapes into a consistent representation
stored in Iceberg. Each record includes structured fields for analytics and a
reference path to an XML file that follows the Dublin Core standard.
"""

from __future__ import annotations

import hashlib
import os
import sys
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Set
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


class SilverTransformer:
    """Transforms bronze JSON payloads into normalized records with Dublin Core metadata path references."""

    def __init__(self) -> None:
        if not SPARK_AVAILABLE:
            raise RuntimeError("PySpark is required to run the silver transformer")

        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.catalog_name = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.database_name = os.getenv("SILVER_DATABASE", "default")
        self.table_name = os.getenv("SILVER_TABLE", "oer_resources")
        self.full_table_name = f"{self.catalog_name}.{self.database_name}.{self.table_name}"
        self.reference_subjects_table = f"{self.catalog_name}.{self.database_name}.reference_subjects"
        self.reference_faculties_table = f"{self.catalog_name}.{self.database_name}.reference_faculties"
        self.reference_programs_table = f"{self.catalog_name}.{self.database_name}.reference_programs"
        self.reference_program_subject_links_table = f"{self.catalog_name}.{self.database_name}.reference_program_subject_links"

        self.spark = self._create_spark_session()
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.database_name}")

        bronze_hint = os.getenv("BRONZE_INPUT")
        self.bronze_root = bronze_hint or f"s3a://{self.bucket}/bronze/"
        print(f"Silver transformer targeting {self.full_table_name} from {self.bronze_root}")

        # Reference datasets (faculties, subjects) for canonical mapping
        self.reference_subjects_by_code: Dict[str, Dict[str, Any]] = {}
        self.reference_subjects_by_name: Dict[str, Dict[str, Any]] = {}
        self.reference_subjects_by_name_en: Dict[str, Dict[str, Any]] = {}
        self.reference_subjects_en_index: defaultdict[str, Set[str]] = defaultdict(set)
        self.reference_faculties: Dict[int, str] = {}
        self.reference_enabled = False
        self.reference_subject_records: List[Dict[str, Any]] = []
        self.reference_faculty_records: List[Dict[str, Any]] = []
        self.reference_program_records: List[Dict[str, Any]] = []
        self.reference_program_subject_links_records: List[Dict[str, Any]] = []
        self.reference_subject_aliases: Dict[int, List[str]] = {}
        self.reference_storage: str = "local"
        self.reference_path: Optional[Path] = None
        self.reference_bucket: Optional[str] = None
        self.reference_prefix: str = ""
        self.minio_client: Optional[Minio] = None
        self.subject_lookup_by_id: Dict[int, Dict[str, Any]] = {}
        self.programs_by_subject: defaultdict[int, List[int]] = defaultdict(list)

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
        self._prepare_subject_mappings()
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
                T.StructField(
                    "matched_subjects",
                    T.ArrayType(
                        T.StructType(
                            [
                                T.StructField("subject_id", T.IntegerType(), True),
                                T.StructField("subject_name", T.StringType(), True),
                                T.StructField("subject_name_en", T.StringType(), True),
                                T.StructField("subject_code", T.StringType(), True),
                                T.StructField("similarity", T.DoubleType(), True),
                                T.StructField("matched_text", T.StringType(), True),
                            ]
                        )
                    ),
                    True,
                ),
                T.StructField("canonical_subjects", T.ArrayType(T.StringType()), True),
                T.StructField("program_ids", T.ArrayType(T.IntegerType()), True),
                T.StructField("unmatched_subjects", T.ArrayType(T.StringType()), True),
                T.StructField("creator_names", T.ArrayType(T.StringType()), True),
                T.StructField("publisher_name", T.StringType(), True),
                T.StructField("language", T.StringType(), True),
                T.StructField("license_name", T.StringType(), True),
                T.StructField("license_url", T.StringType(), True),
                T.StructField("publication_date", T.TimestampType(), True),
                T.StructField("last_updated_at", T.TimestampType(), True),
                T.StructField("scraped_at", T.TimestampType(), True),
                T.StructField("bronze_source_path", T.StringType(), True),
                T.StructField("data_quality_score", T.DoubleType(), True),
                T.StructField("dc_xml", T.StringType(), True),  # Temporary for batch save
                T.StructField("dc_xml_path", T.StringType(), True),
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

                    name_en_key = self._normalize_text(record.get("subject_name_en"))
                    if name_en_key:
                        existing_en = self.reference_subjects_by_name_en.get(name_en_key)
                        if not existing_en or existing_en.get("subject_code") in (None, ""):
                            self.reference_subjects_by_name_en[name_en_key] = record
                        for token in name_en_key.split():
                            if token:
                                self.reference_subjects_en_index[token].add(name_en_key)
                print(f"Loaded {len(self.reference_subjects_by_name)} reference subjects from {self._reference_location_desc('subjects.json')}")
            else:
                print("Reference subjects not found; subject normalization will use heuristics")

            aliases_data = self._load_reference_json("subject_aliases.json")
            if isinstance(aliases_data, list):
                alias_count = 0
                for item in aliases_data:
                    if not isinstance(item, dict):
                        continue
                    subject_id = item.get("subject_id")
                    alias_text = item.get("alias") or item.get("text")
                    if subject_id is None or not alias_text:
                        continue
                    try:
                        sid = int(subject_id)
                    except (TypeError, ValueError):
                        continue
                    self.reference_subject_aliases.setdefault(sid, []).append(str(alias_text))
                    alias_count += 1
                if alias_count:
                    print(f"Loaded {alias_count} subject aliases from {self._reference_location_desc('subject_aliases.json')}")
            elif aliases_data:
                print("Warning: subject_aliases.json has unexpected format; ignoring")

            faculties_data = self._load_reference_json("faculties.json")
            if faculties_data:
                self.reference_faculty_records = faculties_data
                for record in faculties_data:
                    faculty_id = record.get("faculty_id")
                    faculty_name = record.get("faculty_name") or ""
                    if faculty_id is not None:
                        self.reference_faculties[int(faculty_id)] = faculty_name
                print(f"Loaded {len(self.reference_faculties)} faculties from {self._reference_location_desc('faculties.json')}")

            programs_data = self._load_reference_json("programs.json")
            if programs_data:
                self.reference_program_records = programs_data

            program_subject_links = self._load_reference_json("program_subject_links.json")
            if program_subject_links:
                self.reference_program_subject_links_records = program_subject_links

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

    def _prepare_subject_mappings(self) -> None:
        """Pre-compute lookup structures for curriculum alignment."""
        self.subject_lookup_by_id = {}
        self.programs_by_subject = defaultdict(list)

        for record in self.reference_subject_records:
            subject_id = record.get("subject_id")
            if subject_id is None:
                continue
            try:
                sid = int(subject_id)
            except (TypeError, ValueError):
                continue
            self.subject_lookup_by_id[sid] = record

        for link in self.reference_program_subject_links_records:
            subject_id = link.get("subject_id")
            program_id = link.get("program_id")
            if subject_id is None or program_id is None:
                continue
            try:
                sid = int(subject_id)
                pid = int(program_id)
            except (TypeError, ValueError):
                continue
            self.programs_by_subject[sid].append(pid)

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


    def _match_subjects(
        self,
        title: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], List[int]]:
        """
        Match subjects by searching English catalog names within title/description ONLY.
        No metadata subjects/keywords - pure content-based matching.
        """
        if not self.reference_enabled:
            return [], []

        matched: Dict[int, Dict[str, Any]] = {}

        # Match subjects from title/description content only
        self._augment_matches_with_english_text(title, description, matched)

        # Extract matched subjects and program IDs
        matched_subjects = sorted(matched.values(), key=lambda item: item["similarity"], reverse=True)
        program_ids = sorted({
            pid 
            for subject_id in matched 
            for pid in self.programs_by_subject.get(subject_id, [])
        })
        
        return matched_subjects, program_ids

    def _augment_matches_with_english_text(
        self,
        title: Optional[str],
        description: Optional[str],
        matched: Dict[int, Dict[str, Any]],
    ) -> None:
        """
        Match subjects by searching English catalog names within title/description.
        Requires ALL tokens of the subject name to be present in the text to avoid false positives.
        """
        if not self.reference_subjects_by_name_en:
            return
        text_parts = [part for part in (title, description) if part]
        if not text_parts:
            return
        normalized_blob = self._normalize_text(" ".join(text_parts))
        if not normalized_blob:
            return

        blob_with_padding = f" {normalized_blob} "
        token_set = {token for token in normalized_blob.split() if token}
        candidate_keys: Set[str] = set()
        for token in token_set:
            token_keys = self.reference_subjects_en_index.get(token)
            if token_keys:
                candidate_keys.update(token_keys)

        for key in candidate_keys:
            # Require ALL tokens of the subject to be present in the text
            # This prevents "Data Mining" from matching "Data Structures" or "Mining Engineering"
            subject_tokens = set(key.split())
            if not subject_tokens.issubset(token_set):
                continue
            
            # Also check that the full phrase appears in the text
            if f" {key} " not in blob_with_padding:
                continue
            
            record = self.reference_subjects_by_name_en.get(key)
            if not record:
                continue
            subject_id = record.get("subject_id")
            if not subject_id or subject_id in matched:
                continue
            matched[subject_id] = {
                "subject_id": subject_id,
                "subject_name": record.get("subject_name"),
                "subject_name_en": record.get("subject_name_en"),
                "subject_code": record.get("subject_code"),
                "similarity": 0.9,
                "matched_text": record.get("subject_name_en") or record.get("subject_name"),
            }

    def _create_spark_session(self) -> SparkSession:
        session = (
            SparkSession.builder.appName("OER-Silver-Dublin-Core")
            .master(os.getenv("SPARK_MASTER", "local[*]"))
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
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
            .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAXRESULTSIZE", "2g"))
            .config("spark.sql.debug.maxToStringFields", "500")  # Increase field limit for complex schemas
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

        # Batch save DC XMLs before writing fact table
        self._batch_save_dc_xmls(normalized_df)

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
        """
        Resolve bronze paths - ONLY include OER sources (mit_ocw, openstax, otl).
        Exclude textbooks and other reference data.
        """
        hint = os.getenv("BRONZE_INPUT")
        if hint:
            return [value.strip() for value in hint.split(",") if value.strip()]

        # OER sources only
        oer_sources = ["mit_ocw", "openstax", "otl"]
        
        local_candidates: Iterable[Path] = (
            Path(self.bronze_root) if Path(self.bronze_root).exists() else None,
            Path.cwd() / "lakehouse" / "data" / "scraped",
            Path(__file__).resolve().parents[2] / "data" / "scraped",
        )

        resolved: List[str] = []
        for candidate in local_candidates:
            if candidate and candidate.exists():
                # Only add OER source subdirectories
                for source in oer_sources:
                    source_path = candidate / source
                    if source_path.exists():
                        resolved.append(str(source_path))

        if resolved:
            return resolved

        # Fallback: try S3 paths for OER sources
        return [f"{self.bronze_root}{source}/" for source in oer_sources]

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

        # Match subjects from title/description ONLY (no metadata subjects/keywords)
        matched_subjects, matched_program_ids_list = self._match_subjects(
            title,
            description,
        )
        program_id_set: Set[int] = set(matched_program_ids_list)
        canonical_subjects = [
            str(item.get("subject_name"))
            for item in matched_subjects
            if item.get("subject_name")
        ]
        language = self._ensure_language(record.get("language"))
        publisher_name = self._derive_publisher(record, source_system)
        license_name, license_url = self._derive_license(record)

        publication_date = self._parse_datetime(record.get("publication_date") or record.get("year"))
        scraped_at = self._parse_datetime(record.get("scraped_at"))
        last_updated_at = self._parse_datetime(record.get("last_updated_at") or record.get("updated_at") or scraped_at)
        bronze_source_path = self._ensure_str(record.get("bronze_source_path"))

        data_quality_score = self._compute_quality_score(
            title=title,
            description=description,
            creators=creators,
            publisher_name=publisher_name,
            language=language,
            license_name=license_name,
            source_url=source_url,
        )

        # Build Dublin Core XML and save to S3/MinIO
        # Use canonical subjects for Dublin Core metadata
        dc_subjects = canonical_subjects
        dc_xml = self._build_dublin_core_xml(
            identifier=resource_id,
            title=title,
            description=description,
            creators=creators,
            subjects=dc_subjects,
            publisher=publisher_name,
            language=language,
            rights=license_name,
            source=source_system,
            url=source_url,
        )
        dc_xml_path = f"s3a://{self.bucket}/silver/dc_xml/{resource_id}.xml"

        metadata: Dict[str, Any] = {
            "resource_id": resource_id,
            "resource_uid": self._hash_identifier(resource_id),
            "source_system": source_system,
            "source_url": source_url,
            "catalog_provider": publisher_name,
            "title": title,
            "description": description,
            "keywords": [],  # Removed: no longer extracting from metadata
            "subjects": [],  # Removed: no longer extracting from metadata
            "matched_subjects": matched_subjects,
            "canonical_subjects": canonical_subjects,
            "program_ids": sorted(program_id_set),
            "unmatched_subjects": [],  # Removed: no longer tracking unmatched
            "creator_names": creators,
            "publisher_name": publisher_name,
            "language": language,
            "license_name": license_name,
            "license_url": license_url,
            "publication_date": publication_date,
            "last_updated_at": last_updated_at,
            "scraped_at": scraped_at,
            "bronze_source_path": bronze_source_path,
            "data_quality_score": data_quality_score,
            "dc_xml": dc_xml,  # Store XML content temporarily for batch save
            "dc_xml_path": dc_xml_path,
            "ingested_at": now,
        }
        return metadata

    def _write_tables(self, df: DataFrame) -> None:
        if not self.spark:
            return

        deduped_df = df.dropDuplicates(["resource_uid"]).persist()

        # Silver layer: giữ nguyên tất cả thông tin (denormalized)
        fact_df = deduped_df.select(
            "resource_uid",
            "resource_id",
            "source_system",
            "source_url",
            "catalog_provider",
            "title",
            "description",
            "keywords",
            "subjects",
            "matched_subjects",
            "canonical_subjects",
            "program_ids",
            "unmatched_subjects",
            "creator_names",
            "publisher_name",
            "language",
            "license_name",
            "license_url",
            "bronze_source_path",
            "scraped_at",
            "last_updated_at",
            "publication_date",
            "data_quality_score",
            "dc_xml_path",
            "ingested_at",
        )

        self._replace_table(
            fact_df,
            self.full_table_name,
            partition_columns=["source_system", "DATE(ingested_at)"],
        )

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
                T.StructField("subject_name_en", T.StringType(), True),
                T.StructField("subject_code", T.StringType(), True),
                T.StructField("faculty_id", T.IntegerType(), True),
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

        subjects_df = create_df(self.reference_subject_records, subjects_schema)
        faculties_df = create_df(self.reference_faculty_records, faculties_schema)
        programs_df = create_df(self.reference_program_records)
        program_subject_links_df = create_df(self.reference_program_subject_links_records)

        if subjects_df is not None:
            self._replace_table(subjects_df, self.reference_subjects_table)
        if faculties_df is not None:
            self._replace_table(faculties_df, self.reference_faculties_table)
        if programs_df is not None:
            self._replace_table(programs_df, self.reference_programs_table)
        if program_subject_links_df is not None:
            self._replace_table(program_subject_links_df, self.reference_program_subject_links_table)

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

    def _derive_license(self, record: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        name = self._ensure_str(record.get("license") or record.get("rights"))
        url = self._ensure_str(record.get("license_url") or record.get("rights_url"))
        if not name and record.get("legal_notice"):
            legal = self._ensure_str(record["legal_notice"])
            if legal:
                name = legal[:120]
        return name, url

    def _compute_quality_score(
        self,
        *,
        title: Optional[str],
        description: Optional[str],
        creators: List[str],
        publisher_name: Optional[str],
        language: Optional[str],
        license_name: Optional[str],
        source_url: Optional[str],
    ) -> float:
        checks = [
            bool(title),
            bool(description),
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

    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        """Parse a datetime value from various formats."""
        if value is None:
            return None
        text = self._ensure_str(value)
        if not text:
            return None
        try:
            # Handle year-only format (e.g., "2020")
            if text.isdigit() and len(text) == 4:
                return datetime(int(text), 1, 1)
            # Handle ISO format with Z suffix
            text = text.replace("Z", "+00:00")
            return datetime.fromisoformat(text)
        except Exception:
            return None

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
        url: Optional[str],
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

        return ET.tostring(root, encoding="utf-8").decode("utf-8")

    def _batch_save_dc_xmls(self, df: DataFrame) -> None:
        """Batch save Dublin Core XMLs using foreachPartition to avoid serialization issues."""
        def save_partition(rows):
            """Save XMLs for a partition (runs on executor)."""
            try:
                # Create MinIO client per partition (avoid serialization)
                from minio import Minio
                import os
                from io import BytesIO
                
                endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
                access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
                secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
                bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
                secure = os.getenv("MINIO_SECURE", "0").lower() in {"1", "true", "yes"}
                
                client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
                
                for row in rows:
                    resource_id = row.get("resource_id")
                    dc_xml = row.get("dc_xml")
                    
                    if not resource_id or not dc_xml:
                        continue
                    
                    object_name = f"silver/dc_xml/{resource_id}.xml"
                    xml_bytes = dc_xml.encode('utf-8')
                    
                    client.put_object(
                        bucket_name=bucket,
                        object_name=object_name,
                        data=BytesIO(xml_bytes),
                        length=len(xml_bytes),
                        content_type='application/xml'
                    )
            except Exception as exc:
                # Silent fail: don't break the job if XML save fails
                pass
        
        # Select only needed columns and save
        df.select("resource_id", "dc_xml").rdd.foreachPartition(lambda rows: save_partition([row.asDict() for row in rows]))

def re_split_words(text: str) -> List[str]:
    import re

    return [token for token in re.split(r"[^A-Za-z0-9]+", text) if token]



def main() -> None:
    transformer = SilverTransformer()
    transformer.run()


if __name__ == "__main__":
    main()
