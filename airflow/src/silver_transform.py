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
from typing import Any, Dict, Iterable, List, Optional, Tuple, Set, Union
import unicodedata
import re
from uuid import uuid4

try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

# Semantic matcher for subject matching
try:
    # Add src directory to path for imports
    import sys
    src_dir = "/opt/airflow/src"
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    from semantic_matcher import SemanticMatcher
    SEMANTIC_MATCHER_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import SemanticMatcher: {e}")
    SemanticMatcher = None
    SEMANTIC_MATCHER_AVAILABLE = False

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
                    self.reference_path = Path("/opt/airflow/reference")
            else:
                print("Warning: REFERENCE_DATA_URI points to MinIO but minio package not installed; using local fallback")
                self.reference_path = Path("/opt/airflow/reference")
        elif reference_uri:
            self.reference_path = Path(reference_uri)
        else:
            self.reference_path = Path("/opt/airflow/reference")

        self._load_reference_data()
        self._prepare_subject_mappings()
        self._write_reference_dimensions()

        # Initialize semantic matcher for subject matching
        self.semantic_matcher = None
        if SEMANTIC_MATCHER_AVAILABLE:
            try:
                reference_path = str(self.reference_path) if self.reference_path else "/opt/airflow/reference"
                self.semantic_matcher = SemanticMatcher(
                    reference_path=reference_path,
                    threshold=0.45,
                    top_k=3
                )
                self.semantic_matcher.initialize()
                print(f"Semantic matcher initialized with model at {reference_path}")
            except Exception as e:
                print(f"Warning: Failed to initialize semantic matcher: {e}")
                self.semantic_matcher = None
        else:
            print("Warning: Semantic matcher not available, falling back to keyword matching")

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
                T.StructField("title", T.StringType(), True),
                T.StructField("description", T.StringType(), True),
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
                T.StructField("program_ids", T.ArrayType(T.IntegerType()), True),
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
        Match subjects using semantic similarity (sentence embeddings) ONLY.
        
        Uses SemanticMatcher with all-MiniLM-L6-v2 model for accurate matching.
        This avoids false positives like "Learning Management System" → "Machine Learning".
        
        No fallback to keyword matching - semantic model is the only method.
        
        Returns:
            Tuple of (matched_subjects, program_ids)
        """
        if not title:
            return [], []
        
        # Use semantic matcher only
        if not self.semantic_matcher:
            print("Warning: Semantic matcher not available, no subject matching will be performed")
            return [], []
        
        try:
            matches = self.semantic_matcher.match(title, description)
            
            # Convert to expected format with matched_text field
            matched_subjects = []
            for m in matches:
                matched_subjects.append({
                    "subject_id": m.get("subject_id"),
                    "subject_name": m.get("subject_name"),
                    "subject_name_en": m.get("subject_name_en"),
                    "subject_code": m.get("subject_code"),
                    "similarity": m.get("similarity", 0.0),
                    "matched_text": m.get("subject_name_en") or m.get("subject_name"),
                })
            
            # Get program IDs from matched subjects
            program_ids = sorted({
                pid 
                for subj in matched_subjects 
                for pid in self.programs_by_subject.get(subj.get("subject_id"), [])
            })
            
            return matched_subjects, program_ids
            
        except Exception as e:
            print(f"Semantic matching failed: {e}")
            return [], []

    def _create_spark_session(self) -> SparkSession:
        # Use local JARs instead of downloading from Maven
        jars_dir = "/opt/airflow/jars"
        local_jars = ",".join([
            f"{jars_dir}/iceberg-spark-runtime-3.5_2.12-1.9.2.jar",
            f"{jars_dir}/hadoop-aws-3.3.4.jar",
            f"{jars_dir}/aws-java-sdk-bundle-1.12.262.jar"
        ])
        
        # Print JAR paths for debugging
        print(f"[Spark] Using local JARs: {local_jars}")
        
        session = (
            SparkSession.builder.appName("OER-Silver-Dublin-Core")
            .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))  # Use Spark cluster
            .config("spark.jars", local_jars)  # Load JARs into classpath
            .config("spark.driver.extraClassPath", local_jars)  # Add to driver classpath
            .config("spark.executor.extraClassPath", local_jars)  # Add to executor classpath
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.%s" % self.catalog_name, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.%s.type" % self.catalog_name, "hadoop")
            .config("spark.sql.catalog.%s.warehouse" % self.catalog_name, f"s3a://{self.bucket}/silver/")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))  # Reduced for local mode
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
        # Clear any cached data from previous runs to prevent cross-contamination
        # This is critical when multiple tasks run in parallel on the same Spark cluster
        if self.spark:
            try:
                self.spark.catalog.clearCache()
                print("[Cache] Cleared Spark catalog cache to ensure fresh data load")
            except Exception as e:
                print(f"[Cache] Warning: Could not clear cache (Spark may be restarting): {e}")
                # Continue execution - cache clearing is nice-to-have, not critical
        
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

        print(f"[Bronze] Loading data from paths: {candidate_paths}")
        
        try:
            df = (
                self.spark.read.option("recursiveFileLookup", "true")
                .option("multiLine", "true")
                .json(candidate_paths)
                .withColumn("bronze_source_path", F.input_file_name())
            )
            record_count = df.count()
            print(f"[Bronze] Loaded {record_count} raw records from {len(candidate_paths)} path(s)")
            
            # Show sample of source paths for verification
            if record_count > 0:
                source_files = df.select("bronze_source_path").distinct().limit(3).collect()
                print(f"[Bronze] Sample source files: {[row.bronze_source_path for row in source_files]}")
            
            return df
        except Exception as exc:
            print(f"[Bronze] Failed to load bronze payloads: {exc}")
            return None

    def _resolve_bronze_paths(self) -> List[str]:
        """
        Resolve bronze paths - ONLY include OER sources (mit_ocw, openstax, otl).
        Exclude textbooks and other reference data.
        """
        hint = os.getenv("BRONZE_INPUT")
        if hint:
            paths = [value.strip() for value in hint.split(",") if value.strip()]
            print(f"[Paths] Using BRONZE_INPUT env var: {paths}")
            return paths

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
        # Pre-compute subject matches before Spark operations to avoid serialization issues
        total_count = bronze_df.count()
        
        # Process in batches to avoid OOM
        batch_size = 300  # Process 300 records at a time
        subject_matches_cache = {}
        
        if self.semantic_matcher:
            print(f" Starting semantic matching for {total_count} records (batch size: {batch_size})...")
            print(f" Using model: all-MiniLM-L6-v2 (threshold=0.45, top_k=3)")
            
            matched_count = 0
            total_subjects_matched = 0
            processed_count = 0
            
            # Collect ALL records once (they're needed for matching anyway)
            print(f"  Collecting {total_count} records for semantic matching...")
            all_records = bronze_df.select("title", "description").collect()
            print(f"  Collected {len(all_records)} records successfully")
            
            # Process in batches to show progress and avoid memory spikes
            for batch_start in range(0, len(all_records), batch_size):
                batch_end = min(batch_start + batch_size, len(all_records))
                batch_records = all_records[batch_start:batch_end]
                batch_num = (batch_start // batch_size) + 1
                
                print(f"  Processing batch {batch_num}: records {batch_start+1}-{batch_end}")
                
                for row in batch_records:
                    title = row.title if hasattr(row, 'title') else None
                    description = row.description if hasattr(row, 'description') else None
                    if title:
                        cache_key = f"{title}|{description or ''}"
                        
                        # Skip if already in cache (duplicate titles)
                        if cache_key in subject_matches_cache:
                            continue
                            
                        try:
                            matches = self.semantic_matcher.match(title, description)
                            matched_subjects = []
                            for m in matches:
                                matched_subjects.append({
                                    "subject_id": m.get("subject_id"),
                                    "subject_name": m.get("subject_name"),
                                    "subject_name_en": m.get("subject_name_en"),
                                    "subject_code": m.get("subject_code"),
                                    "similarity": m.get("similarity", 0.0),
                                    "matched_text": m.get("subject_name_en") or m.get("subject_name"),
                                })
                            program_ids = sorted({
                                pid 
                                for subj in matched_subjects 
                                for pid in self.programs_by_subject.get(subj.get("subject_id"), [])
                            })
                            subject_matches_cache[cache_key] = (matched_subjects, program_ids)
                            
                            if matched_subjects:
                                matched_count += 1
                                total_subjects_matched += len(matched_subjects)
                                
                        except Exception as e:
                            print(f" Error matching subjects for '{title[:50]}': {e}")
                            subject_matches_cache[cache_key] = ([], [])
                
                processed_count = batch_end
                progress_pct = (processed_count / total_count) * 100
                print(f"  Batch complete. Progress: {processed_count}/{total_count} ({progress_pct:.1f}%) - Total matched: {matched_count}")
            
            # Final summary
            match_rate = (matched_count / total_count * 100) if total_count > 0 else 0
            avg_subjects = (total_subjects_matched / matched_count) if matched_count > 0 else 0
            print(f" Semantic matching completed:")
            print(f"   - Total resources: {total_count}")
            print(f"   - Resources with matches: {matched_count} ({match_rate:.1f}%)")
            print(f"   - Total subjects matched: {total_subjects_matched}")
            print(f"   - Average subjects per resource: {avg_subjects:.2f}")
        else:
            print("  Semantic matcher not available, all resources will have empty matched_subjects")
                        
        # Broadcast the cache to all executors
        subject_matches_broadcast = self.spark.sparkContext.broadcast(subject_matches_cache)
        
        # Create a standalone mapper function that doesn't reference self or semantic_matcher
        # This function will be serialized and sent to executors
        def process_record_standalone(row_dict: Dict[str, Any], subject_cache: Dict) -> Optional[Dict[str, Any]]:
            """Standalone function for executor - no class references, no external imports"""
            import hashlib
            from datetime import datetime
            from typing import Optional, Dict, Any, List, Set
            import unicodedata
            import re
            
            # Helper functions (inlined to avoid serialization issues)
            def ensure_str(value):
                if value is None:
                    return None
                if isinstance(value, str):
                    return value.strip() or None
                return str(value).strip() or None
            
            def ensure_title(record):
                candidates = [
                    record.get("title"),
                    record.get("course_title"),
                    record.get("book_title"),
                    record.get("resource_title"),
                ]
                for c in candidates:
                    if c and isinstance(c, str) and c.strip():
                        return c.strip()
                return None
            
            def clean_string_list(value):
                if value is None:
                    return []
                if isinstance(value, str):
                    return [value.strip()] if value.strip() else []
                if isinstance(value, (list, tuple)):
                    return [ensure_str(x) for x in value if ensure_str(x)]
                return []
            
            def detect_source_system(record):
                for key in ["source_system", "source", "provider"]:
                    val = record.get(key)
                    if val and isinstance(val, str):
                        return val.strip().lower()
                bronze_path = record.get("bronze_source_path", "")
                for known in ["mit_ocw", "openstax", "otl", "oer_commons"]:
                    if known in bronze_path.lower():
                        return known
                return "unknown"
            
            def select_identifier(record, source_system):
                candidates = [
                    record.get("resource_id"),
                    record.get("course_id"),
                    record.get("id"),
                    record.get("uid"),
                ]
                for c in candidates:
                    if c and isinstance(c, (str, int)):
                        return f"{source_system}_{c}"
                title = ensure_title(record)
                if title:
                    safe = re.sub(r"[^a-z0-9]+", "_", title.lower()[:50])
                    return f"{source_system}_{safe}"
                return None
            
            def hash_identifier(resource_id):
                if not resource_id:
                    return None
                normalized = unicodedata.normalize("NFKD", str(resource_id))
                return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
            
            def ensure_language(value):
                if not value:
                    return "en"
                val_str = str(value).strip().lower()
                if val_str in ["en", "eng", "english"]:
                    return "en"
                if val_str in ["vi", "vie", "vietnamese"]:
                    return "vi"
                return val_str[:2] if val_str else "en"
            
            def derive_publisher(record, source_system):
                pub = record.get("publisher")
                if pub and isinstance(pub, str) and pub.strip():
                    return pub.strip()
                source_map = {
                    "mit_ocw": "MIT OpenCourseWare",
                    "openstax": "OpenStax",
                    "otl": "Open Textbook Library",
                    "oer_commons": "OER Commons",
                }
                return source_map.get(source_system, "Unknown")
            
            def derive_license(record):
                lic = record.get("license")
                if lic and isinstance(lic, str):
                    lic_lower = lic.lower()
                    if "cc" in lic_lower or "creative commons" in lic_lower:
                        return (lic.strip(), None)
                    if lic_lower.startswith("http"):
                        return ("Creative Commons", lic.strip())
                return ("Unknown", None)
            
            def parse_datetime(value):
                if value is None:
                    return None
                from datetime import datetime
                if isinstance(value, datetime):
                    return value
                if isinstance(value, (int, float)):
                    try:
                        return datetime(int(value), 1, 1)
                    except:
                        return None
                if isinstance(value, str):
                    for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                        try:
                            return datetime.strptime(value.strip(), fmt)
                        except:
                            continue
                return None
            
            def compute_quality_score(title, description, creators, publisher_name, language, license_name, source_url):
                score = 0.0
                if title:
                    score += 1.5
                if description and len(description) > 50:
                    score += 2.0
                if creators:
                    score += 1.5
                if publisher_name and publisher_name != "Unknown":
                    score += 1.0
                if language and language != "unknown":
                    score += 0.5
                if license_name and license_name != "Unknown":
                    score += 1.5
                if source_url:
                    score += 2.0
                return round(min(score, 10.0), 2)
            
            # Main processing logic
            source_system = detect_source_system(row_dict)
            resource_id = select_identifier(row_dict, source_system)
            if not resource_id:
                return None
            
            now = datetime.utcnow()
            source_url = ensure_str(row_dict.get("url") or row_dict.get("link"))
            title = ensure_title(row_dict)
            description = ensure_str(row_dict.get("description"))
            creators = clean_string_list(row_dict.get("instructors") or row_dict.get("authors") or row_dict.get("creators"))
            
            # Get subject matches from cache (pre-computed on driver)
            cache_key = f"{title}|{description or ''}"
            matched_subjects, matched_program_ids_list = subject_cache.get(cache_key, ([], []))
            program_id_set: Set[int] = set(matched_program_ids_list)
            
            language = ensure_language(row_dict.get("language"))
            publisher_name = derive_publisher(row_dict, source_system)
            license_name, license_url = derive_license(row_dict)
            
            publication_date = parse_datetime(row_dict.get("publication_date") or row_dict.get("year"))
            scraped_at = parse_datetime(row_dict.get("scraped_at"))
            last_updated_at = parse_datetime(row_dict.get("last_updated_at") or row_dict.get("updated_at") or scraped_at)
            bronze_source_path = ensure_str(row_dict.get("bronze_source_path"))
            
            data_quality_score = compute_quality_score(
                title=title,
                description=description,
                creators=creators,
                publisher_name=publisher_name,
                language=language,
                license_name=license_name,
                source_url=source_url,
            )
            
            metadata: Dict[str, Any] = {
                "resource_id": resource_id,
                "resource_uid": hash_identifier(resource_id),
                "source_system": source_system,
                "source_url": source_url,
                "title": title,
                "description": description,
                "matched_subjects": matched_subjects,
                "program_ids": sorted(program_id_set),
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
                "ingested_at": now,
            }
            return metadata
        
        # Map using the standalone function with broadcasted cache
        normalized_rdd = bronze_df.rdd.map(
            lambda row: process_record_standalone(row.asDict(recursive=True), subject_matches_broadcast.value)
        ).filter(lambda item: item is not None)
        
        if normalized_rdd.isEmpty():
            return None
        
        # Convert to Rows for DataFrame creation
        normalized_rdd_rows = normalized_rdd.map(lambda d: Row(**d))
        return self.spark.createDataFrame(normalized_rdd_rows, schema=self.output_schema)

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
            "title",
            "description",
            "matched_subjects",
            "program_ids",
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
            "ingested_at",
        )

        self._replace_table(
            fact_df,
            self.full_table_name,
            partition_columns=["source_system", "DATE(ingested_at)"],
        )

        deduped_df.unpersist(False)

    def _replace_table(
        self, 
        df: DataFrame, 
        table_name: str, 
        partition_columns: Optional[List[str]] = None,
        merge_key: Optional[Union[str, List[str]]] = "resource_uid"
    ) -> None:
        """Write or merge data into Iceberg table using MERGE for incremental updates.
        
        Args:
            df: DataFrame to write
            table_name: Target Iceberg table name
            partition_columns: Optional partitioning columns
            merge_key: Column name(s) to use for MERGE ON condition. 
                      Can be single string or list of strings for composite keys.
                      Default: "resource_uid"
        """
        row_count = df.count()
        
        # Check if table exists
        try:
            existing_df = self.spark.table(table_name)
            table_exists = True
            existing_count = existing_df.count()
            print(f"Table {table_name} exists with {existing_count} records, will merge {row_count} new records")
        except Exception:
            table_exists = False
            print(f"Table {table_name} does not exist, will create with {row_count} records")

        if not table_exists:
            # Create new table with partitioning
            temp_view = f"temp_{uuid4().hex}"
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
            print(f" Created {table_name} with {row_count} records")
        else:
            # Use MERGE for incremental updates (upsert based on merge_key)
            # This preserves existing records and only updates/inserts changed ones
            temp_view = f"temp_{uuid4().hex}"
            df.createOrReplaceTempView(temp_view)
            
            # Build ON condition for single or composite keys
            if isinstance(merge_key, str):
                on_condition = f"target.{merge_key} = source.{merge_key}"
            else:  # List of keys for composite primary key
                on_conditions = [f"target.{key} = source.{key}" for key in merge_key]
                on_condition = " AND ".join(on_conditions)
            
            # Merge logic: UPDATE existing records, INSERT new ones
            merge_sql = f"""
                MERGE INTO {table_name} AS target
                USING {temp_view} AS source
                ON {on_condition}
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
            """
            
            try:
                self.spark.sql(merge_sql)
                self.spark.catalog.dropTempView(temp_view)
                
                # Count records after merge
                final_count = self.spark.table(table_name).count()
                new_records = final_count - existing_count
                print(f" Merged into {table_name}: {new_records} new records (total: {final_count})")
            except Exception as e:
                print(f"  MERGE failed, falling back to append mode: {e}")
                self.spark.catalog.dropTempView(temp_view)
                
                # Fallback: Use append mode with deduplication
                df.writeTo(table_name).append()
                print(f" Appended {row_count} records to {table_name} (fallback mode)")

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

        # Reference tables: use OVERWRITE mode (master data, not incremental)
        # Deduplication ensures data quality from source files
        if subjects_df is not None:
            subjects_df = subjects_df.dropDuplicates(["subject_id"])
            row_count = subjects_df.count()
            print(f"Overwriting reference_subjects with {row_count} records")
            subjects_df.writeTo(self.reference_subjects_table).createOrReplace()
            
        if faculties_df is not None:
            faculties_df = faculties_df.dropDuplicates(["faculty_id"])
            row_count = faculties_df.count()
            print(f"Overwriting reference_faculties with {row_count} records")
            faculties_df.writeTo(self.reference_faculties_table).createOrReplace()
            
        if programs_df is not None:
            programs_df = programs_df.dropDuplicates(["program_id"])
            row_count = programs_df.count()
            print(f"Overwriting reference_programs with {row_count} records")
            programs_df.writeTo(self.reference_programs_table).createOrReplace()
            
        if program_subject_links_df is not None:
            program_subject_links_df = program_subject_links_df.dropDuplicates(["program_id", "subject_id"])
            row_count = program_subject_links_df.count()
            print(f"Overwriting reference_program_subject_links with {row_count} records")
            program_subject_links_df.writeTo(self.reference_program_subject_links_table).createOrReplace()


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


def re_split_words(text: str) -> List[str]:
    import re

    return [token for token in re.split(r"[^A-Za-z0-9]+", text) if token]



def main() -> None:
    transformer = SilverTransformer()
    transformer.run()


if __name__ == "__main__":
    main()