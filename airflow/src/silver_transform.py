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
from typing import Any, Dict, Iterable, List, Optional, Tuple
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


class SilverTransformer:
    def __init__(self) -> None:
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.catalog_name = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.database_name = os.getenv("SILVER_DATABASE", "default")
        self.bronze_input = os.getenv("BRONZE_INPUT", f"s3a://{self.bucket}/bronze/")

        self.resources_curated_table = f"{self.catalog_name}.{self.database_name}.oer_resources_curated"
        self.documents_table = f"{self.catalog_name}.{self.database_name}.oer_documents"
        self.chunks_table = f"{self.catalog_name}.{self.database_name}.oer_chunks"
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
        self.chunk_overlap_chars = int(os.getenv("SILVER_CHUNK_OVERLAP_CHARS", "240"))
        self.chunk_min_chars = int(os.getenv("SILVER_CHUNK_MIN_CHARS", "220"))
        self.max_pages_per_pdf = int(os.getenv("SILVER_MAX_PDF_PAGES", "200"))

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

        self._broadcast_subject_records = self.spark.sparkContext.broadcast(self.reference_subject_records)
        self._broadcast_programs_by_subject = self.spark.sparkContext.broadcast(self.programs_by_subject)

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

        matched_subjects = self.subject_matcher.match(title, description, top_k=5)
        program_ids = sorted(
            {
                pid
                for subj in matched_subjects
                for pid in self.programs_by_subject.get(int(subj["subject_id"]), [])
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
            "data_quality_score": data_quality_score,
            "ingested_at": now,
        }

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
                T.StructField("data_quality_score", T.DoubleType(), True),
                T.StructField("ingested_at", T.TimestampType(), False),
            ]
        )

    def _build_base_df(self, bronze_df: DataFrame) -> DataFrame:
        schema = self._base_schema()
        normalized_rows: List[Dict[str, Any]] = []
        for row in bronze_df.toLocalIterator():
            normalized = self._normalize_record_python(row.asDict(recursive=True))
            if normalized is not None:
                normalized_rows.append(normalized)
        base_df = self.spark.createDataFrame(normalized_rows, schema=schema)

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

        existing_df = self.spark.table(self.resources_curated_table).select(
            "resource_uid",
            F.col("scraped_at").alias("existing_scraped_at"),
            F.col("last_updated_at").alias("existing_last_updated_at"),
            F.col("ingested_at").alias("existing_ingested_at"),
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

        enriched_rows = [self._enrich_document_record(row.asDict(recursive=True)) for row in docs_base.toLocalIterator()]
        documents_df = self.spark.createDataFrame(enriched_rows, schema=docs_schema)
        return documents_df.dropDuplicates(["asset_uid"])

    def _empty_asset_uid_df(self) -> DataFrame:
        return self.spark.createDataFrame([], schema=T.StructType([T.StructField("asset_uid", T.StringType(), False)]))

    def _filter_changed_documents(self, documents_df: DataFrame) -> DataFrame:
        if documents_df.rdd.isEmpty():
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
        current_assets = documents_df.select("asset_uid").dropDuplicates(["asset_uid"]) if not documents_df.rdd.isEmpty() else self._empty_asset_uid_df()
        return existing_assets.join(current_assets, on="asset_uid", how="left_anti")

    def _enrich_document_record(self, row: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.utcnow()
        asset_path = clean_scalar(row.get("asset_path")) or ""
        mime_type = mimetypes.guess_type(asset_path)[0] or "application/pdf"
        etag = None
        size_bytes = None
        last_modified = None

        if self.minio_client and asset_path:
            try:
                stat = self.minio_client.stat_object(self.bucket, asset_path)
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

    def _extract_pdf_pages(self, asset_path: str) -> List[Tuple[int, int, str]]:
        if not self.minio_client or not PYPDF_AVAILABLE or not asset_path:
            return []
        response = None
        pages: List[Tuple[int, int, str]] = []
        try:
            response = self.minio_client.get_object(self.bucket, asset_path)
            pdf_bytes = response.read()
            reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
            max_pages = min(len(reader.pages), self.max_pages_per_pdf)
            for idx in range(max_pages):
                text = self._normalize_pdf_text(clean_scalar(reader.pages[idx].extract_text()) or "")
                if not text:
                    continue
                for chunk_index, chunk in enumerate(self._chunk_text_smart(text), start=1):
                    if chunk:
                        page_no = idx + 1
                        pages.append((page_no, chunk_index, chunk))
            return pages
        except Exception:
            return []
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

    def _split_long_segment(self, text: str) -> List[str]:
        chunks: List[str] = []
        text = text.strip()
        if not text:
            return chunks

        overlap = min(max(self.chunk_overlap_chars, 0), max(self.chunk_max_chars // 2, 0))
        step = max(1, self.chunk_max_chars - overlap)
        start = 0
        while start < len(text):
            end = min(start + self.chunk_max_chars, len(text))
            if end < len(text):
                window_start = min(end, start + max(self.chunk_min_chars, self.chunk_max_chars // 2))
                split_pos = max(
                    text.rfind("\n", window_start, end),
                    text.rfind(". ", window_start, end),
                    text.rfind("? ", window_start, end),
                    text.rfind("! ", window_start, end),
                )
                if split_pos > start:
                    end = split_pos + 1

            chunk = text[start:end].strip()
            if chunk and (len(chunk) >= self.chunk_min_chars or end == len(text)):
                chunks.append(chunk)
            if end >= len(text):
                break
            start = start + step if end <= start else max(start + 1, end - overlap)
        return chunks

    def _chunk_text_smart(self, text: str) -> List[str]:
        paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p and p.strip()]
        if not paragraphs:
            return self._split_long_segment(text)

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
            if len(para) > self.chunk_max_chars:
                flush_current()
                chunks.extend(self._split_long_segment(para))
                continue

            projected = current_len + (2 if current_parts else 0) + len(para)
            if projected <= self.chunk_max_chars:
                current_parts.append(para)
                current_len = projected
            else:
                flush_current()
                current_parts.append(para)
                current_len = len(para)

        flush_current()
        return chunks if chunks else self._split_long_segment(text)

    def _build_chunks_df(self, documents_df: DataFrame) -> DataFrame:
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
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )

        chunk_records: List[Dict[str, Any]] = []
        for row in documents_df.toLocalIterator():
            chunk_records.extend(self._chunk_document_record(row.asDict(recursive=True)))
        chunks_df = self.spark.createDataFrame(chunk_records, schema=chunk_schema)
        return chunks_df.dropDuplicates(["chunk_id"])

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
                    "updated_at": now,
                }
            )
        return records

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
        if not self._table_exists(table_name) or asset_uid_df.rdd.isEmpty():
            return
        asset_uids = [r["asset_uid"] for r in asset_uid_df.select("asset_uid").dropDuplicates(["asset_uid"]).collect() if r["asset_uid"]]
        if not asset_uids:
            return

        batch_size = int(os.getenv("SILVER_DELETE_BATCH_SIZE", "200"))
        for i in range(0, len(asset_uids), batch_size):
            batch = asset_uids[i: i + batch_size]
            escaped = [uid.replace("'", "''") for uid in batch]
            in_clause = ", ".join([f"'{uid}'" for uid in escaped])
            self.spark.sql(f"DELETE FROM {table_name} WHERE asset_uid IN ({in_clause})")

    # -----------------------------
    # Orchestration
    # -----------------------------
    def run_reference_bootstrap_only(self) -> None:
        self.run_reference_bootstrap()
        print("Reference bootstrap completed.")

    def run(self) -> None:
        if self.run_reference_bootstrap_enabled:
            self.run_reference_bootstrap()

        print(f"Reading bronze input from: {self.bronze_input}")
        bronze_df = self._read_bronze()
        if bronze_df.rdd.isEmpty():
            print("No bronze records found; exiting.")
            return

        print("Building normalized silver base dataset...")
        base_df = self._build_base_df(bronze_df)

        print("Applying incremental resource filter...")
        incremental_base_df = self._filter_incremental_resources(base_df).persist()
        if incremental_base_df.rdd.isEmpty():
            print("No new or updated resources after incremental filter; exiting.")
            incremental_base_df.unpersist(False)
            return

        print("Building silver resources_curated...")
        resources_curated_df = self._build_resources_curated_df(incremental_base_df).persist()
        print("Building silver documents...")
        documents_df = self._build_documents_df(incremental_base_df).persist()
        print("Detecting changed/deleted assets for incremental chunking...")
        changed_assets_df = self._filter_changed_documents(documents_df).persist()
        deleted_assets_df = self._find_deleted_assets(incremental_base_df, documents_df).persist()
        affected_assets_df = changed_assets_df.unionByName(deleted_assets_df).dropDuplicates(["asset_uid"]).persist()

        changed_documents_df = (
            documents_df.join(changed_assets_df, on="asset_uid", how="inner").persist()
            if not changed_assets_df.rdd.isEmpty()
            else documents_df.limit(0).persist()
        )
        print("Building silver chunks...")
        chunks_df = self._build_chunks_df(changed_documents_df).persist()

        print("Writing Iceberg tables...")
        self._merge_into(resources_curated_df, self.resources_curated_table, ["resource_uid"], partition_columns=["source_system", "days(ingested_at)"])
        # Keep legacy table updated for current Gold compatibility.
        self._merge_into(resources_curated_df, self.resources_table, ["resource_uid"], partition_columns=["source_system", "days(ingested_at)"])
        self._delete_by_asset_uid(self.chunks_table, affected_assets_df)
        self._delete_by_asset_uid(self.documents_table, deleted_assets_df)
        self._merge_into(documents_df, self.documents_table, ["asset_uid"], partition_columns=["source_system", "days(updated_at)"])
        if not chunks_df.rdd.isEmpty():
            self._merge_into(chunks_df, self.chunks_table, ["chunk_id"], partition_columns=["days(updated_at)"])

        for df in [chunks_df, changed_documents_df, affected_assets_df, deleted_assets_df, changed_assets_df, documents_df, resources_curated_df, incremental_base_df]:
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
