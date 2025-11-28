#!/usr/bin/env python3
"""
Gold -> Elasticsearch Sync
=========================

Builds search-ready documents from Gold layer tables and indexes them into Elasticsearch.
This job can run inside or outside Airflow to keep retrieval workloads decoupled from the
core ETL while reusing the curated gold datasets.
"""

from __future__ import annotations

import os
import json
from datetime import date, datetime
from io import BytesIO
from decimal import Decimal
from pathlib import PurePosixPath
from typing import Any, Dict, Iterator, List, Optional, Tuple

from elasticsearch import Elasticsearch, helpers

from minio import Minio
import pdfplumber
import PyPDF2


try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except ImportError:  # pragma: no cover - handled at runtime
    SparkSession = DataFrame = F = T = None  # type: ignore

SPARK_AVAILABLE = SparkSession is not None
ELASTICSEARCH_AVAILABLE = Elasticsearch is not None
MINIO_AVAILABLE = Minio is not None


class OERElasticsearchIndexer:
    """Sync Gold layer OER resources into an Elasticsearch index."""

    def __init__(self) -> None:
        # Kiểm tra các thư viện cần thiết
        if not SPARK_AVAILABLE:
            raise RuntimeError("PySpark is required to export Gold data to Elasticsearch")
        if not ELASTICSEARCH_AVAILABLE:
            raise RuntimeError(
                "The 'elasticsearch' Python package is required. "
                "Install it via airflow/requirements.txt."
            )

        # Cấu hình kết nối MinIO và Iceberg catalogs
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.silver_catalog = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.silver_database = os.getenv("SILVER_DATABASE", "default")
        self.gold_catalog = os.getenv("ICEBERG_GOLD_CATALOG", "gold")
        self.gold_database = os.getenv("GOLD_DATABASE", "analytics")

        # Định nghĩa các bảng Gold layer cần đồng bộ
        self.dim_programs_table = f"{self.gold_catalog}.{self.gold_database}.dim_programs"
        self.dim_sources_table = f"{self.gold_catalog}.{self.gold_database}.dim_sources"
        self.dim_languages_table = f"{self.gold_catalog}.{self.gold_database}.dim_languages"
        self.dim_date_table = f"{self.gold_catalog}.{self.gold_database}.dim_date"
        self.dim_oer_resources_table = f"{self.gold_catalog}.{self.gold_database}.dim_oer_resources"
        self.fact_oer_resources_table = f"{self.gold_catalog}.{self.gold_database}.fact_oer_resources"

        # Cấu hình Elasticsearch
        self.es_host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        self.es_user = os.getenv("ELASTICSEARCH_USER")
        self.es_password = os.getenv("ELASTICSEARCH_PASSWORD")
        self.index_name = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
        self.batch_size = int(os.getenv("ELASTICSEARCH_BATCH_SIZE", "500"))  # Số documents/batch
        self.request_timeout = int(os.getenv("ELASTICSEARCH_TIMEOUT", "120"))  # Timeout 2 phút
        self.recreate_index = os.getenv("ELASTICSEARCH_RECREATE", "0").lower() in {
            "1",
            "true",
            "yes",
        }

        # Kết nối đến Elasticsearch với authentication
        http_auth = None
        if self.es_user and self.es_password:
            http_auth = (self.es_user, self.es_password)

        self.es = Elasticsearch(
            hosts=[self.es_host],
            basic_auth=http_auth,
            verify_certs=self.es_host.startswith("https"),
        )

        # Cấu hình PDF indexing (có index nội dung PDF hay không)
        self.index_pdf_content = os.getenv("ELASTICSEARCH_INDEX_PDF_CONTENT", "1").lower() in {
            "1",
            "true",
            "yes",
        }
        # PDF Processing Limits (for nested structure)
        self.max_pdf_texts = int(os.getenv("ELASTICSEARCH_MAX_PDF_PER_RESOURCE", "3"))  # Max 3 PDFs per resource
        self.max_pdf_pages = int(os.getenv("ELASTICSEARCH_PDF_MAX_PAGES", "50"))  # Max pages to parse per PDF
        self.max_pdf_page_chars = int(os.getenv("ELASTICSEARCH_PDF_PAGE_CHARS", "2000"))  # Max chars per page chunk
        
        # Kết nối MinIO để lấy PDF content (nếu cần)
        self.minio_client = self._create_minio_client() if self.index_pdf_content else None
        
        # Khởi tạo cache để tối ưu hiệu suất
        self._bronze_record_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}  # Cache Bronze JSON
        self._pdf_listing_cache: Dict[str, List[str]] = {}  # Cache danh sách PDFs
        self._pdf_text_cache: Dict[str, str] = {}  # Cache nội dung PDF đã extract

        # Khởi tạo Spark session để đọc Gold layer (Iceberg tables)
        self.spark = self._create_spark_session()

    # Spark
    def _create_spark_session(self) -> SparkSession:
        """Tạo Spark session để đọc dữ liệu từ Gold layer (Iceberg tables)"""
        # Sử dụng local JARs thay vì download từ Maven (tránh network issues)
        jars_dir = "/opt/airflow/jars"
        local_jars = ",".join([
            f"{jars_dir}/iceberg-spark-runtime-3.5_2.12-1.4.2.jar",  # Iceberg support
            f"{jars_dir}/hadoop-aws-3.3.4.jar",  # S3A filesystem
            f"{jars_dir}/aws-java-sdk-bundle-1.12.262.jar"  # AWS SDK
        ])
        
        print(f"[Spark] Using local JARs: {local_jars}")
        
        # Tạo Spark session với các cấu hình tối ưu
        session = (
            SparkSession.builder.appName("OER-Gold-Elasticsearch")
            .master(os.getenv("SPARK_MASTER", "local[2]"))  # Chạy local với 2 cores
            .config("spark.jars", local_jars)  # Load JARs vào classpath
            .config("spark.driver.extraClassPath", local_jars)  # Classpath cho driver
            .config("spark.executor.extraClassPath", local_jars)  # Classpath cho executor
            # Cấu hình Iceberg extensions
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            # Cấu hình Silver catalog (Iceberg)
            .config(
                f"spark.sql.catalog.{self.silver_catalog}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{self.silver_catalog}.type", "hadoop")
            .config(
                f"spark.sql.catalog.{self.silver_catalog}.warehouse",
                f"s3a://{self.bucket}/silver/",
            )
            # Cấu hình Gold catalog (Iceberg) - Đây là catalog chính để đọc dữ liệu
            .config(
                f"spark.sql.catalog.{self.gold_catalog}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{self.gold_catalog}.type", "hadoop")
            .config(
                f"spark.sql.catalog.{self.gold_catalog}.warehouse",
                f"s3a://{self.bucket}/gold/",
            )
            # Cấu hình S3A filesystem để kết nối MinIO
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")  # MinIO style
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # HTTP (không HTTPS)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # Cấu hình performance tuning
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))  # Giảm partitions cho local mode
            .config("spark.sql.adaptive.enabled", "true")  # Adaptive Query Execution
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))  # RAM cho driver
            .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))  # RAM cho executor
            .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAXRESULTSIZE", "2g"))  # Max result size
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("WARN")
        return session

    # MinIO Utils
    def _create_minio_client(self) -> Optional[Minio]:
        """Tạo MinIO client để lấy PDF content từ Bronze layer"""
        if not MINIO_AVAILABLE:
            print("[Warning] MinIO library not available; PDF indexing disabled")
            self.index_pdf_content = False
            return None

        try:
            # Cấu hình kết nối MinIO
            endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
            endpoint = endpoint.replace("https://", "").replace("http://", "")  # Loại bỏ protocol
            access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
            secure = os.getenv("MINIO_SECURE", "0").lower() in {"1", "true", "yes"}  # HTTP/HTTPS
            client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
            return client
        except Exception as exc:
            print(f"[Warning] Unable to initialize MinIO client: {exc}")
            self.index_pdf_content = False
            return None

    def _parse_s3_uri(self, uri: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not uri:
            return None, None
        cleaned = uri.replace("s3a://", "").replace("s3://", "")
        if "/" not in cleaned:
            return cleaned, ""
        bucket, key = cleaned.split("/", 1)
        return bucket, key

    def _load_bronze_records(self, bronze_path: str) -> Dict[str, Dict[str, Any]]:
        """Load Bronze JSON file và tạo mapping từ resource_id -> record"""
        # Kiểm tra cache trước (tránh đọc lại file nhiều lần)
        if bronze_path in self._bronze_record_cache:
            return self._bronze_record_cache[bronze_path]

        mapping: Dict[str, Dict[str, Any]] = {}
        if not self.minio_client or not bronze_path:
            self._bronze_record_cache[bronze_path] = mapping
            return mapping

        # Parse S3 URI (ví dụ: s3a://bucket/bronze/mit_ocw/data.json)
        bucket, object_name = self._parse_s3_uri(bronze_path)
        if not bucket or not object_name:
            self._bronze_record_cache[bronze_path] = mapping
            return mapping

        # Download Bronze JSON file từ MinIO
        response = None
        try:
            response = self.minio_client.get_object(bucket, object_name)
            raw_bytes = response.read()
            payload = json.loads(raw_bytes.decode("utf-8"))
        except Exception as exc:
            print(f"[Warning] Unable to read bronze file {bronze_path}: {exc}")
            payload = []
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass
                try:
                    response.release_conn()
                except Exception:
                    pass

        if isinstance(payload, list):
            for record in payload:
                if not isinstance(record, dict):
                    continue
                identifiers: List[str] = []
                for key in ("id", "resource_id", "course_id", "url"):
                    value = record.get(key)
                    if value:
                        identifiers.append(str(value))
                if not identifiers:
                    continue
                for identifier in identifiers:
                    mapping[identifier] = record

        self._bronze_record_cache[bronze_path] = mapping
        return mapping

    def _get_bronze_record(self, bronze_path: Optional[str], resource_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Lấy Bronze record để extract metadata (course_id, url) cho việc tìm PDF path"""
        if not bronze_path or not resource_id:
            return None
        records = self._load_bronze_records(bronze_path)
        if not records:
            return None
        rec = records.get(resource_id) or records.get(str(resource_id).strip())
        return rec

    def _bundle_from_minio(
        self,
        source_system: Optional[str],
        record: Optional[Dict[str, Any]],
        resource_id: str,
    ) -> Optional[Dict[str, Any]]:
        if not source_system or not self.minio_client:
            return None

        system = str(source_system).lower()
        pdf_entries: List[Tuple[str, str, str]] = []

        if system in {"mit_ocw", "mit ocw"}:
            course_id = None
            if record:
                course_id = record.get("course_id") or record.get("course_number")
                if not course_id and record.get("url"):
                    slug = record["url"].rstrip("/").split("/")[-1]
                    course_id = slug
            if not course_id:
                return None
            prefix = f"bronze/mit_ocw/pdfs/{course_id}/"
            pdf_entries = self._collect_pdf_entries(prefix)
        elif system in {"otl", "open textbook library"}:
            normalized_id = resource_id.strip()
            prefixes = [
                f"bronze/otl/otl-pdfs/{normalized_id}_",
                f"bronze/otl/otl-pdfs/{normalized_id}/",
                f"bronze/otl/otl-pdfs/{normalized_id}",
            ]
            for prefix in prefixes:
                pdf_entries = self._collect_pdf_entries(prefix)
                if pdf_entries:
                    break
        else:
            return None

        if not pdf_entries:
            return None

        # Build nested pdf_chunks structure
        pdf_chunks: List[Dict[str, Any]] = []
        titles = []
        files = []

        for file_title, file_uri, first_text, pages in pdf_entries:
            titles.append(file_title)
            files.append(file_uri)
            
            # Add each page as a separate chunk (for nested search)
            for page_num, page_text in pages:
                if page_text:  # Only add non-empty pages
                    pdf_chunks.append({
                        "text": page_text[:self.max_pdf_page_chars],  # Truncate to limit
                        "page": page_num,
                        "file": file_title
                    })

        # Create summary pdf_text for backward compatibility (optional)
        pdf_text_summary = " ".join([chunk["text"][:500] for chunk in pdf_chunks[:5]])  # First 5 pages, 500 chars each

        return {
            "pdf_text": pdf_text_summary,  # Compact summary for legacy field
            "pdf_titles": titles,
            "pdf_files": files,
            "pdf_chunks": pdf_chunks,  # Main nested structure for page-level search
        }

    def _collect_pdf_entries(self, prefix: str) -> List[Tuple[str, str, str, List[Tuple[int, str]]]]:
        if prefix in self._pdf_listing_cache:
            objects = self._pdf_listing_cache[prefix]
        else:
            objects = []
            try:
                for obj in self.minio_client.list_objects(self.bucket, prefix=prefix, recursive=True):
                    objects.append(obj.object_name)
            except Exception as exc:
                print(f"[Warning] Unable to list PDFs for prefix {prefix}: {exc}")
            self._pdf_listing_cache[prefix] = objects

        entries: List[Tuple[str, str, str, List[Tuple[int, str]]]] = []
        for object_name in objects:
            pages = self._get_pdf_pages(object_name)
            if not pages:
                continue
            file_title = PurePosixPath(object_name).name
            file_uri = f"s3a://{self.bucket}/{object_name}"
            # Keep first page text as summary (for backward compatibility in titles list)
            first_text = pages[0][1] if pages else ""
            entries.append((file_title, file_uri, first_text, pages))
            if len(entries) >= self.max_pdf_texts:
                break
        return entries

    def _get_pdf_pages(self, object_name: str) -> List[Tuple[int, str]]:
        """Extract per-page text (chunked) with caching."""
        if object_name in self._pdf_text_cache:
            cached = self._pdf_text_cache[object_name]
            if isinstance(cached, list):
                return cached
            if cached:
                return [(1, cached)]

        response = None
        data: Optional[bytes] = None
        try:
            response = self.minio_client.get_object(self.bucket, object_name)
            data = response.read()
        except Exception as exc:
            print(f"[Warning] Unable to download PDF {object_name}: {exc}")
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass
                try:
                    response.release_conn()
                except Exception:
                    pass

        pages = self._extract_pages_from_pdf_bytes(data)
        self._pdf_text_cache[object_name] = pages
        return pages

    def _detect_repeating_pattern(self, lines_list: List[List[str]], position: str = 'first', threshold: float = 0.7) -> Optional[str]:
        """Detect repeating patterns in first/last lines across pages.
        
        Args:
            lines_list: List of line arrays for each page
            position: 'first' for header, 'last' for footer
            threshold: Minimum ratio of pages that must have the pattern
        
        Returns:
            The repeating pattern if found, else None
        """
        if len(lines_list) < 3:  # Need at least 3 pages to detect pattern
            return None
        
        from collections import Counter
        
        # Extract target lines based on position
        target_lines = []
        for lines in lines_list:
            if not lines:
                continue
            if position == 'first' and len(lines) > 0:
                target_lines.append(lines[0].strip())
            elif position == 'last' and len(lines) > 0:
                target_lines.append(lines[-1].strip())
        
        if not target_lines:
            return None
        
        # Find most common line
        counter = Counter(target_lines)
        most_common, count = counter.most_common(1)[0]
        
        # Check if it's repeating enough
        ratio = count / len(target_lines)
        if ratio >= threshold and len(most_common.strip()) > 0:
            # Exclude very short strings (likely not real headers)
            if len(most_common.strip()) < 5:
                return None
            return most_common
        
        return None

    def _remove_header_footer(self, pages_text: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
        """Remove repeating header/footer patterns from pages.
        
        Args:
            pages_text: List of (page_num, text) tuples
        
        Returns:
            Cleaned list of (page_num, text) tuples
        """
        if len(pages_text) < 3:
            return pages_text
        
        # Split each page into lines
        pages_lines = [text.split('\n') for _, text in pages_text]
        
        # Detect header pattern (first line)
        header_pattern = self._detect_repeating_pattern(pages_lines, position='first', threshold=0.7)
        
        # Detect footer pattern (last line)
        footer_pattern = self._detect_repeating_pattern(pages_lines, position='last', threshold=0.7)
        
        # Remove patterns from pages
        cleaned_pages = []
        for page_num, text in pages_text:
            lines = text.split('\n')
            
            # Remove header (first line if matches pattern)
            if header_pattern and len(lines) > 0 and lines[0].strip() == header_pattern:
                lines = lines[1:]
            
            # Remove footer (last line if matches pattern)
            if footer_pattern and len(lines) > 0 and lines[-1].strip() == footer_pattern:
                lines = lines[:-1]
            
            # Rejoin
            cleaned_text = '\n'.join(lines).strip()
            if cleaned_text:  # Only keep non-empty pages
                cleaned_pages.append((page_num, cleaned_text))
        
        return cleaned_pages

    def _clean_text(self, text: Optional[str], max_chars: int) -> Optional[str]:
        """Normalize whitespace and trim long PDF text."""
        if not text:
            return None
        
        # Normalize whitespace
        compact = " ".join(str(text).split())
        if not compact:
            return None
        
        # Truncate if too long
        if len(compact) > max_chars:
            return compact[:max_chars]
        
        return compact

    def _extract_pages_from_pdf_bytes(self, data: Optional[bytes]) -> List[Tuple[int, str]]:
        """Extract text per page with limits, remove headers/footers, clean text."""
        if not data:
            return []

        raw_pages: List[Tuple[int, str]] = []

        # Try pdfplumber first
        if pdfplumber:
            try:
                with pdfplumber.open(BytesIO(data)) as pdf:
                    for idx, page in enumerate(pdf.pages[: self.max_pdf_pages], start=1):
                        page_text = (page.extract_text() or "").strip()
                        if page_text:
                            raw_pages.append((idx, page_text))
            except Exception:
                raw_pages = []

        # Fallback PyPDF2 if pdfplumber failed
        if not raw_pages and PyPDF2:
            try:
                reader = PyPDF2.PdfReader(BytesIO(data))
                for idx, page in enumerate(reader.pages[: self.max_pdf_pages], start=1):
                    try:
                        page_text = (page.extract_text() or "").strip()
                        if page_text:
                            raw_pages.append((idx, page_text))
                    except Exception:
                        continue
            except Exception:
                raw_pages = []

        if not raw_pages:
            return []

        # Step 1: Remove header/footer patterns across all pages
        cleaned_pages = self._remove_header_footer(raw_pages)

        # Step 2: Clean each page individually (page numbers, whitespace, truncate)
        final_pages = []
        for page_num, page_text in cleaned_pages:
            clean = self._clean_text(page_text, self.max_pdf_page_chars)
            if clean:
                final_pages.append((page_num, clean))

        return final_pages

    def _augment_with_pdf_content(self, document: Dict[str, Any]) -> None:
        """Thêm nội dung PDF vào document để index vào Elasticsearch.

        Workflow: Extract PDF text trực tiếp từ MinIO.
        """
        if not self.index_pdf_content or not self.minio_client:
            return

        resource_id = document.get("resource_id")
        if resource_id is None:
            return
        resource_id = str(resource_id)

        bronze_path = document.get("bronze_source_path")
        record = None
        if isinstance(bronze_path, str):
            record = self._get_bronze_record(bronze_path, resource_id)

        bundle = self._bundle_from_minio(document.get("source_system"), record, resource_id)
        if not bundle:
            return

        pdf_text = bundle.get("pdf_text")
        if pdf_text:
            document["pdf_text"] = pdf_text
            current = document.get("search_text")
            search_pdf_part = pdf_text[: self.search_pdf_chars]
            document["search_text"] = " ".join(filter(None, [current, search_pdf_part]))

        if bundle.get("pdf_titles"):
            document["pdf_titles"] = bundle["pdf_titles"]

        if bundle.get("pdf_files"):
            document["pdf_files"] = bundle["pdf_files"]

        if bundle.get("pdf_chunks"):
            document["pdf_chunks"] = bundle["pdf_chunks"]

    def _load_table(self, table_name: str, description: str) -> Optional[DataFrame]:
        try:
            df = self.spark.table(table_name)
            if df.rdd.isEmpty():
                print(f"  {description}: Table empty ({table_name})")
                return None
            print(f"  {description}: Loaded {df.count():,} records")
            return df
        except Exception as exc:
            print(f"  {description}: Unable to read {table_name} ({exc})")
            return None

    # Document build
    def _prepare_fact_enrichment(
        self,
        fact_df: Optional[DataFrame],
        dim_sources: Optional[DataFrame],
        dim_languages: Optional[DataFrame],
        dim_date: Optional[DataFrame],
    ) -> Optional[DataFrame]:
        """Join fact table với các dimension tables để làm giàu dữ liệu"""
        if fact_df is None:
            return None

        # Chọn các columns cần thiết từ fact table
        fact = fact_df.select(
            "resource_key",
            "source_key",
            "language_key",
            "publication_date_key",
            "ingested_date_key",
        )

        # Join với dim_sources để có source_code, source_name (VD: mit_ocw, openstax)
        if dim_sources is not None:
            fact = fact.join(
                dim_sources.select("source_key", "source_code", "source_name"),
                "source_key",
                "left",
            )

        # Join với dim_languages để có language_code (VD: en, vi)
        if dim_languages is not None:
            fact = fact.join(
                dim_languages.select("language_key", "language_code"),
                "language_key",
                "left",
            )

        if dim_date is not None:
            pub_dates = dim_date.select(
                F.col("date_key").alias("pub_key"),
                F.col("date").alias("publication_date"),
            )
            ing_dates = dim_date.select(
                F.col("date_key").alias("ing_key"),
                F.col("date").alias("ingested_date"),
            )

            fact = fact.join(
                pub_dates,
                fact.publication_date_key == pub_dates.pub_key,
                "left",
            ).drop("pub_key")
            fact = fact.join(
                ing_dates,
                fact.ingested_date_key == ing_dates.ing_key,
                "left",
            ).drop("ing_key")

        return fact

    def _build_index_dataframe(
        self,
        dim_resources: DataFrame,
        fact_enrichment: Optional[DataFrame],
    ) -> DataFrame:
        """Xây dựng DataFrame cuối cùng để index vào Elasticsearch"""
        docs = dim_resources.alias("dim")

        # Join với fact enrichment (source, language, dates, metrics)
        if fact_enrichment is not None:
            docs = docs.join(fact_enrichment.alias("fact"), "resource_key", "left")

        column_aliases = [
            ("resource_key", "resource_key"),
            ("resource_id", "resource_id"),
            ("title", "title"),
            ("description", "description"),
            ("source_url", "source_url"),
            ("source_code", "source_system"),
            ("language_code", "language"),
            ("publication_date", "publication_date"),
        ]

        select_exprs = [F.col(col).alias(alias) for col, alias in column_aliases if col in docs.columns]

        # Extract subject names if available
        if "matched_subjects" in docs.columns:
            # Assuming matched_subjects is an array of structs with subject_name
            # We extract just the names as an array of strings
            select_exprs.append(F.col("matched_subjects.subject_name").alias("subjects"))

        docs = docs.select(*select_exprs)

        creator_text_col = "creator_names_text"
        if "creator_names" in docs.columns:
            docs = docs.withColumn(
                creator_text_col,
                F.when(
                    F.col("creator_names").isNotNull(),
                    F.array_join(F.col("creator_names"), " "),
                ).otherwise(F.lit("")),
            )
        else:
            docs = docs.withColumn(creator_text_col, F.lit(""))

        # Build search_text: Tổng hợp TẤT CẢ text có thể search
        # Elasticsearch sẽ index field này để full-text search
        # Search_text: chỉ giữ phần tiêu đề + mô tả (PDF sẽ nối thêm ở bước augment)
        # Also include subjects in search_text for better recall
        search_text_cols = [F.col("title"), F.col("description")]
        if "subjects" in docs.columns:
             search_text_cols.append(F.array_join(F.col("subjects"), " "))

        docs = docs.withColumn(
            "search_text",
            F.concat_ws(" ", *search_text_cols),
        ).drop(creator_text_col)

        return docs

    # Indexing
    def _ensure_index(self) -> None:
        """Tạo Elasticsearch index với mapping (nếu chưa tồn tại)"""
        exists = self.es.indices.exists(index=self.index_name)
        
        # Xóa index cũ nếu RECREATE=1 (để rebuild từ đầu)
        if exists and self.recreate_index:
            print(f"Deleting existing index '{self.index_name}' (ELASTICSEARCH_RECREATE=1)")
            self.es.indices.delete(index=self.index_name)
            exists = False

        if not exists:
            print(f"Creating Elasticsearch index '{self.index_name}'")
            body = {
                "settings": {
                    "index": {
                        "number_of_shards": 1,  # 1 shard cho single node
                        "number_of_replicas": 0  # Không cần replica (dev mode)
                    },
                    "analysis": {
                        "analyzer": {
                            "autocomplete": {
                                "tokenizer": "autocomplete",
                                "filter": ["lowercase"]
                            },
                            "autocomplete_search": {
                                "tokenizer": "lowercase"
                            }
                        },
                        "tokenizer": {
                            "autocomplete": {
                                "type": "edge_ngram",
                                "min_gram": 2,
                                "max_gram": 10,
                                "token_chars": ["letter", "digit"]
                            }
                        }
                    },
                },
                "mappings": {
                    "properties": {
                        # Text fields - Full-text search với analyzer
                        "title": {
                            "type": "text",
                            "analyzer": "english",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256},
                                "autocomplete": {"type": "text", "analyzer": "autocomplete", "search_analyzer": "autocomplete_search"}
                            },
                        },
                        "description": {"type": "text", "analyzer": "english"},
                        "search_text": {"type": "text", "analyzer": "english"},  # Field chính để search
                        "subjects": {
                            "type": "text", 
                            "analyzer": "english",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256}
                            }
                        },
                        
                        # Keyword fields - Filtering (exact match, aggregation)
                        "source_system": {"type": "keyword"},  # mit_ocw, openstax, otl, textbook
                        "language": {"type": "keyword"},  # en, vi
                        "publication_date": {"type": "date"},  # Hỗ trợ recency boosting
                        
                        # PDF content fields
                        "pdf_text": {"type": "text", "analyzer": "english"},  # Nội dung PDF
                        "pdf_titles": {"type": "keyword"},
                        "pdf_files": {"type": "keyword"},
                        
                        # Nested PDF chunks for page-level search
                        "pdf_chunks": {
                            "type": "nested",
                            "properties": {
                                "text": {"type": "text", "analyzer": "english"},
                                "page": {"type": "integer"},
                                "file": {"type": "keyword"}
                            }
                        }
                    }
                }
            }
            self.es.indices.create(index=self.index_name, **body)

    def _bulk_index(self, df: DataFrame) -> None:
        """Bulk index documents vào Elasticsearch (batch processing)"""
        # Cache DataFrame để không tính lại nhiều lần
        cached = df.persist()
        total = cached.count()
        if total == 0:
            cached.unpersist()
            print("No documents to index")
            return

        print(f"Indexing {total:,} documents into '{self.index_name}' (batch={self.batch_size})")
        
        # Bulk index với batching (500 docs/request)
        success, errors = helpers.bulk(
            self.es,
            self._yield_actions(cached),  # Generator yield từng document
            chunk_size=self.batch_size,  # 500 docs/batch
            request_timeout=self.request_timeout,  # 120s timeout
        )
        
        cached.unpersist()  # Giải phóng cache
        
        if errors:
            print(f"[Warning] Elasticsearch bulk errors: {errors}")
        print(f"[Success] Indexed {success:,} documents into {self.index_name}")

    def _yield_actions(self, df: DataFrame) -> Iterator[Dict[str, Any]]:
        """Yield Elasticsearch bulk actions from a Spark DataFrame."""
        for row in df.toLocalIterator():
            doc = row.asDict(recursive=True)
            doc = {k: v for k, v in doc.items() if v is not None}
            self._augment_with_pdf_content(doc)
            doc_id = doc.get("resource_key") or doc.get("resource_id")
            yield {"_index": self.index_name, "_id": doc_id, "_source": doc}

    def run(self) -> None:
        """Main function: Đồng bộ dữ liệu từ Gold layer sang Elasticsearch"""
        print("=" * 80)
        print("Starting Gold -> Elasticsearch sync")
        print("=" * 80)

        # Bước 1: Load Gold tables (dimension và fact)
        dim_resources = self._load_table(self.dim_oer_resources_table, "dim_oer_resources")
        if dim_resources is None:
            print("[Error] dim_oer_resources missing; aborting sync")
            return

        fact_resources = self._load_table(self.fact_oer_resources_table, "fact_oer_resources")
        dim_sources = self._load_table(self.dim_sources_table, "dim_sources")
        dim_languages = self._load_table(self.dim_languages_table, "dim_languages")
        dim_date = self._load_table(self.dim_date_table, "dim_date")

        # Bước 2: Join và enrich dữ liệu
        fact_enrichment = self._prepare_fact_enrichment(
            fact_resources, dim_sources, dim_languages, dim_date
        )

        # Bước 3: Build search documents với search_text
        docs = self._build_index_dataframe(dim_resources, fact_enrichment)
        
        # Bước 4: Tạo Elasticsearch index (nếu chưa có)
        self._ensure_index()
        
        # Bước 5: Bulk index documents vào Elasticsearch
        self._bulk_index(docs)
        
        print("Gold -> Elasticsearch sync complete")


def main() -> None:
    indexer = OERElasticsearchIndexer()
    indexer.run()


if __name__ == "__main__":
    main()

