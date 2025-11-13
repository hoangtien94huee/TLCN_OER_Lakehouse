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

try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:  # pragma: no cover - handled at runtime
    Elasticsearch = None  # type: ignore
    helpers = None  # type: ignore

try:
    from minio import Minio
except ImportError:  # pragma: no cover - handled at runtime
    Minio = None  # type: ignore

try:
    import pdfplumber
except ImportError:  # pragma: no cover
    pdfplumber = None

try:
    import PyPDF2
except ImportError:  # pragma: no cover
    PyPDF2 = None

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
except ImportError:  # pragma: no cover - handled at runtime
    SparkSession = DataFrame = F = None  # type: ignore

SPARK_AVAILABLE = SparkSession is not None
ELASTICSEARCH_AVAILABLE = Elasticsearch is not None
MINIO_AVAILABLE = Minio is not None


class OERElasticsearchIndexer:
    """Sync Gold layer OER resources into an Elasticsearch index."""

    def __init__(self) -> None:
        if not SPARK_AVAILABLE:
            raise RuntimeError("PySpark is required to export Gold data to Elasticsearch")
        if not ELASTICSEARCH_AVAILABLE:
            raise RuntimeError(
                "The 'elasticsearch' Python package is required. "
                "Install it via airflow/requirements.txt."
            )

        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.silver_catalog = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.silver_database = os.getenv("SILVER_DATABASE", "default")
        self.gold_catalog = os.getenv("ICEBERG_GOLD_CATALOG", "gold")
        self.gold_database = os.getenv("GOLD_DATABASE", "analytics")

        # Gold tables
        self.dim_programs_table = f"{self.gold_catalog}.{self.gold_database}.dim_programs"
        self.dim_sources_table = f"{self.gold_catalog}.{self.gold_database}.dim_sources"
        self.dim_languages_table = f"{self.gold_catalog}.{self.gold_database}.dim_languages"
        self.dim_date_table = f"{self.gold_catalog}.{self.gold_database}.dim_date"
        self.dim_oer_resources_table = f"{self.gold_catalog}.{self.gold_database}.dim_oer_resources"
        self.fact_oer_resources_table = f"{self.gold_catalog}.{self.gold_database}.fact_oer_resources"

        # Elasticsearch configuration
        self.es_host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        self.es_user = os.getenv("ELASTICSEARCH_USER")
        self.es_password = os.getenv("ELASTICSEARCH_PASSWORD")
        self.index_name = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
        self.batch_size = int(os.getenv("ELASTICSEARCH_BATCH_SIZE", "500"))
        self.request_timeout = int(os.getenv("ELASTICSEARCH_TIMEOUT", "120"))
        self.recreate_index = os.getenv("ELASTICSEARCH_RECREATE", "0").lower() in {
            "1",
            "true",
            "yes",
        }

        http_auth = None
        if self.es_user and self.es_password:
            http_auth = (self.es_user, self.es_password)

        self.es = Elasticsearch(
            hosts=[self.es_host],
            basic_auth=http_auth,
            verify_certs=self.es_host.startswith("https"),
        )

        self.index_pdf_content = os.getenv("ELASTICSEARCH_INDEX_PDF_CONTENT", "1").lower() in {
            "1",
            "true",
            "yes",
        }
        self.max_pdf_texts = int(os.getenv("ELASTICSEARCH_MAX_PDF_PER_RESOURCE", "3"))
        self.minio_client = self._create_minio_client() if self.index_pdf_content else None
        self._bronze_record_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._pdf_listing_cache: Dict[str, List[str]] = {}
        self._pdf_text_cache: Dict[str, str] = {}

        self.spark = self._create_spark_session()

    # ------------------------------------------------------------------ Spark --
    def _create_spark_session(self) -> SparkSession:
        session = (
            SparkSession.builder.appName("OER-Gold-Elasticsearch")
            .master(os.getenv("SPARK_MASTER", "local[*]"))
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{self.silver_catalog}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{self.silver_catalog}.type", "hadoop")
            .config(
                f"spark.sql.catalog.{self.silver_catalog}.warehouse",
                f"s3a://{self.bucket}/silver/",
            )
            .config(
                f"spark.sql.catalog.{self.gold_catalog}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{self.gold_catalog}.type", "hadoop")
            .config(
                f"spark.sql.catalog.{self.gold_catalog}.warehouse",
                f"s3a://{self.bucket}/gold/",
            )
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
            .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g"))
            .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAXRESULTSIZE", "1g"))
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("WARN")
        return session

    # -------------------------------------------------------------- MinIO Utils --
    def _create_minio_client(self) -> Optional[Minio]:
        if not MINIO_AVAILABLE:
            print("[Warning] MinIO library not available; PDF indexing disabled")
            self.index_pdf_content = False
            return None

        try:
            endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
            endpoint = endpoint.replace("https://", "").replace("http://", "")
            access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
            secure = os.getenv("MINIO_SECURE", "0").lower() in {"1", "true", "yes"}
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
        if bronze_path in self._bronze_record_cache:
            return self._bronze_record_cache[bronze_path]

        mapping: Dict[str, Dict[str, Any]] = {}
        if not self.minio_client or not bronze_path:
            self._bronze_record_cache[bronze_path] = mapping
            return mapping

        bucket, object_name = self._parse_s3_uri(bronze_path)
        if not bucket or not object_name:
            self._bronze_record_cache[bronze_path] = mapping
            return mapping

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
        if not bronze_path or not resource_id:
            return None
        records = self._load_bronze_records(bronze_path)
        if not records:
            return None
        rec = records.get(resource_id) or records.get(str(resource_id).strip())
        return rec

    def _bundle_from_record(self, record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not record:
            return None
        pdfs = record.get("pdfs")
        if not isinstance(pdfs, list) or not pdfs:
            return None

        texts: List[str] = []
        titles: List[str] = []
        files: List[str] = []
        for pdf in pdfs[: self.max_pdf_texts]:
            if not isinstance(pdf, dict):
                continue
            text_value = pdf.get("text_content")
            if isinstance(text_value, str):
                cleaned = text_value.strip()
                if cleaned:
                    texts.append(cleaned)
            title = pdf.get("title")
            if title:
                titles.append(str(title))
            file_path = pdf.get("minio_path") or pdf.get("url")
            if file_path:
                files.append(str(file_path))
        if not texts:
            return None
        return {
            "pdf_text": "\n\n".join(texts),
            "pdf_titles": titles,
            "pdf_files": files,
        }

    def _bundle_from_minio(
        self,
        source_system: Optional[str],
        record: Optional[Dict[str, Any]],
        resource_id: str,
    ) -> Optional[Dict[str, Any]]:
        if not source_system or not self.minio_client:
            return None

        pdf_entries: List[Tuple[str, str, str]] = []

        if source_system == "mit_ocw":
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
        elif source_system == "otl":
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

        texts = [entry[2] for entry in pdf_entries]
        titles = [entry[0] for entry in pdf_entries]
        files = [entry[1] for entry in pdf_entries]

        return {
            "pdf_text": "\n\n".join(texts),
            "pdf_titles": titles,
            "pdf_files": files,
        }

    def _collect_pdf_entries(self, prefix: str) -> List[Tuple[str, str, str]]:
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

        entries: List[Tuple[str, str, str]] = []
        for object_name in objects:
            text = self._get_pdf_text(object_name)
            if not text:
                continue
            file_title = PurePosixPath(object_name).name
            file_uri = f"s3a://{self.bucket}/{object_name}"
            entries.append((file_title, file_uri, text))
            if len(entries) >= self.max_pdf_texts:
                break
        return entries

    def _get_pdf_text(self, object_name: str) -> Optional[str]:
        if object_name in self._pdf_text_cache:
            cached = self._pdf_text_cache[object_name]
            return cached or None

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

        text = self._extract_text_from_pdf_bytes(data)
        self._pdf_text_cache[object_name] = text or ""
        return text

    def _extract_text_from_pdf_bytes(self, data: Optional[bytes]) -> Optional[str]:
        if not data:
            return None

        snippets: List[str] = []
        if pdfplumber:
            try:
                with pdfplumber.open(BytesIO(data)) as pdf:
                    for page in pdf.pages[:3]:
                        page_text = (page.extract_text() or "").strip()
                        if page_text:
                            snippets.append(page_text)
            except Exception:
                pass

        if not snippets and PyPDF2:
            try:
                reader = PyPDF2.PdfReader(BytesIO(data))
                for page in reader.pages[:3]:
                    try:
                        page_text = (page.extract_text() or "").strip()
                        if page_text:
                            snippets.append(page_text)
                    except Exception:
                        continue
            except Exception:
                pass

        if not snippets:
            return None

        combined = "\n".join(snippets)
        return combined[:4000]

    def _augment_with_pdf_content(self, document: Dict[str, Any]) -> None:
        if not self.index_pdf_content or not self.minio_client:
            return

        resource_id = document.get("resource_id")
        if resource_id is None:
            return
        resource_id = str(resource_id)

        bronze_path = document.get("bronze_source_path")
        if not isinstance(bronze_path, str):
            return

        record = self._get_bronze_record(bronze_path, resource_id)
        bundle = self._bundle_from_record(record)
        if not bundle:
            bundle = self._bundle_from_minio(document.get("source_system"), record, resource_id)
        if not bundle:
            return

        pdf_text = bundle.get("pdf_text")
        if pdf_text:
            document["pdf_text"] = pdf_text
            current = document.get("search_text")
            document["search_text"] = " ".join(filter(None, [current, pdf_text]))

        if bundle.get("pdf_titles"):
            document["pdf_titles"] = bundle["pdf_titles"]

        if bundle.get("pdf_files"):
            document["pdf_files"] = bundle["pdf_files"]

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

    # --------------------------------------------------------- Document build --
    def _prepare_fact_enrichment(
        self,
        fact_df: Optional[DataFrame],
        dim_sources: Optional[DataFrame],
        dim_languages: Optional[DataFrame],
        dim_date: Optional[DataFrame],
    ) -> Optional[DataFrame]:
        if fact_df is None:
            return None

        fact = fact_df.select(
            "resource_key",
            "source_key",
            "language_key",
            "publication_date_key",
            "ingested_date_key",
            "program_ids",
            "matched_subjects_count",
            "matched_programs_count",
            "data_quality_score",
        )

        if dim_sources is not None:
            fact = fact.join(
                dim_sources.select("source_key", "source_code", "source_name"),
                "source_key",
                "left",
            )

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

    def _prepare_program_details(
        self, fact_df: Optional[DataFrame], dim_programs: Optional[DataFrame]
    ) -> Optional[DataFrame]:
        if fact_df is None or dim_programs is None:
            return None

        exploded = fact_df.select(
            "resource_key",
            F.explode_outer("program_ids").alias("program_id"),
        ).where(F.col("program_id").isNotNull())

        if exploded.rdd.isEmpty():
            return None

        details = (
            exploded.join(
                dim_programs.select(
                    "program_id",
                    "program_name",
                    "program_code",
                    "faculty_name",
                ),
                "program_id",
                "left",
            )
            .groupBy("resource_key")
            .agg(
                F.collect_list(
                    F.struct(
                        "program_id",
                        "program_name",
                        "program_code",
                        "faculty_name",
                    )
                ).alias("programs")
            )
        )
        return details

    def _build_index_dataframe(
        self,
        dim_resources: DataFrame,
        fact_enrichment: Optional[DataFrame],
        program_details: Optional[DataFrame],
    ) -> DataFrame:
        docs = dim_resources.alias("dim")

        if fact_enrichment is not None:
            docs = docs.join(fact_enrichment.alias("fact"), "resource_key", "left")

        if program_details is not None:
            docs = docs.join(program_details.alias("programs_lookup"), "resource_key", "left")

        column_aliases = [
            ("resource_key", "resource_key"),
            ("resource_id", "resource_id"),
            ("title", "title"),
            ("description", "description"),
            ("source_url", "source_url"),
            ("publisher_name", "publisher_name"),
            ("creator_names", "creator_names"),
            ("matched_subjects", "matched_subjects"),
            ("dc_xml_path", "dc_xml_path"),
            ("bronze_source_path", "bronze_source_path"),
            ("source_code", "source_system"),
            ("source_name", "source_name"),
            ("language_code", "language"),
            ("publication_date", "publication_date"),
            ("ingested_date", "ingested_date"),
            ("program_ids", "program_ids"),
            ("matched_subjects_count", "matched_subjects_count"),
            ("matched_programs_count", "matched_programs_count"),
            ("data_quality_score", "data_quality_score"),
            ("programs", "programs"),
        ]

        select_exprs = [F.col(col).alias(alias) for col, alias in column_aliases if col in docs.columns]

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

        docs = docs.withColumn(
            "search_text",
            F.concat_ws(
                " ",
                F.col("title"),
                F.col("description"),
                F.col(creator_text_col),
                F.col("publisher_name"),
            ),
        ).drop(creator_text_col)

        return docs

    # --------------------------------------------------------------- Indexing --
    def _ensure_index(self) -> None:
        exists = self.es.indices.exists(index=self.index_name)
        if exists and self.recreate_index:
            print(f"Deleting existing index '{self.index_name}' (ELASTICSEARCH_RECREATE=1)")
            self.es.indices.delete(index=self.index_name)
            exists = False

        if not exists:
            print(f"Creating Elasticsearch index '{self.index_name}'")
            body = {
                "settings": {
                    "index": {"number_of_shards": 1, "number_of_replicas": 0},
                    "analysis": {
                        "analyzer": {
                            "folding": {
                                "tokenizer": "standard",
                                "filter": ["lowercase", "asciifolding"],
                            }
                        }
                    },
                },
                "mappings": {
                    "properties": {
                        "title": {
                            "type": "text",
                            "analyzer": "folding",
                            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                        },
                        "description": {"type": "text", "analyzer": "folding"},
                        "search_text": {"type": "text", "analyzer": "folding"},
                        "publisher_name": {"type": "keyword"},
                        "creator_names": {"type": "keyword"},
                        "source_system": {"type": "keyword"},
                        "source_name": {"type": "keyword"},
                        "language": {"type": "keyword"},
                        "program_ids": {"type": "integer"},
                        "matched_subjects_count": {"type": "integer"},
                        "matched_programs_count": {"type": "integer"},
                        "data_quality_score": {"type": "float"},
                        "publication_date": {"type": "date"},
                        "ingested_date": {"type": "date"},
                        "pdf_text": {"type": "text", "analyzer": "folding"},
                        "pdf_titles": {"type": "keyword"},
                        "pdf_files": {"type": "keyword"},
                        "matched_subjects": {
                            "type": "nested",
                            "properties": {
                                "subject_id": {"type": "integer"},
                                "subject_name": {"type": "keyword"},
                                "subject_name_en": {"type": "keyword"},
                                "subject_code": {"type": "keyword"},
                                "similarity": {"type": "float"},
                                "matched_text": {"type": "text", "analyzer": "folding"},
                            },
                        },
                        "programs": {
                            "type": "nested",
                            "properties": {
                                "program_id": {"type": "integer"},
                                "program_name": {"type": "keyword"},
                                "program_code": {"type": "keyword"},
                                "faculty_name": {"type": "keyword"},
                            },
                        },
                    }
                },
            }
            self.es.indices.create(index=self.index_name, **body)

    def _sanitize_value(self, value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, list):
            return [self._sanitize_value(v) for v in value]
        if isinstance(value, dict):
            return {k: self._sanitize_value(v) for k, v in value.items()}
        return value

    def _yield_actions(self, df: DataFrame) -> Iterator[Dict[str, Any]]:
        for row in df.toLocalIterator():
            payload = row.asDict(recursive=True)
            self._augment_with_pdf_content(payload)
            payload.pop("bronze_source_path", None)
            doc_id = str(payload.pop("resource_key"))
            source = {k: self._sanitize_value(v) for k, v in payload.items()}
            yield {
                "_op_type": "index",
                "_index": self.index_name,
                "_id": doc_id,
                "_source": source,
            }

    def _bulk_index(self, df: DataFrame) -> None:
        cached = df.persist()
        total = cached.count()
        if total == 0:
            cached.unpersist()
            print("No documents to index")
            return

        print(f"Indexing {total:,} documents into '{self.index_name}' (batch={self.batch_size})")
        success, errors = helpers.bulk(
            self.es,
            self._yield_actions(cached),
            chunk_size=self.batch_size,
            request_timeout=self.request_timeout,
        )
        cached.unpersist()
        if errors:
            print(f"[Warning] Elasticsearch bulk errors: {errors}")
        print(f"[Success] Indexed {success:,} documents into {self.index_name}")

    # ------------------------------------------------------------------- Run --
    def run(self) -> None:
        print("=" * 80)
        print("Starting Gold -> Elasticsearch sync")
        print("=" * 80)

        dim_resources = self._load_table(self.dim_oer_resources_table, "dim_oer_resources")
        if dim_resources is None:
            print("[Error] dim_oer_resources missing; aborting sync")
            return

        fact_resources = self._load_table(self.fact_oer_resources_table, "fact_oer_resources")
        dim_programs = self._load_table(self.dim_programs_table, "dim_programs")
        dim_sources = self._load_table(self.dim_sources_table, "dim_sources")
        dim_languages = self._load_table(self.dim_languages_table, "dim_languages")
        dim_date = self._load_table(self.dim_date_table, "dim_date")

        fact_enrichment = self._prepare_fact_enrichment(
            fact_resources, dim_sources, dim_languages, dim_date
        )
        program_details = self._prepare_program_details(fact_resources, dim_programs)

        docs = self._build_index_dataframe(dim_resources, fact_enrichment, program_details)
        self._ensure_index()
        self._bulk_index(docs)
        print("Gold -> Elasticsearch sync complete")


def main() -> None:
    indexer = OERElasticsearchIndexer()
    indexer.run()


if __name__ == "__main__":
    main()
