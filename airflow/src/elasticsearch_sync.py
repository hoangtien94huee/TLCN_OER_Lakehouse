#!/usr/bin/env python3
"""
Elasticsearch sync for chatbot retrieval.

Pipeline:
    Silver chunks -> multilingual E5 embeddings -> Elasticsearch index

Supports both flat and hierarchical chunk schemas.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from itertools import islice
from typing import Any, Dict, List, Optional, Tuple

from elasticsearch import Elasticsearch, helpers

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
except ImportError:
    SparkSession = DataFrame = F = None  # type: ignore


SentenceTransformer = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _load_embedding_model(model_name: str):
    global SentenceTransformer
    if SentenceTransformer is None:
        from sentence_transformers import SentenceTransformer as ST

        SentenceTransformer = ST
    return SentenceTransformer(model_name)


class ChatbotElasticsearchSync:
    """Index Silver chunks into Elasticsearch with hierarchical metadata."""

    def __init__(self):
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.silver_catalog = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.silver_database = os.getenv("SILVER_DATABASE", "default")
        self.silver_chunks = f"{self.silver_catalog}.{self.silver_database}.oer_chunks"
        self.silver_documents = f"{self.silver_catalog}.{self.silver_database}.oer_documents"

        self.es_host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        self.index_name = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
        self.batch_size = int(os.getenv("ELASTICSEARCH_BATCH_SIZE", "500"))  # Increased from 100 for faster processing
        self.timeout = int(os.getenv("ELASTICSEARCH_TIMEOUT", "180"))
        self.recreate = os.getenv("ELASTICSEARCH_RECREATE", "1").lower() in {"1", "true", "yes"}
        self.incremental = os.getenv("ELASTICSEARCH_INCREMENTAL", "1").lower() in {"1", "true", "yes"}
        self.index_all_tiers = os.getenv("HIERARCHICAL_INDEX_ALL_TIERS", "1").lower() in {"1", "true", "yes"}
        self.stream_partitions = max(1, int(os.getenv("ELASTICSEARCH_STREAM_PARTITIONS", "32")))
        self.spark_master = os.getenv("ELASTICSEARCH_SPARK_MASTER", os.getenv("SPARK_MASTER", os.getenv("SPARK_MASTER_URL", "local[*]")))

        self.model_name = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-base")
        self.embedding_dim = int(os.getenv("EMBEDDING_DIM", "768"))
        self.minio_bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.minio_public_base_url = os.getenv("MINIO_PUBLIC_BASE_URL", "http://localhost:19000").rstrip("/")

        es_user = os.getenv("ELASTICSEARCH_USER")
        es_password = os.getenv("ELASTICSEARCH_PASSWORD")
        auth = (es_user, es_password) if es_user and es_password else None
        self.es = Elasticsearch(
            hosts=[self.es_host],
            basic_auth=auth,
            verify_certs=self.es_host.startswith("https"),
            request_timeout=self.timeout,
        )

        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        logger.info("Creating Spark session for ES sync...")
        java_home = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
        os.environ.setdefault("JAVA_HOME", java_home)
        os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.pop("JAVA_TOOL_OPTIONS", None)

        minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        endpoint = minio_endpoint if minio_endpoint.startswith(("http://", "https://")) else f"http://{minio_endpoint}"
        logger.info(f"ES sync Spark master: {self.spark_master}")
        builder = SparkSession.builder.appName("ChatbotESSync").master(self.spark_master)

        spark_jars = os.getenv("SPARK_JARS")
        use_local_jars = False
        if spark_jars:
            jar_paths = [p.strip() for p in spark_jars.split(",") if p.strip()]
            use_local_jars = bool(jar_paths) and all(Path(p).exists() for p in jar_paths)
            if use_local_jars:
                logger.info(f"Using local Spark jars: {spark_jars}")
                builder = (
                    builder
                    .config("spark.jars", spark_jars)
                    .config("spark.driver.extraClassPath", spark_jars)
                    .config("spark.executor.extraClassPath", spark_jars)
                )

        if not use_local_jars:
            logger.info("Local Spark jars not found; falling back to spark.jars.packages")
            builder = (
                builder
                .config(
                    "spark.jars.packages",
                    ",".join(
                        [
                            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
                            "org.apache.hadoop:hadoop-aws:3.3.4",
                            "com.amazonaws:aws-java-sdk-bundle:1.12.565",
                        ]
                    ),
                )
                .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2"))
            )

        return (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.silver_catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.silver_catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.silver_catalog}.warehouse", f"s3a://{self.bucket}/silver/")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "oer-airflow-scraper"))
            .config("spark.driver.bindAddress", os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"))
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g"))
            .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAXRESULTSIZE", "1g"))
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
            .getOrCreate()
        )

    def _build_index_dataframe(self) -> Optional[DataFrame]:
        logger.info("Loading chunks from Silver...")
        try:
            chunks = self.spark.table(self.silver_chunks)
        except Exception as exc:
            raise RuntimeError(f"Failed to load chunks table {self.silver_chunks}") from exc

        count = chunks.count()
        if count == 0:
            logger.warning("No chunks available in Silver layer")
            return None
        logger.info(f"Loaded {count:,} chunks")

        defaults: List[Tuple[str, Any]] = [
            ("chunk_type", "section_detail"),
            ("chunk_tier", 3),
            ("chunk_order", 0),
            ("chapter_id", None),
            ("chapter_title", None),
            ("chapter_number", None),
            ("chapter_page_start", None),
            ("chapter_page_end", None),
            ("section_id", None),
            ("section_title", None),
            ("section_number", None),
            ("section_page_start", None),
            ("section_page_end", None),
            ("parent_chunk_id", None),
            ("has_children", False),
            ("is_summary", False),
            ("summary_method", None),
        ]
        for col_name, default_value in defaults:
            if col_name not in chunks.columns:
                chunks = chunks.withColumn(col_name, F.lit(default_value))

        if not self.index_all_tiers:
            chunks = chunks.filter((F.col("chunk_tier").isin([1, 2])) | (F.col("is_summary") == F.lit(True)))
            logger.info("Indexing summaries only (tier 1-2), tier 3 excluded by config")

        if self._table_exists(self.silver_documents):
            docs = self.spark.table(self.silver_documents).select(
                F.col("asset_uid").alias("_doc_asset_uid"),
                F.col("source_system").alias("_doc_source_system"),
                F.col("title").alias("_doc_title"),
                F.col("source_url").alias("_doc_source_url"),
                F.col("asset_path").alias("_doc_asset_path"),
                F.col("file_name").alias("_doc_file_name"),
            )
            chunks = chunks.join(F.broadcast(docs), chunks.asset_uid == docs._doc_asset_uid, "left").drop("_doc_asset_uid")
            if "source_system" not in chunks.columns:
                chunks = chunks.withColumn("source_system", F.col("_doc_source_system"))
            chunks = chunks.withColumn("source_system", F.coalesce(F.col("source_system"), F.col("_doc_source_system")))
            chunks = chunks.withColumn("title", F.coalesce(F.col("title"), F.col("_doc_title")) if "title" in chunks.columns else F.col("_doc_title"))
            chunks = chunks.withColumn("source_url", F.coalesce(F.col("source_url"), F.col("_doc_source_url")) if "source_url" in chunks.columns else F.col("_doc_source_url"))
            chunks = chunks.withColumn("asset_path", F.coalesce(F.col("asset_path"), F.col("_doc_asset_path")) if "asset_path" in chunks.columns else F.col("_doc_asset_path"))
            chunks = chunks.withColumn("file_name", F.coalesce(F.col("file_name"), F.col("_doc_file_name")) if "file_name" in chunks.columns else F.col("_doc_file_name"))
            chunks = chunks.drop("_doc_source_system", "_doc_title", "_doc_source_url", "_doc_asset_path", "_doc_file_name")
        else:
            if "source_system" not in chunks.columns:
                chunks = chunks.withColumn("source_system", F.lit(None))
            if "title" not in chunks.columns:
                chunks = chunks.withColumn("title", F.lit(None))
            if "source_url" not in chunks.columns:
                chunks = chunks.withColumn("source_url", F.lit(None))
            if "asset_path" not in chunks.columns:
                chunks = chunks.withColumn("asset_path", F.lit(None))
            if "file_name" not in chunks.columns:
                chunks = chunks.withColumn("file_name", F.lit(None))

        chunks = chunks.withColumn(
            "minio_url",
            F.when(
                F.col("asset_path").isNotNull() & (F.length(F.trim(F.col("asset_path"))) > 0),
                F.concat(F.lit(f"{self.minio_public_base_url}/{self.minio_bucket}/"), F.regexp_replace(F.col("asset_path"), r"^/+", "")),
            ).otherwise(F.lit(None)),
        )

        df = chunks.select(
            F.col("chunk_id").alias("_id"),
            "chunk_id",
            "resource_uid",
            "asset_uid",
            "source_system",
            "title",
            "source_url",
            "asset_path",
            "file_name",
            "minio_url",
            "chunk_text",
            "page_no",
            "chunk_order",
            "lang",
            "updated_at",
            "chunk_type",
            "chunk_tier",
            "chapter_id",
            "chapter_title",
            "chapter_number",
            "chapter_page_start",
            "chapter_page_end",
            "section_id",
            "section_title",
            "section_number",
            "section_page_start",
            "section_page_end",
            "parent_chunk_id",
            "has_children",
            "is_summary",
            "summary_method",
        )

        df = df.repartition(self.stream_partitions)
        logger.info(f"Prepared chunk dataframe for Elasticsearch indexing with {self.stream_partitions} partitions")
        return df

    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.table(table_name)
            return True
        except Exception:
            return False

    def _ensure_index(self) -> None:
        if self.es.indices.exists(index=self.index_name):
            if self.recreate:
                logger.info(f"Deleting index {self.index_name} (recreate mode)")
                self.es.indices.delete(index=self.index_name)
            else:
                logger.info(f"Index {self.index_name} already exists")
                return

        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "default": {"type": "standard", "stopwords": "_none_"},
                    }
                },
            },
            "mappings": {
                "properties": {
                    "chunk_id": {"type": "keyword"},
                    "resource_uid": {"type": "keyword"},
                    "asset_uid": {"type": "keyword"},
                    "source_system": {"type": "keyword"},
                    "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "source_url": {"type": "keyword"},
                    "asset_path": {"type": "keyword"},
                    "file_name": {"type": "keyword"},
                    "minio_url": {"type": "keyword"},
                    "chunk_text": {"type": "text", "analyzer": "standard"},
                    "page_no": {"type": "integer"},
                    "chunk_order": {"type": "integer"},
                    "lang": {"type": "keyword"},
                    "updated_at": {"type": "date"},
                    "chunk_type": {"type": "keyword"},
                    "chunk_tier": {"type": "integer"},
                    "chapter_id": {"type": "keyword"},
                    "chapter_title": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}},
                    },
                    "chapter_number": {"type": "integer"},
                    "chapter_page_start": {"type": "integer"},
                    "chapter_page_end": {"type": "integer"},
                    "chapter_page_range": {"type": "integer_range"},
                    "section_id": {"type": "keyword"},
                    "section_title": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}},
                    },
                    "section_number": {"type": "keyword"},
                    "section_page_start": {"type": "integer"},
                    "section_page_end": {"type": "integer"},
                    "section_page_range": {"type": "integer_range"},
                    "parent_chunk_id": {"type": "keyword"},
                    "has_children": {"type": "boolean"},
                    "is_summary": {"type": "boolean"},
                    "summary_method": {"type": "keyword"},
                    "embedding": {
                        "type": "dense_vector",
                        "dims": self.embedding_dim,
                        "index": True,
                        "similarity": "cosine",
                    },
                }
            },
        }

        logger.info(f"Creating index {self.index_name} with hierarchical mapping")
        self.es.indices.create(index=self.index_name, body=mapping)

    def _bulk_index(self, df: DataFrame) -> None:
        logger.info("Preparing to index chunk stream...")
        existing_ids = set()
        if self.incremental:
            try:
                logger.info("Loading existing IDs from Elasticsearch for incremental mode...")
                result = helpers.scan(
                    self.es,
                    index=self.index_name,
                    query={"query": {"match_all": {}}, "_source": False},
                    scroll="5m",
                )
                existing_ids = {hit["_id"] for hit in result}
                logger.info(f"Found {len(existing_ids):,} existing documents")
            except Exception as exc:
                logger.warning(f"Failed to load existing IDs, continuing full indexing: {exc}")

        logger.info(f"Loading embedding model: {self.model_name}")
        import torch
        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Using device: {device}")
        model = _load_embedding_model(self.model_name)
        if hasattr(model, '_target_device'):
            model = model.to(device)
        logger.info("Streaming chunks from Spark to driver in batches...")

        def row_batches() -> Any:
            iterator = df.toLocalIterator()
            while True:
                batch = list(islice(iterator, self.batch_size))
                if not batch:
                    break
                yield batch

        indexed_count = 0
        seen_count = 0
        skipped_existing_count = 0
        batch_number = 0

        for batch_rows in row_batches():
            batch_number += 1
            seen_count += len(batch_rows)
            batch_rows = [
                row.asDict(recursive=True) if hasattr(row, "asDict") else row
                for row in batch_rows
            ]
            if existing_ids:
                original_batch_size = len(batch_rows)
                batch_rows = [row for row in batch_rows if row["chunk_id"] not in existing_ids]
                skipped_existing_count += original_batch_size - len(batch_rows)
                if not batch_rows:
                    if batch_number % 10 == 0:
                        logger.info(
                            f"Progress: scanned={seen_count:,}, indexed={indexed_count:,}, skipped_existing={skipped_existing_count:,}"
                        )
                    continue

            texts = [f"passage: {(r['chunk_text'] or '').strip()}" for r in batch_rows]
            # Increase batch size for faster processing (reduce overhead)
            embeddings = model.encode(
                texts,
                normalize_embeddings=True,
                show_progress_bar=False,
                batch_size=64,  # Increased from 32 for better throughput
                convert_to_numpy=True,
            )

            actions: List[Dict[str, Any]] = []
            for row, embedding in zip(batch_rows, embeddings):
                chapter_range = self._build_int_range(row.get("chapter_page_start"), row.get("chapter_page_end"))
                section_range = self._build_int_range(row.get("section_page_start"), row.get("section_page_end"))
                actions.append(
                    {
                        "_index": self.index_name,
                        "_id": row["chunk_id"],
                        "_source": {
                            "chunk_id": row["chunk_id"],
                            "resource_uid": row["resource_uid"],
                            "asset_uid": row["asset_uid"],
                            "source_system": row["source_system"],
                            "title": row.get("title"),
                            "source_url": row.get("source_url"),
                            "asset_path": row.get("asset_path"),
                            "file_name": row.get("file_name"),
                            "minio_url": row.get("minio_url"),
                            "chunk_text": row["chunk_text"],
                            "page_no": row["page_no"],
                            "chunk_order": row["chunk_order"],
                            "lang": row["lang"],
                            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                            "chunk_type": row["chunk_type"],
                            "chunk_tier": row["chunk_tier"],
                            "chapter_id": row["chapter_id"],
                            "chapter_title": row["chapter_title"],
                            "chapter_number": row["chapter_number"],
                            "chapter_page_start": row["chapter_page_start"],
                            "chapter_page_end": row["chapter_page_end"],
                            "chapter_page_range": chapter_range,
                            "section_id": row["section_id"],
                            "section_title": row["section_title"],
                            "section_number": row["section_number"],
                            "section_page_start": row["section_page_start"],
                            "section_page_end": row["section_page_end"],
                            "section_page_range": section_range,
                            "parent_chunk_id": row["parent_chunk_id"],
                            "has_children": row["has_children"],
                            "is_summary": row["is_summary"],
                            "summary_method": row["summary_method"],
                            "embedding": embedding.tolist(),
                        },
                    }
                )

            success, failed = helpers.bulk(
                self.es,
                actions,
                raise_on_error=False,
                request_timeout=self.timeout,
            )
            indexed_count += success
            if failed:
                logger.warning(f"Batch {batch_number}: {failed} failures")
            if batch_number % 10 == 0:
                logger.info(
                    f"Progress: scanned={seen_count:,}, indexed={indexed_count:,}, skipped_existing={skipped_existing_count:,}"
                )

        if indexed_count == 0 and skipped_existing_count > 0:
            logger.info(f"No new chunks to index. Skipped {skipped_existing_count:,} existing documents.")
            return

        logger.info(f"Indexed {indexed_count:,} chunks")
        self.es.indices.refresh(index=self.index_name)

    def _build_int_range(self, start: Any, end: Any) -> Optional[Dict[str, int]]:
        try:
            if start is None or end is None:
                return None
            s = int(start)
            e = int(end)
            if e < s:
                e = s
            return {"gte": s, "lte": e}
        except Exception:
            return None

    def run(self) -> None:
        logger.info("=" * 80)
        logger.info("Chatbot Elasticsearch Sync")
        logger.info(f"ES host: {self.es_host}")
        logger.info(f"Index: {self.index_name}")
        logger.info(f"Embedding model: {self.model_name} ({self.embedding_dim} dims)")
        logger.info(f"Incremental mode: {self.incremental}")
        logger.info(f"Index all tiers: {self.index_all_tiers}")
        logger.info("=" * 80)
        try:
            self._ensure_index()
            df = self._build_index_dataframe()
            if df is None:
                logger.warning("No data to index.")
                return
            self._bulk_index(df)
            logger.info("Elasticsearch sync completed.")
        finally:
            self.spark.stop()


if __name__ == "__main__":
    ChatbotElasticsearchSync().run()
