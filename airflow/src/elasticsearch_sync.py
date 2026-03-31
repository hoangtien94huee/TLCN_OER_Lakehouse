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
        self.batch_size = int(os.getenv("ELASTICSEARCH_BATCH_SIZE", "100"))
        self.timeout = int(os.getenv("ELASTICSEARCH_TIMEOUT", "180"))
        self.recreate = os.getenv("ELASTICSEARCH_RECREATE", "0").lower() in {"1", "true", "yes"}
        self.incremental = os.getenv("ELASTICSEARCH_INCREMENTAL", "1").lower() in {"1", "true", "yes"}
        self.index_all_tiers = os.getenv("HIERARCHICAL_INDEX_ALL_TIERS", "1").lower() in {"1", "true", "yes"}

        self.model_name = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-base")
        self.embedding_dim = int(os.getenv("EMBEDDING_DIM", "768"))

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
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        endpoint = minio_endpoint if minio_endpoint.startswith(("http://", "https://")) else f"http://{minio_endpoint}"

        return (
            SparkSession.builder.appName("ChatbotESSync")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.silver_catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.silver_catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.silver_catalog}.warehouse", f"s3a://{self.bucket}/silver/")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

    def _build_index_dataframe(self) -> Optional[DataFrame]:
        logger.info("Loading chunks from Silver...")
        try:
            chunks = self.spark.table(self.silver_chunks)
        except Exception as exc:
            logger.error(f"Failed to load chunks table {self.silver_chunks}: {exc}")
            return None

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

        if "source_system" not in chunks.columns and self._table_exists(self.silver_documents):
            docs = self.spark.table(self.silver_documents).select(
                F.col("asset_uid").alias("_doc_asset_uid"),
                F.col("source_system").alias("_doc_source_system"),
            )
            chunks = chunks.join(docs, chunks.asset_uid == docs._doc_asset_uid, "left").drop("_doc_asset_uid")
            chunks = chunks.withColumn("source_system", F.col("_doc_source_system")).drop("_doc_source_system")
        elif "source_system" not in chunks.columns:
            chunks = chunks.withColumn("source_system", F.lit(None))

        df = chunks.select(
            F.col("chunk_id").alias("_id"),
            "chunk_id",
            "resource_uid",
            "asset_uid",
            "source_system",
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

        logger.info(f"Prepared {df.count():,} chunks for Elasticsearch indexing")
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
        logger.info(f"Preparing to index {df.count():,} chunks...")
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

        if existing_ids:
            df = df.filter(~F.col("chunk_id").isin(list(existing_ids)))
            remaining = df.count()
            if remaining == 0:
                logger.info("No new chunks to index.")
                return
            logger.info(f"Incremental mode: {remaining:,} new chunks")

        logger.info(f"Loading embedding model: {self.model_name}")
        model = _load_embedding_model(self.model_name)

        rows = df.collect()
        total = len(rows)
        logger.info(f"Generating embeddings for {total:,} chunks...")

        indexed_count = 0
        for offset in range(0, total, self.batch_size):
            batch_rows = rows[offset : offset + self.batch_size]
            texts = [f"passage: {(r['chunk_text'] or '').strip()}" for r in batch_rows]
            embeddings = model.encode(
                texts,
                normalize_embeddings=True,
                show_progress_bar=False,
                batch_size=32,
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
                logger.warning(f"Batch {(offset // self.batch_size) + 1}: {failed} failures")
            if (offset + self.batch_size) % 1000 == 0 or (offset + self.batch_size) >= total:
                logger.info(f"Progress: {indexed_count:,}/{total:,}")

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
