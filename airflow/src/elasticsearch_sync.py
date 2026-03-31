#!/usr/bin/env python3
"""
Chatbot-Optimized Elasticsearch Sync (SIMPLIFIED)
==================================================
Streamlined version focused on RAG retrieval:
- Index MINIMAL fields for chatbot (chunk_text, embedding, metadata)
- Generate E5-Base embeddings (768 dims) from Silver chunks
- Direct indexing to ES (no Gold layer complexity)
- Incremental mode: only process new chunks

Architecture:
    Silver (oer_chunks) → E5-Base (768d) → Elasticsearch (hybrid search)
    
Model: intfloat/multilingual-e5-base (768-dim, multilingual)
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from elasticsearch import Elasticsearch, helpers

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
except ImportError:
    SparkSession = DataFrame = F = None

# Lazy import for sentence-transformers (heavy)
SentenceTransformer = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _load_embedding_model(model_name: str):
    """Lazy load sentence-transformers model."""
    global SentenceTransformer
    if SentenceTransformer is None:
        from sentence_transformers import SentenceTransformer as ST
        SentenceTransformer = ST
    return SentenceTransformer(model_name)


class ChatbotElasticsearchSync:
    """Simplified Elasticsearch sync: chunks → E5-Base → ES."""

    def __init__(self):
        # Iceberg config (ONLY Silver needed)
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.silver_catalog = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.silver_database = os.getenv("SILVER_DATABASE", "default")
        self.silver_chunks = f"{self.silver_catalog}.{self.silver_database}.oer_chunks"

        # Elasticsearch config
        self.es_host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        self.index_name = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
        self.batch_size = int(os.getenv("ELASTICSEARCH_BATCH_SIZE", "100"))
        self.timeout = int(os.getenv("ELASTICSEARCH_TIMEOUT", "180"))
        self.recreate = os.getenv("ELASTICSEARCH_RECREATE", "0") in {"1", "true", "yes"}
        self.incremental = os.getenv("ELASTICSEARCH_INCREMENTAL", "1") in {"1", "true", "yes"}

        # Embedding config
        self.model_name = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-base")
        self.embedding_dim = 768
        self.model_cache = os.getenv("TRANSFORMERS_CACHE", "/tmp/transformers_cache")

        # Connect to ES
        es_user = os.getenv("ELASTICSEARCH_USER")
        es_password = os.getenv("ELASTICSEARCH_PASSWORD")
        auth = (es_user, es_password) if es_user and es_password else None
        self.es = Elasticsearch(
            hosts=[self.es_host],
            basic_auth=auth,
            verify_certs=self.es_host.startswith("https"),
            request_timeout=self.timeout,
        )

        # Spark session
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create SparkSession with Iceberg configs."""
        logger.info("Creating Spark session...")
        
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        minio_access = os.getenv("MINIO_ACCESS_KEY", "admin")
        minio_secret = os.getenv("MINIO_SECRET_KEY", "password")

        spark = (
            SparkSession.builder.appName("ChatbotESSync")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.silver_catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.silver_catalog}.type", "hadoop")
            .config(
                f"spark.sql.catalog.{self.silver_catalog}.warehouse",
                f"s3a://{self.bucket}/{self.silver_catalog}",
            )
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}")
            .config("spark.hadoop.fs.s3a.access.key", minio_access)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

        logger.info("Spark session created")
        return spark

    def _build_index_dataframe(self) -> Optional[DataFrame]:
        """Build minimal DataFrame from Silver chunks for chatbot."""
        logger.info("Loading chunks from Silver for embedding generation...")

        # Load ONLY chunks (no Gold layer)
        try:
            chunks = self.spark.table(self.silver_chunks)
            count = chunks.count()
            if count == 0:
                logger.warning("No chunks found in Silver layer")
                return None
            logger.info(f"Loaded {count:,} chunks from {self.silver_chunks}")
        except Exception as e:
            logger.error(f"Failed to load chunks: {e}")
            return None

        # Select MINIMAL fields for chatbot
        df = chunks.select(
            F.col("chunk_id").alias("_id"),  # Use chunk_id as ES _id
            "chunk_id",
            "resource_uid",
            "chunk_text",
            "page_no",
            "lang",
            "updated_at",
        )

        logger.info(f"Prepared {df.count():,} chunks for indexing (minimal fields)")
        return df

    def _ensure_index(self):
        """Create ES index with MINIMAL chatbot-optimized mapping."""
        if self.es.indices.exists(index=self.index_name):
            if self.recreate:
                logger.info(f"Deleting existing index: {self.index_name}")
                self.es.indices.delete(index=self.index_name)
            else:
                logger.info(f"Index {self.index_name} exists, skipping creation")
                return

        # MINIMAL mapping for chatbot (NO nested, NO extra fields)
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "default": {
                            "type": "standard",
                            "stopwords": "_none_",
                        }
                    }
                },
            },
            "mappings": {
                "properties": {
                    # Core fields for chatbot
                    "chunk_id": {"type": "keyword"},
                    "resource_uid": {"type": "keyword"},
                    "chunk_text": {"type": "text", "analyzer": "standard"},
                    "page_no": {"type": "integer"},
                    "lang": {"type": "keyword"},
                    "updated_at": {"type": "date"},
                    
                    # E5-Base embedding (768 dims)
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 768,
                        "index": True,
                        "similarity": "cosine",
                    },
                },
            },
        }

        logger.info(f"Creating MINIMAL index: {self.index_name} (E5-Base 768 dims)")
        self.es.indices.create(index=self.index_name, body=mapping)

    def _bulk_index(self, df: DataFrame):
        """Generate E5-Base embeddings and bulk index to Elasticsearch."""
        logger.info(f"Indexing {df.count():,} chunks with E5-Base embeddings...")

        # Get existing chunk_ids for incremental mode
        existing_ids = set()
        if self.incremental:
            try:
                logger.info("Fetching existing chunk_ids from ES for incremental mode...")
                result = helpers.scan(
                    self.es,
                    index=self.index_name,
                    query={"query": {"match_all": {}}, "_source": False},
                    scroll="5m",
                )
                existing_ids = {hit["_id"] for hit in result}
                logger.info(f"Found {len(existing_ids):,} existing chunks in ES")
            except Exception as e:
                logger.warning(f"Failed to fetch existing IDs: {e}")

        # Filter to only NEW chunks
        if existing_ids:
            df = df.filter(~F.col("chunk_id").isin(list(existing_ids)))
            count = df.count()
            if count == 0:
                logger.info("✅ No new chunks to index (incremental mode)")
                return
            logger.info(f"Incremental: processing {count:,} NEW chunks")

        # Load E5-Base model
        logger.info(f"Loading embedding model: {self.model_name}")
        model = _load_embedding_model(self.model_name)

        # Collect chunks to process
        chunks_data = df.collect()
        total = len(chunks_data)
        logger.info(f"Generating embeddings for {total:,} chunks (batch={self.batch_size})")

        # Process in batches
        indexed_count = 0
        for i in range(0, total, self.batch_size):
            batch = chunks_data[i : i + self.batch_size]
            
            # Generate embeddings for batch
            texts = [f"passage: {row['chunk_text']}" for row in batch]
            embeddings = model.encode(
                texts,
                normalize_embeddings=True,
                show_progress_bar=False,
                batch_size=32,
            )

            # Build ES actions
            actions = []
            for row, embedding in zip(batch, embeddings):
                action = {
                    "_index": self.index_name,
                    "_id": row["chunk_id"],
                    "_source": {
                        "chunk_id": row["chunk_id"],
                        "resource_uid": row["resource_uid"],
                        "chunk_text": row["chunk_text"],
                        "page_no": row["page_no"],
                        "lang": row["lang"],
                        "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                        "embedding": embedding.tolist(),
                    },
                }
                actions.append(action)

            # Bulk index
            success, failed = helpers.bulk(
                self.es,
                actions,
                raise_on_error=False,
                request_timeout=self.timeout,
            )
            indexed_count += success
            
            if failed:
                logger.warning(f"Batch {i // self.batch_size + 1}: {failed} failures")
            
            if (i + self.batch_size) % 1000 == 0 or (i + self.batch_size) >= total:
                logger.info(f"Progress: {indexed_count:,}/{total:,} chunks indexed")

        logger.info(f"✅ Indexed {indexed_count:,} chunks with E5-Base embeddings")
        
        # Refresh index
        self.es.indices.refresh(index=self.index_name)

    def run(self):
        """Main execution flow."""
        logger.info("=" * 80)
        logger.info("Chatbot Elasticsearch Sync (SIMPLIFIED)")
        logger.info("=" * 80)
        logger.info(f"ES Host: {self.es_host}")
        logger.info(f"Index: {self.index_name}")
        logger.info(f"Model: {self.model_name} ({self.embedding_dim} dims)")
        logger.info(f"Incremental: {self.incremental}")
        logger.info(f"Recreate: {self.recreate}")
        logger.info("=" * 80)

        try:
            # 1. Ensure index exists
            self._ensure_index()

            # 2. Build DataFrame (minimal fields from Silver)
            df = self._build_index_dataframe()
            if df is None:
                logger.warning("No data to index")
                return

            # 3. Generate embeddings & bulk index
            self._bulk_index(df)

            logger.info("=" * 80)
            logger.info("✅ Chatbot Elasticsearch Sync Complete")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Sync failed: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    sync = ChatbotElasticsearchSync()
    sync.run()
