#!/usr/bin/env python3
"""
Chatbot-Optimized Elasticsearch Sync
=====================================
Streamlined version focused on RAG retrieval:
- Index chunk-level data from Silver (oer_chunks) WITH embeddings
- Metadata from Gold layer for filtering
- No PDF re-parsing (use pre-chunked data)
- Incremental upsert: only sync new/changed documents
- Embeddings generated at Silver layer, just copy to ES

Flow:
    Silver (oer_chunks with embeddings) 
        → Gold (metadata) 
        → ES (hybrid search ready)

Model: paraphrase-multilingual-MiniLM-L12-v2 (384-dim, EN+VI)
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ChatbotElasticsearchSync:
    """Sync Gold/Silver data to Elasticsearch for chatbot RAG."""

    def __init__(self):
        # Iceberg catalogs
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.silver_catalog = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.silver_database = os.getenv("SILVER_DATABASE", "default")
        self.gold_catalog = os.getenv("ICEBERG_GOLD_CATALOG", "gold")
        self.gold_database = os.getenv("GOLD_DATABASE", "analytics")

        # Table references
        self.dim_resources = f"{self.gold_catalog}.{self.gold_database}.dim_oer_resources"
        self.fact_resources = f"{self.gold_catalog}.{self.gold_database}.fact_oer_resources"
        self.dim_sources = f"{self.gold_catalog}.{self.gold_database}.dim_sources"
        self.dim_languages = f"{self.gold_catalog}.{self.gold_database}.dim_languages"
        self.dim_date = f"{self.gold_catalog}.{self.gold_database}.dim_date"
        self.silver_documents = f"{self.silver_catalog}.{self.silver_database}.oer_documents"
        self.silver_chunks = f"{self.silver_catalog}.{self.silver_database}.oer_chunks"

        # Elasticsearch config
        self.es_host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        self.index_name = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
        self.batch_size = int(os.getenv("ELASTICSEARCH_BATCH_SIZE", "300"))
        self.timeout = int(os.getenv("ELASTICSEARCH_TIMEOUT", "180"))
        self.recreate = os.getenv("ELASTICSEARCH_RECREATE", "0") in {"1", "true", "yes"}
        self.incremental = os.getenv("ELASTICSEARCH_INCREMENTAL", "1") in {"1", "true", "yes"}

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
        """Create Spark session for reading Iceberg tables."""
        os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
        os.environ.setdefault("SPARK_MASTER", os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        os.environ.setdefault("SPARK_DRIVER_HOST", "oer-airflow-scraper")
        os.environ.setdefault("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0")
        os.environ.pop("JAVA_TOOL_OPTIONS", None)

        jars_dir = Path("/opt/airflow/jars")
        local_jars = [
            jars_dir / "iceberg-spark-runtime-3.5_2.12-1.9.2.jar",
            jars_dir / "hadoop-aws-3.3.4.jar",
            jars_dir / "aws-java-sdk-bundle-1.12.262.jar",
        ]
        existing_jars = [str(j) for j in local_jars if j.exists()]
        packages = (
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )

        builder = (
            SparkSession.builder.appName("Chatbot-ES-Sync")
            .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.silver_catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.silver_catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.silver_catalog}.warehouse", f"s3a://{self.bucket}/silver/")
            .config(f"spark.sql.catalog.{self.gold_catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.gold_catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.gold_catalog}.warehouse", f"s3a://{self.bucket}/gold/")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "oer-airflow-scraper"))
            .config("spark.driver.bindAddress", os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"))
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
        )

        if existing_jars:
            builder = builder.config("spark.jars", ",".join(existing_jars))
            logger.info(f"[Spark] Using local JARs: {len(existing_jars)} files")
        else:
            builder = builder.config("spark.jars.packages", packages)
            logger.info("[Spark] Using Maven packages (no local JARs found)")

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _load_table(self, table_name: str) -> Optional[DataFrame]:
        """Load Iceberg table, return None if empty/missing."""
        try:
            df = self.spark.table(table_name)
            count = df.count()
            if count == 0:
                logger.warning(f"Table {table_name} is empty")
                return None
            logger.info(f"Loaded {table_name}: {count:,} records")
            return df
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            return None

    def _build_index_dataframe(self) -> Optional[DataFrame]:
        """Build final DataFrame for Elasticsearch indexing."""
        logger.info("Building index DataFrame from Gold/Silver...")

        # Load tables
        dim_resources = self._load_table(self.dim_resources)
        fact_resources = self._load_table(self.fact_resources)
        dim_sources = self._load_table(self.dim_sources)
        dim_languages = self._load_table(self.dim_languages)
        dim_date = self._load_table(self.dim_date)
        documents = self._load_table(self.silver_documents)
        chunks = self._load_table(self.silver_chunks)

        if dim_resources is None:
            logger.error("dim_oer_resources is required but missing")
            return None

        # Start with dim_resources
        df = dim_resources.alias("dim")

        # Join fact for metrics
        if fact_resources is not None:
            fact = fact_resources.select(
                "resource_key",
                "source_key",
                "language_key",
                "publication_date_key",
                F.col("data_quality_score").alias("quality_score"),
                F.col("matched_subjects_count").alias("subjects_count"),
            ).alias("fact")
            df = df.join(fact, "resource_key", "left")

            # Join source/language dimensions
            if dim_sources is not None:
                df = df.join(
                    dim_sources.select("source_key", F.col("source_code").alias("source_system")),
                    "source_key",
                    "left",
                )
            if dim_languages is not None:
                df = df.join(
                    dim_languages.select("language_key", F.col("language_code").alias("language")),
                    "language_key",
                    "left",
                )
            if dim_date is not None:
                df = df.join(
                    dim_date.select(
                        F.col("date_key").alias("pub_key"),
                        F.col("date").alias("publication_date"),
                    ),
                    df.publication_date_key == F.col("pub_key"),
                    "left",
                ).drop("pub_key")

        # Aggregate documents
        if documents is not None:
            doc_agg = (
                documents.groupBy("resource_uid")
                .agg(
                    F.collect_list(
                        F.struct(
                            "asset_uid",
                            F.col("asset_path").alias("path"),
                            "file_name",
                            "mime_type",
                            "size_bytes",
                        )
                    ).alias("doc_documents"),
                    F.count("*").alias("doc_count"),
                )
                .select(
                    F.col("resource_uid").alias("doc_uid"),
                    "doc_documents",
                    "doc_count",
                )
            )
            df = df.join(doc_agg, df.resource_key == F.col("doc_uid"), "left").drop("doc_uid")

        # Aggregate chunks (include embeddings from Silver)
        if chunks is not None:
            chunk_agg = (
                chunks.groupBy("resource_uid")
                .agg(
                    F.collect_list(
                        F.struct(
                            "chunk_id",
                            "asset_uid",
                            "page_no",
                            "chunk_order",
                            F.col("chunk_text").alias("text"),
                            "token_count",
                            "lang",
                            "embedding",  # Include embedding from Silver
                        )
                    ).alias("chunk_chunk_items"),
                    F.count("*").alias("chunk_cnt"),
                    F.max("updated_at").alias("chunk_updated_at"),
                    # Use first chunk's embedding as doc-level embedding (simple approach)
                    # This is faster than averaging all chunks and often good enough
                    F.first(F.col("embedding"), ignorenulls=True).alias("doc_embedding"),
                )
                .select(
                    F.col("resource_uid").alias("chunk_uid"),
                    "chunk_chunk_items",
                    "chunk_cnt",
                    "chunk_updated_at",
                    "doc_embedding",
                )
            )
            df = df.join(chunk_agg, df.resource_key == F.col("chunk_uid"), "left").drop("chunk_uid")

        # Select final columns (include doc_embedding from Silver)
        df = df.select(
            F.col("resource_key").alias("_id"),
            "resource_id",
            "title",
            "description",
            "source_url",
            F.coalesce("source_system", F.lit("unknown")).alias("source_system"),
            F.coalesce("language", F.lit("en")).alias("language"),
            F.col("matched_subjects.subject_id").alias("subject_ids"),
            F.col("matched_subjects.subject_name").alias("subjects"),
            F.coalesce("program_ids", F.array()).alias("program_ids"),
            "license_name",
            "license_url",
            "publication_date",
            F.coalesce("quality_score", F.lit(0.5)).alias("quality_score"),
            F.coalesce("subjects_count", F.lit(0)).alias("subjects_count"),
            F.coalesce("doc_count", F.lit(0)).alias("document_count"),
            F.coalesce("chunk_cnt", F.lit(0)).alias("chunk_count"),
            F.coalesce("doc_documents", F.array()).alias("documents"),
            F.coalesce("chunk_chunk_items", F.array()).alias("chunk_items"),
            F.coalesce("chunk_updated_at", F.current_timestamp()).alias("updated_at"),
            "doc_embedding",  # Doc-level embedding (avg of chunks)
        )

        # Build search_text: title + description + subjects + chunk snippets
        df = df.withColumn(
            "search_text",
            F.concat_ws(
                " ",
                F.col("title"),
                F.col("description"),
                F.array_join(F.coalesce("subjects", F.array()), " "),
                F.expr(
                    "array_join(transform(slice(chunk_items, 1, 10), x -> substring(x.text, 1, 300)), ' ')"
                ),
            ),
        )

        logger.info(f"Index DataFrame built: {df.count():,} documents")
        return df

    def _ensure_index(self):
        """Create ES index with chatbot-optimized mapping."""
        if self.es.indices.exists(index=self.index_name):
            if self.recreate:
                logger.info(f"Deleting existing index: {self.index_name}")
                self.es.indices.delete(index=self.index_name)
            else:
                logger.info(f"Index {self.index_name} exists, skipping creation")
                return

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
                    "resource_id": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "standard"},
                    "description": {"type": "text", "analyzer": "standard"},
                    "search_text": {"type": "text", "analyzer": "standard"},
                    "source_url": {"type": "keyword"},
                    "source_system": {"type": "keyword"},
                    "language": {"type": "keyword"},
                    "subject_ids": {"type": "keyword"},
                    "subjects": {"type": "keyword"},
                    "program_ids": {"type": "keyword"},
                    "license_name": {"type": "keyword"},
                    "license_url": {"type": "keyword"},
                    "publication_date": {"type": "date"},
                    "quality_score": {"type": "float"},
                    "subjects_count": {"type": "integer"},
                    "document_count": {"type": "integer"},
                    "chunk_count": {"type": "integer"},
                    "updated_at": {"type": "date"},
                    "documents": {
                        "type": "nested",
                        "properties": {
                            "asset_uid": {"type": "keyword"},
                            "path": {"type": "keyword"},
                            "file_name": {"type": "text"},
                            "mime_type": {"type": "keyword"},
                            "size_bytes": {"type": "long"},
                        },
                    },
                    "chunk_items": {
                        "type": "nested",
                        "properties": {
                            "chunk_id": {"type": "keyword"},
                            "asset_uid": {"type": "keyword"},
                            "page_no": {"type": "integer"},
                            "chunk_order": {"type": "integer"},
                            "text": {"type": "text", "analyzer": "standard"},
                            "token_count": {"type": "integer"},
                            "lang": {"type": "keyword"},
                            "embedding": {
                                "type": "dense_vector",
                                "dims": 384,  # paraphrase-multilingual-MiniLM-L12-v2
                                "index": True,
                                "similarity": "cosine",
                            },
                        },
                    },
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 384,  # Doc-level (avg of chunks)
                        "index": True,
                        "similarity": "cosine",
                    },
                },
            },
        }

        logger.info(f"Creating index: {self.index_name}")
        self.es.indices.create(index=self.index_name, body=mapping)

    def _bulk_index(self, df: DataFrame):
        """Bulk index documents to Elasticsearch."""
        logger.info(f"Indexing {df.count():,} documents (batch={self.batch_size})...")

        # Get watermark for incremental
        watermark = None
        if self.incremental:
            try:
                resp = self.es.search(
                    index=self.index_name,
                    body={
                        "size": 0,
                        "aggs": {"max_updated": {"max": {"field": "updated_at"}}},
                    },
                )
                watermark_ms = resp["aggregations"]["max_updated"].get("value")
                if watermark_ms:
                    watermark = datetime.fromtimestamp(watermark_ms / 1000)
                    logger.info(f"Incremental mode: watermark = {watermark}")
            except Exception as e:
                logger.warning(f"Failed to get watermark: {e}")

        # Filter incremental
        if watermark:
            df = df.filter(F.col("updated_at") > F.lit(watermark))
            count = df.count()
            if count == 0:
                logger.info("No new documents to index (incremental)")
                return
            logger.info(f"Incremental: indexing {count:,} new/updated documents")

        # Convert to actions
        def _yield_actions():
            for row in df.toLocalIterator():
                doc = row.asDict(recursive=True)
                doc_id = doc.pop("_id")
                
                # Map doc_embedding to embedding field
                if doc.get("doc_embedding"):
                    doc["embedding"] = doc.pop("doc_embedding")
                else:
                    doc.pop("doc_embedding", None)
                
                # Clean None values
                doc = {k: v for k, v in doc.items() if v is not None}
                yield {"_index": self.index_name, "_id": doc_id, "_source": doc}

        success, errors = helpers.bulk(
            self.es,
            _yield_actions(),
            chunk_size=self.batch_size,
            request_timeout=self.timeout,
            raise_on_error=False,
        )

        # Log embedding stats
        with_emb = df.filter(F.col("doc_embedding").isNotNull()).count()
        logger.info(f"Indexed {success} documents ({with_emb} with embeddings)")
        if errors:
            logger.error(f"Failed to index {len(errors)} documents")

    def _export_for_embedding(self, output_path: str):
        """Export texts for embedding generation."""
        logger.info(f"Exporting texts for embedding → {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        query = {
            "size": 500,
            "_source": [
                "resource_id",
                "title",
                "description",
                "source_system",
                "language",
                "subjects",
                "chunk_items.chunk_id",
                "chunk_items.asset_uid",
                "chunk_items.page_no",
                "chunk_items.chunk_order",
                "chunk_items.text",
                "chunk_items.token_count",
                "chunk_items.lang",
            ],
            "query": {"match_all": {}},
        }

        count = 0
        with open(output_path, "w", encoding="utf-8") as f:
            resp = self.es.search(index=self.index_name, body=query, scroll="5m")
            scroll_id = resp["_scroll_id"]

            while True:
                hits = resp.get("hits", {}).get("hits", [])
                if not hits:
                    break

                for hit in hits:
                    src = hit["_source"]
                    title = src.get("title", "")
                    desc = (src.get("description") or "")[:500]
                    subjects = " ".join(src.get("subjects") or [])
                    doc_text = f"{title}. {desc}. {subjects}".strip()

                    chunks = []
                    for ch in src.get("chunk_items") or []:
                        text = (ch.get("text") or "").strip()
                        if text:
                            chunks.append(
                                {
                                    "chunk_id": ch.get("chunk_id"),
                                    "asset_uid": ch.get("asset_uid"),
                                    "page_no": ch.get("page_no"),
                                    "chunk_order": ch.get("chunk_order"),
                                    "text": text[:2000],
                                    "lang": ch.get("lang", "en"),
                                }
                            )

                    record = {
                        "_id": hit["_id"],
                        "resource_id": src.get("resource_id"),
                        "title": title,
                        "source_system": src.get("source_system"),
                        "language": src.get("language"),
                        "subjects": src.get("subjects") or [],
                        "doc_text": doc_text,
                        "chunks": chunks,
                    }
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1

                resp = self.es.scroll(scroll_id=scroll_id, scroll="5m")

            try:
                self.es.clear_scroll(scroll_id=scroll_id)
            except:
                pass

        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logger.info(f"Exported {count} records ({size_mb:.1f} MB) → {output_path}")
        logger.info("Next: Generate embeddings with multilingual model (e.g., distiluse-base-multilingual-cased-v2)")
        logger.info("Then import back: python import_embeddings.py <enriched_file.jsonl>")

    def run(self):
        """Run full sync pipeline."""
        logger.info("=" * 80)
        logger.info("Starting Chatbot Elasticsearch Sync")
        logger.info("=" * 80)

        df = self._build_index_dataframe()
        if df is None:
            logger.error("Failed to build index DataFrame")
            return

        self._ensure_index()
        self._bulk_index(df)

        # Log embedding coverage
        total = df.count()
        with_emb = df.filter(F.col("doc_embedding").isNotNull()).count()
        logger.info(f"[Embedding Coverage] {with_emb}/{total} docs ({with_emb*100//total if total > 0 else 0}%)")
        logger.info("Note: Run Silver embedding pipeline to generate embeddings")

        logger.info("=" * 80)
        logger.info("Chatbot Elasticsearch Sync Complete")
        logger.info("=" * 80)


def main():
    sync = ChatbotElasticsearchSync()
    sync.run()


if __name__ == "__main__":
    main()
