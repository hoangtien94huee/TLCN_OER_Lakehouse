#!/usr/bin/env python3
"""
Silver Layer Embedding Generator
=================================
Generate embeddings for oer_chunks at Silver layer.
Incremental: only process chunks without embeddings.

Usage:
    # Standalone
    python silver_embeddings.py

    # From Airflow DAG
    from src.silver_embeddings import SilverEmbeddingGenerator
    generator = SilverEmbeddingGenerator()
    generator.run()
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

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


class SilverEmbeddingGenerator:
    """Generate embeddings for Silver oer_chunks table."""

    # Model config
    MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"  # 384-dim, fast, multilingual
    EMBEDDING_DIM = 384
    BATCH_SIZE = 256  # Increased from 64 (8GB RAM available)

    def __init__(self):
        # Iceberg config
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.catalog_name = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.database_name = os.getenv("SILVER_DATABASE", "default")
        self.chunks_table = f"{self.catalog_name}.{self.database_name}.oer_chunks"

        # Spark session
        self.spark = self._create_spark_session()

        # Embedding model (lazy loaded)
        self._model = None

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session for Iceberg operations."""
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
            SparkSession.builder.appName("Silver-Embedding-Generator")
            .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.catalog_name}.warehouse", f"s3a://{self.bucket}/silver/")
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
            logger.info("[Spark] Using Maven packages")

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark

    @property
    def model(self):
        """Lazy load embedding model."""
        if self._model is None:
            logger.info(f"Loading embedding model: {self.MODEL_NAME}")
            self._model = _load_embedding_model(self.MODEL_NAME)
            logger.info(f"Model loaded. Embedding dimension: {self.EMBEDDING_DIM}")
        return self._model

    def _table_exists(self, table_name: str) -> bool:
        """Check if Iceberg table exists."""
        try:
            self.spark.table(table_name)
            return True
        except Exception:
            return False

    def _ensure_embedding_columns(self):
        """Add embedding columns to oer_chunks if not exist."""
        if not self._table_exists(self.chunks_table):
            logger.warning(f"Table {self.chunks_table} does not exist. Skipping embedding.")
            return False
        
        try:
            existing_cols = {c.lower() for c, _ in self.spark.table(self.chunks_table).dtypes}

            if "embedding" not in existing_cols:
                logger.info("Adding 'embedding' column to oer_chunks")
                self.spark.sql(f"""
                    ALTER TABLE {self.chunks_table} 
                    ADD COLUMN embedding ARRAY<FLOAT>
                """)

            if "embedded_at" not in existing_cols:
                logger.info("Adding 'embedded_at' column to oer_chunks")
                self.spark.sql(f"""
                    ALTER TABLE {self.chunks_table} 
                    ADD COLUMN embedded_at TIMESTAMP
                """)

            logger.info("Embedding columns ready")
            return True

        except Exception as e:
            logger.error(f"Failed to ensure embedding columns: {e}")
            raise

    def _get_chunks_to_embed(self) -> Optional[DataFrame]:
        """Get chunks that need embedding (new or text changed)."""
        if not self._table_exists(self.chunks_table):
            logger.warning(f"Table {self.chunks_table} does not exist.")
            return None
        
        try:
            df = self.spark.table(self.chunks_table)
            total = df.count()
            
            if total == 0:
                logger.info("No chunks in table")
                return None

            # Filter: embedding is NULL
            pending = df.filter(F.col("embedding").isNull())
            pending_count = pending.count()

            logger.info(f"Total chunks: {total:,}")
            logger.info(f"Pending embedding: {pending_count:,}")

            return pending

        except Exception as e:
            logger.error(f"Failed to get chunks: {e}")
            raise

    def _generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a batch of texts."""
        if not texts:
            return []

        embeddings = self.model.encode(
            texts,
            batch_size=self.BATCH_SIZE,
            show_progress_bar=False,
            convert_to_numpy=True,
        )

        return [emb.tolist() for emb in embeddings]

    def _process_embeddings(self, chunks_df: DataFrame) -> DataFrame:
        """Generate embeddings for chunks DataFrame on driver (single-threaded)."""
        count = chunks_df.count()
        if count == 0:
            logger.info("No chunks to process")
            return chunks_df

        logger.info(f"Generating embeddings for {count:,} chunks (single-thread mode)...")

        # Collect chunks to driver for embedding (sentence-transformers runs on driver)
        # For large datasets, consider using Spark UDF with model broadcast
        chunk_records = []
        for row in chunks_df.toLocalIterator():
            chunk_records.append(row.asDict(recursive=True))

        # Batch process
        now = datetime.utcnow()
        processed = 0
        batch_texts = []
        batch_indices = []

        for idx, record in enumerate(chunk_records):
            text = record.get("chunk_text") or ""
            if text.strip():
                batch_texts.append(text[:2000])  # Limit text length
                batch_indices.append(idx)

            # Process batch
            if len(batch_texts) >= self.BATCH_SIZE or (idx == len(chunk_records) - 1 and batch_texts):
                embeddings = self._generate_embeddings_batch(batch_texts)

                for batch_idx, emb in zip(batch_indices, embeddings):
                    chunk_records[batch_idx]["embedding"] = emb
                    chunk_records[batch_idx]["embedded_at"] = now

                processed += len(batch_texts)
                if processed % 500 == 0 or processed == count:
                    logger.info(f"  Processed {processed:,}/{count:,} chunks ({processed*100//count}%)")

                batch_texts = []
                batch_indices = []

        # Create DataFrame with embeddings
        schema = T.StructType([
            T.StructField("chunk_id", T.StringType(), False),
            T.StructField("resource_uid", T.StringType(), False),
            T.StructField("asset_uid", T.StringType(), False),
            T.StructField("page_no", T.IntegerType(), False),
            T.StructField("chunk_order", T.IntegerType(), False),
            T.StructField("chunk_text", T.StringType(), True),
            T.StructField("token_count", T.IntegerType(), True),
            T.StructField("lang", T.StringType(), True),
            T.StructField("updated_at", T.TimestampType(), False),
            T.StructField("embedding", T.ArrayType(T.FloatType()), True),
            T.StructField("embedded_at", T.TimestampType(), True),
        ])

        return self.spark.createDataFrame(chunk_records, schema=schema)

    def _merge_embeddings(self, embedded_df: DataFrame):
        """Merge embedded chunks back to Iceberg table."""
        if embedded_df.rdd.isEmpty():
            logger.info("No embeddings to merge")
            return

        count = embedded_df.count()
        logger.info(f"Merging {count:,} embeddings into {self.chunks_table}")

        # Create temp view
        temp_view = f"tmp_embedded_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        embedded_df.createOrReplaceTempView(temp_view)

        # Merge only embedding columns
        self.spark.sql(f"""
            MERGE INTO {self.chunks_table} t
            USING {temp_view} s
            ON t.chunk_id = s.chunk_id
            WHEN MATCHED THEN UPDATE SET 
                t.embedding = s.embedding,
                t.embedded_at = s.embedded_at
        """)

        self.spark.catalog.dropTempView(temp_view)
        logger.info(f"Merged {count:,} embeddings successfully")

    def run(self):
        """Run incremental embedding generation on Airflow driver."""
        logger.info("=" * 80)
        logger.info("Silver Embedding Generator")
        logger.info(f"Model: {self.MODEL_NAME} ({self.EMBEDDING_DIM} dims)")
        logger.info(f"Batch size: {self.BATCH_SIZE}")
        logger.info("=" * 80)

        # Check if table exists
        if not self._table_exists(self.chunks_table):
            logger.warning(f"Table {self.chunks_table} does not exist.")
            logger.warning("Run Silver transform pipeline first to create chunks.")
            return {"processed": 0, "status": "skipped", "reason": "table_not_found"}

        # Ensure columns exist
        if not self._ensure_embedding_columns():
            return {"processed": 0, "status": "skipped", "reason": "column_setup_failed"}

        # Get pending chunks
        pending_chunks = self._get_chunks_to_embed()
        if pending_chunks is None:
            return {"processed": 0, "status": "skipped", "reason": "no_chunks"}
        
        pending_count = pending_chunks.count()

        if pending_count == 0:
            logger.info("All chunks already have embeddings. Nothing to do.")
            return {"processed": 0, "status": "up_to_date"}

        # Generate embeddings
        import time
        start_time = time.time()
        
        embedded_df = self._process_embeddings(pending_chunks)

        # Merge back to Iceberg
        self._merge_embeddings(embedded_df)
        
        elapsed = time.time() - start_time
        rate = pending_count / elapsed if elapsed > 0 else 0

        logger.info("=" * 80)
        logger.info(f"Embedding generation complete: {pending_count:,} chunks processed")
        logger.info(f"Time: {elapsed:.1f}s ({rate:.1f} chunks/sec)")
        logger.info("=" * 80)

        return {"processed": pending_count, "status": "success", "time_sec": round(elapsed, 1), "rate": round(rate, 1)}

    def get_stats(self) -> dict:
        """Get embedding statistics."""
        try:
            df = self.spark.table(self.chunks_table)
            total = df.count()
            with_embedding = df.filter(F.col("embedding").isNotNull()).count()
            without_embedding = total - with_embedding

            return {
                "total_chunks": total,
                "with_embedding": with_embedding,
                "without_embedding": without_embedding,
                "coverage_pct": round(with_embedding * 100 / total, 2) if total > 0 else 0,
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {"error": str(e)}


def main():
    generator = SilverEmbeddingGenerator()
    result = generator.run()
    print(f"\nResult: {result}")

    stats = generator.get_stats()
    print(f"Stats: {stats}")


if __name__ == "__main__":
    main()
