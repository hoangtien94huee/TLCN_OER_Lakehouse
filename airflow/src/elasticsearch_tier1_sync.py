#!/usr/bin/env python3
"""
Compact Elasticsearch sync for PageIndex tier-1 retrieval.

This index is metadata-only (no chunk_text, no embedding), optimized for:
- title/description BM25
- subject/program link filtering
- one document per resource_uid
"""

from __future__ import annotations

import logging
import os
from itertools import islice
from pathlib import Path
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch, helpers

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
except ImportError:
    SparkSession = DataFrame = F = None  # type: ignore


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PageIndexTier1ElasticsearchSync:
    def __init__(self) -> None:
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.silver_catalog = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.silver_database = os.getenv("SILVER_DATABASE", "default")
        self.resources_table = f"{self.silver_catalog}.{self.silver_database}.oer_resources_curated"
        self.documents_table = f"{self.silver_catalog}.{self.silver_database}.oer_documents"

        self.es_host = os.getenv("PAGEINDEX_TIER1_ES_HOST", os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")).rstrip("/")
        self.index_name = os.getenv("PAGEINDEX_TIER1_ES_INDEX", "oer_resources_tier1").strip()
        self.batch_size = max(100, int(os.getenv("PAGEINDEX_TIER1_ES_BATCH_SIZE", "1000")))
        self.timeout = max(10, int(os.getenv("PAGEINDEX_TIER1_ES_SYNC_TIMEOUT", os.getenv("ELASTICSEARCH_TIMEOUT", "180"))))
        self.recreate = os.getenv("PAGEINDEX_TIER1_ES_RECREATE", os.getenv("ELASTICSEARCH_RECREATE", "1")).lower() in {
            "1",
            "true",
            "yes",
        }
        self.incremental = os.getenv("PAGEINDEX_TIER1_ES_INCREMENTAL", "1").lower() in {
            "1",
            "true",
            "yes",
        }
        self.stream_partitions = max(1, int(os.getenv("PAGEINDEX_TIER1_STREAM_PARTITIONS", "8")))
        self.spark_master = os.getenv(
            "PAGEINDEX_TIER1_SPARK_MASTER",
            os.getenv("ELASTICSEARCH_SPARK_MASTER", os.getenv("SPARK_MASTER", os.getenv("SPARK_MASTER_URL", "local[*]"))),
        )

        es_user = os.getenv("PAGEINDEX_TIER1_ES_USERNAME", os.getenv("ELASTICSEARCH_USER", "")).strip()
        es_password = os.getenv("PAGEINDEX_TIER1_ES_PASSWORD", os.getenv("ELASTICSEARCH_PASSWORD", "")).strip()
        auth = (es_user, es_password) if es_user and es_password else None
        self.es = Elasticsearch(
            hosts=[self.es_host],
            basic_auth=auth,
            verify_certs=self.es_host.startswith("https"),
            request_timeout=self.timeout,
        )
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        if SparkSession is None:
            raise RuntimeError("PySpark not available in current runtime.")

        java_home = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
        os.environ.setdefault("JAVA_HOME", java_home)
        os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.pop("JAVA_TOOL_OPTIONS", None)

        minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        endpoint = minio_endpoint if minio_endpoint.startswith(("http://", "https://")) else f"http://{minio_endpoint}"
        spark_jars = os.getenv("SPARK_JARS")

        builder = SparkSession.builder.appName("PageIndexTier1ESSync").master(self.spark_master)
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
                            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
                            "org.apache.hadoop:hadoop-aws:3.3.4",
                            "com.amazonaws:aws-java-sdk-bundle:1.12.565",
                        ]
                    ),
                )
                .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2"))
            )

        default_driver_host = "127.0.0.1" if self.spark_master.startswith("local") else "oer-airflow-scraper"
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
            .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", default_driver_host))
            .config("spark.driver.bindAddress", os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"))
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g"))
            .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAXRESULTSIZE", "1g"))
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
            .getOrCreate()
        )

    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.table(table_name)
            return True
        except Exception:
            return False

    def _build_index_dataframe(self) -> Optional[DataFrame]:
        if not self._table_exists(self.resources_table):
            raise RuntimeError(f"Missing required table: {self.resources_table}")

        resources = self.spark.table(self.resources_table).select(
            "resource_uid",
            "source_system",
            "source_url",
            "title",
            "description",
            "language",
            "matched_subjects",
            "program_ids",
            "subject_match_confidence",
            "subject_match_uncertain",
            "last_updated_at",
            "scraped_at",
            "ingested_at",
        )
        resources = resources.filter(F.col("resource_uid").isNotNull() & (F.length(F.trim(F.col("resource_uid"))) > 0))

        empty_int_array = F.from_json(F.lit("[]"), "array<int>")
        empty_str_array = F.from_json(F.lit("[]"), "array<string>")

        resources = (
            resources
            .withColumn(
                "subject_ids",
                F.expr("array_distinct(filter(transform(matched_subjects, x -> x.subject_id), x -> x is not null))"),
            )
            .withColumn(
                "subject_names_vi",
                F.expr("array_distinct(filter(transform(matched_subjects, x -> x.subject_name), x -> x is not null and length(trim(x)) > 0))"),
            )
            .withColumn(
                "subject_names_en",
                F.expr("array_distinct(filter(transform(matched_subjects, x -> x.subject_name_en), x -> x is not null and length(trim(x)) > 0))"),
            )
            .withColumn(
                "subject_codes",
                F.expr("array_distinct(filter(transform(matched_subjects, x -> x.subject_code), x -> x is not null and length(trim(x)) > 0))"),
            )
            .withColumn("program_ids", F.coalesce(F.col("program_ids"), empty_int_array))
            .withColumn("subject_ids", F.coalesce(F.col("subject_ids"), empty_int_array))
            .withColumn("subject_names_vi", F.coalesce(F.col("subject_names_vi"), empty_str_array))
            .withColumn("subject_names_en", F.coalesce(F.col("subject_names_en"), empty_str_array))
            .withColumn("subject_codes", F.coalesce(F.col("subject_codes"), empty_str_array))
            .drop("matched_subjects")
        )

        if self._table_exists(self.documents_table):
            docs = self.spark.table(self.documents_table).select(
                "resource_uid",
                "asset_uid",
                "updated_at",
            )
            docs = docs.filter(F.col("resource_uid").isNotNull() & (F.length(F.trim(F.col("resource_uid"))) > 0))
            docs_agg = (
                docs.groupBy("resource_uid")
                .agg(
                    F.array_sort(F.collect_set("asset_uid")).alias("_asset_uids"),
                    F.max("updated_at").alias("_doc_updated_at"),
                )
                .withColumn("asset_uids", F.expr("filter(_asset_uids, x -> x is not null and length(trim(x)) > 0)"))
                .withColumn("asset_uid", F.when(F.size(F.col("asset_uids")) > 0, F.element_at(F.col("asset_uids"), 1)).otherwise(F.lit(None)))
                .drop("_asset_uids")
            )
            resources = resources.join(F.broadcast(docs_agg), on="resource_uid", how="left")
        else:
            resources = resources.withColumn("asset_uid", F.lit(None)).withColumn("asset_uids", empty_str_array).withColumn("_doc_updated_at", F.lit(None))

        df = (
            resources
            .withColumn(
                "updated_at",
                F.coalesce(F.col("_doc_updated_at"), F.col("last_updated_at"), F.col("scraped_at"), F.col("ingested_at")),
            )
            .drop("_doc_updated_at")
            .select(
                F.col("resource_uid").alias("_id"),
                "resource_uid",
                "asset_uid",
                "asset_uids",
                "source_system",
                "source_url",
                "title",
                "description",
                "language",
                "subject_ids",
                "subject_names_vi",
                "subject_names_en",
                "subject_codes",
                "program_ids",
                "subject_match_confidence",
                "subject_match_uncertain",
                "updated_at",
            )
            .repartition(self.stream_partitions)
        )

        count = df.count()
        if count == 0:
            logger.warning("No records found for compact tier1 index.")
            return None
        logger.info("Prepared %s metadata docs for tier1 compact index", f"{count:,}")
        return df

    def _ensure_index(self) -> None:
        if self.es.indices.exists(index=self.index_name):
            if self.recreate:
                logger.info("Deleting existing compact tier1 index: %s", self.index_name)
                self.es.indices.delete(index=self.index_name)
            else:
                logger.info("Index %s already exists, skip create", self.index_name)
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
                "dynamic": "strict",
                "properties": {
                    "resource_uid": {"type": "keyword"},
                    "asset_uid": {"type": "keyword"},
                    "asset_uids": {"type": "keyword"},
                    "source_system": {"type": "keyword"},
                    "source_url": {"type": "keyword"},
                    "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "description": {"type": "text"},
                    "language": {"type": "keyword"},
                    "subject_ids": {"type": "integer"},
                    "subject_names_vi": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "subject_names_en": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "subject_codes": {"type": "keyword"},
                    "program_ids": {"type": "integer"},
                    "subject_match_confidence": {"type": "half_float"},
                    "subject_match_uncertain": {"type": "boolean"},
                    "updated_at": {"type": "date"},
                },
            },
        }
        logger.info("Creating compact tier1 index: %s", self.index_name)
        self.es.indices.create(index=self.index_name, body=mapping)

    def _bulk_index(self, df: DataFrame) -> None:
        existing_ids = set()
        if self.incremental and not self.recreate:
            try:
                logger.info("Loading existing ids from compact tier1 index for incremental mode...")
                scan_iter = helpers.scan(
                    self.es,
                    index=self.index_name,
                    query={"query": {"match_all": {}}, "_source": False},
                    scroll="5m",
                )
                existing_ids = {hit.get("_id") for hit in scan_iter if hit.get("_id")}
                logger.info("Found %s existing docs", f"{len(existing_ids):,}")
            except Exception as exc:
                logger.warning("Failed loading existing ids, continue full upsert: %s", exc)

        def row_batches() -> Any:
            iterator = df.toLocalIterator()
            while True:
                batch = list(islice(iterator, self.batch_size))
                if not batch:
                    break
                yield batch

        indexed_count = 0
        scanned_count = 0
        skipped_count = 0
        batch_no = 0

        for batch in row_batches():
            batch_no += 1
            rows = [row.asDict(recursive=True) if hasattr(row, "asDict") else row for row in batch]
            scanned_count += len(rows)

            actions: List[Dict[str, Any]] = []
            for row in rows:
                doc_id = str(row.get("resource_uid") or "").strip()
                if not doc_id:
                    continue
                if existing_ids and doc_id in existing_ids:
                    skipped_count += 1
                    continue
                updated_at = row.get("updated_at")
                actions.append(
                    {
                        "_index": self.index_name,
                        "_id": doc_id,
                        "_source": {
                            "resource_uid": doc_id,
                            "asset_uid": row.get("asset_uid"),
                            "asset_uids": row.get("asset_uids") or [],
                            "source_system": row.get("source_system"),
                            "source_url": row.get("source_url"),
                            "title": row.get("title"),
                            "description": row.get("description"),
                            "language": row.get("language"),
                            "subject_ids": row.get("subject_ids") or [],
                            "subject_names_vi": row.get("subject_names_vi") or [],
                            "subject_names_en": row.get("subject_names_en") or [],
                            "subject_codes": row.get("subject_codes") or [],
                            "program_ids": row.get("program_ids") or [],
                            "subject_match_confidence": row.get("subject_match_confidence"),
                            "subject_match_uncertain": row.get("subject_match_uncertain"),
                            "updated_at": updated_at.isoformat() if updated_at else None,
                        },
                    }
                )

            if not actions:
                continue
            success, failed = helpers.bulk(
                self.es,
                actions,
                raise_on_error=False,
                request_timeout=self.timeout,
            )
            indexed_count += success
            if failed:
                logger.warning("Batch %s has %s failed operations", batch_no, failed)

            if batch_no % 10 == 0:
                logger.info(
                    "Progress tier1 compact: scanned=%s indexed=%s skipped=%s",
                    f"{scanned_count:,}",
                    f"{indexed_count:,}",
                    f"{skipped_count:,}",
                )

        logger.info(
            "Tier1 compact sync done: scanned=%s indexed=%s skipped=%s",
            f"{scanned_count:,}",
            f"{indexed_count:,}",
            f"{skipped_count:,}",
        )
        self.es.indices.refresh(index=self.index_name)

    def run(self) -> None:
        logger.info("=" * 80)
        logger.info("PageIndex Tier1 Compact Elasticsearch Sync")
        logger.info("ES host: %s", self.es_host)
        logger.info("Index: %s", self.index_name)
        logger.info("Incremental: %s", self.incremental)
        logger.info("Recreate: %s", self.recreate)
        logger.info("=" * 80)
        try:
            self._ensure_index()
            df = self._build_index_dataframe()
            if df is None:
                return
            self._bulk_index(df)
        finally:
            self.spark.stop()


if __name__ == "__main__":
    PageIndexTier1ElasticsearchSync().run()

