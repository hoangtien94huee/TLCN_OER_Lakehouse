#!/usr/bin/env python3
import argparse
import json
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
    s3_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    spark = (
        SparkSession.builder
        .appName("Export Chunks for Colab")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hadoop")
        .config("spark.sql.catalog.silver.warehouse", f"s3a://{bucket}/silver/")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.565",
            ])
        )
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    return spark


def export_chunks(output_path: str, limit: int = None):
    spark = create_spark_session()
    try:
        table = "silver.default.oer_chunks"
        docs_table = "silver.default.oer_documents"

        logger.info(f"Loading chunks from {table}")
        df = spark.table(table)

        if "source_system" not in df.columns:
            docs = spark.table(docs_table).select(
                F.col("asset_uid").alias("_doc_asset_uid"),
                F.col("source_system").alias("_doc_source_system"),
            )
            df = (
                df.join(F.broadcast(docs), df.asset_uid == docs._doc_asset_uid, "left")
                  .drop("_doc_asset_uid")
                  .withColumn("source_system", F.col("_doc_source_system"))
                  .drop("_doc_source_system")
            )

        wanted_cols = [
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
        ]

        available_cols = [c for c in wanted_cols if c in df.columns]
        df = df.select(*available_cols)

        if limit:
            df = df.limit(limit)

        total = df.count()
        logger.info(f"Exporting {total:,} chunks to {output_path}")

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            for row in df.toLocalIterator():
                record = row.asDict(recursive=True)
                for k, v in record.items():
                    if hasattr(v, "isoformat"):
                        record[k] = v.isoformat()
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        logger.info(f"Done: {total:,} chunks")
        return total
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", "-o", default="data/chunks_for_embedding.jsonl")
    parser.add_argument("--limit", "-l", type=int)
    args = parser.parse_args()
    export_chunks(args.output, args.limit)


if __name__ == "__main__":
    main()