"""
OER Lakehouse - Elasticsearch Tier1 Compact Sync DAG
====================================================

This DAG builds a compact metadata index for PageIndex tier-1 retrieval.
No chunk-level content, no embeddings.

Workflow:
1. Validate Elasticsearch connection
2. Validate Silver serving tables (resources/documents)
3. Sync compact metadata docs to Elasticsearch index
4. Verify index schema and document count
"""

from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "oer-lakehouse",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=8),
}

dag = DAG(
    "elasticsearch_sync",
    default_args=default_args,
    description="Sync compact tier1 metadata index for PageIndex retrieval",
    schedule_interval=timedelta(days=7),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["search", "elasticsearch", "silver-layer", "lakehouse", "pageindex", "tier1"],
)


def check_elasticsearch_connection(**context):
    from elasticsearch import Elasticsearch

    es_host = os.getenv("PAGEINDEX_TIER1_ES_HOST", os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200"))
    print(f"Checking Elasticsearch connection at {es_host}")
    es = Elasticsearch(hosts=[es_host])
    info = es.info()
    health = es.cluster.health()
    status = health.get("status", "unknown")
    print("Elasticsearch connected successfully")
    print(f"  Cluster: {info.get('cluster_name', 'unknown')}")
    print(f"  Version: {info.get('version', {}).get('number', 'unknown')}")
    print(f"  Health: {status}")
    if status == "red":
        raise Exception("Elasticsearch cluster health is RED - cannot proceed")
    return {
        "status": "connected",
        "cluster": info.get("cluster_name"),
        "version": info.get("version", {}).get("number"),
        "health": status,
    }


def check_silver_layer_tables(**context):
    from pyspark.sql import SparkSession

    bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
    silver_catalog = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
    silver_database = os.getenv("SILVER_DATABASE", "default")
    resources_table = f"{silver_catalog}.{silver_database}.oer_resources_curated"
    documents_table = f"{silver_catalog}.{silver_database}.oer_documents"

    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    os.environ.setdefault("SPARK_MASTER", os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    os.environ.setdefault("SPARK_DRIVER_HOST", "oer-airflow-scraper")
    os.environ.setdefault("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0")
    os.environ.pop("JAVA_TOOL_OPTIONS", None)

    jars_dir = Path("/opt/airflow/jars")
    jar_candidates = [
        jars_dir / "iceberg-spark-runtime-3.5_2.12-1.9.2.jar",
        jars_dir / "hadoop-aws-3.3.4.jar",
        jars_dir / "aws-java-sdk-bundle-1.12.262.jar",
    ]
    existing_local_jars = [str(p) for p in jar_candidates if p.exists()]
    spark_packages = (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    builder = (
        SparkSession.builder
        .appName("ES-Tier1-TableCheck")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{silver_catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{silver_catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{silver_catalog}.warehouse", f"s3a://{bucket}/silver/")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "oer-airflow-scraper"))
        .config("spark.driver.bindAddress", os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"))
        .config("spark.driver.memory", "1g")
    )

    if existing_local_jars:
        builder = builder.config("spark.jars", ",".join(existing_local_jars))
    else:
        builder = builder.config("spark.jars.packages", os.getenv("SPARK_PACKAGES", spark_packages))

    spark = builder.getOrCreate()
    try:
        stats = {}
        for table in [resources_table, documents_table]:
            df = spark.table(table)
            count = df.count()
            print(f"{table}: {count:,} records")
            stats[table] = count
            if count == 0:
                raise Exception(f"Required table {table} is empty")
        return stats
    finally:
        spark.stop()


def run_elasticsearch_sync(**context):
    from src.elasticsearch_tier1_sync import PageIndexTier1ElasticsearchSync

    print("=" * 80)
    print("Starting PageIndex Tier1 Compact Elasticsearch Sync")
    print("=" * 80)
    print(f"ES_HOST: {os.getenv('PAGEINDEX_TIER1_ES_HOST', os.getenv('ELASTICSEARCH_HOST', 'http://elasticsearch:9200'))}")
    print(f"ES_INDEX: {os.getenv('PAGEINDEX_TIER1_ES_INDEX', 'oer_resources_tier1')}")
    print(f"RECREATE: {os.getenv('PAGEINDEX_TIER1_ES_RECREATE', os.getenv('ELASTICSEARCH_RECREATE', '1'))}")
    print(f"INCREMENTAL: {os.getenv('PAGEINDEX_TIER1_ES_INCREMENTAL', '1')}")
    print()

    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    os.environ.setdefault("PAGEINDEX_TIER1_SPARK_MASTER", os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    os.environ.setdefault("SPARK_DRIVER_HOST", "oer-airflow-scraper")
    os.environ.setdefault("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0")
    os.environ.pop("JAVA_TOOL_OPTIONS", None)

    sync = PageIndexTier1ElasticsearchSync()
    sync.run()
    print("=" * 80)
    print("PageIndex Tier1 Compact Elasticsearch Sync Complete")
    print("=" * 80)


def verify_elasticsearch_index(**context):
    from elasticsearch import Elasticsearch

    es_host = os.getenv("PAGEINDEX_TIER1_ES_HOST", os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200"))
    index_name = os.getenv("PAGEINDEX_TIER1_ES_INDEX", "oer_resources_tier1")
    es = Elasticsearch(hosts=[es_host])

    if not es.indices.exists(index=index_name):
        raise Exception(f"Index {index_name} does not exist after sync")

    stats = es.indices.stats(index=index_name)
    doc_count = stats["indices"][index_name]["primaries"]["docs"]["count"]
    size_bytes = stats["indices"][index_name]["primaries"]["store"]["size_in_bytes"]
    size_mb = size_bytes / (1024 * 1024)

    mapping = es.indices.get_mapping(index=index_name)
    properties = mapping[index_name]["mappings"].get("properties", {})
    required_fields = {
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
    }
    missing = sorted([field for field in required_fields if field not in properties])
    if missing:
        raise Exception(f"Index mapping missing required fields: {missing}")

    forbidden_fields = ["chunk_text", "embedding", "page_no", "chapter_title", "section_title"]
    existed_forbidden = [field for field in forbidden_fields if field in properties]
    if existed_forbidden:
        raise Exception(f"Compact tier1 index still contains forbidden fields: {existed_forbidden}")

    test_query = es.search(
        index=index_name,
        body={
            "size": 3,
            "query": {
                "multi_match": {
                    "query": "mathematics",
                    "fields": [
                        "title^8",
                        "description^6",
                        "subject_names_vi^5",
                        "subject_names_en^5",
                        "subject_codes^5",
                    ],
                    "lenient": True,
                }
            },
        },
    )
    test_hits = test_query["hits"]["total"]["value"]

    print(f"Verified compact index: {index_name}")
    print(f"  Documents: {doc_count:,}")
    print(f"  Size: {size_mb:.2f} MB")
    print(f"  Fields: {len(properties)}")
    print(f"  Test search hits: {test_hits}")
    if doc_count == 0:
        raise Exception("Index has 0 documents")
    return {
        "index": index_name,
        "doc_count": doc_count,
        "size_mb": round(size_mb, 2),
        "field_count": len(properties),
        "test_hits": test_hits,
    }


def generate_sync_report(**context):
    ti = context["ti"]
    es_check = ti.xcom_pull(task_ids="check_elasticsearch")
    table_stats = ti.xcom_pull(task_ids="check_silver_tables")
    index_stats = ti.xcom_pull(task_ids="verify_index")
    report = {
        "sync_date": datetime.now().isoformat(),
        "status": "SUCCESS",
        "elasticsearch": es_check,
        "silver_tables": table_stats,
        "index_stats": index_stats,
    }
    print("=" * 80)
    print("TIER1 COMPACT ELASTICSEARCH SYNC REPORT")
    print("=" * 80)
    print(report)
    print("=" * 80)
    return report


start = DummyOperator(task_id="start", dag=dag)
check_elasticsearch = PythonOperator(task_id="check_elasticsearch", python_callable=check_elasticsearch_connection, dag=dag)
check_silver_tables = PythonOperator(task_id="check_silver_tables", python_callable=check_silver_layer_tables, dag=dag)
sync_to_elasticsearch = PythonOperator(task_id="sync_to_elasticsearch", python_callable=run_elasticsearch_sync, dag=dag)
verify_index = PythonOperator(task_id="verify_index", python_callable=verify_elasticsearch_index, dag=dag)
generate_report = PythonOperator(task_id="generate_report", python_callable=generate_sync_report, dag=dag)
end = DummyOperator(task_id="end", dag=dag)


start >> [check_elasticsearch, check_silver_tables]
[check_elasticsearch, check_silver_tables] >> sync_to_elasticsearch
sync_to_elasticsearch >> verify_index >> generate_report >> end

