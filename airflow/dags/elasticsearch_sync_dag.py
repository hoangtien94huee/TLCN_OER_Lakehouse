"""
OER Lakehouse - Elasticsearch Sync DAG
======================================

This DAG syncs Gold layer data to Elasticsearch for full-text search.
Extracts PDF content from Bronze layer and indexes into Elasticsearch.

Workflow:
1. Wait for Gold layer processing to complete (optional)
2. Validate Elasticsearch connection
3. Sync Gold layer data to Elasticsearch index
4. Verify index health and document count

Dependencies: 
- Gold layer tables must exist (dim_oer_resources, fact_oer_resources, etc.)
- Elasticsearch must be running and accessible
- MinIO must be accessible for PDF content extraction
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
import os


# DAG Configuration
default_args = {
    'owner': 'oer-lakehouse',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=48),  # Embedding generation is CPU-intensive
}

dag = DAG(
    'elasticsearch_sync',
    default_args=default_args,
    description='Sync Gold layer OER resources to Elasticsearch for full-text search',
    schedule_interval=timedelta(days=7),  # Run weekly
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['search', 'elasticsearch', 'gold-layer', 'lakehouse'],
)


# === Utility Functions ===

def check_elasticsearch_connection(**context):
    """Validate Elasticsearch is running and accessible"""
    from elasticsearch import Elasticsearch
    
    es_host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
    
    print(f"Checking Elasticsearch connection at {es_host}")
    
    try:
        es = Elasticsearch(hosts=[es_host])
        info = es.info()
        
        print(f" Elasticsearch connected successfully")
        print(f"  Cluster: {info.get('cluster_name', 'unknown')}")
        print(f"  Version: {info.get('version', {}).get('number', 'unknown')}")
        
        # Check cluster health
        health = es.cluster.health()
        status = health.get('status', 'unknown')
        print(f"  Health: {status}")
        
        if status == 'red':
            raise Exception("Elasticsearch cluster health is RED - cannot proceed")
        
        return {
            'status': 'connected',
            'cluster': info.get('cluster_name'),
            'version': info.get('version', {}).get('number'),
            'health': status
        }
        
    except Exception as e:
        print(f" Elasticsearch connection failed: {e}")
        raise


def check_gold_layer_tables(**context):
    """Verify Gold layer tables exist and have data"""
    from pyspark.sql import SparkSession
    
    bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
    gold_catalog = os.getenv("ICEBERG_GOLD_CATALOG", "gold")
    gold_database = os.getenv("GOLD_DATABASE", "analytics")
    
    # Harden Spark env for Airflow task runtime
    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    os.environ.setdefault("SPARK_MASTER", os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    os.environ.setdefault("SPARK_DRIVER_HOST", "oer-airflow-scraper")
    os.environ.setdefault("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0")
    os.environ.pop("JAVA_TOOL_OPTIONS", None)

    # Prefer local jars; fallback to packages when local jars are unavailable
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
        .appName("ES-Sync-TableCheck")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{gold_catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{gold_catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{gold_catalog}.warehouse", f"s3a://{bucket}/gold/")
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hadoop")
        .config("spark.sql.catalog.silver.warehouse", f"s3a://{bucket}/silver/")
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
        # Check required tables
        required_tables = [
            f"{gold_catalog}.{gold_database}.dim_oer_resources",
            f"{gold_catalog}.{gold_database}.fact_oer_resources",
        ]
        silver_required_tables = [
            "silver.default.oer_documents",
            "silver.default.oer_chunks",
        ]
        
        optional_tables = [
            f"{gold_catalog}.{gold_database}.dim_sources",
            f"{gold_catalog}.{gold_database}.dim_languages",
            f"{gold_catalog}.{gold_database}.dim_date",
        ]
        
        table_stats = {}
        
        for table in required_tables:
            try:
                df = spark.table(table)
                count = df.count()
                print(f" {table}: {count:,} records")
                table_stats[table] = count
                
                if count == 0:
                    raise Exception(f"Required table {table} is empty!")
                    
            except Exception as e:
                print(f" {table}: MISSING or ERROR - {e}")
                raise Exception(f"Required table {table} not found: {e}")

        for table in silver_required_tables:
            try:
                df = spark.table(table)
                count = df.count()
                print(f" {table}: {count:,} records")
                table_stats[table] = count
            except Exception as e:
                print(f" {table}: MISSING or ERROR - {e}")
                raise Exception(f"Required table {table} not found: {e}")
        
        for table in optional_tables:
            try:
                df = spark.table(table)
                count = df.count()
                print(f" {table}: {count:,} records")
                table_stats[table] = count
            except Exception as e:
                print(f" {table}: Not available (optional) - {e}")
                table_stats[table] = 0
        
        total_resources = table_stats.get(required_tables[0], 0)
        print(f"\n Total OER resources to index: {total_resources:,}")
        
        return table_stats
        
    finally:
        spark.stop()


def run_elasticsearch_sync(**context):
    """Execute the main Elasticsearch sync job"""
    from src.elasticsearch_sync import ChatbotElasticsearchSync
    
    print("=" * 80)
    print("Starting Chatbot Elasticsearch Sync")
    print("=" * 80)
    
    # Log configuration
    print(f"Configuration:")
    print(f"  ES_HOST: {os.getenv('ELASTICSEARCH_HOST', 'http://elasticsearch:9200')}")
    print(f"  ES_INDEX: {os.getenv('ELASTICSEARCH_INDEX', 'oer_resources')}")
    print(f"  BATCH_SIZE: {os.getenv('ELASTICSEARCH_BATCH_SIZE', '300')}")
    print(f"  RECREATE_INDEX: {os.getenv('ELASTICSEARCH_RECREATE', '0')}")
    print(f"  INCREMENTAL: {os.getenv('ELASTICSEARCH_INCREMENTAL', '1')}")
    print()
    
    # Run sync
    sync = ChatbotElasticsearchSync()
    sync.run()
    
    print("=" * 80)
    print("Chatbot Elasticsearch Sync Complete")
    print("=" * 80)


def verify_elasticsearch_index(**context):
    """Verify the Elasticsearch index after sync"""
    from elasticsearch import Elasticsearch
    
    es_host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
    index_name = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
    
    es = Elasticsearch(hosts=[es_host])
    
    print(f"Verifying index: {index_name}")
    
    # Check index exists
    if not es.indices.exists(index=index_name):
        raise Exception(f"Index {index_name} does not exist after sync!")
    
    # Get index stats
    stats = es.indices.stats(index=index_name)
    doc_count = stats['indices'][index_name]['primaries']['docs']['count']
    size_bytes = stats['indices'][index_name]['primaries']['store']['size_in_bytes']
    size_mb = size_bytes / (1024 * 1024)
    
    print(f" Index Stats:")
    print(f"  Documents: {doc_count:,}")
    print(f"  Size: {size_mb:.2f} MB")
    
    # Check mapping
    mapping = es.indices.get_mapping(index=index_name)
    properties = mapping[index_name]['mappings'].get('properties', {})
    print(f"  Fields: {len(properties)}")
    
    # Verify embedding field (512d dense_vector)
    emb_field = properties.get('embedding', {})
    if emb_field.get('type') == 'dense_vector':
        dims = emb_field.get('dims', 0)
        print(f"  Embedding field: dense_vector dims={dims} ✓")
    else:
        print(f"  WARNING: 'embedding' dense_vector field missing from mapping!")
    
    # Sample search to verify
    result = es.search(
        index=index_name,
        body={
            "size": 1,
            "query": {"match_all": {}}
        }
    )
    
    total_hits = result['hits']['total']['value']
    print(f"  Searchable docs: {total_hits:,}")
    
    if total_hits == 0:
        raise Exception("Index has 0 searchable documents!")
    
    # Test search functionality
    test_query = es.search(
        index=index_name,
        body={
            "size": 3,
            "query": {
                "multi_match": {
                    "query": "mathematics",
                    "fields": ["title^3", "description", "search_text"]
                }
            }
        }
    )
    
    test_hits = test_query['hits']['total']['value']
    print(f"  Test search 'mathematics': {test_hits} results")
    
    # Check PDF content (if enabled)
    pdf_query = es.search(
        index=index_name,
        body={
            "size": 0,
            "query": {
                "exists": {"field": "pdf_text"}
            }
        }
    )
    
    pdf_docs = pdf_query['hits']['total']['value']
    print(f"  Documents with PDF content: {pdf_docs:,}")
    
    # Count docs with embedding vectors
    embed_query = es.search(
        index=index_name,
        body={
            "size": 0,
            "query": {
                "exists": {"field": "embedding"}
            }
        }
    )
    embed_docs = embed_query['hits']['total']['value']
    print(f"  Documents with embedding vectors: {embed_docs:,}")
    
    # Summary
    print()
    print("=" * 50)
    print(f" Elasticsearch Index Verification PASSED")
    print(f"  Total: {doc_count:,} documents indexed")
    print(f"  PDF Content: {pdf_docs:,} documents ({pdf_docs*100/doc_count:.1f}%)")
    print(f"  Embedding vectors: {embed_docs:,} documents ({embed_docs*100/doc_count:.1f}%)")
    print("=" * 50)
    
    return {
        'index': index_name,
        'doc_count': doc_count,
        'size_mb': round(size_mb, 2),
        'pdf_docs': pdf_docs,
        'embed_docs': embed_docs
    }


def generate_sync_report(**context):
    """Generate a summary report of the sync operation"""
    ti = context['ti']
    
    # Pull results from previous tasks
    es_check = ti.xcom_pull(task_ids='check_elasticsearch')
    table_stats = ti.xcom_pull(task_ids='check_gold_tables')
    index_stats = ti.xcom_pull(task_ids='verify_index')
    
    report = {
        'sync_date': datetime.now().isoformat(),
        'elasticsearch': es_check,
        'gold_tables': table_stats,
        'index_stats': index_stats,
        'status': 'SUCCESS'
    }
    
    print("\n" + "=" * 80)
    print("ELASTICSEARCH SYNC REPORT")
    print("=" * 80)
    print(f"Sync Date: {report['sync_date']}")
    print(f"Status: {report['status']}")
    print()
    print("Elasticsearch:")
    if es_check:
        print(f"  Cluster: {es_check.get('cluster', 'N/A')}")
        print(f"  Version: {es_check.get('version', 'N/A')}")
        print(f"  Health: {es_check.get('health', 'N/A')}")
    print()
    print("Index Statistics:")
    if index_stats:
        print(f"  Index: {index_stats.get('index', 'N/A')}")
        print(f"  Documents: {index_stats.get('doc_count', 0):,}")
        print(f"  Size: {index_stats.get('size_mb', 0)} MB")
        print(f"  PDF Documents: {index_stats.get('pdf_docs', 0):,}")
        print(f"  Embedding Vectors: {index_stats.get('embed_docs', 0):,}")
    print("=" * 80)
    
    return report


# === DAG Tasks ===

# Start
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Optional: Wait for Gold layer processing (commented out for manual triggers)
# wait_for_gold = ExternalTaskSensor(
#     task_id='wait_for_gold_layer',
#     external_dag_id='gold_layer_processing',
#     external_task_id='end',
#     timeout=3600,
#     poke_interval=60,
#     mode='reschedule',
#     dag=dag,
# )

# Check Elasticsearch connection
check_elasticsearch = PythonOperator(
    task_id='check_elasticsearch',
    python_callable=check_elasticsearch_connection,
    dag=dag,
)

# Check Gold layer tables
check_gold_tables = PythonOperator(
    task_id='check_gold_tables',
    python_callable=check_gold_layer_tables,
    dag=dag,
)

# Run the main sync
sync_to_elasticsearch = PythonOperator(
    task_id='sync_to_elasticsearch',
    python_callable=run_elasticsearch_sync,
    dag=dag,
)

# Verify the index
verify_index = PythonOperator(
    task_id='verify_index',
    python_callable=verify_elasticsearch_index,
    dag=dag,
)

# Generate report
generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_sync_report,
    dag=dag,
)

# End
end = DummyOperator(
    task_id='end',
    dag=dag,
)


# === Task Dependencies ===

start >> [check_elasticsearch, check_gold_tables]
[check_elasticsearch, check_gold_tables] >> sync_to_elasticsearch
sync_to_elasticsearch >> verify_index >> generate_report >> end
