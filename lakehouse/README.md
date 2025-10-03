# OER Lakehouse

This repository provisions a local OER Lakehouse using MinIO (S3-compatible), Apache Iceberg (REST catalog), Spark, and Airflow.

## Directory Structure

```
lakehouse/
├── airflow/               # Airflow image, DAGs and ETL code
│   ├── dags/
│   ├── src/
│   ├── Dockerfile
│   ├── entrypoint.sh
│   └── requirements.txt
├── iceberg-rest/          # Iceberg REST catalog image
└── spark/                 # Spark image (if applicable)
```

## Components

- MinIO: S3-compatible object storage
- Iceberg REST: Catalog and table metadata service
- Spark: ETL and SQL over Iceberg tables
- Airflow: Orchestration and standalone runners

## Quick Start

Prerequisites: Docker, Docker Compose.

1) Start services
```bash
docker compose up -d
```

2) Buckets and endpoints
- Bucket created automatically: `oer-lakehouse`
- REST catalog points to MinIO endpoint with path-style access
- Spark reads Bronze via `s3a://`, Iceberg table locations use `s3://`

3) Run Silver transform (standalone)
```bash
docker exec oer-airflow-scraper python /opt/airflow/src/silver_transform.py
```

4) Query data
```bash
docker exec oer-airflow-scraper /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.connector.catalog.InMemoryCatalog \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=rest \
  --conf spark.sql.catalog.lakehouse.uri=http://iceberg-rest:8181 \
  --conf spark.sql.defaultCatalog=lakehouse \
  --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.hadoop.HadoopFileIO \
  --conf spark.hadoop.fs.s3a.endpoint=http://MINIO_IP:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.s3.impl=org.apache.hadoop.fs.s3a.S3A \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  -e "SELECT COUNT(*) FROM lakehouse.default.oer_resources_dc"
```

Replace MINIO_IP with the container IP of `oer-minio` (example: 172.18.0.2).

## Conventions

- Bronze files: `s3a://oer-lakehouse/bronze/...`
- Iceberg databases and tables: `s3://oer-lakehouse/silver/...`
- Spark session disables Hive to avoid Hive warnings

## Troubleshooting

- UnknownHost or timeout when creating tables
  - Use path-style access, disable virtual host style
  - In `docker-compose.yml` for `iceberg-rest`:
    - `CATALOG_S3_PATH_STYLE_ACCESS: "true"`
    - `CATALOG_S3_VIRTUAL_HOST_ENABLED: "false"`
    - `CATALOG_S3_V2_VIRTUAL_HOST_ENABLED: "false"`
    - Set `CATALOG_S3_ENDPOINT` to the MinIO container IP

- No FileSystem for scheme "s3"
  - Spark lacks `s3` handler; route `s3` to S3A and/or use HadoopFileIO on Spark side
  - Add to Spark configs:
    - `spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`
    - `spark.hadoop.fs.AbstractFileSystem.s3.impl=org.apache.hadoop.fs.s3a.S3A`
    - `spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.hadoop.HadoopFileIO`

- Hive warnings in logs
  - Disable Hive in Spark session:
    - `spark.sql.catalogImplementation=in-memory`
    - `spark.sql.catalog.spark_catalog=org.apache.spark.sql.connector.catalog.InMemoryCatalog`

## Notes

- Credentials default to `minioadmin:minioadmin` for local setup
- Tables are partitioned by `source_system`
- The Silver transform normalizes list fields and enforces per-table schemas

