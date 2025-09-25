# OER Lakehouse

**Hệ thống Data Lakehouse đơn giản để thu thập và xử lý Open Educational Resources (OER)**

## Mục tiêu

Thu thập dữ liệu OER từ MIT OCW, OpenStax, Open Textbook Library và xử lý qua Bronze → Silver → Gold layers với Apache Iceberg.

## Kiến trúc

```
OER Sources → Bronze (JSON) → Silver (Iceberg) → Gold (Analytics)
  Scripts   →   MinIO S3   →    Spark    →    Jupyter
```

## Services

| Service | Port | Purpose |
|---------|------|---------|
| Airflow | 8080 | Workflow orchestration |
| Jupyter | 8888 | Data exploration |
| MinIO Console | 9001 | Storage management |
| Spark Master | 8081 | Processing engine |

## Quick Start

```bash
# Start services
docker-compose up -d

# Run pipeline
docker exec oer-airflow python /opt/airflow/scripts/create_schema.py
docker exec oer-airflow python /opt/airflow/scripts/bronze_mit_ocw.py
docker exec oer-airflow python /opt/airflow/scripts/silver_transform.py
docker exec oer-airflow python /opt/airflow/scripts/gold_analytics.py
```

## Access Services

- Airflow: http://localhost:8080 (admin/admin)
- Jupyter: http://localhost:8888
- MinIO: http://localhost:9001 (minioadmin/minioadmin)
- Spark: http://localhost:8081

## Data Layers (Lakehouse Architecture)

Data được lưu trực tiếp trong MinIO S3, không cần traditional warehouse:

- **Bronze**: Raw JSON từ scrapers → `s3://oer-lakehouse/bronze/`
- **Silver**: Iceberg tables với unified schema → `s3://oer-lakehouse/silver/`
- **Gold**: Analytics tables với ML features → `s3://oer-lakehouse/gold/`

## Cấu trúc project

```
 lakehouse/
 ├── airflow/           # Data orchestration
 │   ├── dags/          # Airflow workflows
 │   └── src/           # Processing scripts
 ├── spark/             # Spark processing
 └── data/              # Data storage
     ├── scraped/       # Raw scraped data
     ├── notebooks/     # Jupyter notebooks
     ├── logs/          # Application logs
     ├── spark/         # Spark temporary files
     └── iceberg-jars/  # Iceberg JAR files
```

## Development

Scripts trong `lakehouse/airflow/src/`:
- `bronze_*.py` - Data scrapers
- `silver_transform.py` - Data cleaning
- `gold_analytics.py` - Analytics generation
- `create_schema.py` - Schema setup

Cleanup: `docker-compose down -v`