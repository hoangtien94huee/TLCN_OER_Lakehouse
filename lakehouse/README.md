# OER Lakehouse Architecture
# =========================

Inspired by [building-lakehouse](https://github.com/harrydevforlife/building-lakehouse) project, 
our OER Lakehouse uses a clean, modular architecture for processing Open Educational Resources.

## 📁 Directory Structure

```
lakehouse/
├── airflow/               # Data Orchestration
│   ├── dags/             # Airflow DAGs
│   ├── src/              # 🌟 Standalone Python scripts
│   ├── Dockerfile        # Airflow container
│   ├── entrypoint.sh     # Startup script
│   └── requirements.txt  # Python dependencies
└── spark/                # Data Processing
    ├── transformers/     # Spark transformation jobs
    └── Dockerfile        # Custom Spark container
```

## 🏗️ Lakehouse Layers

### Bronze Layer (Raw Data)
- **Storage**: MinIO S3-compatible object storage
- **Format**: JSON files from OER sources
- **Sources**: MIT OCW, OpenStax, Open Textbook Library
- **Path**: `s3://oer-lakehouse/bronze/`

### Silver Layer (Cleaned Data)
- **Storage**: Apache Iceberg tables
- **Format**: Parquet with schema evolution
- **Schema**: Unified OER schema (Dublin Core + LRMI)
- **Path**: `s3://oer-lakehouse/silver/`

### Gold Layer (Analytics Ready)
- **Storage**: Apache Iceberg tables
- **Format**: Aggregated and ML-ready datasets
- **Purpose**: Business intelligence and recommendations
- **Path**: `s3://oer-lakehouse/gold/`

## 🔄 Data Flow

```
OER Sources → Bronze → Silver → Gold → Analytics
     ↓           ↓        ↓       ↓
  Scrapers → MinIO → Spark → Jupyter
```

1. **Ingestion**: Standalone scripts scrape OER sources
2. **Processing**: Spark transforms and validates data
3. **Storage**: Iceberg manages table versions and metadata
4. **Analysis**: Jupyter notebooks for exploration and insights

## 🚀 Quick Start

```bash
# Start all services
docker-compose up -d

# Setup schemas
docker exec oer-airflow python /opt/airflow/scripts/create_schema.py

# Run data pipeline
docker exec oer-airflow python /opt/airflow/scripts/bronze_mit_ocw.py

# Access services
open http://localhost:8080  # Airflow
open http://localhost:8888  # Jupyter
open http://localhost:9001  # MinIO Console
```

## 📊 Key Features

- **Schema Evolution**: Iceberg supports schema changes
- **ACID Transactions**: Reliable data updates
- **Time Travel**: Query historical data versions
- **Scalable Processing**: Spark handles large datasets
- **Standalone Scripts**: Self-contained Python processing
- **Jupyter Integration**: Interactive data exploration

## 🎯 Use Cases

- **Data Collection**: Automated OER harvesting from multiple sources
- **Data Quality**: Validation and standardization
- **Analytics**: Subject analysis and content metrics
- **Research**: ML feature generation for recommendation systems

