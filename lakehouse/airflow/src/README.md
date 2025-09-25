# OER Lakehouse - Standalone Scripts
# ==================================

This directory contains standalone Python scripts for OER Lakehouse data processing, inspired by the [building-lakehouse project](https://github.com/harrydevforlife/building-lakehouse) architecture.

## 📜 **Scripts Overview**

Each script is **self-contained** and can run independently without complex module dependencies.

### 🥉 **Bronze Layer Scripts** (Data Collection)
```bash
python bronze_mit_ocw.py      # Scrape MIT OpenCourseWare
python bronze_openstax.py     # Scrape OpenStax textbooks  
python bronze_otl.py          # Scrape Open Textbook Library
```

### 🥈 **Silver Layer Scripts** (Data Processing)
```bash
python silver_transform.py    # Transform Bronze → Silver (Unified schema)
```

### 🥇 **Gold Layer Scripts** (Analytics)
```bash
python gold_analytics.py      # Generate analytics tables and ML features
```

### 🔧 **Utility Scripts**
```bash
python create_schema.py       # Setup database schemas and tables
python minio_utils.py         # MinIO storage operations and maintenance
```

## 🚀 **Usage Examples**

### Setup Environment
```bash
# Inside Airflow container
export MINIO_ENDPOINT=minio:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export MINIO_BUCKET=oer-lakehouse
export HIVE_METASTORE_URI=thrift://hive-metastore:9083
```

### Initialize Lakehouse
```bash
# 1. Setup schemas and MinIO structure
python create_schema.py
python minio_utils.py setup

# 2. Collect data from sources
python bronze_mit_ocw.py
python bronze_openstax.py
python bronze_otl.py

# 3. Transform to Silver layer
python silver_transform.py

# 4. Generate Gold analytics
python gold_analytics.py
```

### Maintenance Tasks
```bash
# View MinIO statistics
python minio_utils.py stats

# Cleanup old files
python minio_utils.py cleanup 30

# Full maintenance
python minio_utils.py maintenance
```

## 📊 **Data Flow**

```
bronze_*.py → MinIO Bronze Layer (JSON)
       ↓
silver_transform.py → Iceberg Silver Tables (Unified Schema)
       ↓
gold_analytics.py → Iceberg Gold Tables (Analytics + ML)
```

## ⚙️ **Configuration**

All scripts use environment variables for configuration:

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO server endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET` | `oer-lakehouse` | Primary bucket name |
| `HIVE_METASTORE_URI` | `thrift://hive-metastore:9083` | Hive Metastore connection |
| `SCRAPING_DELAY_BASE` | `2.0` | Base delay between requests (seconds) |
| `MAX_DOCUMENTS` | `100` | Maximum documents to scrape per source |
| `BATCH_SIZE` | `25` | Batch size for data processing |

## 🎯 **Script Features**

### ✅ **Self-Contained**
- No complex import dependencies
- All required logic in single file
- Easy to debug and modify

### ✅ **Environment Aware**
- Reads configuration from environment variables
- Works in Docker containers
- Handles missing dependencies gracefully

### ✅ **Error Handling**
- Comprehensive error handling
- Informative logging
- Graceful degradation

### ✅ **Production Ready**
- Configurable batch processing
- Memory efficient
- Monitoring friendly

## 🛠️ **Development**

### Running Individual Scripts
```bash
# From lakehouse/airflow/src directory
cd /path/to/lakehouse/airflow/src

# Run any script directly
python bronze_mit_ocw.py
python silver_transform.py
python gold_analytics.py
```

### Testing Scripts
```bash
# Set test environment
export MAX_DOCUMENTS=5
export BATCH_SIZE=2

# Run with limited data
python bronze_mit_ocw.py
```

### Customization
Each script can be easily customized by:
1. Modifying environment variables
2. Editing script constants at the top
3. Adding custom logic within functions

## 📋 **Integration with Airflow**

These scripts integrate seamlessly with Airflow DAGs:

```python
from airflow.operators.bash import BashOperator

scrape_mit_task = BashOperator(
    task_id='scrape_mit_ocw',
    bash_command='python /opt/airflow/scripts/bronze_mit_ocw.py',
    env={
        'MAX_DOCUMENTS': '50',
        'BATCH_SIZE': '10'
    }
)

transform_task = BashOperator(
    task_id='silver_transform',
    bash_command='python /opt/airflow/scripts/silver_transform.py'
)
```

## 🎉 **Benefits of This Architecture**

1. **🎯 Simplicity**: Each script does one thing well
2. **🔧 Maintainability**: Easy to understand and modify  
3. **📦 Portability**: Can run anywhere Python is available
4. **🚀 Scalability**: Easy to parallelize and distribute
5. **🛡️ Reliability**: Isolated failures don't affect other scripts
6. **📊 Observability**: Clear logging and error reporting

---

**Inspired by [building-lakehouse project](https://github.com/harrydevforlife/building-lakehouse) - Clean, modular, and professional!** 🌟

