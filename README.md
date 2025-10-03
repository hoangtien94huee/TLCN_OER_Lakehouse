# OER Lakehouse

**Hệ thống Data Lakehouse hiện đại để thu thập, xử lý và phân tích Open Educational Resources (OER)**

## 📋 Mục lục

- [Giới thiệu](#-giới-thiệu)
- [Tính năng](#-tính-năng)
- [Kiến trúc hệ thống](#-kiến-trúc-hệ-thống)
- [Yêu cầu](#-yêu-cầu)
- [Cài đặt](#-cài-đặt)
- [Sử dụng](#-sử-dụng)
- [Cấu trúc dự án](#-cấu-trúc-dự-án)
- [Data Pipeline](#-data-pipeline)
- [API và Giao diện](#-api-và-giao-diện)
- [Cấu hình nâng cao](#-cấu-hình-nâng-cao)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)

## 🎯 Giới thiệu

**OER Lakehouse** là một hệ thống Data Lakehouse hoàn chỉnh được xây dựng để thu thập, xử lý và phân tích dữ liệu từ các nguồn Open Educational Resources hàng đầu:

- **MIT OpenCourseWare (MIT OCW)** - Khóa học và tài liệu từ MIT
- **OpenStax** - Sách giáo khoa mở
- **Open Textbook Library (OTL)** - Thư viện sách giáo khoa mở

Hệ thống áp dụng kiến trúc **Medallion Architecture** (Bronze → Silver → Gold) với Apache Iceberg, cung cấp khả năng xử lý dữ liệu quy mô lớn, time-travel, schema evolution và ACID transactions.

## ✨ Tính năng

### 🔍 Thu thập dữ liệu (Bronze Layer)
- ✅ Web scraping tự động với retry mechanism
- ✅ Hỗ trợ đa phương thức: Selenium + BeautifulSoup
- ✅ Thu thập metadata đầy đủ (Dublin Core chuẩn)
- ✅ Download và xử lý PDF, video, transcript
- ✅ Multimedia content extraction (YouTube, native videos)
- ✅ Rate limiting và error handling thông minh

### 🔄 Xử lý dữ liệu (Silver Layer)
- ✅ Schema thống nhất theo Dublin Core Metadata Standard
- ✅ Data cleaning và normalization
- ✅ Deduplication và data quality checks
- ✅ Apache Iceberg tables với partitioning
- ✅ ACID transactions và time-travel queries

### 📊 Phân tích dữ liệu (Gold Layer)
- ✅ Analytics tables cho reporting
- ✅ Feature engineering cho Machine Learning
- ✅ Aggregations và metrics
- ✅ Content statistics và trends
- ✅ Creator và subject analytics

### 🚀 Orchestration & Monitoring
- ✅ Apache Airflow DAGs cho workflow automation
- ✅ Scheduling và dependency management
- ✅ Monitoring và logging
- ✅ Error notification và retry policies
- ✅ Web UI cho quản lý workflows

### 💾 Storage & Processing
- ✅ MinIO S3-compatible object storage
- ✅ Apache Spark distributed processing
- ✅ Iceberg REST catalog
- ✅ Jupyter Notebook cho data exploration
- ✅ PostgreSQL cho metadata

## 🏗️ Kiến trúc hệ thống

### Kiến trúc tổng quan

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES (OER)                         │
├─────────────────┬─────────────────┬──────────────────────────┤
│  MIT OCW        │   OpenStax      │   Open Textbook Library  │
└────────┬────────┴────────┬────────┴──────────┬──────────────┘
         │                 │                    │
         └─────────────────┼────────────────────┘
                          │
                  ┌───────▼────────┐
                  │   AIRFLOW      │  (Orchestration)
                  │   Scheduler    │
                  └───────┬────────┘
                          │
         ┌────────────────┼────────────────┐
         │                │                │
    ┌────▼─────┐    ┌────▼─────┐    ┌────▼─────┐
    │  BRONZE  │    │  SILVER  │    │   GOLD   │
    │  (Raw)   │───▶│(Cleaned) │───▶│(Analytics)│
    └────┬─────┘    └────┬─────┘    └────┬─────┘
         │               │                │
         └───────────────┼────────────────┘
                         │
              ┌──────────▼──────────┐
              │   MinIO (S3)        │
              │   Object Storage    │
              └──────────┬──────────┘
                         │
              ┌──────────▼──────────┐
              │  Iceberg REST       │
              │  Catalog            │
              └──────────┬──────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
    ┌────▼─────┐   ┌────▼─────┐   ┌────▼─────┐
    │  Spark   │   │ Jupyter  │   │PostgreSQL│
    │ Processing│   │Notebooks │   │ Metadata │
    └──────────┘   └──────────┘   └──────────┘
```

### Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw Data)                                    │
│  ─────────────────────────                                  │
│  • Raw JSON files from scrapers                             │
│  • Minimal processing                                       │
│  • Source system preserved                                  │
│  • Location: s3://oer-lakehouse/bronze/                     │
│  • Files: mit_ocw_*.json, openstax_*.json, otl_*.json      │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Cleaned & Standardized)                      │
│  ──────────────────────────────────────                     │
│  • Apache Iceberg tables                                    │
│  • Dublin Core metadata standard                            │
│  • Schema validation & normalization                        │
│  • Deduplication applied                                    │
│  • Location: s3://oer-lakehouse/silver/                     │
│  • Table: oer_resources_dc                                  │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Analytics & Aggregations)                      │
│  ──────────────────────────────────────                     │
│  • Aggregated metrics & KPIs                                │
│  • ML features & embeddings                                 │
│  • Business intelligence tables                             │
│  • Location: s3://oer-lakehouse/gold/                       │
│  • Tables: content_stats, creator_stats, etc.              │
└─────────────────────────────────────────────────────────────┘
```

### Tech Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Orchestration** | Apache Airflow | 2.x | Workflow scheduling & monitoring |
| **Storage** | MinIO | Latest | S3-compatible object storage |
| **Processing** | Apache Spark | 3.5+ | Distributed data processing |
| **Filesystem** | Hadoop | 3.3.4 | Hadoop FileSystem & S3A support |
| **Table Format** | Apache Iceberg | 1.x | Modern table format with ACID |
| **Catalog** | Iceberg REST | Latest | Table metadata management |
| **Database** | PostgreSQL | 17 | Metadata & catalog backend |
| **Notebooks** | Jupyter | Latest | Data exploration & analysis |
| **Language** | Python | 3.10+ | Primary programming language |

## 📦 Yêu cầu

### Phần cứng
- **RAM**: Tối thiểu 8GB (khuyến nghị 16GB)
- **CPU**: 4 cores trở lên
- **Disk**: 20GB trống cho Docker images và data

### Phần mềm
- **Docker**: Version 20.10+ ([Cài đặt Docker](https://docs.docker.com/get-docker/))
- **Docker Compose**: Version 2.0+ ([Cài đặt Docker Compose](https://docs.docker.com/compose/install/))
- **Git**: Để clone repository

### Hệ điều hành
- Linux (Ubuntu 20.04+, CentOS 8+)
- macOS (10.15+)
- Windows 10/11 (với WSL2)

## 🚀 Cài đặt

### 1. Clone repository

```bash
git clone https://github.com/hoangtien94huee/TLCN_OER_Lakehouse.git
cd TLCN_OER_Lakehouse
```

### 2. Khởi động services

```bash
# Khởi động tất cả services
docker-compose up -d

# Kiểm tra trạng thái
docker-compose ps
```

### 3. Khởi tạo schema

```bash
# Tạo database schemas và tables
docker exec oer-airflow-scraper python /opt/airflow/src/create_schema.py
```

### 4. Kiểm tra services

Đợi khoảng 2-3 phút để các services khởi động hoàn tất, sau đó truy cập:

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Jupyter Lab**: http://localhost:8888 (no password)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Spark Master UI**: http://localhost:8081

## 💻 Sử dụng

### Chạy Pipeline thủ công

#### Option 1: Chạy từng bước

```bash
# 1. Thu thập dữ liệu từ MIT OCW
docker exec oer-airflow-scraper python /opt/airflow/src/bronze_mit_ocw.py

# 2. Thu thập dữ liệu từ OpenStax
docker exec oer-airflow-scraper python /opt/airflow/src/bronze_openstax.py

# 3. Thu thập dữ liệu từ OTL
docker exec oer-airflow-scraper python /opt/airflow/src/bronze_otl.py

# 4. Transform sang Silver layer
docker exec oer-airflow-scraper python /opt/airflow/src/silver_transform.py

# 5. Tạo Gold analytics
docker exec oer-airflow-scraper python /opt/airflow/src/gold_analytics.py
```

#### Option 2: Sử dụng Airflow DAGs

1. Truy cập Airflow UI: http://localhost:8080
2. Login với `admin/admin`
3. Bật các DAGs:
   - `mit_ocw_scraper_daily`
   - `openstax_scraper_daily`
   - `otl_scraper_daily`
   - `silver_layer_processing`
   - `gold_layer_processing_dag`
4. Trigger DAGs thủ công hoặc đợi schedule

### Query dữ liệu với Spark SQL

```bash
docker exec oer-airflow-scraper /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=rest \
  --conf spark.sql.catalog.lakehouse.uri=http://iceberg-rest:8181 \
  --conf spark.sql.defaultCatalog=lakehouse \
  -e "SELECT source_system, COUNT(*) FROM lakehouse.default.oer_resources_dc GROUP BY source_system"
```

### Sử dụng Jupyter Notebooks

1. Truy cập Jupyter: http://localhost:8888
2. Navigate đến `/home/jovyan/notebooks`
3. Tạo notebook mới và import PySpark
4. Example code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OER Analysis") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "rest") \
    .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181") \
    .getOrCreate()

# Query dữ liệu
df = spark.sql("SELECT * FROM lakehouse.default.oer_resources_dc LIMIT 10")
df.show()
```

## 📁 Cấu trúc dự án

```
TLCN_OER_Lakehouse/
├── docker-compose.yml          # Docker services configuration
├── clean.bat                   # Cleanup script (Windows)
├── README.md                   # Documentation chính
│
├── lakehouse/                  # Main lakehouse directory
│   ├── README.md              # Lakehouse documentation
│   │
│   ├── airflow/               # Airflow orchestration
│   │   ├── Dockerfile         # Custom Airflow image
│   │   ├── entrypoint.sh      # Container entrypoint
│   │   ├── requirements.txt   # Python dependencies
│   │   │
│   │   ├── dags/              # Airflow DAG definitions
│   │   │   ├── mit_ocw_scraper_dag.py
│   │   │   ├── openstax_scraper_dag.py
│   │   │   ├── otl_scraper_dag.py
│   │   │   ├── silver_layer_processing_dag.py
│   │   │   └── gold_layer_processing_dag.py
│   │   │
│   │   ├── src/               # Processing scripts
│   │   │   ├── README.md      # Scripts documentation
│   │   │   ├── create_schema.py        # Schema initialization
│   │   │   ├── bronze_mit_ocw.py       # MIT OCW scraper
│   │   │   ├── bronze_openstax.py      # OpenStax scraper
│   │   │   ├── bronze_otl.py           # OTL scraper
│   │   │   ├── silver_transform.py     # Data transformation
│   │   │   ├── gold_analytics.py       # Analytics generation
│   │   │   └── minio_utils.py          # MinIO utilities
│   │   │
│   │   ├── notebooks/         # Jupyter notebooks
│   │   └── scripts/           # Helper scripts
│   │
│   ├── spark/                 # Spark configuration
│   │   ├── Dockerfile         # Custom Spark image
│   │   └── transformers/      # Spark transformers
│   │
│   ├── iceberg-rest/          # Iceberg REST catalog
│   │   ├── Dockerfile         # REST catalog image
│   │   ├── core-site.xml      # Hadoop configuration
│   │   └── *.jar              # Required JAR files
│   │
│   └── data/                  # Data storage (mounted volumes)
│       ├── scraped/           # Raw scraped data
│       ├── logs/              # Application logs
│       ├── notebooks/         # Saved notebooks
│       ├── spark/             # Spark temp files
│       └── iceberg-jars/      # Iceberg dependencies
│
└── scripts/                   # Utility scripts
    └── init-postgres.sh       # PostgreSQL initialization
```

## 🔄 Data Pipeline

### Bronze Layer - Data Collection

**Nguồn dữ liệu:**

1. **MIT OCW** (`bronze_mit_ocw.py`)
   - Courses: ~300 courses
   - Metadata: Title, description, instructors, department
   - Content: PDFs, videos, transcripts
   - Features: YouTube API integration, multimedia extraction

2. **OpenStax** (`bronze_openstax.py`)
   - Textbooks: ~70+ textbooks
   - Formats: PDF, Web view, Instructor resources
   - Metadata: ISBN, authors, subjects, license

3. **Open Textbook Library** (`bronze_otl.py`)
   - Textbooks: 1000+ textbooks
   - Metadata: Authors, publishers, subjects
   - Links: External resource URLs

**Output:** Raw JSON files in `s3://oer-lakehouse/bronze/`

### Silver Layer - Data Processing

**Script:** `silver_transform.py`

**Transformations:**
- Schema standardization theo Dublin Core
- Data cleaning và validation
- Normalization (dates, languages, formats)
- Deduplication based on identifier
- Quality checks

**Output:** Iceberg table `lakehouse.default.oer_resources_dc`

**Schema:**
```sql
CREATE TABLE oer_resources_dc (
    identifier STRING,
    title STRING,
    creator ARRAY<STRING>,
    subject ARRAY<STRING>,
    description STRING,
    publisher STRING,
    contributor ARRAY<STRING>,
    date STRING,
    type STRING,
    format STRING,
    source STRING,
    language STRING,
    relation ARRAY<STRING>,
    coverage STRING,
    rights STRING,
    -- Technical fields
    source_system STRING,
    source_url STRING,
    bronze_object STRING,
    silver_ingestion_time TIMESTAMP,
    -- Partitioning
    ingestion_date DATE
) PARTITIONED BY (source_system, ingestion_date)
```

### Gold Layer - Analytics

**Script:** `gold_analytics.py`

**Analytics Tables:**

1. **content_statistics** - Content metrics by source
2. **creator_statistics** - Creator productivity metrics
3. **subject_statistics** - Subject distribution analysis
4. **temporal_statistics** - Time-based trends
5. **ml_features** - Features for machine learning

**Example Queries:**

```sql
-- Top creators by content count
SELECT creator, resource_count, unique_subjects
FROM lakehouse.gold.creator_statistics
ORDER BY resource_count DESC
LIMIT 10;

-- Content growth over time
SELECT year_month, total_resources, new_resources
FROM lakehouse.gold.temporal_statistics
ORDER BY year_month DESC;
```

## 🎨 API và Giao diện

### Airflow Web UI

**URL:** http://localhost:8080  
**Credentials:** admin/admin

**Features:**
- View và manage DAGs
- Trigger manual runs
- Monitor task execution
- View logs và errors
- Configure schedules

### MinIO Console

**URL:** http://localhost:9001  
**Credentials:** minioadmin/minioadmin

**Features:**
- Browse buckets và objects
- Upload/download files
- Manage access policies
- View storage metrics

### Spark Master UI

**URL:** http://localhost:8081

**Features:**
- Monitor Spark applications
- View worker status
- Track job execution
- Resource utilization

### Jupyter Lab

**URL:** http://localhost:8888  
**No password required**

**Features:**
- Interactive notebooks
- PySpark integration
- Data visualization
- Exploratory analysis

## ⚙️ Cấu hình nâng cao

### Environment Variables

Cấu hình trong `docker-compose.yml`:

```yaml
# MinIO Configuration
MINIO_ENDPOINT: minio:9000
MINIO_ACCESS_KEY: minioadmin
MINIO_SECRET_KEY: minioadmin
MINIO_BUCKET: oer-lakehouse

# Bronze Layer Configuration
DOWNLOAD_STRATEGY: selective
MAX_PDF_SIZE_MB: 20
IMPORTANT_CATEGORIES: lecture_notes,textbook,handout,reading
SKIP_CATEGORIES: exam,assignment,quiz

# Spark Configuration
SPARK_MASTER: spark://spark-master:7077
SPARK_WORKER_MEMORY: 2g
SPARK_WORKER_CORES: 2

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__PARALLELISM: 8
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1
```

### Scraping Configuration

Điều chỉnh trong các scripts `bronze_*.py`:

```python
# MIT OCW
max_documents = 300  # Số lượng courses
batch_size = 25      # Batch size cho xử lý
delay = 2.0          # Delay giữa requests (seconds)

# OpenStax
max_documents = 100  # Số lượng textbooks

# OTL
max_documents = 1000 # Số lượng textbooks
```

### Resource Limits

Điều chỉnh trong `docker-compose.yml`:

```yaml
services:
  spark-worker:
    environment:
      - SPARK_WORKER_MEMORY=4g  # Tăng memory
      - SPARK_WORKER_CORES=4    # Tăng cores
```

## 🔧 Troubleshooting

### Service không khởi động

```bash
# Kiểm tra logs
docker-compose logs [service-name]

# Ví dụ
docker-compose logs oer-airflow-scraper
docker-compose logs minio

# Restart specific service
docker-compose restart [service-name]
```

### Lỗi kết nối MinIO

```bash
# Kiểm tra MinIO đã running
docker ps | grep minio

# Test connection
docker exec oer-airflow-scraper python -c "from minio import Minio; \
  client = Minio('minio:9000', 'minioadmin', 'minioadmin', secure=False); \
  print('Connected:', client.bucket_exists('oer-lakehouse'))"
```

### Lỗi Spark

```bash
# Kiểm tra Spark Master
curl http://localhost:8081

# Kiểm tra Spark logs
docker-compose logs spark-master
docker-compose logs spark-worker
```

### Reset toàn bộ hệ thống

```bash
# Windows
clean.bat

# Linux/Mac
docker-compose down -v
docker-compose up -d
docker exec oer-airflow-scraper python /opt/airflow/src/create_schema.py
```

### Xem logs chi tiết

```bash
# Airflow logs
tail -f lakehouse/data/logs/scheduler/latest/*.log

# DAG-specific logs
tail -f lakehouse/data/logs/dag_id=mit_ocw_scraper_daily/run_id=*/task_id=*/*.log
```

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Authors

- **Hoàng Tiến** - [hoangtien94huee](https://github.com/hoangtien94huee)

## Acknowledgments

- [Apache Iceberg](https://iceberg.apache.org/) - Modern table format
- [Apache Spark](https://spark.apache.org/) - Distributed processing
- [Apache Airflow](https://airflow.apache.org/) - Workflow orchestration
- [MinIO](https://min.io/) - S3-compatible storage
- [MIT OpenCourseWare](https://ocw.mit.edu/)
- [OpenStax](https://openstax.org/)
- [Open Textbook Library](https://open.umn.edu/opentextbooks/)

## Support

For issues, questions, or suggestions:
- Open an issue on [GitHub Issues](https://github.com/hoangtien94huee/TLCN_OER_Lakehouse/issues)
- Contact: [Your Email]

---

**Happy Data Engineering! 🚀**