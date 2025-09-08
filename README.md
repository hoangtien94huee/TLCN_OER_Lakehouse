# Tệp README cho OER Scraper với Airflow

## Tổng quan

Dự án này sử dụng Apache Airflow để tự động cào dữ liệu tài liệu OER (Open Educational Resources) từ OpenStax hàng ngày. Hệ thống được thiết kế để:

- ✅ Cào dữ liệu tự động hàng ngày lúc 00:00
- ✅ Tránh trùng lặp tài liệu bằng hash và database
- ✅ Chạy trong Docker container với Selenium headless
- ✅ Lưu trữ dữ liệu có cấu trúc và log chi tiết
- ✅ Tự động dọn dẹp file cũ (> 30 ngày)

## Cấu trúc dự án

```
├── Dockerfile              # Container setup với Chrome + Selenium
├── docker-compose.yml      # Orchestration
├── requirements.txt        # Python dependencies
├── airflow.cfg            # Airflow configuration
├── entrypoint.sh          # Container startup script
├── selenium_scraper.py    # Core scraper logic
├── oer_scraper_dag.py     # Airflow DAG định nghĩa workflow
├── scraped_data/          # Dữ liệu đầu ra (JSON files)
├── database/              # SQLite database cho tracking
└── logs/                  # Airflow logs
```

## Tính năng chính

### 1. Tránh trùng lặp thông minh
- Tạo hash cho mỗi document dựa trên title + description + URL
- Lưu trữ hash trong SQLite database
- Chỉ xử lý document mới hoặc đã thay đổi
- Log chi tiết: mới/cập nhật/bỏ qua

### 2. Selenium headless trong Docker
- Chạy Chrome headless cho môi trường production
- Tự động cài đặt ChromeDriver
- Xử lý JavaScript-rendered content
- Fallback sang requests nếu Selenium lỗi

### 3. Airflow workflow
- DAG chạy hàng ngày lúc 00:00
- Retry logic khi có lỗi
- Health checks
- Cleanup tự động file cũ

## Cài đặt và chạy

### 1. Build và chạy container

```bash
# Clone repository
git clone <your-repo>
cd tlcn

# Build và chạy
docker-compose up -d --build

# Kiểm tra logs
docker-compose logs -f
```

### 2. Truy cập Airflow UI

- URL: http://localhost:8080
- Username: admin
- Password: admin

### 3. Kích hoạt DAG

1. Truy cập Airflow UI
2. Tìm DAG "oer_scraper_daily"
3. Bật DAG bằng toggle switch
4. DAG sẽ chạy tự động hàng ngày

### 4. Chạy thủ công (test)

```bash
# Trigger DAG ngay lập tức
docker exec oer-airflow-scraper airflow dags trigger oer_scraper_daily
```

## Cấu trúc dữ liệu đầu ra

### File JSON hàng ngày
```json
{
  "scraping_date": "2025-01-15",
  "total_found": 50,
  "new_documents": 5,
  "updated_documents": 2,
  "documents": [
    {
      "id": "abc123...",
      "title": "College Algebra",
      "description": "Comprehensive algebra textbook...",
      "authors": ["Jay Abramson"],
      "subjects": ["mathematics"],
      "source": "OpenStax",
      "url": "https://openstax.org/details/books/college-algebra",
      "scraped_at": "2025-01-15T10:30:00"
    }
  ]
}
```

### Database schema
```sql
-- Tracking documents
CREATE TABLE scraped_documents (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    source TEXT NOT NULL,
    scraped_at TIMESTAMP,
    last_updated TIMESTAMP
);

-- Logging runs
CREATE TABLE scraping_logs (
    id INTEGER PRIMARY KEY,
    run_date DATE NOT NULL,
    total_found INTEGER,
    new_documents INTEGER,
    updated_documents INTEGER,
    status TEXT,
    created_at TIMESTAMP
);
```

## Monitoring và troubleshooting

### 1. Kiểm tra logs
```bash
# Airflow logs
docker-compose logs oer-scraper

# Database logs
docker exec oer-airflow-scraper sqlite3 /opt/airflow/database/scraped_documents.db ".tables"
```

### 2. Debug DAG
```bash
# List DAGs
docker exec oer-airflow-scraper airflow dags list

# Test task
docker exec oer-airflow-scraper airflow tasks test oer_scraper_daily scrape_openstax_documents 2025-01-15
```

### 3. Kiểm tra scraping results
```bash
# Xem file output mới nhất
ls -la scraped_data/

# Đếm documents trong database
docker exec oer-airflow-scraper sqlite3 /opt/airflow/database/scraped_documents.db "SELECT COUNT(*) FROM scraped_documents;"
```

## Cấu hình nâng cao

### 1. Thay đổi schedule
Sửa trong `oer_scraper_dag.py`:
```python
schedule_interval=timedelta(hours=12)  # Chạy 2 lần/ngày
# hoặc
schedule_interval='0 2 * * *'  # Chạy lúc 2:00 AM mỗi ngày
```

### 2. Thêm website khác
Mở rộng `selenium_scraper.py`:
```python
def scrape_mit_opencourseware(self):
    # Logic cào MIT OCW
    pass

def scrape_khan_academy(self):
    # Logic cào Khan Academy
    pass
```

### 3. Notification
Thêm vào DAG args:
```python
default_args = {
    'email': ['admin@company.com'],
    'email_on_failure': True,
    'email_on_success': True,
}
```

## Performance tuning

### 1. Giảm delay giữa requests
```python
scraper = AdvancedOERScraper(delay=0.5)  # Từ 2.0 xuống 0.5
```

### 2. Tăng parallelism
Sửa `airflow.cfg`:
```ini
parallelism = 32
dag_concurrency = 16
max_active_tasks_per_dag = 8
```

### 3. Memory optimization
```bash
# Giới hạn memory cho container
docker-compose.yml:
  deploy:
    resources:
      limits:
        memory: 2G
```

## Backup và restore

### 1. Backup database
```bash
docker exec oer-airflow-scraper sqlite3 /opt/airflow/database/scraped_documents.db ".backup /opt/airflow/database/backup.db"
```

### 2. Backup scraped data
```bash
tar -czf scraped_data_backup_$(date +%Y%m%d).tar.gz scraped_data/
```

## Troubleshooting thường gặp

### 1. Chrome/Selenium lỗi
```bash
# Kiểm tra Chrome version
docker exec oer-airflow-scraper google-chrome --version

# Test Selenium
docker exec oer-airflow-scraper python -c "from selenium import webdriver; print('OK')"
```

### 2. Permission errors
```bash
# Fix permissions
sudo chown -R $USER:$USER scraped_data/ database/ logs/
```

### 3. Database locked
```bash
# Stop container và restart
docker-compose down
docker-compose up -d
```

## Mở rộng

Hệ thống có thể mở rộng để:
- Cào nhiều website OER khác
- Tích hợp với Elasticsearch/MongoDB
- Thêm API endpoints
- Machine learning cho phân loại nội dung
- Real-time notifications qua Slack/Email
