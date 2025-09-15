# OER Lakehouse Scraper (Airflow + Selenium + MinIO)

### Tổng quan dự án
- Thu thập tài nguyên OER (Open Educational Resources) từ nhiều nguồn: `OpenStax`, `Open Textbook Library (OTL)`, và `MIT OpenCourseWare (MIT OCW)`.
- Điều phối bằng Apache Airflow chạy trong Docker, sử dụng Selenium + Google Chrome headless khi cần.
- Lưu trữ dữ liệu thô theo mô hình data lake trên MinIO (S3-compatible), kèm backup/emergency-recovery.
- Lưu log, dữ liệu tạm và cấu hình DAG rõ ràng, dễ mở rộng để thêm nguồn OER mới.

### Kiến trúc & thành phần
- `docker-compose.yml`: định nghĩa các services
  - `postgres` (PostgreSQL 14) – metadata cho Airflow
  - `minio` – object storage (console: port 9001)
  - `oer-scraper` – Airflow Webserver + Scheduler + DAGs
- `Dockerfile`: build image Airflow, cài Chrome/ChromeDriver, pip deps, copy DAGs, entrypoint.
- `entrypoint.sh`: khởi tạo Airflow DB, tạo user admin mặc định, chạy scheduler + webserver.
- `dags/`: các DAG và module scraper
  - `openstax_scraper_dag.py`, `openstax_scraper.py`
  - `otl_scraper_dag.py`, `otl_scraper.py`
  - `mit_ocw_scraper_dag.py`, `mit_ocw_scraper.py`
- `scraped_data/`: nơi container ghi tạm JSON trước khi upload MinIO.
- `logs/`: logs chi tiết của Airflow.
- `requirements.txt`: phiên bản thư viện.

### Dịch vụ & cổng
- Airflow Web UI: http://localhost:8080 (user/pass: admin/admin)
- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9001 (user/pass mặc định: minioadmin/minioadmin)
- PostgreSQL: localhost:5432 (user/pass: airflow/airflow, DB: airflow)

### Cài đặt & chạy nhanh (Docker)
Chạy từ thư mục gốc dự án (có `docker-compose.yml`, `Dockerfile`).

```bash
# Khởi chạy tất cả services, build image nếu cần
docker compose up -d --build

# Xem logs (tùy chọn)
docker compose logs -f oer-scraper
```

Sau khi webserver lên, truy cập Airflow UI tại http://localhost:8080, đăng nhập `admin/admin`.

### Rebuild khi sửa Dockerfile hoặc requirements
```bash
# Rebuild nhanh (giữ cache các layer khác)
docker compose build oer-scraper

docker compose up -d oer-scraper

# Rebuild không dùng cache (chắc chắn cài lại mọi thứ)
docker compose build --no-cache oer-scraper

docker compose up -d oer-scraper
```

### Cấu hình MinIO (ENV)
Các biến môi trường đã được truyền vào container `oer-scraper` trong `docker-compose.yml`:
- `MINIO_ENABLE=1`
- `MINIO_ENDPOINT=minio:9000`
- `MINIO_ACCESS_KEY=minioadmin`
- `MINIO_SECRET_KEY=minioadmin`
- `MINIO_SECURE=0` (đổi `1` nếu dùng HTTPS proxy)
- `MINIO_BUCKET=oer-raw`

Gợi ý key tổ chức dữ liệu trên MinIO:
- OpenStax: `openstax/{YYYY-MM-DD}/books_batch_{timestamp}.json`
- OTL: `otl/{YYYY-MM-DD}/textbooks_{timestamp}.json`
- MIT OCW: `mit_ocw/{YYYY-MM-DD}/courses_batch_{timestamp}.json`

### DAGs hiện có
- `openstax_scraper_daily`
  - Cào OpenStax (mặc định dùng Selenium, có fallback). Tự động backup MinIO, có emergency recovery.
- `otl_scraper_daily`
  - Cào Open Textbook Library bằng Selenium (headless). Hỗ trợ throttling, backoff, random UA/proxy, limit theo subject.
- `mit_ocw_scraper_daily`
  - Cào MIT OCW, tối ưu delay và batch, upload MinIO, có kiểm tra chất lượng và recovery.

Tần suất mặc định: mỗi DAG đặt `schedule_interval=timedelta(days=30)` và `catchup=False` (có nghĩa chạy định kỳ hàng tháng, bạn có thể trigger thủ công bất kỳ lúc nào trong UI).

### Chạy DAGs
1) Vào Airflow UI → bật DAG mong muốn.
2) Trigger `Run` nếu muốn chạy ngay hoặc chỉnh sửa `Schedule` nếu cần.

Một số tham số cho OTL có thể truyền qua 3 cách: ENV, Airflow Variables, hoặc `DagRun conf` khi trigger:
- Chế độ theo leaf: `SUBJECT_NAME`, `SUBJECT_URL`
- Chế độ theo root + tự khám phá child: `ROOT_SUBJECT_NAME`, `SUBJECTS_INDEX_URL`
- Cào toàn bộ leaves: `SUBJECTS_INDEX_URL`
- Giới hạn: `OTL_MAX_PARENTS`, `OTL_MAX_CHILDREN`
- Throttling/Anti-bot:
  - `OTL_WAIT_UNTIL_CLEAR=true|false`, `OTL_WAIT_MAX_MINUTES`
  - `OTL_DELAY_BASE_SEC`, `OTL_DELAY_JITTER_SEC`
  - `OTL_RETRY_BACKOFF_BASE_SEC`, `OTL_RETRY_BACKOFF_MAX_SEC`
  - `OTL_PER_BOOK_DELAY_SEC`, `OTL_SUBJECT_COOLDOWN_SEC`
  - `OTL_FORCE_RANDOM_UA=true|false`, `OTL_USER_AGENT`, `OTL_PROXY`
  - `OTL_BOOK_MAX_ATTEMPTS`, `OTL_RETRY_PDF_ON_MISS=true|false`
  - `OTL_SLOW_MODE=true|false` (bật preset an toàn nếu cần)

Ví dụ `DagRun conf` cho OTL (cào toàn bộ, preset an toàn):
```json
{
  "subjects_index_url": "https://open.umn.edu/opentextbooks/subjects",
  "OTL_WAIT_UNTIL_CLEAR": "true",
  "OTL_WAIT_MAX_MINUTES": "30",
  "OTL_DELAY_BASE_SEC": "1.2",
  "OTL_DELAY_JITTER_SEC": "0.6",
  "OTL_RETRY_BACKOFF_BASE_SEC": "5",
  "OTL_RETRY_BACKOFF_MAX_SEC": "60",
  "OTL_PER_BOOK_DELAY_SEC": "0.8",
  "OTL_SUBJECT_COOLDOWN_SEC": "4",
  "OTL_BOOK_MAX_ATTEMPTS": "6",
  "OTL_RETRY_PDF_ON_MISS": "true"
}
```

### Dòng dữ liệu & lưu trữ
- Dữ liệu thô được ghi tạm ở `scraped_data/` (được mount vào container) trước khi upload lên MinIO.
- Có các task dọn dẹp giữ lại ~30 ngày gần nhất để không đầy ổ đĩa.

### Mở rộng: thêm nguồn OER mới
Bạn có thể thêm nguồn mới theo mẫu sẵn có:
1) Tạo module scraper mới, ví dụ `dags/my_source_scraper.py` với các hàm chính: crawl, save local, upload MinIO, emergency backup.
2) Tạo DAG mới `dags/my_source_scraper_dag.py`:
   - Khai báo `DATA_PATH=/opt/airflow/scraped_data`
   - Các task: `setup_directories` → `scrape_my_source_documents` → `cleanup_old_files` → `health_check` (+ tuỳ chọn `check_minio_backups`, `emergency_recovery`, `validate_data_quality`).
3) Mount `dags/` sẵn trong compose, chỉ cần rebuild container nếu bạn thay đổi dependencies (code Python thuần trong `dags/` không yêu cầu rebuild image).
4) Nếu cần thêm thư viện mới → cập nhật `requirements.txt` rồi rebuild:
```bash
docker compose build --no-cache oer-scraper

docker compose up -d oer-scraper
```

### Phát triển & gỡ lỗi
- Xem log task trong Airflow UI (tab Logs của từng task).
- Xem log tổng service:
```bash
docker compose logs -f oer-scraper
```
- Vào MinIO Console để kiểm tra file đã upload.

### Tài khoản mặc định
- Airflow: `admin/admin`
- MinIO: `minioadmin/minioadmin`

### Yêu cầu hệ thống
- Docker Desktop / Docker Engine + Docker Compose v2
- Hệ điều hành đã thử: Windows 10/11, Linux, macOS (CPU x86_64)

### Ghi chú
- Chrome/ChromeDriver đã được cố định để tương thích; khi Chrome thay đổi lớn, có thể cần cập nhật `Dockerfile`.
- Khi gặp rate limit/anti-bot, bật chế độ chậm hoặc đặt proxy/UA phù hợp.
