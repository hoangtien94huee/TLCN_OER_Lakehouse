# OER Lakehouse Scraper (Airflow + Selenium)

### Tổng quan
- Scraper cho 2 nguồn: `OpenStax` và `Open Textbook Library (OTL)`.
- Orchestrate bằng Apache Airflow (Docker), Selenium headless Chrome.
- Log chi tiết, tuỳ biến tốc độ/throttle, upload MinIO theo chuẩn thư mục data lake.

### Cấu trúc thư mục
```
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── airflow.cfg
├── entrypoint.sh
├── dags/
│   ├── oer_scraper_dag.py        # DAG cho OpenStax
│   ├── otl_scraper_dag.py        # DAG cho Open Textbook Library (Selenium-only)
│   ├── openstax_scraper.py       # Module scraper OpenStax (self-contained)
│   └── otl_scraper.py            # Module scraper OTL (self-contained)
├── scraped_data/                 # Lưu JSON tạm trong container
├── database/
└── logs/
```

### Chạy nhanh (Docker)
```bash
docker-compose up -d --build
# Airflow UI: http://localhost:8080 (admin/admin)
```

### DAGs
- `oer_scraper_daily`: OpenStax
- `otl_scraper_daily`: Open Textbook Library

### Cấu hình MinIO (ENV hoặc Airflow Variables)
- `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`, `MINIO_SECURE=false|true`
- Đường dẫn upload OTL: `oer-raw/otl/{YYYY-MM-DD}/{filename}.json`
- File local OTL: `scraped_data/open_textbook_library_{YYYY-MM-DD}.json`

### OTL scraping modes (qua ENV/Variables/DAG Run conf)
Chọn 1 trong các cách vào DAG `otl_scraper_daily`:
- Leaf một subject: `SUBJECT_NAME`, `SUBJECT_URL`
- Root + tự phát hiện child: `ROOT_SUBJECT_NAME`, `SUBJECTS_INDEX_URL`
- Toàn bộ leaves: `SUBJECTS_INDEX_URL`
- Tuỳ chọn giới hạn: `OTL_MAX_PARENTS`, `OTL_MAX_CHILDREN`

### Throttling/Anti-bot (OTL)
- Chờ hết “Retry later”: `OTL_WAIT_UNTIL_CLEAR=true`, `OTL_WAIT_MAX_MINUTES=30`
- Nhịp cơ bản: `OTL_DELAY_BASE_SEC`, `OTL_DELAY_JITTER_SEC`
- Backoff: `OTL_RETRY_BACKOFF_BASE_SEC`, `OTL_RETRY_BACKOFF_MAX_SEC`
- Per-book/subject cooldown: `OTL_PER_BOOK_DELAY_SEC`, `OTL_SUBJECT_COOLDOWN_SEC`
- User-Agent/Proxy: `OTL_FORCE_RANDOM_UA=true`, `OTL_USER_AGENT`, `OTL_PROXY`
- Retry giữa trang sách: `OTL_BOOK_MAX_ATTEMPTS`, `OTL_RETRY_PDF_ON_MISS=true`
- Slow preset: `OTL_SLOW_MODE=true` (đặt giá trị an toàn mặc định)

### Schema output (mỗi document)
```json
{
  "id": "<md5(url_title)>",
  "title": "...",
  "description": "...",
  "authors": ["..."],
  "subjects": ["..."],
  "source": "Open Textbook Library | OpenStax",
  "url": "<ưu tiên PDF nếu có, nếu không là URL sách>",
  "url_pdf": "<PDF nếu có>",
  "scraped_at": "ISO8601"
}
```

### Ví dụ cấu hình DAG Run (OTL – toàn bộ leaves, siêu an toàn)
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

### OpenStax
- DAG: `oer_scraper_daily`
- Tuỳ chọn giới hạn khi test: `OPENSTAX_MAX_BOOKS`
- MinIO dùng cùng thông số trên; object key tương tự theo ngày nguồn OpenStax.

### Vận hành
- Kích hoạt DAG trong Airflow UI, hoặc Trigger DAG Run và truyền conf như trên.
- Logs chi tiết hiển thị tiến độ: phát hiện subject, phân trang/scroll, số sách, parse từng sách, phát hiện PDF.

### Dọn dẹp
- Task `cleanup_old_files` tự xoá JSON local > 30 ngày trong `scraped_data/`.

### Troubleshooting ngắn
- Gặp “Retry later” nhiều: bật `OTL_WAIT_UNTIL_CLEAR`, tăng backoff, thêm `OTL_PROXY`, giới hạn `OTL_MAX_PARENTS/CHILDREN`, chạy ngoài giờ cao điểm.
