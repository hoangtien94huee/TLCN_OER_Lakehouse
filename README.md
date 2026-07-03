# OER Lakehouse - Hệ thống Quản lý & Tra cứu Tài nguyên Giáo dục Mở Thông minh

## Giới thiệu

**OER Lakehouse** là một giải pháp kiến trúc Data Lakehouse toàn diện, được thiết kế để giải quyết bài toán thu thập, lưu trữ, chuẩn hóa và khai thác dữ liệu từ các nguồn tài nguyên giáo dục mở (OER) lớn trên thế giới như **MIT OpenCourseWare (OCW)**, **OpenStax**, và **Open Textbook Library (OTL)**. 

Dự án tích hợp sâu với kho lưu trữ số **DSpace 9**, kết hợp với công cụ tìm kiếm phân tích **Elasticsearch** và mô hình ngôn ngữ lớn **LLM (Ollama/Qwen2.5)** để cung cấp hệ thống chatbot trợ lý học tập (RAG) thông thái, có khả năng hiểu ngữ cảnh môn học và tìm kiếm chính xác đến từng trang tài liệu PDF.

---

## Sơ đồ Kiến trúc Hệ thống

Hệ thống hoạt động theo mô hình luồng dữ liệu tự động từ lúc thu thập đến khi phục vụ người dùng cuối:

![Sơ đồ kiến trúc luồng dữ liệu](image/structure.png)

---

## Tính năng Nổi bật

### 1. Thu thập Tự động (Apache Airflow Scrapers)
* Tự động crawl tài liệu, bài giảng, và sách giáo trình từ các nguồn học liệu mở hàng đầu.
* Lên lịch chạy định kỳ, tự động phát hiện và tải xuống các tài liệu mới dạng PDF cùng siêu dữ liệu (metadata).

### 2. Kiến trúc Medallion (Apache Spark + Apache Iceberg + MinIO)
Hệ thống tổ chức dữ liệu theo kiến trúc 3 lớp chuẩn hóa hiệu năng cao:
* **Bronze (Raw Layer)**: Lưu trữ các tệp tin thô (PDF, metadata dạng JSON) trực tiếp trên MinIO (Object Storage tương thích S3).
* **Silver (Cleaned Layer)**: Sử dụng Apache Spark xử lý, tách trang PDF, làm sạch văn bản (loại bỏ header/footer lặp lại), phân tích cấu trúc chương (TOC) và lưu trữ dưới định dạng bảng Apache Iceberg.
* **Gold (Aggregated Layer)**: Tổ chức dữ liệu dưới dạng Star Schema phục vụ báo cáo và đồng bộ dữ liệu tìm kiếm.

### 3. Tìm kiếm & Trích xuất Thông minh (Deep PDF Search)
* **Nested Page Indexing**: Lập chỉ mục nội dung ở cấp độ từng trang PDF riêng lẻ thay vì toàn bộ cuốn sách, giúp tìm kiếm đạt độ chính xác tối đa.
* **BM25 Search & Keyword Boosting**: Thuật toán xếp hạng nâng cao trên Elasticsearch, tự động tối ưu hóa từ khóa chuyên ngành Giải tích/Đại số và tăng trọng số cho các chương định nghĩa cốt lõi.

### 4. Trợ lý Học tập RAG (Context-Aware Chatbot)
* Tích hợp chatbot thông minh trực tiếp vào hệ thống **Moodle LMS** và **DSpace 9**.
* **Hiểu ngữ cảnh học tập**: Chatbot tự động nhận diện thông tin môn học, chương mục (`course`, `section`, `activity`) và lịch sử trò chuyện để cá nhân hóa kết quả phản hồi.
* **Kết nối Local LLM**: Sử dụng mô hình **Qwen2.5:7b-instruct** chạy thông qua Ollama (kết nối mạng Tailscale) giúp bảo mật dữ liệu và dịch thuật thuật ngữ toán học/kỹ thuật sang tiếng Việt tự nhiên, chuẩn xác.

---

## Cấu trúc Thư mục Dự án

```
TLCN_OER_Lakehouse/
├── airflow/
│   ├── dags/                           # Các định nghĩa DAGs của Airflow
│   │   ├── mit_ocw_scraper_dag.py      # Thu thập dữ liệu MIT OCW
│   │   ├── openstax_scraper_dag.py     # Thu thập dữ liệu OpenStax
│   │   ├── otl_scraper_dag.py          # Thu thập dữ liệu OTL
│   │   ├── silver_layer_processing_dag.py # Chuyển đổi dữ liệu sang Silver Layer
│   │   ├── gold_layer_processing_dag.py   # Chuyển đổi dữ liệu sang Gold Layer
│   │   └── elasticsearch_sync_dag.py   # Đồng bộ dữ liệu sang Elasticsearch
│   ├── src/                            # Các module xử lý dữ liệu chính
│   │   ├── bronze_*.py                 # Script thu thập dữ liệu gốc
│   │   ├── silver_transform.py         # ETL tiền xử lý văn bản, trích xuất TOC
│   │   ├── gold_analytics.py           # Phân tích tổng hợp dữ liệu
│   │   ├── elasticsearch_sync.py       # Lập chỉ mục tìm kiếm trang PDF
│   │   ├── pageindex.py                # Công cụ tìm kiếm & Router RAG cốt lõi
│   │   └── chatbot_api.py              # FastAPI phục vụ Chatbot API
│   ├── Dockerfile
│   └── requirements.txt
│
├── demo/
│   ├── dspace-angular-fresh/           # DSpace 9 Angular Frontend (Custom Theme)
│   │   └── src/app/chatbot-popup/      # Popup widget chat trên giao diện DSpace
│   ├── chatbot-popup-demo.html         # Giao diện demo chatbot độc lập
│   └── chatbot.html
│
├── moodle/
│   └── local/oerchatbot/               # Plugin Chatbot tích hợp cho Moodle LMS
│       ├── amd/src/widget.js           # Logic chat client, xử lý định dạng LaTeX & Markdown
│       ├── amd/build/widget.min.js     # Bản JS rút gọn chạy thực tế trên Moodle
│       └── classes/hook/before_footer.php # Hook chèn widget nổi vào footer Moodle
│
├── scripts/                            # Các script cấu hình và cài đặt nhanh
│   ├── moodle_seed_data.php            # Dữ liệu mẫu (course, category) cho Moodle
│   └── run_pageindex_stack.sh          # Script khởi động cụm xử lý
│
├── docker-compose.yml                  # File cấu hình Docker khởi chạy toàn bộ dịch vụ
└── README.md
```

---

## Hướng dẫn Khởi động Nhanh

### 1. Yêu cầu Hệ thống
* **RAM**: Khuyến nghị tối thiểu 16 GB RAM (do chạy đồng thời cả cụm Spark, Elasticsearch và DSpace).
* **CPU**: Tối thiểu 4 Cores.
* **Hệ điều hành**: Ubuntu 20.04+, Windows 10/11 với WSL2, hoặc macOS.
* **Công cụ**: Docker Engine + Docker Compose.

### 2. Cấu hình Biến Môi trường
Tạo file `.env` tại thư mục gốc của dự án với các thông tin sau:
```bash
# Cấu hình kết nối Ollama GPU qua Tailscale (hoặc Local)
LOCAL_LLM_BACKEND=ollama
LOCAL_LLM_MODEL=qwen2.5:7b-instruct
LOCAL_LLM_BASE_URL=http://100.68.202.102:11434  # Thay bằng IP Ollama của bạn

# Các thông số timeout cấu hình cho API RAG
PAGEINDEX_ASK_TIMEOUT=75
LOCAL_LLM_PROBE_TIMEOUT=10
```

### 3. Khởi động Các Dịch vụ Core
Chạy lệnh sau để khởi động cụm Lakehouse cốt lõi (Postgres, MinIO, Spark, Elasticsearch, Airflow, DSpace):
```bash
docker compose up -d
```

Để chạy thêm nền tảng học tập trực tuyến Moodle LMS phụ trợ:
```bash
docker compose --profile lms up -d
```

---

## Hướng dẫn cấu hình Chatbot trên Moodle LMS

1. Đảm bảo plugin `oerchatbot` đã được cài đặt và cập nhật trong container Moodle.
2. Truy cập Moodle với tài khoản Admin:
   * **URL**: `http://localhost:18085` (hoặc cổng cấu hình tương ứng)
   * **User**: `admin`
   * **Password**: `Admin@12345`
3. Đi tới: `Site administration -> Plugins -> Local plugins -> OER Chatbot`
4. Thực hiện cấu hình các thông số:
   * **Chatbot API URL**: `http://host.docker.internal:18088/api/ask` (nếu API chạy ở máy host) hoặc trỏ trực tiếp tới IP của API container.
   * **Enable widget**: Bật (Checked).
5. **Cập nhật JS và Dọn Cache**:
   Nếu bạn chỉnh sửa file JavaScript của chatbot client (`moodle/local/oerchatbot/amd/src/widget.js`), bạn cần đồng bộ vào Docker và xóa cache của Moodle để thay đổi có hiệu lực:
   ```bash
   # Đồng bộ file build vào container
   docker cp moodle/local/oerchatbot/amd/src/widget.js oer-moodle:/bitnami/moodle/local/oerchatbot/amd/build/widget.min.js
   
   # Purge caches Moodle
   docker exec oer-moodle php /bitnami/moodle/admin/cli/purge_caches.php
   ```

---

## Địa chỉ Truy cập và Giám sát Hệ thống

| Dịch vụ | URL | Thông tin đăng nhập mặc định |
| :--- | :--- | :--- |
| **Hệ thống Airflow** | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` |
| **DSpace 9 Frontend** | [http://localhost:4000](http://localhost:4000) | `admin@dspace.org` / `dspace` |
| **Moodle LMS Portal** | [http://localhost:18085](http://localhost:18085) | `admin` / `Admin@12345` |
| **MinIO Console (S3)** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **Spark Master Board** | [http://localhost:8081](http://localhost:8081) | - |
| **Chatbot Backend API**| [http://localhost:18088](http://localhost:18088) | - |

---

## Troubleshooting (Xử lý sự cố thường gặp)

### 1. Tìm kiếm và Chatbot phản hồi chậm hoặc bị Timeout
* **Nguyên nhân**: Kết nối mạng tới Ollama server qua VPN/Tailscale bị trễ hoặc GPU quá tải.
* **Khắc phục**: 
  1. Kiểm tra trạng thái kết nối Ollama bằng lệnh: `curl http://100.68.202.102:11434/api/tags`
  2. Tăng giá trị `LOCAL_LLM_PROBE_TIMEOUT` trong file `.env` lên `10` hoặc `15`.

### 2. Lỗi giao diện Chat hiển thị bullet lặp lại hoặc lỗi in đậm
* **Nguyên nhân**: Lịch sử phiên chat cũ vẫn được lưu trong `sessionStorage` của trình duyệt và gửi kèm lên API khiến mô hình LLM lặp lại cấu trúc lỗi cũ.
* **Khắc phục**: Đóng tab trình duyệt hiện tại và truy cập lại ở Tab mới, hoặc mở Tab ẩn danh (Incognito) để dọn sạch lịch sử phiên chat cũ.

### 3. Reset toàn bộ dữ liệu hệ thống về ban đầu
Để làm sạch toàn bộ container và xóa các volume dữ liệu cũ nhằm cài đặt lại từ đầu:
```bash
docker compose --profile lms down -v
docker compose --profile lms up -d --build
```
