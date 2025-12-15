# OER Data Lakehouse & Intelligent Search Platform

## Giới thiệu Dự án

**OER Data Lakehouse & Intelligent Search Platform** là một giải pháp công nghệ toàn diện nhằm giải quyết bài toán phân mảnh và khó tiếp cận của Tài nguyên Giáo dục Mở (Open Educational Resources - OER). Trong bối cảnh tài liệu học thuật nằm rải rác trên nhiều nền tảng khác nhau (MIT OCW, OpenStax, OTL...), việc tìm kiếm và tổng hợp kiến thức trở nên khó khăn đối với giảng viên và sinh viên.

Dự án này xây dựng một **Data Lakehouse** tập trung, có khả năng mở rộng cao, kết hợp với một **Search Engine** thông minh. Hệ thống không chỉ lưu trữ dữ liệu mà còn hiểu sâu nội dung bên trong các tài liệu PDF, cho phép tìm kiếm chính xác đến từng trang sách và gợi ý tài liệu phù hợp nhất với nhu cầu người dùng.

---

## Tính năng & Điểm nổi bật

### 1. Kiến trúc Data Lakehouse Hiện đại (Scalable Architecture)
Hệ thống được xây dựng dựa trên kiến trúc **Medallion (Bronze/Silver/Gold)** chuẩn mực trong công nghiệp dữ liệu:
*   **Bronze Layer (Raw Data)**: Lưu trữ dữ liệu thô nguyên bản từ các nguồn (JSON, PDF) trên MinIO, đảm bảo không mất mát thông tin gốc.
*   **Silver Layer (Cleaned Data)**: Dữ liệu được làm sạch, chuẩn hóa schema và lưu trữ dưới định dạng **Apache Iceberg**, hỗ trợ các tính năng ACID transaction và time-travel.
*   **Gold Layer (Curated Data)**: Dữ liệu được mô hình hóa theo dạng **Star Schema** (Dimension/Fact tables) tối ưu cho việc truy vấn Analytics và Search Indexing.

### 2. Hệ thống Tìm kiếm Chuyên sâu (Advanced Search Engine)
Khác biệt với các hệ thống tìm kiếm thông thường chỉ trả về tên file, hệ thống này cung cấp khả năng tìm kiếm sâu (Deep Search):
*   **Nested PDF Search**: Sử dụng cấu trúc dữ liệu lồng nhau (Nested Objects) trong Elasticsearch để index nội dung từng trang PDF riêng biệt. Kết quả tìm kiếm sẽ chỉ ra chính xác từ khóa xuất hiện ở **trang nào** và hiển thị đoạn văn bản (snippet) ngữ cảnh tương ứng.
*   **Smart Text Cleaning**: Tích hợp thuật toán tự động phát hiện và loại bỏ **Header/Footer** (như tên sách, số trang lặp lại) để làm sạch nội dung trước khi index, giúp tăng độ chính xác (Precision) của kết quả tìm kiếm lên đáng kể.
*   **Custom Relevance Scoring**: Cơ chế xếp hạng kết quả thông minh, kết hợp giữa độ phù hợp từ khóa, tần suất xuất hiện và tính mới của tài liệu (**Gaussian Decay Scoring** - ưu tiên tài liệu mới xuất bản).

### 3. Tự động hóa Quy trình Dữ liệu (Automated ETL Pipelines)
Toàn bộ quy trình xử lý dữ liệu được tự động hóa hoàn toàn bằng **Apache Airflow**:
*   **Web Scraping**: Tự động thu thập dữ liệu định kỳ từ các nguồn MIT OCW, OpenStax.
*   **Data Transformation**: Các Spark Jobs tự động làm sạch, chuyển đổi và mô hình hóa dữ liệu qua các tầng Bronze -> Silver -> Gold.
*   **Search Sync**: Tự động đồng bộ dữ liệu mới nhất từ Gold Layer vào Elasticsearch mà không làm gián đoạn dịch vụ tìm kiếm.

### 4. Ứng dụng Tìm kiếm & Gợi ý (Search & Recommendation App)
*   **Giao diện Người dùng Thân thiện**: Cho phép tìm kiếm full-text, lọc theo nguồn, ngôn ngữ, và xem trước nội dung PDF ngay trên trình duyệt.
*   **Recommendation Engine**: Hệ thống gợi ý tài liệu liên quan (Content-based Filtering) dựa trên sự tương đồng về nội dung và chủ đề, giúp người dùng khám phá thêm các tài liệu hữu ích.

---

## Kiến trúc Hệ thống Chi tiết

Sơ đồ luồng dữ liệu (Data Flow) của hệ thống:

![Sơ đồ luồng dữ liệu](image/structure.png)


## Công nghệ & Công cụ (Tech Stack)

Dự án sử dụng bộ công nghệ hiện đại (Modern Data Stack):

| Lĩnh vực | Công nghệ | Vai trò |
| :--- | :--- | :--- |
| **Storage** | **MinIO** | Object Storage lưu trữ dữ liệu (S3 Compatible), thay thế HDFS. |
| **Table Format** | **Apache Iceberg** | Định dạng bảng dữ liệu mở, hỗ trợ ACID và Schema Evolution. |
| **Processing** | **Apache Spark** (PySpark) | Engine xử lý dữ liệu lớn phân tán mạnh mẽ. |
| **Orchestration** | **Apache Airflow** | Quản lý, lên lịch và giám sát các luồng công việc (Workflows). |
| **Search Engine** | **Elasticsearch 8.15** | Công cụ tìm kiếm và phân tích phân tán, hỗ trợ Full-text search. |
| **Backend API** | **FastAPI** (Python) | Framework xây dựng API hiệu năng cao, bất đồng bộ. |
| **Frontend** | **Jinja2 Templates**, Bootstrap | Giao diện người dùng đơn giản, hiệu quả. |
| **Infrastructure** | **Docker**, Docker Compose | Đóng gói và triển khai ứng dụng nhất quán trên mọi môi trường. |

---

## Cấu trúc Thư mục Dự án

```
TLCN_OER_Lakehouse/
├── airflow/
│   ├── dags/                          # Định nghĩa các luồng xử lý (DAGs)
│   │   ├── silver_layer_processing.py # ETL Bronze -> Silver
│   │   ├── gold_layer_processing.py   # ETL Silver -> Gold
│   │   └── elasticsearch_sync.py      # Sync Gold -> Elasticsearch
│   ├── src/                           # Mã nguồn xử lý chính (Spark Jobs & Utils)
│   │   ├── bronze_*.py                # Scrapers cho từng nguồn
│   │   ├── silver_*.py                # Logic làm sạch dữ liệu
│   │   ├── gold_*.py                  # Logic tạo Dimension/Fact tables
│   │   ├── elasticsearch_sync.py      # Logic xử lý PDF và Indexing
│   │   └── recommendation_engine.py   # Logic gợi ý tài liệu
│   └── requirements.txt               # Các thư viện Python cần thiết
├── search_app/                        # Ứng dụng Web Tìm kiếm
│   ├── main.py                        # FastAPI Application Entrypoint
│   ├── templates/                     # Giao diện HTML
│   └── static/                        # Tài nguyên tĩnh (CSS/JS)
├── docker-compose.yml                 # Cấu hình triển khai toàn bộ hệ thống
└── README.md                          # Tài liệu hướng dẫn (File này)
```

---

## Hướng dẫn Cài đặt & Triển khai

### Yêu cầu hệ thống
*   **Hệ điều hành**: Linux (Ubuntu/CentOS) hoặc Windows (WSL2), macOS.
*   **Phần mềm**: Docker Desktop (hoặc Docker Engine + Docker Compose).
*   **Tài nguyên**: Tối thiểu 8GB RAM (Khuyến nghị 16GB để chạy mượt mà Spark và Elasticsearch).

### Các bước triển khai chi tiết

1.  **Clone mã nguồn dự án**:
    ```bash
    git clone https://github.com/username/TLCN_OER_Lakehouse.git
    cd TLCN_OER_Lakehouse
    ```

2.  **Khởi động môi trường Docker**:
    Lệnh này sẽ tải các images cần thiết và khởi động toàn bộ các services (Airflow, MinIO, Spark, Elasticsearch...).
    ```bash
    docker-compose up -d
    ```
    *Lưu ý: Lần đầu chạy có thể mất vài phút để tải images.*

3.  **Truy cập các giao diện quản trị**:
    Sau khi khởi động thành công, bạn có thể truy cập:
    *   **Search Portal (Người dùng cuối)**: [http://localhost:8000](http://localhost:8000)
    *   **Airflow UI (Quản lý ETL)**: [http://localhost:8080](http://localhost:8080)
        *   Tài khoản mặc định: `airflow` / `airflow`
    *   **MinIO Console (Quản lý Dữ liệu)**: [http://localhost:9001](http://localhost:9001)
        *   Tài khoản mặc định: `minioadmin` / `minioadmin`

4.  **Vận hành dữ liệu mẫu**:
    Để hệ thống có dữ liệu, bạn cần chạy các DAGs trong Airflow theo thứ tự:
    1.  Vào Airflow UI -> Tìm DAG `silver_layer_processing` -> Bật (Unpause) -> Trigger DAG.
    2.  Đợi Silver chạy xong -> Trigger DAG `gold_layer_processing`.
    3.  Đợi Gold chạy xong -> Trigger DAG `elasticsearch_sync` để đưa dữ liệu vào Search Engine.

---

## Phân tích Kỹ thuật Chuyên sâu

### 1. Thuật toán Xử lý PDF (Smart PDF Processing)
Để giải quyết vấn đề "rác" dữ liệu trong PDF (header/footer lặp lại), hệ thống áp dụng thuật toán:
*   **Pattern Detection**: Phân tích dòng đầu và dòng cuối của tất cả các trang trong một tài liệu.
*   **Threshold Filtering**: Nếu một chuỗi ký tự xuất hiện ở vị trí cố định trên >70% số trang, nó được xác định là Header/Footer.
*   **Cleaning**: Loại bỏ chuỗi này khỏi nội dung index, giúp từ khóa tìm kiếm không bị match nhầm vào các phần không quan trọng này.

### 2. Chiến lược Indexing (Nested Indexing Strategy)
Thay vì gộp toàn bộ nội dung sách vào một trường text khổng lồ, hệ thống chia nhỏ (chunking) theo trang:
```json
{
  "title": "Introduction to Machine Learning",
  "pdf_chunks": [
    { "page": 1, "text": "Chapter 1: Supervised Learning..." },
    { "page": 2, "text": "In this chapter we discuss..." }
  ]
}
```
Điều này cho phép Elasticsearch thực hiện **Nested Query**, trả về chính xác: *"Tìm thấy từ khóa 'Supervised Learning' tại **Trang 1**"* thay vì chỉ trả về tên cuốn sách chung chung.

### 3. Công thức Xếp hạng (Ranking Formula)
Điểm số (Score) của một tài liệu được tính toán tổng hợp:
$$ Score = (Relevance \times Boost) + (Recency \times 1.5) + (NestedMatch \times 3.0) $$
*   **Relevance**: Độ khớp của từ khóa (BM25).
*   **Recency**: Sử dụng hàm **Gaussian Decay** để giảm dần điểm của các tài liệu quá cũ (ví dụ: tài liệu 10 năm trước sẽ bị giảm điểm so với tài liệu năm nay).
*   **NestedMatch**: Cộng điểm thưởng lớn nếu từ khóa xuất hiện trong nội dung chi tiết của PDF, đảm bảo người dùng tìm được tài liệu có nội dung thực sự liên quan.

