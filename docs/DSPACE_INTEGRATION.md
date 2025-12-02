# Hướng dẫn tích hợp OER vào DSpace

## Tổng quan

Hệ thống đã được thiết lập để đồng bộ tài nguyên OER từ Gold Layer (Data Lakehouse) vào DSpace Repository.

## Kiến trúc

```
┌─────────────────┐      ┌──────────────┐      ┌─────────────┐
│  Bronze Layer   │ ───> │ Silver Layer │ ───> │ Gold Layer  │
│  (Raw Scrapes)  │      │ (Cleaned)    │      │ (Analytics) │
└─────────────────┘      └──────────────┘      └─────────────┘
                                                       │
                         ┌─────────────────────────────┼────────────────────────┐
                         │                             │                        │
                         ▼                             ▼                        ▼
                  ┌──────────────┐            ┌──────────────┐        ┌──────────────┐
                  │ Elasticsearch│            │    DSpace    │        │   Dremio     │
                  │  (Search UI) │            │  (Library)   │        │  (BI/SQL)    │
                  └──────────────┘            └──────────────┘        └──────────────┘
```

## Thành phần đã cài đặt

### 1. DSpace 9.x

- **Backend REST API**: http://localhost:8180/server/api
- **Frontend UI**: http://localhost:4000
- **Admin**: admin@dspace.org / dspace
- **Collection UUID**: `1e9be5e4-6056-4f43-bad0-0288669ce6ad`

### 2. Airflow DAG

- **DAG Name**: `dspace_integration_dag`
- **Schedule**: Daily (`@daily`)
- **Location**: `/opt/airflow/dags/dspace_integration_dag.py`

### 3. Sync Script

- **Location**: `/opt/airflow/src/dspace_sync.py`
- **Function**: Đọc Gold Layer → Tạo items trong DSpace → Upload PDFs

## Cách sử dụng

### Bước 1: Truy cập DSpace UI

1. Mở browser: http://localhost:4000
2. Đăng nhập: admin@dspace.org / dspace
3. Kiểm tra Community "OER Repository" đã được tạo

### Bước 2: Chạy DAG từ Airflow

1. Mở Airflow UI: http://localhost:8080
2. Tìm DAG: `dspace_integration_dag`
3. Enable DAG (toggle button)
4. Click "Trigger DAG" để chạy manual
5. Theo dõi logs trong Task Instance

### Bước 3: Kiểm tra kết quả

1. Vào DSpace UI: http://localhost:4000
2. Browse Collection "Open Educational Resources"
3. Xem các items đã được sync

## Cấu hình

### Environment Variables (trong docker-compose.yml)

```yaml
- DSPACE_API_ENDPOINT=http://dspace:8080/server/api
- DSPACE_USER=admin@dspace.org
- DSPACE_PASSWORD=dspace
- DSPACE_COLLECTION_UUID=1e9be5e4-6056-4f43-bad0-0288669ce6ad
```

### Metadata Mapping (Gold Layer → Dublin Core)

| Gold Layer Field   | DSpace Field              | Example                            |
| ------------------ | ------------------------- | ---------------------------------- |
| `title`            | `dc.title`                | "Introduction to Computer Science" |
| `creator_names`    | `dc.contributor.author`   | ["John Doe", "Jane Smith"]         |
| `description`      | `dc.description.abstract` | "Course materials for CS101"       |
| `publication_date` | `dc.date.issued`          | "2024-09-01"                       |
| `publisher_name`   | `dc.publisher`            | "MIT OpenCourseWare"               |
| `language_code`    | `dc.language.iso`         | "en"                               |
| `source_url`       | `dc.identifier.uri`       | "https://ocw.mit.edu/..."          |

## Quản lý

### Xem logs sync

```bash
docker logs oer-airflow-scraper -f
```

### Restart services

```bash
# Restart Airflow
docker-compose restart oer-scraper

# Restart DSpace
docker-compose restart dspace dspace-angular

# Restart Solr
docker-compose restart dspacesolr
```

### Xóa và tạo lại Collection

```bash
# Vào DSpace container
docker exec -it dspace bash

# Xóa community (sẽ xóa cả collection bên trong)
/dspace/bin/dspace delete-community -i <community-uuid>

# Tạo lại bằng structure-builder
/dspace/bin/dspace structure-builder -f /tmp/oer-structure.xml -o /tmp/output.xml -e admin@dspace.org
```

### Kiểm tra Collection UUID mới

```bash
# Từ máy host
curl http://localhost:8180/server/api/core/collections | jq '.._embedded.collections[] | {uuid, name, handle}'
```

## Lưu ý

### 1. Khởi động từ đầu

Khi restart toàn bộ stack, DSpace sẽ:

- Tự động tạo Solr cores (`search`, `statistics`)
- Migrate database schema
- Tạo admin user nếu chưa có

### 2. Duplicate Handling

Script `dspace_sync.py` kiểm tra duplicate dựa vào `dc.identifier.uri` (source_url).
Nếu item đã tồn tại, sẽ skip.

### 3. PDF Upload

- PDFs được lấy từ MinIO bucket: `oer-lakehouse`
- Path format: `s3a://oer-lakehouse/bronze/.../*.pdf`
- Cần đảm bảo Airflow container có thể access MinIO

### 4. Performance

- Sync chạy sequential qua REST API (1 item/request)
- Với dataset lớn, cân nhắc batch processing hoặc parallel workers
- Monitor Spark executor memory nếu xử lý nhiều PDFs

## Troubleshooting

### Lỗi 403 khi tạo item

- Kiểm tra authentication token còn valid không
- Retry login nếu session timeout

### Lỗi Collection not found

- Xác nhận Collection UUID đúng
- Kiểm tra collection có tồn tại:
  ```bash
  curl http://localhost:8180/server/api/core/collections/<uuid>
  ```

### Solr không có cores

- Restart `dspacesolr` container
- Kiểm tra logs: `docker logs dspacesolr`
- Verify cores: http://localhost:8983/solr/admin/cores?action=STATUS

### Angular UI lỗi 500

- Kiểm tra backend API: http://localhost:8180/server/api
- Restart Angular: `docker-compose restart dspace-angular`
- Xem logs: `docker logs dspace-angular`

## API Testing

### Get Collection Info

```bash
curl http://localhost:8180/server/api/core/collections/1e9be5e4-6056-4f43-bad0-0288669ce6ad
```

### Search Items

```bash
curl "http://localhost:8180/server/api/discover/search/objects?query=*&dsoType=ITEM"
```

### Login (Get Token)

```bash
curl -X POST http://localhost:8180/server/api/authn/login \
  -d "user=admin@dspace.org&password=dspace" \
  -c cookies.txt -b cookies.txt
```

## Tài liệu tham khảo

- DSpace 9 REST API: https://wiki.lyrasis.org/display/DSDOC9x/REST+API
- DSpace Structure Builder: https://wiki.lyrasis.org/display/DSDOC9x/AIP+Batch+Ingest
- Iceberg Tables: https://iceberg.apache.org/docs/latest/
