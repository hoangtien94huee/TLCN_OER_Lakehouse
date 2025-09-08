from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os
from selenium_scraper import AdvancedOERScraper

# Cấu hình DAG riêng cho Open Textbook Library
default_args = {
    'owner': 'oer-scraper',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'otl_scraper_daily',
    default_args=default_args,
    description='Daily scraping for Open Textbook Library',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['scraping', 'oer', 'open_textbook_library'],
)

DATA_PATH = '/opt/airflow/scraped_data'

def setup_directories():
    os.makedirs(DATA_PATH, exist_ok=True)
    print(f"[OTL] Đã tạo thư mục: {DATA_PATH}")

def scrape_open_textbook_library_documents(**context):
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OTL] Bắt đầu cào dữ liệu cho ngày: {execution_date}")
    # Selenium + tối ưu streaming
    scraper = AdvancedOERScraper(delay=0.0, use_selenium=True)
    try:
        documents = scraper.scrape_open_textbook_library()
        # Upload PDFs to MinIO if downloaded
        if documents and str(os.getenv('OTL_DOWNLOAD_PDFS', '0')).lower() in {'1', 'true', 'yes'}:
            for doc in documents:
                if doc.get('pdf_path'):
                    s3_key = scraper.upload_pdf_to_minio(doc['pdf_path'], 'otl', execution_date)
                    if s3_key:
                        doc['pdf_s3_key'] = s3_key

        # Upload raw JSONL to MinIO
        object_key = scraper.upload_raw_records_to_minio(documents, 'otl', execution_date)
        print(f"[OTL] MinIO object: {object_key}")
        return {
            'execution_date': execution_date,
            'total_found': len(documents),
            'minio_object_key': object_key
        }
    except Exception as e:
        print(f"[OTL] Lỗi khi cào dữ liệu: {e}")
        raise
    finally:
        scraper.cleanup()

def cleanup_old_files(**context):
    """Xóa các file JSON quá 30 ngày (so sánh theo date, an toàn timezone)."""
    logical_date = context.get('logical_date') or context.get('execution_date')
    cutoff_date = (logical_date.date() - timedelta(days=30))
    removed_files = 0

    try:
        entries = os.listdir(DATA_PATH)
    except FileNotFoundError:
        print(f"[OTL] Thư mục không tồn tại: {DATA_PATH}")
        return

    for filename in entries:
        if filename.startswith('open_textbook_library_') and filename.endswith('.json'):
            try:
                date_str = filename.replace('open_textbook_library_', '').replace('.json', '')
                file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                if file_date < cutoff_date:
                    file_path = os.path.join(DATA_PATH, filename)
                    try:
                        os.remove(file_path)
                        removed_files += 1
                        print(f"[OTL] Đã xóa file cũ: {filename}")
                    except OSError as e:
                        print(f"[OTL] Không thể xóa {filename}: {e}")
            except ValueError:
                # Tên file không đúng định dạng ngày, bỏ qua
                continue
    print(f"[OTL] Đã dọn dẹp {removed_files} file cũ")

setup_task = PythonOperator(
    task_id='setup_directories',
    python_callable=setup_directories,
    dag=dag,
)

scrape_task = PythonOperator(
    task_id='scrape_open_textbook_library_documents',
    python_callable=scrape_open_textbook_library_documents,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files,
    dag=dag,
)

health_check_task = BashOperator(
    task_id='health_check',
    bash_command='echo "OTL scraping pipeline completed successfully at $(date)"',
    dag=dag,
)

setup_task >> scrape_task >> cleanup_task >> health_check_task


