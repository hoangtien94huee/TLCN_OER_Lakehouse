from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os
from selenium_scraper import AdvancedOERScraper

# Cấu hình DAG
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
    'oer_scraper_daily',
    default_args=default_args,
    description='Daily OER documents scraping from OpenStax',
    schedule_interval=timedelta(days=1),  # Chạy hàng ngày
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['scraping', 'oer', 'openstax'],
)

DATA_PATH = '/opt/airflow/scraped_data'

def setup_directories():
    """Tạo các thư mục cần thiết"""
    os.makedirs(DATA_PATH, exist_ok=True)
    print(f"Đã tạo thư mục: {DATA_PATH}")

def scrape_openstax_documents(**context):
    """Task cào documents từ OpenStax"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"Bắt đầu cào dữ liệu cho ngày: {execution_date}")
    
    # Khởi tạo scraper
    scraper = AdvancedOERScraper(delay=1.0, use_selenium=True)
    
    try:
        # Cào dữ liệu
        if scraper.use_selenium:
            documents = scraper.scrape_openstax_with_selenium()
        else:
            documents = scraper.scrape_openstax_fallback()
        
        # Upload raw JSONL to MinIO
        object_key = scraper.upload_raw_records_to_minio(documents, 'openstax', execution_date)
        print(f"[OpenStax] MinIO object: {object_key}")
        return {
            'execution_date': execution_date,
            'total_found': len(documents),
            'minio_object_key': object_key
        }
        
    except Exception as e:
        print(f"Lỗi khi cào dữ liệu: {e}")
        raise
        
    finally:
        scraper.cleanup()


def cleanup_old_files(**context):
    """Dọn dẹp các file cũ (giữ lại 30 ngày gần nhất)"""
    execution_date = context['execution_date']
    cutoff_date = execution_date - timedelta(days=30)
    
    removed_files = 0
    
    for filename in os.listdir(DATA_PATH):
        if filename.startswith('openstax_documents_') and filename.endswith('.json'):
            # Trích xuất ngày từ tên file
            try:
                date_str = filename.replace('openstax_documents_', '').replace('.json', '')
                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                
                if file_date < cutoff_date:
                    file_path = os.path.join(DATA_PATH, filename)
                    os.remove(file_path)
                    removed_files += 1
                    print(f"Đã xóa file cũ: {filename}")
                    
            except ValueError:
                continue
    
    print(f"Đã dọn dẹp {removed_files} file cũ")

# Định nghĩa các tasks
setup_task = PythonOperator(
    task_id='setup_directories',
    python_callable=setup_directories,
    dag=dag,
)

scrape_task = PythonOperator(
    task_id='scrape_openstax_documents',
    python_callable=scrape_openstax_documents,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files,
    dag=dag,
)

# Health check
health_check_task = BashOperator(
    task_id='health_check',
    bash_command='echo "Scraping pipeline completed successfully at $(date)"',
    dag=dag,
)

# Thiết lập dependencies
setup_task >> scrape_task >> cleanup_task >> health_check_task
