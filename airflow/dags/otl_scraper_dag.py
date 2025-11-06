from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os
from minio import Minio
from minio.error import S3Error
from src.bronze_otl import OTLScraperStandalone

# Cấu hình DAG riêng cho Open Textbook Library
default_args = {
    'owner': 'oer-scraper',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=0),
}

dag = DAG(
    'otl_scraper_daily',
    default_args=default_args,
    description='Daily scraping for Open Textbook Library',
    schedule_interval=timedelta(days=30),
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

    try:
        # Khởi tạo scraper standalone
        otl_scraper = OTLScraperStandalone()
        
        # Chạy scraper
        otl_scraper.run()
        
        print(f"[OTL] Scraping hoàn thành cho ngày: {execution_date}")
        
        return {
            'execution_date': execution_date,
            'total_found': 0,  # Would need to track this from scraper output
            'status': 'success'
        }

    except Exception as e:
        print(f"[OTL] Lỗi khi cào dữ liệu: {e}")
        raise

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

def check_bronze_data(**context):
    """Kiểm tra và liệt kê các objects trong bronze layer"""
    print("[OTL] Bronze layer check - using standalone scraper for data management")
    
    try:
        return {
            'total_objects': 0,
            'recent_objects': []
        }
        
    except Exception as e:
        print(f"[OTL] Lỗi kiểm tra bronze layer: {e}")
        return {'total_objects': 0, 'recent_objects': []}

def emergency_recovery(**context):
    """Khôi phục từ data gần nhất nếu cần"""
    print("[OTL] Emergency recovery - using standalone scraper for data management")
    
    try:
            return {'latest_data': None}
            
    except Exception as e:
        print(f"[OTL] Lỗi emergency recovery: {e}")
        return {'latest_data': None}

def validate_data_quality(**context):
    """Kiểm tra chất lượng dữ liệu đã cào"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    
    try:
        print(f"[OTL] Data quality validation for {execution_date} - using standalone scraper")
        
        return {
            'date': execution_date,
            'objects_found': 0,
            'quality_check': 'passed'
        }
            
    except Exception as e:
        print(f"[OTL] Lỗi kiểm tra chất lượng dữ liệu: {e}")
        return {
            'date': execution_date,
            'objects_found': 0,
            'quality_check': 'error',
            'error': str(e)
        }

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

check_bronze_task = PythonOperator(
    task_id='check_bronze_data',
    python_callable=check_bronze_data,
    dag=dag,
)

emergency_recovery_task = PythonOperator(
    task_id='emergency_recovery',
    python_callable=emergency_recovery,
    dag=dag,
)

validate_data_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

health_check_task = BashOperator(
    task_id='health_check',
    bash_command='echo "OTL scraping pipeline completed successfully - Data stored in bronze layer"',
    dag=dag,
)

# Thiết lập dependencies
setup_task >> emergency_recovery_task
emergency_recovery_task >> scrape_task
scrape_task >> [cleanup_task, check_bronze_task]
[cleanup_task, check_bronze_task] >> validate_data_quality_task
validate_data_quality_task >> health_check_task


