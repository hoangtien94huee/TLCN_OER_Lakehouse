from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os
from src.scrapers import OpenStaxScraper
from src.utils.bronze_storage import BronzeStorageManager

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
    'openstax_scraper_daily',
    default_args=default_args,
    description='Daily OpenStax scraping with MinIO backup support',
    schedule_interval=timedelta(days=30),  # Chạy hàng tháng
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['scraping', 'oer', 'openstax'],
)

DATA_PATH = '/opt/airflow/scraped_data'

def setup_directories():
    """Tạo các thư mục cần thiết"""
    os.makedirs(DATA_PATH, exist_ok=True)
    print(f"[OpenStax] Đã tạo thư mục: {DATA_PATH}")

def scrape_openstax_documents(**context):
    """Task cào documents từ OpenStax với bronze layer storage"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OpenStax] Bắt đầu cào dữ liệu cho ngày: {execution_date}")
    
    # Khởi tạo Bronze Storage Manager
    bronze_storage = BronzeStorageManager()
    
    # Khởi tạo scraper với MinIO support
    openstax = OpenStaxScraper(delay=1.0, use_selenium=True)
    
    try:
        # Cào dữ liệu với error handling
        if openstax.use_selenium:
            documents = openstax.scrape_openstax_with_selenium()
        else:
            documents = openstax.scrape_openstax_fallback()
        
        if documents:
            # Lưu vào bronze layer với cấu trúc chuẩn
            result = bronze_storage.save_scraped_data(
                documents=documents,
                source='openstax',
                execution_date=execution_date,
                file_type='books',
                metadata={
                    'scraper_config': {
                        'delay': 1.0,
                        'use_selenium': True
                    },
                    'total_processed': len(documents)
                }
            )
            
            print(f"[OpenStax] Đã lưu vào bronze layer:")
            print(f"  - Data: {result.get('data_object_key')}")
            print(f"  - Metadata: {result.get('metadata_object_key')}")
            print(f"[OpenStax] Tổng cộng cào được: {len(documents)} documents")
            
            return {
                'execution_date': execution_date,
                'total_found': len(documents),
                'bronze_data_key': result.get('data_object_key'),
                'bronze_metadata_key': result.get('metadata_object_key')
            }
        else:
            print(f"[OpenStax] Không có dữ liệu để lưu")
            return {
                'execution_date': execution_date,
                'total_found': 0,
                'bronze_data_key': None,
                'bronze_metadata_key': None
            }
        
    except KeyboardInterrupt:
        print("[OpenStax] Task bị ngắt bởi người dùng")
        raise
        
    except Exception as e:
        print(f"[OpenStax] Lỗi khi cào dữ liệu: {e}")
        raise
        
    finally:
        openstax.cleanup()


def cleanup_old_files(**context):
    """Dọn dẹp các file cũ (giữ lại 30 ngày gần nhất)"""
    logical_date = context.get('logical_date') or context.get('execution_date')
    cutoff_date = (logical_date.date() - timedelta(days=30))
    removed_files = 0
    
    try:
        entries = os.listdir(DATA_PATH)
    except FileNotFoundError:
        print(f"[OpenStax] Thư mục không tồn tại: {DATA_PATH}")
        return
    
    for filename in entries:
        if filename.startswith('openstax_') and filename.endswith('.json'):
            # Trích xuất ngày từ tên file
            try:
                # Hỗ trợ nhiều format tên file khác nhau
                date_str = None
                if 'documents_' in filename:
                    date_str = filename.split('documents_')[1].replace('.json', '')
                elif '_partial_' in filename:
                    date_str = filename.split('_partial_')[0].replace('openstax_', '')
                elif '_error_backup' in filename:
                    date_str = filename.split('_error_backup')[0].replace('openstax_', '')
                    
                if date_str:
                    file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    
                    if file_date < cutoff_date:
                        file_path = os.path.join(DATA_PATH, filename)
                        try:
                            os.remove(file_path)
                            removed_files += 1
                            print(f"[OpenStax] Đã xóa file cũ: {filename}")
                        except OSError as e:
                            print(f"[OpenStax] Không thể xóa {filename}: {e}")
                            
            except ValueError:
                # Tên file không đúng định dạng ngày, bỏ qua
                continue
    
    print(f"[OpenStax] Đã dọn dẹp {removed_files} file cũ")

def check_minio_backups(**context):
    """Kiểm tra và liệt kê các objects trong bronze layer"""
    bronze_storage = BronzeStorageManager()
    
    try:
        bronze_objects = bronze_storage.list_bronze_data(source='openstax')
        print(f"[OpenStax] Tìm thấy {len(bronze_objects)} files trong bronze layer:")
        
        # Hiển thị 5 files mới nhất
        recent_objects = sorted(bronze_objects, key=lambda x: x['last_modified'], reverse=True)[:5]
        for obj in recent_objects:
            print(f"  - {obj['object_name']} ({obj['size']} bytes)")
            
        return {
            'total_objects': len(bronze_objects),
            'recent_objects': [obj['object_name'] for obj in recent_objects]
        }
        
    except Exception as e:
        print(f"[OpenStax] Lỗi kiểm tra bronze layer: {e}")
        return {'total_objects': 0, 'recent_objects': []}

def emergency_recovery(**context):
    """Khôi phục từ data gần nhất nếu cần"""
    bronze_storage = BronzeStorageManager()
    
    try:
        latest_data = bronze_storage.get_latest_data('openstax')
        
        if latest_data:
            print(f"[OpenStax] Data gần nhất trong bronze layer: {latest_data['object_name']}")
            print(f"[OpenStax] Emergency recovery system đã sẵn sàng")
            return {'latest_data': latest_data['object_name']}
        else:
            print("[OpenStax] Không tìm thấy data nào trong bronze layer")
            return {'latest_data': None}
            
    except Exception as e:
        print(f"[OpenStax] Lỗi emergency recovery: {e}")
        return {'latest_data': None}

def validate_data_quality(**context):
    """Kiểm tra chất lượng dữ liệu đã cào"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    bronze_storage = BronzeStorageManager()
    
    try:
        # Kiểm tra bronze layer data
        bronze_objects = bronze_storage.list_bronze_data(
            source='openstax',
            start_date=execution_date,
            end_date=execution_date
        )
        
        if bronze_objects:
            print(f"[OpenStax] Tìm thấy {len(bronze_objects)} objects cho ngày {execution_date}")
            
            # Kiểm tra chất lượng dữ liệu cơ bản
            data_files = [obj for obj in bronze_objects if 'books_' in obj['object_name']]
            metadata_files = [obj for obj in bronze_objects if 'metadata_' in obj['object_name']]
            
            quality_score = 100
            if not data_files:
                quality_score -= 50
                print("[OpenStax] Warning: Không tìm thấy data files")
            if not metadata_files:
                quality_score -= 20
                print("[OpenStax] Warning: Không tìm thấy metadata files")
            
            return {
                'date': execution_date,
                'objects_found': len(bronze_objects),
                'data_files': len(data_files),
                'metadata_files': len(metadata_files),
                'quality_score': quality_score,
                'quality_check': 'passed' if quality_score >= 80 else 'warning' if quality_score >= 50 else 'failed'
            }
        else:
            print(f"[OpenStax] Không tìm thấy objects nào cho ngày {execution_date}")
            return {
                'date': execution_date,
                'objects_found': 0,
                'quality_check': 'failed'
            }
            
    except Exception as e:
        print(f"[OpenStax] Lỗi kiểm tra chất lượng dữ liệu: {e}")
        return {
            'date': execution_date,
            'objects_found': 0,
            'quality_check': 'error',
            'error': str(e)
        }

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

# Task kiểm tra MinIO backups
minio_check_task = PythonOperator(
    task_id='check_minio_backups',
    python_callable=check_minio_backups,
    dag=dag,
)

# Task emergency recovery (chạy manual khi cần)
emergency_recovery_task = PythonOperator(
    task_id='emergency_recovery',
    python_callable=emergency_recovery,
    dag=dag,
    # Task này sẽ được trigger manual khi cần recovery
    trigger_rule='none_failed_or_skipped',
)

# Task validate data quality
validate_data_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# Health check
health_check_task = BashOperator(
    task_id='health_check',
    bash_command='echo "OpenStax scraping pipeline completed successfully - Data stored in bronze layer"',
    dag=dag,
)

# Thiết lập dependencies với validate data quality
setup_task >> emergency_recovery_task
emergency_recovery_task >> scrape_task
scrape_task >> [cleanup_task, minio_check_task]
[cleanup_task, minio_check_task] >> validate_data_quality_task
validate_data_quality_task >> health_check_task

# Notes về sử dụng DAG:
# 1. Task bình thường: setup_task -> scrape_task -> cleanup_task + minio_check_task -> health_check_task
# 2. Emergency recovery: Chạy manual task 'emergency_recovery' khi cần khôi phục từ backup
# 3. MinIO Environment Variables cần thiết:
#    - MINIO_ENABLE=1
#    - MINIO_ENDPOINT=minio:9000  
#    - MINIO_ACCESS_KEY=minioadmin
#    - MINIO_SECRET_KEY=minioadmin
#    - MINIO_BUCKET=oer-raw
# 4. Auto backup sẽ được tạo khi:
#    - Task bị lỗi
#    - Task bị ngắt (KeyboardInterrupt)
#    - Task hoàn thành thành công
