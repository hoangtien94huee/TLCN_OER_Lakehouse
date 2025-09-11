from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os
from openstax_scraper import OpenStaxScraper

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
    """Task cào documents từ OpenStax với MinIO backup support"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OpenStax] Bắt đầu cào dữ liệu cho ngày: {execution_date}")
    
    # Khởi tạo scraper với MinIO support
    openstax = OpenStaxScraper(delay=1.0, use_selenium=True)
    
    try:
        # Cào dữ liệu với error handling và auto backup
        if openstax.use_selenium:
            documents = openstax.scrape_openstax_with_selenium()
        else:
            documents = openstax.scrape_openstax_fallback()
        
        # Upload raw JSONL to MinIO (final backup)
        object_key = ""
        if documents:
            object_key = openstax.save_to_minio(documents, 'openstax', execution_date, 'books_batch')
            
        print(f"[OpenStax] MinIO object: {object_key}")
        print(f"[OpenStax] Tổng cộng cào được: {len(documents)} documents")
        
        return {
            'execution_date': execution_date,
            'total_found': len(documents),
            'minio_object_key': object_key
        }
        
    except KeyboardInterrupt:
        print("[OpenStax] Task bị ngắt bởi người dùng")
        # Emergency backup đã được thực hiện trong scraper
        raise
        
    except Exception as e:
        print(f"[OpenStax] Lỗi khi cào dữ liệu: {e}")
        # Emergency backup đã được thực hiện trong scraper
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
    """Kiểm tra và liệt kê các backup có sẵn trong MinIO"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OpenStax] Kiểm tra MinIO backups cho ngày: {execution_date}")
    
    # Khởi tạo scraper để sử dụng MinIO functions
    openstax = OpenStaxScraper(delay=0, use_selenium=False)
    
    try:
        # Liệt kê các backup có sẵn
        backups = openstax.list_minio_backups("openstax")
        
        # Đếm số backup
        emergency_backups = [b for b in backups if b['is_emergency']]
        normal_backups = [b for b in backups if not b['is_emergency']]
        
        print(f"[OpenStax] Tổng cộng: {len(backups)} backup files")
        print(f"[OpenStax] Emergency backups: {len(emergency_backups)}")
        print(f"[OpenStax] Normal backups: {len(normal_backups)}")
        
        return {
            'total_backups': len(backups),
            'emergency_backups': len(emergency_backups),
            'normal_backups': len(normal_backups),
            'latest_backup': backups[0]['path'] if backups else None
        }
        
    except Exception as e:
        print(f"[OpenStax] Lỗi khi kiểm tra MinIO backups: {e}")
        return {
            'total_backups': 0,
            'emergency_backups': 0,
            'normal_backups': 0,
            'latest_backup': None
        }
    
    finally:
        openstax.cleanup()

def emergency_recovery(**context):
    """Task khôi phục dữ liệu từ emergency backup (chỉ chạy khi cần)"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OpenStax] Bắt đầu emergency recovery cho ngày: {execution_date}")
    
    # Khởi tạo scraper
    openstax = OpenStaxScraper(delay=0, use_selenium=False)
    
    try:
        # Liệt kê các emergency backup
        backups = openstax.list_minio_backups("openstax")
        emergency_backups = [b for b in backups if b['is_emergency']]
        
        if not emergency_backups:
            print("[OpenStax] Không tìm thấy emergency backup nào")
            return {'recovered_documents': 0, 'backup_used': None}
        
        # Sử dụng emergency backup gần nhất
        latest_emergency = emergency_backups[0]
        print(f"[OpenStax] Khôi phục từ: {latest_emergency['path']}")
        
        # Khôi phục dữ liệu
        recovered_docs = openstax.restore_from_minio(latest_emergency['path'])
        
        if recovered_docs:
            # Lưu dữ liệu đã khôi phục
            openstax.save_to_json(recovered_docs, f"openstax_recovered_{execution_date}.json")
            
            # Tạo backup chính thức từ dữ liệu đã khôi phục
            final_backup = openstax.save_to_minio(recovered_docs, 'openstax_recovered', execution_date, 'books_recovered')
            
            print(f"[OpenStax] Đã khôi phục {len(recovered_docs)} documents")
            print(f"[OpenStax] Đã tạo final backup: {final_backup}")
            
            return {
                'recovered_documents': len(recovered_docs),
                'backup_used': latest_emergency['path'],
                'final_backup': final_backup
            }
        else:
            print("[OpenStax] Không thể khôi phục dữ liệu từ backup")
            return {'recovered_documents': 0, 'backup_used': latest_emergency['path']}
            
    except Exception as e:
        print(f"[OpenStax] Lỗi khi thực hiện emergency recovery: {e}")
        return {'recovered_documents': 0, 'backup_used': None, 'error': str(e)}
        
    finally:
        openstax.cleanup()

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

# Health check
health_check_task = BashOperator(
    task_id='health_check',
    bash_command='echo "OpenStax scraping pipeline completed successfully at $(date)"',
    dag=dag,
)

# Thiết lập dependencies - thêm minio_check_task vào pipeline
# emergency_recovery_task là independent task, có thể chạy manual
setup_task >> scrape_task >> [cleanup_task, minio_check_task] >> health_check_task

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
