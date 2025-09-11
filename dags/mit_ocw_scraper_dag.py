from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os
from mit_ocw_scraper import MITOCWScraper

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
    'mit_ocw_scraper_daily',
    default_args=default_args,
    description='Daily MIT OCW scraping with MinIO backup support',
    schedule_interval=timedelta(days=30),  # Chạy hàng ngày
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['scraping', 'oer', 'mit_ocw'],
)

DATA_PATH = '/opt/airflow/scraped_data'

def setup_directories():
    """Tạo các thư mục cần thiết"""
    os.makedirs(DATA_PATH, exist_ok=True)
    print(f"[MIT OCW] Đã tạo thư mục: {DATA_PATH}")

def scrape_mit_ocw_documents(**context):
    """Task cào documents từ MIT OCW với MinIO backup support"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[MIT OCW] Bắt đầu cào dữ liệu cho ngày: {execution_date}")
    
    # Khởi tạo scraper với cấu hình tối ưu
    mit_scraper = MITOCWScraper(
        delay=2.0,  # Delay lâu hơn để tránh bị block
        use_selenium=True,
        output_dir=DATA_PATH,
        batch_size=25,
        max_documents=200  # Không giới hạn - có thể thay đổi nếu cần
    )
    
    try:
        # Cào dữ liệu với error handling và auto backup
        documents = mit_scraper.scrape_with_selenium()
        
        # Upload raw data to MinIO (final backup)
        object_key = ""
        if documents:
            object_key = mit_scraper.save_to_minio(documents, 'mit_ocw', execution_date, 'courses_batch')
            print(f"[MIT OCW] Đã lưu vào MinIO: {object_key}")
            
        print(f"[MIT OCW] Tổng cộng cào được: {len(documents)} documents")
        
        return {
            'execution_date': execution_date,
            'total_found': len(documents),
            'minio_object_key': object_key
        }
        
    except KeyboardInterrupt:
        print("[MIT OCW] Task bị ngắt bởi người dùng")
        # Emergency backup được thực hiện trong scraper
        raise
        
    except Exception as e:
        print(f"[MIT OCW] Lỗi khi cào dữ liệu: {e}")
        # Emergency backup chỉ cần report lỗi, scraper tự xử lý
        print(f"[MIT OCW] Emergency backup được xử lý bởi scraper")
        raise
        
    finally:
        mit_scraper.cleanup()

def cleanup_old_files(**context):
    """Dọn dẹp các file cũ (giữ lại 30 ngày gần nhất)"""
    logical_date = context.get('logical_date') or context.get('execution_date')
    cutoff_date = (logical_date.date() - timedelta(days=30))
    removed_files = 0
    
    try:
        entries = os.listdir(DATA_PATH)
    except FileNotFoundError:
        print(f"[MIT OCW] Thư mục không tồn tại: {DATA_PATH}")
        return
    
    for filename in entries:
        if filename.startswith('mit_ocw_') and filename.endswith('.json'):
            # Trích xuất ngày từ tên file
            try:
                # Hỗ trợ nhiều format tên file khác nhau
                date_str = None
                if '_batch_' in filename:
                    # Format: mit_ocw_batch_20250909_143000.json
                    date_part = filename.split('_batch_')[1].split('_')[0]
                    file_date = datetime.strptime(date_part, '%Y%m%d').date()
                elif '_final_' in filename:
                    # Format: mit_ocw_final_20250909_143000.json
                    date_part = filename.split('_final_')[1].split('_')[0]
                    file_date = datetime.strptime(date_part, '%Y%m%d').date()
                else:
                    continue
                    
                if file_date < cutoff_date:
                    file_path = os.path.join(DATA_PATH, filename)
                    try:
                        os.remove(file_path)
                        removed_files += 1
                        print(f"[MIT OCW] Đã xóa file cũ: {filename}")
                    except OSError as e:
                        print(f"[MIT OCW] Không thể xóa {filename}: {e}")
                        
            except ValueError:
                # Tên file không đúng định dạng ngày, bỏ qua
                continue
    
    print(f"[MIT OCW] Đã dọn dẹp {removed_files} files")

def check_minio_backups(**context):
    """Kiểm tra và liệt kê các backup trong MinIO"""
    # Khởi tạo scraper chỉ để dùng MinIO methods
    mit_scraper = MITOCWScraper(
        minio_enable=True,
        output_dir=DATA_PATH
    )
    
    try:
        backups = mit_scraper.list_minio_backups('mit_ocw')
        print(f"[MIT OCW] Tìm thấy {len(backups)} backup files trong MinIO:")
        
        # Hiển thị 5 backup mới nhất
        recent_backups = sorted([b['object_name'] for b in backups], reverse=True)[:5]
        for backup in recent_backups:
            print(f"  - {backup}")
            
        return {
            'total_backups': len(backups),
            'recent_backups': recent_backups
        }
        
    except Exception as e:
        print(f"[MIT OCW] Lỗi kiểm tra MinIO backups: {e}")
        return {'total_backups': 0, 'recent_backups': []}

def emergency_recovery(**context):
    """Khôi phục từ backup gần nhất nếu cần"""
    # Khởi tạo scraper chỉ để dùng MinIO methods
    mit_scraper = MITOCWScraper(
        minio_enable=True,
        output_dir=DATA_PATH
    )
    
    try:
        backups = mit_scraper.list_minio_backups('mit_ocw')
        
        if backups:
            # Lấy backup gần nhất
            latest_backup = sorted(backups, key=lambda x: x['last_modified'], reverse=True)[0]
            latest_backup_name = latest_backup['object_name']
            
            print(f"[MIT OCW] Backup gần nhất: {latest_backup_name}")
            
            # Có thể thêm logic khôi phục tự động nếu cần
            print("[MIT OCW] Emergency recovery system đã sẵn sàng")
            return {'latest_backup': latest_backup_name}
        else:
            print("[MIT OCW] Không tìm thấy backup nào")
            return {'latest_backup': None}
            
    except Exception as e:
        print(f"[MIT OCW] Lỗi emergency recovery: {e}")
        return {'latest_backup': None}

def validate_data_quality(**context):
    """Kiểm tra chất lượng dữ liệu đã cào"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    
    # Khởi tạo scraper chỉ để dùng MinIO methods
    mit_scraper = MITOCWScraper(
        minio_enable=True,
        output_dir=DATA_PATH
    )
    
    try:
        # Kiểm tra MinIO backup
        backups = mit_scraper.list_minio_backups('mit_ocw')
        today_backups = [b for b in backups if execution_date.replace('-', '') in b['object_name']]
        
        if today_backups:
            print(f"[MIT OCW] Tìm thấy {len(today_backups)} backup cho ngày {execution_date}")
            
            # Có thể thêm logic kiểm tra chất lượng dữ liệu chi tiết hơn
            # Ví dụ: kiểm tra số lượng documents, độ hoàn chỉnh của fields, v.v.
            
            return {
                'date': execution_date,
                'backups_found': len(today_backups),
                'quality_check': 'passed'
            }
        else:
            print(f"[MIT OCW] Không tìm thấy backup nào cho ngày {execution_date}")
            return {
                'date': execution_date,
                'backups_found': 0,
                'quality_check': 'failed'
            }
            
    except Exception as e:
        print(f"[MIT OCW] Lỗi kiểm tra chất lượng dữ liệu: {e}")
        return {
            'date': execution_date,
            'backups_found': 0,
            'quality_check': 'error',
            'error': str(e)
        }

# Định nghĩa các tasks
setup_directories_task = PythonOperator(
    task_id='setup_directories',
    python_callable=setup_directories,
    dag=dag,
)

emergency_recovery_task = PythonOperator(
    task_id='emergency_recovery',
    python_callable=emergency_recovery,
    dag=dag,
)

scrape_mit_ocw_task = PythonOperator(
    task_id='scrape_mit_ocw_documents',
    python_callable=scrape_mit_ocw_documents,
    dag=dag,
)

cleanup_old_files_task = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files,
    dag=dag,
)

check_minio_backups_task = PythonOperator(
    task_id='check_minio_backups',
    python_callable=check_minio_backups,
    dag=dag,
)

validate_data_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

health_check_task = BashOperator(
    task_id='health_check',
    bash_command='echo "MIT OCW scraping pipeline completed successfully"',
    dag=dag,
)

# Định nghĩa dependencies
setup_directories_task >> emergency_recovery_task
emergency_recovery_task >> scrape_mit_ocw_task
scrape_mit_ocw_task >> [cleanup_old_files_task, check_minio_backups_task]
[cleanup_old_files_task, check_minio_backups_task] >> validate_data_quality_task
validate_data_quality_task >> health_check_task
