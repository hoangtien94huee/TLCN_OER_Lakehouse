from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os
from src.scrapers import MITOCWScraper
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
    """Task cào documents từ MIT OCW với bronze layer storage"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[MIT OCW] Bắt đầu cào dữ liệu cho ngày: {execution_date}")
    
    # Khởi tạo Bronze Storage Manager
    bronze_storage = BronzeStorageManager()
    
    # Khởi tạo scraper với cấu hình tối ưu
    mit_scraper = MITOCWScraper(
        delay=2.0,  # Delay lâu hơn để tránh bị block
        use_selenium=True,
        output_dir=DATA_PATH,
        batch_size=25,
        max_documents=100  # Không giới hạn - có thể thay đổi nếu cần
    )
    
    try:
        # Cào dữ liệu với error handling
        documents = mit_scraper.scrape_with_selenium()
        
        if documents:
            # Lưu vào bronze layer với cấu trúc chuẩn
            result = bronze_storage.save_scraped_data(
                documents=documents,
                source='mit_ocw',
                execution_date=execution_date,
                file_type='courses',
                metadata={
                    'scraper_config': {
                        'delay': 2.0,
                        'use_selenium': True,
                        'batch_size': 25,
                        'max_documents': 200
                    },
                    'total_processed': len(documents)
                }
            )
            
            print(f"[MIT OCW] Đã lưu vào bronze layer:")
            print(f"  - Data: {result.get('data_object_key')}")
            print(f"  - Metadata: {result.get('metadata_object_key')}")
            print(f"[MIT OCW] Tổng cộng cào được: {len(documents)} documents")
            
            return {
                'execution_date': execution_date,
                'total_found': len(documents),
                'bronze_data_key': result.get('data_object_key'),
                'bronze_metadata_key': result.get('metadata_object_key')
            }
        else:
            print(f"[MIT OCW] Không có dữ liệu để lưu")
            return {
                'execution_date': execution_date,
                'total_found': 0,
                'bronze_data_key': None,
                'bronze_metadata_key': None
            }
        
    except KeyboardInterrupt:
        print("[MIT OCW] Task bị ngắt bởi người dùng")
        raise
        
    except Exception as e:
        print(f"[MIT OCW] Lỗi khi cào dữ liệu: {e}")
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
    """Kiểm tra và liệt kê các backup trong bronze layer"""
    bronze_storage = BronzeStorageManager()
    
    try:
        bronze_objects = bronze_storage.list_bronze_data(source='mit_ocw')
        print(f"[MIT OCW] Tìm thấy {len(bronze_objects)} files trong bronze layer:")
        
        # Hiển thị 5 files mới nhất
        recent_objects = sorted(bronze_objects, key=lambda x: x['last_modified'], reverse=True)[:5]
        for obj in recent_objects:
            print(f"  - {obj['object_name']} ({obj['size']} bytes)")
            
        return {
            'total_objects': len(bronze_objects),
            'recent_objects': [obj['object_name'] for obj in recent_objects]
        }
        
    except Exception as e:
        print(f"[MIT OCW] Lỗi kiểm tra bronze layer: {e}")
        return {'total_objects': 0, 'recent_objects': []}
        print(f"[MIT OCW] Lỗi kiểm tra MinIO backups: {e}")
        return {'total_backups': 0, 'recent_backups': []}

def emergency_recovery(**context):
    """Khôi phục từ backup gần nhất nếu cần"""
    bronze_storage = BronzeStorageManager()
    
    try:
        latest_data = bronze_storage.get_latest_data('mit_ocw')
        
        if latest_data:
            print(f"[MIT OCW] Data gần nhất trong bronze layer: {latest_data['object_name']}")
            print(f"[MIT OCW] Emergency recovery system đã sẵn sàng")
            return {'latest_data': latest_data['object_name']}
        else:
            print("[MIT OCW] Không tìm thấy data nào trong bronze layer")
            return {'latest_data': None}
            
    except Exception as e:
        print(f"[MIT OCW] Lỗi emergency recovery: {e}")
        return {'latest_data': None}

def validate_data_quality(**context):
    """Kiểm tra chất lượng dữ liệu đã cào"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    bronze_storage = BronzeStorageManager()
    
    try:
        # Kiểm tra bronze layer data
        bronze_objects = bronze_storage.list_bronze_data(
            source='mit_ocw',
            start_date=execution_date,
            end_date=execution_date
        )
        
        if bronze_objects:
            print(f"[MIT OCW] Tìm thấy {len(bronze_objects)} objects cho ngày {execution_date}")
            
            # Kiểm tra chất lượng dữ liệu cơ bản
            data_files = [obj for obj in bronze_objects if 'courses_' in obj['object_name']]
            metadata_files = [obj for obj in bronze_objects if 'metadata_' in obj['object_name']]
            
            quality_score = 100
            if not data_files:
                quality_score -= 50
                print("[MIT OCW] Warning: Không tìm thấy data files")
            if not metadata_files:
                quality_score -= 20
                print("[MIT OCW] Warning: Không tìm thấy metadata files")
            
            return {
                'date': execution_date,
                'objects_found': len(bronze_objects),
                'data_files': len(data_files),
                'metadata_files': len(metadata_files),
                'quality_score': quality_score,
                'quality_check': 'passed' if quality_score >= 80 else 'warning' if quality_score >= 50 else 'failed'
            }
        else:
            print(f"[MIT OCW] Không tìm thấy objects nào cho ngày {execution_date}")
            return {
                'date': execution_date,
                'objects_found': 0,
                'quality_check': 'failed'
            }
            
    except Exception as e:
        print(f"[MIT OCW] Lỗi kiểm tra chất lượng dữ liệu: {e}")
        return {
            'date': execution_date,
            'objects_found': 0,
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
    bash_command='echo "MIT OCW scraping pipeline completed successfully - Data stored in bronze layer"',
    dag=dag,
)

# Định nghĩa dependencies
setup_directories_task >> emergency_recovery_task
emergency_recovery_task >> scrape_mit_ocw_task
scrape_mit_ocw_task >> [cleanup_old_files_task, check_minio_backups_task]
[cleanup_old_files_task, check_minio_backups_task] >> validate_data_quality_task
validate_data_quality_task >> health_check_task
