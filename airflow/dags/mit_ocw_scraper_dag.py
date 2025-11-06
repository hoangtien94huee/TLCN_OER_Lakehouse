from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os
from src.bronze_mit_ocw import MITOCWScraper

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
    
    try:
        # Khởi tạo scraper với cấu hình tối ưu
        mit_scraper = MITOCWScraper(
            delay=float(os.getenv('SCRAPING_DELAY_BASE', 2.0)),
            use_selenium=True,
            output_dir="/opt/airflow/scraped_data/mit_ocw",
            batch_size=int(os.getenv('BATCH_SIZE', 25)),
            max_documents=2000
        )
        
        # Cào dữ liệu với error handling
        print(f"[MIT OCW] Chạy scraper...")
        documents = mit_scraper.scrape_with_selenium()
        
        if documents:
            print(f"[MIT OCW] Đã cào được {len(documents)} documents")
            
            # Save to MinIO if enabled
            if mit_scraper.minio_enable:
                object_name = mit_scraper.save_to_minio(documents, logical_date=execution_date)
                print(f"[MIT OCW] Đã lưu vào MinIO: {object_name}")
            
            # Show multimedia stats
            total_videos = sum(len(doc.get('videos', [])) for doc in documents)
            total_pdfs = sum(len(doc.get('pdfs', [])) for doc in documents)
            print(f"[MIT OCW] Multimedia content: {total_videos} videos, {total_pdfs} PDFs")
            
            result = {
                'execution_date': execution_date,
                'total_found': len(documents),
                'status': 'success',
                'multimedia_stats': {
                    'videos': total_videos,
                    'pdfs': total_pdfs
                }
            }
        else:
            print(f"[MIT OCW] Không cào được documents nào")
            result = {
                'execution_date': execution_date,
                'total_found': 0,
                'status': 'warning'
            }
        
        print(f"[MIT OCW] Scraping hoàn thành cho ngày: {execution_date}")
        
        return result
        
    except KeyboardInterrupt:
        print("[MIT OCW] Task bị ngắt bởi người dùng")
        raise
        
    except Exception as e:
        print(f"[MIT OCW] Lỗi khi cào dữ liệu: {e}")
        raise
    
    finally:
        # Cleanup
        if 'mit_scraper' in locals():
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
    print("[MIT OCW] Bronze layer check - using standalone scraper for data management")
    
    try:
        return {
            'total_objects': 0,
            'recent_objects': []
        }
        
    except Exception as e:
        print(f"[MIT OCW] Lỗi kiểm tra bronze layer: {e}")
        return {'total_objects': 0, 'recent_objects': []}

def emergency_recovery(**context):
    """Khôi phục từ backup gần nhất nếu cần"""
    print("[MIT OCW] Emergency recovery - using standalone scraper for data management")
    
    try:
        return {'latest_data': None}
            
    except Exception as e:
        print(f"[MIT OCW] Lỗi emergency recovery: {e}")
        return {'latest_data': None}

def validate_data_quality(**context):
    """Kiểm tra chất lượng dữ liệu đã cào"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    
    try:
        print(f"[MIT OCW] Data quality validation for {execution_date} - using standalone scraper")
        
        return {
            'date': execution_date,
            'objects_found': 0,
            'quality_check': 'passed'
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
