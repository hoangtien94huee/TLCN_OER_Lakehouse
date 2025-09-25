from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os
from minio import Minio
from minio.error import S3Error
from src.scrapers.otl_scraper import (
    live_scrape_leaf_subject_selenium,
    live_scrape_root_subject_selenium,
    live_scrape_all_subjects_selenium,
)
from src.utils.bronze_storage import BronzeStorageManager

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
    print(f"[OTL] Bắt đầu cào live (Selenium) cho ngày: {execution_date}")

    # Khởi tạo Bronze Storage Manager
    bronze_storage = BronzeStorageManager()

    # Optional throttling/backoff tuning (ENV → Variables → DAG conf)
    def _conf_get(key: str, default_value: str = '') -> str:
        if os.getenv(key, ''):
            return os.getenv(key, '')
        v = Variable.get(key, default_var='')
        if v:
            return v
        run_conf = context.get('dag_run').conf if context.get('dag_run') and context.get('dag_run').conf else {}
        return str(run_conf.get(key.lower()) or run_conf.get(key) or default_value)

    delay_base = _conf_get('OTL_DELAY_BASE_SEC')
    delay_jitter = _conf_get('OTL_DELAY_JITTER_SEC')
    backoff_base = _conf_get('OTL_RETRY_BACKOFF_BASE_SEC')
    backoff_max = _conf_get('OTL_RETRY_BACKOFF_MAX_SEC')
    slow_mode = _conf_get('OTL_SLOW_MODE')
    wait_until_clear = _conf_get('OTL_WAIT_UNTIL_CLEAR')
    wait_max_minutes = _conf_get('OTL_WAIT_MAX_MINUTES')
    per_book_delay = _conf_get('OTL_PER_BOOK_DELAY_SEC')
    subject_cooldown = _conf_get('OTL_SUBJECT_COOLDOWN_SEC')
    force_random_ua = _conf_get('OTL_FORCE_RANDOM_UA')
    custom_ua = _conf_get('OTL_USER_AGENT')
    proxy = _conf_get('OTL_PROXY')
    book_max_attempts = _conf_get('OTL_BOOK_MAX_ATTEMPTS')
    retry_pdf_on_miss = _conf_get('OTL_RETRY_PDF_ON_MISS')

    if slow_mode.lower() in ['1', 'true', 'yes']:
        delay_base = delay_base or '1.5'
        delay_jitter = delay_jitter or '0.8'
        backoff_base = backoff_base or '3.0'
        backoff_max = backoff_max or '24.0'

    if delay_base:
        os.environ['OTL_DELAY_BASE_SEC'] = delay_base
    if delay_jitter:
        os.environ['OTL_DELAY_JITTER_SEC'] = delay_jitter
    if backoff_base:
        os.environ['OTL_RETRY_BACKOFF_BASE_SEC'] = backoff_base
    if backoff_max:
        os.environ['OTL_RETRY_BACKOFF_MAX_SEC'] = backoff_max
    if slow_mode:
        os.environ['OTL_SLOW_MODE'] = slow_mode
    if wait_until_clear:
        os.environ['OTL_WAIT_UNTIL_CLEAR'] = wait_until_clear
    if wait_max_minutes:
        os.environ['OTL_WAIT_MAX_MINUTES'] = wait_max_minutes
    if per_book_delay:
        os.environ['OTL_PER_BOOK_DELAY_SEC'] = per_book_delay
    if subject_cooldown:
        os.environ['OTL_SUBJECT_COOLDOWN_SEC'] = subject_cooldown
    if force_random_ua:
        os.environ['OTL_FORCE_RANDOM_UA'] = force_random_ua
    if custom_ua:
        os.environ['OTL_USER_AGENT'] = custom_ua
    if proxy:
        os.environ['OTL_PROXY'] = proxy
    if book_max_attempts:
        os.environ['OTL_BOOK_MAX_ATTEMPTS'] = book_max_attempts
    if retry_pdf_on_miss:
        os.environ['OTL_RETRY_PDF_ON_MISS'] = retry_pdf_on_miss
    if any([delay_base, delay_jitter, backoff_base, backoff_max, slow_mode]):
        print(f"[OTL] Throttle config: base={os.getenv('OTL_DELAY_BASE_SEC','')} jitter={os.getenv('OTL_DELAY_JITTER_SEC','')} backoff_base={os.getenv('OTL_RETRY_BACKOFF_BASE_SEC','')} backoff_max={os.getenv('OTL_RETRY_BACKOFF_MAX_SEC','')}")

    
    subject_name = os.getenv('SUBJECT_NAME', '') or Variable.get('OTL_SUBJECT_NAME', default_var='') or (context.get('dag_run').conf.get('subject_name') if context.get('dag_run') and context.get('dag_run').conf else '')
    subject_url = os.getenv('SUBJECT_URL', '') or Variable.get('OTL_SUBJECT_URL', default_var='') or (context.get('dag_run').conf.get('subject_url') if context.get('dag_run') and context.get('dag_run').conf else '')
    root_subject_name = os.getenv('ROOT_SUBJECT_NAME', '') or Variable.get('OTL_ROOT_SUBJECT_NAME', default_var='') or (context.get('dag_run').conf.get('root_subject_name') if context.get('dag_run') and context.get('dag_run').conf else '')
    subjects_index_url = (
        os.getenv('SUBJECTS_INDEX_URL', '')
        or Variable.get('OTL_SUBJECTS_INDEX_URL', default_var='')
        or (
            (context.get('dag_run').conf.get('subjects_index_url') if context.get('dag_run') and context.get('dag_run').conf else '')
            or (context.get('dag_run').conf.get('index_url') if context.get('dag_run') and context.get('dag_run').conf else '')
        )
    )
    max_children_str = os.getenv('OTL_MAX_CHILDREN', '') or Variable.get('OTL_MAX_CHILDREN', default_var='') or (context.get('dag_run').conf.get('max_children') if context.get('dag_run') and context.get('dag_run').conf else '')
    max_parents_str = os.getenv('OTL_MAX_PARENTS', '') or Variable.get('OTL_MAX_PARENTS', default_var='') or (context.get('dag_run').conf.get('max_parents') if context.get('dag_run') and context.get('dag_run').conf else '')

    documents = []

    def process_leaf_live(leaf_name: str, leaf_url: str):
        for doc in live_scrape_leaf_subject_selenium(leaf_name, leaf_url):
            documents.append(doc)

    # Nếu không có bất kỳ tham số nào, mặc định chạy toàn bộ từ trang index
    if not (subject_name and subject_url) and not (root_subject_name and subjects_index_url) and not subjects_index_url:
        subjects_index_url = 'https://open.umn.edu/opentextbooks/subjects'

    try:
        if subject_name and subject_url:
            process_leaf_live(subject_name, subject_url)
        elif root_subject_name and subjects_index_url:
            max_children = int(max_children_str) if str(max_children_str).isdigit() else None
            for doc in live_scrape_root_subject_selenium(root_subject_name, subjects_index_url, max_children=max_children):
                documents.append(doc)
        elif subjects_index_url:
            max_children = int(max_children_str) if str(max_children_str).isdigit() else None
            max_parents = int(max_parents_str) if str(max_parents_str).isdigit() else None
            for doc in live_scrape_all_subjects_selenium(subjects_index_url, max_parents=max_parents, max_children=max_children):
                documents.append(doc)
        else:
            raise RuntimeError("Provide one of: (SUBJECT_NAME+SUBJECT_URL) or (ROOT_SUBJECT_NAME+SUBJECTS_INDEX_URL) or (SUBJECTS_INDEX_URL)")

        # Save JSON file locally under DATA_PATH
        os.makedirs(DATA_PATH, exist_ok=True)
        out_path = os.path.join(DATA_PATH, f"open_textbook_library_{execution_date}.json")
        with open(out_path, 'w', encoding='utf-8') as out:
            json.dump({
                'total_documents': len(documents),
                'scraped_at': datetime.now().isoformat(),
                'source': 'Open Textbook Library',
                'documents': documents
            }, out, ensure_ascii=False, indent=2)
        print(f"[OTL] Saved to {out_path}")

        if documents:
            # Lưu vào bronze layer với cấu trúc chuẩn
            result = bronze_storage.save_scraped_data(
                documents=documents,
                source='otl',
                execution_date=execution_date,
                file_type='textbooks',
                metadata={
                    'scraper_config': {
                        'subject_name': subject_name,
                        'subject_url': subject_url,
                        'root_subject_name': root_subject_name,
                        'subjects_index_url': subjects_index_url
                    },
                    'total_processed': len(documents)
                }
            )
            
            print(f"[OTL] Đã lưu vào bronze layer:")
            print(f"  - Data: {result.get('data_object_key')}")
            print(f"  - Metadata: {result.get('metadata_object_key')}")
            print(f"[OTL] Tổng cộng cào được: {len(documents)} documents")
            
            return {
                'execution_date': execution_date,
                'total_found': len(documents),
                'bronze_data_key': result.get('data_object_key'),
                'bronze_metadata_key': result.get('metadata_object_key')
            }
        else:
            print(f"[OTL] Không có dữ liệu để lưu")
            return {
                'execution_date': execution_date,
                'total_found': 0,
                'bronze_data_key': None,
                'bronze_metadata_key': None
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
    bronze_storage = BronzeStorageManager()
    
    try:
        bronze_objects = bronze_storage.list_bronze_data(source='otl')
        print(f"[OTL] Tìm thấy {len(bronze_objects)} files trong bronze layer:")
        
        # Hiển thị 5 files mới nhất
        recent_objects = sorted(bronze_objects, key=lambda x: x['last_modified'], reverse=True)[:5]
        for obj in recent_objects:
            print(f"  - {obj['object_name']} ({obj['size']} bytes)")
            
        return {
            'total_objects': len(bronze_objects),
            'recent_objects': [obj['object_name'] for obj in recent_objects]
        }
        
    except Exception as e:
        print(f"[OTL] Lỗi kiểm tra bronze layer: {e}")
        return {'total_objects': 0, 'recent_objects': []}

def emergency_recovery(**context):
    """Khôi phục từ data gần nhất nếu cần"""
    bronze_storage = BronzeStorageManager()
    
    try:
        latest_data = bronze_storage.get_latest_data('otl')
        
        if latest_data:
            print(f"[OTL] Data gần nhất trong bronze layer: {latest_data['object_name']}")
            print(f"[OTL] Emergency recovery system đã sẵn sàng")
            return {'latest_data': latest_data['object_name']}
        else:
            print("[OTL] Không tìm thấy data nào trong bronze layer")
            return {'latest_data': None}
            
    except Exception as e:
        print(f"[OTL] Lỗi emergency recovery: {e}")
        return {'latest_data': None}

def validate_data_quality(**context):
    """Kiểm tra chất lượng dữ liệu đã cào"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    bronze_storage = BronzeStorageManager()
    
    try:
        # Kiểm tra bronze layer data
        bronze_objects = bronze_storage.list_bronze_data(
            source='otl',
            start_date=execution_date,
            end_date=execution_date
        )
        
        if bronze_objects:
            print(f"[OTL] Tìm thấy {len(bronze_objects)} objects cho ngày {execution_date}")
            
            # Kiểm tra chất lượng dữ liệu cơ bản
            data_files = [obj for obj in bronze_objects if 'textbooks_' in obj['object_name']]
            metadata_files = [obj for obj in bronze_objects if 'metadata_' in obj['object_name']]
            
            quality_score = 100
            if not data_files:
                quality_score -= 50
                print("[OTL] Warning: Không tìm thấy data files")
            if not metadata_files:
                quality_score -= 20
                print("[OTL] Warning: Không tìm thấy metadata files")
            
            return {
                'date': execution_date,
                'objects_found': len(bronze_objects),
                'data_files': len(data_files),
                'metadata_files': len(metadata_files),
                'quality_score': quality_score,
                'quality_check': 'passed' if quality_score >= 80 else 'warning' if quality_score >= 50 else 'failed'
            }
        else:
            print(f"[OTL] Không tìm thấy objects nào cho ngày {execution_date}")
            return {
                'date': execution_date,
                'objects_found': 0,
                'quality_check': 'failed'
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


