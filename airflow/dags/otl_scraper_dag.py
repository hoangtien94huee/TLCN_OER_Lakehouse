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

def scrape_open_textbook_library_documents(**context):
    """Scrape OTL documents and save to bronze layer
    
    Config tối ưu:
    - Sequential mode (ổn định hơn parallel)
    - Batch size: 50 books/subject
    - Delay: 1.5s giữa các requests
    - Có thể filter subjects cụ thể qua biến subjects_filter
    """
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OTL] Starting scrape for {execution_date}")

    try:
        # Có thể override config qua Airflow Variables
        batch_size = Variable.get('otl_batch_size', default_var=50)
        delay = Variable.get('otl_delay', default_var=1.5)
        subjects_filter = Variable.get('otl_subjects_filter', default_var=None)
        
        # Parse subjects_filter if provided (comma-separated string)
        if subjects_filter and isinstance(subjects_filter, str):
            subjects_filter = [s.strip() for s in subjects_filter.split(',')]
        else:
            subjects_filter = None
        
        # Initialize and run scraper with optimized settings
        otl_scraper = OTLScraperStandalone(
            parallel=False,          # Sequential mode - ổn định hơn
            batch_size=int(batch_size),
            delay=float(delay),
            max_workers=2,           # Nếu dùng parallel
            subjects_filter=subjects_filter,  # None = all subjects
            download_pdfs=True,
            pdf_types=['textbook']   # Chỉ lấy textbook PDF chính
        )
        otl_scraper.run()
        
        print(f"[OTL] Scraping completed successfully")
        return {'execution_date': execution_date, 'status': 'success'}

    except Exception as e:
        print(f"[OTL] Scraping failed: {e}")
        raise

# Define task
scrape_task = PythonOperator(
    task_id='scrape_open_textbook_library_documents',
    python_callable=scrape_open_textbook_library_documents,
    dag=dag,
)

# Simple workflow: just scrape
scrape_task


