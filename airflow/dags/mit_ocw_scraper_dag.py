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

def scrape_mit_ocw_documents(**context):
    """Scrape MIT OCW documents and save to bronze layer"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[MIT OCW] Starting scrape for {execution_date}")
    
    try:
        # Initialize scraper
        mit_scraper = MITOCWScraper(
            delay=float(os.getenv('SCRAPING_DELAY_BASE', 2.0)),
            use_selenium=True,
            output_dir="/opt/airflow/scraped_data/mit_ocw",
            batch_size=int(os.getenv('BATCH_SIZE', 25)),
            max_documents=50
        )
        
        # Run scraper
        documents = mit_scraper.scrape_with_selenium()
        
        if documents:
            print(f"[MIT OCW] Scraped {len(documents)} courses")
            
            # Save to MinIO
            if mit_scraper.minio_enable:
                object_name = mit_scraper.save_to_minio(documents, logical_date=execution_date)
                print(f"[MIT OCW] Saved to MinIO: {object_name}")
            
            return {'execution_date': execution_date, 'total_found': len(documents), 'status': 'success'}
        else:
            print(f"[MIT OCW] No documents scraped")
            return {'execution_date': execution_date, 'total_found': 0, 'status': 'warning'}
        
    except Exception as e:
        print(f"[MIT OCW] Scraping failed: {e}")
        raise
    finally:
        if 'mit_scraper' in locals():
            mit_scraper.cleanup()

# Define task
scrape_mit_ocw_task = PythonOperator(
    task_id='scrape_mit_ocw_documents',
    python_callable=scrape_mit_ocw_documents,
    dag=dag,
)

# Simple workflow: just scrape
scrape_mit_ocw_task
