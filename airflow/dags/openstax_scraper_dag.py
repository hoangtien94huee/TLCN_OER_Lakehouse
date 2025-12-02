from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os
from src.bronze_openstax import OpenStaxScraperStandalone

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

def scrape_openstax_documents(**context):
    """Scrape OpenStax documents and save to bronze layer"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OpenStax] Starting scrape for {execution_date}")
    
    try:
        # Initialize and run scraper
        openstax_scraper = OpenStaxScraperStandalone()
        openstax_scraper.run()
        
        print(f"[OpenStax] Scraping completed successfully")
        return {'execution_date': execution_date, 'status': 'success'}
        
    except Exception as e:
        print(f"[OpenStax] Scraping failed: {e}")
        raise

# Define task
scrape_task = PythonOperator(
    task_id='scrape_openstax_documents',
    python_callable=scrape_openstax_documents,
    dag=dag,
)

# Simple workflow: just scrape
scrape_task
