"""
DAG for exporting Gold layer OER data to SAF format for DSpace import.
Uses Simple Archive Format (SAF) for reliable batch import.

Export creates a ZIP file that can be imported via DSpace UI:
  Admin > System > Import Batch > Select Collection > Upload ZIP

After running this DAG:
1. Copy ZIP from container: docker cp oer-airflow-scraper:/opt/airflow/saf_export.zip ./
2. Go to DSpace UI: http://localhost:4000/admin/batch-import
3. Select collection "Open Educational Resources" 
4. Upload the ZIP file and click Proceed

DSpace Config (for reference):
- Collection Handle: 123456789/2
- Collection UUID: 1e9be5e4-6056-4f43-bad0-0288669ce6ad
- Admin Email: admin@dspace.org
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import os

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration
SAF_OUTPUT_DIR = "/opt/airflow/saf_export"


def export_to_saf(**context):
    """Export Gold layer data to SAF format."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')
    
    from saf_exporter import export_gold_to_saf
    
    execution_date = context['ds']
    logger.info(f"[SAF Export] Starting export for {execution_date}")
    
    # Export all records (or set limit for testing)
    limit = None  # Set to a number for testing, e.g., 100
    create_zip = True  # Create ZIP for faster transfer
    
    output_path = export_gold_to_saf(limit=limit, output_dir=SAF_OUTPUT_DIR, create_zip=create_zip)
    
    # Check if it's a ZIP file or directory
    is_zip = output_path.endswith('.zip')
    
    if is_zip:
        # Count items from ZIP
        import zipfile
        with zipfile.ZipFile(output_path, 'r') as zf:
            item_dirs = set(name.split('/')[0] for name in zf.namelist() if name.startswith('item_'))
            item_count = len(item_dirs)
        
        zip_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logger.info(f"[SAF Export] Exported {item_count} items to ZIP: {output_path} ({zip_size_mb:.2f} MB)")
    else:
        # Count from directory
        item_count = len([d for d in os.listdir(output_path) if d.startswith('item_')])
        logger.info(f"[SAF Export] Exported {item_count} items to {output_path}")
    
    # Push to XCom for next task
    context['task_instance'].xcom_push(key='saf_output_path', value=output_path)
    context['task_instance'].xcom_push(key='item_count', value=item_count)
    context['task_instance'].xcom_push(key='is_zip', value=is_zip)
    
    return output_path


def verify_saf_export(**context):
    """Verify SAF export was successful."""
    import zipfile
    
    ti = context['task_instance']
    output_path = ti.xcom_pull(key='saf_output_path', task_ids='export_to_saf')
    item_count = ti.xcom_pull(key='item_count', task_ids='export_to_saf')
    is_zip = ti.xcom_pull(key='is_zip', task_ids='export_to_saf')
    
    if not output_path or not os.path.exists(output_path):
        raise Exception(f"SAF export not found: {output_path}")
    
    if item_count == 0:
        raise Exception("No items were exported")
    
    # Verify first item
    if is_zip:
        # Verify from ZIP
        with zipfile.ZipFile(output_path, 'r') as zf:
            # Check first item has required files
            first_item_files = [name for name in zf.namelist() if name.startswith('item_000001/')]
            required_files = ['item_000001/dublin_core.xml', 'item_000001/contents']
            
            for required in required_files:
                if required not in first_item_files:
                    raise Exception(f"Missing required file in ZIP: {required}")
            
            # Log sample dublin_core.xml
            sample_dc = zf.read('item_000001/dublin_core.xml').decode('utf-8')
            logger.info(f"[SAF Verify] Sample dublin_core.xml:\n{sample_dc[:1000]}")
    else:
        # Verify from directory
        first_item = os.path.join(output_path, "item_000001")
        if os.path.exists(first_item):
            required_files = ['dublin_core.xml', 'contents']
            for f in required_files:
                if not os.path.exists(os.path.join(first_item, f)):
                    raise Exception(f"Missing required file: {f} in {first_item}")
            
            # Log sample dublin_core.xml
            with open(os.path.join(first_item, 'dublin_core.xml'), 'r') as f:
                sample_dc = f.read()
                logger.info(f"[SAF Verify] Sample dublin_core.xml:\n{sample_dc[:1000]}")
    
    logger.info(f"[SAF Verify] Export verified: {item_count} items ready for import via DSpace UI")
    return True


with DAG(
    'dspace_saf_export_dag',
    default_args=default_args,
    description='Export Gold layer to SAF format for DSpace import',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dspace', 'saf', 'export'],
) as dag:
    
    # Task 1: Export Gold layer to SAF format
    export_task = PythonOperator(
        task_id='export_to_saf',
        python_callable=export_to_saf,
        provide_context=True,
    )
    
    # Task 2: Verify export
    verify_task = PythonOperator(
        task_id='verify_saf_export',
        python_callable=verify_saf_export,
        provide_context=True,
    )
    
    # Task dependencies (export only, import via DSpace UI)
    export_task >> verify_task
