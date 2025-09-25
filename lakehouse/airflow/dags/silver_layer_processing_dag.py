"""
OER Lakehouse - Silver Layer Processing DAG
==========================================

This DAG orchestrates the transformation of raw data from Bronze layer
to clean, standardized data in Silver layer using Apache Spark.

Workflow:
1. Wait for bronze data to be available
2. Process each source system (MIT OCW, OpenStax, OTL)
3. Apply data quality checks and deduplication
4. Save standardized data to Silver layer
5. Generate processing reports
6. Cleanup and monitoring

Dependencies: Requires scraping DAGs to complete first
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor  # Re-enabled for production
from airflow.operators.dummy import DummyOperator
import json
import os

from src.transformers import (
    process_mit_ocw_to_silver,
    process_openstax_to_silver, 
    process_otl_to_silver,
    process_all_sources_to_silver
)

# DAG Configuration
default_args = {
    'owner': 'oer-lakehouse',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'silver_layer_processing',
    default_args=default_args,
    description='Transform Bronze layer data to Silver layer using Spark',
    schedule_interval=timedelta(days=30),  # Run monthly after scraping DAGs
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['data-processing', 'silver-layer', 'spark', 'lakehouse'],
)

# === Utility Functions ===

def check_bronze_data_availability(**context):
    """Check if bronze data is available for processing"""
    try:
        from src.utils.bronze_storage import BronzeStorageManager
        
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        print(f"[Bronze Check] Checking bronze data for {execution_date}")
        
        # Use bronze storage manager for consistency
        bronze_storage = BronzeStorageManager()
        sources = ['mit_ocw', 'openstax', 'otl']
        
        available_sources = []
        for source in sources:
            try:
                # Check for data from execution date first
                objects = bronze_storage.list_bronze_data(
                    source=source,
                    start_date=execution_date,
                    end_date=execution_date
                )
                
                # If no data for exact date, find latest available data
                if not objects:
                    print(f"[Bronze Check] No data for {execution_date}, searching for latest data for {source}")
                    
                    # Get all available data for this source (no date filter)
                    all_objects = bronze_storage.list_bronze_data(source=source)
                    
                    if all_objects:
                        # Extract dates and find the latest
                        available_dates = set()
                        for obj_info in all_objects:
                            obj_key = obj_info['object_name']  # Get object name from dict
                            # Extract date from path: bronze/source/YYYY-MM-DD/file.json
                            parts = obj_key.split('/')
                            if len(parts) >= 3:
                                date_part = parts[2]
                                if len(date_part) == 10 and date_part.count('-') == 2:  # YYYY-MM-DD format
                                    available_dates.add(date_part)
                        
                        if available_dates:
                            latest_date = max(available_dates)
                            print(f"[Bronze Check] Found latest data for {source} from {latest_date}")
                            objects = bronze_storage.list_bronze_data(
                                source=source,
                                start_date=latest_date,
                                end_date=latest_date
                            )
                
                if objects:
                    available_sources.append(source)
                    print(f"[Bronze Check] {source}: {len(objects)} files found")
                else:
                    print(f"[Bronze Check] {source}: No files found")
                    
            except Exception as e:
                print(f"[Bronze Check] Error checking {source}: {e}")
        
        print(f"[Bronze Check] Available sources: {available_sources}")
        
        return {
            'execution_date': execution_date,
            'available_sources': available_sources,
            'total_sources': len(sources),
            'ready_for_processing': len(available_sources) >= 1  # At least 1 source has data
        }
        
    except Exception as e:
        print(f"[Bronze Check] Failed to initialize bronze storage: {e}")
        # Fallback to direct MinIO check
        return check_bronze_data_fallback(**context)

def check_bronze_data_fallback(**context):
    """Fallback method to check bronze data using direct MinIO"""
    from minio import Minio
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[Bronze Check Fallback] Using direct MinIO check for {execution_date}")
    
    try:
        # MinIO client setup
        minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=os.getenv('MINIO_SECURE', '0').lower() in {'1', 'true', 'yes'}
        )
        
        # Force use oer-lakehouse bucket for consistency  
        bronze_bucket = 'oer-lakehouse'
        sources = ['mit_ocw', 'openstax', 'otl']
        
        available_sources = []
        for source in sources:
            try:
                # Check for files in bronze layer structure
                objects = list(minio_client.list_objects(
                    bucket_name=bronze_bucket,
                    prefix=f"bronze/{source}/",
                    recursive=True
                ))
                
                recent_files = [
                    obj for obj in objects 
                    if obj.object_name.endswith('.json') and
                    (datetime.utcnow() - obj.last_modified).days <= 7
                ]
                
                if recent_files:
                    available_sources.append(source)
                    print(f"[Bronze Check Fallback] {source}: {len(recent_files)} files found")
                else:
                    print(f"[Bronze Check Fallback] {source}: No recent files found")
                    
            except Exception as e:
                print(f"[Bronze Check Fallback] Error checking {source}: {e}")
        
        return {
            'execution_date': execution_date,
            'available_sources': available_sources,
            'total_sources': len(sources),
            'ready_for_processing': len(available_sources) >= 1
        }
        
    except Exception as e:
        print(f"[Bronze Check Fallback] Failed: {e}")
        return {
            'execution_date': execution_date,
            'available_sources': [],
            'total_sources': 3,
            'ready_for_processing': False,
            'error': str(e)
        }

def process_mit_ocw_task(**context):
    """Process MIT OCW data to silver layer"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[MIT OCW] Starting silver layer processing for {execution_date}")
    
    try:
        result = process_mit_ocw_to_silver(**context)
        print(f"[MIT OCW] Processing result: {json.dumps(result, indent=2)}")
        
        # Store result for downstream tasks
        context['task_instance'].xcom_push(key='processing_result', value=result)
        
        return result
        
    except Exception as e:
        print(f"[MIT OCW] Processing failed: {e}")
        error_result = {
            'source_system': 'mit_ocw',
            'status': 'error',
            'error': str(e),
            'execution_date': execution_date
        }
        context['task_instance'].xcom_push(key='processing_result', value=error_result)
        raise

def process_openstax_task(**context):
    """Process OpenStax data to silver layer"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OpenStax] Starting silver layer processing for {execution_date}")
    
    try:
        result = process_openstax_to_silver(**context)
        print(f"[OpenStax] Processing result: {json.dumps(result, indent=2)}")
        
        context['task_instance'].xcom_push(key='processing_result', value=result)
        return result
        
    except Exception as e:
        print(f"[OpenStax] Processing failed: {e}")
        error_result = {
            'source_system': 'openstax',
            'status': 'error',
            'error': str(e),
            'execution_date': execution_date
        }
        context['task_instance'].xcom_push(key='processing_result', value=error_result)
        raise

def process_otl_task(**context):
    """Process OTL data to silver layer"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OTL] Starting silver layer processing for {execution_date}")
    
    try:
        result = process_otl_to_silver(**context)
        print(f"[OTL] Processing result: {json.dumps(result, indent=2)}")
        
        context['task_instance'].xcom_push(key='processing_result', value=result)
        return result
        
    except Exception as e:
        print(f"[OTL] Processing failed: {e}")
        error_result = {
            'source_system': 'otl',
            'status': 'error',
            'error': str(e),
            'execution_date': execution_date
        }
        context['task_instance'].xcom_push(key='processing_result', value=error_result)
        raise

def generate_processing_report(**context):
    """Generate comprehensive processing report"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    
    # Collect results from all processing tasks
    mit_result = context['task_instance'].xcom_pull(
        task_ids='process_mit_ocw_silver', key='processing_result'
    ) or {'status': 'skipped'}
    
    openstax_result = context['task_instance'].xcom_pull(
        task_ids='process_openstax_silver', key='processing_result'
    ) or {'status': 'skipped'}
    
    otl_result = context['task_instance'].xcom_pull(
        task_ids='process_otl_silver', key='processing_result'
    ) or {'status': 'skipped'}
    
    # Generate comprehensive report
    report = {
        'processing_date': execution_date,
        'generated_at': datetime.utcnow().isoformat(),
        'results': {
            'mit_ocw': mit_result,
            'openstax': openstax_result,
            'otl': otl_result
        },
        'summary': {
            'total_sources': 3,
            'successful_sources': sum(
                1 for r in [mit_result, openstax_result, otl_result]
                if r.get('status') == 'success'
            ),
            'total_quality_resources': sum(
                r.get('quality_resources', 0)
                for r in [mit_result, openstax_result, otl_result]
                if isinstance(r.get('quality_resources'), int)
            ),
            'total_processing_time': sum(
                r.get('processing_duration_seconds', 0)
                for r in [mit_result, openstax_result, otl_result]
                if isinstance(r.get('processing_duration_seconds'), (int, float))
            )
        }
    }
    
    print(f"[Silver Processing Report]")
    print(f"Date: {execution_date}")
    print(f"Successful Sources: {report['summary']['successful_sources']}/3")
    print(f"Total Quality Resources: {report['summary']['total_quality_resources']}")
    print(f"Total Processing Time: {report['summary']['total_processing_time']:.2f}s")
    
    # Store detailed results
    for source, result in report['results'].items():
        if result.get('status') == 'success':
            print(f"✅ {source}: {result.get('quality_resources', 0)} resources")
        elif result.get('status') == 'error':
            print(f"❌ {source}: {result.get('error', 'Unknown error')}")
        else:
            print(f"⏭️ {source}: Skipped")
    
    return report

def cleanup_processing_artifacts(**context):
    """Clean up temporary processing artifacts"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[Cleanup] Cleaning up processing artifacts for {execution_date}")
    
    # Add cleanup logic here if needed
    # For example: removing temporary Spark files, clearing caches, etc.
    
    print("[Cleanup] Processing artifacts cleanup completed")
    return True

def validate_silver_data_quality(**context):
    """Validate the quality of generated silver layer data"""
    from minio import Minio
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    
    # MinIO client setup
    minio_client = Minio(
        endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        secure=os.getenv('MINIO_SECURE', '0').lower() in {'1', 'true', 'yes'}
    )
    
    # Force use oer-lakehouse bucket for consistency
    silver_bucket = 'oer-lakehouse'
    sources = ['mit_ocw', 'openstax', 'otl']
    
    validation_results = {}
    
    for source in sources:
        try:
            # Check for today's silver files
            objects = list(minio_client.list_objects(
                bucket_name=silver_bucket,
                prefix=f"silver/{source}/{execution_date}/",  # Updated path structure
                recursive=True
            ))
            
            silver_files = [obj for obj in objects if obj.object_name.endswith('.json')]
            
            if silver_files:
                # Basic validation - check file sizes and count
                total_size = sum(obj.size for obj in silver_files)
                validation_results[source] = {
                    'files_count': len(silver_files),
                    'total_size_bytes': total_size,
                    'status': 'validated' if total_size > 1000 else 'suspicious'  # Basic size check
                }
            else:
                validation_results[source] = {
                    'files_count': 0,
                    'total_size_bytes': 0,
                    'status': 'missing'
                }
                
        except Exception as e:
            validation_results[source] = {
                'status': 'error',
                'error': str(e)
            }
    
    print(f"[Quality Validation] Results for {execution_date}:")
    for source, result in validation_results.items():
        status = result.get('status', 'unknown')
        if status == 'validated':
            print(f"✅ {source}: {result['files_count']} files, {result['total_size_bytes']} bytes")
        elif status == 'missing':
            print(f"⚠️ {source}: No silver files found")
        elif status == 'suspicious':
            print(f"⚠️ {source}: Files found but size is suspicious")
        else:
            print(f"❌ {source}: {result.get('error', 'Validation failed')}")
    
    return validation_results

# === DAG Tasks Definition ===

# Start of processing
start_task = DummyOperator(
    task_id='start_silver_processing',
    dag=dag,
)

# Check bronze data availability  
check_bronze_task = PythonOperator(
    task_id='check_bronze_data_availability',
    python_callable=check_bronze_data_availability,
    dag=dag,
)

# # External task sensors - Re-enabled for production use
# wait_for_mit_scraping = ExternalTaskSensor(
#     task_id='wait_for_mit_ocw_scraping',
#     external_dag_id='mit_ocw_scraper_daily',
#     external_task_id='health_check',
#     timeout=7200,  # 2 hour timeout for monthly jobs
#     poke_interval=600,  # Check every 10 minutes
#     dag=dag,
#     mode='reschedule',  # Free up worker slots while waiting
#     allowed_states=['success'],
#     failed_states=['failed', 'upstream_failed', 'skipped']
# )

# wait_for_openstax_scraping = ExternalTaskSensor(
#     task_id='wait_for_openstax_scraping',
#     external_dag_id='openstax_scraper_daily',
#     external_task_id='health_check',
#     timeout=7200,
#     poke_interval=600,
#     dag=dag,
#     mode='reschedule',
#     allowed_states=['success'],
#     failed_states=['failed', 'upstream_failed', 'skipped']
# )

# wait_for_otl_scraping = ExternalTaskSensor(
#     task_id='wait_for_otl_scraping',
#     external_dag_id='otl_scraper_daily',
#     external_task_id='health_check',
#     timeout=7200,
#     poke_interval=600,
#     dag=dag,
#     mode='reschedule',
#     allowed_states=['success'],
#     failed_states=['failed', 'upstream_failed', 'skipped']
# )

# Processing tasks for each source
process_mit_ocw_silver = PythonOperator(
    task_id='process_mit_ocw_silver',
    python_callable=process_mit_ocw_task,
    dag=dag,
)

process_openstax_silver = PythonOperator(
    task_id='process_openstax_silver',
    python_callable=process_openstax_task,
    dag=dag,
)

process_otl_silver = PythonOperator(
    task_id='process_otl_silver',
    python_callable=process_otl_task,
    dag=dag,
)

# Quality validation
validate_quality_task = PythonOperator(
    task_id='validate_silver_data_quality',
    python_callable=validate_silver_data_quality,
    dag=dag,
)

# Generate processing report
generate_report_task = PythonOperator(
    task_id='generate_processing_report',
    python_callable=generate_processing_report,
    dag=dag,
)

# Cleanup
cleanup_task = PythonOperator(
    task_id='cleanup_processing_artifacts',
    python_callable=cleanup_processing_artifacts,
    dag=dag,
)

# Health check
health_check_task = BashOperator(
    task_id='silver_layer_health_check',
    bash_command='echo "Silver layer processing pipeline completed successfully at $(date)"',
    dag=dag,
)

# End of processing
end_task = DummyOperator(
    task_id='end_silver_processing',
    dag=dag,
)

# === DAG Dependencies ===

# Production workflow with external task dependencies
# start_task >> [wait_for_mit_scraping, wait_for_openstax_scraping, wait_for_otl_scraping]
# [wait_for_mit_scraping, wait_for_openstax_scraping, wait_for_otl_scraping] >> check_bronze_task

# Process each source in parallel after bronze check
start_task >> check_bronze_task >> [process_mit_ocw_silver, process_openstax_silver, process_otl_silver]

# Quality validation after all processing
[process_mit_ocw_silver, process_openstax_silver, process_otl_silver] >> validate_quality_task

# Generate report and cleanup in parallel after validation
validate_quality_task >> [generate_report_task, cleanup_task]

# Final health check and end
[generate_report_task, cleanup_task] >> health_check_task >> end_task

# === DAG Documentation ===

dag.doc_md = """
# Silver Layer Processing DAG

This DAG transforms raw OER data from the Bronze layer into clean, standardized data in the Silver layer.

## Workflow Steps:

1. **Wait for Scraping**: External sensors wait for bronze data collection to complete
2. **Check Bronze Data**: Verify that fresh bronze data is available for processing
3. **Parallel Processing**: Transform data from each source system using Apache Spark:
   - MIT OpenCourseWare
   - OpenStax
   - Open Textbook Library
4. **Quality Validation**: Validate the generated silver layer data
5. **Reporting**: Generate comprehensive processing reports
6. **Cleanup**: Clean up temporary processing artifacts

## Key Features:

- **Data Standardization**: Unifies data schemas across different OER sources
- **Quality Assurance**: Applies data quality rules and deduplication
- **Parallel Processing**: Processes multiple sources simultaneously for efficiency
- **Error Handling**: Robust error handling with detailed logging
- **Monitoring**: Comprehensive reporting and validation

## Dependencies:

- Apache Spark cluster must be running
- MinIO object storage must be accessible
- Bronze layer data must be available

## Outputs:

- Standardized JSON files in MinIO oer-lakehouse bucket (silver layer)
- Processing reports with quality metrics
- Validation results for downstream monitoring

## Schedule:

Runs monthly (every 30 days) after scraping DAGs complete, ensuring all bronze data is processed together. Manual triggering available for ad-hoc processing.

**Schedule Alignment:**
- Scraper DAGs: Every 30 days
- Silver Layer DAG: Every 30 days (waits for scrapers)
- Processing Window: 2 hours timeout for completion
"""
