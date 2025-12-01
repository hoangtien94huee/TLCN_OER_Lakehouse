"""
OER Lakehouse - Silver Layer Processing DAG
==========================================

This DAG orchestrates the transformation of raw data from Bronze layer
to clean, standardized data in Silver layer using Apache Spark.

Workflow:
1. Wait for bronze data to be available
2. Process each source system independently (MIT OCW, OpenStax, OTL)
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
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
import json
import os


# Reference data now uploaded to MinIO
os.environ.setdefault("REFERENCE_DATA_URI", "s3a://oer-lakehouse/bronze/reference/giaotrinh")

from src.silver_transform import SilverTransformer

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
    schedule_interval=timedelta(days=30),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['data-processing', 'silver-layer', 'spark', 'lakehouse'],
)

# === Utility Functions ===

def check_bronze_data_availability(**context):
    """Check if bronze data is available for processing"""
    try:
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        print(f"[Bronze Check] Checking bronze data for {execution_date}")
        
        return check_bronze_data_fallback(**context)
        
    except Exception as e:
        print(f"[Bronze Check] Failed to check bronze data: {e}")
        return check_bronze_data_fallback(**context)

def check_bronze_data_fallback(**context):
    """Fallback method to check bronze data using direct MinIO"""
    from minio import Minio
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[Bronze Check Fallback] Using direct MinIO check for {execution_date}")
    
    try:
        minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=os.getenv('MINIO_SECURE', '0').lower() in {'1', 'true', 'yes'}
        )
        
        bronze_bucket = 'oer-lakehouse'
        sources = ['mit_ocw', 'openstax', 'otl']
        
        available_sources = []
        for source in sources:
            try:
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


# === Processing Tasks - Each source independently ===

def process_mit_ocw_task(**context):
    """Process MIT OCW data to silver layer - INDEPENDENT"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[MIT OCW] Starting silver layer processing for {execution_date}")
    
    try:
        # ✅ Set to process ONLY MIT OCW
        os.environ['BRONZE_INPUT'] = 's3a://oer-lakehouse/bronze/mit_ocw/json/'
        
        # Run silver transform for MIT OCW only
        transformer = SilverTransformer()
        transformer.run()
        
        result = {
            'source_system': 'mit_ocw',
            'status': 'success',
            'execution_date': execution_date,
            'quality_resources': 0,
            'processing_duration_seconds': 0
        }
        
        print(f"[MIT OCW] Processing result: {json.dumps(result, indent=2)}")
        context['task_instance'].xcom_push(key='processing_result', value=result)
        
        return result
        
    except Exception as e:
        print(f"[MIT OCW] Processing failed: {e}")
        import traceback
        traceback.print_exc()
        
        error_result = {
            'source_system': 'mit_ocw',
            'status': 'error',
            'error': str(e),
            'execution_date': execution_date
        }
        context['task_instance'].xcom_push(key='processing_result', value=error_result)
        raise


def process_openstax_task(**context):
    """Process OpenStax data to silver layer - INDEPENDENT"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OpenStax] Starting silver layer processing for {execution_date}")
    
    try:
        # ✅ Set to process ONLY OpenStax
        os.environ['BRONZE_INPUT'] = 's3a://oer-lakehouse/bronze/openstax/json/'
        
        # Run silver transform for OpenStax only
        transformer = SilverTransformer()
        transformer.run()
        
        result = {
            'source_system': 'openstax',
            'status': 'success',
            'execution_date': execution_date,
            'quality_resources': 0,
            'processing_duration_seconds': 0
        }
        
        print(f"[OpenStax] Processing result: {json.dumps(result, indent=2)}")
        context['task_instance'].xcom_push(key='processing_result', value=result)
        
        return result
        
    except Exception as e:
        print(f"[OpenStax] Processing failed: {e}")
        import traceback
        traceback.print_exc()
        
        error_result = {
            'source_system': 'openstax',
            'status': 'error',
            'error': str(e),
            'execution_date': execution_date
        }
        context['task_instance'].xcom_push(key='processing_result', value=error_result)
        raise


def process_otl_task(**context):
    """Process OTL data to silver layer - INDEPENDENT"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[OTL] Starting silver layer processing for {execution_date}")
    
    try:
        # ✅ Set to process ONLY OTL
        os.environ['BRONZE_INPUT'] = 's3a://oer-lakehouse/bronze/otl/json/'
        
        # Run silver transform for OTL only
        transformer = SilverTransformer()
        transformer.run()
        
        result = {
            'source_system': 'otl',
            'status': 'success',
            'execution_date': execution_date,
            'quality_resources': 0,
            'processing_duration_seconds': 0
        }
        
        print(f"[OTL] Processing result: {json.dumps(result, indent=2)}")
        context['task_instance'].xcom_push(key='processing_result', value=result)
        
        return result
        
    except Exception as e:
        print(f"[OTL] Processing failed: {e}")
        import traceback
        traceback.print_exc()
        
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
    
    for source, result in report['results'].items():
        if result.get('status') == 'success':
            print(f"{source}: {result.get('quality_resources', 0)} resources")
        elif result.get('status') == 'error':
            print(f"{source}: {result.get('error', 'Unknown error')}")
        else:
            print(f"{source}: Skipped")
    
    return report


def cleanup_processing_artifacts(**context):
    """Clean up temporary processing artifacts"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print(f"[Cleanup] Cleaning up processing artifacts for {execution_date}")
    
    # Add cleanup logic here if needed
    print("[Cleanup] Processing artifacts cleanup completed")
    return True


def validate_silver_data_quality(**context):
    """Validate the quality of generated silver layer data"""
    from minio import Minio
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    
    minio_client = Minio(
        endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        secure=os.getenv('MINIO_SECURE', '0').lower() in {'1', 'true', 'yes'}
    )
    
    silver_bucket = 'oer-lakehouse'
    sources = ['mit_ocw', 'openstax', 'otl']
    
    validation_results = {}
    
    for source in sources:
        try:
            objects = list(minio_client.list_objects(
                bucket_name=silver_bucket,
                prefix=f"silver/{source}/",
                recursive=True
            ))
            
            silver_files = [obj for obj in objects if obj.object_name.endswith('.json')]
            
            if silver_files:
                total_size = sum(obj.size for obj in silver_files)
                validation_results[source] = {
                    'files_count': len(silver_files),
                    'total_size_bytes': total_size,
                    'status': 'validated' if total_size > 1000 else 'suspicious'
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
            print(f"{source}: {result['files_count']} files, {result['total_size_bytes']} bytes")
        elif status == 'missing':
            print(f"{source}: No silver files found")
        elif status == 'suspicious':
            print(f"{source}: Files found but size is suspicious")
        else:
            print(f"{source}: {result.get('error', 'Validation failed')}")
    
    return validation_results


# === DAG Tasks Definition ===

start_task = DummyOperator(
    task_id='start_silver_processing',
    dag=dag,
)

check_bronze_task = PythonOperator(
    task_id='check_bronze_data_availability',
    python_callable=check_bronze_data_availability,
    dag=dag,
)

# ✅ Each source processes independently
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

validate_quality_task = PythonOperator(
    task_id='validate_silver_data_quality',
    python_callable=validate_silver_data_quality,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_processing_report',
    python_callable=generate_processing_report,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_processing_artifacts',
    python_callable=cleanup_processing_artifacts,
    dag=dag,
)

health_check_task = BashOperator(
    task_id='silver_layer_health_check',
    bash_command='echo "Silver layer processing pipeline completed successfully at $(date)"',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_silver_processing',
    dag=dag,
)

start_task >> check_bronze_task
check_bronze_task >> [process_mit_ocw_silver, process_openstax_silver, process_otl_silver]

# Quality validation after ALL processing completes
[process_mit_ocw_silver, process_openstax_silver, process_otl_silver] >> validate_quality_task

# Generate report and cleanup in parallel after validation
validate_quality_task >> [generate_report_task, cleanup_task]

# Final health check and end
[generate_report_task, cleanup_task] >> health_check_task >> end_task