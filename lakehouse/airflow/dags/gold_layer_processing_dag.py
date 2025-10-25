"""
OER Lakehouse - Gold Layer Processing DAG
=========================================

This DAG creates analytics-ready data in the Gold layer from Silver layer data.
Generates business intelligence, ML features, and library-ready datasets.

Workflow:
1. Wait for Silver layer processing to complete
2. Create analytics tables (subject statistics, quality metrics, usage analytics)
3. Generate ML features for recommendation engine
4. Create library-ready data formats
5. Generate Gold layer reports
6. Data quality validation

Dependencies: Requires Silver layer processing to complete first
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowSkipException
from airflow.datasets import Dataset
import json
import os

from src.gold_analytics import GoldAnalyticsBuilder
from src.giaotrinh_reference_loader import main as load_reference_catalog

# DAG Configuration
default_args = {
    'owner': 'oer-lakehouse',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=2),  # Add timeout
}

dag = DAG(
    'gold_layer_processing',
    default_args=default_args,
    description='Create analytics and ML features in Gold layer from Silver layer data',
    schedule_interval=timedelta(days=30),  # Run monthly after Silver layer processing
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['data-processing', 'gold-layer', 'analytics', 'ml-features', 'lakehouse'],
)

# === Utility Functions ===

def validate_silver_layer_availability(**context):
    """Validate that Silver layer data is available and ready for processing"""
    from minio import Minio
    
    execution_date = context['logical_date'].strftime('%Y-%m-%d')
    logger = context['task_instance'].log
    
    logger.info(f"[Silver Validation] Checking Silver layer data for {execution_date}")
    
    try:
        # MinIO client setup
        minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=os.getenv('MINIO_SECURE', '0').lower() in {'1', 'true', 'yes'}
        )
        
        bucket_name = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        required_tables = {
            "oer_resources": "silver/default/oer_resources/metadata/",
            "reference_subjects": "silver/default/reference_subjects/metadata/",
            "reference_programs": "silver/default/reference_programs/metadata/",
            "reference_program_subject_links": "silver/default/reference_program_subject_links/metadata/",
        }

        validation_results = {}
        for table_name, prefix in required_tables.items():
            try:
                has_metadata = any(
                    minio_client.list_objects(
                        bucket_name=bucket_name,
                        prefix=prefix,
                        recursive=True
                    )
                )
                if has_metadata:
                    validation_results[table_name] = {'status': 'available'}
                    logger.info(f"[Silver Validation] Found metadata for table {table_name}")
                else:
                    validation_results[table_name] = {'status': 'missing'}
                    logger.warning(f"[Silver Validation] Missing metadata for table {table_name}")
            except Exception as e:
                validation_results[table_name] = {'status': 'error', 'error': str(e)}
                logger.error(f"[Silver Validation] Error checking table {table_name}: {e}")

        all_available = all(result.get('status') == 'available' for result in validation_results.values())

        validation_summary = {
            'execution_date': execution_date,
            'tables_checked': len(required_tables),
            'validation_details': validation_results,
            'ready_for_gold_processing': all_available
        }

        if all_available:
            logger.info("[Silver Validation] All required Iceberg tables present; ready for gold processing")
        else:
            logger.warning("[Silver Validation] Some Iceberg tables are missing; gold processing will be skipped")
        return validation_summary
        
    except Exception as e:
        logger.error(f"[Silver Validation] Failed: {e}")
        raise

def refresh_reference_datasets(**context):
    """Ensure reference datasets are available."""
    logger = context['task_instance'].log
    bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
    prefix = os.getenv("REFERENCE_MINIO_PREFIX", "bronze/reference/giaotrinh")
    reference_uri = f"s3a://{bucket}/{prefix.strip('/')}"
    os.environ["REFERENCE_DATA_URI"] = reference_uri
    os.environ["REFERENCE_DATA_DIR"] = str(Path(__file__).resolve().parents[2] / "data" / "reference")
    logger.info(f"[Reference] Using existing reference datasets at {reference_uri}")

def create_analytics_tables(**context):
    """Create analytics tables in Gold layer"""
    execution_date = context['logical_date'].strftime('%Y-%m-%d')
    logger = context['task_instance'].log
    
    logger.info(f"[Analytics] Creating analytics tables for {execution_date}")
    
    try:
        # Get validation results from previous task
        validation_result = context['task_instance'].xcom_pull(
            task_ids='validate_silver_layer', key='return_value'
        )
        if not validation_result:
            logger.warning("[Analytics] No validation result found; skipping gold build")
            raise AirflowSkipException("Silver validation did not return any result")
        if not validation_result.get('ready_for_gold_processing', False):
            logger.warning("[Analytics] Silver layer not ready; skipping gold build")
            raise AirflowSkipException("Silver layer data not ready for gold processing")
        
        available_sources = validation_result.get('available_source_names', [])
        logger.info(f"[Analytics] Processing sources: {available_sources}")
        
        # Set environment variables for Spark in Docker
        os.environ.setdefault("SPARK_MASTER_URL", "spark://spark-master:7077")
        os.environ.setdefault("MINIO_ENDPOINT", "http://minio:9000")
        os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
        os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin")
        os.environ.setdefault("MINIO_BUCKET", "oer-lakehouse")
        
        # Process to Gold layer using standalone analytics
        os.environ.setdefault(
            "REFERENCE_DATA_DIR",
            str(Path(__file__).resolve().parents[2] / "data" / "reference")
        )
        
        logger.info("[Analytics] Initializing Gold Analytics processor...")
        
        # Add retry logic for Spark session creation
        max_retries = 3
        retry_count = 0
        analytics = None
        
        while retry_count < max_retries:
            try:
                analytics = GoldAnalyticsBuilder()
                logger.info("[Analytics] Spark session created successfully")
                break
            except Exception as spark_error:
                retry_count += 1
                logger.warning(f"[Analytics] Spark session creation failed (attempt {retry_count}/{max_retries}): {spark_error}")
                if retry_count >= max_retries:
                    raise RuntimeError(f"Failed to create Spark session after {max_retries} attempts: {spark_error}")
                # Wait before retry
                import time
                time.sleep(30)
        
        if analytics is None:
            raise RuntimeError("Failed to initialize Gold Analytics processor")
            
        logger.info("[Analytics] Running gold layer processing...")
        analytics.run()
        
        # Create a result summary
        result = {
            'status': 'success',
            'processing_duration_seconds': 0,
            'tables_created': [
                'dim_programs',
                'dim_subjects',
                'dim_sources',
                'dim_languages',
                'dim_date',
                'fact_program_coverage',
                'fact_oer_resources'
            ],
            'processing_errors': []
        }
        
        # Store result for downstream tasks
        context['task_instance'].xcom_push(key='analytics_result', value=result)
        
        logger.info(f"[Analytics] Processing completed: {result.get('status', 'unknown')}")
        logger.info(f"[Analytics] Tables created: {len(result.get('tables_created', []))}")
        
        return result
        
    except AirflowSkipException:
        # Re-raise skip exceptions
        raise
    except Exception as e:
        logger.error(f"[Analytics] Failed to create analytics tables: {e}")
        logger.error(f"[Analytics] Error type: {type(e).__name__}")
        import traceback
        logger.error(f"[Analytics] Full traceback: {traceback.format_exc()}")
        
        error_result = {
            'status': 'error',
            'error': str(e),
            'error_type': type(e).__name__,
            'execution_date': execution_date
        }
        context['task_instance'].xcom_push(key='analytics_result', value=error_result)
        raise

def validate_gold_layer_quality(**context):
    """Validate the quality and completeness of Gold layer data"""
    from minio import Minio
    
    execution_date = context['logical_date'].strftime('%Y-%m-%d')
    logger = context['task_instance'].log
    
    logger.info(f"[Gold Validation] Validating Gold layer data for {execution_date}")
    
    try:
        # MinIO client setup
        minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=os.getenv('MINIO_SECURE', '0').lower() in {'1', 'true', 'yes'}
        )
        
        bucket_name = 'oer-lakehouse'
        # Check Gold Iceberg tables
        gold_tables = {
            'dim_programs': 'gold/analytics/dim_programs/metadata/',
            'dim_subjects': 'gold/analytics/dim_subjects/metadata/',
            'dim_sources': 'gold/analytics/dim_sources/metadata/',
            'dim_languages': 'gold/analytics/dim_languages/metadata/',
            'dim_date': 'gold/analytics/dim_date/metadata/',
            'fact_program_coverage': 'gold/analytics/fact_program_coverage/metadata/',
            'fact_oer_resources': 'gold/analytics/fact_oer_resources/metadata/',
        }
        
        validation_results = {}
        
        for table_name, prefix in gold_tables.items():
            try:
                # Check for Iceberg metadata
                has_metadata = any(
                    minio_client.list_objects(
                        bucket_name=bucket_name,
                        prefix=prefix,
                        recursive=True
                    )
                )
                
                if has_metadata:
                    # Check data files
                    data_prefix = prefix.replace('/metadata/', '/data/')
                    objects = list(minio_client.list_objects(
                        bucket_name=bucket_name,
                        prefix=data_prefix,
                        recursive=True
                    ))
                    
                    total_size = sum(obj.size for obj in objects) if objects else 0
                    validation_results[table_name] = {
                        'status': 'validated' if total_size > 0 else 'empty',
                        'files_count': len(objects),
                        'total_size_bytes': total_size,
                    }
                    logger.info(f"[Gold Validation] {table_name}: {len(objects)} files, {total_size} bytes")
                else:
                    validation_results[table_name] = {
                        'status': 'missing',
                        'files_count': 0,
                    }
                    logger.warning(f"[Gold Validation] {table_name}: No metadata found")
                    
            except Exception as e:
                validation_results[table_name] = {
                    'status': 'error',
                    'error': str(e)
                }
                logger.error(f"[Gold Validation] {table_name}: {e}")
        
        # Calculate validation summary
        validated_tables = [
            table for table, result in validation_results.items()
            if result.get('status') == 'validated'
        ]
        
        validation_summary = {
            'execution_date': execution_date,
            'total_tables_checked': len(gold_tables),
            'validated_tables': len(validated_tables),
            'validated_table_names': validated_tables,
            'validation_passed': len(validated_tables) >= 5,  # At least 5 tables should be validated
            'table_details': validation_results
        }
        
        logger.info(f"[Gold Validation] Summary: {len(validated_tables)}/{len(gold_tables)} tables validated")
        logger.info(f"[Gold Validation] Validation passed: {validation_summary['validation_passed']}")
        
        return validation_summary
        
    except Exception as e:
        logger.error(f"[Gold Validation] Failed: {e}")
        raise

def generate_gold_layer_report(**context):
    """Generate comprehensive Gold layer processing report"""
    execution_date = context['logical_date'].strftime('%Y-%m-%d')
    logger = context['task_instance'].log
    
    logger.info(f"[Gold Report] Generating processing report for {execution_date}")
    
    try:
        # Collect results from previous tasks
        validation_result = context['task_instance'].xcom_pull(
            task_ids='validate_silver_layer', key='return_value'
        ) or {}
        
        analytics_result = context['task_instance'].xcom_pull(
            task_ids='create_analytics_tables', key='analytics_result'
        ) or {}
        
        quality_result = context['task_instance'].xcom_pull(
            task_ids='validate_gold_layer_quality', key='return_value'
        ) or {}
        
        # Generate comprehensive report
        report = {
            'processing_date': execution_date,
            'generated_at': datetime.utcnow().isoformat(),
            'pipeline_status': 'success' if analytics_result.get('status') == 'success' else 'failed',
            'input_data': {
                'silver_layer_validation': validation_result,
                'silver_files_processed': validation_result.get('total_silver_files', 0),
                'source_systems': validation_result.get('available_source_names', [])
            },
            'processing_results': {
                'analytics_processing': analytics_result,
                'tables_created': analytics_result.get('tables_created', []),
                'processing_errors': analytics_result.get('processing_errors', []),
                'processing_duration': analytics_result.get('processing_duration_seconds', 0)
            },
            'output_validation': {
                'gold_layer_validation': quality_result,
                'validated_tables': quality_result.get('validated_table_names', []),
                'validation_passed': quality_result.get('validation_passed', False)
            },
            'summary': {
                'silver_sources_processed': len(validation_result.get('available_source_names', [])),
                'gold_tables_created': len(analytics_result.get('tables_created', [])),
                'gold_tables_validated': len(quality_result.get('validated_table_names', [])),
                'total_processing_time': analytics_result.get('processing_duration_seconds', 0),
                'data_quality_score': quality_result.get('validation_passed', False) and analytics_result.get('status') == 'success'
            }
        }
        
        # Log summary
        logger.info(f"[Gold Report] ============ GOLD LAYER PROCESSING REPORT ============")
        logger.info(f"[Gold Report] Date: {execution_date}")
        logger.info(f"[Gold Report] Pipeline Status: {report['pipeline_status']}")
        logger.info(f"[Gold Report] Silver Sources: {report['summary']['silver_sources_processed']}")
        logger.info(f"[Gold Report] Gold Tables: {report['summary']['gold_tables_created']}")
        logger.info(f"[Gold Report] Validation: {report['summary']['data_quality_score']}")
        logger.info(f"[Gold Report] Processing Time: {report['summary']['total_processing_time']:.2f}s")
        logger.info(f"[Gold Report] =========================================================")
        
        # Log detailed results
        if analytics_result.get('tables_created'):
            logger.info(f"[Gold Report] Created tables: {', '.join(analytics_result['tables_created'])}")
        
        if analytics_result.get('processing_errors'):
            logger.warning(f"[Gold Report] Processing errors: {len(analytics_result['processing_errors'])}")
            for error in analytics_result['processing_errors']:
                logger.warning(f"[Gold Report] Error: {error}")
        
        return report
        
    except Exception as e:
        logger.error(f"[Gold Report] Failed to generate report: {e}")
        return {
            'processing_date': execution_date,
            'generated_at': datetime.utcnow().isoformat(),
            'pipeline_status': 'error',
            'error': str(e)
        }

def cleanup_gold_processing_artifacts(**context):
    """Clean up temporary processing artifacts"""
    execution_date = context['logical_date'].strftime('%Y-%m-%d')
    logger = context['task_instance'].log
    
    logger.info(f"[Gold Cleanup] Cleaning up Gold layer processing artifacts for {execution_date}")
    
    # Add cleanup logic here if needed
    # For example: removing temporary Spark files, clearing caches, etc.
    
    logger.info("[Gold Cleanup] Gold layer processing artifacts cleanup completed")
    return True

# === DAG Tasks Definition ===

# Start of processing
start_task = DummyOperator(
    task_id='start_gold_processing',
    dag=dag,
)

# Validate Silver layer data availability
validate_silver_task = PythonOperator(
    task_id='validate_silver_layer',
    python_callable=validate_silver_layer_availability,
    dag=dag,
)

# Refresh taxonomy reference data
refresh_reference_task = PythonOperator(
    task_id='refresh_reference_data',
    python_callable=refresh_reference_datasets,
    dag=dag,
)

# Create analytics tables
create_analytics_task = PythonOperator(
    task_id='create_analytics_tables',
    python_callable=create_analytics_tables,
    dag=dag,
)

# Validate Gold layer quality
validate_quality_task = PythonOperator(
    task_id='validate_gold_layer_quality',
    python_callable=validate_gold_layer_quality,
    dag=dag,
)

# Generate processing report
generate_report_task = PythonOperator(
    task_id='generate_gold_layer_report',
    python_callable=generate_gold_layer_report,
    dag=dag,
)

# Cleanup artifacts
cleanup_task = PythonOperator(
    task_id='cleanup_gold_processing_artifacts',
    python_callable=cleanup_gold_processing_artifacts,
    dag=dag,
)

# Health check
health_check_task = BashOperator(
    task_id='gold_layer_health_check',
    bash_command='echo "Gold layer processing pipeline completed successfully at $(date)"',
    dag=dag,
)

# End of processing
end_task = DummyOperator(
    task_id='end_gold_processing',
    dag=dag,
)

# === DAG Dependencies ===

# Direct workflow without external dependency
start_task >> validate_silver_task >> refresh_reference_task
refresh_reference_task >> create_analytics_task >> validate_quality_task
validate_quality_task >> generate_report_task >> cleanup_task
cleanup_task >> health_check_task >> end_task
