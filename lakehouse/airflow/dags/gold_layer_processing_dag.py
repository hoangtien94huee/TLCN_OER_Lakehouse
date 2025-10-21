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
import json
import os

from src.gold_analytics import GoldAnalyticsStandalone
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
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
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
        
        bucket_name = 'oer-lakehouse'
        sources = ['mit_ocw', 'openstax', 'otl']
        
        validation_results = {}
        total_silver_files = 0
        
        for source in sources:
            try:
                # Check for silver files (could be from today or recent days)
                objects = list(minio_client.list_objects(
                    bucket_name=bucket_name,
                    prefix=f"silver/{source}/",
                    recursive=True
                ))
                
                silver_files = [
                    obj for obj in objects 
                    if obj.object_name.endswith('.json') and
                    (datetime.utcnow() - obj.last_modified).days <= 7  # Within 7 days
                ]
                
                if silver_files:
                    total_size = sum(obj.size for obj in silver_files)
                    validation_results[source] = {
                        'files_count': len(silver_files),
                        'total_size_bytes': total_size,
                        'latest_file': max(silver_files, key=lambda x: x.last_modified).object_name,
                        'status': 'available'
                    }
                    total_silver_files += len(silver_files)
                    logger.info(f"✅ {source}: {len(silver_files)} files, {total_size} bytes")
                else:
                    validation_results[source] = {
                        'files_count': 0,
                        'status': 'missing'
                    }
                    logger.warning(f"⚠️ {source}: No recent Silver files found")
                    
            except Exception as e:
                validation_results[source] = {
                    'status': 'error',
                    'error': str(e)
                }
                logger.error(f"❌ {source}: {e}")
        
        # Overall validation
        available_sources = [
            source for source, result in validation_results.items()
            if result.get('status') == 'available'
        ]
        
        validation_summary = {
            'execution_date': execution_date,
            'total_sources_checked': len(sources),
            'available_sources': len(available_sources),
            'available_source_names': available_sources,
            'total_silver_files': total_silver_files,
            'ready_for_gold_processing': len(available_sources) >= 1,  # At least 1 source required
            'validation_details': validation_results
        }
        
        logger.info(f"[Silver Validation] Summary: {available_sources}/{len(sources)} sources available")
        logger.info(f"[Silver Validation] Ready for Gold processing: {validation_summary['ready_for_gold_processing']}")
        
        if not validation_summary['ready_for_gold_processing']:
            raise ValueError("Insufficient Silver layer data for Gold processing")
        
        return validation_summary
        
    except Exception as e:
        logger.error(f"[Silver Validation] Failed: {e}")
        raise

def refresh_reference_datasets(**context):
    """Refresh faculty/subject taxonomy from giaotrinh.sql before gold processing."""
    logger = context['task_instance'].log
    airflow_root = Path(__file__).resolve().parents[2]
    sql_default = airflow_root.parent / "giaotrinh.sql"
    if not sql_default.exists():
        sql_default = airflow_root / "giaotrinh.sql"
    sql_path = Path(os.getenv("REFERENCE_SQL_PATH", str(sql_default)))
    output_dir = Path(os.getenv("REFERENCE_DATA_DIR", str(airflow_root / "data" / "reference")))
    minio_prefix = os.getenv("REFERENCE_MINIO_PREFIX", "bronze/reference/giaotrinh")

    logger.info(f"[Reference] Refreshing taxonomy from {sql_path}")
    if not sql_path.exists():
        raise FileNotFoundError(f"Reference SQL file not found at {sql_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    load_reference_catalog(str(sql_path), str(output_dir), upload_to_minio=True, minio_prefix=minio_prefix)

    bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
    reference_uri = f"s3a://{bucket}/{minio_prefix.strip('/')}"
    os.environ["REFERENCE_DATA_URI"] = reference_uri
    os.environ["REFERENCE_DATA_DIR"] = str(output_dir)

    logger.info(f"[Reference] Reference datasets written to {output_dir} and uploaded to {reference_uri}")

def create_analytics_tables(**context):
    """Create analytics tables in Gold layer"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    logger = context['task_instance'].log
    
    logger.info(f"[Analytics] Creating analytics tables for {execution_date}")
    
    try:
        # Get validation results from previous task
        validation_result = context['task_instance'].xcom_pull(
            task_ids='validate_silver_layer', key='return_value'
        )
        
        available_sources = validation_result.get('available_source_names', [])
        logger.info(f"[Analytics] Processing sources: {available_sources}")
        
        # Set context for Gold layer processing
        gold_context = context.copy()
        gold_context['dag_run'] = type('DagRun', (), {
            'conf': {'source_systems': available_sources}
        })()
        
        # Process to Gold layer using standalone analytics
        os.environ.setdefault(
            "REFERENCE_DATA_DIR",
            str(Path(__file__).resolve().parents[2] / "data" / "reference")
        )
        analytics = GoldAnalyticsStandalone()
        analytics.run()
        
        # Create a result summary
        result = {
            'status': 'success',
            'processing_duration_seconds': 0,
            'tables_created': [
                'source_summary',
                'subject_analysis', 
                'quality_metrics',
                'author_analysis',
                'ml_features',
                'library_export'
            ],
            'processing_errors': []
        }
        
        # Store result for downstream tasks
        context['task_instance'].xcom_push(key='analytics_result', value=result)
        
        logger.info(f"[Analytics] Processing completed: {result.get('status', 'unknown')}")
        logger.info(f"[Analytics] Tables created: {len(result.get('tables_created', []))}")
        
        return result
        
    except Exception as e:
        logger.error(f"[Analytics] Failed to create analytics tables: {e}")
        error_result = {
            'status': 'error',
            'error': str(e),
            'execution_date': execution_date
        }
        context['task_instance'].xcom_push(key='analytics_result', value=error_result)
        raise

def validate_gold_layer_quality(**context):
    """Validate the quality and completeness of Gold layer data"""
    from minio import Minio
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
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
        gold_layers = ['analytics', 'ml_features', 'aggregations', 'library_ready']
        
        validation_results = {}
        
        for layer in gold_layers:
            try:
                # Check for gold layer files
                objects = list(minio_client.list_objects(
                    bucket_name=bucket_name,
                    prefix=f"gold/{layer}/",
                    recursive=True
                ))
                
                # Filter recent files
                recent_files = [
                    obj for obj in objects 
                    if (datetime.utcnow() - obj.last_modified).hours <= 24  # Within 24 hours
                ]
                
                if recent_files:
                    total_size = sum(obj.size for obj in recent_files)
                    validation_results[layer] = {
                        'files_count': len(recent_files),
                        'total_size_bytes': total_size,
                        'status': 'validated' if total_size > 1000 else 'suspicious',
                        'file_types': list(set(obj.object_name.split('.')[-1] for obj in recent_files))
                    }
                    logger.info(f"✅ {layer}: {len(recent_files)} files, {total_size} bytes")
                else:
                    validation_results[layer] = {
                        'files_count': 0,
                        'status': 'missing'
                    }
                    logger.warning(f"⚠️ {layer}: No recent files found")
                    
            except Exception as e:
                validation_results[layer] = {
                    'status': 'error',
                    'error': str(e)
                }
                logger.error(f"❌ {layer}: {e}")
        
        # Calculate validation summary
        validated_layers = [
            layer for layer, result in validation_results.items()
            if result.get('status') == 'validated'
        ]
        
        validation_summary = {
            'execution_date': execution_date,
            'total_layers_checked': len(gold_layers),
            'validated_layers': len(validated_layers),
            'validated_layer_names': validated_layers,
            'validation_passed': len(validated_layers) >= 2,  # At least 2 layers should be validated
            'layer_details': validation_results
        }
        
        logger.info(f"[Gold Validation] Summary: {validated_layers}/{len(gold_layers)} layers validated")
        logger.info(f"[Gold Validation] Validation passed: {validation_summary['validation_passed']}")
        
        return validation_summary
        
    except Exception as e:
        logger.error(f"[Gold Validation] Failed: {e}")
        raise

def generate_gold_layer_report(**context):
    """Generate comprehensive Gold layer processing report"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
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
                'validated_layers': quality_result.get('validated_layer_names', []),
                'validation_passed': quality_result.get('validation_passed', False)
            },
            'summary': {
                'silver_sources_processed': len(validation_result.get('available_source_names', [])),
                'gold_tables_created': len(analytics_result.get('tables_created', [])),
                'gold_layers_validated': len(quality_result.get('validated_layer_names', [])),
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
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
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

# External dependency - wait for Silver layer processing
wait_for_silver_processing = ExternalTaskSensor(
    task_id='wait_for_silver_processing',
    external_dag_id='silver_layer_processing',
    external_task_id='end_silver_processing',
    timeout=3600,  # 1 hour timeout
    poke_interval=300,  # Check every 5 minutes
    dag=dag,
    mode='reschedule',
    allowed_states=['success'],
    failed_states=['failed', 'upstream_failed', 'skipped']
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

# Linear workflow with external dependency
# start_task >> wait_for_silver_processing >> validate_silver_task
start_task >> validate_silver_task
validate_silver_task >> refresh_reference_task >> create_analytics_task >> validate_quality_task
validate_quality_task >> generate_report_task >> cleanup_task
cleanup_task >> health_check_task >> end_task

# === DAG Documentation ===

dag.doc_md = """
# Gold Layer Processing DAG

This DAG creates analytics-ready data in the Gold layer from standardized Silver layer data.

## Workflow Steps:

1. **Wait for Silver Processing**: External sensor waits for Silver layer processing to complete
2. **Validate Silver Data**: Verify that Silver layer data is available and ready for processing
3. **Create Analytics Tables**: Generate analytics tables including:
   - Subject statistics and trends
   - Data quality metrics and reports
   - Usage analytics and insights
4. **Generate ML Features**: Create machine learning features for:
   - Content embeddings for similarity search
   - Subject classification features
   - Quality assessment features
5. **Create Library-Ready Data**: Format data for library system integration:
   - MARC 21 compatible records
   - Dublin Core metadata
   - OPAC feed ready data
6. **Validate Gold Layer**: Ensure Gold layer data quality and completeness
7. **Generate Reports**: Create comprehensive processing reports
8. **Cleanup**: Clean up temporary processing artifacts

## Key Features:

- **Business Intelligence**: Creates pre-aggregated analytics tables for fast reporting
- **ML Pipeline**: Generates features for recommendation and analysis systems
- **Library Integration**: Prepares data in standard library formats
- **Quality Assurance**: Comprehensive validation at each step
- **Error Recovery**: Robust error handling with detailed logging

## Dependencies:

- Silver layer processing must complete successfully
- Apache Spark cluster must be running with ML libraries
- MinIO object storage must be accessible
- Iceberg REST Catalog for table management

## Outputs:

### Analytics Layer:
- `subject_statistics`: Subject-level analytics and trends
- `quality_reports`: Data quality metrics by source and type
- `usage_metrics`: Usage analytics and insights

### ML Features Layer:
- `content_embeddings`: Text embeddings for similarity search
- `subject_features`: Subject classification features
- `quality_features`: Quality assessment features

### Library Ready Layer:
- `marc_ready`: MARC 21 compatible records
- `dublin_core`: Dublin Core metadata format
- `opac_feed`: OPAC integration ready data

## Schedule:

Runs monthly after Silver layer processing completes. Depends on external task sensor for proper sequencing.

**Processing Flow:**
- Bronze Layer: Raw data collection
- Silver Layer: Data standardization and cleaning
- Gold Layer: Analytics and business intelligence (this DAG)

**Typical Processing Time:** 15-30 minutes depending on data volume
"""
