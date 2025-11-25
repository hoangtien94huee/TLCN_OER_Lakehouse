#!/usr/bin/env python3
"""
Database Schema Creator - Setup Lakehouse Schemas
==================================================

Standalone script to create database schemas and tables for OER Lakehouse.
Based on building-lakehouse pattern.
"""

import os
import time
from typing import Optional

# MinIO imports
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    print("Warning: MinIO library not found")

# Spark imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("Warning: PySpark not available")

class SchemaCreatorStandalone:
    """Standalone schema creator for OER Lakehouse"""
    
    def __init__(self):
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        self.spark = self._create_spark_session() if SPARK_AVAILABLE else None
        self.minio_client = self._setup_minio() if MINIO_AVAILABLE else None
        
        # Catalog configuration
        self.catalog_name = "lakehouse"
        self.default_db = "default"
        self.gold_db = "gold"
        
        print(f"Schema Creator initialized")
    
    def _create_spark_session(self) -> Optional[SparkSession]:
        """Create Spark session with Iceberg configuration - optimized for minimal Airflow load"""
        try:
            builder = (
                SparkSession.builder.appName("OER-Schema-Creator")
                .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            )

            packages = os.getenv("SPARK_JARS_PACKAGES")
            if packages:
                builder = builder.config("spark.jars.packages", packages)
                print(f"[Schema Creator] Using Spark packages: {packages}")

            python_exec = os.getenv("PYSPARK_PYTHON")
            if python_exec:
                builder = builder.config("spark.pyspark.python", python_exec)
            driver_python = os.getenv("PYSPARK_DRIVER_PYTHON")
            if driver_python:
                builder = builder.config("spark.pyspark.driver.python", driver_python)

            spark = (
                builder.config("spark.driver.memory", "256m")
                .config("spark.driver.maxResultSize", "128m")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.default.parallelism", "2")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate()
            )
            
            spark.sparkContext.setLogLevel("WARN")
            print("Spark session created for schema operations")
            return spark
            
        except Exception as e:
            print(f"Spark session creation failed: {e}")
            return None
    
    def _setup_minio(self) -> Optional[Minio]:
        """Setup MinIO client and wait for availability"""
        try:
            endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
            access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
            secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
            secure = os.getenv('MINIO_SECURE', '0') == '1'
            
            # Remove http:// prefix if present
            if endpoint.startswith('http://'):
                endpoint = endpoint[7:]
            elif endpoint.startswith('https://'):
                endpoint = endpoint[8:]
                secure = True
            
            client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
            
            # Wait for MinIO to be available
            max_retries = 10
            for i in range(max_retries):
                try:
                    client.list_buckets()
                    print(f"MinIO client connected successfully")
                    return client
                except Exception as e:
                    if i < max_retries - 1:
                        print(f"Waiting for MinIO... (attempt {i+1}/{max_retries})")
                        time.sleep(2)
                    else:
                        print(f"MinIO connection failed after {max_retries} attempts: {e}")
                        return None
            
        except Exception as e:
            print(f"MinIO setup failed: {e}")
            return None
    
    def create_minio_structure(self):
        """Create MinIO bucket and lakehouse directory structure"""
        if not self.minio_client:
            print("MinIO client not available, skipping bucket creation")
            return False
        
        try:
            print("Creating MinIO lakehouse structure...")
            
            # Create bucket if not exists
            if not self.minio_client.bucket_exists(self.bucket):
                self.minio_client.make_bucket(self.bucket)
                print(f"Bucket {self.bucket} created")
            else:
                print(f"Bucket {self.bucket} already exists")
            
            # Create directory structure by uploading marker files
            directories = [
                "bronze/",
                "bronze/mit_ocw/",
                "bronze/openstax/",
                "bronze/otl/",
                "silver/",
                "silver/oer_resources_dc/",
                "silver/oer_subjects/",
                "silver/oer_relations/",
                "silver/oer_multimedia/",
                "silver/oer_quality_audit/",
                "silver/oer_creators/",
                "silver/oer_history/",
                "silver/oer_language_normalized/",
                "gold/",
                "gold/analytics/",
                "gold/ml_features/"
            ]
            
            for directory in directories:
                try:
                    # Create a marker file to ensure directory exists
                    marker_content = f"Directory marker for {directory}\nCreated at: {time.strftime('%Y-%m-%d %H:%M:%S')}"
                    
                    from io import BytesIO
                    marker_data = BytesIO(marker_content.encode())
                    
                    self.minio_client.put_object(
                        bucket_name=self.bucket,
                        object_name=f"{directory}.keep",
                        data=marker_data,
                        length=len(marker_content)
                    )
                    print(f"Created directory: {directory}")
                    
                except Exception as e:
                    print(f"Warning: Could not create directory {directory}: {e}")
            
            print("MinIO lakehouse structure created successfully")
            return True
            
        except Exception as e:
            print(f"Error creating MinIO structure: {e}")
            return False
    
    def create_databases(self):
        """Create lakehouse databases"""
        if not self.spark:
            return False
        
        try:
            print(" Creating databases...")
            
            # Default database for Silver layer
            self.spark.sql(f"""
                CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.default_db}
                COMMENT 'Default database for Silver layer OER resources'
            """)
            print(f" Database {self.catalog_name}.{self.default_db} created")
            
            # Gold database for Analytics
            self.spark.sql(f"""
                CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.gold_db}
                COMMENT 'Gold database for OER analytics and ML features'
            """)
            print(f" Database {self.catalog_name}.{self.gold_db} created")
            
            return True
            
        except Exception as e:
            print(f" Error creating databases: {e}")
            return False
    
    def create_silver_schema(self):
        """Create Silver layer main table schema"""
        if not self.spark:
            return False
        
        try:
            print(" Creating Silver layer schema...")
            
            # Main OER resources table
            silver_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.default_db}.oer_resources_dc (
                    dc_identifier STRING COMMENT 'Primary identifier (URL or canonical ID)',
                    dc_title STRING COMMENT 'Resource title',
                    dc_creator ARRAY<STRING> COMMENT 'Creators or instructors',
                    dc_subject ARRAY<STRING> COMMENT 'Subject areas and topics',
                    dc_description STRING COMMENT 'Resource description',
                    dc_publisher STRING COMMENT 'Publishing organization or source',
                    dc_contributor ARRAY<STRING> COMMENT 'Additional contributors',
                    dc_date STRING COMMENT 'Key date in ISO format',
                    dc_type STRING COMMENT 'DCMI type classification',
                    dc_format STRING COMMENT 'Resource format or MIME type',
                    dc_source STRING COMMENT 'Collection or system of origin',
                    dc_language STRING COMMENT 'Primary language (ISO code)',
                    dc_relation ARRAY<STRING> COMMENT 'Related resource URLs',
                    dc_coverage STRING COMMENT 'Spatial or temporal coverage',
                    dc_rights STRING COMMENT 'Rights statement or license',
                    source_system STRING COMMENT 'Originating scraper identifier',
                    bronze_object STRING COMMENT 'Source bronze object path',
                    ingested_at TIMESTAMP COMMENT 'Record ingestion timestamp',
                    quality_score DOUBLE COMMENT 'Data quality score (0.0-1.0)'
                )
                USING iceberg
                PARTITIONED BY (source_system)
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.metadata.delete-after-commit.enabled' = 'true',
                    'write.metadata.previous-versions-max' = '5'
                )
                COMMENT 'Silver layer table storing Dublin Core records'
            """
            
            self.spark.sql(silver_table_sql)
            print(" Silver layer oer_resources_dc table created")
            
            # Create indexes for better query performance
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.default_db}.oer_subjects (
                    dc_subject STRING,
                    dc_identifier STRING,
                    source_system STRING,
                    ingested_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (source_system)
                COMMENT 'Normalized subjects view of Dublin Core resources'
            """)
            print(" Silver layer oer_subjects table created")

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.default_db}.oer_relations (
                    dc_identifier STRING,
                    relation_url STRING,
                    relation_type STRING,
                    source_system STRING,
                    ingested_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (source_system)
                TBLPROPERTIES (
                    'format-version' = '2'
                )
                COMMENT 'Flattened related resources for Dublin Core records'
            """)
            print(" Silver layer oer_relations table created")

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.default_db}.oer_multimedia (
                    media_id STRING,
                    dc_identifier STRING,
                    media_type STRING,
                    title STRING,
                    url STRING,
                    transcript_available BOOLEAN,
                    size_mb DOUBLE,
                    source_system STRING,
                    ingested_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (source_system)
                TBLPROPERTIES (
                    'format-version' = '2'
                )
                COMMENT 'Multimedia artefacts associated with resources'
            """)
            print(" Silver layer oer_multimedia table created")

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.default_db}.oer_quality_audit (
                    dc_identifier STRING,
                    issue_code STRING,
                    severity STRING,
                    detail STRING,
                    source_system STRING,
                    detected_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (source_system)
                TBLPROPERTIES (
                    'format-version' = '2'
                )
                COMMENT 'Quality checks and issues detected during ingestion'
            """)
            print(" Silver layer oer_quality_audit table created")

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.default_db}.oer_creators (
                    dc_identifier STRING,
                    creator_name STRING,
                    normalized_name STRING,
                    role STRING,
                    source_system STRING,
                    ingested_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (source_system)
                TBLPROPERTIES (
                    'format-version' = '2'
                )
                COMMENT 'Normalized creator and contributor information'
            """)
            print(" Silver layer oer_creators table created")

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.default_db}.oer_history (
                    dc_identifier STRING,
                    bronze_object STRING,
                    record_checksum STRING,
                    source_system STRING,
                    ingested_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (source_system)
                TBLPROPERTIES (
                    'format-version' = '2'
                )
                COMMENT 'Change tracking for Bronze to Silver normalization'
            """)
            print(" Silver layer oer_history table created")

            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.default_db}.oer_language_normalized (
                    dc_identifier STRING,
                    raw_language STRING,
                    normalized_language STRING,
                    source_system STRING,
                    ingested_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (source_system)
                TBLPROPERTIES (
                    'format-version' = '2'
                )
                COMMENT 'Normalized language mappings for resources'
            """)
            print(" Silver layer oer_language_normalized table created")

            
            return True
            
        except Exception as e:
            print(f" Error creating Silver schema: {e}")
            return False
    
    def create_gold_schema(self):
        """Create Gold layer analytics table schemas"""
        if not self.spark:
            return False
        
        try:
            print(" Creating Gold layer schemas...")
            
            # Source summary table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.gold_db}.source_summary (
                    source STRING,
                    total_resources BIGINT,
                    avg_quality_score DOUBLE,
                    unique_subjects BIGINT,
                    unique_authors BIGINT,
                    creative_commons_count BIGINT,
                    first_scraped TIMESTAMP,
                    last_updated TIMESTAMP,
                    analytics_generated_at TIMESTAMP
                )
                USING iceberg
                COMMENT 'Summary statistics by source'
            """)
            print(" Gold source_summary table created")
            
            # Subject analysis table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.gold_db}.subject_analysis (
                    subject STRING,
                    source STRING,
                    format STRING,
                    resource_count BIGINT,
                    avg_quality DOUBLE,
                    sample_titles ARRAY<STRING>,
                    analytics_generated_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (source)
                COMMENT 'Subject-based resource analysis'
            """)
            print(" Gold subject_analysis table created")
            
            # Quality metrics table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.gold_db}.quality_metrics (
                    source STRING,
                    total_resources BIGINT,
                    high_quality_count BIGINT,
                    medium_quality_count BIGINT,
                    low_quality_count BIGINT,
                    avg_quality_score DOUBLE,
                    min_quality_score DOUBLE,
                    max_quality_score DOUBLE,
                    title_completeness BIGINT,
                    description_completeness BIGINT,
                    authors_completeness BIGINT,
                    subjects_completeness BIGINT,
                    metrics_generated_at TIMESTAMP
                )
                USING iceberg
                COMMENT 'Data quality metrics by source'
            """)
            print(" Gold quality_metrics table created")
            
            # ML features table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.gold_db}.ml_features (
                    id STRING,
                    title STRING,
                    description STRING,
                    source STRING,
                    format STRING,
                    language STRING,
                    quality_score DOUBLE,
                    title_length INT,
                    description_length INT,
                    description_word_count INT,
                    subject_count INT,
                    author_count INT,
                    subjects ARRAY<STRING>,
                    authors ARRAY<STRING>,
                    has_isbn BOOLEAN,
                    has_publisher BOOLEAN,
                    is_creative_commons BOOLEAN,
                    download_options_count INT,
                    days_since_created BIGINT,
                    features_generated_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (source, format)
                COMMENT 'ML features for recommendation engine'
            """)
            print(" Gold ml_features table created")
            
            # Library export table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.gold_db}.library_export (
                    id STRING,
                    title STRING,
                    description STRING,
                    authors ARRAY<STRING>,
                    subjects ARRAY<STRING>,
                    publisher STRING,
                    isbn STRING,
                    url STRING,
                    license STRING,
                    format STRING,
                    language STRING,
                    source STRING,
                    publication_date STRING,
                    material_type STRING,
                    dublin_core_fields STRUCT<
                        dc_title: STRING,
                        dc_creator: STRING,
                        dc_description: STRING,
                        dc_subject: ARRAY<STRING>,
                        dc_publisher: STRING,
                        dc_date: STRING,
                        dc_type: STRING,
                        dc_language: STRING,
                        dc_rights: STRING,
                        dc_identifier: STRING
                    >,
                    quality_score DOUBLE,
                    export_generated_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (source, format)
                COMMENT 'Optimized export for library integration'
            """)
            print(" Gold library_export table created")
            
            return True
            
        except Exception as e:
            print(f" Error creating Gold schemas: {e}")
            return False
    
    def create_system_tables(self):
        """Create system/monitoring tables"""
        if not self.spark:
            return False
        
        try:
            print(" Creating system tables...")
            
            # Pipeline execution log
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.default_db}.pipeline_log (
                    execution_id STRING,
                    pipeline_name STRING,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status STRING,
                    records_processed BIGINT,
                    error_message STRING,
                    created_at TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (DATE(start_time))
                COMMENT 'Pipeline execution log'
            """)
            print(" System pipeline_log table created")
            
            # Data quality log
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog_name}.{self.default_db}.quality_log (
                    check_id STRING,
                    table_name STRING,
                    check_type STRING,
                    check_result STRING,
                    passed_count BIGINT,
                    failed_count BIGINT,
                    check_timestamp TIMESTAMP,
                    details MAP<STRING, STRING>
                )
                USING iceberg
                PARTITIONED BY (DATE(check_timestamp))
                COMMENT 'Data quality check results'
            """)
            print(" System quality_log table created")
            
            return True
            
        except Exception as e:
            print(f" Error creating system tables: {e}")
            return False
    
    def show_schema_info(self):
        """Show information about created schemas"""
        if not self.spark:
            return
        
        try:
            print(f"\n LAKEHOUSE SCHEMA INFORMATION")
            print("=" * 50)
            
            # Show databases
            print(f"\n DATABASES:")
            dbs = self.spark.sql(f"SHOW DATABASES IN {self.catalog_name}").collect()
            for db in dbs:
                print(f"  - {self.catalog_name}.{db['namespace']}")
            
            # Show Silver tables
            print(f"\n SILVER LAYER TABLES:")
            silver_tables = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.default_db}").collect()
            for table in silver_tables:
                print(f"  - {table['tableName']}")
            
            # Show Gold tables
            print(f"\n GOLD LAYER TABLES:")
            try:
                gold_tables = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.gold_db}").collect()
                for table in gold_tables:
                    print(f"  - {table['tableName']}")
            except:
                print("  (Gold database not yet created)")
            
        except Exception as e:
            print(f" Error showing schema info: {e}")
    
    def run(self):
        """Main execution function"""
        print("Starting schema creation...")
        
        success = True
        
        # First, create MinIO structure (this doesn't require Spark)
        if not self.create_minio_structure():
            print("Warning: MinIO structure creation failed, continuing with Spark schemas...")
        
        # Then create Spark schemas if available
        if self.spark:
            # Create databases
            if not self.create_databases():
                success = False
            
            # Create Silver schemas
            if not self.create_silver_schema():
                success = False
            
            # Create Gold schemas
            if not self.create_gold_schema():
                success = False
            
            # Create system tables
            if not self.create_system_tables():
                success = False
            
            # Show schema information
            self.show_schema_info()
        else:
            print("Spark not available, skipping table schema creation")
        
        if success:
            print(f"\nAll schemas created successfully!")
            print(f"Ready for data processing:")
            print(f"  1. Run bronze_*.py scripts to collect data")
            print(f"  2. Run silver_transform.py to process to Silver layer")
            print(f"  3. Run gold_analytics.py to generate analytics")
        else:
            print(f"\nSome schemas may have failed to create")
        
        if self.spark:
            self.spark.stop()

def main():
    """Entry point for standalone execution"""
    creator = SchemaCreatorStandalone()
    creator.run()

if __name__ == "__main__":
    main()

