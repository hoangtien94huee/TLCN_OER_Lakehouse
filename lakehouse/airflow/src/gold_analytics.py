#!/usr/bin/env python3
"""
Gold Layer Analytics - Silver to Gold
=====================================

Standalone script to create analytics tables and ML features from Silver layer.
Based on building-lakehouse pattern.
"""

import os
from datetime import datetime
from typing import Optional

# Spark imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("Warning: PySpark not available")

class GoldAnalyticsStandalone:
    """Standalone Gold layer analytics generator using Spark + Iceberg"""
    
    def __init__(self):
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        self.spark = self._create_spark_session() if SPARK_AVAILABLE else None
        
        # Table configuration
        self.catalog_name = "lakehouse"
        self.database_name = "default"
        self.silver_table = f"{self.catalog_name}.{self.database_name}.oer_resources"
        self.gold_database = "gold"
        
        print(f"🚀 Gold Analytics initialized")
        
        if self.spark:
            self._create_gold_database()
    
    def _create_spark_session(self) -> Optional[SparkSession]:
        """Create Spark session with Iceberg configuration"""
        try:
            spark = SparkSession.builder \
                .appName("OER-Gold-Analytics") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.lakehouse.type", "hive") \
                .config("spark.sql.catalog.lakehouse.uri", os.getenv('HIVE_METASTORE_URI', 'thrift://hive-metastore:9083')) \
                .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{self.bucket}/gold/") \
                .config("spark.sql.catalog.lakehouse.s3.endpoint", os.getenv('MINIO_ENDPOINT', 'http://minio:9000')) \
                .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY', 'minioadmin')) \
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY', 'minioadmin')) \
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT', 'http://minio:9000')) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            print("✅ Spark session created for Gold layer")
            return spark
            
        except Exception as e:
            print(f"❌ Spark session creation failed: {e}")
            return None
    
    def _create_gold_database(self):
        """Create gold database if it doesn't exist"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.gold_database}")
            print(f"✅ Gold database {self.catalog_name}.{self.gold_database} ready")
        except Exception as e:
            print(f"⚠️ Gold database creation warning: {e}")
    
    def create_source_summary(self):
        """Create summary table by source"""
        try:
            print("📊 Creating source summary table...")
            
            summary_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.source_summary
                USING iceberg
                AS
                SELECT 
                    source,
                    count(*) as total_resources,
                    avg(quality_score) as avg_quality_score,
                    count(DISTINCT subjects) as unique_subjects,
                    count(DISTINCT authors) as unique_authors,
                    count(CASE WHEN license LIKE '%CC%' THEN 1 END) as creative_commons_count,
                    min(created_at) as first_scraped,
                    max(updated_at) as last_updated,
                    current_timestamp() as analytics_generated_at
                FROM {self.silver_table}
                GROUP BY source
            """
            
            self.spark.sql(summary_sql)
            print("✅ Source summary table created")
            
        except Exception as e:
            print(f"❌ Error creating source summary: {e}")
    
    def create_subject_analysis(self):
        """Create subject-based analysis table"""
        try:
            print("📚 Creating subject analysis table...")
            
            subject_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.subject_analysis
                USING iceberg
                AS
                SELECT 
                    explode(subjects) as subject,
                    source,
                    format,
                    count(*) as resource_count,
                    avg(quality_score) as avg_quality,
                    collect_list(title) as sample_titles,
                    current_timestamp() as analytics_generated_at
                FROM {self.silver_table}
                WHERE size(subjects) > 0
                GROUP BY explode(subjects), source, format
                HAVING count(*) >= 2
                ORDER BY resource_count DESC
            """
            
            self.spark.sql(subject_sql)
            print("✅ Subject analysis table created")
            
        except Exception as e:
            print(f"❌ Error creating subject analysis: {e}")
    
    def create_quality_metrics(self):
        """Create data quality metrics table"""
        try:
            print("🎯 Creating quality metrics table...")
            
            quality_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.quality_metrics
                USING iceberg
                AS
                SELECT 
                    source,
                    count(*) as total_resources,
                    sum(CASE WHEN quality_score >= 0.8 THEN 1 ELSE 0 END) as high_quality_count,
                    sum(CASE WHEN quality_score >= 0.6 THEN 1 ELSE 0 END) as medium_quality_count,
                    sum(CASE WHEN quality_score < 0.6 THEN 1 ELSE 0 END) as low_quality_count,
                    avg(quality_score) as avg_quality_score,
                    min(quality_score) as min_quality_score,
                    max(quality_score) as max_quality_score,
                    
                    -- Completeness metrics
                    sum(CASE WHEN title IS NOT NULL AND title != '' THEN 1 ELSE 0 END) as title_completeness,
                    sum(CASE WHEN description IS NOT NULL AND description != '' THEN 1 ELSE 0 END) as description_completeness,
                    sum(CASE WHEN size(authors) > 0 THEN 1 ELSE 0 END) as authors_completeness,
                    sum(CASE WHEN size(subjects) > 0 THEN 1 ELSE 0 END) as subjects_completeness,
                    
                    current_timestamp() as metrics_generated_at
                FROM {self.silver_table}
                GROUP BY source
            """
            
            self.spark.sql(quality_sql)
            print("✅ Quality metrics table created")
            
        except Exception as e:
            print(f"❌ Error creating quality metrics: {e}")
    
    def create_author_analysis(self):
        """Create author productivity analysis"""
        try:
            print("👥 Creating author analysis table...")
            
            author_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.author_analysis
                USING iceberg
                AS
                SELECT 
                    explode(authors) as author,
                    source,
                    count(*) as resource_count,
                    collect_list(DISTINCT format) as formats,
                    collect_list(DISTINCT explode(subjects)) as subjects_taught,
                    avg(quality_score) as avg_quality,
                    min(created_at) as first_resource,
                    max(updated_at) as latest_resource,
                    current_timestamp() as analytics_generated_at
                FROM {self.silver_table}
                WHERE size(authors) > 0
                GROUP BY explode(authors), source
                HAVING count(*) >= 2
                ORDER BY resource_count DESC
            """
            
            self.spark.sql(author_sql)
            print("✅ Author analysis table created")
            
        except Exception as e:
            print(f"❌ Error creating author analysis: {e}")
    
    def create_ml_features(self):
        """Create ML feature table for recommendations"""
        try:
            print("🤖 Creating ML features table...")
            
            ml_features_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.ml_features
                USING iceberg
                AS
                SELECT 
                    id,
                    title,
                    description,
                    source,
                    format,
                    language,
                    quality_score,
                    
                    -- Text features
                    length(title) as title_length,
                    length(description) as description_length,
                    size(split(description, ' ')) as description_word_count,
                    
                    -- Content features
                    size(subjects) as subject_count,
                    size(authors) as author_count,
                    subjects,
                    authors,
                    
                    -- Metadata features
                    CASE WHEN isbn IS NOT NULL AND isbn != '' THEN 1 ELSE 0 END as has_isbn,
                    CASE WHEN publisher IS NOT NULL AND publisher != '' THEN 1 ELSE 0 END as has_publisher,
                    CASE WHEN license LIKE '%CC%' THEN 1 ELSE 0 END as is_creative_commons,
                    
                    -- Download features
                    size(download_links) as download_options_count,
                    
                    -- Temporal features
                    datediff(current_date(), to_date(created_at)) as days_since_created,
                    
                    current_timestamp() as features_generated_at
                FROM {self.silver_table}
            """
            
            self.spark.sql(ml_features_sql)
            print("✅ ML features table created")
            
        except Exception as e:
            print(f"❌ Error creating ML features: {e}")
    
    def create_library_export(self):
        """Create table optimized for library integration"""
        try:
            print("📖 Creating library export table...")
            
            library_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.library_export
                USING iceberg
                AS
                SELECT 
                    id,
                    title,
                    description,
                    authors,
                    subjects,
                    publisher,
                    isbn,
                    url,
                    license,
                    format,
                    language,
                    source,
                    publication_date,
                    
                    -- MARC fields mapping
                    CASE 
                        WHEN format = 'textbook' THEN 'Book'
                        WHEN format = 'course' THEN 'Educational Resource'
                        ELSE 'Online Resource'
                    END as material_type,
                    
                    -- Dublin Core mapping
                    struct(
                        title as dc_title,
                        authors[0] as dc_creator,
                        description as dc_description,
                        subjects as dc_subject,
                        publisher as dc_publisher,
                        publication_date as dc_date,
                        format as dc_type,
                        language as dc_language,
                        license as dc_rights,
                        url as dc_identifier
                    ) as dublin_core_fields,
                    
                    quality_score,
                    current_timestamp() as export_generated_at
                FROM {self.silver_table}
                WHERE quality_score >= 0.5  -- Only export quality resources
            """
            
            self.spark.sql(library_sql)
            print("✅ Library export table created")
            
        except Exception as e:
            print(f"❌ Error creating library export: {e}")
    
    def generate_analytics_report(self):
        """Generate and display analytics report"""
        try:
            print("\n📈 GOLD LAYER ANALYTICS REPORT")
            print("=" * 50)
            
            # Source summary
            print("\n🗂️  SOURCE SUMMARY:")
            source_df = self.spark.sql(f"SELECT * FROM {self.catalog_name}.{self.gold_database}.source_summary ORDER BY total_resources DESC")
            source_df.show(truncate=False)
            
            # Top subjects
            print("\n📚 TOP SUBJECTS:")
            subject_df = self.spark.sql(f"""
                SELECT subject, sum(resource_count) as total_count 
                FROM {self.catalog_name}.{self.gold_database}.subject_analysis 
                GROUP BY subject 
                ORDER BY total_count DESC 
                LIMIT 10
            """)
            subject_df.show(truncate=False)
            
            # Quality overview
            print("\n🎯 QUALITY OVERVIEW:")
            quality_df = self.spark.sql(f"SELECT * FROM {self.catalog_name}.{self.gold_database}.quality_metrics")
            quality_df.show(truncate=False)
            
        except Exception as e:
            print(f"⚠️ Error generating report: {e}")
    
    def run(self):
        """Main execution function"""
        print("🚀 Starting Gold layer analytics generation...")
        
        if not self.spark:
            print("❌ Spark not available, cannot proceed")
            return
        
        try:
            # Check if Silver table exists
            tables = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.database_name}").collect()
            silver_exists = any('oer_resources' in str(row) for row in tables)
            
            if not silver_exists:
                print("❌ Silver layer table not found. Run silver_transform.py first.")
                return
            
            # Create all Gold layer tables
            self.create_source_summary()
            self.create_subject_analysis()
            self.create_quality_metrics()
            self.create_author_analysis()
            self.create_ml_features()
            self.create_library_export()
            
            # Generate report
            self.generate_analytics_report()
            
            print(f"\n✅ Gold layer analytics generation completed!")
            print(f"📊 Available Gold tables:")
            print(f"  - {self.catalog_name}.{self.gold_database}.source_summary")
            print(f"  - {self.catalog_name}.{self.gold_database}.subject_analysis")
            print(f"  - {self.catalog_name}.{self.gold_database}.quality_metrics")
            print(f"  - {self.catalog_name}.{self.gold_database}.author_analysis")
            print(f"  - {self.catalog_name}.{self.gold_database}.ml_features")
            print(f"  - {self.catalog_name}.{self.gold_database}.library_export")
            
        except Exception as e:
            print(f"❌ Error in Gold layer processing: {e}")
        
        finally:
            if self.spark:
                self.spark.stop()

def main():
    """Entry point for standalone execution"""
    analytics = GoldAnalyticsStandalone()
    analytics.run()

if __name__ == "__main__":
    main()

