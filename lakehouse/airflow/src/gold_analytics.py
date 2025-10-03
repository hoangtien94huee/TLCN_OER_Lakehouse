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

# Type hints
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyspark.sql import SparkSession

class GoldAnalyticsStandalone:
    """Standalone Gold layer analytics generator using Spark + Iceberg"""
    
    def __init__(self):
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        self.spark = self._create_spark_session() if SPARK_AVAILABLE else None
        
        # Table configuration
        self.catalog_name = "lakehouse"
        self.database_name = "default"
        self.silver_table = f"{self.catalog_name}.{self.database_name}.oer_resources_dc"
        self.gold_database = "gold"
        
        print(f"Gold Analytics initialized")
        
        if self.spark:
            self._create_gold_database()
    
    def _create_spark_session(self) -> Optional["SparkSession"]:
        """Create Spark session with Iceberg configuration"""
        if not SPARK_AVAILABLE:
            print("Spark not available, cannot create session")
            return None
            
        try:
            spark = SparkSession.builder \
                .appName("OER-Gold-Analytics") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.lakehouse.type", "rest") \
                .config("spark.sql.catalog.lakehouse.uri", os.getenv('ICEBERG_REST_URI', 'http://iceberg-rest:8181')) \
                .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY', 'minioadmin')) \
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY', 'minioadmin')) \
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT', 'http://minio:9000')) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            print("Spark session created for Gold layer")
            return spark
            
        except Exception as e:
            print(f"Spark session creation failed: {e}")
            return None
    
    def _create_gold_database(self):
        """Create gold database if it doesn't exist"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.gold_database}")
            print(f"Gold database {self.catalog_name}.{self.gold_database} ready")
        except Exception as e:
            print(f"Gold database creation warning: {e}")
    
    def create_source_summary(self):
        """Create summary table by source system"""
        try:
            print("[gold] Creating source summary table...")

            summary_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.source_summary
                USING iceberg
                AS
                WITH base AS (
                    SELECT *
                    FROM {self.silver_table}
                ),
                subject_counts AS (
                    SELECT source_system, COUNT(DISTINCT subject) AS unique_subjects
                    FROM base
                    LATERAL VIEW explode(dc_subject) AS subject
                    WHERE subject IS NOT NULL AND subject != ''
                    GROUP BY source_system
                ),
                creator_counts AS (
                    SELECT source_system, COUNT(DISTINCT creator) AS unique_creators
                    FROM base
                    LATERAL VIEW explode(dc_creator) AS creator
                    WHERE creator IS NOT NULL AND creator != ''
                    GROUP BY source_system
                )
                SELECT
                    b.source_system,
                    COUNT(*) AS total_resources,
                    AVG(b.quality_score) AS avg_quality_score,
                    COALESCE(MAX(sc.unique_subjects), 0) AS unique_subjects,
                    COALESCE(MAX(cc.unique_creators), 0) AS unique_creators,
                    SUM(CASE WHEN lower(b.dc_rights) LIKE '%cc%' THEN 1 ELSE 0 END) AS creative_commons_count,
                    MIN(b.ingested_at) AS first_ingested,
                    MAX(b.ingested_at) AS last_ingested,
                    CURRENT_TIMESTAMP() AS analytics_generated_at
                FROM base b
                LEFT JOIN subject_counts sc ON b.source_system = sc.source_system
                LEFT JOIN creator_counts cc ON b.source_system = cc.source_system
                GROUP BY b.source_system
            """

            self.spark.sql(summary_sql)
            print("[gold] Source summary table created")

        except Exception as e:
            print(f"[gold] Error creating source summary: {e}")

    def create_subject_analysis(self):
        """Create subject-based analysis table"""
        try:
            print("[gold] Creating subject analysis table...")

            subject_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.subject_analysis
                USING iceberg
                AS
                WITH exploded AS (
                    SELECT
                        dc_identifier,
                        dc_title,
                        source_system,
                        dc_type,
                        quality_score,
                        subject
                    FROM {self.silver_table}
                    LATERAL VIEW explode(dc_subject) AS subject
                    WHERE subject IS NOT NULL AND subject != ''
                )
                SELECT
                    subject,
                    source_system,
                    dc_type,
                    COUNT(*) AS resource_count,
                    AVG(quality_score) AS avg_quality,
                    collect_list(dc_title) AS sample_titles,
                    CURRENT_TIMESTAMP() AS analytics_generated_at
                FROM exploded
                GROUP BY subject, source_system, dc_type
                HAVING COUNT(*) >= 2
                ORDER BY resource_count DESC
            """

            self.spark.sql(subject_sql)
            print("[gold] Subject analysis table created")

        except Exception as e:
            print(f"[gold] Error creating subject analysis: {e}")

    def create_quality_metrics(self):
        """Create data quality metrics table"""
        try:
            print("[gold] Creating quality metrics table...")

            quality_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.quality_metrics
                USING iceberg
                AS
                SELECT
                    source_system,
                    COUNT(*) AS total_resources,
                    SUM(CASE WHEN quality_score >= 0.8 THEN 1 ELSE 0 END) AS high_quality_count,
                    SUM(CASE WHEN quality_score >= 0.6 AND quality_score < 0.8 THEN 1 ELSE 0 END) AS medium_quality_count,
                    SUM(CASE WHEN quality_score < 0.6 THEN 1 ELSE 0 END) AS low_quality_count,
                    AVG(quality_score) AS avg_quality_score,
                    MIN(quality_score) AS min_quality_score,
                    MAX(quality_score) AS max_quality_score,
                    SUM(CASE WHEN dc_title IS NOT NULL AND dc_title != '' THEN 1 ELSE 0 END) AS title_completeness,
                    SUM(CASE WHEN dc_description IS NOT NULL AND dc_description != '' THEN 1 ELSE 0 END) AS description_completeness,
                    SUM(CASE WHEN size(dc_creator) > 0 THEN 1 ELSE 0 END) AS creator_completeness,
                    SUM(CASE WHEN size(dc_subject) > 0 THEN 1 ELSE 0 END) AS subject_completeness,
                    CURRENT_TIMESTAMP() AS metrics_generated_at
                FROM {self.silver_table}
                GROUP BY source_system
            """

            self.spark.sql(quality_sql)
            print("[gold] Quality metrics table created")

        except Exception as e:
            print(f"[gold] Error creating quality metrics: {e}")

    def create_author_analysis(self):
        """Create author productivity analysis"""
        try:
            print("[gold] Creating author analysis table...")

            author_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.author_analysis
                USING iceberg
                AS
                WITH expanded AS (
                    SELECT
                        dc_identifier,
                        source_system,
                        dc_type,
                        quality_score,
                        ingested_at,
                        creator,
                        subject
                    FROM {self.silver_table}
                    LATERAL VIEW explode(dc_creator) AS creator
                    LATERAL VIEW explode_outer(dc_subject) AS subject
                    WHERE creator IS NOT NULL AND creator != ''
                )
                SELECT
                    creator AS author,
                    source_system,
                    COUNT(DISTINCT dc_identifier) AS resource_count,
                    collect_set(dc_type) AS formats,
                    collect_set(subject) FILTER (WHERE subject IS NOT NULL AND subject != '') AS subjects_covered,
                    AVG(quality_score) AS avg_quality,
                    MIN(ingested_at) AS first_resource,
                    MAX(ingested_at) AS latest_resource,
                    CURRENT_TIMESTAMP() AS analytics_generated_at
                FROM expanded
                GROUP BY creator, source_system
            """

            self.spark.sql(author_sql)
            print("[gold] Author analysis table created")

        except Exception as e:
            print(f"[gold] Error creating author analysis: {e}")

    def create_ml_features(self):
        """Create ML feature table for recommendations"""
        try:
            print("[gold] Creating ML features table...")

            ml_features_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.ml_features
                USING iceberg
                AS
                SELECT
                    dc_identifier,
                    dc_title,
                    dc_description,
                    source_system,
                    dc_type,
                    dc_format,
                    dc_language,
                    quality_score,
                    length(dc_title) AS title_length,
                    length(dc_description) AS description_length,
                    size(split(coalesce(dc_description, ''), ' ')) AS description_word_count,
                    size(dc_subject) AS subject_count,
                    size(dc_creator) AS creator_count,
                    dc_subject,
                    dc_creator,
                    CASE WHEN dc_publisher IS NOT NULL AND dc_publisher != '' THEN 1 ELSE 0 END AS has_publisher,
                    CASE WHEN lower(dc_rights) LIKE '%cc%' THEN 1 ELSE 0 END AS is_creative_commons,
                    size(dc_relation) AS relation_count,
                    datediff(current_date(), to_date(dc_date)) AS days_since_reference_date,
                    ingested_at,
                    bronze_object,
                    CURRENT_TIMESTAMP() AS features_generated_at
                FROM {self.silver_table}
            """

            self.spark.sql(ml_features_sql)
            print("[gold] ML features table created")

        except Exception as e:
            print(f"[gold] Error creating ML features: {e}")

    def create_library_export(self):
        """Create table optimized for library integration"""
        try:
            print("[gold] Creating library export table...")

            library_sql = f"""
                CREATE OR REPLACE TABLE {self.catalog_name}.{self.gold_database}.library_export
                USING iceberg
                AS
                SELECT
                    dc_identifier,
                    dc_title,
                    dc_description,
                    dc_creator,
                    dc_contributor,
                    dc_subject,
                    dc_publisher,
                    dc_date,
                    dc_type,
                    dc_format,
                    dc_language,
                    dc_rights,
                    dc_source,
                    source_system,
                    dc_relation,
                    quality_score,
                    struct(
                        dc_title AS dc_title,
                        element_at(dc_creator, 1) AS dc_creator,
                        dc_description AS dc_description,
                        dc_subject AS dc_subject,
                        dc_publisher AS dc_publisher,
                        dc_date AS dc_date,
                        dc_type AS dc_type,
                        dc_language AS dc_language,
                        dc_rights AS dc_rights,
                        dc_identifier AS dc_identifier
                    ) AS dublin_core_fields,
                    CURRENT_TIMESTAMP() AS export_generated_at
                FROM {self.silver_table}
                WHERE quality_score >= 0.5
            """

            self.spark.sql(library_sql)
            print("[gold] Library export table created")

        except Exception as e:
            print(f"[gold] Error creating library export: {e}")

    def generate_analytics_report(self):
        """Generate and display analytics report"""
        try:
            print()
            print("GOLD LAYER ANALYTICS REPORT")
            print("=" * 50)
            print()
            print("SOURCE SUMMARY:")
            source_df = self.spark.sql(
                f"SELECT * FROM {self.catalog_name}.{self.gold_database}.source_summary ORDER BY total_resources DESC"
            )
            source_df.show(truncate=False)
            print()
            print("TOP SUBJECTS:")
            subject_df = self.spark.sql(
                f"""
                SELECT subject, SUM(resource_count) AS total_count
                FROM {self.catalog_name}.{self.gold_database}.subject_analysis
                GROUP BY subject
                ORDER BY total_count DESC
                LIMIT 10
                """
            )
            subject_df.show(truncate=False)
            print()
            print("QUALITY OVERVIEW:")
            quality_df = self.spark.sql(
                f"SELECT * FROM {self.catalog_name}.{self.gold_database}.quality_metrics"
            )
            quality_df.show(truncate=False)
        except Exception as e:
            print(f"Error generating report: {e}")

    def run(self):
        """Main execution function"""
        print("Starting Gold layer analytics generation...")

        if not self.spark:
            print("Spark not available, cannot proceed")
            return

        try:
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.catalog_name}.{self.database_name}"
            ).collect()
            silver_exists = any(getattr(row, 'tableName', '') == 'oer_resources_dc' for row in tables)

            if not silver_exists:
                print("Silver layer table not found. Run silver_transform.py first.")
                return

            self.create_source_summary()
            self.create_subject_analysis()
            self.create_quality_metrics()
            self.create_author_analysis()
            self.create_ml_features()
            self.create_library_export()

            self.generate_analytics_report()

            print()
            print("Gold layer analytics generation completed!")
            print("Available Gold tables:")
            print(f"  - {self.catalog_name}.{self.gold_database}.source_summary")
            print(f"  - {self.catalog_name}.{self.gold_database}.subject_analysis")
            print(f"  - {self.catalog_name}.{self.gold_database}.quality_metrics")
            print(f"  - {self.catalog_name}.{self.gold_database}.author_analysis")
            print(f"  - {self.catalog_name}.{self.gold_database}.ml_features")
            print(f"  - {self.catalog_name}.{self.gold_database}.library_export")

        except Exception as e:
            print(f"Error in Gold layer processing: {e}")

        finally:
            if self.spark:
                self.spark.stop()

def main():
    """Entry point for standalone execution"""
    analytics = GoldAnalyticsStandalone()
    analytics.run()

if __name__ == "__main__":
    main()

