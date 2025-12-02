"""
SAF (Simple Archive Format) Exporter for DSpace Import
Exports Gold layer OER resources to SAF format for batch import into DSpace.

Simplified version - only generates dublin_core.xml (metadata only, no bitstreams).
Uses Iceberg 1.9.2 for reading from Gold layer.
"""

import os
import logging
import shutil
import zipfile
from typing import Optional
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SAFExporter:
    """Export Gold layer data to DSpace SAF (Simple Archive Format) - metadata only."""
    
    def __init__(self, output_dir: str = "/opt/airflow/saf_export"):
        """Initialize SAF Exporter."""
        self.output_dir = output_dir
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.minio_access = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.spark: Optional[SparkSession] = None
        
    def init_spark(self):
        """Initialize Spark session for reading Iceberg tables."""
        if self.spark is None:
            jars_dir = os.getenv("SPARK_JARS_DIR", "/opt/airflow/jars")
            
            # Iceberg 1.9.2 JAR files
            jar_files = [
                f"{jars_dir}/iceberg-spark-runtime-3.5_2.12-1.9.2.jar",
                f"{jars_dir}/hadoop-aws-3.3.4.jar",
                f"{jars_dir}/aws-java-sdk-bundle-1.12.262.jar"
            ]
            
            self.spark = (
                SparkSession.builder
                .appName("SAF_Exporter_Iceberg")
                .master("local[*]")
                .config("spark.jars", ",".join(jar_files))
                .config("spark.driver.extraClassPath", ",".join(jar_files))
                # Iceberg Catalog config
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.type", "hadoop")
                .config("spark.sql.catalog.iceberg.warehouse", f"s3a://{self.bucket}/warehouse")
                # S3/MinIO config
                .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
                .config("spark.hadoop.fs.s3a.access.key", self.minio_access)
                .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                # Memory settings
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .getOrCreate()
            )
            logger.info(f"Spark {self.spark.version} initialized with Iceberg 1.9.2")
            
    def create_dublin_core_xml(self, row: dict) -> str:
        """Create dublin_core.xml content for an item."""
        root = Element('dublin_core')
        root.set('schema', 'dc')
        
        def add_element(element: str, qualifier: str, value: str, language: str = None):
            """Add a dcvalue element."""
            if not value or value == 'None' or value == '':
                return
            dcvalue = SubElement(root, 'dcvalue')
            dcvalue.set('element', element)
            if qualifier:
                dcvalue.set('qualifier', qualifier)
            if language:
                dcvalue.set('language', language)
            dcvalue.text = str(value)
        
        # Title (required)
        title = row.get('title', 'Untitled')
        add_element('title', None, title)
        
        # Description/Abstract
        description = row.get('description')
        if description:
            add_element('description', 'abstract', description[:10000])  # Limit length
        
        # Creator/Author - from creator_names array
        authors = row.get('creator_names')
        if authors:
            if isinstance(authors, list):
                for auth in authors:
                    if auth:
                        add_element('contributor', 'author', auth)
            elif isinstance(authors, str):
                for auth in authors.split(','):
                    auth = auth.strip()
                    if auth:
                        add_element('contributor', 'author', auth)
        
        # Date Issued - default to current year
        add_element('date', 'issued', str(datetime.now().year))
        
        # Publisher
        publisher = row.get('publisher_name')
        if publisher:
            add_element('publisher', None, publisher)
        
        # Identifier - Source URL
        source_url = row.get('source_url')
        if source_url:
            add_element('identifier', 'uri', source_url)
        
        # Type - OER type
        add_element('type', None, 'Open Educational Resource')
        
        # License/Rights
        license_name = row.get('license_name')
        if license_name:
            add_element('rights', None, license_name)
        
        license_url = row.get('license_url')
        if license_url:
            add_element('rights', 'uri', license_url)
        
        # Subject - Matched curriculum subjects (KEY FEATURE: OER-to-Subject mapping)
        # Only Vietnamese subject names are exported
        subjects = row.get('matched_subjects')
        if subjects:
            if isinstance(subjects, list):
                for subj in subjects:
                    # Handle Spark Row objects and dicts
                    if hasattr(subj, 'asDict'):
                        subj = subj.asDict()
                    if isinstance(subj, dict):
                        # Add Vietnamese subject name only
                        subject_name = subj.get('subject_name')
                        if subject_name:
                            add_element('subject', None, subject_name)
                    else:
                        add_element('subject', None, str(subj))
        
        # Source - from bronze_source_path to derive source system
        bronze_source = row.get('bronze_source_path') or ''
        if 'mit_ocw' in bronze_source:
            add_element('source', None, 'MIT OpenCourseWare')
        elif 'openstax' in bronze_source:
            add_element('source', None, 'OpenStax')
        elif 'open_textbook_library' in bronze_source or 'otl' in bronze_source:
            add_element('source', None, 'Open Textbook Library')
        
        # Format XML nicely
        rough_string = tostring(root, encoding='unicode')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="  ", encoding=None)
    
    def read_gold_data(self, limit: int = None):
        """
        Read OER data from Gold layer Parquet.
        
        Gold layer schema (dim_oer_resources):
        - resource_key, resource_id, title, description, source_url
        - publisher_name, license_name, license_url
        - creator_names (array), matched_subjects (array of struct)
        - dc_xml_path, bronze_source_path
        """
        logger.info("Reading Gold layer data...")
        
        # Read directly from Gold Parquet (Iceberg 1.9.2)
        gold_oer_path = f"s3a://{self.bucket}/gold/analytics/dim_oer_resources/data/*.parquet"
        
        logger.info(f"Reading from: {gold_oer_path}")
        
        df = self.spark.read.parquet(gold_oer_path)
        
        # Filter valid records
        df = df.where(
            (df.title.isNotNull()) & 
            (df.title != "") &
            (df.title != "Untitled")
        )
        
        # DEDUPLICATE by resource_id - keep first occurrence
        # This is necessary because Gold layer may have duplicates from multiple runs
        logger.info("Deduplicating by resource_id...")
        df = df.dropDuplicates(["resource_id"])
        
        if limit:
            df = df.limit(limit)
            
        total_count = df.count()
        logger.info(f"Found {total_count} unique records in Gold layer")
        
        return df
    
    def export_to_saf(self, limit: int = None, create_zip: bool = True) -> str:
        """
        Export Gold layer data to SAF format (metadata only - dublin_core.xml).
        
        Args:
            limit: Optional limit on number of items to export
            create_zip: If True, create a ZIP file of the export (default: True)
            
        Returns:
            Path to the SAF export directory (or ZIP file if create_zip=True)
        """
        logger.info("="*60)
        logger.info("Starting SAF export with Iceberg 1.9.2...")
        logger.info("="*60)
        
        # Initialize Spark
        self.init_spark()
        
        # Clean output directory contents
        if os.path.exists(self.output_dir):
            for item in os.listdir(self.output_dir):
                item_path = os.path.join(self.output_dir, item)
                try:
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
                except Exception as e:
                    logger.warning(f"Could not remove {item_path}: {e}")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Read Gold layer data
        try:
            df = self.read_gold_data(limit=limit)
            
            # Collect records
            logger.info("Collecting records from Spark DataFrame...")
            records = [row.asDict() for row in df.collect()]
            logger.info(f"Successfully collected {len(records)} records")
            
        except Exception as e:
            logger.error(f"Failed to read Gold layer: {e}")
            import traceback
            traceback.print_exc()
            raise
        
        # Export each record to SAF format
        exported_count = 0
        errors = []
        
        for idx, row in enumerate(records):
            try:
                # Create item directory
                item_dir = os.path.join(self.output_dir, f"item_{idx+1:06d}")
                os.makedirs(item_dir, exist_ok=True)
                
                # Create dublin_core.xml
                dc_content = self.create_dublin_core_xml(row)
                dc_path = os.path.join(item_dir, "dublin_core.xml")
                with open(dc_path, 'w', encoding='utf-8') as f:
                    f.write(dc_content)
                
                # Create empty contents file (required by SAF format)
                contents_path = os.path.join(item_dir, "contents")
                with open(contents_path, 'w', encoding='utf-8') as f:
                    f.write('')  # Empty - no bitstreams
                
                exported_count += 1
                
                if (idx + 1) % 1000 == 0:
                    logger.info(f"Exported {idx + 1}/{len(records)} items...")
                    
            except Exception as e:
                error_msg = f"Error exporting item {idx}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        logger.info("="*60)
        logger.info(f"SAF export completed: {exported_count} items")
        logger.info(f"Output directory: {self.output_dir}")
        if errors:
            logger.warning(f"Encountered {len(errors)} errors during export")
        logger.info("="*60)
        
        # Create ZIP file if requested
        if create_zip:
            zip_path = f"{self.output_dir}.zip"
            logger.info(f"Creating ZIP archive: {zip_path}")
            
            try:
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for root, dirs, files in os.walk(self.output_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, self.output_dir)
                            zipf.write(file_path, arcname)
                
                zip_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
                logger.info(f" ZIP created: {zip_path} ({zip_size_mb:.2f} MB)")
                return zip_path
                
            except Exception as e:
                logger.error(f"Failed to create ZIP: {e}")
                return self.output_dir
            
        return self.output_dir
    
    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()
            self.spark = None


def export_gold_to_saf(limit: int = None, output_dir: str = "/opt/airflow/saf_export", create_zip: bool = True) -> str:
    """
    Convenience function to export Gold layer to SAF format.
    
    Args:
        limit: Optional limit on number of items
        output_dir: Output directory path
        create_zip: Create ZIP archive (default: True for faster upload)
        
    Returns:
        Path to SAF export directory or ZIP file
    """
    exporter = SAFExporter(output_dir=output_dir)
    try:
        return exporter.export_to_saf(limit=limit, create_zip=create_zip)
    finally:
        exporter.cleanup()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Test export with limit and ZIP
    result = export_gold_to_saf(limit=10, create_zip=True)
    print(f"\nExport result: {result}")
