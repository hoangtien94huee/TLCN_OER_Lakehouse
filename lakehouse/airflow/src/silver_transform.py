#!/usr/bin/env python3
"""
Silver Layer Transformation - Bronze to Silver
===============================================

Standalone script to transform Bronze layer data to Silver layer using Spark + Iceberg.
Based on building-lakehouse pattern.
"""

import os
import json
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict

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

# MinIO imports
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    print("Warning: MinIO library not found")

@dataclass
class UnifiedOERResource:
    """Unified OER resource schema for Silver layer"""
    id: str
    title: str
    description: str
    url: str
    source: str
    subjects: List[str]
    authors: List[str]
    language: str
    format: str
    license: str
    publication_date: Optional[str] = None
    isbn: Optional[str] = None
    publisher: Optional[str] = None
    download_links: Optional[List[Dict[str, str]]] = None
    created_at: str = ""
    updated_at: str = ""
    quality_score: float = 0.0

class SilverTransformStandalone:
    """Standalone Silver layer transformer using Spark + Iceberg"""
    
    def __init__(self):
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        self.spark = self._create_spark_session() if SPARK_AVAILABLE else None
        self.minio_client = self._setup_minio() if MINIO_AVAILABLE else None
        
        # Table configuration
        self.catalog_name = "lakehouse"
        self.database_name = "default"
        self.table_name = "oer_resources"
        self.full_table_name = f"{self.catalog_name}.{self.database_name}.{self.table_name}"
        
        print(f"🚀 Silver Transform initialized")
        
        if self.spark:
            self._create_database_if_not_exists()
            self._create_table_if_not_exists()
    
    def _create_spark_session(self) -> Optional["SparkSession"]:
        """Create Spark session with Iceberg configuration"""
        if not SPARK_AVAILABLE:
            print("Spark not available, cannot create session")
            return None
            
        try:
            # Spark configuration for Iceberg
            spark = SparkSession.builder \
                .appName("OER-Silver-Transform") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.lakehouse.type", "hive") \
                .config("spark.sql.catalog.lakehouse.uri", os.getenv('HIVE_METASTORE_URI', 'thrift://hive-metastore:9083')) \
                .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{self.bucket}/silver/") \
                .config("spark.sql.catalog.lakehouse.s3.endpoint", os.getenv('MINIO_ENDPOINT', 'http://minio:9000')) \
                .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY', 'minioadmin')) \
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY', 'minioadmin')) \
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT', 'http://minio:9000')) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            print("✅ Spark session created with Iceberg support")
            return spark
            
        except Exception as e:
            print(f"❌ Spark session creation failed: {e}")
            return None
    
    def _setup_minio(self):
        """Setup MinIO client"""
        try:
            endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000').replace('http://', '')
            access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
            secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
            secure = os.getenv('MINIO_SECURE', '0') == '1'
            
            client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
            
            # Test connection
            client.bucket_exists(self.bucket)
            print("✅ MinIO connection established")
            return client
            
        except Exception as e:
            print(f"❌ MinIO setup failed: {e}")
            return None
    
    def _create_database_if_not_exists(self):
        """Create database if it doesn't exist"""
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.database_name}")
            print(f"✅ Database {self.catalog_name}.{self.database_name} ready")
        except Exception as e:
            print(f"⚠️ Database creation warning: {e}")
    
    def _create_table_if_not_exists(self):
        """Create Iceberg table if it doesn't exist"""
        try:
            # Define schema
            schema = """
                id STRING,
                title STRING,
                description STRING,
                url STRING,
                source STRING,
                subjects ARRAY<STRING>,
                authors ARRAY<STRING>,
                language STRING,
                format STRING,
                license STRING,
                publication_date STRING,
                isbn STRING,
                publisher STRING,
                download_links ARRAY<STRUCT<format: STRING, url: STRING, text: STRING>>,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                quality_score DOUBLE
            """
            
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.full_table_name} (
                    {schema}
                ) USING iceberg
                PARTITIONED BY (source)
                TBLPROPERTIES (
                    'format-version' = '2'
                )
            """
            
            self.spark.sql(create_sql)
            print(f"✅ Table {self.full_table_name} ready")
            
        except Exception as e:
            print(f"⚠️ Table creation warning: {e}")
    
    def get_bronze_files(self) -> List[str]:
        """Get list of Bronze layer files from MinIO"""
        if not self.minio_client:
            return []
        
        try:
            files = []
            objects = self.minio_client.list_objects(self.bucket, prefix="bronze/", recursive=True)
            
            for obj in objects:
                if obj.object_name.endswith('.json'):
                    files.append(f"s3a://{self.bucket}/{obj.object_name}")
            
            print(f"🔍 Found {len(files)} bronze files")
            return files
            
        except Exception as e:
            print(f"❌ Error listing bronze files: {e}")
            return []
    
    def normalize_mit_ocw(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize MIT OCW record to unified schema"""
        return {
            'id': record.get('id', ''),
            'title': record.get('title', ''),
            'description': record.get('description', ''),
            'url': record.get('url', ''),
            'source': 'mit_ocw',
            'subjects': record.get('subjects', []),
            'authors': record.get('instructors', []),
            'language': record.get('language', 'en'),
            'format': 'course',
            'license': record.get('license', 'CC BY-NC-SA'),
            'publication_date': record.get('raw_data', {}).get('scraped_at', ''),
            'isbn': '',
            'publisher': 'MIT',
            'download_links': [],
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
            'quality_score': self._calculate_quality_score(record)
        }
    
    def normalize_openstax(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize OpenStax record to unified schema"""
        return {
            'id': record.get('id', ''),
            'title': record.get('title', ''),
            'description': record.get('description', ''),
            'url': record.get('url', ''),
            'source': 'openstax',
            'subjects': record.get('subjects', []),
            'authors': record.get('authors', []),
            'language': record.get('language', 'en'),
            'format': 'textbook',
            'license': record.get('license', 'CC BY'),
            'publication_date': record.get('publication_date', ''),
            'isbn': record.get('isbn', ''),
            'publisher': 'OpenStax',
            'download_links': record.get('download_links', []),
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
            'quality_score': self._calculate_quality_score(record)
        }
    
    def normalize_otl(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize OTL record to unified schema"""
        return {
            'id': record.get('id', ''),
            'title': record.get('title', ''),
            'description': record.get('description', ''),
            'url': record.get('url', ''),
            'source': 'otl',
            'subjects': record.get('subjects', []),
            'authors': record.get('authors', []),
            'language': record.get('language', 'en'),
            'format': 'textbook',
            'license': record.get('license', 'Open License'),
            'publication_date': record.get('publication_date', ''),
            'isbn': record.get('isbn', ''),
            'publisher': record.get('publisher', ''),
            'download_links': record.get('download_links', []),
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
            'quality_score': self._calculate_quality_score(record)
        }
    
    def _calculate_quality_score(self, record: Dict[str, Any]) -> float:
        """Calculate data quality score (0.0 to 1.0)"""
        score = 0.0
        
        # Title completeness (20%)
        if record.get('title', '').strip():
            score += 0.2
        
        # Description completeness (20%)
        if record.get('description', '').strip():
            score += 0.2
        
        # Authors/Instructors (15%)
        authors = record.get('authors', []) or record.get('instructors', [])
        if authors:
            score += 0.15
        
        # Subjects (15%)
        if record.get('subjects', []):
            score += 0.15
        
        # URL validity (10%)
        url = record.get('url', '')
        if url and url.startswith('http'):
            score += 0.1
        
        # License info (10%)
        if record.get('license', '').strip():
            score += 0.1
        
        # Additional metadata (10%)
        if record.get('isbn', '') or record.get('publication_date', ''):
            score += 0.1
        
        return round(score, 2)
    
    def process_bronze_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Process a single bronze file and return normalized records"""
        try:
            print(f"📖 Processing: {file_path}")
            
            # Read JSON file using Spark
            df = self.spark.read.json(file_path)
            records = df.collect()
            
            normalized_records = []
            
            for row in records:
                record = row.asDict()
                
                # Determine source and normalize accordingly
                source = record.get('source', '')
                
                if 'mit_ocw' in source:
                    normalized = self.normalize_mit_ocw(record)
                elif 'openstax' in source:
                    normalized = self.normalize_openstax(record)
                elif 'otl' in source:
                    normalized = self.normalize_otl(record)
                else:
                    print(f"⚠️ Unknown source: {source}")
                    continue
                
                # Add record ID if missing
                if not normalized.get('id'):
                    normalized['id'] = hashlib.md5(normalized['url'].encode()).hexdigest()
                
                normalized_records.append(normalized)
            
            print(f"✅ Processed {len(normalized_records)} records from {file_path}")
            return normalized_records
            
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")
            return []
    
    def write_to_silver(self, records: List[Dict[str, Any]]):
        """Write normalized records to Silver layer Iceberg table"""
        if not records or not self.spark:
            return
        
        try:
            print(f"💾 Writing {len(records)} records to Silver layer...")
            
            # Convert to Spark DataFrame
            df = self.spark.createDataFrame(records)
            
            # Write to Iceberg table (merge/upsert)
            df.writeTo(self.full_table_name).using("iceberg").createOrReplace()
            
            print(f"✅ Successfully wrote {len(records)} records to {self.full_table_name}")
            
        except Exception as e:
            print(f"❌ Error writing to Silver layer: {e}")
    
    def run(self):
        """Main execution function"""
        print("🚀 Starting Bronze to Silver transformation...")
        
        if not self.spark:
            print("❌ Spark not available, cannot proceed")
            return
        
        # Get bronze files
        bronze_files = self.get_bronze_files()
        if not bronze_files:
            print("❌ No bronze files found")
            return
        
        # Process all files
        all_records = []
        for file_path in bronze_files:
            records = self.process_bronze_file(file_path)
            all_records.extend(records)
        
        # Write to Silver layer
        if all_records:
            self.write_to_silver(all_records)
            
            # Print summary
            sources = {}
            for record in all_records:
                source = record['source']
                sources[source] = sources.get(source, 0) + 1
            
            print(f"\n📊 Silver Layer Summary:")
            for source, count in sources.items():
                print(f"  {source}: {count} records")
            
            avg_quality = sum(r['quality_score'] for r in all_records) / len(all_records)
            print(f"  Average quality score: {avg_quality:.2f}")
        
        print(f"✅ Bronze to Silver transformation completed!")
        
        if self.spark:
            self.spark.stop()

def main():
    """Entry point for standalone execution"""
    transformer = SilverTransformStandalone()
    transformer.run()

if __name__ == "__main__":
    main()

