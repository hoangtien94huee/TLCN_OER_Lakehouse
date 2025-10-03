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
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
import re
from urllib.parse import urlparse

# Spark imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
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

class SilverTransformStandalone:
    """Standalone Silver layer transformer using Spark + Iceberg"""
    
    def __init__(self):
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        self._ensure_spark_home()
        self.spark = self._create_spark_session() if SPARK_AVAILABLE else None
        self.minio_client = self._setup_minio() if MINIO_AVAILABLE else None
        
        if not SPARK_AVAILABLE:
            raise RuntimeError("PySpark is not installed in this environment")
        if not self.spark:
            raise RuntimeError("Spark session could not be created")


        # Table configuration
        self.catalog_name = "lakehouse"
        self.database_name = "default"
        self.table_name = "oer_resources_dc"
        self.full_table_name = f"{self.catalog_name}.{self.database_name}.{self.table_name}"
        self.subjects_table = f"{self.catalog_name}.{self.database_name}.oer_subjects"
        self.relations_table = f"{self.catalog_name}.{self.database_name}.oer_relations"
        self.multimedia_table = f"{self.catalog_name}.{self.database_name}.oer_multimedia"
        self.quality_table = f"{self.catalog_name}.{self.database_name}.oer_quality_audit"
        self.creators_table = f"{self.catalog_name}.{self.database_name}.oer_creators"
        self.history_table = f"{self.catalog_name}.{self.database_name}.oer_history"
        self.language_table = f"{self.catalog_name}.{self.database_name}.oer_language_normalized"
        
        print("Silver Transform initialized")
        
        # Ensure MinIO paths exist before attempting to create DB/tables
        self._ensure_minio_paths()

        if self.spark:
            self._create_database_if_not_exists()
            self._create_table_if_not_exists()
            self._create_support_tables_if_not_exists()

    def _ensure_spark_home(self) -> None:
        """Ensure SPARK_HOME points to a valid Spark installation before creating sessions."""
        if not SPARK_AVAILABLE:
            return

        spark_home_env = os.environ.get('SPARK_HOME')
        if spark_home_env:
            spark_submit = Path(spark_home_env) / 'bin' / 'spark-submit'
            if spark_submit.exists():
                return
            print(f"Configured SPARK_HOME {spark_home_env} missing spark-submit; attempting auto-configuration")

        try:
            import pyspark  # type: ignore
            package_home = Path(pyspark.__file__).resolve().parent
            candidate_home = package_home
            candidate_submit = candidate_home / 'bin' / 'spark-submit'
            if candidate_submit.exists():
                if spark_home_env and spark_home_env != str(candidate_home):
                    print(f"SPARK_HOME updated from {spark_home_env} to {candidate_home}")
                else:
                    print(f"SPARK_HOME set to {candidate_home}")
                os.environ['SPARK_HOME'] = str(candidate_home)
                os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
                return
            print(f"PySpark package found at {candidate_home} but spark-submit is missing")
        except Exception as exc:
            print(f"Unable to auto-configure SPARK_HOME: {exc}")

    
    def _create_spark_session(self) -> Optional["SparkSession"]:
        """Create Spark session with Iceberg configuration"""
        if not SPARK_AVAILABLE:
            print("Spark not available, cannot create session")
            return None
            
        try:
            # Spark configuration for Iceberg REST Catalog
            spark = SparkSession.builder \
                .appName("OER-Silver-Transform") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.lakehouse.type", "rest") \
                .config("spark.sql.catalog.lakehouse.uri", os.getenv('ICEBERG_REST_URI', 'http://iceberg-rest:8181')) \
                .config("spark.sql.defaultCatalog", "lakehouse") \
                .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY', 'minioadmin')) \
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY', 'minioadmin')) \
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT', 'http://minio:9000')) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.network.timeout", "300s") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            print("Spark session created with Iceberg support")
            return spark
            
        except Exception as e:
            print(f"Spark session creation failed: {e}")
            return None
    
    def _setup_minio(self):
        """Setup MinIO client"""
        try:
            endpoint_raw = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
            if '://' not in endpoint_raw:
                endpoint_raw = f'http://{endpoint_raw}'
            parsed = urlparse(endpoint_raw)
            endpoint = parsed.netloc or parsed.path

            access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
            secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

            secure_env = os.getenv('MINIO_SECURE')
            if secure_env is not None:
                secure = secure_env.lower() in {'1', 'true', 'yes'}
            else:
                secure = parsed.scheme == 'https'

            client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

            # Test connection
            client.bucket_exists(self.bucket)
            print("MinIO connection established")
            return client

        except Exception as e:
            print(f"MinIO setup failed: {e}")
            return None

    def _ensure_minio_paths(self) -> None:
        """Ensure the required MinIO bucket and silver prefix exist to avoid s3a path issues."""
        if not self.minio_client:
            return
        try:
            bucket_exists = self.minio_client.bucket_exists(self.bucket)
            if not bucket_exists:
                self.minio_client.make_bucket(self.bucket)
                print(f"Created MinIO bucket: {self.bucket}")
            # Ensure silver/ prefix has a placeholder so Iceberg can resolve path
            placeholder_object = "silver/.placeholder"
            from io import BytesIO
            data = BytesIO(b"")
            self.minio_client.put_object(self.bucket, placeholder_object, data, 0)
            print("Ensured MinIO prefix: silver/")
        except Exception as exc:
            print(f"Warning ensuring MinIO paths: {exc}")
    
    def _create_database_if_not_exists(self):
        """Create database if it doesn't exist with s3a location to avoid local FS."""
        try:
            db_location = f"s3a://{self.bucket}/silver/{self.database_name}"
            self.spark.sql(
                f"""
                CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{self.database_name}
                LOCATION '{db_location}'
                """
            )
            print(f"Database {self.catalog_name}.{self.database_name} ready")
        except Exception as e:
            print(f"Database creation warning: {e}")
    
    def _create_table_if_not_exists(self):
        """Create Iceberg table if it doesn't exist"""
        try:
            # Define schema
            schema = """
                dc_identifier STRING,
                dc_title STRING,
                dc_creator ARRAY<STRING>,
                dc_subject ARRAY<STRING>,
                dc_description STRING,
                dc_publisher STRING,
                dc_contributor ARRAY<STRING>,
                dc_date STRING,
                dc_type STRING,
                dc_format STRING,
                dc_source STRING,
                dc_language STRING,
                dc_relation ARRAY<STRING>,
                dc_coverage STRING,
                dc_rights STRING,
                source_system STRING,
                bronze_object STRING,
                ingested_at TIMESTAMP,
                quality_score DOUBLE
            """
            
            table_location = f"s3a://{self.bucket}/silver/{self.table_name}"
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.full_table_name} (
                    {schema}
                ) USING iceberg
                PARTITIONED BY (source_system)
                LOCATION '{table_location}'
                TBLPROPERTIES (
                    'format-version' = '2'
                )
            """
            
            print(f"Creating/ensuring main table at {table_location}...")
            self.spark.sql(create_sql)
            print(f"Table {self.full_table_name} ready")
            
        except Exception as e:
            print(f"Table creation warning: {e}")
    
    def _create_support_tables_if_not_exists(self):
        if not self.spark:
            return

        tables = [
            (self.subjects_table, """
                dc_identifier STRING,
                dc_subject STRING,
                source_system STRING,
                ingested_at TIMESTAMP
            """, 'source_system'),
            (self.relations_table, """
                dc_identifier STRING,
                relation_url STRING,
                relation_type STRING,
                source_system STRING,
                ingested_at TIMESTAMP
            """, 'source_system'),
            (self.multimedia_table, """
                media_id STRING,
                dc_identifier STRING,
                media_type STRING,
                title STRING,
                url STRING,
                transcript_available BOOLEAN,
                size_mb DOUBLE,
                source_system STRING,
                ingested_at TIMESTAMP
            """, 'source_system'),
            (self.quality_table, """
                dc_identifier STRING,
                issue_code STRING,
                severity STRING,
                detail STRING,
                source_system STRING,
                detected_at TIMESTAMP
            """, 'source_system'),
            (self.creators_table, """
                dc_identifier STRING,
                creator_name STRING,
                normalized_name STRING,
                role STRING,
                source_system STRING,
                ingested_at TIMESTAMP
            """, 'source_system'),
            (self.history_table, """
                dc_identifier STRING,
                bronze_object STRING,
                record_checksum STRING,
                source_system STRING,
                ingested_at TIMESTAMP
            """, 'source_system'),
            (self.language_table, """
                dc_identifier STRING,
                raw_language STRING,
                normalized_language STRING,
                source_system STRING,
                ingested_at TIMESTAMP
            """, 'source_system')
        ]

        for table_name, columns, partition_key in tables:
            try:
                simple_table_name = table_name.split('.')[-1]
                table_location = f"s3a://{self.bucket}/silver/{simple_table_name}"
                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {columns}
                    ) USING iceberg
                    PARTITIONED BY ({partition_key})
                    LOCATION '{table_location}'
                    TBLPROPERTIES (
                        'format-version' = '2'
                    )
                """
                print(f"Creating/ensuring support table {simple_table_name} at {table_location}...")
                self.spark.sql(create_sql)
                print(f"Table {table_name} ready")
            except Exception as exc:
                print(f"Support table creation warning for {table_name}: {exc}")

    def get_bronze_files(self) -> List[Dict[str, str]]:
        """Get list of Bronze layer files from MinIO"""
        if not self.minio_client:
            return []

        try:
            files: List[Dict[str, str]] = []
            objects = self.minio_client.list_objects(self.bucket, prefix="bronze/", recursive=True)

            for obj in objects:
                name = obj.object_name
                if '/.placeholder' in name:
                    continue
                if name.endswith((".json", ".jsonl")):
                    files.append({
                        'uri': f"s3a://{self.bucket}/{name}",
                        'object_name': name
                    })

            print(f"Found {len(files)} bronze files")
            return files

        except Exception as exc:
            print(f"Error listing bronze files: {exc}")
            return []

    def _detect_source_system(self, record: Dict[str, Any]) -> str:
        candidates = [record.get('source'), record.get('source_system'), record.get('provider')]
        for candidate in candidates:
            if not candidate:
                continue
            value = str(candidate).lower()
            if 'mit' in value or 'ocw' in value:
                return 'mit_ocw'
            if 'openstax' in value:
                return 'openstax'
            if 'open textbook library' in value or 'otl' in value:
                return 'otl'
        return ''

    def _resolve_identifier(self, record: Dict[str, Any], prefix: str) -> str:
        for key in ('url', 'identifier', 'id'):
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        title = (record.get('title') or record.get('name') or '').strip()
        base = f"{prefix}:{title}" if title else prefix
        return hashlib.md5(base.encode('utf-8')).hexdigest()

    def _normalize_list(self, value: Any) -> List[str]:
        if not value:
            return []
        if isinstance(value, list):
            items = [str(v) for v in value]
        elif isinstance(value, str):
            items = re.split(r'[;,|/]', value)
        else:
            items = [str(value)]
        return [item.strip() for item in items if item and item.strip()]

    def _normalize_language(self, value: Optional[str]) -> str:
        if not value or not str(value).strip():
            return 'en'
        return str(value).strip().lower()

    def _format_date(self, value: Any) -> str:
        if not value:
            return ''
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def _collect_relations(self, record: Dict[str, Any]) -> List[str]:
        urls: set[str] = set()
        for field in ('url_pdf', 'pdf_url'):
            candidate = record.get(field)
            if isinstance(candidate, str) and candidate.strip():
                urls.add(candidate.strip())
        for key in ('videos', 'pdfs', 'download_links', 'related_links'):
            items = record.get(key, [])
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        for candidate_key in ('url', 'video_url', 'download_url', 'href'):
                            link = item.get(candidate_key)
                            if isinstance(link, str) and link.strip():
                                urls.add(link.strip())
                    elif isinstance(item, str) and item.strip():
                        urls.add(item.strip())
        raw = record.get('raw_data')
        if isinstance(raw, dict):
            for key in ('videos', 'pdfs', 'related_links'):
                items = raw.get(key, [])
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            link = item.get('url') or item.get('href')
                            if isinstance(link, str) and link.strip():
                                urls.add(link.strip())
                        elif isinstance(item, str) and item.strip():
                            urls.add(item.strip())
        return sorted(urls)

    def _build_coverage(self, values: List[Any]) -> str:
        parts: List[str] = []
        for value in values:
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    text = str(item).strip()
                    if text and text not in parts:
                        parts.append(text)
            elif isinstance(value, str):
                text = value.strip()
                if text and text not in parts:
                    parts.append(text)
        return '; '.join(parts)

    def _classify_relation(self, url: str) -> str:
        if not url:
            return 'external'
        url_lower = url.lower()
        if 'youtube' in url_lower or 'youtu.be' in url_lower:
            return 'video'
        if url_lower.endswith('.pdf'):
            return 'pdf'
        if any(url_lower.endswith(ext) for ext in ('.mp3', '.wav', '.aac', '.m4a')):
            return 'audio'
        if 'github.com' in url_lower or 'gitlab.com' in url_lower:
            return 'code'
        if 'openstax' in url_lower:
            return 'openstax'
        if 'ocw.mit.edu' in url_lower:
            return 'mit_ocw'
        return 'external'

    def _normalize_creator_name(self, name: str) -> str:
        if not name:
            return ''
        return re.sub(r"\s+", " ", name).strip().title()

    def normalize_mit_ocw(self, record: Dict[str, Any], bronze_object: str) -> Dict[str, Any]:
        creators = self._normalize_list(record.get('instructors'))
        subjects = self._normalize_list(record.get('subjects') or record.get('subject'))
        relations = self._collect_relations(record)
        coverage = self._build_coverage([record.get('semester'), record.get('level')])
        return {
            'dc_identifier': self._resolve_identifier(record, 'mit_ocw'),
            'dc_title': (record.get('title') or '').strip(),
            'dc_creator': creators,
            'dc_subject': subjects,
            'dc_description': (record.get('description') or '').strip(),
            'dc_publisher': 'MIT OpenCourseWare',
            'dc_contributor': [],
            'dc_date': self._format_date(record.get('scraped_at') or (record.get('raw_data') or {}).get('scraped_at')),
            'dc_type': 'InteractiveResource',
            'dc_format': 'text/html',
            'dc_source': 'MIT OpenCourseWare',
            'dc_language': self._normalize_language(record.get('language')),
            'dc_relation': relations,
            'dc_coverage': coverage,
            'dc_rights': (record.get('license') or 'CC BY-NC-SA 4.0').strip(),
            'source_system': 'mit_ocw',
            'bronze_object': bronze_object,
            'ingested_at': datetime.utcnow()
        }

    def normalize_openstax(self, record: Dict[str, Any], bronze_object: str) -> Dict[str, Any]:
        creators = self._normalize_list(record.get('authors'))
        subjects = self._normalize_list(record.get('subjects') or record.get('subject'))
        contributors = self._normalize_list(record.get('contributors'))
        relations = self._collect_relations(record)
        return {
            'dc_identifier': self._resolve_identifier(record, 'openstax'),
            'dc_title': (record.get('title') or '').strip(),
            'dc_creator': creators,
            'dc_subject': subjects,
            'dc_description': (record.get('description') or '').strip(),
            'dc_publisher': 'OpenStax',
            'dc_contributor': contributors,
            'dc_date': self._format_date(record.get('publication_date') or record.get('scraped_at')),
            'dc_type': 'Text',
            'dc_format': 'text/html',
            'dc_source': 'OpenStax',
            'dc_language': self._normalize_language(record.get('language')),
            'dc_relation': relations,
            'dc_coverage': '',
            'dc_rights': (record.get('license') or 'CC BY 4.0').strip(),
            'source_system': 'openstax',
            'bronze_object': bronze_object,
            'ingested_at': datetime.utcnow()
        }

    def normalize_otl(self, record: Dict[str, Any], bronze_object: str) -> Dict[str, Any]:
        creators = self._normalize_list(record.get('authors'))
        subjects = self._normalize_list(record.get('subjects') or record.get('subject'))
        relations = self._collect_relations(record)
        return {
            'dc_identifier': self._resolve_identifier(record, 'otl'),
            'dc_title': (record.get('title') or '').strip(),
            'dc_creator': creators,
            'dc_subject': subjects,
            'dc_description': (record.get('description') or '').strip(),
            'dc_publisher': 'Open Textbook Library',
            'dc_contributor': [],
            'dc_date': self._format_date(record.get('publication_date') or record.get('scraped_at')),
            'dc_type': 'Text',
            'dc_format': 'text/html',
            'dc_source': 'Open Textbook Library',
            'dc_language': self._normalize_language(record.get('language')),
            'dc_relation': relations,
            'dc_coverage': '',
            'dc_rights': (record.get('license') or 'Various').strip(),
            'source_system': 'otl',
            'bronze_object': bronze_object,
            'ingested_at': datetime.utcnow()
        }

    def _calculate_quality_score(self, record: Dict[str, Any]) -> float:
        score = 0.0
        if record.get('dc_title'):
            score += 0.2
        if record.get('dc_description'):
            score += 0.2
        if record.get('dc_creator'):
            score += 0.15
        if record.get('dc_subject'):
            score += 0.15
        if record.get('dc_source'):
            score += 0.1
        if record.get('dc_rights'):
            score += 0.1
        if record.get('dc_date'):
            score += 0.1
        return round(min(score, 1.0), 2)

    def _build_relation_rows(self, normalized: Dict[str, Any], relations: List[str]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for url in relations:
            if not url:
                continue
            rows.append({
                'dc_identifier': normalized['dc_identifier'],
                'relation_url': url,
                'relation_type': self._classify_relation(url),
                'source_system': normalized['source_system'],
                'ingested_at': normalized['ingested_at']
            })
        return rows

    def _build_multimedia_rows(self, record: Dict[str, Any], normalized: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        base_id = normalized['dc_identifier']
        source = normalized['source_system']
        timestamp = normalized['ingested_at']

        def _safe_float(value: Any) -> Optional[float]:
            try:
                if value in (None, ''):
                    return None
                return float(value)
            except (TypeError, ValueError):
                return None

        def add_media(media_type: str, title: str, url: Optional[str], transcript: bool = False, size_mb: Optional[Any] = None):
            if not url:
                return
            media_id = hashlib.md5(f"{base_id}|{url}|{media_type}".encode('utf-8')).hexdigest()
            rows.append({
                'media_id': media_id,
                'dc_identifier': base_id,
                'media_type': media_type,
                'title': (title or '').strip(),
                'url': url,
                'transcript_available': bool(transcript),
                'size_mb': _safe_float(size_mb),
                'source_system': source,
                'ingested_at': timestamp
            })

        videos = record.get('videos') or (record.get('raw_data') or {}).get('videos') or []
        for video in videos:
            if not isinstance(video, dict):
                continue
            url = video.get('url') or video.get('video_url') or video.get('href')
            title = video.get('title') or video.get('name') or normalized['dc_title']
            transcript_flag = video.get('transcript') or video.get('transcript_url')
            media_type = video.get('type') or 'video'
            add_media(media_type, title, url, bool(transcript_flag), video.get('size_mb'))

        pdfs = record.get('pdfs') or (record.get('raw_data') or {}).get('pdfs') or []
        for pdf in pdfs:
            if not isinstance(pdf, dict):
                continue
            url = pdf.get('url') or pdf.get('download_url') or pdf.get('href')
            title = pdf.get('title') or pdf.get('name') or normalized['dc_title']
            add_media(pdf.get('category') or 'pdf', title, url, False, pdf.get('size_mb'))

        url_pdf = record.get('url_pdf') or record.get('pdf_url')
        if url_pdf:
            add_media('pdf', normalized['dc_title'], url_pdf)

        return rows

    def _collect_quality_issues(self, normalized: Dict[str, Any]) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []
        identifier = normalized['dc_identifier']
        source = normalized['source_system']
        timestamp = normalized['ingested_at']

        def add_issue(code: str, severity: str, detail: str):
            issues.append({
                'dc_identifier': identifier,
                'issue_code': code,
                'severity': severity,
                'detail': detail,
                'source_system': source,
                'detected_at': timestamp
            })

        if not normalized.get('dc_title'):
            add_issue('MISSING_TITLE', 'high', 'Title is missing')
        if not normalized.get('dc_description'):
            add_issue('MISSING_DESCRIPTION', 'medium', 'Description is missing')
        if not normalized.get('dc_creator'):
            add_issue('MISSING_CREATOR', 'medium', 'Creators not provided')
        if not normalized.get('dc_subject'):
            add_issue('MISSING_SUBJECT', 'medium', 'Subjects not provided')
        if not normalized.get('dc_rights'):
            add_issue('MISSING_RIGHTS', 'medium', 'Rights statement missing')
        if not normalized.get('dc_date'):
            add_issue('MISSING_DATE', 'low', 'Primary date missing')

        return issues

    def _build_creator_rows(self, normalized: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        identifier = normalized['dc_identifier']
        source = normalized['source_system']
        timestamp = normalized['ingested_at']
        for creator in normalized.get('dc_creator', []) or []:
            rows.append({
                'dc_identifier': identifier,
                'creator_name': creator,
                'normalized_name': self._normalize_creator_name(creator),
                'role': 'creator',
                'source_system': source,
                'ingested_at': timestamp
            })
        for contributor in normalized.get('dc_contributor', []) or []:
            rows.append({
                'dc_identifier': identifier,
                'creator_name': contributor,
                'normalized_name': self._normalize_creator_name(contributor),
                'role': 'contributor',
                'source_system': source,
                'ingested_at': timestamp
            })
        return rows

    def _build_history_row(self, normalized: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
        checksum = hashlib.md5(json.dumps(record, sort_keys=True, default=str).encode('utf-8')).hexdigest()
        return {
            'dc_identifier': normalized['dc_identifier'],
            'bronze_object': normalized['bronze_object'],
            'record_checksum': checksum,
            'source_system': normalized['source_system'],
            'ingested_at': normalized['ingested_at']
        }

    def _build_language_row(self, normalized: Dict[str, Any], raw_language: Optional[str]) -> Optional[Dict[str, Any]]:
        normalized_language = normalized.get('dc_language')
        if raw_language is None and not normalized_language:
            return None
        return {
            'dc_identifier': normalized['dc_identifier'],
            'raw_language': raw_language,
            'normalized_language': normalized_language,
            'source_system': normalized['source_system'],
            'ingested_at': normalized['ingested_at']
        }

    def process_bronze_file(self, file_uri: str, object_name: str) -> Dict[str, List[Dict[str, Any]]]:
        aggregated: Dict[str, List[Dict[str, Any]]] = {
            'resources': [],
            'subjects': [],
            'relations': [],
            'multimedia': [],
            'quality': [],
            'creators': [],
            'history': [],
            'languages': []
        }
        try:
            print(f"Processing {object_name}")
            df = self.spark.read.option("multiline", "true").json(file_uri)
            for row in df.toLocalIterator():
                record = row.asDict(recursive=True)
                source_system = self._detect_source_system(record)
                if not source_system:
                    print(f"Unknown source for object {object_name}, skipping record")
                    continue
                if source_system == 'mit_ocw':
                    normalized = self.normalize_mit_ocw(record, object_name)
                elif source_system == 'openstax':
                    normalized = self.normalize_openstax(record, object_name)
                elif source_system == 'otl':
                    normalized = self.normalize_otl(record, object_name)
                else:
                    print(f"Unsupported source {source_system} in {object_name}")
                    continue
                normalized['quality_score'] = self._calculate_quality_score(normalized)
                aggregated['resources'].append(normalized)
                if normalized.get('dc_subject'):
                    for subject in normalized['dc_subject']:
                        if not subject:
                            continue
                        aggregated['subjects'].append({
                            'dc_identifier': normalized['dc_identifier'],
                            'dc_subject': subject,
                            'source_system': normalized['source_system'],
                            'ingested_at': normalized['ingested_at']
                        })
                relations = normalized.get('dc_relation') or []
                aggregated['relations'].extend(self._build_relation_rows(normalized, relations))
                aggregated['multimedia'].extend(self._build_multimedia_rows(record, normalized))
                aggregated['quality'].extend(self._collect_quality_issues(normalized))
                aggregated['creators'].extend(self._build_creator_rows(normalized))
                aggregated['history'].append(self._build_history_row(normalized, record))
                raw_language = record.get('language')
                if not raw_language:
                    raw_data = record.get('raw_data') or {}
                    raw_language = raw_data.get('language') or raw_data.get('locale')
                if isinstance(raw_language, str):
                    raw_language = raw_language.strip() or None
                language_row = self._build_language_row(normalized, raw_language)
                if language_row:
                    aggregated['languages'].append(language_row)
            print(f"Processed {len(aggregated['resources'])} records from {object_name}")
            return aggregated
        except Exception as exc:
            print(f"Error processing {object_name}: {exc}")
            return aggregated

    def _write_table(self, records: List[Dict[str, Any]], table_name: str, dedup_columns: Optional[List[str]], label: str) -> int:
        if not records or not self.spark:
            return 0
        
        # Define explicit schema for resource table
        schema = StructType([
            StructField("dc_identifier", StringType(), False),
            StructField("dc_title", StringType(), True),
            StructField("dc_creator", ArrayType(StringType()), True),
            StructField("dc_subject", ArrayType(StringType()), True),
            StructField("dc_description", StringType(), True),
            StructField("dc_publisher", StringType(), True),
            StructField("dc_contributor", ArrayType(StringType()), True),
            StructField("dc_date", StringType(), True),
            StructField("dc_type", StringType(), True),
            StructField("dc_format", StringType(), True),
            StructField("dc_source", StringType(), True),
            StructField("dc_language", StringType(), True),
            StructField("dc_relation", ArrayType(StringType()), True),
            StructField("dc_coverage", StringType(), True),
            StructField("dc_rights", StringType(), True),
            StructField("source_system", StringType(), False),
            StructField("bronze_object", StringType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("quality_score", DoubleType(), True)
        ])
        
        df = self.spark.createDataFrame(records, schema=schema)
        if dedup_columns:
            df = df.dropDuplicates(dedup_columns)
        df = df.cache()
        count = df.count()
        df.writeTo(table_name).using("iceberg").overwritePartitions()
        df.unpersist()
        print(f"Wrote {count} {label} rows to {table_name}")
        return count

    def write_to_silver(self, records: List[Dict[str, Any]]):
        self._write_table(records, self.full_table_name, ['dc_identifier', 'source_system'], 'resource')

    def run(self):
        print("Starting Bronze to Silver transformation...")
        if not self.spark:
            print("Spark not available, cannot proceed")
            return
        bronze_files = self.get_bronze_files()
        if not bronze_files:
            print("No bronze files found")
            return
        aggregated: Dict[str, List[Dict[str, Any]]] = {
            'resources': [],
            'subjects': [],
            'relations': [],
            'multimedia': [],
            'quality': [],
            'creators': [],
            'history': [],
            'languages': []
        }
        for file_info in bronze_files:
            partial = self.process_bronze_file(file_info['uri'], file_info['object_name'])
            for key in aggregated:
                aggregated[key].extend(partial.get(key, []))
        resources = aggregated['resources']
        if resources:
            self.write_to_silver(resources)
            self._write_table(aggregated['subjects'], self.subjects_table, ['dc_identifier', 'dc_subject'], 'subject')
            self._write_table(aggregated['relations'], self.relations_table, ['dc_identifier', 'relation_url'], 'relation')
            self._write_table(aggregated['multimedia'], self.multimedia_table, ['media_id'], 'multimedia')
            self._write_table(aggregated['quality'], self.quality_table, ['dc_identifier', 'issue_code'], 'quality issue')
            self._write_table(aggregated['creators'], self.creators_table, ['dc_identifier', 'creator_name', 'role'], 'creator')
            self._write_table(aggregated['history'], self.history_table, ['dc_identifier', 'record_checksum'], 'history entry')
            self._write_table(aggregated['languages'], self.language_table, ['dc_identifier', 'normalized_language', 'raw_language'], 'language mapping')
            sources: Dict[str, int] = {}
            for record in resources:
                source = record['source_system']
                sources[source] = sources.get(source, 0) + 1
            print("\nSilver layer summary:")

            for source, count in sources.items():
                print(f"{source}: {count} records")
            avg_quality = sum(r['quality_score'] for r in resources) / len(resources)
            print(f"Average quality score: {avg_quality:.2f}")
        else:
            print("No normalized records generated")
        print("Bronze to Silver transformation completed!")
        if self.spark:
            self.spark.stop()

def main():
    """Entry point for standalone execution"""
    transformer = SilverTransformStandalone()
    transformer.run()

if __name__ == "__main__":
    main()

