#!/usr/bin/env python3
"""
Gold -> DSpace Sync
==================

Synchronizes OER resources from the Gold Layer to a DSpace 7+ (including 9) repository.
Uses the DSpace REST API to create items and upload bitstreams (PDFs).
"""

import os
import json
import requests
import logging
from typing import Dict, Any, Optional, List
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from minio import Minio
except ImportError:
    logger.warning("PySpark or MinIO not available. This script requires them to run.")
    SparkSession = None
    Minio = None

class DSpaceSynchronizer:
    def __init__(self):
        # Configuration
        # Note: Container name is 'dspace' (not 'dspace-server')
        # Internal port is 8080, external is 8180
        self.dspace_api = os.getenv("DSPACE_API_ENDPOINT", "http://dspace:8080/server/api").rstrip("/")
        self.dspace_user = os.getenv("DSPACE_USER", "admin@dspace.org")
        self.dspace_password = os.getenv("DSPACE_PASSWORD", "dspace")
        self.collection_uuid = os.getenv("DSPACE_COLLECTION_UUID")  # Target collection UUID
        
        # MinIO Config
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
        self.minio_access = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        
        # Spark Config
        self.gold_catalog = os.getenv("ICEBERG_GOLD_CATALOG", "gold")
        self.gold_db = os.getenv("GOLD_DATABASE", "analytics")
        
        # Table names
        self.fact_table = f"{self.gold_catalog}.{self.gold_db}.fact_oer_resources"
        self.dim_resources_table = f"{self.gold_catalog}.{self.gold_db}.dim_oer_resources"
        self.dim_sources_table = f"{self.gold_catalog}.{self.gold_db}.dim_sources"
        self.dim_languages_table = f"{self.gold_catalog}.{self.gold_db}.dim_languages"
        self.dim_date_table = f"{self.gold_catalog}.{self.gold_db}.dim_date"
        
        # State
        self.session = requests.Session()
        self.token = None
        self.minio_client = None
        self.spark = None

    def _init_services(self):
        """Initialize Spark and MinIO connections."""
        if Minio:
            self.minio_client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access,
                secret_key=self.minio_secret,
                secure=False
            )
        
        if SparkSession:
            # Configure Spark with Iceberg catalog support
            # Use Spark cluster mode
            spark_master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
            jar_path = os.getenv("SPARK_JARS", "/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar")
            
            self.spark = (
                SparkSession.builder
                .appName("DSpace-Sync")
                .master(spark_master)
                .config("spark.jars", jar_path)
                .config("spark.driver.extraClassPath", jar_path)
                .config("spark.executor.extraClassPath", jar_path)
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.gold.type", "hadoop")
                .config("spark.sql.catalog.gold.warehouse", f"s3a://{self.bucket}/gold")
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
                .config("spark.hadoop.fs.s3a.access.key", self.minio_access)
                .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .getOrCreate()
            )

    def _set_csrf_token(self, csrf_token: str):
        """Set CSRF token in header and cookie, clearing any existing ones first."""
        # Clear existing CSRF cookies to avoid duplicates
        cookies_to_remove = [key for key in self.session.cookies.keys() if 'XSRF' in key.upper()]
        for key in cookies_to_remove:
            del self.session.cookies[key]
        
        # Set new CSRF token in header and cookie
        self.session.headers.update({'X-XSRF-TOKEN': csrf_token})
        self.session.cookies.set('DSPACE-XSRF-COOKIE', csrf_token, domain='', path='/server')

    def _refresh_csrf_from_cookies(self):
        """Refresh X-XSRF-TOKEN header from cookies."""
        csrf_token = self.session.cookies.get('DSPACE-XSRF-COOKIE')
        if csrf_token:
            self.session.headers.update({'X-XSRF-TOKEN': csrf_token})

    def login(self) -> bool:
        """Authenticate with DSpace 7+ REST API."""
        try:
            # Step 1: Get CSRF token via status endpoint
            status_resp = self.session.get(f"{self.dspace_api}/authn/status")
            
            csrf_token = status_resp.headers.get('DSPACE-XSRF-TOKEN')
            if not csrf_token:
                logger.error("No CSRF token in status response")
                return False
            
            logger.info(f"Got CSRF token: {csrf_token[:20]}...")
            
            # Step 2: Set CSRF token in both header AND cookie
            # Clear any existing CSRF cookies first to avoid duplicates
            self._set_csrf_token(csrf_token)
            
            # Step 3: Post Credentials as FORM DATA (not JSON!)
            # DSpace uses "user" field, not "email"
            login_data = {
                "user": self.dspace_user,
                "password": self.dspace_password
            }
            
            resp = self.session.post(
                f"{self.dspace_api}/authn/login",
                data=login_data,  # form data, not json
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                allow_redirects=True
            )
            
            logger.info(f"Login response status: {resp.status_code}")
            
            if resp.status_code == 200:
                # Step 4: Get Bearer Token from Authorization header
                auth_header = resp.headers.get("Authorization")
                if auth_header:
                    self.session.headers.update({"Authorization": auth_header})
                    # Update CSRF token for subsequent requests
                    new_csrf = resp.headers.get('DSPACE-XSRF-TOKEN')
                    if new_csrf:
                        self._set_csrf_token(new_csrf)
                    logger.info("Successfully logged into DSpace.")
                    return True
                else:
                    logger.error("Login succeeded but no Authorization header")
                    return False
            else:
                logger.error(f"Login failed with status {resp.status_code}")
                logger.error(f"Response: {resp.text}")
                return False
                
        except Exception as e:
            logger.error(f"DSpace login error: {e}")
            return False

    def check_item_exists(self, source_url: str) -> Optional[str]:
        """Check if item exists in DSpace by searching for source_url (dc.identifier.uri)."""
        # Note: This assumes DSpace Discovery is configured to index dc.identifier.uri
        # Alternatively, we can use a custom lookup if we store DSpace UUID back in Lakehouse
        try:
            search_endpoint = f"{self.dspace_api}/discover/search/objects"
            params = {
                "query": f"dc.identifier.uri:{source_url}",
                "dsoType": "ITEM"
            }
            resp = self.session.get(search_endpoint, params=params)
            if resp.status_code == 200:
                data = resp.json()
                embedded = data.get("_embedded", {}).get("searchResult", {}).get("_embedded", {}).get("objects", [])
                if embedded:
                    return embedded[0]["_embedded"]["indexableObject"]["uuid"]
            return None
        except Exception as e:
            logger.warning(f"Error checking existence for {source_url}: {e}")
            return None

    def create_metadata_payload(self, row: Any) -> Dict[str, Any]:
        """Map Gold Layer fields to DSpace Dublin Core metadata."""
        metadata = {}
        
        # Helper to add field
        def add_field(key, value, lang=None):
            if not value: return
            if key not in metadata: metadata[key] = []
            entry = {"value": str(value)}
            if lang: entry["language"] = lang
            metadata[key].append(entry)

        # 1. Title
        add_field("dc.title", row.title)
        
        # 2. Authors (Array)
        if row.creator_names:
            for author in row.creator_names:
                add_field("dc.contributor.author", author)
        
        # 3. Description/Abstract
        add_field("dc.description.abstract", row.description, "en")
        
        # 4. Date Issued
        if row.publication_date:
            add_field("dc.date.issued", str(row.publication_date))
            
        # 5. Publisher
        add_field("dc.publisher", row.publisher_name)
        
        # 6. Language
        add_field("dc.language.iso", row.language_code)
        
        # 7. Identifier (Source URL)
        add_field("dc.identifier.uri", row.source_url)
        
        # 8. Type
        add_field("dc.type", "Learning Object") # Default type
        
        # 9. Subjects
        if hasattr(row, 'matched_subjects') and row.matched_subjects:
            # Assuming matched_subjects is a list of structs or strings
            # Adjust based on actual Spark schema
            pass 

        return metadata

    def create_workspace_item(self, metadata: Dict[str, Any]) -> Optional[str]:
        """Create a new Workspace Item in the target collection (DSpace 7/8/9 compatible)."""
        if not self.collection_uuid:
            logger.error("Collection UUID not set.")
            return None

        # Endpoint to create a workspace item
        endpoint = f"{self.dspace_api}/submission/workspaceitems"
        params = {"owningCollection": self.collection_uuid}
        
        try:
            # Refresh CSRF token before POST
            self._refresh_csrf_from_cookies()
            
            # Step 1: Create Empty Item
            # In DSpace 7+, we POST to /workspaceitems with the collection UUID
            # Must include Content-Type header even for empty body
            headers = {"Content-Type": "application/json"}
            resp = self.session.post(endpoint, params=params, headers=headers, json={})
            resp.raise_for_status()
            wsi = resp.json()
            wsi_id = wsi['id']
            
            logger.info(f"Initialized WorkspaceItem {wsi_id}")

            # Step 2: Add Metadata via PATCH
            # DSpace 7+ uses JSON Patch to update metadata
            operations = []
            for field, values in metadata.items():
                for val in values:
                    # Construct the operation
                    # Note: 'language' is optional in the value dict
                    value_entry = [{"value": val["value"]}]
                    if "language" in val:
                        value_entry[0]["language"] = val["language"]

                    # Use correct path format for DSpace 7+
                    field_name = field if field.startswith('dc.') else f'dc.{field}'
                    op = {
                        "op": "add",
                        "path": f"/sections/traditionalpageone/{field_name}",
                        "value": value_entry
                    }
                    operations.append(op)
            
            if operations:
                patch_endpoint = f"{self.dspace_api}/submission/workspaceitems/{wsi_id}"
                # Refresh CSRF token again before PATCH
                self._refresh_csrf_from_cookies()
                
                # Must use application/json-patch+json for PATCH
                headers = {
                    "Content-Type": "application/json-patch+json",
                }
                resp = self.session.patch(patch_endpoint, json=operations, headers=headers)
                if resp.status_code not in [200, 201]:
                    logger.warning(f"PATCH returned {resp.status_code}: {resp.text[:500]}")
                logger.info(f"Patched metadata for WSI {wsi_id}")
            
            return wsi_id
            
        except Exception as e:
            logger.error(f"Error creating item: {e}")
            if 'resp' in locals(): 
                logger.error(f"Response: {resp.text}")
            return None

    def upload_bitstream(self, wsi_id: str, pdf_path: str):
        """Upload PDF file from MinIO to the Workspace Item."""
        if not self.minio_client or not pdf_path:
            return

        try:
            # 1. Get file stream from MinIO
            bucket, key = self._parse_s3_path(pdf_path)
            if not bucket or not key: return
            
            response = self.minio_client.get_object(bucket, key)
            file_content = response.read()
            filename = os.path.basename(key)
            
            # 2. Upload to DSpace
            # Endpoint: /api/submission/workspaceitems/{id}/sections/upload
            # For DSpace 7+, we post to the 'upload' section
            endpoint = f"{self.dspace_api}/submission/workspaceitems/{wsi_id}/sections/upload"
            
            # Multipart upload
            files = {'file': (filename, file_content, 'application/pdf')}
            resp = self.session.post(endpoint, files=files)
            resp.raise_for_status()
            
            logger.info(f"Uploaded {filename} to WSI {wsi_id}")
            
        except Exception as e:
            logger.error(f"Error uploading bitstream {pdf_path}: {e}")
        finally:
            if 'response' in locals(): response.close()

    def deposit_item(self, wsi_id: str):
        """Deposit (Install) the Workspace Item into the workflow/archive."""
        try:
            # To deposit, we create a workflowitem from the workspaceitem
            endpoint = f"{self.dspace_api}/workflow/workflowitems"
            
            # The body should be the URI of the workspace item
            # Content-Type: text/uri-list
            headers = {"Content-Type": "text/uri-list"}
            data = f"{self.dspace_api}/submission/workspaceitems/{wsi_id}"
            
            resp = self.session.post(endpoint, data=data, headers=headers)
            resp.raise_for_status()
            logger.info(f"Deposited WSI {wsi_id} to workflow/archive.")
            
        except Exception as e:
            logger.error(f"Error depositing item {wsi_id}: {e}")
            if 'resp' in locals(): logger.error(resp.text)

    def _parse_s3_path(self, path: str) -> tuple:
        clean = path.replace("s3a://", "").replace("s3://", "")
        parts = clean.split("/", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, None

    def run(self):
        """Main execution flow."""
        self._init_services()
        if not self.login():
            return

        # Read and JOIN Gold Layer tables to get complete data
        logger.info(f"Reading from Gold layer tables and joining...")
        
        # Load tables
        fact_df = self.spark.table(self.fact_table)
        dim_resources_df = self.spark.table(self.dim_resources_table)
        dim_sources_df = self.spark.table(self.dim_sources_table)
        dim_languages_df = self.spark.table(self.dim_languages_table)
        dim_date_df = self.spark.table(self.dim_date_table)
        
        # JOIN fact with dimensions to get complete information
        df = (
            fact_df.alias("fact")
            .join(
                dim_resources_df.alias("res"),
                fact_df.resource_key == dim_resources_df.resource_key,
                "inner"
            )
            .join(
                dim_sources_df.alias("src"),
                fact_df.source_key == dim_sources_df.source_key,
                "left"
            )
            .join(
                dim_languages_df.alias("lang"),
                fact_df.language_key == dim_languages_df.language_key,
                "left"
            )
            .join(
                dim_date_df.alias("pub_date"),
                fact_df.publication_date_key == dim_date_df.date_key,
                "left"
            )
            .select(
                # From dim_resources
                F.col("res.resource_key"),
                F.col("res.resource_id"),
                F.col("res.title"),
                F.col("res.description"),
                F.col("res.source_url"),
                F.col("res.publisher_name"),
                F.col("res.creator_names"),
                F.col("res.matched_subjects"),
                # From dim_sources
                F.col("src.source_name"),
                F.col("src.source_code"),
                # From dim_languages
                F.col("lang.language_code"),
                # From dim_date (publication date)
                F.col("pub_date.date").alias("publication_date"),
                # From fact
                F.col("fact.data_quality_score"),
            )
        )
        
        # Collect to driver (Batch processing)
        # For very large datasets, this should be distributed, but REST API calls are usually sequential or limited rate
        records = df.collect()
        
        logger.info(f"Found {len(records)} records to sync.")
        
        for row in records:
            try:
                # Check duplication
                if self.check_item_exists(row.source_url):
                    logger.info(f"Skipping existing item: {row.title}")
                    continue
                
                # Create Metadata
                metadata = self.create_metadata_payload(row)
                
                # Create Item
                wsi_id = self.create_workspace_item(metadata)
                
                if wsi_id:
                    # Upload PDF (assuming 'pdf_files' is a list of s3 paths in the row)
                    # You might need to adjust this field name based on your Gold schema
                    # For now, let's assume we look up PDFs similar to Elasticsearch sync or if stored in row
                    pass 
                    # TODO: Implement PDF path lookup if not directly in Gold table
                    # If Gold table has 'pdf_path' or similar:
                    # self.upload_bitstream(wsi_id, row.pdf_path)
                    
                    # Deposit
                    self.deposit_item(wsi_id)
                    
            except Exception as e:
                logger.error(f"Failed to process row {row.resource_key}: {e}")

if __name__ == "__main__":
    syncer = DSpaceSynchronizer()
    syncer.run()
