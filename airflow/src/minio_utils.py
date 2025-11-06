#!/usr/bin/env python3
"""
MinIO Utilities - Object Storage Operations
===========================================

Standalone script for MinIO object storage management.
Based on building-lakehouse pattern.
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

# MinIO imports
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    print("Warning: MinIO library not found")

class MinIOUtilsStandalone:
    """Standalone MinIO utilities for OER Lakehouse"""
    
    def __init__(self):
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        
        if not MINIO_AVAILABLE:
            print(" MinIO not available")
            self.client = None
            return
        
        self.client = self._setup_minio()
        
        print(f"MinIO Utils initialized for bucket: {self.bucket}")
    
    def _setup_minio(self):
        """Setup MinIO client"""
        try:
            endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000').replace('http://', '').replace('https://', '')
            access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
            secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
            secure = os.getenv('MINIO_SECURE', '0') == '1'
            
            client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
            print(f" MinIO client connected to {endpoint}")
            return client
            
        except Exception as e:
            print(f" MinIO setup failed: {e}")
            return None
    
    def create_bucket(self, bucket_name: str = None):
        """Create bucket if it doesn't exist"""
        if not self.client:
            return False
        
        bucket_name = bucket_name or self.bucket
        
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print(f"Created bucket: {bucket_name}")
            else:
                print(f"Bucket already exists: {bucket_name}")
            
            return True
            
        except Exception as e:
            print(f" Error creating bucket {bucket_name}: {e}")
            return False
    
    def setup_lakehouse_structure(self):
        """Create lakehouse folder structure in MinIO"""
        if not self.client:
            return False
        
        try:
            # Create bucket
            self.create_bucket()
            
            # Create folder structure by uploading empty placeholder files
            folders = [
                'bronze/mit_ocw/',
                'bronze/openstax/',
                'bronze/otl/',
                'silver/',
                'gold/',
                'logs/'
            ]
            
            for folder in folders:
                placeholder_content = json.dumps({
                    'folder': folder,
                    'description': f'Lakehouse {folder.rstrip("/")} layer',
                    'created_at': datetime.now().isoformat()
                }, indent=2)
                
                # Upload placeholder
                from io import BytesIO
                placeholder_bytes = BytesIO(placeholder_content.encode('utf-8'))
                
                self.client.put_object(
                    self.bucket,
                    f"{folder}.placeholder.json",
                    placeholder_bytes,
                    len(placeholder_content.encode('utf-8')),
                    content_type='application/json'
                )
        
            print(f"Lakehouse structure created in bucket {self.bucket}")
            return True
            
        except Exception as e:
            print(f"Error setting up lakehouse structure: {e}")
            return False
    
    def list_objects(self, prefix: str = "", max_objects: int = 100) -> List[Dict[str, Any]]:
        """List objects in bucket with optional prefix filter"""
        if not self.client:
            return []
        
        try:
            objects = []
            
            for obj in self.client.list_objects(self.bucket, prefix=prefix, recursive=True):
                if len(objects) >= max_objects:
                    break
                
                objects.append({
                    'name': obj.object_name,
                    'size': obj.size,
                    'last_modified': obj.last_modified.isoformat() if obj.last_modified else None,
                    'etag': obj.etag
                })
            
            return objects
            
        except Exception as e:
            print(f"Error listing objects: {e}")
            return []
    
    def get_bucket_stats(self) -> Dict[str, Any]:
        """Get comprehensive bucket statistics"""
        if not self.client:
            return {}
        
        try:
            print("Calculating bucket statistics...")
            
            stats = {
                'bucket_name': self.bucket,
                'total_objects': 0,
                'total_size_bytes': 0,
                'layers': {
                    'bronze': {'objects': 0, 'size_bytes': 0},
                    'silver': {'objects': 0, 'size_bytes': 0},
                    'gold': {'objects': 0, 'size_bytes': 0},
                    'other': {'objects': 0, 'size_bytes': 0}
                },
                'sources': {
                    'mit_ocw': {'objects': 0, 'size_bytes': 0},
                    'openstax': {'objects': 0, 'size_bytes': 0},
                    'otl': {'objects': 0, 'size_bytes': 0}
                },
                'file_types': {},
                'generated_at': datetime.now().isoformat()
            }
            
            # Count objects and sizes
            for obj in self.client.list_objects(self.bucket, recursive=True):
                stats['total_objects'] += 1
                stats['total_size_bytes'] += obj.size
                
                # Categorize by layer
                obj_name = obj.object_name
                if obj_name.startswith('bronze/'):
                    stats['layers']['bronze']['objects'] += 1
                    stats['layers']['bronze']['size_bytes'] += obj.size
                elif obj_name.startswith('silver/'):
                    stats['layers']['silver']['objects'] += 1
                    stats['layers']['silver']['size_bytes'] += obj.size
                elif obj_name.startswith('gold/'):
                    stats['layers']['gold']['objects'] += 1
                    stats['layers']['gold']['size_bytes'] += obj.size
                else:
                    stats['layers']['other']['objects'] += 1
                    stats['layers']['other']['size_bytes'] += obj.size
                
                # Categorize by source
                if 'mit_ocw' in obj_name:
                    stats['sources']['mit_ocw']['objects'] += 1
                    stats['sources']['mit_ocw']['size_bytes'] += obj.size
                elif 'openstax' in obj_name:
                    stats['sources']['openstax']['objects'] += 1
                    stats['sources']['openstax']['size_bytes'] += obj.size
                elif 'otl' in obj_name:
                    stats['sources']['otl']['objects'] += 1
                    stats['sources']['otl']['size_bytes'] += obj.size
                
                # Categorize by file type
                file_ext = obj_name.split('.')[-1].lower() if '.' in obj_name else 'unknown'
                if file_ext not in stats['file_types']:
                    stats['file_types'][file_ext] = {'objects': 0, 'size_bytes': 0}
                stats['file_types'][file_ext]['objects'] += 1
                stats['file_types'][file_ext]['size_bytes'] += obj.size
            
            return stats
            
        except Exception as e:
            print(f"Error calculating bucket stats: {e}")
            return {}
    
    def cleanup_old_files(self, prefix: str, days_old: int = 30):
        """Clean up old files older than specified days"""
        if not self.client:
            return False
        
        try:
            from datetime import timedelta
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            deleted_count = 0
            
            for obj in self.client.list_objects(self.bucket, prefix=prefix, recursive=True):
                if obj.last_modified and obj.last_modified.replace(tzinfo=None) < cutoff_date:
                    self.client.remove_object(self.bucket, obj.object_name)
                    print(f"🗑️ Deleted old file: {obj.object_name}")
                    deleted_count += 1
            
            print(f"Cleaned up {deleted_count} old files from {prefix}")
            return True
            
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return False
    
    def backup_bronze_layer(self, backup_bucket: str):
        """Backup bronze layer to another bucket"""
        if not self.client:
            return False
        
        try:
            # Create backup bucket if not exists
            if not self.client.bucket_exists(backup_bucket):
                self.client.make_bucket(backup_bucket)
            
            copied_count = 0
            
            for obj in self.client.list_objects(self.bucket, prefix='bronze/', recursive=True):
                # Copy object to backup bucket
                copy_source = f"{self.bucket}/{obj.object_name}"
                self.client.copy_object(
                    backup_bucket,
                    obj.object_name,
                    f"/{copy_source}"
                )
                copied_count += 1
            
            print(f"Backed up {copied_count} bronze files to {backup_bucket}")
            return True
            
        except Exception as e:
            print(f"Error during backup: {e}")
            return False
    
    def print_stats_report(self):
        """Print comprehensive statistics report"""
        stats = self.get_bucket_stats()
        if not stats:
            return
        
        print(f"\nMINIO BUCKET STATISTICS REPORT")
        print("=" * 50)
        print(f"Bucket: {stats['bucket_name']}")
        print(f"Total Objects: {stats['total_objects']:,}")
        print(f"Total Size: {self._format_bytes(stats['total_size_bytes'])}")
        print(f"Generated: {stats['generated_at']}")
        
        print(f"\nLAKEHOUSE LAYERS:")
        for layer, data in stats['layers'].items():
            if data['objects'] > 0:
                print(f"  {layer.capitalize()}: {data['objects']:,} objects, {self._format_bytes(data['size_bytes'])}")
        
        print(f"\nSOURCES:")
        for source, data in stats['sources'].items():
            if data['objects'] > 0:
                print(f"  {source.replace('_', ' ').title()}: {data['objects']:,} objects, {self._format_bytes(data['size_bytes'])}")
        
        print(f"\nFILE TYPES:")
        for file_type, data in sorted(stats['file_types'].items(), key=lambda x: x[1]['objects'], reverse=True):
            if data['objects'] > 0:
                print(f"  .{file_type}: {data['objects']:,} objects, {self._format_bytes(data['size_bytes'])}")
    
    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes to human readable string"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024:
                return f"{bytes_val:.1f} {unit}"
            bytes_val /= 1024
        return f"{bytes_val:.1f} PB"
    
    def run_maintenance(self):
        """Run maintenance tasks"""
        print("Running MinIO maintenance...")
        
        # Setup structure if needed
        self.setup_lakehouse_structure()
        
        # Print statistics
        self.print_stats_report()
        
        # Optional: cleanup old temporary files
        cleanup_days = int(os.getenv('CLEANUP_DAYS', 30))
        if cleanup_days > 0:
            print(f"\nCleaning up files older than {cleanup_days} days...")
            self.cleanup_old_files('logs/', cleanup_days)
        
        print("\nMinIO maintenance completed!")

def main():
    """Entry point for standalone execution"""
    if len(os.sys.argv) > 1:
        command = os.sys.argv[1].lower()
        
        utils = MinIOUtilsStandalone()
        
        if command == 'setup':
            utils.setup_lakehouse_structure()
        elif command == 'stats':
            utils.print_stats_report()
        elif command == 'cleanup':
            days = int(os.sys.argv[2]) if len(os.sys.argv) > 2 else 30
            utils.cleanup_old_files('logs/', days)
        elif command == 'maintenance':
            utils.run_maintenance()
        else:
            print("Usage: python minio_utils.py [setup|stats|cleanup|maintenance]")
    else:
        # Default: run maintenance
        utils = MinIOUtilsStandalone()
        utils.run_maintenance()

if __name__ == "__main__":
    main()

