#!/usr/bin/env python3
"""
MinIO Schema Creation Script
===========================

Creates the initial lakehouse structure in MinIO for the OER project.
"""

import os
import sys
from pathlib import Path

# Add src to Python path
sys.path.append('/opt/airflow/src')

try:
    from minio_utils import MinIOUtilsStandalone
    
    print("Initializing MinIO lakehouse structure...")
    
    # Initialize MinIO utils
    minio_utils = MinIOUtilsStandalone()
    
    if minio_utils.client:
        # Create basic lakehouse structure
        success = minio_utils.create_lakehouse_structure()
        if success:
            print("MinIO lakehouse structure created successfully")
        else:
            print("MinIO lakehouse structure creation had issues")
    else:
        print("MinIO not available, skipping schema creation")
        
except Exception as e:
    print(f"MinIO setup warning: {e}")
    print("Continuing without MinIO schema...")

print("MinIO initialization completed")