#!/usr/bin/env python3
"""
Import chunks từ JSON với STREAMING - không load toàn bộ file vào RAM.
Phù hợp cho file JSON lớn (1.5GB+).
"""

import json
import os
import re
import sys
import ijson  # streaming JSON parser


def _extract_source_url_from_text(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"Saylor URL:\s*(https?://[^\s]+)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"(https?://[^\s)]+)", text)
    return m.group(1).strip() if m else ""


def _build_minio_url(asset_path: str, minio_public_base_url: str, minio_bucket: str) -> str:
    path = (asset_path or "").strip().lstrip("/")
    if not path:
        return ""
    return f"{minio_public_base_url.rstrip('/')}/{minio_bucket}/{path}"


def _normalize_chunk_metadata(chunk: dict, minio_public_base_url: str, minio_bucket: str) -> dict:
    # Make sure UI-facing reference fields are always present when possible.
    text = chunk.get("chunk_text") or ""
    title = chunk.get("title") or chunk.get("chapter_title") or chunk.get("section_title") or ""
    source_url = chunk.get("source_url") or _extract_source_url_from_text(text)
    asset_path = chunk.get("asset_path") or ""
    file_name = chunk.get("file_name") or (asset_path.split("/")[-1] if asset_path else "")
    minio_url = chunk.get("minio_url") or _build_minio_url(asset_path, minio_public_base_url, minio_bucket)

    chunk["title"] = title
    chunk["source_url"] = source_url
    chunk["asset_path"] = asset_path
    chunk["file_name"] = file_name
    chunk["minio_url"] = minio_url
    return chunk

def import_json_streaming(
    json_file: str,
    limit: int = None,
    es_host: str = "http://elasticsearch:9200",
    index_name: str = "oer_resources",
    recreate: bool = True,
    batch_size: int = 500,
    minio_public_base_url: str = "http://localhost:19000",
    minio_bucket: str = "oer-lakehouse",
):
    """Stream JSON và import vào ES - Không load toàn bộ vào RAM."""
    
    from elasticsearch import Elasticsearch, helpers
    
    print("="*80)
    print("🚀 STREAMING JSON IMPORT (Low Memory)")
    print("="*80)
    print(f"JSON file: {json_file}")
    print(f"ES host: {es_host}")
    print(f"Index: {index_name}")
    print(f"Limit: {limit if limit else 'All'}")
    print(f"Batch size: {batch_size}")
    print("="*80)
    
    if not os.path.exists(json_file):
        print(f"❌ File not found: {json_file}")
        return
    
    file_size_gb = os.path.getsize(json_file) / (1024**3)
    print(f"File size: {file_size_gb:.2f} GB")
    
    # Connect to ES
    print(f"\n🔌 Connecting to Elasticsearch...")
    es = Elasticsearch(hosts=[es_host], request_timeout=180)
    
    if not es.ping():
        print("❌ Cannot connect!")
        return
    print("✅ Connected")
    
    # Delete old index
    if recreate and es.indices.exists(index=index_name):
        print(f"\n🗑️  Deleting old index...")
        es.indices.delete(index=index_name)
        print("✅ Deleted")
    
    # Create index
    if not es.indices.exists(index=index_name):
        print(f"\n📋 Creating index...")
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "30s"  # Faster indexing
            },
            "mappings": {
                "properties": {
                    "chunk_id": {"type": "keyword"},
                    "resource_uid": {"type": "keyword"},
                    "asset_uid": {"type": "keyword"},
                    "source_system": {"type": "keyword"},
                    "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "source_url": {"type": "keyword"},
                    "asset_path": {"type": "keyword"},
                    "file_name": {"type": "keyword"},
                    "minio_url": {"type": "keyword"},
                    "chunk_text": {"type": "text"},
                    "page_no": {"type": "integer"},
                    "chunk_order": {"type": "integer"},
                    "lang": {"type": "keyword"},
                    "updated_at": {"type": "date"},
                    "chunk_type": {"type": "keyword"},
                    "chunk_tier": {"type": "integer"},
                    "chapter_id": {"type": "keyword"},
                    "chapter_title": {"type": "text"},
                    "chapter_number": {"type": "integer"},
                    "chapter_page_start": {"type": "integer"},
                    "chapter_page_end": {"type": "integer"},
                    "section_id": {"type": "keyword"},
                    "section_title": {"type": "text"},
                    "section_number": {"type": "keyword"},
                    "section_page_start": {"type": "integer"},
                    "section_page_end": {"type": "integer"},
                    "parent_chunk_id": {"type": "keyword"},
                    "has_children": {"type": "boolean"},
                    "is_summary": {"type": "boolean"},
                    "summary_method": {"type": "keyword"},
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 768,
                        "index": True,
                        "similarity": "cosine"
                    }
                }
            }
        }
        es.indices.create(index=index_name, body=mapping)
        print("✅ Index created")
    
    # Stream and index
    print(f"\n📥 Streaming chunks from JSON file...")
    print("   (This uses minimal memory)")
    
    indexed_count = 0
    batch = []
    
    try:
        with open(json_file, 'rb') as f:
            # Parse JSON array with streaming
            parser = ijson.items(f, 'item')
            
            for i, chunk in enumerate(parser):
                # Check limit
                if limit and i >= limit:
                    break
                
                # Validate chunk_id (must be <= 512 bytes for ES keyword type)
                chunk_id = chunk.get('chunk_id', '')
                if not chunk_id or len(chunk_id.encode('utf-8')) > 512:
                    print(f"\n⚠️  Skipping invalid chunk at index {i}: chunk_id too long or missing")
                    continue
                chunk = _normalize_chunk_metadata(chunk, minio_public_base_url, minio_bucket)
                batch.append(chunk)
                
                # Bulk index when batch is full
                if len(batch) >= batch_size:
                    actions = [
                        {
                            "_index": index_name,
                            "_id": c["chunk_id"],
                            "_source": c
                        }
                        for c in batch
                    ]
                    
                    success, failed = helpers.bulk(
                        es, actions, 
                        raise_on_error=False,
                        request_timeout=180
                    )
                    indexed_count += success
                    
                    if len(failed) > 0:
                        print(f"\n⚠️  Warning: {len(failed)} documents failed in batch")
                    
                    # Progress
                    print(f"   Indexed {indexed_count:,} chunks...", end='\r')
                    
                    batch = []
            
            # Index remaining batch
            if batch:
                actions = [
                    {
                        "_index": index_name,
                        "_id": c["chunk_id"],
                        "_source": c
                    }
                    for c in batch
                ]
                
                success, failed = helpers.bulk(
                    es, actions,
                    raise_on_error=False,
                    request_timeout=180
                )
                indexed_count += success
        
        print(f"\n✅ Successfully indexed {indexed_count:,} chunks")
        
    except Exception as e:
        print(f"\n❌ Error during streaming: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Refresh
    print(f"\n🔄 Refreshing index...")
    es.indices.refresh(index=index_name)
    es.indices.put_settings(
        index=index_name,
        body={"index": {"refresh_interval": "1s"}}  # Restore normal refresh
    )
    
    # Verify
    print(f"\n📊 Verification:")
    count = es.count(index=index_name)
    print(f"   Total documents: {count['count']:,}")
    
    # Sample
    result = es.search(index=index_name, size=1)
    if result['hits']['hits']:
        sample = result['hits']['hits'][0]['_source']
        print(f"\n📄 Sample:")
        print(f"   chunk_id: {sample['chunk_id'][:40]}...")
        print(f"   has embedding: {'embedding' in sample}")
        if 'embedding' in sample:
            print(f"   embedding dim: {len(sample['embedding'])}")
    
    print("\n" + "="*80)
    print("✅ IMPORT COMPLETED!")
    print("="*80)


if __name__ == "__main__":
    json_file = "/opt/airflow/database/exports/chunks_part_03.json"
    limit = None
    
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
    if len(sys.argv) > 2:
        limit = int(sys.argv[2])
    
    import_json_streaming(
        json_file=json_file,
        limit=limit,
        es_host=os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200"),
        index_name=os.getenv("ELASTICSEARCH_INDEX", "oer_resources"),
        recreate=False,
        batch_size=500,
        minio_public_base_url=os.getenv("MINIO_PUBLIC_BASE_URL", "http://localhost:19000"),
        minio_bucket=os.getenv("MINIO_BUCKET", "oer-lakehouse"),
    )
