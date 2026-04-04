#!/usr/bin/env python3
"""
Import chunks with embeddings (from Colab) into Elasticsearch.

Usage:
    python import_embeddings_to_es.py --input chunks_with_embeddings.json
"""

import argparse
import json
import logging
import os
from typing import Any, Dict, List

from elasticsearch import Elasticsearch, helpers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_index(es: Elasticsearch, index_name: str, embedding_dim: int = 768):
    """Create Elasticsearch index with hierarchical mapping."""
    if es.indices.exists(index=index_name):
        logger.warning(f"Index {index_name} already exists. Deleting...")
        es.indices.delete(index=index_name)
    
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "chunk_id": {"type": "keyword"},
                "chunk_text": {"type": "text", "analyzer": "standard"},
                "chunk_tier": {"type": "keyword"},
                "chunk_index": {"type": "integer"},
                "embedding": {
                    "type": "dense_vector",
                    "dims": embedding_dim,
                    "index": True,
                    "similarity": "cosine",
                },
                "resource_uid": {"type": "keyword"},
                "asset_uid": {"type": "keyword"},
                "source_system": {"type": "keyword"},
                "page_no": {"type": "integer"},
                "chapter_title": {"type": "text"},
                "section_title": {"type": "text"},
            }
        },
    }
    
    logger.info(f"Creating index {index_name}...")
    es.indices.create(index=index_name, body=mapping)
    logger.info(f"✓ Created index {index_name}")


def build_int_range(start: Any, end: Any) -> Dict[str, int] | None:
    """Build integer range for Elasticsearch."""
    if start is not None and end is not None:
        try:
            return {"gte": int(start), "lte": int(end)}
        except (ValueError, TypeError):
            return None
    return None


def import_chunks(input_file: str, es_host: str, index_name: str, batch_size: int = 500):
    """Import chunks with embeddings to Elasticsearch."""
    logger.info(f"Loading chunks from {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        chunks = json.load(f)
    
    logger.info(f"Loaded {len(chunks):,} chunks")
    
    # Verify embeddings
    if not chunks or 'embedding' not in chunks[0]:
        raise ValueError("Input file must contain 'embedding' field in each chunk")
    
    embedding_dim = len(chunks[0]['embedding'])
    logger.info(f"Embedding dimension: {embedding_dim}")
    
    # Connect to Elasticsearch
    logger.info(f"Connecting to Elasticsearch: {es_host}")
    es = Elasticsearch(es_host)
    
    if not es.ping():
        raise ConnectionError(f"Cannot connect to Elasticsearch at {es_host}")
    
    logger.info("✓ Connected to Elasticsearch")
    
    # Create index
    create_index(es, index_name, embedding_dim)
    
    # Prepare bulk actions
    logger.info(f"Preparing bulk actions...")
    actions = []
    for chunk in chunks:
        # Build page ranges
        chapter_range = build_int_range(
            chunk.get('chapter_page_start'),
            chunk.get('chapter_page_end')
        )
        section_range = build_int_range(
            chunk.get('section_page_start'),
            chunk.get('section_page_end')
        )
        
        doc = {
            "_index": index_name,
            "_id": chunk.get("chunk_id"),
            "_source": {
                "chunk_id": chunk.get("chunk_id"),
                "chunk_text": chunk.get("chunk_text"),
                "chunk_tier": chunk.get("chunk_tier"),
                "chunk_index": chunk.get("chunk_index"),
                "embedding": chunk.get("embedding"),
                "document_id": chunk.get("document_id"),
                "title": chunk.get("title"),
                "author": chunk.get("author"),
                "subject": chunk.get("subject"),
                "chapter_title": chunk.get("chapter_title"),
                "section_title": chunk.get("section_title"),
            }
        }
        
        if chapter_range:
            doc["_source"]["chapter_page_range"] = chapter_range
        if section_range:
            doc["_source"]["section_page_range"] = section_range
        
        actions.append(doc)
    
    # Bulk index
    logger.info(f"Indexing {len(actions):,} documents in batches of {batch_size}...")
    success_count = 0
    error_count = 0
    
    for i in range(0, len(actions), batch_size):
        batch = actions[i:i + batch_size]
        success, errors = helpers.bulk(
            es,
            batch,
            stats_only=False,
            raise_on_error=False,
        )
        success_count += success
        if errors:
            error_count += len(errors)
            logger.warning(f"Batch {i//batch_size + 1}: {len(errors)} errors")
        
        if (i // batch_size + 1) % 10 == 0:
            logger.info(f"Progress: {i + len(batch):,}/{len(actions):,} documents indexed")
    
    logger.info(f"✓ Indexing complete!")
    logger.info(f"  Success: {success_count:,}")
    logger.info(f"  Errors: {error_count:,}")
    
    # Verify
    es.indices.refresh(index=index_name)
    doc_count = es.count(index=index_name)['count']
    logger.info(f"  Documents in index: {doc_count:,}")


def main():
    parser = argparse.ArgumentParser(description="Import chunks with embeddings to Elasticsearch")
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Input JSON file with embeddings (from Colab)"
    )
    parser.add_argument(
        "--es-host",
        default=os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200"),
        help="Elasticsearch host (default: http://localhost:9200)"
    )
    parser.add_argument(
        "--index",
        default=os.getenv("ELASTICSEARCH_INDEX", "oer_resources"),
        help="Index name (default: oer_resources)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for bulk indexing (default: 500)"
    )
    
    args = parser.parse_args()
    import_chunks(args.input, args.es_host, args.index, args.batch_size)


if __name__ == "__main__":
    main()
