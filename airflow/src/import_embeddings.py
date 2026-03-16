#!/usr/bin/env python3
"""
Import pre-computed embeddings into Elasticsearch.
==================================================

Reads the enriched JSONL produced by the Colab/Kaggle embedding notebook
and bulk-updates Elasticsearch documents with embedding vectors.

Usage (from host machine):
    docker cp data/exports/oer_embeddings.jsonl oer-airflow-scraper:/opt/airflow/exports/
    docker exec oer-airflow-scraper python /opt/airflow/src/import_embeddings.py /opt/airflow/exports/oer_embeddings.jsonl

Or directly if elasticsearch is accessible:
    python scripts/import_embeddings.py data/exports/oer_embeddings.jsonl

Input JSONL format (each line):
    {"_id": "resource_key", "embedding": [0.1, 0.2, ...], "chunk_embeddings": [[0.1, ...], [0.2, ...], ...]}

What it does:
    1. Reads the JSONL file line by line (low memory)
    2. For each doc: updates the `embedding` field (doc-level KNN vector)
    3. For each doc: updates `pdf_chunks[i].embedding` (chunk-level vectors)
    4. Uses ES bulk _update API — no reindex needed
"""

import json
import os
import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


def main(embeddings_file: str) -> None:
    es_host = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
    index_name = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")

    print(f"Connecting to Elasticsearch at {es_host}")
    es = Elasticsearch(hosts=[es_host])
    info = es.info()
    print(f"Connected — cluster: {info.get('cluster_name')}, version: {info['version']['number']}")

    # Count lines for progress
    total = sum(1 for _ in open(embeddings_file, "r", encoding="utf-8") if _.strip())
    print(f"Importing {total:,} embedding records from {embeddings_file}")

    def _update_actions():
        with open(embeddings_file, "r", encoding="utf-8") as fh:
            for line_num, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue

                record = json.loads(line)
                doc_id = record["_id"]
                doc_embedding = record.get("embedding")
                chunk_embeddings = record.get("chunk_embeddings", [])

                if not doc_embedding:
                    continue

                # Build the update body
                update_body = {"embedding": doc_embedding}

                # If chunk embeddings exist, update pdf_chunks with painless script
                if chunk_embeddings:
                    yield {
                        "_op_type": "update",
                        "_index": index_name,
                        "_id": doc_id,
                        "script": {
                            "source": """
                                ctx._source.embedding = params.doc_emb;
                                if (ctx._source.pdf_chunks != null && params.chunk_embs.size() > 0) {
                                    for (int i = 0; i < ctx._source.pdf_chunks.size() && i < params.chunk_embs.size(); i++) {
                                        ctx._source.pdf_chunks[i].embedding = params.chunk_embs[i];
                                    }
                                }
                            """,
                            "params": {
                                "doc_emb": doc_embedding,
                                "chunk_embs": chunk_embeddings,
                            },
                            "lang": "painless",
                        },
                    }
                else:
                    # No chunk embeddings — just update doc-level
                    yield {
                        "_op_type": "update",
                        "_index": index_name,
                        "_id": doc_id,
                        "doc": update_body,
                    }

                if line_num % 100 == 0:
                    print(f"  Progress: {line_num}/{total} ({line_num*100//total}%)")

    success, failed = 0, 0
    for ok, info_item in streaming_bulk(
        es, _update_actions(), chunk_size=200, raise_on_error=False
    ):
        if ok:
            success += 1
        else:
            failed += 1
            if failed <= 5:
                print(f"  Update error: {info_item}")

    print(f"\nDone — {success:,} updated, {failed} failed")

    # Verify
    count_resp = es.search(
        index=index_name,
        body={"size": 0, "query": {"exists": {"field": "embedding"}}},
    )
    embed_count = count_resp["hits"]["total"]["value"]
    print(f"Documents with embedding vectors: {embed_count:,}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python import_embeddings.py <embeddings_file.jsonl>")
        print("       python import_embeddings.py data/exports/oer_embeddings.jsonl")
        sys.exit(1)
    main(sys.argv[1])
