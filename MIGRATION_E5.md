# Migration to E5-Base Embedding Model

## Summary

Successfully implemented E5-Base multilingual embedding model with direct Elasticsearch indexing via **Airflow DAGs**.

## Changes Made

### 1. Model Update ✅
- **Old**: `paraphrase-multilingual-MiniLM-L12-v2` (384 dims)
- **New**: `intfloat/multilingual-e5-base` (768 dims)
- **Benefits**: Better cross-lingual performance (Vietnamese ↔ English)

Files updated:
- `airflow/src/silver_embeddings.py` - Updated model config and E5 prefixes
- `airflow/src/chatbot_api.py` - Updated query embedding with E5 format
- `docker-compose.yml` - Updated EMBEDDING_MODEL env var

### 2. Direct Elasticsearch Pipeline ✅
Created new pipeline: `airflow/src/embedding_to_es_direct.py`

**Architecture**:
```
Iceberg (text only) → E5-Base → Elasticsearch (vectors + text)
```

**Solves**:
- ❌ Slow Iceberg MERGE operations
- ❌ Schema width limitations for large vectors
- ✅ Fast bulk indexing to ES
- ✅ Easy model changes (just re-index ES)

### 3. Airflow DAG Integration ✅

**New DAG**: `airflow/dags/embedding_migration_dag.py`
- One-time migration DAG (manual trigger)
- Automated backup → delete → re-index workflow
- Verification steps

**Updated DAG**: `airflow/dags/silver_layer_processing_dag.py`
- Changed to use `DirectEmbeddingPipeline`
- Future runs use E5-Base automatically
- No code changes needed after migration

### 4. E5 Query/Passage Format ✅
Implemented proper E5 prefixes:
- Documents: `"passage: {text}"`
- Queries: `"query: {text}"`
- Normalization: L2 normalization enabled

## Running the Migration

### Step 1: Start Airflow
```bash
cd /home/lib/oer_chatbot_project/TLCN_OER_Lakehouse
docker compose --profile etl up -d
```

### Step 2: Access Airflow UI
```
http://localhost:18080
Default credentials: admin/admin
```

### Step 3: Trigger Migration DAG
1. Navigate to **embedding_model_migration** DAG
2. Click "Trigger DAG" button
3. Monitor progress in Graph View
4. Wait for completion (~30-60 minutes)

### Step 4: Verify
```bash
# Check index
curl localhost:9200/oer_resources/_count

# Test cross-lingual search
curl -X POST localhost:18088/api/ask \
  -H 'Content-Type: application/json' \
  -d '{"question": "toán học là gì"}'
```

### Step 5: Disable Migration DAG
After successful migration:
1. In Airflow UI, toggle OFF the **embedding_model_migration** DAG
2. Future embeddings will use E5-Base automatically via **silver_layer_processing** DAG

## Technical Details

### E5-Base Model
- **Size**: ~1.1 GB
- **RAM usage**: ~3 GB
- **Dimensions**: 768
- **Languages**: 100+ (including Vietnamese)
- **Cross-lingual**: Native multilingual alignment

### Elasticsearch Mapping
```json
{
  "embedding": {
    "type": "dense_vector",
    "dims": 768,
    "index": true,
    "similarity": "cosine",
    "index_options": {
      "type": "hnsw",
      "m": 16,
      "ef_construction": 200
    }
  }
}
```

### Performance Expectations
- Embedding generation: 50-100 chunks/sec
- Vector search latency: <100ms (p95)
- Cross-lingual accuracy: +40-50% vs MiniLM
- Memory: Safe for 8GB system

## DAG Workflow

### Migration DAG (One-time)
```
start
  → validate_elasticsearch
  → backup_current_index
  → delete_old_index
  → generate_e5_embeddings
  → verify_migration
  → end
```

### Regular DAG (Ongoing)
```
silver_layer_processing
  → ...transform tasks...
  → generate_embeddings (now uses E5-Base + direct ES)
  → end
```

## Rollback Plan

If issues occur:

### Option 1: Restore from Backup
```bash
# In Airflow Python Operator or manually
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts=["localhost:9200"])

# Find backup index
es.cat.indices(index="oer_resources_backup_*")

# Restore
es.reindex(
  body={
    "source": {"index": "oer_resources_backup_XXXXXX"},
    "dest": {"index": "oer_resources"}
  }
)
```

### Option 2: Revert Code
```python
# airflow/src/embedding_to_es_direct.py
MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
EMBEDDING_DIM = 384
```

Then re-trigger migration DAG.

## Monitoring

Check Airflow logs for:
- Embedding generation speed
- ES indexing rate
- Memory usage
- Error messages

Access logs:
```bash
docker logs oer-airflow-scraper -f
```

## References

- Facebook MUSE: https://engineering.fb.com/2018/01/24/ml-applications/under-the-hood-multilingual-embeddings/
- E5 Model: https://huggingface.co/intfloat/multilingual-e5-base
- Elasticsearch kNN: https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html
- Airflow Best Practices: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html

