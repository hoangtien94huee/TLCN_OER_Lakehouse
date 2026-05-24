#!/usr/bin/env bash
set -euo pipefail

ES_HOST="${ES_HOST:-http://localhost:9200}"
COMPACT_INDEX="${COMPACT_INDEX:-oer_resources_tier1}"
LEGACY_INDEX="${LEGACY_INDEX:-oer_resources}"
DELETE_LEGACY_INDEX="${DELETE_LEGACY_INDEX:-0}"

echo "== Build compact tier1 index =="
docker compose exec \
  -e PAGEINDEX_TIER1_ES_RECREATE=1 \
  -e PAGEINDEX_TIER1_ES_INCREMENTAL=0 \
  oer-scraper \
  python /opt/airflow/src/elasticsearch_tier1_sync.py

echo
echo "== Verify compact index =="
curl -fsS "${ES_HOST}/${COMPACT_INDEX}/_count"
echo
curl -fsS "${ES_HOST}/${COMPACT_INDEX}/_mapping"
echo

if [[ "${DELETE_LEGACY_INDEX}" == "1" ]]; then
  echo "== Delete legacy chunk index: ${LEGACY_INDEX} =="
  curl -fsS -X DELETE "${ES_HOST}/${LEGACY_INDEX}"
  echo
else
  echo "Legacy index is kept. Set DELETE_LEGACY_INDEX=1 to remove: ${LEGACY_INDEX}"
fi

