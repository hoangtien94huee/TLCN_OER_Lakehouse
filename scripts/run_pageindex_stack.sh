#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/3] Starting required services (etl profile)..."
docker compose --profile etl up -d minio postgres spark-master spark-worker
docker compose --profile etl up -d --build oer-scraper

echo "[2/3] Waiting for Airflow web health..."
for i in {1..60}; do
  if curl -fsS "http://localhost:18080/health" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[3/3] Checking PageIndex API health..."
if command -v jq >/dev/null 2>&1; then
  curl -fsS "http://localhost:18088/api/health" | jq .
else
  curl -fsS "http://localhost:18088/api/health" | python3 -m json.tool
fi

echo
echo "PageIndex API is up at: http://localhost:18088"
echo "Use: ./scripts/smoke_test_pageindex_api.sh"
