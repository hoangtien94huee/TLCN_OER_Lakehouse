#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:18088}"
QUESTION="${1:-Giải thích khái niệm hồi quy tuyến tính và cho ví dụ đơn giản}"

pretty_print_json() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  else
    python3 -m json.tool
  fi
}

echo "== Health =="
curl -fsS "$API_BASE/api/health" | pretty_print_json

echo
echo "== Runtime Config =="
curl -fsS "$API_BASE/api/debug/pageindex_config" | pretty_print_json

echo
echo "== Local LLM Debug =="
curl -fsS "$API_BASE/api/debug/local_llm" | pretty_print_json

echo
echo "== Tier1 Candidates =="
curl -fsS -X POST "$API_BASE/api/debug/tier1_candidates" \
  -H "Content-Type: application/json" \
  -d "$(python3 - <<'PY' "$QUESTION"
import json
import sys
print(json.dumps({"question": sys.argv[1], "top_k": 3, "language": "vi"}, ensure_ascii=False))
PY
)" | pretty_print_json

echo
echo "== Build Compact Tier1 Index (manual command) =="
echo "docker compose exec oer-scraper python /opt/airflow/src/elasticsearch_tier1_sync.py"

echo
echo "== get_document =="
curl -fsS -X POST "$API_BASE/api/debug/get_document" \
  -H "Content-Type: application/json" \
  -d "$(python3 - <<'PY' "$QUESTION"
import json
import sys
print(json.dumps({"question": sys.argv[1], "top_k": 3, "language": "vi"}, ensure_ascii=False))
PY
)" | pretty_print_json

echo
echo "== ask =="
curl -fsS -X POST "$API_BASE/api/ask" \
  -H "Content-Type: application/json" \
  -d "$(python3 - <<'PY' "$QUESTION"
import json
import sys
print(json.dumps({"question": sys.argv[1], "top_k": 4, "language": "vi"}, ensure_ascii=False))
PY
)" | pretty_print_json
