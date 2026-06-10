#!/usr/bin/env bash
# ============================================================
# run_all.sh — Run the full OER Chatbot evaluation pipeline
#
# Usage:
#   bash run_all.sh              # full run (all 4 steps)
#   bash run_all.sh --force      # regenerate test set
#   bash run_all.sh --skip-llm  # skip LLM judge in step 3
#
# Prerequisites:
#   pip install requests          (only external dependency)
#   Groq API key in ../.env       (GROQ_API_KEY=gsk_...)
#   Chatbot API running at :18088
#   Elasticsearch running at :9200
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

FORCE=""
SKIP_LLM=""

for arg in "$@"; do
  case "$arg" in
    --force)    FORCE="--force"    ;;
    --skip-llm) SKIP_LLM="--skip-llm" ;;
  esac
done

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   OER Chatbot Evaluation — Full Run              ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ── Step 1: Generate test set ─────────────────────────────
echo "▶ [1/4] Generating test set (100 questions)..."
python3 01_generate_test_set.py $FORCE
echo "  ✓ test_set.json ready"
echo ""

# ── Step 2: Run pipeline ──────────────────────────────────
echo "▶ [2/4] Running pipeline (100 questions → /api/ask)..."
echo "  (This may take 5–10 minutes depending on chatbot speed)"
python3 02_run_pipeline.py
echo "  ✓ pipeline_outputs.json ready"
echo ""

# ── Step 3: Compute metrics ───────────────────────────────
echo "▶ [3/4] Computing metrics..."
if [ -n "$SKIP_LLM" ]; then
  echo "  (LLM judge skipped — retrieval + system metrics only)"
  python3 03_compute_metrics.py --skip-llm
else
  echo "  (LLM judge via Groq — ~15 min for all 100 questions)"
  python3 03_compute_metrics.py
fi
echo "  ✓ evaluation_report.json ready"
echo ""

# ── Step 4: Generate report ───────────────────────────────
echo "▶ [4/4] Generating report..."
python3 04_generate_report.py
echo "  ✓ evaluation_report.csv ready"
echo ""

echo "════════════════════════════════════════════════════"
echo "Done! Output files:"
echo "  test_set.json           — 100 questions with ground truth"
echo "  pipeline_outputs.json   — chatbot answers + retrieved contexts"
echo "  evaluation_report.json  — all computed metrics"
echo "  evaluation_report.csv   — machine-readable metrics table"
echo "  logs/                   — per-script logs"
echo "════════════════════════════════════════════════════"
