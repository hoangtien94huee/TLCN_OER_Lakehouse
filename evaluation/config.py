"""
Central configuration for the OER Chatbot evaluation system.

Run scripts from: /home/lib/oer_chatbot_project/TLCN_OER_Lakehouse/evaluation/
  python3 01_generate_test_set.py
  python3 02_run_pipeline.py
  python3 03_compute_metrics.py
  python3 04_generate_report.py
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT       = Path(__file__).parent
LOGS_DIR   = ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

TEST_SET_PATH      = ROOT / "test_set.json"
PIPELINE_OUT_PATH  = ROOT / "pipeline_outputs.json"
REPORT_JSON_PATH   = ROOT / "evaluation_report.json"
REPORT_CSV_PATH    = ROOT / "evaluation_report.csv"

# ---------------------------------------------------------------------------
# Simple .env loader (no python-dotenv required)
# ---------------------------------------------------------------------------
def _load_env(env_file: Path) -> dict:
    result = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            result[k.strip()] = v.strip().strip('"').strip("'")
    return result

_env = _load_env(ROOT.parent / ".env")

# ---------------------------------------------------------------------------
# Service endpoints (accessible from host machine)
# ---------------------------------------------------------------------------
API_BASE  = os.getenv("EVAL_API_BASE", "http://172.19.24.202:18088/api")
ES_HOST   = os.getenv("EVAL_ES_HOST",  "http://localhost:9200")
ES_INDEX  = "oer_resources_tier1"

# ---------------------------------------------------------------------------
# Groq LLM  (same model the chatbot uses — free tier)
# ---------------------------------------------------------------------------
GROQ_API_KEY  = os.getenv("GROQ_API_KEY") or _env.get("GROQ_API_KEY", "")
GROQ_API_KEY_2 = os.getenv("GROQ_API_KEY_2") or _env.get("GROQ_API_KEY_2", "")
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_MODEL    = "llama-3.3-70b-versatile"

# ---------------------------------------------------------------------------
# Google Gemini (alternative LLM for test-set generation)
# ---------------------------------------------------------------------------
GEMINI_API_KEY  = os.getenv("GEMINI_API_KEY") or _env.get("GEMINI_API_KEY", "")
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
GEMINI_MODEL    = "gemini-2.0-flash"

# ---------------------------------------------------------------------------
# Test-set composition
# ---------------------------------------------------------------------------
TARGET_COUNTS = {
    "definition":    20,   # "What is X?", "Define Y..."
    "find_material": 20,   # "Find materials about Z..."
    "comparison":    20,   # "Difference between A and B..."
    "multi_step":    20,   # multi-hop reasoning
    "out_of_scope":  20,   # questions not in OER corpus
}
TOTAL_QUESTIONS = sum(TARGET_COUNTS.values())   # 100

# ES sampling targets (oversampled to survive failures)
ES_SAMPLE_PER_SOURCE = {"mit_ocw": 40, "open textbook library": 25, "openstax": 15}

# ---------------------------------------------------------------------------
# Timeouts & retries
# ---------------------------------------------------------------------------
API_TIMEOUT_SEC   = 90    # chatbot /api/ask can be slow
DEBUG_TIMEOUT_SEC = 60    # debug API endpoints
GROQ_TIMEOUT_SEC    = 40
GEMINI_TIMEOUT_SEC  = 60
MAX_RETRIES         = 3
GROQ_RETRY_DELAY    = 20    # seconds between Groq retries
GEMINI_RETRY_DELAY  = 5     # seconds between Gemini retries
INTER_CALL_DELAY    = 1.0   # seconds between Gemini calls (generous free-tier quota)

# Pipeline run settings
PIPELINE_BATCH_PAUSE = 10  # seconds pause every 10 questions

# ---------------------------------------------------------------------------
# Metric thresholds (for rating)
# ---------------------------------------------------------------------------
THRESHOLDS = {
    "hit_rate":           {"fair": 0.50, "good": 0.70, "excellent": 0.85},
    "tier1_recall_at_k":  {"fair": 0.60, "good": 0.80, "excellent": 0.90},
    "grounded_rate":      {"fair": 0.50, "good": 0.70, "excellent": 0.85},
    "faithfulness":       {"fair": 0.50, "good": 0.70, "excellent": 0.85},
    "oos_detection":      {"fair": 0.50, "good": 0.70, "excellent": 0.85},
    "latency_ms":         {},   # lower is better — handled separately
}


def rate(metric: str, value: float) -> str:
    """Return Vietnamese rating string for a metric value."""
    t = THRESHOLDS.get(metric, {})
    if not t:
        return "—"
    if value >= t.get("excellent", 9999):
        return "Rất tốt"
    if value >= t.get("good", 9999):
        return "Tốt"
    if value >= t.get("fair", 9999):
        return "Trung bình"
    return "Kém"
