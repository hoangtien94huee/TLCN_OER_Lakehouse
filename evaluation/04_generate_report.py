"""
Script 4 — Generate human-readable evaluation report.

Reads evaluation_report.json and produces:
  • Pretty-printed summary table to stdout + log
  • Per-question-type breakdown table
  • evaluation_report.csv  (machine-readable)

Usage:
  python3 04_generate_report.py
  python3 04_generate_report.py --no-csv    # skip CSV output
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    REPORT_JSON_PATH, REPORT_CSV_PATH, LOGS_DIR,
    rate,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "04_generate_report.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def _fmt(value: Any, decimals: int = 4, suffix: str = "") -> str:
    """Format a numeric value or return '—' for None."""
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.{decimals}f}{suffix}"
    return str(value) + suffix


def _bar(value: float | None, width: int = 20) -> str:
    """Simple ASCII progress bar for 0–1 values."""
    if value is None:
        return " " * width
    filled = int(round(value * width))
    return "█" * filled + "░" * (width - filled)


def _sep(width: int = 70, char: str = "─") -> str:
    return char * width


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------
def print_header(report: dict) -> None:
    g = report["global"]
    log.info("")
    log.info("╔" + "═" * 68 + "╗")
    log.info("║   OER CHATBOT EVALUATION REPORT — PageIndex RAG" + " " * 19 + "║")
    log.info("╚" + "═" * 68 + "╝")
    log.info("  Total questions   : %d", g.get("total_questions", "?"))
    log.info("  In-scope          : %d", g.get("in_scope_questions", "?"))
    log.info("  Out-of-scope      : %d", g.get("total_questions", 0) - g.get("in_scope_questions", 0))
    log.info("  Error rate        : %s", _fmt(g.get("error_rate"), 2, " (fraction failed)"))
    log.info("")


def print_summary_table(report: dict) -> None:
    g = report["global"]

    rows = [
        # (display_name, metric_key, value, unit)
        ("Hit Rate (Evidence)",      "hit_rate",      g.get("hit_rate"),           ""),
        ("Tier-1 Recall@K",         "hit_rate",      g.get("tier1_recall_at_k"), ""),
        ("Faithfulness (PageIndex)", "faithfulness",  g.get("grounded_rate"),      ""),
        ("OOS Detection",            "oos_detection", g.get("oos_detection_rate"), ""),
    ]

    log.info(_sep())
    log.info("%-22s │ %-8s │ %-22s │ %-10s", "Metric", "Score", "Visual", "Rating")
    log.info(_sep())
    for name, key, value, _ in rows:
        bar   = _bar(value)
        score = _fmt(value)
        rtg   = rate(key, value) if value is not None else "—"
        log.info("%-22s │ %-8s │ %s │ %s", name, score, bar, rtg)
    log.info(_sep())
    log.info("")

    log.info("─── Latency ───────────────────────────────────────")
    log.info("  Average  : %s ms", _fmt(g.get("latency_avg_ms"), 1))
    log.info("  P50      : %s ms", _fmt(g.get("latency_p50_ms"), 1))
    log.info("  P95      : %s ms", _fmt(g.get("latency_p95_ms"), 1))
    log.info("")



def print_breakdown_by_type(report: dict) -> None:
    by_type = report.get("by_type", {})
    if not by_type:
        return

    log.info(_sep())
    log.info("BREAKDOWN BY QUESTION TYPE")
    log.info(_sep())
    log.info("%-16s │ %5s │ %8s │ %8s │ %10s │ %8s",
             "Type", "Count", "Hit Rate", "Recall@K", "Faith(PI)", "Lat(ms)")
    log.info(_sep())

    type_order = ["definition", "find_material", "comparison", "multi_step", "out_of_scope"]
    for qtype in type_order:
        d = by_type.get(qtype, {})
        if not d:
            log.info("%-16s │ %5s │ %8s │ %8s │ %10s │ %8s",
                     qtype, "0", "—", "—", "—", "—")
            continue

        if qtype == "out_of_scope":
            hit_str    = _fmt(d.get("oos_detection_rate"), 4)
            recall_str = "N/A"
            ground_str = "N/A"
        else:
            hit_str    = _fmt(d.get("hit_rate"), 4)
            recall_str = _fmt(d.get("tier1_recall_at_k"), 4)
            ground_str = _fmt(d.get("grounded_rate"), 4)

        log.info("%-16s │ %5d │ %8s │ %8s │ %10s │ %8s",
                 qtype,
                 d.get("count", 0),
                 hit_str,
                 recall_str,
                 ground_str,
                 _fmt(d.get("avg_latency_ms"), 1))

    log.info(_sep())
    log.info("  * Hit Rate = PageIndex evidence_hit_rate | OOS column = detection rate")
    log.info("")


def print_confidence_distribution(report: dict) -> None:
    per_q = report.get("per_question", [])
    if not per_q:
        return

    counts: dict[str, int] = {}
    for q in per_q:
        conf = q.get("confidence", "unknown")
        counts[conf] = counts.get(conf, 0) + 1

    log.info("─── Confidence distribution ────────────────────────")
    for conf, cnt in sorted(counts.items()):
        bar = "█" * cnt
        log.info("  %-10s │ %3d │ %s", conf, cnt, bar)
    log.info("")


def print_difficulty_breakdown(report: dict) -> None:
    per_q = report.get("per_question", [])
    if not per_q:
        return

    diff_hits: dict[str, list[float]] = {}
    for q in per_q:
        diff = q.get("difficulty", "medium")
        hit  = q.get("hit", 0.0)
        diff_hits.setdefault(diff, []).append(hit)

    log.info("─── Hit Rate by difficulty ─────────────────────────")
    for diff in ["easy", "medium", "hard"]:
        hits = diff_hits.get(diff, [])
        avg  = sum(hits) / len(hits) if hits else None
        log.info("  %-8s : %s  (n=%d)", diff, _fmt(avg), len(hits))
    log.info("")


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------
_GLOBAL_FIELDS = [
    "hit_rate", "tier1_recall_at_k", "grounded_rate",
    "faithfulness", "oos_detection_rate",
    "latency_avg_ms", "latency_p50_ms", "latency_p95_ms",
    "error_rate", "total_questions",
]

_TYPE_FIELDS = [
    "question_type", "count", "hit_rate", "tier1_recall_at_k",
    "grounded_rate", "faithfulness", "avg_latency_ms",
]

_PER_Q_FIELDS = [
    "id", "question_type", "difficulty",
    "hit", "tier1_recall_at_k", "grounded_answer_rate", "faithfulness",
    "latency_ms", "confidence", "has_error",
]


def save_csv(report: dict, path: Path) -> None:
    g = report["global"]
    by_type = report.get("by_type", {})
    per_q   = report.get("per_question", [])

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # --- Sheet 1: Global metrics ---
        writer.writerow(["=== GLOBAL METRICS ==="])
        writer.writerow(["metric", "value", "rating"])
        metric_labels = {
            "hit_rate":           "Hit Rate (Evidence)",
            "tier1_recall_at_k":  "Tier-1 Recall@K",
            "grounded_rate":      "Faithfulness (PageIndex)",
            "faithfulness":       "Faithfulness (Groq LLM judge)",
            "oos_detection_rate": "OOS Detection Rate",
            "latency_avg_ms":     "Avg Latency (ms)",
            "latency_p50_ms":     "P50 Latency (ms)",
            "latency_p95_ms":     "P95 Latency (ms)",
            "error_rate":         "Error Rate",
            "total_questions":    "Total Questions",
        }
        for field, label in metric_labels.items():
            value = g.get(field)
            rtg = rate(field.replace("oos_detection_rate", "oos_detection"), value) \
                if isinstance(value, float) else ""
            writer.writerow([label, _fmt(value) if value is not None else "", rtg])

        writer.writerow([])

        # --- Sheet 2: By-type breakdown ---
        writer.writerow(["=== BY QUESTION TYPE ==="])
        writer.writerow(["question_type", "count", "hit_rate / oos_detection",
                         "tier1_recall_at_k", "grounded_rate", "faithfulness", "avg_latency_ms"])

        for qtype in ["definition", "find_material", "comparison", "multi_step", "out_of_scope"]:
            d = by_type.get(qtype, {})
            if qtype == "out_of_scope":
                hit_val = d.get("oos_detection_rate")
            else:
                hit_val = d.get("hit_rate")
            writer.writerow([
                qtype,
                d.get("count", 0),
                _fmt(hit_val),
                _fmt(d.get("tier1_recall_at_k")) if qtype != "out_of_scope" else "N/A",
                _fmt(d.get("grounded_rate"))      if qtype != "out_of_scope" else "N/A",
                _fmt(d.get("faithfulness")),
                _fmt(d.get("avg_latency_ms"), 1),
            ])

        writer.writerow([])

        # --- Sheet 3: Per-question details ---
        writer.writerow(["=== PER QUESTION SCORES ==="])
        writer.writerow(_PER_Q_FIELDS)
        for q in per_q:
            writer.writerow([
                q.get("id", ""),
                q.get("question_type", ""),
                q.get("difficulty", ""),
                _fmt(q.get("hit"), 4),
                _fmt(q.get("tier1_recall_at_k"), 4),
                _fmt(q.get("grounded_answer_rate"), 4),
                _fmt(q.get("faithfulness")),
                _fmt(q.get("latency_ms"), 1),
                q.get("confidence", ""),
                q.get("has_error", False),
            ])

    log.info("CSV saved to %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate OER evaluation report")
    parser.add_argument("--no-csv", action="store_true", help="Skip CSV export")
    args = parser.parse_args()

    if not REPORT_JSON_PATH.exists():
        log.error("evaluation_report.json not found. Run 03_compute_metrics.py first.")
        sys.exit(1)

    report = json.loads(REPORT_JSON_PATH.read_text())
    log.info("Loaded evaluation report (%d per-question records)",
             len(report.get("per_question", [])))

    print_header(report)
    print_summary_table(report)
    print_breakdown_by_type(report)
    print_confidence_distribution(report)
    print_difficulty_breakdown(report)

    if not args.no_csv:
        save_csv(report, REPORT_CSV_PATH)

    log.info("Report complete.")
    log.info("  JSON : %s", REPORT_JSON_PATH)
    if not args.no_csv:
        log.info("  CSV  : %s", REPORT_CSV_PATH)


if __name__ == "__main__":
    main()
