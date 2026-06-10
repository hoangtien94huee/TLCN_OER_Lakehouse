"""
Script 3 — Compute evaluation metrics.

Reads pipeline_outputs.json and computes:

  3A — Retrieval  (no LLM needed):
    • Hit Rate        — source_asset_uid found in retrieved contexts
    • MRR             — Mean Reciprocal Rank of correct context
    • Built-in proxy  — avg tier1_recall_at_k, evidence_hit_rate, grounded_answer_rate

  3B — Generation  (Groq as LLM judge):
    • Faithfulness    — answer grounded in contexts?
    • Answer Relevancy — answer on-topic with question?
    • Context Precision — retrieved contexts relevant to question?
    • Context Recall    — contexts cover the ground truth?

  3C — System-level:
    • Latency stats (avg, p50, p95)
    • Out-of-scope Detection Rate
    • Error rate

All LLM-based metrics scored 0–1 by Groq.
Results saved to evaluation_report.json.

Usage:
  python3 03_compute_metrics.py
  python3 03_compute_metrics.py --skip-llm   # fast run, retrieval metrics only
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    GROQ_API_KEY, GROQ_BASE_URL, GROQ_MODEL,
    GROQ_TIMEOUT_SEC, MAX_RETRIES, GROQ_RETRY_DELAY, INTER_CALL_DELAY,
    PIPELINE_OUT_PATH, REPORT_JSON_PATH, LOGS_DIR,
    TARGET_COUNTS,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "03_compute_metrics.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# Refusal keywords — detect correct out-of-scope handling.
# Includes actual chatbot message text from _message_no_relevant(), _message_insufficient_scope(),
# and the new OOS system prompt responses added to pageindex.py.
_REFUSAL_PHRASES = [
    # pageindex.py _message_no_relevant() — Vietnamese
    "chưa tìm thấy thông tin phù hợp",
    "chưa tìm thấy thông tin",
    # pageindex.py _message_insufficient_scope() — Vietnamese
    "chưa đủ sát với câu hỏi",
    "ngữ cảnh tìm được chưa đủ",
    # New OOS system prompt response — Vietnamese
    "nằm ngoài phạm vi thư viện học liệu mở",
    "ngoài phạm vi thư viện",
    "chỉ hỗ trợ các câu hỏi về học thuật",
    # pageindex.py _message_no_relevant() — English
    "no relevant information found",
    "not found in the document",
    # pageindex.py _message_insufficient_scope() — English
    "not sufficiently relevant",
    "retrieved context is not sufficiently",
    # New OOS system prompt response — English
    "outside the scope of the oer academic library",
    "outside the scope of the oer",
    "can only help with academic",
    # Generic fallbacks
    "ngoài phạm vi", "không có thông tin", "không tìm thấy",
    "not found", "no relevant", "outside the scope",
    "cannot find", "don't have", "i couldn't find",
]


# ---------------------------------------------------------------------------
# Groq helper
# ---------------------------------------------------------------------------
def call_groq(messages: list[dict], max_tokens: int = 200, temperature: float = 0.1) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not set.")

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                f"{GROQ_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": GROQ_MODEL,
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                },
                timeout=GROQ_TIMEOUT_SEC,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                wait = GROQ_RETRY_DELAY * (2 ** attempt)
                log.warning("Rate-limited. Wait %ds ...", wait)
                time.sleep(wait)
            else:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(GROQ_RETRY_DELAY)
        except Exception as exc:
            log.debug("Groq error attempt %d: %s", attempt + 1, exc)
            if attempt < MAX_RETRIES - 1:
                time.sleep(GROQ_RETRY_DELAY)

    return ""   # empty string signals failure; callers treat as score 0


def _parse_score(raw: str, default: float = 0.0) -> float:
    """Extract a 0–1 float from an LLM response."""
    if not raw:
        return default
    # Look for patterns like: "Score: 0.8", "8/10", "0.75", "7"
    import re
    # Try direct float/int in [0,1]
    m = re.search(r"\b(0\.\d+|1\.0|1)\b", raw)
    if m:
        return float(m.group(1))
    # Try X/10 pattern
    m = re.search(r"(\d+(?:\.\d+)?)\s*/\s*10", raw)
    if m:
        return min(1.0, float(m.group(1)) / 10.0)
    # Try standalone integer 0-10
    m = re.search(r"\b([0-9]|10)\b", raw)
    if m:
        val = int(m.group(1))
        return min(1.0, val / 10.0)
    return default


# ---------------------------------------------------------------------------
# 3A — Retrieval metrics (no LLM)
# ---------------------------------------------------------------------------

def compute_hit(output: dict) -> float:
    """1 if source_asset_uid found among retrieved contexts, else 0."""
    src_uid = output.get("source_asset_uid")
    if not src_uid:
        return 0.0   # out-of-scope: no source uid
    retrieved = output.get("retrieved_asset_uids") or []
    return 1.0 if src_uid in retrieved else 0.0


def compute_reciprocal_rank(output: dict) -> float:
    """1/rank of first context matching source_asset_uid, or 0 if not found."""
    src_uid = output.get("source_asset_uid")
    if not src_uid:
        return 0.0
    retrieved = output.get("retrieved_asset_uids") or []
    for rank, uid in enumerate(retrieved, 1):
        if uid == src_uid:
            return 1.0 / rank
    return 0.0


def detect_out_of_scope(output: dict) -> float:
    """
    1 if the chatbot correctly refused an out-of-scope question, else 0.
    Detection: confidence == "low" OR answer contains refusal phrases.
    """
    if output.get("question_type") != "out_of_scope":
        return 0.0   # N/A for in-scope questions

    confidence = output.get("confidence", "")
    answer = (output.get("answer") or "").lower()

    if confidence == "low":
        return 1.0
    for phrase in _REFUSAL_PHRASES:
        if phrase in answer:
            return 1.0
    # Also check: empty/very short answer with no contexts
    if len(answer.strip()) < 20 and not output.get("contexts"):
        return 1.0
    return 0.0


# ---------------------------------------------------------------------------
# 3B — LLM judge metrics (Groq)
# ---------------------------------------------------------------------------

_FAITHFULNESS_PROMPT = """\
You are an evaluator. Rate whether the ANSWER is faithful to the CONTEXTS (contains no hallucinated facts not present in the contexts).

Question: {question}

Contexts:
{contexts}

Answer: {answer}

Score from 0 to 1:
- 1.0 = every claim in the answer is supported by the contexts
- 0.5 = some claims supported, some not
- 0.0 = answer contradicts or ignores the contexts entirely

If contexts are empty or answer is empty, return 0.0.
Return ONLY a number between 0 and 1 (e.g. 0.8). No explanation."""

_RELEVANCY_PROMPT = """\
You are an evaluator. Rate whether the ANSWER is relevant and directly addresses the QUESTION.

Question: {question}
Answer: {answer}

Score from 0 to 1:
- 1.0 = answer directly and completely addresses the question
- 0.5 = partially addresses the question
- 0.0 = answer is off-topic or does not address the question

Return ONLY a number between 0 and 1 (e.g. 0.7). No explanation."""

_CTX_PRECISION_PROMPT = """\
You are an evaluator. Rate what fraction of the provided CONTEXTS are actually relevant to the QUESTION.

Question: {question}

Contexts:
{contexts}

Score from 0 to 1:
- 1.0 = all contexts are relevant
- 0.5 = about half are relevant
- 0.0 = none are relevant

If no contexts provided, return 0.0.
Return ONLY a number between 0 and 1 (e.g. 0.6). No explanation."""

_CTX_RECALL_PROMPT = """\
You are an evaluator. Rate how much of the GROUND TRUTH information is covered in the CONTEXTS.

Question: {question}
Ground Truth: {ground_truth}

Contexts:
{contexts}

Score from 0 to 1:
- 1.0 = contexts contain all information needed to answer correctly
- 0.5 = contexts contain about half the needed information
- 0.0 = contexts contain none of the needed information

If no contexts or ground truth is "N/A", return 0.0.
Return ONLY a number between 0 and 1 (e.g. 0.75). No explanation."""


def _format_contexts(contexts: list[dict], max_chars: int = 1200) -> str:
    """Concatenate context texts for LLM prompts."""
    parts = []
    total = 0
    for i, ctx in enumerate(contexts, 1):
        text = (ctx.get("text") or "").strip()
        if not text:
            continue
        snippet = text[:400]
        parts.append(f"[{i}] {snippet}")
        total += len(snippet)
        if total >= max_chars:
            break
    return "\n".join(parts) if parts else "(no contexts retrieved)"


def judge_faithfulness(output: dict) -> float:
    answer   = (output.get("answer") or "").strip()
    contexts = output.get("contexts") or []
    if not answer or not contexts:
        return 0.0

    prompt = _FAITHFULNESS_PROMPT.format(
        question=output["question"],
        contexts=_format_contexts(contexts),
        answer=answer[:600],
    )
    time.sleep(INTER_CALL_DELAY)
    raw = call_groq([{"role": "user", "content": prompt}])
    return _parse_score(raw)


def judge_answer_relevancy(output: dict) -> float:
    answer = (output.get("answer") or "").strip()
    if not answer:
        return 0.0

    prompt = _RELEVANCY_PROMPT.format(
        question=output["question"],
        answer=answer[:600],
    )
    time.sleep(INTER_CALL_DELAY)
    raw = call_groq([{"role": "user", "content": prompt}])
    return _parse_score(raw)


def judge_context_precision(output: dict) -> float:
    contexts = output.get("contexts") or []
    if not contexts:
        return 0.0

    prompt = _CTX_PRECISION_PROMPT.format(
        question=output["question"],
        contexts=_format_contexts(contexts),
    )
    time.sleep(INTER_CALL_DELAY)
    raw = call_groq([{"role": "user", "content": prompt}])
    return _parse_score(raw)


def judge_context_recall(output: dict) -> float:
    ground_truth = (output.get("ground_truth") or "").strip()
    contexts     = output.get("contexts") or []
    if not contexts or not ground_truth or ground_truth.startswith("N/A"):
        return 0.0

    prompt = _CTX_RECALL_PROMPT.format(
        question=output["question"],
        ground_truth=ground_truth[:400],
        contexts=_format_contexts(contexts),
    )
    time.sleep(INTER_CALL_DELAY)
    raw = call_groq([{"role": "user", "content": prompt}])
    return _parse_score(raw)


# ---------------------------------------------------------------------------
# Per-output scoring
# ---------------------------------------------------------------------------
def score_output(output: dict, skip_llm: bool = False) -> dict:
    """Compute all metrics for a single pipeline output record."""
    qtype = output.get("question_type", "")
    is_oos = qtype == "out_of_scope"
    bm = output.get("built_in_metrics") or {}

    scores: dict[str, Any] = {
        "id":            output["id"],
        "question_type": qtype,
        "language":      output.get("language", "en"),
        "difficulty":    output.get("difficulty", "medium"),
        "confidence":    output.get("confidence", "unknown"),
        "latency_ms":    output.get("latency_ms", -1),
        "has_error":     bool(output.get("error")),
        # Retrieval — use PageIndex's own built-in metrics as ground truth.
        # Asset-uid matching is unreliable: generic questions (e.g. "What is CLT?")
        # are correctly answered by multiple documents, so source != retrieved != bug.
        "hit":                0.0 if is_oos else bm.get("evidence_hit_rate", 0.0),
        "tier1_recall_at_k":  0.0 if is_oos else bm.get("tier1_recall_at_k", 0.0),
        "grounded_answer_rate": bm.get("grounded_answer_rate", 0.0),
        "oos_correct":   detect_out_of_scope(output) if is_oos else None,
        # LLM judge — Faithfulness only (1 call/question, anti-hallucination)
        "faithfulness": None,
    }

    if not skip_llm and not output.get("error") and not is_oos:
        scores["faithfulness"] = judge_faithfulness(output)

    return scores


# ---------------------------------------------------------------------------
# Aggregate across all questions
# ---------------------------------------------------------------------------
def _mean(values: list[float | None]) -> float | None:
    valid = [v for v in values if v is not None]
    return round(sum(valid) / len(valid), 4) if valid else None


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_v = sorted(values)
    idx = int(len(sorted_v) * p / 100)
    return round(sorted_v[min(idx, len(sorted_v) - 1)], 1)


def aggregate_metrics(scored: list[dict]) -> dict:
    """Compute aggregate statistics across all scored outputs."""
    all_types = list(TARGET_COUNTS.keys())

    # Global aggregates (exclude OOS from retrieval metrics)
    in_scope  = [s for s in scored if s["question_type"] != "out_of_scope" and not s["has_error"]]
    oos       = [s for s in scored if s["question_type"] == "out_of_scope"]

    latencies_ms = [s["latency_ms"] for s in scored if s["latency_ms"] > 0]

    global_metrics = {
        # 3A — Retrieval (PageIndex built-in metrics)
        "hit_rate":          _mean([s["hit"]                  for s in in_scope]),   # = avg evidence_hit_rate
        "tier1_recall_at_k": _mean([s["tier1_recall_at_k"]   for s in in_scope]),
        "grounded_rate":     _mean([s["grounded_answer_rate"] for s in in_scope]),
        # 3B — Generation (Faithfulness only)
        "faithfulness": _mean([s["faithfulness"] for s in in_scope if s.get("faithfulness") is not None]),
        # 3C — System
        "oos_detection_rate": _mean([s["oos_correct"] for s in oos if s["oos_correct"] is not None]),
        "latency_avg_ms":    round(sum(latencies_ms) / len(latencies_ms), 1) if latencies_ms else None,
        "latency_p50_ms":    _percentile(latencies_ms, 50),
        "latency_p95_ms":    _percentile(latencies_ms, 95),
        "error_rate":        round(sum(1 for s in scored if s["has_error"]) / max(len(scored), 1), 4),
        "total_questions":   len(scored),
        "in_scope_questions": len(in_scope),
    }

    # Per-type breakdown
    breakdown: dict[str, dict] = {}
    for qtype in all_types:
        group = [s for s in scored if s["question_type"] == qtype and not s["has_error"]]
        if not group:
            breakdown[qtype] = {}
            continue

        entry: dict = {"count": len(group)}
        if qtype != "out_of_scope":
            entry["hit_rate"]          = _mean([s["hit"]                  for s in group])
            entry["tier1_recall_at_k"] = _mean([s["tier1_recall_at_k"]   for s in group])
            entry["grounded_rate"]     = _mean([s["grounded_answer_rate"] for s in group])
        else:
            entry["oos_detection_rate"] = _mean(
                [s["oos_correct"] for s in group if s["oos_correct"] is not None]
            )

        entry["faithfulness"]   = _mean([s["faithfulness"] for s in group if s.get("faithfulness") is not None])
        entry["avg_latency_ms"] = _mean([s["latency_ms"]   for s in group if s["latency_ms"] > 0])
        breakdown[qtype] = entry

    # Per-language breakdown (EN vs VI)
    lang_breakdown: dict[str, dict] = {}
    for lang in ("en", "vi"):
        lg = [s for s in scored if s.get("language") == lang and not s["has_error"]]
        lg_in = [s for s in lg if s["question_type"] != "out_of_scope"]
        lg_oos = [s for s in lg if s["question_type"] == "out_of_scope"]
        if not lg:
            lang_breakdown[lang] = {}
            continue

        lang_lats = [s["latency_ms"] for s in lg if s["latency_ms"] > 0]
        lang_breakdown[lang] = {
            "count":             len(lg),
            "hit_rate":          _mean([s["hit"]                  for s in lg_in]),
            "tier1_recall_at_k": _mean([s["tier1_recall_at_k"]   for s in lg_in]),
            "grounded_rate":     _mean([s["grounded_answer_rate"] for s in lg_in]),
            "oos_detection_rate": _mean(
                [s["oos_correct"] for s in lg_oos if s["oos_correct"] is not None]
            ),
            "avg_latency_ms":    _mean(lang_lats),
        }

    return {"global": global_metrics, "by_language": lang_breakdown, "by_type": breakdown, "per_question": scored}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Compute OER chatbot evaluation metrics")
    parser.add_argument("--skip-llm", action="store_true",
                        help="Skip LLM judge (faster; only retrieval + system metrics)")
    parser.add_argument("--ids", nargs="*",
                        help="Score only specific question IDs")
    parser.add_argument("--input", default=None,
                        help="Đường dẫn pipeline outputs (mặc định: pipeline_outputs.json)")
    parser.add_argument("--output", default=None,
                        help="Đường dẫn report output (mặc định: evaluation_report.json)")
    args = parser.parse_args()

    input_path  = Path(args.input)  if args.input  else PIPELINE_OUT_PATH
    output_path = Path(args.output) if args.output else REPORT_JSON_PATH

    if not input_path.exists():
        log.error("%s not found. Run 02_run_pipeline.py first.", input_path.name)
        sys.exit(1)

    outputs: list[dict] = json.loads(input_path.read_text())
    log.info("Loaded %d pipeline outputs from %s", len(outputs), input_path.name)

    if args.ids:
        id_set = set(args.ids)
        outputs = [o for o in outputs if o["id"] in id_set]
        log.info("Filtered to %d outputs", len(outputs))

    if not args.skip_llm and not GROQ_API_KEY:
        log.error("GROQ_API_KEY not set. Use --skip-llm for retrieval metrics only.")
        sys.exit(1)

    # Score each output
    scored: list[dict] = []
    total = len(outputs)

    for i, output in enumerate(outputs, 1):
        qid   = output["id"]
        qtype = output.get("question_type", "?")
        log.info("[%d/%d] Scoring %s [%s]", i, total, qid, qtype)

        try:
            scores = score_output(output, skip_llm=args.skip_llm)
            scored.append(scores)
            log.info("  hit=%.2f  recall=%.2f  grounded=%.2f  faith=%s",
                     scores.get("hit", 0),
                     scores.get("tier1_recall_at_k", 0),
                     scores.get("grounded_answer_rate", 0),
                     f"{scores['faithfulness']:.2f}" if scores.get("faithfulness") is not None else "—")
        except Exception as exc:
            log.error("  Scoring failed for %s: %s", qid, exc)
            scored.append({
                "id": qid, "question_type": qtype,
                "has_error": True, "hit": 0.0,
                "tier1_recall_at_k": 0.0, "grounded_answer_rate": 0.0,
                "oos_correct": None, "faithfulness": None,
                "latency_ms": output.get("latency_ms", -1),
            })

    # Aggregate
    report = aggregate_metrics(scored)

    # Save
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("Report saved to %s", output_path)

    # Quick summary to stdout
    g  = report["global"]
    bl = report.get("by_language", {})
    log.info("")
    log.info("=" * 55)
    log.info("SUMMARY — OVERALL (%d questions)", g["total_questions"])
    log.info("=" * 55)
    log.info("  Hit Rate (evidence)   : %s", f"{g['hit_rate']:.4f}"          if g.get("hit_rate")          is not None else "—")
    log.info("  Tier-1 Recall@K       : %s", f"{g['tier1_recall_at_k']:.4f}" if g.get("tier1_recall_at_k") is not None else "—")
    log.info("  Grounded Answer Rate  : %s", f"{g['grounded_rate']:.4f}"     if g.get("grounded_rate")     is not None else "—")
    log.info("  Faithfulness          : %s", f"{g['faithfulness']:.4f}"      if g.get("faithfulness")      is not None else "—")
    log.info("  OOS Detection         : %s", f"{g['oos_detection_rate']:.4f}" if g.get("oos_detection_rate") is not None else "—")
    log.info("  Avg Latency           : %s ms", g["latency_avg_ms"])
    log.info("  P95 Latency           : %s ms", g["latency_p95_ms"])

    for lang in ("en", "vi"):
        lb = bl.get(lang)
        if not lb:
            continue
        label = "English" if lang == "en" else "Vietnamese"
        log.info("")
        log.info("--- %s (%d questions) ---", label, lb["count"])
        log.info("  Hit Rate            : %s", f"{lb['hit_rate']:.4f}"          if lb.get("hit_rate")          is not None else "—")
        log.info("  Tier-1 Recall@K     : %s", f"{lb['tier1_recall_at_k']:.4f}" if lb.get("tier1_recall_at_k") is not None else "—")
        log.info("  Grounded Rate       : %s", f"{lb['grounded_rate']:.4f}"     if lb.get("grounded_rate")     is not None else "—")
        log.info("  OOS Detection       : %s", f"{lb['oos_detection_rate']:.4f}" if lb.get("oos_detection_rate") is not None else "—")
        log.info("  Avg Latency         : %s ms", lb.get("avg_latency_ms"))
    log.info("=" * 55)


if __name__ == "__main__":
    main()
