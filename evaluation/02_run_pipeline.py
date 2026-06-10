"""
Script 2 — Run all test questions through the chatbot pipeline.

For each question in test_set.json:
  - POST to /api/ask
  - Measure wall-clock latency
  - Collect answer, contexts, sources, confidence, built-in metrics
  - Handle timeouts / rate-limits with retry

Resume-safe: questions that already have results in pipeline_outputs.json
are skipped unless --force is used.

Usage:
  python3 02_run_pipeline.py
  python3 02_run_pipeline.py --force     # re-run everything
  python3 02_run_pipeline.py --ids Q001 Q005   # run specific questions only
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
    API_BASE, API_TIMEOUT_SEC,
    MAX_RETRIES, PIPELINE_BATCH_PAUSE,
    TEST_SET_PATH, PIPELINE_OUT_PATH, LOGS_DIR,
    GROQ_API_KEY, GROQ_BASE_URL, GROQ_MODEL,
    GROQ_TIMEOUT_SEC,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "02_run_pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# VI→EN pre-translation (improved Groq prompt)
# ---------------------------------------------------------------------------
_VI_TO_EN_PROMPT_TEMPLATE = """\
You are an academic search query translator for an OER (Open Educational Resources) library.
All indexed documents are in English. Translate the Vietnamese question below into the best \
possible English search query for a BM25 keyword search engine.

Rules:
- Return ONLY a JSON object, no explanation.
- "query_en" must be a natural English question or phrase (not just keywords).
- "keywords_en" must include: the direct translation, academic synonyms, alternative phrasings, \
  and common abbreviations used in English textbooks.
- Include at least 5-8 keywords/phrases in keywords_en.
- Preserve proper nouns and technical terms exactly.

Schema:
{{
  "query_en": "best single English search query",
  "keywords_en": ["term1", "synonym", "alternative phrasing", "abbreviation", ...]
}}

Vietnamese question: {question}"""


def translate_vi_to_en(question: str) -> tuple[str, list[str]]:
    """
    Translate a Vietnamese question to English using Groq with an academic prompt.
    Returns (query_en, keywords_en). Falls back to original question on failure.
    """
    if not GROQ_API_KEY:
        log.warning("GROQ_API_KEY not set — skipping pre-translation.")
        return question, []

    prompt = _VI_TO_EN_PROMPT_TEMPLATE.format(question=question)
    try:
        resp = requests.post(
            f"{GROQ_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 300,
                "temperature": 0.1,
                "response_format": {"type": "json_object"},
            },
            timeout=GROQ_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        data = json.loads(raw)
        query_en   = str(data.get("query_en") or "").strip() or question
        keywords   = [str(k).strip() for k in (data.get("keywords_en") or []) if str(k).strip()]
        # Combine: "<query_en> keyword1 keyword2 ..." for richer BM25 matching
        combined = query_en
        if keywords:
            combined = f"{query_en} {' '.join(keywords)}"
        return combined, keywords
    except Exception as exc:
        log.warning("Pre-translation failed (%s) — using original question.", exc)
        return question, []


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------
def call_pipeline(question: str, top_k: int = 5, language: str | None = None) -> tuple[dict[str, Any], float]:
    """
    POST question to /api/ask.
    Returns (response_dict, latency_ms).
    Raises on persistent failure.
    """
    payload: dict[str, Any] = {"question": question, "top_k": top_k}
    if language:
        payload["language"] = language  # vi → chatbot trả lời tiếng Việt; en → tiếng Anh

    for attempt in range(MAX_RETRIES):
        t0 = time.perf_counter()
        try:
            resp = requests.post(
                f"{API_BASE}/ask",
                json=payload,
                timeout=API_TIMEOUT_SEC,
            )
            latency_ms = (time.perf_counter() - t0) * 1000
            resp.raise_for_status()
            return resp.json(), round(latency_ms, 1)

        except requests.Timeout:
            latency_ms = (time.perf_counter() - t0) * 1000
            log.warning("Attempt %d: timeout after %.0f ms", attempt + 1, latency_ms)
            if attempt < MAX_RETRIES - 1:
                time.sleep(3)
        except requests.HTTPError as exc:
            latency_ms = (time.perf_counter() - t0) * 1000
            log.warning("Attempt %d: HTTP %s", attempt + 1, exc.response.status_code)
            if attempt < MAX_RETRIES - 1:
                time.sleep(3)
        except Exception as exc:
            latency_ms = (time.perf_counter() - t0) * 1000
            log.warning("Attempt %d: %s", attempt + 1, exc)
            if attempt < MAX_RETRIES - 1:
                time.sleep(3)

    raise RuntimeError(f"Pipeline call failed after {MAX_RETRIES} attempts.")


def _make_error_output(question_row: dict, error_msg: str) -> dict:
    """Return a placeholder output when the API call fails."""
    return {
        "id": question_row["id"],
        "question": question_row["question"],
        "language": question_row.get("language", "en"),
        "answer": "",
        "contexts": [],
        "sources": [],
        "ground_truth": question_row["ground_truth"],
        "source_asset_uid": question_row.get("source_asset_uid"),
        "source_title": question_row.get("source_title"),
        "question_type": question_row["question_type"],
        "difficulty": question_row.get("difficulty", "medium"),
        "confidence": "error",
        "search_mode": "pageindex",
        "retrieved_asset_uids": [],
        "latency_ms": -1,
        "built_in_metrics": {},
        "error": error_msg,
    }


def _extract_output(question_row: dict, api_resp: dict, latency_ms: float) -> dict:
    """Flatten API response into a clean pipeline output record."""
    contexts = api_resp.get("contexts") or []
    sources  = api_resp.get("sources")  or []

    # Collect all asset_uids returned by the chatbot (for hit-rate checking)
    retrieved_uids: list[str] = []
    for ctx in contexts:
        uid = ctx.get("asset_uid") or ctx.get("chunk_id", "")
        if uid and uid not in retrieved_uids:
            retrieved_uids.append(uid)

    # Clean contexts: keep only evaluation-relevant fields
    clean_contexts: list[dict] = []
    for ctx in contexts:
        clean_contexts.append({
            "asset_uid":     ctx.get("asset_uid", ""),
            "chunk_id":      ctx.get("chunk_id", ""),
            "page_no":       ctx.get("page_no"),
            "text":          (ctx.get("text") or "")[:1500],   # truncate for storage
            "title":         ctx.get("title", ""),
            "section_title": ctx.get("section_title", ""),
            "retrieval_score": ctx.get("retrieval_score"),
        })

    built_in = api_resp.get("metrics") or {}

    return {
        "id":               question_row["id"],
        "question":         question_row["question"],
        "language":         question_row.get("language", "en"),
        "answer":           api_resp.get("answer", ""),
        "contexts":         clean_contexts,
        "sources":          sources[:5],  # top 5 sources
        "ground_truth":     question_row["ground_truth"],
        "source_asset_uid": question_row.get("source_asset_uid"),
        "source_title":     question_row.get("source_title"),
        "question_type":    question_row["question_type"],
        "difficulty":       question_row.get("difficulty", "medium"),
        "confidence":       api_resp.get("confidence", "unknown"),
        "search_mode":      api_resp.get("search_mode", "pageindex"),
        "retrieved_asset_uids": retrieved_uids,
        "latency_ms":       latency_ms,
        "built_in_metrics": {
            "tier1_recall_at_k":   built_in.get("tier1_recall_at_k", 0.0),
            "evidence_hit_rate":   built_in.get("evidence_hit_rate", 0.0),
            "grounded_answer_rate": built_in.get("grounded_answer_rate", 0.0),
            "pages_loaded_total":  built_in.get("pages_loaded_total", 0),
            "pages_hit_total":     built_in.get("pages_hit_total", 0),
        },
        "error": None,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Run OER chatbot evaluation pipeline")
    parser.add_argument("--force", action="store_true",
                        help="Re-run all questions, ignoring cached results")
    parser.add_argument("--ids", nargs="*",
                        help="Run only specific question IDs (e.g. Q001 Q005)")
    parser.add_argument("--top-k", type=int, default=5,
                        help="top_k contexts to retrieve per question (default: 5)")
    parser.add_argument("--test-set", default=None,
                        help="Đường dẫn file test set (mặc định: test_set.json)")
    parser.add_argument("--output", default=None,
                        help="Đường dẫn file output (mặc định: pipeline_outputs.json)")
    parser.add_argument("--translate-vi", action="store_true",
                        help="Pre-translate VI questions to EN via Groq before calling API")
    args = parser.parse_args()

    input_path  = Path(args.test_set) if args.test_set else TEST_SET_PATH
    output_path = Path(args.output)   if args.output   else PIPELINE_OUT_PATH

    # Load test set
    if not input_path.exists():
        log.error("%s not found. Run generate script first.", input_path.name)
        sys.exit(1)
    test_set: list[dict] = json.loads(input_path.read_text())
    log.info("Loaded %d questions from %s", len(test_set), input_path.name)

    # Filter to specific IDs if requested
    if args.ids:
        id_set = set(args.ids)
        test_set = [q for q in test_set if q["id"] in id_set]
        log.info("Filtered to %d questions: %s", len(test_set), args.ids)

    # Load existing results for resume support
    existing: dict[str, dict] = {}
    if output_path.exists() and not args.force:
        saved = json.loads(output_path.read_text())
        existing = {r["id"]: r for r in saved}
        log.info("Loaded %d existing results (resume mode)", len(existing))

    results: list[dict] = list(existing.values())
    existing_ids = set(existing.keys())

    # API health check
    try:
        r = requests.get(f"{API_BASE}/health", timeout=10)
        r.raise_for_status()
        log.info("API health: OK")
    except Exception as exc:
        log.error("API unreachable: %s", exc)
        sys.exit(1)

    # Run pipeline
    to_run = [q for q in test_set if q["id"] not in existing_ids]
    log.info("Questions to run: %d (skip %d already done)", len(to_run), len(existing_ids))

    success_count = 0
    error_count   = 0
    total_latency = 0.0

    if args.translate_vi and not GROQ_API_KEY:
        log.error("--translate-vi requires GROQ_API_KEY to be set in config.")
        sys.exit(1)
    if args.translate_vi:
        log.info("VI→EN pre-translation enabled (Groq: %s)", GROQ_MODEL)

    for i, question_row in enumerate(to_run, 1):
        qid   = question_row["id"]
        qtext = question_row["question"]
        qtype = question_row["question_type"]
        qlang = question_row.get("language", "en")

        log.info("[%d/%d] %s [%s] %.60s ...", i, len(to_run), qid, qtype, qtext)

        # Pre-translate VI → EN if requested
        translated_question = None
        if args.translate_vi and qlang == "vi":
            translated_question, kw = translate_vi_to_en(qtext)
            log.info("  [translate] %s", translated_question[:100])

        send_text = translated_question if translated_question else qtext
        send_lang = "en" if translated_question else qlang

        try:
            api_resp, latency_ms = call_pipeline(
                send_text, top_k=args.top_k, language=send_lang
            )
            output = _extract_output(question_row, api_resp, latency_ms)
            if translated_question:
                output["translated_question"] = translated_question
            success_count += 1
            total_latency += latency_ms
            n_ctx = len(output["contexts"])
            log.info("  → answer len=%d  contexts=%d  confidence=%s  latency=%.0fms",
                     len(output["answer"]), n_ctx,
                     output["confidence"], latency_ms)
        except Exception as exc:
            log.error("  → FAILED: %s", exc)
            output = _make_error_output(question_row, str(exc))
            error_count += 1

        results.append(output)

        # Save after every question (crash-safe)
        output_path.write_text(
            json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # Pause every N questions to avoid overloading the API
        if i % PIPELINE_BATCH_PAUSE == 0 and i < len(to_run):
            log.info("--- Batch pause %ds ---", PIPELINE_BATCH_PAUSE)
            time.sleep(PIPELINE_BATCH_PAUSE)

    # Final summary
    log.info("")
    log.info("=" * 50)
    log.info("Pipeline run complete")
    log.info("  Total processed : %d", success_count + error_count)
    log.info("  Success         : %d", success_count)
    log.info("  Errors          : %d", error_count)
    if success_count:
        log.info("  Avg latency     : %.0f ms", total_latency / success_count)
    log.info("  Results saved to: %s", output_path)


if __name__ == "__main__":
    main()
