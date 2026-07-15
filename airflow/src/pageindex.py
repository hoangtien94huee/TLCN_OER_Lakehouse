"""
PageIndex retrieval engine for OER QA.

Vectorless retrieval flow:
1. Select a document using metadata, TOC, and reference-subject signals.
2. Inspect document structure to narrow likely sections.
3. Read a small page range directly from the PDF.
4. Expand the range gradually if evidence is still weak.
"""

from __future__ import annotations

from collections import OrderedDict
from datetime import timedelta
import json
import logging
import os
import re
import socket
import struct
import time
import unicodedata
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


try:
    from minio import Minio

    MINIO_AVAILABLE = True
except ImportError:
    Minio = None  # type: ignore
    MINIO_AVAILABLE = False

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    SPARK_AVAILABLE = True
except ImportError:
    SparkSession = None  # type: ignore
    F = None  # type: ignore
    SPARK_AVAILABLE = False

try:
    import pdfplumber

    PDFPLUMBER_AVAILABLE = True
except ImportError:
    pdfplumber = None  # type: ignore
    PDFPLUMBER_AVAILABLE = False

try:
    from pypdf import PdfReader as PyPDFReader

    PYPDF_AVAILABLE = True
except ImportError:
    try:
        import pypdf

        PyPDFReader = pypdf.PdfReader
        PYPDF_AVAILABLE = True
    except ImportError:
        PyPDFReader = None  # type: ignore
        PYPDF_AVAILABLE = False


logger = logging.getLogger(__name__)

from pageindex_types import PageIndexError, QueryBundle  # re-export
from pageindex_helpers import (
    _strip_surrogate_chars,
    _normalize_pdf_text,
    _ascii_fold,
    _tokenize,
    _derive_en_keywords_from_vi,
    _derive_vi_keywords_from_en,
    _detect_query_language,
    _detect_lang,
    _parse_moodle_context,
    _strip_moodle_context,
    _detect_recommendation_intent,
    _recommendation_intent_ambiguous,
    _is_recommendation_query,
    _detect_find_material_intent,
    _detect_query_intent,
    _is_definition_query,
    _extract_definition_target,
    _contains_unresolved_placeholder,
    _is_implicit_concept_placeholder,
    _extract_course_name_hint,
    _extract_document_title_hint,
    _extract_section_name_hint,
    _build_course_scope_profile,
    _evaluate_course_scope_text,
    _extract_requested_concept,
    _has_example_cue,
    _has_definition_cue,
    _has_targeted_definition_cue,
    _is_english_dominant_text,
    _estimate_transcript_noise,
    _resolve_answer_language,
    _message_no_relevant,
    _message_unresolved_concept,
    _message_insufficient_scope,
    _is_obviously_out_of_scope,
    _message_no_document,
    _message_time_budget,
    _overlap_score,
    _estimate_formula_density,
    _estimate_garbled_text_ratio,
    _dedupe_keep_order,
    _detect_default_gateway_ipv4,
    _safe_json_loads,
    _to_python,
    _clamp_page_range,
    _range_to_expr,
    _parse_pages_expr,
    _collapse_pages,
    _phrase_overlap,
    _recommendation_generic_terms,
    _extract_subject_focus_terms,
    _extract_subject_focus_phrases,
    _expand_definition_target_tokens,
)
from pageindex_backend import _BackendMixin
from pageindex_retrieval import _RetrievalMixin
from pageindex_reading import _ReadingMixin
from pageindex_recommend import _RecommendMixin
from pageindex_generation import _GenerationMixin


def _is_greeting(question: str) -> bool:
    q = _ascii_fold(_strip_moodle_context(question)).strip().lower().replace("?", "").replace("!", "").replace(".", "").replace(",", "")
    greetings = {
        "chao", "chao ban", "xin chao", "hello", "hi", "chao ad", "chao bot", "chao em", "chao anh", "chao chi", "greetings", "hey", "chao ad"
    }
    return q in greetings


class PageIndexEngine(_BackendMixin, _RetrievalMixin, _ReadingMixin, _RecommendMixin, _GenerationMixin):
    def __init__(self) -> None:
        self.bucket = os.getenv("MINIO_BUCKET", "oer-lakehouse")
        self.catalog_name = os.getenv("ICEBERG_SILVER_CATALOG", "silver")
        self.database_name = os.getenv("SILVER_DATABASE", "default")
        self.documents_table = f"{self.catalog_name}.{self.database_name}.oer_documents"
        self.structure_table = f"{self.catalog_name}.{self.database_name}.oer_document_structure"
        self.resources_table = f"{self.catalog_name}.{self.database_name}.oer_resources_curated"
        self.reference_subjects_table = f"{self.catalog_name}.{self.database_name}.reference_subjects"
        self.reference_program_subject_links_table = f"{self.catalog_name}.{self.database_name}.reference_program_subject_links"

        self.spark_master = os.getenv("CHATBOT_SPARK_MASTER", os.getenv("SPARK_MASTER", os.getenv("SPARK_MASTER_URL", "local[*]")))
        default_driver_host = "127.0.0.1" if self.spark_master.startswith("local") else "oer-airflow-scraper"
        self.spark_driver_host = os.getenv("CHATBOT_SPARK_DRIVER_HOST", os.getenv("SPARK_DRIVER_HOST", default_driver_host))
        self.spark_driver_bind_address = os.getenv("CHATBOT_SPARK_DRIVER_BIND_ADDRESS", os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"))
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        self.minio_public_base_url = os.getenv("MINIO_PUBLIC_BASE_URL", "http://localhost:19000").rstrip("/")
        self.minio_presigned_expiry_seconds = max(60, int(os.getenv("MINIO_PRESIGNED_EXPIRY_SECONDS", "3600")))

        self.local_llm_backend = os.getenv("LOCAL_LLM_BACKEND", "ollama").strip().lower()
        self.local_llm_api_key = (
            os.getenv("LOCAL_LLM_API_KEY")
            or os.getenv("GROQ_API_KEY")
            or os.getenv("OPENAI_API_KEY")
            or os.getenv("GEMINI_API_KEY")
            or ""
        ).strip()
        if self.local_llm_backend == "vllm":
            default_base_url = os.getenv("LOCAL_LLM_VLLM_BASE_URL", "http://localhost:8000/v1")
            default_model = os.getenv("LOCAL_LLM_VLLM_MODEL", "Qwen/Qwen2.5-7B-Instruct")
        elif self.local_llm_backend == "groq":
            default_base_url = os.getenv("LOCAL_LLM_GROQ_BASE_URL", "https://api.groq.com/openai/v1")
            default_model = os.getenv("LOCAL_LLM_GROQ_MODEL", "llama-3.3-70b-versatile")
        elif self.local_llm_backend == "gemini":
            default_base_url = os.getenv("LOCAL_LLM_GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta")
            default_model = os.getenv("LOCAL_LLM_GEMINI_MODEL", "gemini-2.0-flash")
        else:
            default_base_url = os.getenv("LOCAL_LLM_OLLAMA_BASE_URL", "http://localhost:11434")
            default_model = os.getenv("LOCAL_LLM_OLLAMA_MODEL", "qwen2.5:7b-instruct")

        base_url_env = os.getenv("LOCAL_LLM_BASE_URL", "").strip()
        self.local_llm_base_url = (base_url_env or default_base_url).rstrip("/")
        self.local_llm_api_url = os.getenv("LOCAL_LLM_API_URL", "").strip()
        self.local_llm_health_url = os.getenv("LOCAL_LLM_HEALTH_URL", "").strip()
        model_env = os.getenv("LOCAL_LLM_MODEL", "").strip()
        self.local_llm_model = model_env or default_model
        self.local_llm_timeout = int(os.getenv("LOCAL_LLM_TIMEOUT", "180"))
        self.local_llm_connect_timeout = int(os.getenv("LOCAL_LLM_CONNECT_TIMEOUT", "1"))
        self.local_llm_probe_timeout = int(os.getenv("LOCAL_LLM_PROBE_TIMEOUT", "1"))
        self.local_llm_probe_required = os.getenv("LOCAL_LLM_PROBE_REQUIRED", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.ask_timeout = int(os.getenv("PAGEINDEX_ASK_TIMEOUT", "75"))
        self.llm_json_timeout = int(os.getenv("PAGEINDEX_LLM_JSON_TIMEOUT", "20"))
        self.llm_answer_timeout = int(os.getenv("PAGEINDEX_LLM_ANSWER_TIMEOUT", "90"))
        self.llm_context_max_chars = max(2000, int(os.getenv("PAGEINDEX_LLM_CONTEXT_MAX_CHARS", "10000")))
        self.disable_fallback = os.getenv("PAGEINDEX_DISABLE_FALLBACK", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.tier1_timeout = max(1, int(os.getenv("PAGEINDEX_TIER1_TIMEOUT", "6")))
        self.tier2_min_budget = max(1, int(os.getenv("PAGEINDEX_TIER2_MIN_BUDGET", "10")))
        fallback_urls_env = os.getenv("LOCAL_LLM_FALLBACK_BASE_URLS", "").strip()
        fallback_urls: List[str] = []
        if fallback_urls_env:
            fallback_urls.extend([p.strip().rstrip("/") for p in fallback_urls_env.split(",") if p.strip()])
        if self.local_llm_backend == "ollama":
            gateway_ip = _detect_default_gateway_ipv4()
            if gateway_ip:
                fallback_urls.append(f"http://{gateway_ip}:11434")
            fallback_urls.extend(
                [
                    "http://host.docker.internal:11434",
                    "http://gateway.docker.internal:11434",
                    "http://172.17.0.1:11434",
                    "http://localhost:11434",
                    "http://ollama:11434",
                ]
            )
        self.local_llm_fallback_base_urls = _dedupe_keep_order(fallback_urls)
        self.page_window = int(os.getenv("PAGEINDEX_INITIAL_PAGE_WINDOW", "3"))
        self.page_expand_step = int(os.getenv("PAGEINDEX_PAGE_EXPAND_STEP", "3"))
        self.page_expand_acceleration = int(os.getenv("PAGEINDEX_PAGE_EXPAND_ACCELERATION", "1"))
        self.page_max_window = int(os.getenv("PAGEINDEX_PAGE_MAX_WINDOW", "15"))
        self.max_rounds = int(os.getenv("PAGEINDEX_MAX_ROUNDS", "4"))
        self.max_document_candidates = int(os.getenv("PAGEINDEX_MAX_DOCUMENT_CANDIDATES", "3"))
        self.max_pages_per_call = int(os.getenv("PAGEINDEX_MAX_PAGES_PER_CALL", "6"))
        self.tier1_backend = os.getenv("PAGEINDEX_TIER1_BACKEND", "elasticsearch").strip().lower()
        self.tier1_es_enabled = os.getenv("PAGEINDEX_TIER1_ES_ENABLED", "1").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.tier1_es_host = os.getenv("PAGEINDEX_TIER1_ES_HOST", os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")).strip().rstrip("/")
        self.tier1_es_index = os.getenv("PAGEINDEX_TIER1_ES_INDEX", "oer_resources_tier1").strip()
        # --- Tier-2 page-level inverted index (BM25 over per-page text) ---
        # Backend "pdf" (default) keeps the legacy flow: load PDF range + linear
        # keyword overlap. Backend "elasticsearch" retrieves the best pages of the
        # selected document directly from an inverted index (oer_pages_tier2),
        # which improves cross-lingual (VI) content retrieval. Falls back to PDF
        # per-document when the page index has no data for that asset.
        self.tier2_backend = os.getenv("PAGEINDEX_TIER2_BACKEND", "pdf").strip().lower()
        self.tier2_es_enabled = os.getenv("PAGEINDEX_TIER2_ES_ENABLED", "1").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.tier2_es_index = os.getenv("PAGEINDEX_TIER2_ES_INDEX", "oer_pages_tier2").strip()
        self.tier2_es_pages = max(1, int(os.getenv("PAGEINDEX_TIER2_ES_PAGES", "8")))
        # Cross-book mode (Phuong an 2): for content questions, run ONE BM25 query
        # over the page index across ALL books, take the best pages directly, and
        # answer in a single Groq call — instead of the per-document round loop.
        # This bypasses the Tier-1 wrong-book bottleneck while keeping latency low
        # (one ~30ms ES query, no PDF reads). Requires tier2 ES backend.
        self.tier2_crossbook = os.getenv("PAGEINDEX_TIER2_CROSSBOOK", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.tier2_crossbook_pages = max(1, int(os.getenv("PAGEINDEX_TIER2_CROSSBOOK_PAGES", "8")))
        # Số nguồn HIỂN THỊ cho người dùng (vẫn lấy tier2_crossbook_pages trang cho LLM
        # tổng hợp, nhưng chỉ trả về top-N trang làm nguồn để giao diện gọn).
        self.tier2_crossbook_show = max(1, int(os.getenv("PAGEINDEX_TIER2_CROSSBOOK_SHOW", "3")))
        # Cross-book has no Tier-1/judge gate, and BM25 cannot distinguish
        # out-of-scope questions (they still match some page lexically). So add
        # one semantic LLM scope-check before answering: if the question is not
        # answerable from academic OER, refuse (restores OOS detection).
        self.tier2_crossbook_scope_check = os.getenv("PAGEINDEX_TIER2_CROSSBOOK_SCOPE_CHECK", "1").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        # Course-scoping: when the Moodle course_name matches a curated course in
        # course_book_map.json, restrict cross-book retrieval to that course's
        # books (so it answers from the course's textbooks and naturally refuses
        # off-course questions). Falls back to global cross-book otherwise.
        self.tier2_course_scoped = os.getenv("PAGEINDEX_TIER2_COURSE_SCOPED", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.tier2_course_map_path = os.getenv(
            "PAGEINDEX_TIER2_COURSE_MAP_PATH",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "course_book_map.json"),
        )
        self._course_map: Optional[Dict[str, Any]] = None
        self.tier1_es_username = os.getenv("PAGEINDEX_TIER1_ES_USERNAME", os.getenv("ELASTICSEARCH_USERNAME", "")).strip()
        self.tier1_es_password = os.getenv("PAGEINDEX_TIER1_ES_PASSWORD", os.getenv("ELASTICSEARCH_PASSWORD", "")).strip()
        self.tier1_es_timeout = max(1.0, float(os.getenv("PAGEINDEX_TIER1_ES_TIMEOUT", "4")))
        self.tier1_es_candidate_pool = max(5, int(os.getenv("PAGEINDEX_TIER1_ES_CANDIDATE_POOL", "36")))
        self.tier1_es_topk_buffer = max(1, int(os.getenv("PAGEINDEX_TIER1_ES_TOPK_BUFFER", "4")))
        self.tier1_bm25_bonus_weight = max(0.0, float(os.getenv("PAGEINDEX_TIER1_BM25_BONUS_WEIGHT", "0.35")))
        self.tier1_bm25_bonus_cap = max(0.0, float(os.getenv("PAGEINDEX_TIER1_BM25_BONUS_CAP", "8.0")))
        self.tier1_min_bm25 = max(0.0, float(os.getenv("PAGEINDEX_TIER1_MIN_BM25", "1.2")))
        self.use_query_language_as_doc_filter = os.getenv("PAGEINDEX_USE_QUERY_LANGUAGE_AS_DOC_FILTER", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.cache_enabled = os.getenv("PAGEINDEX_CACHE_ENABLED", "1").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.cache_max_items = max(16, int(os.getenv("PAGEINDEX_CACHE_MAX_ITEMS", "256")))

        self._spark: Optional[SparkSession] = None
        self._minio_client: Optional[Minio] = None
        self._minio_signer_client: Optional[Minio] = None
        self._reference_cache: Optional[Dict[str, Any]] = None
        self._document_meta_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._structure_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._page_text_cache: "OrderedDict[str, Dict[int, str]]" = OrderedDict()
        self._page_content_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._pdf_bytes_cache: "OrderedDict[str, bytes]" = OrderedDict()
        self._local_llm_unavailable = False
        self._local_llm_checked = False
        self._local_llm_last_error = ""

    def get_runtime_config(self) -> Dict[str, Any]:
        return {
            "retrieval_mode": "pageindex",
            "vector_db_enabled": False,
            "catalog_name": self.catalog_name,
            "database_name": self.database_name,
            "documents_table": self.documents_table,
            "structure_table": self.structure_table,
            "resources_table": self.resources_table,
            "reference_subjects_table": self.reference_subjects_table,
            "reference_program_subject_links_table": self.reference_program_subject_links_table,
            "local_llm_backend": self.local_llm_backend,
            "local_llm_base_url": self.local_llm_base_url,
            "local_llm_api_url": self.local_llm_api_url,
            "local_llm_health_url": self.local_llm_health_url,
            "local_llm_fallback_base_urls": self.local_llm_fallback_base_urls,
            "local_llm_model": self.local_llm_model,
            "local_llm_timeout": self.local_llm_timeout,
            "local_llm_connect_timeout": self.local_llm_connect_timeout,
            "local_llm_probe_timeout": self.local_llm_probe_timeout,
            "local_llm_probe_required": self.local_llm_probe_required,
            "pageindex_ask_timeout": self.ask_timeout,
            "pageindex_tier1_timeout": self.tier1_timeout,
            "pageindex_tier2_min_budget": self.tier2_min_budget,
            "pageindex_llm_json_timeout": self.llm_json_timeout,
            "pageindex_llm_answer_timeout": self.llm_answer_timeout,
            "pageindex_llm_context_max_chars": self.llm_context_max_chars,
            "pageindex_disable_fallback": self.disable_fallback,
            "tier1_backend": self.tier1_backend,
            "tier1_es_enabled": self.tier1_es_enabled,
            "tier1_es_host": self.tier1_es_host,
            "tier1_es_index": self.tier1_es_index,
            "tier1_es_timeout": self.tier1_es_timeout,
            "tier1_es_candidate_pool": self.tier1_es_candidate_pool,
            "tier1_es_topk_buffer": self.tier1_es_topk_buffer,
            "tier1_bm25_bonus_weight": self.tier1_bm25_bonus_weight,
            "tier1_bm25_bonus_cap": self.tier1_bm25_bonus_cap,
            "tier1_min_bm25": self.tier1_min_bm25,
            "use_query_language_as_doc_filter": self.use_query_language_as_doc_filter,
            "cache_enabled": self.cache_enabled,
            "cache_max_items": self.cache_max_items,
            "cache_stats": {
                "document_meta_items": len(self._document_meta_cache),
                "document_structure_items": len(self._structure_cache),
                "page_text_items": len(self._page_text_cache),
                "page_content_items": len(self._page_content_cache),
                "pdf_bytes_items": len(self._pdf_bytes_cache),
            },
            "local_llm_unavailable": self._local_llm_unavailable,
            "local_llm_last_error": self._local_llm_last_error,
            "page_window": self.page_window,
            "page_expand_step": self.page_expand_step,
            "page_expand_acceleration": self.page_expand_acceleration,
            "page_max_window": self.page_max_window,
            "max_rounds": self.max_rounds,
            "max_document_candidates": self.max_document_candidates,
            "max_pages_per_call": self.max_pages_per_call,
        }

    def debug_local_llm(self) -> Dict[str, Any]:
        started = time.monotonic()
        self._ensure_local_llm_endpoint()
        result: Dict[str, Any] = {
            "ok": False,
            "backend": self.local_llm_backend,
            "base_url": self.local_llm_base_url,
            "api_url": self.local_llm_api_url,
            "model": self.local_llm_model,
            "local_llm_unavailable": self._local_llm_unavailable,
            "last_error": self._local_llm_last_error,
            "elapsed_ms": 0,
        }

        if not self._local_llm_enabled():
            result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
            return result

        ping_prompt = (
            "Return JSON with schema: {\"ok\": boolean, \"message\": string}. "
            "Set ok=true and message='pong'. "
            "Directly return the final JSON structure. Do not output anything else."
        )
        ping_default = {"ok": False, "message": ""}
        data = self._call_local_llm_json(
            ping_prompt,
            ping_default,
            request_timeout=max(1, min(self.local_llm_timeout, max(self.llm_json_timeout, self.local_llm_probe_timeout))),
        )
        result["ok"] = bool(data.get("ok"))
        result["message"] = str(data.get("message") or "")
        result["local_llm_unavailable"] = self._local_llm_unavailable
        result["last_error"] = self._local_llm_last_error
        result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
        return result

    def ask(
        self,
        question: str,
        top_k: int = 5,
        source_system: Optional[str] = None,
        language: Optional[str] = None,
        history: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        trace: List[Dict[str, Any]] = []
        ask_deadline = time.monotonic() + max(1, self.ask_timeout)
        pages_loaded_total = 0
        pages_hit_total = 0
        found_relevant_evidence = False
        answer_language = _resolve_answer_language(language, question)
        document_language_filter = language if (self.use_query_language_as_doc_filter and str(language or "").lower() in {"vi", "en"}) else None

        def _remaining_time() -> int:
            return max(0, int(ask_deadline - time.monotonic()))

        # Check for greeting first
        if _is_greeting(question):
            greeting_msg = (
                "Chào bạn! Mình là trợ lý học tập OER. "
                "Mình có thể giúp bạn giải đáp kiến thức, tìm kiếm định nghĩa, công thức hoặc cung cấp ví dụ về môn học này. "
                "Bạn cần mình hỗ trợ gì hôm nay?"
            )
            if answer_language == "en":
                greeting_msg = (
                    "Hello! I am your OER learning assistant. "
                    "I can help you search for information, examples, formulas, and definitions from this course's textbooks. "
                    "How can I help you today?"
                )
            return {
                "question": question,
                "answer": greeting_msg,
                "contexts": [],
                "confidence": "high",
                "search_mode": "pageindex",
                "pageindex_trace": [{"tool": "greeting_handler", "reason": "Intercepted greeting query"}],
                "query_bundle": {
                    "query_vi_original": question,
                    "intent": "greeting",
                    "language": answer_language
                },
                "metrics": {
                    "tier1_recall_at_k": 0.0,
                    "tier1_recall_at_k_type": "proxy",
                    "tier1_k": 1,
                    "evidence_hit_rate": 0.0,
                    "grounded_answer_rate": 0.0,
                    "pages_loaded_total": 0,
                    "pages_hit_total": 0
                }
            }

        # Check for chapter summary request
        chapter_num = self._parse_chapter_number(question)
        if chapter_num is not None:
            resolved_book = self._resolve_book_from_history(question, history)
            if resolved_book:
                trace.append({"tool": "chapter_summarizer_route", "book": resolved_book["title"], "chapter": chapter_num})
                return self._generate_chapter_summary(resolved_book, chapter_num, answer_language, question)

        # Check for book summary request first
        if self._is_summary_request(question):
            resolved_book = self._resolve_book_from_history(question, history)
            if resolved_book:
                trace.append({"tool": "toc_summarizer_route", "book": resolved_book["title"]})
                return self._generate_toc_summary(resolved_book, answer_language, question)

        prebundle = self._build_query_bundle(question)
        
        # Inject active book context from conversation history if missing from question
        if not prebundle.document_title and history:
            active_book = self._extract_active_book_title_from_history(history)
            if active_book:
                prebundle.document_title = active_book
                trace.append({"tool": "active_book_context", "book": active_book})

        detected_intent = prebundle.intent

        # ── LLM Intent Classification (Tier 2) ──────────────────────────────
        # To achieve robust semantic routing and support arbitrary natural language phrasing
        # (e.g., "cho tôi tài liệu học môn sinh học"), we use the zero-shot local LLM classifier
        # for all queries except explicit summary requests.
        llm_intent_used = False
        if self._local_llm_enabled() and detected_intent != "off_topic":
            llm_intent = self._classify_intent_with_llm(
                _strip_moodle_context(question), timeout=6
            )
            trace.append({
                "tool": "llm_intent_classifier",
                "pattern_intent": detected_intent,
                "llm_intent": llm_intent,
                "trigger": "semantic_routing",
            })
            if llm_intent in ("recommendation", "out_of_scope", "definition", "find_material", "listing"):
                detected_intent = llm_intent
                prebundle.intent = llm_intent
                llm_intent_used = True
            elif detected_intent == "explanation" and llm_intent == "general":
                llm_intent_used = True

        # OOS guard: fast keyword check first, then LLM for uncertain cases
        def _build_oos_response() -> Dict[str, Any]:
            oos_answer = (
                "This question is outside the scope of the OER academic library. "
                "I can only help with academic and educational topics."
                if answer_language == "en" else
                "Câu hỏi này nằm ngoài phạm vi thư viện học liệu mở OER. "
                "Mình chỉ hỗ trợ các câu hỏi về học thuật và giáo dục."
            )
            return {
                "question": question,
                "answer": oos_answer,
                "contexts": [],
                "confidence": "low",
                "search_mode": "pageindex",
                "pageindex_trace": trace,
                "query_bundle": {"intent": "off_topic", "language": answer_language},
                "metrics": {
                    "tier1_recall_at_k": 0.0, "tier1_recall_at_k_type": "proxy",
                    "tier1_k": 0, "evidence_hit_rate": 0.0,
                    "grounded_answer_rate": 0.0,
                    "pages_loaded_total": 0, "pages_hit_total": 0,
                },
            }

        # Tier 1 OOS: fast regex check (catches obvious lifestyle/entertainment keywords)
        if detected_intent == "out_of_scope" or _is_obviously_out_of_scope(question):
            trace.append({"tool": "oos_guard", "method": "pattern"})
            return _build_oos_response()

        # Tier 2 OOS: LLM check for questions that slipped through the keyword filter
        # Only invoke LLM when the question doesn't match any academic intent patterns
        # (intent stays 'explanation' as default, but question has no academic signals)
        if not llm_intent_used and detected_intent == "explanation":
            q_folded = _ascii_fold(_strip_moodle_context(question))
            academic_signals = [
                "la gi", "dinh nghia", "giai thich", "tinh chat", "cong thuc",
                "what is", "define", "explain", "formula", "derivative",
                "integral", "matrix", "probability", "algorithm", "database",
                "tai lieu", "sach", "giao trinh",
            ]
            has_academic_signal = any(s in q_folded for s in academic_signals)
            if not has_academic_signal:
                llm_intent = self._classify_intent_with_llm(
                    _strip_moodle_context(question), timeout=5
                )
                trace.append({
                    "tool": "llm_intent_classifier",
                    "pattern_intent": detected_intent,
                    "llm_intent": llm_intent,
                    "trigger": "no_academic_signal",
                })
                if llm_intent == "out_of_scope":
                    return _build_oos_response()
                detected_intent = llm_intent
                prebundle.intent = llm_intent
        # ── End LLM Intent Classification ───────────────────────────────────

        if detected_intent == "recommendation":
            # Course-scoped: recommend the curated books of the Moodle course.
            if self.tier2_course_scoped:
                course = self._resolve_course_books(prebundle.course_name)
                if course and course.get("books"):
                    trace.append({"tool": "course_recommend_route", "course": course["name"]})
                    return self._recommend_course_books(question, course, trace, answer_language, top_k)
            return self.recommend_books(
                question=question,
                top_k=top_k,
                source_system=source_system,
                language=language,
                bundle=prebundle,
            )
        if prebundle.has_unresolved_placeholder:
            trace.append(
                {
                    "tool": "query_guard",
                    "reason": "Cau hoi chua resolve placeholder khái niệm.",
                    "guard": "unresolved_placeholder",
                }
            )
            return {
                "question": question,
                "answer": _message_unresolved_concept(answer_language),
                "contexts": [],
                "confidence": "low",
                "search_mode": "pageindex",
                "pageindex_trace": trace,
                "query_bundle": {
                    "query_vi_original": prebundle.query_vi_original,
                    "query_en_semantic": prebundle.query_en_semantic,
                    "query_vi_semantic": prebundle.query_vi_semantic,
                    "keywords_en": prebundle.keywords_en,
                    "keywords_vi": prebundle.keywords_vi,
                    "intent": prebundle.intent,
                    "language": prebundle.language,
                    "query_mode": prebundle.query_mode,
                    "course_name": prebundle.course_name,
                    "concept_target": prebundle.concept_target,
                    "has_unresolved_placeholder": prebundle.has_unresolved_placeholder,
                },
                "metrics": {
                    "tier1_recall_at_k": 0.0,
                    "tier1_recall_at_k_type": "proxy",
                    "tier1_k": int(self.max_document_candidates),
                    "evidence_hit_rate": 0.0,
                    "grounded_answer_rate": 0.0,
                    "pages_loaded_total": 0,
                    "pages_hit_total": 0,
                },
            }

        document_result = self.get_document(
            question,
            top_k=max(1, min(self.max_document_candidates, top_k)),
            source_system=source_system,
            language=document_language_filter,
            reason="Chon tai lieu co kha nang chua cau tra loi dua tren metadata, subject va TOC.",
            bundle=prebundle,
        )
        trace.append(
            {
                "tool": "get_document",
                "reason": document_result.get("reason"),
                "documents_found": len(document_result.get("documents") or []),
                "tier1_backend": ((document_result.get("tier1") or {}).get("backend")),
                "tier1_used": bool((document_result.get("tier1") or {}).get("used")),
                "tier1_error": (document_result.get("tier1") or {}).get("error"),
                "answer_language": answer_language,
                "document_language_filter": document_language_filter,
            }
        )
        trace.append(
            {
                "tool": "tier1_budget",
                "reason": "Tầng 1 dùng BM25 metadata + subject-link trên Elasticsearch.",
                "tier1_elapsed_ms": (document_result.get("tier1") or {}).get("elapsed_ms"),
                "tier1_timeout_s": self.tier1_timeout,
                "remaining_seconds_after_tier1": _remaining_time(),
            }
        )
        query_bundle_data = document_result.get("query_bundle") or {}
        query_intent = str(query_bundle_data.get("intent") or _detect_query_intent(question))
        if _remaining_time() < self.tier2_min_budget and query_intent != "recommendation":
            return {
                "question": question,
                "answer": _message_time_budget(answer_language),
                "contexts": [],
                "confidence": "low",
                "search_mode": "pageindex",
                "pageindex_trace": trace,
                "query_bundle": document_result.get("query_bundle"),
                "metrics": {
                    "tier1_recall_at_k": 0.0,
                    "tier1_recall_at_k_type": "proxy",
                    "tier1_k": int(self.max_document_candidates),
                    "evidence_hit_rate": 0.0,
                    "grounded_answer_rate": 0.0,
                    "pages_loaded_total": 0,
                    "pages_hit_total": 0,
                },
            }

        documents = document_result.get("documents") or []
        if not documents:
            if not self._crossbook_relevance_ok(question, [], course_name=prebundle.course_name):
                return _build_oos_response()
            return {
                "question": question,
                "answer": _message_no_document(answer_language, prebundle.course_name),
                "contexts": [],
                "confidence": "low",
                "search_mode": "pageindex",
                "pageindex_trace": trace,
                "query_bundle": document_result.get("query_bundle"),
                "metrics": self._build_ask_metrics(
                    document_result=document_result,
                    selected_document=None,
                    pages_loaded_total=0,
                    pages_hit_total=0,
                    contexts=[],
                    answer="",
                    found_relevant_evidence=False,
                ),
            }

        bundle_data = query_bundle_data
        bundle = QueryBundle(
            query_vi_original=str(bundle_data.get("query_vi_original") or question),
            query_en_semantic=str(bundle_data.get("query_en_semantic") or question),
            query_vi_semantic=str(bundle_data.get("query_vi_semantic") or question),
            keywords_en=[str(x) for x in bundle_data.get("keywords_en") or []],
            keywords_vi=[str(x) for x in bundle_data.get("keywords_vi") or []],
            intent=str(bundle_data.get("intent") or _detect_query_intent(question)),
            language=str(bundle_data.get("language") or _detect_lang(question)),
            query_mode=str(bundle_data.get("query_mode") or _detect_query_language(question)),
            course_name=str(bundle_data.get("course_name") or prebundle.course_name or ""),
            section_name=str(bundle_data.get("section_name") or prebundle.section_name or ""),
            document_title=str(bundle_data.get("document_title") or prebundle.document_title or ""),
            concept_target=str(bundle_data.get("concept_target") or prebundle.concept_target or ""),
            concept_target_en=str(bundle_data.get("concept_target_en") or ""),
            has_unresolved_placeholder=bool(
                bundle_data.get("has_unresolved_placeholder")
                if "has_unresolved_placeholder" in bundle_data
                else prebundle.has_unresolved_placeholder
            ),
        )

        # Phuong an 2: cross-book page retrieval for content questions. One BM25
        # query over the page index picks the best pages across all books and
        # answers in a single Groq call, bypassing the Tier-1 wrong-book gate.
        if self._tier2_crossbook_active() and query_intent != "recommendation":
            crossbook_result = self._answer_from_crossbook_es(
                question=question,
                bundle=bundle,
                documents=documents,
                document_result=document_result,
                trace=trace,
                answer_language=answer_language,
                remaining_time_fn=_remaining_time,
            )
            if crossbook_result is not None:
                return crossbook_result
            # If cross-book search returned None (no relevant pages matched above quality/score thresholds),
            # do not fall back to the legacy per-document loop, as it would bypass the score/structural filters
            # and retrieve low-quality noise pages, causing hallucinations.
            trace.append({
                "tool": "crossbook_refusal_final",
                "reason": "Page index khong tra ve ket qua chat luong cao; tu choi de tranh RAG hallucination.",
            })
            if not self._crossbook_relevance_ok(question, [], course_name=bundle.course_name):
                return _build_oos_response()
            return {
                "question": question,
                "answer": _message_no_document(answer_language, bundle.course_name),
                "contexts": [],
                "confidence": "low",
                "search_mode": "pageindex",
                "pageindex_trace": trace,
                "query_bundle": bundle_data,
                "metrics": {
                    "tier1_recall_at_k": 0.0,
                    "tier1_recall_at_k_type": "proxy",
                    "tier1_k": int(self.max_document_candidates),
                    "evidence_hit_rate": 0.0,
                    "grounded_answer_rate": 0.0,
                    "pages_loaded_total": 0,
                    "pages_hit_total": 0,
                },
            }

        best_result: Optional[Dict[str, Any]] = None
        for document in documents[: self.max_document_candidates]:
            if _remaining_time() <= 1:
                trace.append(
                    {
                        "tool": "time_budget_guard",
                        "reason": "Dung som de tranh vuot timeout tong cua /api/ask.",
                        "remaining_seconds": _remaining_time(),
                    }
                )
                break
            structure = self.get_document_structure(
                str(document.get("asset_uid") or ""),
                reason="Khoanh vung chapter/section truoc khi doc trang chi tiet.",
            )
            trace.append(
                {
                    "tool": "get_document_structure",
                    "reason": structure.get("reason"),
                    "asset_uid": document.get("asset_uid"),
                    "found": bool(structure.get("found")),
                }
            )
            initial_range = self._select_initial_range(document, structure, bundle)
            current_range = initial_range

            for round_index in range(self.max_rounds):
                if _remaining_time() <= 1:
                    trace.append(
                        {
                            "tool": "time_budget_guard",
                            "reason": "Dung som de tranh vuot timeout tong cua /api/ask.",
                            "remaining_seconds": _remaining_time(),
                        }
                    )
                    break
                page_result = None
                if self._tier2_es_active():
                    page_result = self._get_page_content_es(
                        str(document.get("asset_uid") or ""),
                        bundle,
                        top_k=self.tier2_es_pages,
                    )
                    if page_result.get("content"):
                        pages_expr = "es_bm25"
                    else:
                        # Fallback to legacy PDF flow when the page index has no
                        # data for this document.
                        page_result = None
                if page_result is None:
                    pages_expr = _range_to_expr(current_range[0], current_range[1])
                    page_result = self.get_page_content(
                        str(document.get("asset_uid") or ""),
                        pages_expr,
                        reason="Doc mot vung trang hep de tim bang chung truc tiep.",
                    )
                trace.append(
                    {
                        "tool": page_result.get("tool") or "get_page_content",
                        "reason": page_result.get("reason"),
                        "asset_uid": document.get("asset_uid"),
                        "pages": pages_expr,
                        "pages_loaded": len(page_result.get("content") or []),
                    }
                )
                content = page_result.get("content") or []
                pages_loaded_total += len(content)
                search_terms = list(
                    dict.fromkeys(
                        bundle.keywords_vi
                        + bundle.keywords_en
                        + _tokenize(bundle.query_en_semantic)
                        + _tokenize(bundle.query_vi_semantic)
                    )
                )
                pages_hit_total += sum(1 for item in content if _overlap_score(item.get("text"), search_terms) > 0)
                if not content:
                    next_range = self._next_range_or_stop(current_range, structure, document, round_index)
                    if next_range == current_range:
                        break
                    current_range = next_range
                    continue

                ranked_content = list(content)
                if bundle.intent == "explanation":
                    target_terms = _expand_definition_target_tokens(bundle.concept_target)
                    if _is_definition_query(question) and target_terms:
                        ranked_content.sort(
                            key=lambda item: (
                                -(
                                    2.5 if _has_targeted_definition_cue(item.get("text"), target_terms) else 0.0
                                )
                                - _overlap_score(
                                    f"{item.get('chapter_title') or ''} {item.get('section_title') or ''} {item.get('text') or ''}",
                                    target_terms,
                                ),
                                _estimate_garbled_text_ratio(item.get("text")),
                                _estimate_formula_density(item.get("text")),
                                int(item.get("page_no") or 10**9),
                            )
                        )
                    else:
                        ranked_content.sort(
                            key=lambda item: (
                                _estimate_formula_density(item.get("text")),
                                int(item.get("page_no") or 10**9),
                            )
                        )

                contexts: List[Dict[str, Any]] = []
                for item in ranked_content[:top_k]:
                    contexts.append(
                        {
                            "text": item.get("text"),
                            "page_no": item.get("page_no"),
                            "title": document.get("title"),
                            "section_title": item.get("section_title"),
                            "chapter_title": item.get("chapter_title"),
                            "source_url": page_result.get("source_url"),
                            "minio_url": page_result.get("minio_url"),
                            "asset_path": document.get("asset_path"),
                            "asset_uid": document.get("asset_uid"),
                            "chunk_id": f"{document.get('asset_uid')}::page::{item.get('page_no')}",
                            "retrieval_score": float(document.get("score") or 0.0),
                        }
                    )

                judge_timeout = min(self.llm_json_timeout, _remaining_time())
                judged = self._judge_page_evidence(
                    question,
                    bundle,
                    page_result,
                    llm_timeout=judge_timeout if judge_timeout > 0 else None,
                )
                trace.append(
                    {
                        "tool": "evidence_judge",
                        "round": round_index + 1,
                        "relevant": bool(judged.get("relevant")),
                        "sufficient": bool(judged.get("sufficient")),
                        "confidence": judged.get("confidence"),
                    }
                )
                if bool(judged.get("relevant")):
                    found_relevant_evidence = True
                if not judged.get("relevant"):
                    next_range = self._next_range_or_stop(current_range, structure, document, round_index)
                    if next_range == current_range:
                        break
                    current_range = next_range
                    continue

                validation = self._validate_contexts_for_answer(question, bundle, contexts)
                trace.append(
                    {
                        "tool": "context_validation",
                        "round": round_index + 1,
                        "valid": bool(validation.get("valid")),
                        "reason": validation.get("reason"),
                        "course_mismatch_count": validation.get("course_mismatch_count"),
                        "concept_mismatch_count": validation.get("concept_mismatch_count"),
                        "needs_example": validation.get("needs_example"),
                        "has_example": validation.get("has_example"),
                    }
                )
                if not validation.get("valid"):
                    invalid_scope_result = {
                        "question": question,
                        "answer": _message_insufficient_scope(answer_language, prebundle.course_name),
                        "contexts": [],
                        "confidence": "low",
                        "search_mode": "pageindex",
                        "pageindex_trace": trace,
                        "query_bundle": document_result.get("query_bundle"),
                        "document": {
                            "asset_uid": document.get("asset_uid"),
                            "resource_uid": document.get("resource_uid"),
                            "title": document.get("title"),
                            "source_system": document.get("source_system"),
                        },
                        "metrics": self._build_ask_metrics(
                            document_result=document_result,
                            selected_document=document,
                            pages_loaded_total=pages_loaded_total,
                            pages_hit_total=pages_hit_total,
                            contexts=[],
                            answer="",
                            found_relevant_evidence=found_relevant_evidence,
                        ),
                    }
                    if not best_result or not (best_result.get("contexts") or []):
                        best_result = invalid_scope_result
                    next_range = self._next_range_or_stop(current_range, structure, document, round_index)
                    if next_range == current_range:
                        break
                    current_range = next_range
                    continue

                contexts = list(validation.get("contexts") or contexts)
                base_confidence = str(judged.get("confidence") or "medium").lower()
                confidence_score = self._confidence_to_score(base_confidence) * float(validation.get("alignment_score") or 0.0)
                confidence = self._score_to_confidence(confidence_score)
                if not judged.get("sufficient"):
                    partial_confidence = confidence
                    if self.disable_fallback:
                        remaining_for_partial = _remaining_time()
                        partial_timeout = max(1, min(self.llm_answer_timeout, remaining_for_partial)) if remaining_for_partial > 0 else 1
                        partial_answer = self._generate_answer(
                            question,
                            document,
                            contexts,
                            partial_confidence,
                            answer_language=answer_language,
                            llm_timeout=partial_timeout,
                        )
                    else:
                        partial_answer = self._fallback_answer(
                            question,
                            contexts,
                            partial_confidence,
                            answer_language=answer_language,
                        )
                    best_result = {
                        "question": question,
                        "answer": partial_answer,
                        "contexts": contexts,
                        "confidence": partial_confidence,
                        "search_mode": "pageindex",
                        "pageindex_trace": trace,
                        "query_bundle": document_result.get("query_bundle"),
                        "document": {
                            "asset_uid": document.get("asset_uid"),
                            "resource_uid": document.get("resource_uid"),
                            "title": document.get("title"),
                            "source_system": document.get("source_system"),
                        },
                        "metrics": self._build_ask_metrics(
                            document_result=document_result,
                            selected_document=document,
                            pages_loaded_total=pages_loaded_total,
                            pages_hit_total=pages_hit_total,
                            contexts=contexts,
                            answer=partial_answer,
                            found_relevant_evidence=found_relevant_evidence,
                        ),
                    }
                    trace.append(
                        {
                            "tool": "insufficient_evidence_guard",
                            "reason": (
                                "Bang chung moi o muc lien quan, chua du de ket luan. Tiep tuc mo rong range."
                                if not self.disable_fallback
                                else "Fallback tam tat de quan sat output LLM truc tiep khi bang chung chua du."
                            ),
                            "round": round_index + 1,
                            "confidence": partial_confidence,
                        }
                    )
                    next_range = self._next_range_or_stop(current_range, structure, document, round_index)
                    if next_range == current_range:
                        break
                    current_range = next_range
                    continue

                remaining_before_answer = _remaining_time()
                if remaining_before_answer <= 1:
                    if self.disable_fallback:
                        answer = self._generate_answer(
                            question,
                            document,
                            contexts,
                            confidence,
                            answer_language=answer_language,
                            llm_timeout=1,
                        )
                    else:
                        answer = self._fallback_answer(
                            question,
                            contexts,
                            "medium",
                            answer_language=answer_language,
                        )
                    trace.append(
                        {
                            "tool": "time_budget_guard",
                            "reason": (
                                "Bo qua goi LLM sinh cau tra loi vi sap het budget."
                                if not self.disable_fallback
                                else "Fallback tam tat; van goi LLM voi timeout ngan de xem output thuc te."
                            ),
                            "remaining_seconds": remaining_before_answer,
                        }
                    )
                else:
                    answer_timeout = max(1, min(self.llm_answer_timeout, remaining_before_answer))
                    answer = self._generate_answer(
                        question,
                        document,
                        contexts,
                        confidence,
                        answer_language=answer_language,
                        llm_timeout=answer_timeout,
                    )
                    generated_validation = self._validate_generated_answer(answer, contexts, answer_language)
                    trace.append(
                        {
                            "tool": "answer_validation",
                            "valid": bool(generated_validation.get("valid")),
                            "reason": generated_validation.get("reason"),
                        }
                    )
                    if not generated_validation.get("valid"):
                        validation_reason = generated_validation.get("reason", "")
                        repaired_ok = False
                        if validation_reason == "missing_required_sections" and answer.strip():
                            repaired = self._repair_answer_format(answer, contexts, answer_language)
                            if repaired:
                                repaired_validation = self._validate_generated_answer(repaired, contexts, answer_language)
                                if repaired_validation.get("valid"):
                                    answer = repaired
                                    repaired_ok = True
                                    trace.append({
                                        "tool": "answer_repair",
                                        "valid": True,
                                        "reason": "format_repaired_from_llm_output",
                                    })
                        if not repaired_ok and not self.disable_fallback:
                            answer = self._fallback_answer(
                                question, contexts, confidence, answer_language=answer_language,
                            )
                best_result = {
                    "question": question,
                    "answer": answer,
                    "contexts": contexts,
                    "confidence": confidence,
                    "search_mode": "pageindex",
                    "pageindex_trace": trace,
                    "query_bundle": document_result.get("query_bundle"),
                    "document": {
                        "asset_uid": document.get("asset_uid"),
                        "resource_uid": document.get("resource_uid"),
                        "title": document.get("title"),
                        "source_system": document.get("source_system"),
                    },
                    "metrics": self._build_ask_metrics(
                        document_result=document_result,
                        selected_document=document,
                        pages_loaded_total=pages_loaded_total,
                        pages_hit_total=pages_hit_total,
                        contexts=contexts,
                        answer=answer,
                        found_relevant_evidence=found_relevant_evidence,
                    ),
                }
                return best_result

        if best_result:
            return best_result

        return {
            "question": question,
            "answer": _message_no_relevant(answer_language, prebundle.course_name),
            "contexts": [],
            "confidence": "low",
            "search_mode": "pageindex",
            "pageindex_trace": trace,
            "query_bundle": document_result.get("query_bundle"),
            "metrics": self._build_ask_metrics(
                document_result=document_result,
                selected_document=None,
                pages_loaded_total=pages_loaded_total,
                pages_hit_total=pages_hit_total,
                contexts=[],
                answer="",
                found_relevant_evidence=found_relevant_evidence,
            ),
        }
