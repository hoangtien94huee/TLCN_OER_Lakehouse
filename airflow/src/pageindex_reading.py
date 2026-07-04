"""PDF/page reading, ES page content, PageIndex evidence range & validation.

Mixin tach tu pageindex.py (behaviour-preserving). Cac method van thao tac tren `self`
cua PageIndexEngine; config/state duoc khoi tao o PageIndexEngine.__init__.
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

from pageindex_types import PageIndexError, QueryBundle
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


class _ReadingMixin:
    """PDF/page reading, ES page content, PageIndex evidence range & validation."""

    def _get_pdf_bytes(self, asset_path: str) -> bytes:
        if not asset_path:
            raise PageIndexError("Document asset_path trống, không thể đọc trang.")
        cache_key = str(asset_path).lstrip("/")
        cached = self._cache_get(self._pdf_bytes_cache, cache_key)
        if isinstance(cached, (bytes, bytearray)):
            return bytes(cached)
        client = self._get_minio_client()
        response = client.get_object(self.bucket, cache_key)
        try:
            data = response.read()
            self._cache_set(self._pdf_bytes_cache, cache_key, data)
            return data
        finally:
            response.close()
            response.release_conn()

    def _extract_selected_pages(self, pdf_bytes: bytes, pages: Sequence[int]) -> Dict[int, str]:
        selected = sorted(set(int(p) for p in pages if int(p) > 0))
        if not selected:
            return {}

        if PDFPLUMBER_AVAILABLE:
            try:
                with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                    out: Dict[int, str] = {}
                    total = len(pdf.pages)
                    for page_no in selected:
                        if page_no > total:
                            continue
                        text = pdf.pages[page_no - 1].extract_text() or ""
                        out[page_no] = _normalize_pdf_text(text)
                    if any(out.values()):
                        return out
            except Exception as exc:
                logger.warning("pdfplumber page extraction failed: %s", exc)

        if PYPDF_AVAILABLE and PyPDFReader is not None:
            try:
                reader = PyPDFReader(BytesIO(pdf_bytes))
                total = len(reader.pages)
                out = {}
                for page_no in selected:
                    if page_no > total:
                        continue
                    try:
                        raw = reader.pages[page_no - 1].extract_text(extraction_mode="layout") or ""
                    except Exception:
                        raw = reader.pages[page_no - 1].extract_text() or ""
                    out[page_no] = _normalize_pdf_text(raw)
                return out
            except Exception as exc:
                logger.warning("pypdf page extraction failed: %s", exc)

        raise PageIndexError("Không thể trích xuất nội dung trang PDF.")

    def _locate_page_in_structure(self, page_no: int, toc_json: Any) -> Dict[str, Optional[str]]:
        for section in self._extract_sections_from_toc(toc_json):
            start_page = int(section.get("page_start") or page_no)
            end_page = int(section.get("page_end") or start_page)
            if start_page <= page_no <= end_page:
                return {
                    "chapter_title": str(section.get("chapter_title") or ""),
                    "section_title": str(section.get("section_title") or ""),
                }
        return {"chapter_title": None, "section_title": None}

    def _get_page_texts_cached(self, asset_uid: str, pdf_bytes: bytes, page_numbers: Sequence[int]) -> Dict[int, str]:
        key = str(asset_uid or "").strip()
        normalized = sorted(set(int(p) for p in page_numbers if int(p) > 0))
        if not normalized:
            return {}
        if not key:
            return self._extract_selected_pages(pdf_bytes, normalized)

        cache_entry = self._cache_get(self._page_text_cache, key)
        page_map: Dict[int, str] = dict(cache_entry) if isinstance(cache_entry, dict) else {}
        missing = [p for p in normalized if p not in page_map]
        if missing:
            extracted = self._extract_selected_pages(pdf_bytes, missing)
            if extracted:
                page_map.update(extracted)
        self._cache_set(self._page_text_cache, key, page_map)
        return {p: page_map.get(p, "") for p in normalized}

    def get_page_content(self, asset_uid: str, pages: str, reason: str = "") -> Dict[str, Any]:
        key = str(asset_uid or "").strip()
        page_numbers = _parse_pages_expr(pages)[: self.max_pages_per_call]
        normalized_pages = _collapse_pages(page_numbers)
        cache_key = f"{key}:{normalized_pages}"
        cached = self._cache_get(self._page_content_cache, cache_key)
        if isinstance(cached, dict):
            result = dict(cached)
            result["reason"] = reason
            result["cache_hit"] = True
            return result

        doc = self._get_document_meta(key)
        if not doc:
            return {
                "tool": "get_page_content",
                "reason": reason,
                "asset_uid": asset_uid,
                "pages": pages,
                "found": False,
                "content": [],
                "cache_hit": False,
            }

        structure_result = self.get_document_structure(key, reason="Map page to section")
        toc_json = ((structure_result.get("structure") or {}).get("table_of_contents_json"))
        pdf_bytes = self._get_pdf_bytes(str(doc.get("asset_path") or ""))
        page_texts = self._get_page_texts_cached(key, pdf_bytes, page_numbers)

        content: List[Dict[str, Any]] = []
        for page_no in page_numbers:
            text = _strip_surrogate_chars(page_texts.get(page_no) or "").strip()
            if not text:
                continue
            section_meta = self._locate_page_in_structure(page_no, toc_json)
            content.append(
                {
                    "page_no": int(page_no),
                    "text": text,
                    "chapter_title": section_meta.get("chapter_title"),
                    "section_title": section_meta.get("section_title"),
                }
            )

        result = {
            "tool": "get_page_content",
            "reason": reason,
            "asset_uid": asset_uid,
            "pages": pages,
            "found": True,
            "title": doc.get("title"),
            "source_url": doc.get("source_url"),
            "minio_url": f"{self.minio_public_base_url}/{self.bucket}/{str(doc.get('asset_path') or '').lstrip('/')}" if doc.get("asset_path") else None,
            "content": content,
            "cache_hit": False,
        }
        self._cache_set(self._page_content_cache, cache_key, result)
        return result

    def _tier2_es_active(self) -> bool:
        """True when Tier-2 should retrieve pages from the inverted index."""
        return (
            self.tier2_backend == "elasticsearch"
            and self.tier2_es_enabled
            and bool(self.tier1_es_host)
            and bool(self.tier2_es_index)
        )

    def _get_page_content_es(self, asset_uid: str, bundle: QueryBundle, top_k: int) -> Dict[str, Any]:
        """Retrieve the best pages of one document from the page-level inverted
        index (oer_pages_tier2) via BM25. Returns the same shape as
        get_page_content() so the downstream Tier-2 logic is unchanged."""
        empty = {
            "tool": "get_page_content_es",
            "asset_uid": asset_uid,
            "pages": "es_bm25",
            "found": False,
            "content": [],
            "cache_hit": False,
        }
        key = str(asset_uid or "").strip()
        if not key:
            return empty

        # Since the ES index is 100% English textbooks, use English query signals only
        # when available to avoid accent-folding collisions.
        en_parts = [
            str(bundle.query_en_semantic or "").strip(),
            " ".join(bundle.keywords_en),
        ]
        query_text_en = " ".join(p for p in en_parts if p).strip()
        if query_text_en:
            query_text = query_text_en
        else:
            query_text = " ".join(
                p for p in [
                    str(bundle.query_vi_semantic or "").strip(),
                    " ".join(bundle.keywords_vi),
                ] if p
            ).strip() or str(bundle.query_vi_original or "").strip()
        # Clean punctuation and remove common stopwords to avoid low-quality keyword matches (like "what is", "là gì")
        # that lead to false positives on completely unrelated documents.
        ENGLISH_STOPWORDS = {"what", "is", "a", "the", "of", "to", "in", "for", "on", "with", "at", "by", "an", "be", "this", "that", "from", "how", "why", "who", "where", "which", "can", "do", "does", "did", "are", "was", "were", "been", "have", "has", "had", "i", "you", "he", "she", "they", "we", "it", "about"}
        VIETNAMESE_STOPWORDS = {"là", "gì", "của", "và", "trong", "cho", "để", "ở", "tại", "bởi", "với", "được", "bị", "này", "kia", "đó", "nào", "sao", "thế", "cái", "con", "sự", "việc"}
        clean_words = []
        seen = set()
        for word in query_text.lower().replace("?", "").replace(".", "").replace(",", "").split():
            if word not in ENGLISH_STOPWORDS and word not in VIETNAMESE_STOPWORDS:
                if word not in seen:
                    clean_words.append(word)
                    seen.add(word)
        if clean_words:
            query_text = " ".join(clean_words)

        if not query_text:
            return empty

        body = {
            "size": max(1, int(top_k)),
            "_source": ["asset_uid", "page_no", "chapter_title", "section_title", "text", "title"],
            "query": {
                "bool": {
                    "filter": [{"term": {"asset_uid": key}}],
                    "must": [
                        {
                            "multi_match": {
                                "query": query_text,
                                "type": "best_fields",
                                "fields": ["text", "section_title^2", "chapter_title^2", "title^3"],
                                "operator": "or",
                                "minimum_should_match": "2<70%"
                            }
                        }
                    ],
                }
            },
        }
        auth = None
        if self.tier1_es_username and self.tier1_es_password:
            auth = (self.tier1_es_username, self.tier1_es_password)
        try:
            resp = requests.post(
                f"{self.tier1_es_host}/{self.tier2_es_index}/_search",
                json=body,
                timeout=(1, min(float(self.tier1_timeout), float(self.tier1_es_timeout))),
                auth=auth,
            )
            resp.raise_for_status()
            hits = ((resp.json().get("hits") or {}).get("hits") or [])
        except Exception:
            return empty

        content: List[Dict[str, Any]] = []
        for hit in hits:
            src = hit.get("_source") or {}
            text = _strip_surrogate_chars(str(src.get("text") or "")).strip()
            if not text:
                continue
            es_page_no = int(src.get("page_no") or 0)
            content.append({
                "page_no": es_page_no + 1,
                "text": text,
                "chapter_title": src.get("chapter_title"),
                "section_title": src.get("section_title"),
                "bm25_score": float(hit.get("_score") or 0.0),
            })
        if not content:
            return empty

        source_url = None
        minio_url = None
        try:
            doc = self._get_document_meta(key) or {}
            source_url = doc.get("source_url")
            asset_path = str(doc.get("asset_path") or "")
            if asset_path:
                minio_url = f"{self.minio_public_base_url}/{self.bucket}/{asset_path.lstrip('/')}"
        except Exception:
            pass

        return {
            "tool": "get_page_content_es",
            "asset_uid": asset_uid,
            "pages": "es_bm25",
            "found": True,
            "source_url": source_url,
            "minio_url": minio_url,
            "content": content,
            "cache_hit": False,
        }

    def _sample_summary_pages_from_es(self, asset_uid: str, max_pages: int = 8) -> Dict[str, Any]:
        empty = {"pages": [], "page_count": 0, "mode": "no_toc_metadata_only"}
        key = str(asset_uid or "").strip()
        if not key or not self._tier2_es_active():
            return empty

        auth = None
        if self.tier1_es_username and self.tier1_es_password:
            auth = (self.tier1_es_username, self.tier1_es_password)

        def _search_pages(body: Dict[str, Any]) -> List[Dict[str, Any]]:
            try:
                resp = requests.post(
                    f"{self.tier1_es_host}/{self.tier2_es_index}/_search",
                    json=body,
                    timeout=(1, min(float(self.tier1_timeout), float(self.tier1_es_timeout))),
                    auth=auth,
                )
                resp.raise_for_status()
                hits = ((resp.json().get("hits") or {}).get("hits") or [])
            except Exception as exc:
                logger.warning("No-TOC summary page sampling failed for %s: %s", key, exc)
                return []

            pages: List[Dict[str, Any]] = []
            for hit in hits:
                src = hit.get("_source") or {}
                text = _strip_surrogate_chars(str(src.get("text") or "")).strip()
                if not text:
                    continue
                try:
                    page_no = int(src.get("page_no") or 0)
                except Exception:
                    page_no = 0
                if page_no <= 0:
                    continue
                pages.append(
                    {
                        "page_no": page_no,
                        "text": text,
                        "chapter_title": src.get("chapter_title"),
                        "section_title": src.get("section_title"),
                        "title": src.get("title"),
                        "score": float(hit.get("_score") or 0.0),
                    }
                )
            return pages

        page_count = 0
        try:
            count_body = {
                "size": 0,
                "query": {"term": {"asset_uid": key}},
                "aggs": {
                    "max_page": {"max": {"field": "page_no"}},
                    "page_count": {"cardinality": {"field": "page_no"}},
                },
            }
            resp = requests.post(
                f"{self.tier1_es_host}/{self.tier2_es_index}/_search",
                json=count_body,
                timeout=(1, min(float(self.tier1_timeout), float(self.tier1_es_timeout))),
                auth=auth,
            )
            resp.raise_for_status()
            aggs = resp.json().get("aggregations") or {}
            page_count = int((aggs.get("max_page") or {}).get("value") or (aggs.get("page_count") or {}).get("value") or 0)
        except Exception:
            page_count = 0

        overview_body = {
            "size": max(1, min(max_pages, 6)),
            "_source": ["asset_uid", "page_no", "chapter_title", "section_title", "text", "title"],
            "query": {
                "bool": {
                    "filter": [{"term": {"asset_uid": key}}],
                    "must": [
                        {
                            "multi_match": {
                                "query": "contents table of contents preface introduction overview chapter outline syllabus",
                                "fields": ["section_title^4", "chapter_title^3", "text", "title^2"],
                                "operator": "or",
                            }
                        }
                    ],
                }
            },
            "sort": [{"_score": "desc"}, {"page_no": "asc"}],
        }
        selected: List[Dict[str, Any]] = []
        seen_pages: set = set()
        for page in _search_pages(overview_body):
            pno = int(page.get("page_no") or 0)
            if pno and pno not in seen_pages:
                selected.append(page)
                seen_pages.add(pno)
            if len(selected) >= max_pages:
                break

        sample_numbers: List[int] = []
        if page_count > 0:
            candidates = [
                1,
                2,
                3,
                max(1, page_count // 3),
                max(1, page_count // 2),
                max(1, (page_count * 2) // 3),
                max(1, page_count - 2),
                max(1, page_count - 1),
                page_count,
            ]
            sample_numbers = [n for n in _dedupe_keep_order(candidates) if n > 0]
        if sample_numbers and len(selected) < max_pages:
            sample_body = {
                "size": len(sample_numbers),
                "_source": ["asset_uid", "page_no", "chapter_title", "section_title", "text", "title"],
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"asset_uid": key}},
                            {"terms": {"page_no": sample_numbers}},
                        ]
                    }
                },
                "sort": [{"page_no": "asc"}],
            }
            for page in _search_pages(sample_body):
                pno = int(page.get("page_no") or 0)
                if pno and pno not in seen_pages:
                    selected.append(page)
                    seen_pages.add(pno)
                if len(selected) >= max_pages:
                    break

        selected.sort(key=lambda item: int(item.get("page_no") or 0))
        return {
            "pages": selected[:max_pages],
            "page_count": page_count,
            "mode": "no_toc_sampled_pages" if selected else "no_toc_metadata_only",
        }

    def _next_range_or_stop(self, current_range, structure, document, round_index):
        """In ES Tier-2 mode the inverted index already returns the best pages,
        so range expansion is a no-op (returning current_range makes the round
        loop terminate). Otherwise behaves like _expand_range."""
        if self._tier2_es_active():
            return current_range
        return self._expand_range(current_range, structure, document, round_index)

    def _select_initial_range(self, document: Dict[str, Any], structure: Dict[str, Any], bundle: QueryBundle) -> Tuple[int, int]:
        sections = structure.get("sections") or []
        definition_query = _is_definition_query(bundle.query_vi_original)
        if document.get("section_candidates"):
            top = document["section_candidates"][0]
            concept_selected = False
            # When Moodle provides section_name, match it against document TOC
            # to start reading exactly where the learner is.
            moodle_section = str(bundle.section_name or "").strip()
            if moodle_section:
                moodle_section_tokens = _tokenize(moodle_section)
                if moodle_section_tokens:
                    best_section_score = -1.0
                    for candidate in document.get("section_candidates") or []:
                        candidate_text = _ascii_fold(
                            f"{candidate.get('chapter_title') or ''} {candidate.get('section_title') or ''}"
                        )
                        score = _overlap_score(candidate_text, moodle_section_tokens)
                        if score > best_section_score:
                            best_section_score = score
                            top = candidate
                    if best_section_score >= 1.0:
                        concept_selected = True
            if definition_query and not concept_selected:
                target_terms = _expand_definition_target_tokens(bundle.concept_target)
                if target_terms:
                    best_candidate = top
                    best_score = -1.0
                    def_cues = [
                        "definition",
                        "define",
                        "what is",
                        "introduction",
                        "overview",
                        "concept",
                        "fundamentals",
                    ]
                    for candidate in document.get("section_candidates") or []:
                        chapter_text = _ascii_fold(str(candidate.get("chapter_title") or ""))
                        section_text = _ascii_fold(str(candidate.get("section_title") or ""))
                        combined_text = f"{chapter_text} {section_text}".strip()
                        section_overlap = _overlap_score(section_text, target_terms)
                        chapter_overlap = _overlap_score(chapter_text, target_terms)
                        concept_score = (section_overlap * 2.5) + (chapter_overlap * 1.0)
                        if concept_score <= 0.0:
                            continue
                        cue_bonus = 2.0 if any(cue in combined_text for cue in def_cues) else 0.0
                        page_start = int(candidate.get("page_start") or 1)
                        early_bonus = 0.6 if page_start <= 60 else (0.3 if page_start <= 150 else 0.0)
                        score = (concept_score * 3.0) + cue_bonus + early_bonus
                        if score > best_score:
                            best_score = score
                            best_candidate = candidate
                    if best_score > 0.0:
                        top = best_candidate
                        concept_selected = True

                cues = [
                    "introduction",
                    "overview",
                    "definition",
                    "concept",
                    "fundamentals",
                    "database management",
                    "dbms",
                    "what is",
                ]
                if not concept_selected and not (definition_query and target_terms):
                    for candidate in document.get("section_candidates") or []:
                        section_text = _ascii_fold(
                            f"{candidate.get('chapter_title') or ''} {candidate.get('section_title') or ''}"
                        )
                        if any(cue in section_text for cue in cues):
                            top = candidate
                            break
            start_page = int(top.get("page_start") or 1)
            end_page = int(top.get("page_end") or start_page)
            section_span = max(1, end_page - start_page + 1)
            if section_span > self.page_window:
                end_page = start_page + self.page_window - 1
        elif sections:
            start_page = int(sections[0].get("page_start") or 1)
            end_page = int(sections[0].get("page_end") or start_page)
            section_span = max(1, end_page - start_page + 1)
            if section_span > self.page_window:
                end_page = start_page + self.page_window - 1
        else:
            total_pages = int((structure.get("structure") or {}).get("total_pages") or document.get("total_pages") or 1)
            start_page = 1
            end_page = min(total_pages, self.page_window)
        if bundle.intent == "explanation":
            end_page = min(end_page, start_page + max(0, self.page_window - 1))
        total_pages = int((structure.get("structure") or {}).get("total_pages") or document.get("total_pages") or end_page)
        bounded_end = min(end_page, start_page + max(0, self.page_window - 1))
        return _clamp_page_range(start_page, bounded_end, total_pages)

    def _expand_range(
        self,
        current: Tuple[int, int],
        structure: Dict[str, Any],
        document: Dict[str, Any],
        round_index: int,
    ) -> Tuple[int, int]:
        total_pages = int((structure.get("structure") or {}).get("total_pages") or document.get("total_pages") or current[1])
        step = self.page_expand_step + (max(0, round_index) * max(0, self.page_expand_acceleration))
        left_step = max(0, step // 2)
        new_start = max(1, current[0] - left_step)
        new_end = current[1] + step
        new_start, new_end = _clamp_page_range(new_start, new_end, total_pages)

        # Keep each call focused even after expansion.
        if (new_end - new_start + 1) > self.page_max_window:
            new_start = max(1, new_end - self.page_max_window + 1)
            new_start, new_end = _clamp_page_range(new_start, new_end, total_pages)
        return new_start, new_end

    def _judge_page_evidence(
        self,
        question: str,
        bundle: QueryBundle,
        page_result: Dict[str, Any],
        llm_timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        content = page_result.get("content") or []
        joined_text = "\n\n".join(str(item.get("text") or "") for item in content)
        search_terms = list(dict.fromkeys(bundle.keywords_vi + bundle.keywords_en))
        heuristic_score = _overlap_score(joined_text, search_terms)
        definition_query = _is_definition_query(question)
        definition_target = (bundle.concept_target or _extract_requested_concept(question)) if definition_query else ""
        target_tokens = _expand_definition_target_tokens(definition_target)
        target_overlap = _overlap_score(joined_text, target_tokens) if target_tokens else 0.0
        has_definition_cue = _has_definition_cue(joined_text)
        has_targeted_definition_cue = _has_targeted_definition_cue(joined_text, target_tokens) if target_tokens else False
        transcript_noise = _estimate_transcript_noise(joined_text)

        # For definition queries on EN source text (e.g. "derivative"), target overlap
        # is a stronger signal than raw VI/EN keyword overlap.
        if definition_query and target_overlap > heuristic_score:
            heuristic_score = target_overlap
        if definition_query and has_targeted_definition_cue:
            heuristic_score += 0.8

        if bundle.intent == "explanation" and _estimate_formula_density(joined_text) >= 0.35:
            heuristic_score = max(0.0, heuristic_score - 1.0)
        if bundle.intent == "explanation" and definition_query:
            # Do not require overlap against most expanded synonyms; 1-2 direct hits
            # are already meaningful for short concept definitions.
            min_target_overlap = 1.0 if len(set(target_tokens)) <= 6 else 2.0
            if target_tokens and target_overlap < min_target_overlap:
                heuristic_score = max(0.0, heuristic_score - 1.0)
            if not has_definition_cue:
                heuristic_score = max(0.0, heuristic_score - 1.0)
        if bundle.intent == "explanation" and transcript_noise >= 0.03 and not has_definition_cue:
            heuristic_score = max(0.0, heuristic_score - 1.0)

        relevant_threshold = 1.5 if bundle.intent == "explanation" else 2.0
        sufficient_threshold = 3.5 if bundle.intent == "explanation" else 4.0
        if definition_query:
            relevant_threshold = 1.0
            sufficient_threshold = 2.5 if bundle.intent == "explanation" else 3.0

        default = {
            "relevant": heuristic_score >= relevant_threshold,
            "sufficient": heuristic_score >= sufficient_threshold,
            "confidence": "medium" if heuristic_score >= sufficient_threshold else ("low" if heuristic_score < relevant_threshold else "medium"),
            "evidence_summary": "",
            "missing_info": "" if heuristic_score >= sufficient_threshold else "Bằng chứng trực tiếp còn ít.",
        }
        if definition_query and (not has_definition_cue or (target_tokens and target_overlap < 1.0)):
            # Keep definition quality bar high, but avoid over-penalizing bilingual/lecture-style text
            # when there is still strong topical overlap.
            default["sufficient"] = False
            if target_overlap >= 2.0 and heuristic_score >= (relevant_threshold + 0.8):
                default["confidence"] = "medium"
                default["missing_info"] = "Chưa có câu định nghĩa chuẩn, nhưng đã có đoạn mô tả liên quan."
            else:
                default["confidence"] = "low" if default["confidence"] != "high" else default["confidence"]
                default["missing_info"] = "Chưa có đoạn định nghĩa trực tiếp cho khái niệm được hỏi."

        return default

    def _confidence_to_score(self, confidence: str) -> float:
        value = str(confidence or "").strip().lower()
        if value == "high":
            return 0.9
        if value == "medium":
            return 0.6
        return 0.3

    def _score_to_confidence(self, score: float) -> str:
        if score >= 0.8:
            return "high"
        if score >= 0.55:
            return "medium"
        return "low"

    def _validate_contexts_for_answer(
        self,
        question: str,
        bundle: QueryBundle,
        contexts: Sequence[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not contexts:
            return {
                "valid": False,
                "reason": "empty_context",
                "contexts": [],
                "alignment_score": 0.0,
                "course_mismatch_count": 0,
                "concept_mismatch_count": 0,
                "needs_example": False,
                "has_example": False,
            }

        course_profile = _build_course_scope_profile(bundle.course_name)
        concept_target = str(bundle.concept_target or "").strip()
        concept_terms = _expand_definition_target_tokens(concept_target) if concept_target else []
        definition_query = _is_definition_query(question)
        needs_example = any(marker in _ascii_fold(question) for marker in ["vi du", "example"])

        validated: List[Dict[str, Any]] = []
        course_mismatch_count = 0
        concept_mismatch_count = 0
        has_example = False
        has_definition_evidence = False

        for item in contexts:
            text = str(item.get("text") or "")
            scope_text = (
                f"{item.get('title') or ''} {item.get('chapter_title') or ''} {item.get('section_title') or ''} {text}"
            ).strip()
            scope_eval = _evaluate_course_scope_text(scope_text, course_profile)
            if bool(scope_eval.get("mismatch")):
                course_mismatch_count += 1
                continue

            concept_overlap = _overlap_score(scope_text, concept_terms) if concept_terms else _overlap_score(
                scope_text, bundle.keywords_vi + bundle.keywords_en
            )
            if concept_terms and concept_overlap <= 0.0:
                phrase_hit = _phrase_overlap(scope_text, [concept_target]) if concept_target else 0.0
                if phrase_hit <= 0.0:
                    concept_mismatch_count += 1
                    continue

            if _has_example_cue(scope_text):
                has_example = True
            if _has_targeted_definition_cue(scope_text, concept_terms):
                has_definition_evidence = True
            elif _has_definition_cue(scope_text) and concept_overlap >= 1.0:
                has_definition_evidence = True

            validated.append(dict(item))

        if definition_query and concept_terms and not has_definition_evidence:
            return {
                "valid": False,
                "reason": "missing_definition_evidence",
                "contexts": validated,
                "alignment_score": 0.35 if validated else 0.0,
                "course_mismatch_count": course_mismatch_count,
                "concept_mismatch_count": concept_mismatch_count,
                "needs_example": needs_example,
                "has_example": has_example,
            }

        if course_profile and course_mismatch_count >= max(2, (len(contexts) // 2) + 1):
            return {
                "valid": False,
                "reason": "course_scope_drift",
                "contexts": validated,
                "alignment_score": 0.2 if validated else 0.0,
                "course_mismatch_count": course_mismatch_count,
                "concept_mismatch_count": concept_mismatch_count,
                "needs_example": needs_example,
                "has_example": has_example,
            }

        if not validated:
            return {
                "valid": False,
                "reason": "course_or_concept_mismatch",
                "contexts": [],
                "alignment_score": 0.0,
                "course_mismatch_count": course_mismatch_count,
                "concept_mismatch_count": concept_mismatch_count,
                "needs_example": needs_example,
                "has_example": has_example,
            }

        total = float(max(1, len(contexts)))
        alignment_score = 1.0 - (
            float(course_mismatch_count + concept_mismatch_count) / (2.0 * total)
        )
        if definition_query and concept_terms and not has_definition_evidence:
            alignment_score -= 0.2
        if needs_example and not has_example:
            alignment_score -= 0.15
        alignment_score = max(0.0, min(1.0, alignment_score))
        reason = "ok"
        if definition_query and concept_terms and not has_definition_evidence:
            reason = "weak_definition_evidence"
        elif needs_example and not has_example:
            reason = "missing_example_evidence"
        return {
            "valid": True,
            "reason": reason,
            "contexts": validated[: max(1, len(contexts))],
            "alignment_score": alignment_score,
            "course_mismatch_count": course_mismatch_count,
            "concept_mismatch_count": concept_mismatch_count,
            "needs_example": needs_example,
            "has_example": has_example,
        }

    def _validate_generated_answer(
        self,
        answer: str,
        contexts: Sequence[Dict[str, Any]],
        answer_language: str,
    ) -> Dict[str, Any]:
        text = str(answer or "").strip()
        if not text:
            return {"valid": False, "reason": "empty_answer"}
        # 3-section format: 1) Trả lời/Answer  2) Chi tiết/Details  3) Nguồn/Sources
        section_ok = all(
            re.search(rf"(^|\n)\s*{idx}[\.)]\s+", text, flags=re.IGNORECASE)
            for idx in [1, 2, 3]
        )
        # Also accept legacy 4-section format for backward compatibility
        if not section_ok:
            section_ok = all(
                re.search(rf"(^|\n)\s*{idx}[\.)]\s+", text, flags=re.IGNORECASE)
                for idx in [1, 2, 3, 4]
            )
        # Also accept markdown bold headers
        if not section_ok:
            md_labels = [
                r"\*\*\s*(?:Trả lời|Tra loi|Answer|Định nghĩa|Definition)\s*[:：]?\s*\*\*",
                r"\*\*\s*(?:Chi tiết|Chi tiet|Details|Giải thích|Explanation)\s*[^*]*\*\*",
                r"\*\*\s*(?:Nguồn|Nguon|Source)\s*[:：]?\s*\*\*",
            ]
            section_ok = all(
                re.search(pattern, text, flags=re.IGNORECASE)
                for pattern in md_labels
            )
        if not section_ok:
            return {"valid": False, "reason": "missing_required_sections"}

        max_source = max(1, len(contexts))
        pattern = r"\[(?:Nguon|Nguồn|Source)\s+(\d+)\]"
        cited = [int(x) for x in re.findall(pattern, text, flags=re.IGNORECASE) if str(x).isdigit()]
        if not cited:
            return {"valid": False, "reason": "missing_citation"}
        if any(idx < 1 or idx > max_source for idx in cited):
            return {"valid": False, "reason": "citation_out_of_range"}

        if answer_language == "vi" and "khong du bang chung phu hop" in _ascii_fold(text):
            return {"valid": True, "reason": "insufficient_scope_message"}
        return {"valid": True, "reason": "ok"}

    def _fallback_answer(
        self,
        question: str,
        contexts: List[Dict[str, Any]],
        confidence: str,
        answer_language: str = "vi",
    ) -> str:
        if not contexts:
            cn = str(_parse_moodle_context(question).get("course_name") or "").strip()
            return _message_no_relevant(answer_language, cn)
        ranked = sorted(
            contexts,
            key=lambda item: (
                _estimate_formula_density(item.get("text")),
                -len(_tokenize(item.get("text"))),
                int(item.get("page_no") or 10**9),
            ),
        )
        selected = ranked[:2]
        is_definition_query = _is_definition_query(question)
        definition_target = _extract_requested_concept(question) if is_definition_query else ""
        # Include section/chapter titles so definition-cue checks can match structural metadata
        # (e.g. "3.1. Defining the Derivative" confirms the concept even if page text lacks "X is Y").
        def _context_search_text(item: Dict[str, Any]) -> str:
            parts = [
                str(item.get("section_title") or ""),
                str(item.get("chapter_title") or ""),
                str(item.get("text") or ""),
            ]
            return "\n".join(p for p in parts if p)

        joined_top_text = "\n".join(_context_search_text(item) for item in selected)
        has_definition_cue = _has_definition_cue(joined_top_text)
        target_tokens = _expand_definition_target_tokens(definition_target)
        target_overlap = _overlap_score(joined_top_text, target_tokens) if target_tokens else 0.0

        question_terms = _dedupe_keep_order(_tokenize(question) + target_tokens)

        def _clean_snippet(value: Any) -> str:
            text = re.sub(r"\s+", " ", str(value or "")).strip()
            if not text:
                return ""
            text = text.replace("|", " ")
            # Remove common OCR/header noise before selecting evidence sentences.
            text = re.sub(r"^\d+\s+", "", text)
            text = re.sub(r"\b\d+\s*•\s*", " ", text)
            text = re.sub(r"\b\d+(\.\d+){1,3}\b", " ", text)
            text = re.sub(r"\b(learning objectives|objectives)\b", "", text, flags=re.IGNORECASE)
            text = re.sub(
                r"\b(recognize|calculate|identify|describe|explain|estimate)\b[^.]{0,140}\.",
                " ",
                text,
                flags=re.IGNORECASE,
            )
            text = re.sub(r"\bfigure\s+\d+(\.\d+)?\b", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\bintroduction\s+\d+\b", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\bchapter\s+\d+\b", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\bsection\s+\d+(\.\d+)?\b", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\bchapter review\b", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\baccess for free at openstax\.org\b", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\bopenstax\b", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\s{2,}", " ", text).strip(" -:;,")
            return text

        def _excerpt(value: Any, max_chars: int = 220) -> str:
            text = _clean_snippet(value)
            if not text:
                return ""
            candidates = [
                s.strip(" -:;,")
                for s in re.split(r"(?<=[.!?])\s+|(?<=;)\s+", text)
                if s.strip()
            ]
            if not candidates:
                candidates = [text]

            def _score(sentence: str) -> float:
                overlap = _overlap_score(sentence, question_terms)
                definition_bonus = 1.0 if _has_definition_cue(sentence) else 0.0
                targeted_bonus = 3.0 if (target_tokens and _has_targeted_definition_cue(sentence, target_tokens)) else 0.0
                density_penalty = _estimate_formula_density(sentence) * 2.2
                garbled_penalty = 2.0 if re.search(r"[A-Za-z]{20,}", sentence) else 0.0
                token_count = len(_tokenize(sentence))
                length_bonus = 0.6 if token_count >= 8 else (0.2 if token_count >= 5 else -0.8)
                return (overlap * 2.0) + definition_bonus + targeted_bonus + length_bonus - density_penalty - garbled_penalty

            scored = [(sentence, _score(sentence)) for sentence in candidates]
            best, best_score = max(scored, key=lambda item: item[1])
            if best_score < 0.2:
                return ""
            if len(best) <= max_chars:
                return best
            return best[:max_chars].rsplit(" ", 1)[0] + "..."

        def _build_readable_summary(primary_text: str, secondary_text: str = "") -> str:
            main = _excerpt(primary_text, max_chars=180)
            second = _excerpt(secondary_text, max_chars=140) if secondary_text else ""
            second_overlap = _overlap_score(second, question_terms) if second else 0.0
            if answer_language == "en":
                if not main:
                    return "The retrieved pages contain only partial information."
                if second and second.lower() != main.lower() and second_overlap >= 0.8:
                    return f"{main} Additionally, {second}"
                return main

            if not main:
                return "Các trang đã truy hồi mới cung cấp thông tin rời rạc, chưa thành đoạn giải thích rõ."
            if second and second.lower() != main.lower() and second_overlap >= 0.8:
                return f"{main} Ngoài ra, {second}"
            return main

        if is_definition_query and (
            (target_tokens and target_overlap < 1.0)
            or (target_tokens and not _has_targeted_definition_cue(joined_top_text, target_tokens))
        ):
            if answer_language == "en":
                return (
                    "1) Answer: No definition can be confirmed from the retrieved context.\n"
                    "2) Details: Evidence is still limited and not tightly aligned with the requested concept.\n"
                    "3) Sources: [Source 1]"
                )
            return (
                "1) Trả lời: Chưa thể xác nhận định nghĩa chuẩn từ ngữ cảnh đã truy hồi.\n"
                "2) Chi tiết: Bằng chứng còn hạn chế và chưa bám sát khái niệm được hỏi.\n"
                "3) Nguồn: [Nguồn 1]"
            )

        main = selected[0]
        main_page = main.get("page_no")
        if answer_language == "en":
            main_cite = f"page {main_page}" if main_page else "unknown page"
        else:
            main_cite = f"trang {main_page}" if main_page else "trang không xác định"
        main_excerpt = _excerpt(main.get("text"))
        main_title = str(main.get("title") or "").strip()
        include_second = False
        second_excerpt = ""
        if len(selected) > 1:
            second_text = str(selected[1].get("text") or "")
            second_overlap = _overlap_score(second_text, question_terms)
            include_second = second_overlap >= 0.8 or _has_definition_cue(second_text)
            if include_second:
                second_excerpt = _excerpt(second_text, max_chars=180)
        readable_summary = _build_readable_summary(main.get("text"), selected[1].get("text") if len(selected) > 1 else "")
        en_context_cleared = False
        en_source_note = ""
        if answer_language != "en":
            if main_excerpt and _is_english_dominant_text(main_excerpt):
                en_context_cleared = True
                en_source_note = " (nguồn tiếng Anh)"
            if readable_summary and _is_english_dominant_text(readable_summary) and not main_excerpt:
                readable_summary = "Ngữ cảnh hiện tại chủ yếu là tiếng Anh — xem tài liệu nguồn để biết chi tiết."

        if is_definition_query and target_tokens and (
            not main_excerpt
            or not _has_targeted_definition_cue(main_excerpt, target_tokens)
        ):
            if en_context_cleared and target_overlap >= 2.0:
                section_hint = str(selected[0].get("section_title") or selected[0].get("chapter_title") or "").strip()
                section_hint = re.sub(r"\*+$", "", section_hint).strip()
                page_hint = selected[0].get("page_no")
                en_excerpt = _excerpt(selected[0].get("text"), max_chars=250)
                if answer_language == "en":
                    location = f"section '{section_hint}'" if section_hint else (f"page {page_hint}" if page_hint else "the retrieved pages")
                    return (
                        f"1) Answer: The concept was found in {location} of the document. "
                        f"{en_excerpt}\n"
                        f"2) Details: See the source document for the complete explanation.\n"
                        "3) Sources: [Source 1]"
                    )
                location_vi = f"phần '{section_hint}'" if section_hint else (f"trang {page_hint}" if page_hint else "các trang đã truy hồi")
                detail_text = en_excerpt if en_excerpt else f"Xem tài liệu nguồn tại {location_vi} để biết chi tiết."
                return (
                    f"1) Trả lời: Khái niệm được tìm thấy trong {location_vi} của tài liệu{en_source_note}.\n"
                    f"2) Chi tiết: {detail_text}\n"
                    "3) Nguồn: [Nguồn 1]"
                )
            if main_excerpt:
                if answer_language == "en":
                    return (
                        f"1) Answer: {main_excerpt}\n"
                        f"2) Details: The context addresses this topic but does not provide a formal definition.\n"
                        "3) Sources: [Source 1]"
                    )
                return (
                    f"1) Trả lời: {main_excerpt}\n"
                    f"2) Chi tiết: Ngữ cảnh có đề cập đến chủ đề này nhưng chưa có định nghĩa hình thức{en_source_note}.\n"
                    "3) Nguồn: [Nguồn 1]"
                )
            if answer_language == "en":
                return (
                    "1) Answer: No direct definition can be confirmed from the retrieved context.\n"
                    "2) Details: Evidence is still limited or not directly aligned to the requested concept.\n"
                    "3) Sources: [Source 1]"
                )
            return (
                "1) Trả lời: Chưa xác nhận được câu định nghĩa trực tiếp từ ngữ cảnh đã truy hồi.\n"
                "2) Chi tiết: Bằng chứng còn hạn chế hoặc chưa bám trực tiếp vào khái niệm được hỏi.\n"
                "3) Nguồn: [Nguồn 1]"
            )

        if answer_language == "en":
            detail_parts: List[str] = [f"[Source 1 - {main_cite}] {main_excerpt}"]
        else:
            detail_parts = [f"[Nguồn 1 - {main_cite}] {main_excerpt}"]
        if len(selected) > 1 and include_second and second_excerpt:
            second = selected[1]
            second_page = second.get("page_no")
            if answer_language == "en":
                second_cite = f"page {second_page}" if second_page else "unknown page"
                detail_parts.append(f"[Source 2 - {second_cite}] {second_excerpt}")
            else:
                second_cite = f"trang {second_page}" if second_page else "trang không xác định"
                detail_parts.append(f"[Nguồn 2 - {second_cite}] {second_excerpt}")

        if answer_language == "en":
            source_count = max(1, len(detail_parts))
            details = readable_summary
            if second_excerpt and _has_example_cue(second_excerpt):
                details += f" Example: {second_excerpt}"
            return (
                f"1) Answer: {main_excerpt or readable_summary}\n"
                f"2) Details: {details}\n"
                f"3) Sources: {', '.join([f'[Source {i}]' for i in range(1, source_count + 1)])}"
            )

        source_count = max(1, len(detail_parts))
        details_vi = readable_summary
        if second_excerpt and _has_example_cue(second_excerpt):
            details_vi += f" Ví dụ: {second_excerpt}"
        return (
            f"1) Trả lời: {main_excerpt or readable_summary}\n"
            f"2) Chi tiết: {details_vi}\n"
            f"3) Nguồn: {', '.join([f'[Nguồn {i}]' for i in range(1, source_count + 1)])}"
        )

    def _get_page_counts(self, asset_uids: List[str]) -> Dict[str, int]:
        if not asset_uids:
            return {}
        body = {
            "size": 0,
            "query": {
                "terms": {
                    "asset_uid": asset_uids
                }
            },
            "aggs": {
                "by_book": {
                    "terms": {
                        "field": "asset_uid",
                        "size": len(asset_uids)
                    }
                }
            }
        }
        auth = None
        if self.tier1_es_username and self.tier1_es_password:
            auth = (self.tier1_es_username, self.tier1_es_password)
        try:
            resp = requests.post(
                f"{self.tier1_es_host}/{self.tier2_es_index}/_search",
                json=body,
                timeout=3.0,
                auth=auth,
            )
            resp.raise_for_status()
            buckets = resp.json().get("aggregations", {}).get("by_book", {}).get("buckets", [])
            return {b["key"]: b["doc_count"] for b in buckets}
        except Exception as exc:
            logger.warning("Error fetching book page counts: %s", exc)
            return {}
