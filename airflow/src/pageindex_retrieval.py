"""Query bundle, Tier-1 doc search, course-scoping & cross-book retrieval.

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


class _RetrievalMixin:
    """Query bundle, Tier-1 doc search, course-scoping & cross-book retrieval."""

    def _build_query_bundle(self, question: str, llm_timeout: Optional[int] = None) -> QueryBundle:
        question_core = _strip_moodle_context(question) or str(question or "")
        intent = _detect_query_intent(question_core)
        query_mode = _detect_query_language(question_core)
        language = _detect_lang(question_core)
        course_name = _extract_course_name_hint(question)
        section_name = _extract_section_name_hint(question)
        document_title = _extract_document_title_hint(question)
        concept_target = _extract_requested_concept(question_core)
        has_unresolved_placeholder = _contains_unresolved_placeholder(question_core)
        keywords_base = _tokenize(question_core)
        query_en_semantic = question_core
        query_vi_semantic = question_core
        if query_mode == "vi":
            keywords_vi = list(keywords_base)
            keywords_en = _derive_en_keywords_from_vi(question_core, keywords_vi)
            if keywords_en:
                query_en_semantic = " ".join(keywords_en)
        elif query_mode == "mixed":
            keywords_vi = list(keywords_base)
            keywords_en = list(keywords_base) + _derive_en_keywords_from_vi(question_core, keywords_base)
        else:
            keywords_en = list(keywords_base)
            keywords_vi = _derive_vi_keywords_from_en(question_core, keywords_en)
            if keywords_vi:
                query_vi_semantic = " ".join(keywords_vi)

        if query_mode in {"vi", "mixed"} and self._local_llm_enabled():
            prompt = (
                "Ban la bo phan rewrite query cho he thong PageIndex.\n"
                "Tai lieu goc chu yeu bang tieng Anh, nguoi dung co the hoi bang tieng Viet/Anh/mixed.\n"
                "Hay chuan hoa query song ngu de truy hoi metadata va section/page.\n"
                "Tra ve JSON theo schema:\n"
                "{"
                "\"query_en_semantic\": string, "
                "\"query_vi_semantic\": string, "
                "\"keywords_en\": [string], "
                "\"keywords_vi\": [string]"
                "}\n"
                f"Cau hoi: {question_core}\n"
                "Directly return the final JSON structure. Do not output anything else."
            )
            fallback = {
                "query_en_semantic": query_en_semantic,
                "query_vi_semantic": query_vi_semantic,
                "keywords_en": keywords_en,
                "keywords_vi": keywords_vi,
            }
            try:
                data = self._call_local_llm_json(
                    prompt,
                    fallback,
                    request_timeout=llm_timeout if llm_timeout is not None else self.llm_json_timeout,
                )
            except Exception as exc:
                logger.warning("Local LLM query rewrite timed out or failed, using fallback. detail=%s", exc)
                data = fallback
            query_en_semantic = str(data.get("query_en_semantic") or question_core).strip()
            query_vi_semantic = str(data.get("query_vi_semantic") or question_core).strip()
            kw_en = [str(x).strip() for x in data.get("keywords_en") or [] if str(x).strip()]
            kw_vi = [str(x).strip() for x in data.get("keywords_vi") or [] if str(x).strip()]
            
            # Merge heuristic fallbacks to guarantee key term preservation
            for term in fallback["keywords_en"]:
                if term not in kw_en:
                    kw_en.append(term)
            for term in fallback["keywords_vi"]:
                if term not in kw_vi:
                    kw_vi.append(term)
                    
            if kw_en:
                keywords_en = _tokenize(" ".join(kw_en))
            if kw_vi:
                keywords_vi = _tokenize(" ".join(kw_vi))

        if not keywords_en:
            keywords_en = _derive_en_keywords_from_vi(question_core, keywords_vi)
        if not keywords_vi:
            keywords_vi = _derive_vi_keywords_from_en(question_core, keywords_en)

        folded_question = _ascii_fold(question_core)
        if any(marker in folded_question for marker in ["co so du lieu", "database", "dbms", "sql"]):
            keywords_en = _dedupe_keep_order(
                keywords_en + ["database", "dbms", "sql", "relational", "transaction", "normalization"]
            )
            db_enrichment = "database dbms sql relational model normalization transaction"
            query_en_semantic = f"{query_en_semantic} {db_enrichment}".strip() if query_en_semantic else db_enrichment

        implicit_concept_query = _is_definition_query(question_core) and _is_implicit_concept_placeholder(question_core)
        if implicit_concept_query and not concept_target:
            seed_terms: List[str] = []
            # When Moodle provides section_name (e.g. "Derivatives"), use it as the
            # primary signal — it tells us exactly which topic the learner is viewing.
            if section_name:
                section_tokens = _tokenize(section_name)
                seed_terms.extend(section_tokens)
                expanded = _expand_definition_target_tokens(_ascii_fold(section_name))
                seed_terms.extend(expanded)
            if not seed_terms:
                profile = _build_course_scope_profile(course_name)
                profile_name = str(profile.get("name") or "")
                if profile_name == "calculus_i":
                    seed_terms = ["calculus", "limit", "continuity", "derivative", "integral", "function"]
                else:
                    allow_terms = [str(x or "") for x in (profile.get("allow_terms") or [])]
                    for term in allow_terms:
                        seed_terms.extend(_tokenize(term))
                    if not seed_terms:
                        seed_terms = ["overview", "introduction", "fundamentals", "concept"]

            seed_terms = _dedupe_keep_order([tok for tok in seed_terms if len(tok) >= 3])[:10]
            if seed_terms:
                keywords_en = _dedupe_keep_order(keywords_en + seed_terms)
                keywords_vi = _dedupe_keep_order(keywords_vi + _derive_vi_keywords_from_en(" ".join(seed_terms), seed_terms))
                if not str(query_en_semantic or "").strip() or _ascii_fold(query_en_semantic) == _ascii_fold(question_core):
                    query_en_semantic = " ".join(seed_terms)

        if concept_target:
            concept_seed_terms = [
                term
                for term in _expand_definition_target_tokens(concept_target)
                if len(str(term or "").strip()) >= 3
            ][:12]
            if concept_seed_terms:
                keywords_en = _dedupe_keep_order(keywords_en + concept_seed_terms)
                keywords_vi = _dedupe_keep_order(
                    keywords_vi
                    + _tokenize(concept_target)
                    + _derive_vi_keywords_from_en(" ".join(concept_seed_terms), concept_seed_terms)
                )
                if query_mode in {"vi", "mixed"}:
                    query_en_semantic = " ".join(
                        _dedupe_keep_order(_tokenize(query_en_semantic) + concept_seed_terms)
                    )

        keywords_en = _dedupe_keep_order(keywords_en)
        keywords_vi = _dedupe_keep_order(keywords_vi)

        return QueryBundle(
            query_vi_original=question,
            query_en_semantic=query_en_semantic,
            query_vi_semantic=query_vi_semantic,
            keywords_en=keywords_en,
            keywords_vi=keywords_vi,
            intent=intent,
            language=language,
            query_mode=query_mode,
            course_name=course_name,
            section_name=section_name,
            document_title=document_title,
            concept_target=concept_target,
            has_unresolved_placeholder=has_unresolved_placeholder,
        )

    def _infer_subject_hints(self, bundle: QueryBundle) -> Dict[str, Any]:
        cache = self._load_reference_cache()
        subjects = cache.get("subjects") or []
        programs_by_subject = cache.get("programs_by_subject") or {}
        moodle_context = _parse_moodle_context(bundle.query_vi_original)
        preferred_course_name = str(moodle_context.get("course_name") or "").strip()
        preferred_course_fold = _ascii_fold(preferred_course_name)

        generic_tokens = {"co", "so", "du", "lieu", "mon", "hoc", "tai", "sach", "data"}
        search_terms = [
            term
            for term in list(dict.fromkeys(bundle.keywords_vi + bundle.keywords_en))
            if term not in generic_tokens and len(term) >= 3
        ]
        question_fold = _ascii_fold(_strip_moodle_context(bundle.query_vi_original))
        if any(marker in question_fold for marker in ["co so du lieu", "database", "dbms", "sql"]):
            search_terms = _dedupe_keep_order(
                search_terms + ["database", "dbms", "sql", "relational", "transaction", "normalization"]
            )
            search_terms = [term for term in search_terms if term not in {"van", "hoa", "truy", "chuan", "giao", "dich"}]
        scored: List[Dict[str, Any]] = []
        for subject in subjects:
            name_vi = str(subject.get("subject_name") or "")
            name_en = str(subject.get("subject_name_en") or "")
            code = str(subject.get("subject_code") or "")
            score = (
                (_overlap_score(name_vi, search_terms) * 3.0)
                + (_overlap_score(name_en, search_terms) * 3.0)
                + (_overlap_score(code, search_terms) * 4.0)
            )
            if preferred_course_fold:
                name_vi_fold = _ascii_fold(name_vi)
                name_en_fold = _ascii_fold(name_en)
                if preferred_course_fold and preferred_course_fold in {name_vi_fold, name_en_fold}:
                    score += 12.0
                elif (
                    (preferred_course_fold and preferred_course_fold in name_vi_fold)
                    or (preferred_course_fold and preferred_course_fold in name_en_fold)
                    or (name_vi_fold and name_vi_fold in preferred_course_fold)
                    or (name_en_fold and name_en_fold in preferred_course_fold)
                ):
                    score += 6.0
            if score <= 0:
                continue
            subject_id = subject.get("subject_id")
            try:
                subject_id_int = int(subject_id)
            except Exception:
                continue
            scored.append(
                {
                    "subject_id": subject_id_int,
                    "subject_name": name_vi,
                    "subject_name_en": name_en,
                    "subject_code": code,
                    "score": score,
                    "program_ids": programs_by_subject.get(subject_id_int, []),
                }
            )

        scored.sort(key=lambda item: item["score"], reverse=True)
        top_subjects = scored[:5]
        program_ids: List[int] = []
        for subject in top_subjects:
            for program_id in subject.get("program_ids") or []:
                if program_id not in program_ids:
                    program_ids.append(program_id)

        return {
            "subjects": top_subjects,
            "subject_ids": [int(item["subject_id"]) for item in top_subjects],
            "program_ids": program_ids,
        }

    def _search_tier1_candidates(
        self,
        bundle: QueryBundle,
        subject_hints: Dict[str, Any],
        source_system: Optional[str],
        top_k: int,
    ) -> Dict[str, Any]:
        started = time.monotonic()
        base_result: Dict[str, Any] = {
            "backend": self.tier1_backend,
            "enabled": self.tier1_es_enabled and self.tier1_backend == "elasticsearch",
            "es_host": self.tier1_es_host,
            "es_index": self.tier1_es_index,
            "query_text": "",
            "documents": [],
            "used": False,
            "error": "",
            "elapsed_ms": 0,
        }
        if not base_result["enabled"]:
            base_result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
            return base_result
        if not self.tier1_es_host or not self.tier1_es_index:
            base_result["error"] = "Thiếu PAGEINDEX_TIER1_ES_HOST hoặc PAGEINDEX_TIER1_ES_INDEX."
            base_result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
            return base_result

        query_parts = _dedupe_keep_order(
            [
                str(bundle.query_vi_original or "").strip(),
                str(bundle.query_en_semantic or "").strip(),
                str(bundle.query_vi_semantic or "").strip(),
                " ".join(bundle.keywords_vi),
                " ".join(bundle.keywords_en),
            ]
        )
        query_text = " ".join([part for part in query_parts if part]).strip()
        if not query_text:
            query_text = str(bundle.query_vi_original or bundle.query_en_semantic or "").strip()
        base_result["query_text"] = query_text
        if not query_text:
            base_result["error"] = "Câu hỏi rỗng sau bước chuẩn hóa query."
            base_result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
            return base_result

        shortlist_limit = max(8, int(top_k) * self.tier1_es_topk_buffer, self.max_document_candidates * 3)
        size = max(self.tier1_es_candidate_pool, shortlist_limit)
        should_clauses: List[Dict[str, Any]] = [
            {
                "multi_match": {
                    "query": query_text,
                    "type": "best_fields",
                    "operator": "or",
                    "fields": [
                        "title^10",
                        "description^7",
                        "subject_names_vi^6",
                        "subject_names_en^6",
                        "subject_codes^6",
                    ],
                    "lenient": True,
                    "fuzziness": "AUTO",
                }
            }
        ]

        if bundle.query_en_semantic and bundle.query_en_semantic != query_text:
            should_clauses.append(
                {
                    "multi_match": {
                        "query": bundle.query_en_semantic,
                        "type": "best_fields",
                        "operator": "or",
                        "fields": [
                            "title^10",
                            "description^7",
                            "subject_names_en^6",
                            "subject_codes^5",
                        ],
                        "lenient": True,
                        "boost": 1.2,
                    }
                }
            )

        if bundle.query_vi_semantic or bundle.query_vi_original:
            should_clauses.append(
                {
                    "match_phrase": {
                        "title": {
                            "query": (bundle.query_vi_semantic or bundle.query_vi_original),
                            "boost": 5.0,
                        }
                    }
                }
            )

        if bundle.query_en_semantic:
            should_clauses.append(
                {
                    "match_phrase": {
                        "title": {
                            "query": bundle.query_en_semantic,
                            "boost": 6.0,
                        }
                    }
                }
            )

        subject_ids = [int(x) for x in subject_hints.get("subject_ids") or [] if str(x).isdigit()]
        if subject_ids:
            should_clauses.append(
                {
                    "constant_score": {
                        "filter": {"terms": {"subject_ids": subject_ids[:20]}},
                        "boost": 2.0,
                    }
                }
            )

        program_ids = [int(x) for x in subject_hints.get("program_ids") or [] if str(x).isdigit()]
        if program_ids:
            should_clauses.append(
                {
                    "constant_score": {
                        "filter": {"terms": {"program_ids": program_ids[:20]}},
                        "boost": 1.2,
                    }
                }
            )

        course_name = str(bundle.course_name or "").strip()
        if course_name:
            should_clauses.append(
                {
                    "match_phrase": {
                        "title": {
                            "query": course_name,
                            "boost": 9.0,
                        }
                    }
                }
            )
            should_clauses.append(
                {
                    "multi_match": {
                        "query": course_name,
                        "type": "best_fields",
                        "operator": "or",
                        "fields": [
                            "title^9",
                            "description^5",
                            "subject_names_vi^7",
                            "subject_names_en^7",
                            "subject_codes^7",
                        ],
                        "lenient": True,
                        "boost": 2.2,
                    }
                }
            )

        filters: List[Dict[str, Any]] = []
        if source_system:
            filters.append({"term": {"source_system": source_system}})

        body: Dict[str, Any] = {
            "size": size,
            "_source": [
                "resource_uid",
                "asset_uid",
                "title",
                "description",
                "source_system",
                "source_url",
                "subject_ids",
                "subject_names_vi",
                "subject_names_en",
                "subject_codes",
                "program_ids",
            ],
            "collapse": {"field": "resource_uid"},
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1,
                }
            },
            "sort": [{"_score": {"order": "desc"}}],
            "track_total_hits": True,
        }
        if filters:
            body["query"]["bool"]["filter"] = filters

        request_url = f"{self.tier1_es_host}/{self.tier1_es_index}/_search"
        auth = None
        if self.tier1_es_username and self.tier1_es_password:
            auth = (self.tier1_es_username, self.tier1_es_password)

        try:
            response = requests.post(
                request_url,
                json=body,
                timeout=(1, min(float(self.tier1_timeout), float(self.tier1_es_timeout))),
                auth=auth,
            )
            response.raise_for_status()
            payload = response.json()
            hits = ((payload.get("hits") or {}).get("hits") or [])
            deduped_docs: List[Dict[str, Any]] = []
            seen = set()
            for hit in hits:
                src = hit.get("_source") or {}
                resource_uid = str(src.get("resource_uid") or "").strip()
                asset_uid = str(src.get("asset_uid") or "").strip()
                if not resource_uid:
                    continue
                uniq = (resource_uid, asset_uid)
                if uniq in seen:
                    continue
                seen.add(uniq)
                deduped_docs.append(
                    {
                        "resource_uid": resource_uid,
                        "asset_uid": asset_uid,
                        "title": src.get("title"),
                        "description": src.get("description"),
                        "source_system": src.get("source_system"),
                        "source_url": src.get("source_url"),
                        "subject_ids": src.get("subject_ids") or [],
                        "subject_names_vi": src.get("subject_names_vi") or [],
                        "subject_names_en": src.get("subject_names_en") or [],
                        "subject_codes": src.get("subject_codes") or [],
                        "program_ids": src.get("program_ids") or [],
                        "bm25_score": float(hit.get("_score") or 0.0),
                    }
                )
                if len(deduped_docs) >= shortlist_limit:
                    break
            base_result["documents"] = deduped_docs
            base_result["used"] = bool(deduped_docs)
        except requests.RequestException as exc:
            base_result["error"] = str(exc)
        except Exception as exc:
            base_result["error"] = f"Tầng 1 ES lỗi không xác định: {exc}"
        finally:
            base_result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
        return base_result

    def _load_document_rows(
        self,
        source_system: Optional[str],
        language: Optional[str],
        resource_uids: Optional[Sequence[str]] = None,
        asset_uids: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        spark = self._get_spark()
        if not self._table_exists(self.documents_table):
            raise PageIndexError(f"Thiếu bảng Silver documents: {self.documents_table}")
        docs = spark.table(self.documents_table).select(
            "asset_uid",
            "resource_uid",
            "source_system",
            "source_url",
            "title",
            "asset_path",
            "language",
        )
        if source_system:
            docs = docs.filter(F.col("source_system") == F.lit(source_system))
        if language:
            docs = docs.filter(F.lower(F.col("language")) == F.lit(language.lower()))
        filtered_resource_uids = [str(uid).strip() for uid in (resource_uids or []) if str(uid).strip()]
        filtered_asset_uids = [str(uid).strip() for uid in (asset_uids or []) if str(uid).strip()]
        if filtered_resource_uids and filtered_asset_uids:
            docs = docs.filter(
                (F.col("resource_uid").isin(filtered_resource_uids))
                | (F.col("asset_uid").isin(filtered_asset_uids))
            )
        elif filtered_resource_uids:
            docs = docs.filter(F.col("resource_uid").isin(filtered_resource_uids))
        elif filtered_asset_uids:
            docs = docs.filter(F.col("asset_uid").isin(filtered_asset_uids))

        if self._table_exists(self.structure_table):
            structures = spark.table(self.structure_table).select(
                "asset_uid",
                "total_pages",
                "toc_method",
                "toc_confidence",
                "table_of_contents_json",
                "structure_valid",
            )
            docs = docs.join(structures, on="asset_uid", how="left")

        if self._table_exists(self.resources_table):
            resources = spark.table(self.resources_table).select(
                "resource_uid",
                "matched_subjects",
                "program_ids",
                "subject_match_confidence",
                "subject_match_uncertain",
            )
            docs = docs.join(resources, on="resource_uid", how="left")

        return [_to_python(row) for row in docs.collect()]

    def _extract_sections_from_toc(self, toc_json: Any) -> List[Dict[str, Any]]:
        toc = _safe_json_loads(toc_json, [])
        if not isinstance(toc, list):
            return []
        sections: List[Dict[str, Any]] = []
        for chapter_index, chapter in enumerate(toc, start=1):
            if not isinstance(chapter, dict):
                continue
            chapter_title = str(chapter.get("chapter_title") or f"Chapter {chapter_index}")
            chapter_start = int(chapter.get("page_start") or 1)
            chapter_end = int(chapter.get("page_end") or chapter_start)
            raw_sections = chapter.get("sections") or []
            if not raw_sections:
                sections.append(
                    {
                        "chapter_title": chapter_title,
                        "section_title": chapter_title,
                        "page_start": chapter_start,
                        "page_end": chapter_end,
                    }
                )
                continue
            for section in raw_sections:
                if not isinstance(section, dict):
                    continue
                sections.append(
                    {
                        "chapter_title": chapter_title,
                        "section_title": str(section.get("section_title") or chapter_title),
                        "page_start": int(section.get("page_start") or chapter_start),
                        "page_end": int(section.get("page_end") or chapter_end),
                    }
                )
        return sections

    def _score_document_candidate(self, row: Dict[str, Any], bundle: QueryBundle, subject_hints: Dict[str, Any]) -> Dict[str, Any]:
        matched_subjects = _safe_json_loads(_to_python(row.get("matched_subjects") or []), [])
        if not isinstance(matched_subjects, list):
            matched_subjects = []
        doc_subject_ids = []
        for subject in matched_subjects:
            try:
                doc_subject_ids.append(int(subject.get("subject_id")))
            except Exception:
                continue
        raw_program_ids = _safe_json_loads(_to_python(row.get("program_ids") or []), [])
        if not isinstance(raw_program_ids, list):
            raw_program_ids = []
        doc_program_ids = [int(p) for p in raw_program_ids if str(p).isdigit()]

        title = str(row.get("title") or "")
        sections = self._extract_sections_from_toc(row.get("table_of_contents_json"))
        section_scope_text = " ".join(
            [
                f"{str(section.get('chapter_title') or '')} {str(section.get('section_title') or '')}"
                for section in sections[:20]
            ]
        )
        course_profile = _build_course_scope_profile(bundle.course_name)
        course_scope_eval = _evaluate_course_scope_text(f"{title} {section_scope_text}", course_profile)
        concept_terms = _expand_definition_target_tokens(bundle.concept_target) if bundle.concept_target else []
        definition_query = _is_definition_query(bundle.query_vi_original)
        concept_overlap_score = _overlap_score(f"{title} {section_scope_text}", concept_terms) if concept_terms else 0.0
        semantic_terms = _dedupe_keep_order(_tokenize(bundle.query_en_semantic) + _tokenize(bundle.query_vi_semantic))
        search_terms = list(dict.fromkeys(bundle.keywords_vi + bundle.keywords_en + semantic_terms))
        if any(marker in _ascii_fold(bundle.query_vi_original) for marker in ["co so du lieu", "database", "dbms", "sql"]):
            search_terms = [
                term
                for term in search_terms
                if term not in {"co", "so", "du", "lieu", "data", "van", "hoa", "truy", "chuan", "giao", "dich"} and len(term) >= 3
            ]
            search_terms = _dedupe_keep_order(
                search_terms + ["database", "dbms", "sql", "relational", "transaction", "normalization"]
            )
        title_score = (_overlap_score(title, search_terms) * 6.0)
        subject_score = (len(set(doc_subject_ids).intersection(set(subject_hints.get("subject_ids") or []))) * 5.5)
        program_score = (len(set(doc_program_ids).intersection(set(subject_hints.get("program_ids") or []))) * 2.0)

        section_candidates: List[Dict[str, Any]] = []
        toc_score = 0.0
        for section in sections:
            section_title = str(section.get("section_title") or "")
            chapter_title = str(section.get("chapter_title") or "")
            score = (_overlap_score(section_title, search_terms) * 4.0) + (_overlap_score(chapter_title, search_terms) * 2.0)
            if definition_query and concept_terms:
                section_text = f"{chapter_title} {section_title}"
                concept_section_overlap = _overlap_score(section_text, concept_terms)
                if concept_section_overlap > 0.0:
                    score += concept_section_overlap * 2.5
                    cue_terms = ["definition", "define", "what is", "introduction", "overview", "concept"]
                    if any(cue in _ascii_fold(section_text) for cue in cue_terms):
                        score += 3.0
            if score <= 0:
                continue
            section_candidates.append(
                {
                    "chapter_title": chapter_title,
                    "section_title": section_title,
                    "page_start": int(section.get("page_start") or 1),
                    "page_end": int(section.get("page_end") or int(section.get("page_start") or 1)),
                    "score": score,
                }
            )
        section_candidates.sort(key=lambda item: item["score"], reverse=True)
        toc_score = sum(item["score"] for item in section_candidates[:3])

        confidence_score = float(row.get("subject_match_confidence") or 0.0)
        toc_confidence = float(row.get("toc_confidence") or 0.0)
        toc_structure_score = (toc_confidence * 3.0) + (1.5 if bool(row.get("structure_valid")) else 0.0)
        uncertain_penalty = -1.0 if bool(row.get("subject_match_uncertain")) else 0.0

        explanation_bonus = 0.0
        if bundle.intent == "explanation" and section_candidates:
            top_section_text = (
                f"{section_candidates[0].get('chapter_title') or ''} "
                f"{section_candidates[0].get('section_title') or ''}"
            ).lower()
            if any(k in top_section_text for k in ["introduction", "overview", "definition", "concept", "fundamentals"]):
                explanation_bonus += 2.0

        course_scope_penalty = 0.0
        course_scope_bonus = 0.0
        if course_scope_eval.get("profile_name"):
            course_scope_bonus = float(course_scope_eval.get("allow_hits") or 0.0) * 1.8
            if bool(course_scope_eval.get("mismatch")):
                course_scope_penalty = 8.0
            elif float(course_scope_eval.get("allow_hits") or 0.0) <= 0.0:
                course_scope_penalty = 1.5
        concept_bonus = 0.0
        concept_penalty = 0.0
        if concept_terms:
            concept_bonus = concept_overlap_score * 2.0
            if concept_overlap_score <= 0.0:
                concept_penalty = 1.0

        total_score = (
            title_score
            + subject_score
            + program_score
            + toc_score
            + confidence_score
            + toc_structure_score
            + uncertain_penalty
            + explanation_bonus
            + course_scope_bonus
            + concept_bonus
            - course_scope_penalty
            - concept_penalty
        )
        query_signal_score = title_score + subject_score + program_score + toc_score
        return {
            "asset_uid": row.get("asset_uid"),
            "resource_uid": row.get("resource_uid"),
            "title": title or "Unknown",
            "source_system": row.get("source_system"),
            "source_url": row.get("source_url"),
            "asset_path": row.get("asset_path"),
            "language": row.get("language"),
            "total_pages": int(row.get("total_pages") or 0),
            "toc_method": row.get("toc_method"),
            "toc_confidence": float(row.get("toc_confidence") or 0.0),
            "structure_valid": bool(row.get("structure_valid")) if row.get("structure_valid") is not None else False,
            "table_of_contents_json": row.get("table_of_contents_json"),
            "matched_subjects": matched_subjects,
            "program_ids": doc_program_ids,
            "score": total_score,
            "score_breakdown": {
                "title_score": title_score,
                "subject_score": subject_score,
                "program_score": program_score,
                "toc_score": toc_score,
                "query_signal_score": query_signal_score,
                "subject_confidence_score": confidence_score,
                "toc_structure_score": toc_structure_score,
                "uncertain_penalty": uncertain_penalty,
                "intent_bonus": explanation_bonus,
                "course_scope_bonus": course_scope_bonus,
                "course_scope_penalty": -course_scope_penalty,
                "concept_bonus": concept_bonus,
                "concept_penalty": -concept_penalty,
            },
            "section_candidates": section_candidates[:12],
            "course_scope_mismatch": bool(course_scope_eval.get("mismatch")),
            "course_scope_allow_hits": float(course_scope_eval.get("allow_hits") or 0.0),
            "course_scope_deny_hits": float(course_scope_eval.get("deny_hits") or 0.0),
            "concept_overlap_score": float(concept_overlap_score),
        }

    def get_document(
        self,
        question: str,
        top_k: int = 3,
        source_system: Optional[str] = None,
        language: Optional[str] = None,
        reason: str = "",
        bundle: Optional[QueryBundle] = None,
    ) -> Dict[str, Any]:
        bundle = bundle or self._build_query_bundle(question)
        subject_hints = self._infer_subject_hints(bundle)
        tier1_result = self._search_tier1_candidates(
            bundle=bundle,
            subject_hints=subject_hints,
            source_system=source_system,
            top_k=max(1, top_k),
        )
        tier1_docs = tier1_result.get("documents") or []
        tier1_enabled = bool(tier1_result.get("enabled"))
        tier1_error = str(tier1_result.get("error") or "").strip()
        if tier1_enabled and not tier1_error and not tier1_docs:
            return {
                "tool": "get_document",
                "reason": reason,
                "query_bundle": {
                    "query_vi_original": bundle.query_vi_original,
                    "query_en_semantic": bundle.query_en_semantic,
                    "query_vi_semantic": bundle.query_vi_semantic,
                    "keywords_en": bundle.keywords_en,
                    "keywords_vi": bundle.keywords_vi,
                    "intent": bundle.intent,
                    "language": bundle.language,
                    "query_mode": bundle.query_mode,
                    "course_name": bundle.course_name,
                    "section_name": bundle.section_name,
                    "concept_target": bundle.concept_target,
                    "has_unresolved_placeholder": bundle.has_unresolved_placeholder,
                },
                "subject_hints": subject_hints,
                "tier1": tier1_result,
                "documents": [],
            }
        tier1_by_asset = {
            str(doc.get("asset_uid") or "").strip(): doc
            for doc in tier1_docs
            if str(doc.get("asset_uid") or "").strip()
        }
        tier1_by_resource = {
            str(doc.get("resource_uid") or "").strip(): doc
            for doc in tier1_docs
            if str(doc.get("resource_uid") or "").strip()
        }

        rows = self._load_document_rows(
            source_system=source_system,
            language=language,
            resource_uids=list(tier1_by_resource.keys()) if tier1_by_resource else None,
            asset_uids=list(tier1_by_asset.keys()) if tier1_by_asset else None,
        )
        if not rows and language:
            rows = self._load_document_rows(
                source_system=source_system,
                language=None,
                resource_uids=list(tier1_by_resource.keys()) if tier1_by_resource else None,
                asset_uids=list(tier1_by_asset.keys()) if tier1_by_asset else None,
            )
        if not rows and (tier1_by_asset or tier1_by_resource):
            # Index may be stale compared to Silver; fallback to Spark-only candidate scan.
            rows = self._load_document_rows(source_system=source_system, language=language)
        if not rows and language:
            rows = self._load_document_rows(source_system=source_system, language=None)

        candidates = [self._score_document_candidate(row, bundle, subject_hints) for row in rows]
        filtered_candidates: List[Dict[str, Any]] = []
        for candidate in candidates:
            tier1_doc = (
                tier1_by_asset.get(str(candidate.get("asset_uid") or "").strip())
                or tier1_by_resource.get(str(candidate.get("resource_uid") or "").strip())
            )
            bm25_score = float((tier1_doc or {}).get("bm25_score") or 0.0)
            candidate["score_breakdown"]["bm25_score"] = bm25_score
            query_signal_score = float((candidate.get("score_breakdown") or {}).get("query_signal_score") or 0.0)
            # Drop weak candidates that only have structural confidence but no lexical/subject signal.
            if query_signal_score <= 0.0 and bm25_score < self.tier1_min_bm25:
                continue
            bm25_bonus = min(self.tier1_bm25_bonus_cap, max(0.0, bm25_score * self.tier1_bm25_bonus_weight))
            candidate["score"] = float(candidate.get("score") or 0.0) + bm25_bonus
            candidate["score_breakdown"]["tier1_bm25_bonus"] = bm25_bonus
            if tier1_doc:
                candidate["tier1"] = tier1_doc
            filtered_candidates.append(candidate)

        candidates = [candidate for candidate in filtered_candidates if float(candidate.get("score") or 0.0) > 0.0]
        non_mismatch_candidates = [item for item in candidates if not bool(item.get("course_scope_mismatch"))]
        if non_mismatch_candidates:
            candidates = non_mismatch_candidates
        concept_candidates = [item for item in candidates if float(item.get("concept_overlap_score") or 0.0) > 0.0]
        if concept_candidates and bundle.concept_target:
            candidates = concept_candidates
        candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
        return {
            "tool": "get_document",
            "reason": reason,
            "query_bundle": {
                "query_vi_original": bundle.query_vi_original,
                "query_en_semantic": bundle.query_en_semantic,
                "query_vi_semantic": bundle.query_vi_semantic,
                "keywords_en": bundle.keywords_en,
                "keywords_vi": bundle.keywords_vi,
                "intent": bundle.intent,
                "language": bundle.language,
                "query_mode": bundle.query_mode,
                "course_name": bundle.course_name,
                "section_name": bundle.section_name,
                "concept_target": bundle.concept_target,
                "has_unresolved_placeholder": bundle.has_unresolved_placeholder,
            },
            "subject_hints": subject_hints,
            "tier1": tier1_result,
            "documents": candidates[: max(1, top_k)],
        }

    def _get_document_meta(self, asset_uid: str) -> Optional[Dict[str, Any]]:
        key = str(asset_uid or "").strip()
        if not key:
            return None
        cached = self._cache_get(self._document_meta_cache, key)
        if isinstance(cached, dict):
            return cached

        spark = self._get_spark()
        docs = (
            spark.table(self.documents_table)
            .filter(F.col("asset_uid") == F.lit(key))
            .limit(1)
            .collect()
        )
        if not docs:
            return None
        data = _to_python(docs[0])
        self._cache_set(self._document_meta_cache, key, data)
        return data

    def get_document_structure(self, asset_uid: str, reason: str = "") -> Dict[str, Any]:
        key = str(asset_uid or "").strip()
        if key:
            cached = self._cache_get(self._structure_cache, key)
            if isinstance(cached, dict):
                return {
                    "tool": "get_document_structure",
                    "reason": reason,
                    "asset_uid": key,
                    "found": bool(cached.get("found")),
                    "structure": cached.get("structure"),
                    "sections": cached.get("sections") or [],
                    "cache_hit": True,
                }

        if not self._table_exists(self.structure_table):
            return {
                "tool": "get_document_structure",
                "reason": reason,
                "asset_uid": asset_uid,
                "found": False,
                "structure": None,
                "sections": [],
                "cache_hit": False,
            }
        spark = self._get_spark()
        row = (
            spark.table(self.structure_table)
            .filter(F.col("asset_uid") == F.lit(asset_uid))
            .limit(1)
            .collect()
        )
        if not row:
            return {
                "tool": "get_document_structure",
                "reason": reason,
                "asset_uid": asset_uid,
                "found": False,
                "structure": None,
                "sections": [],
                "cache_hit": False,
            }
        data = _to_python(row[0])
        result = {
            "tool": "get_document_structure",
            "reason": reason,
            "asset_uid": asset_uid,
            "found": True,
            "structure": data,
            "sections": self._extract_sections_from_toc(data.get("table_of_contents_json")),
            "cache_hit": False,
        }
        if key:
            self._cache_set(
                self._structure_cache,
                key,
                {
                    "found": True,
                    "structure": data,
                    "sections": result.get("sections") or [],
                },
            )
        return result

    def _load_course_map(self) -> Dict[str, Any]:
        """Load curated {course_name -> asset_uids} map (cached). Keyed by
        ascii-folded course name for robust matching."""
        if self._course_map is not None:
            return self._course_map
        result: Dict[str, Any] = {}
        try:
            path = self.tier2_course_map_path
            if path and os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                for name, val in (raw or {}).items():
                    uids = [str(u) for u in (val.get("asset_uids") or []) if u]
                    books = val.get("books") or []
                    folded = _ascii_fold(str(name))
                    if folded and uids:
                        result[folded] = {"name": name, "asset_uids": uids, "books": books}
        except Exception:
            result = {}
        self._course_map = result
        return result

    def _resolve_course_books(self, course_name: str) -> Optional[Dict[str, Any]]:
        """Return {'name', 'asset_uids'} for a Moodle course_name, or None when
        the course is not in the curated map (-> use global cross-book)."""
        cmap = self._load_course_map()
        folded = _ascii_fold(str(course_name or ""))
        if not cmap or not folded:
            return None
        if folded in cmap:
            return cmap[folded]
        # tolerate Moodle suffixes/prefixes (e.g. "Giải tích 1 - HK1 2024")
        for key, val in cmap.items():
            if key and (key in folded or folded in key):
                return val
        return None

    def _get_tier1_docs_by_asset(self, asset_uids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        uids = [str(uid).strip() for uid in asset_uids if str(uid).strip()]
        if not uids or not self.tier1_es_host or not self.tier1_es_index:
            return {}
        body = {
            "size": len(uids),
            "_source": [
                "resource_uid",
                "asset_uid",
                "title",
                "description",
                "source_system",
                "source_url",
                "subject_names_vi",
                "subject_names_en",
                "subject_codes",
            ],
            "query": {"terms": {"asset_uid": uids}},
        }
        auth = None
        if self.tier1_es_username and self.tier1_es_password:
            auth = (self.tier1_es_username, self.tier1_es_password)
        try:
            resp = requests.post(
                f"{self.tier1_es_host}/{self.tier1_es_index}/_search",
                json=body,
                timeout=max(1.0, min(4.0, self.tier1_es_timeout)),
                auth=auth,
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", {}).get("hits", [])
            docs: Dict[str, Dict[str, Any]] = {}
            for hit in hits:
                src = hit.get("_source") or {}
                uid = str(src.get("asset_uid") or "").strip()
                if uid:
                    docs[uid] = src
            return docs
        except Exception as exc:
            logger.warning("Error fetching tier1 docs by asset: %s", exc)
            return {}

    def _source_label(self, source_system: str, source_url: str = "") -> str:
        folded = _ascii_fold(" ".join([source_system, source_url]))
        if "openstax" in folded:
            return "OpenStax"
        if "open umn" in folded or "opentextbooks" in folded or "open textbook library" in folded:
            return "Open Textbook Library"
        if "mit" in folded or "ocw" in folded:
            return "MIT OpenCourseWare"
        if source_system:
            return str(source_system).replace("_", " ").title()
        host = urlparse(str(source_url or "")).netloc.replace("www.", "")
        return host or "Nguồn OER"

    def _classify_material_type(self, item: Dict[str, Any], page_count: int = 0) -> str:
        title = str(item.get("title") or "")
        description = str(item.get("description") or "")
        source_system = str(item.get("source_system") or "")
        source_url = str(item.get("source_url") or "")
        folded = _ascii_fold(" ".join([title, description, source_system, source_url]))
        textbook_source = any(marker in folded for marker in ["openstax", "open textbook", "opentextbooks", "open textbook library"])
        if page_count and page_count < 40 and not textbook_source:
            return "tài liệu ngắn"
        if textbook_source or "textbook" in _ascii_fold(" ".join([title, source_system, source_url])):
            return "sách giáo trình"
        if any(marker in folded for marker in ["lecture transcript", "lecture notes", "slides", "handout"]):
            return "bài giảng/ghi chú"
        if re.search(r"\blec(?:ture)?\d+\b", folded) or re.search(r"\bf\d{2}-lec\d+\b", folded):
            return "bài giảng/ghi chú"
        if "course" in folded or "ocw" in folded:
            return "khóa học OER"
        if "book" in folded or "self contained" in folded:
            return "sách"
        return "tài liệu OER"

    def _score_course_material_quality(
        self,
        item: Dict[str, Any],
        page_count: int,
        map_index: int,
        preferred_subject_name: str = "",
    ) -> Tuple[float, str, bool]:
        title = str(item.get("title") or "")
        description = str(item.get("description") or "")
        source_system = str(item.get("source_system") or "")
        source_url = str(item.get("source_url") or "")
        folded = _ascii_fold(" ".join([title, description, source_system, source_url]))
        title_folded = _ascii_fold(title)
        course_folded = _ascii_fold(preferred_subject_name)
        material_type = self._classify_material_type(item, page_count)

        score = 80.0 - (float(map_index) * 0.75)
        subject_reason = ""

        base_focus_terms = _extract_subject_focus_terms(preferred_subject_name)
        base_focus_terms.extend(_derive_en_keywords_from_vi(preferred_subject_name, _tokenize(preferred_subject_name)))
        base_focus_terms.extend(_derive_vi_keywords_from_en(preferred_subject_name, _tokenize(preferred_subject_name)))
        focus_terms = _dedupe_keep_order([term for term in base_focus_terms if len(str(term)) >= 3])
        focus_phrases = _extract_subject_focus_phrases(preferred_subject_name, focus_terms)
        focus_overlap = _overlap_score(" ".join([title, description]), focus_terms)
        focus_phrase_hits = _phrase_overlap(" ".join([title, description]), focus_phrases)
        title_focus_overlap = _overlap_score(title, focus_terms)
        if focus_overlap > 0:
            score += focus_overlap * 5.0
        if focus_phrase_hits > 0:
            score += focus_phrase_hits * 12.0
        if title_focus_overlap > 0:
            score += title_focus_overlap * 8.0

        linear_course = any(marker in course_folded for marker in ["linear algebra", "dai so tuyen tinh"])
        if linear_course:
            linear_markers = [
                "linear algebra",
                "matrix",
                "matrices",
                "vector",
                "vectors",
                "eigenvalue",
                "eigenvector",
                "linear map",
                "linear transformation",
                "dai so tuyen tinh",
            ]
            linear_hit = any(marker in folded for marker in linear_markers)
            elementary_algebra_only = (
                "algebra" in title_folded
                and not any(marker in title_folded for marker in ["linear", "matrix", "vector"])
                and any(marker in title_folded for marker in ["elementary", "intermediate", "introductory", "advanced"])
            )
            if linear_hit:
                score += 30.0
                subject_reason = "Đúng trọng tâm Đại số tuyến tính."
            if elementary_algebra_only:
                score -= 55.0
                subject_reason = "Không ưu tiên vì là đại số phổ thông/đại cương, không phải Đại số tuyến tính."
        elif preferred_subject_name and focus_terms and focus_overlap <= 0.0 and focus_phrase_hits <= 0.0:
            score -= 18.0
            subject_reason = "Không ưu tiên vì metadata chưa bám sát tên môn."

        if page_count >= 250:
            score += 28.0
        elif page_count >= 120:
            score += 20.0
        elif page_count >= 60:
            score += 10.0
        elif page_count > 0:
            score -= 30.0

        textbook_source = any(marker in folded for marker in ["openstax", "open textbook", "opentextbooks", "open textbook library"])
        title_source_folded = _ascii_fold(" ".join([title, source_system, source_url]))
        if textbook_source or "textbook" in title_source_folded:
            score += 28.0
        if any(marker in folded for marker in ["complete set", "independent study", "self contained", "self-contained"]):
            score += 10.0
        if any(marker in folded for marker in ["lecture transcript", "slides", "handout"]):
            score -= 45.0
        if re.search(r"\blec(?:ture)?\d+\b", folded) or re.search(r"\bf\d{2}-lec\d+\b", folded):
            score -= 45.0
        if page_count and page_count < 40:
            score -= 35.0

        incomplete = bool(
            page_count > 0
            and page_count < 40
            and any(marker in material_type for marker in ["ngắn", "bài giảng"])
        )
        reason = "Ưu tiên vì có quy mô và mô tả giống tài liệu học hoàn chỉnh."
        if "sách giáo trình" in material_type:
            reason = "Ưu tiên vì là sách giáo trình/học liệu hoàn chỉnh."
        elif "khóa học" in material_type:
            reason = "Phù hợp vì là học liệu khóa học OER có cấu trúc."
        elif incomplete:
            reason = "Không ưu tiên vì giống bài giảng ngắn hơn là giáo trình hoàn chỉnh."
        if subject_reason and not subject_reason.startswith("Không ưu tiên"):
            reason = f"{reason} {subject_reason}"
        elif subject_reason:
            reason = subject_reason
        return score, reason, incomplete

    def _display_course_material_title(
        self,
        item: Dict[str, Any],
        title_counts: Dict[str, int],
    ) -> str:
        title = str(item.get("title") or "Tài liệu").strip()
        title_fold = _ascii_fold(title)
        source_label = str(item.get("recommendation_source_label") or "").strip()
        material_type = str(item.get("recommendation_material_type") or "").strip()
        page_count = int(item.get("recommendation_page_count") or 0)
        details: List[str] = []
        if source_label:
            details.append(source_label)
        if material_type:
            details.append(material_type)
        if page_count > 0:
            details.append(f"khoảng {page_count} trang")
        generic_short_title = len(_tokenize(title)) <= 2
        if (title_counts.get(title_fold, 0) > 1 or generic_short_title) and details:
            return f"{title} — {', '.join(details)}"
        if page_count > 0 and material_type in {"bài giảng/ghi chú", "tài liệu ngắn"}:
            return f"{title} — {material_type}, khoảng {page_count} trang"
        return title

    def _rank_course_map_books(
        self,
        course: Dict[str, Any],
        top_k: int = 5,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        all_books = [b for b in (course.get("books") or []) if isinstance(b, dict)]
        name = str(course.get("name") or "")
        target_k = max(1, min(5, int(top_k or 5)))
        asset_uids = [str(b.get("asset_uid") or "").strip() for b in all_books if str(b.get("asset_uid") or "").strip()]
        page_counts = self._get_page_counts(asset_uids)
        tier1_docs = self._get_tier1_docs_by_asset(asset_uids)

        enriched: List[Dict[str, Any]] = []
        for map_index, raw_book in enumerate(all_books):
            uid = str(raw_book.get("asset_uid") or "").strip()
            meta = tier1_docs.get(uid, {})
            item = dict(raw_book)
            for key in ["resource_uid", "title", "description", "source_system", "source_url"]:
                if meta.get(key):
                    item[key] = meta.get(key)
            page_count = int(page_counts.get(uid) or 0)
            quality_score, quality_reason, incomplete = self._score_course_material_quality(item, page_count, map_index, name)
            source_label = self._source_label(str(item.get("source_system") or ""), str(item.get("source_url") or ""))
            material_type = self._classify_material_type(item, page_count)
            item["recommendation_rank_score"] = quality_score
            item["recommendation_reason"] = quality_reason
            item["recommendation_page_count"] = page_count
            item["recommendation_source_label"] = source_label
            item["recommendation_material_type"] = material_type
            item["recommendation_incomplete"] = incomplete
            item["recommendation_subject_mismatch"] = quality_reason.startswith("Không ưu tiên")
            enriched.append(item)

        enriched.sort(key=lambda b: float(b.get("recommendation_rank_score") or 0.0), reverse=True)
        complete_books = [b for b in enriched if not bool(b.get("recommendation_incomplete"))]
        preferred_books = [
            b
            for b in enriched
            if not bool(b.get("recommendation_incomplete"))
            and not bool(b.get("recommendation_subject_mismatch"))
        ]
        if len(preferred_books) >= min(3, target_k):
            books = preferred_books[:target_k]
            dropped_incomplete = len(enriched) - len(complete_books)
        else:
            books = enriched[:target_k]
            dropped_incomplete = 0
        return books, enriched, dropped_incomplete

    def _crossbook_refusal(self, question, trace, document_result, answer_language, course_name=None):
        """Build a low-confidence refusal (out-of-scope / out-of-course)."""
        return {
            "question": question,
            "answer": _message_no_relevant(answer_language, course_name or ""),
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

    def _recommend_course_books(self, question, course, trace, answer_language, top_k=5):
        """Recommend the curated books of a Moodle course (course-scoped 'gợi ý
        sách'), instead of the noisy subject-based recommendation."""
        name = course.get("name") or ""
        books, enriched, dropped_incomplete = self._rank_course_map_books(course, top_k=top_k)

        title_counts: Dict[str, int] = {}
        for item in books:
            folded_title = _ascii_fold(str(item.get("title") or ""))
            if folded_title:
                title_counts[folded_title] = title_counts.get(folded_title, 0) + 1

        contexts = []
        for b in books:
            display_title = self._display_course_material_title(b, title_counts)
            contexts.append(
                {
                    "title": display_title,
                    "asset_uid": b.get("asset_uid"),
                    "source_url": b.get("source_url"),
                    "text": b.get("recommendation_reason") or b.get("description") or display_title,
                    "retrieval_score": float(b.get("recommendation_rank_score") or 0.0),
                }
            )
        lines = []
        for i, b in enumerate(books, 1):
            display_title = self._display_course_material_title(b, title_counts)
            line = f"{i}. {display_title}"
            lines.append(line)
        if answer_language == "en":
            intro = f"You are in the course \"{name}\". Here are {len(books)} core materials of this course:"
            tail = "\n\nConfidence: High"
        else:
            intro = f"Bạn đang ở môn \"{name}\". Mình gợi ý {len(books)} tài liệu của môn:"
            tail = "\n\nĐộ tin cậy: Cao"
        answer = f"{intro}\n\n" + "\n\n".join(lines) + tail
        trace.append(
            {
                "tool": "course_recommend",
                "course": name,
                "books": len(books),
                "candidates": len(enriched),
                "dropped_incomplete": dropped_incomplete,
                "quality_ranked": True,
            }
        )
        return {
            "question": question,
            "answer": answer,
            "contexts": contexts,
            "sources": self._build_sources(contexts),
            "confidence": "high",
            "search_mode": "pageindex",
            "pageindex_trace": trace,
            "query_bundle": None,
            "metrics": {
                "tier1_recall_at_k": 1.0,
                "tier1_recall_at_k_type": "curated_course",
                "tier1_k": len(books),
                "evidence_hit_rate": 1.0,
                "grounded_answer_rate": 1.0,
                "pages_loaded_total": 0,
                "pages_hit_total": 0,
            },
        }

    def _tier2_crossbook_active(self) -> bool:
        return self.tier2_crossbook and self._tier2_es_active()

    def _crossbook_relevance_ok(self, question: str, contexts: List[Dict[str, Any]], llm_timeout: Optional[int] = None, course_name: Optional[str] = None) -> bool:
        """One semantic LLM check. Without course_name: is the question academic
        (vs lifestyle)? With course_name: is the question within that course's
        subject? Returns True if relevant (or if no LLM — don't block)."""
        if not self.tier2_crossbook_scope_check or not self._local_llm_enabled():
            return True
        if course_name:
            # Retrieval-confidence override: the retrieved pages are already filtered
            # to THIS course's own books (asset_uid filter in _answer_from_crossbook_es),
            # so a strongly-matching page means the question IS answerable from the
            # course material. Trust the BM25 score and skip the fuzzy LLM gate to avoid
            # FALSE refusals on abstract / paraphrased (esp. Vietnamese) questions where
            # the 7B judge is uncertain. Only marginal-score questions still hit the gate.
            # Tunable: lower PAGEINDEX_TIER2_SCOPE_OVERRIDE_SCORE to answer more, raise it
            # to be stricter. Does NOT increase hallucination: a high score means the
            # page really contains the query terms.
            override_score = float(os.getenv("PAGEINDEX_TIER2_SCOPE_OVERRIDE_SCORE", "40.0"))
            top_score = max(
                (float(c.get("retrieval_score") or 0.0) for c in (contexts or [])),
                default=0.0,
            )
            if top_score >= override_score:
                return True
            # Course-membership check: reject questions about a clearly different subject.
            prompt = (
                f"A student is studying the course \"{course_name}\".\n"
                "Decide whether the QUESTION is within THIS course's subject area.\n"
                "relevant=false if the question clearly belongs to a DIFFERENT academic subject "
                "(e.g. a pure calculus question while in an Economics course), or is non-academic "
                "(cooking, sports, shopping, news...).\n"
                "relevant=true if it fits the course's subject (even if phrased simply or in Vietnamese).\n"
                "Examples:\n"
                'Course "Kinh tế học" (Economics), Q: What is a derivative of a function? -> {"relevant": false}\n'
                'Course "Giải tích" (Calculus), Q: What is a derivative of a function? -> {"relevant": true}\n'
                'Course "Giải tích" (Calculus), Q: What is inflation? -> {"relevant": false}\n'
                'Course "Kinh tế học" (Economics), Q: What is inflation? -> {"relevant": true}\n'
                'Course "Vật lý đại cương" (Physics), Q: What is photosynthesis? -> {"relevant": false}\n'
                'Return ONLY JSON: {"relevant": true} or {"relevant": false}\n\n'
                f"Course: {course_name}\nQ: {question}\n"
                "Directly return the final JSON. Do not output anything else."
            )
            data = self._call_local_llm_json(prompt, {"relevant": True}, request_timeout=llm_timeout)
            return bool(data.get("relevant", True))
        prompt = (
            "You classify whether a QUESTION belongs in an ACADEMIC textbook library (OER).\n"
            "relevant=false = everyday/non-academic: cooking & recipes, sports results, "
            "movies/music/celebrities, shopping & product picks, gadget/phone specs, real-time prices "
            "(gold, crypto, stocks), travel/visa, gardening/pets, app or website how-to, booking tickets, "
            "current politicians/news, greeting cards/personal writing, personal fitness/diet tips.\n"
            "relevant=true = academic/educational/scientific topics: definitions, theories, science, math, "
            "biology, chemistry, physics, economics, business concepts, law, history, engineering, statistics.\n"
            "Judge by TOPIC, ignore language/phrasing. Examples:\n"
            'Q: What is the best gaming laptop under $1000? -> {"relevant": false}\n'
            'Q: What are the new features of the iPhone 16? -> {"relevant": false}\n'
            'Q: What is the current gold price? -> {"relevant": false}\n'
            'Q: How to book train tickets on an app? -> {"relevant": false}\n'
            'Q: Suggest some good movies to watch. -> {"relevant": false}\n'
            'Q: How to care for an apricot tree? -> {"relevant": false}\n'
            'Q: What is photosynthesis? -> {"relevant": true}\n'
            'Q: What is international business? -> {"relevant": true}\n'
            'Q: Explain the derivative of a function. -> {"relevant": true}\n'
            'Return ONLY JSON: {"relevant": true} or {"relevant": false}\n\n'
            f"Q: {question}\n"
            "Directly return the final JSON. Do not output anything else."
        )
        data = self._call_local_llm_json(prompt, {"relevant": True}, request_timeout=llm_timeout)
        return bool(data.get("relevant", True))

    def _answer_from_crossbook_es(
        self,
        question: str,
        bundle: QueryBundle,
        documents: List[Dict[str, Any]],
        document_result: Dict[str, Any],
        trace: List[Dict[str, Any]],
        answer_language: str,
        remaining_time_fn,
    ) -> Optional[Dict[str, Any]]:
        """Phuong an 2: single BM25 query over the page index across ALL books,
        take the best pages, answer in one Groq call. Returns None to fall back
        to the legacy per-document loop when the index yields nothing."""
        # Lead with extracted KEYWORDS (discriminative content terms) so generic
        # question words ("what is ...") do not dominate. Do NOT boost `title`:
        # the book title is repeated on every page doc, so boosting it lets a
        # stopword in the question hijack retrieval to a book whose TITLE matches
        # (e.g. "What is X?" -> "What is Capitalism?"). Match on page content only.
        # Since the ES index is 100% English textbooks, use English query signals only
        # when available to avoid accent-folding collisions.
        # Guard against the VI-leak path: query_en_semantic falls back to the raw
        # Vietnamese question (see _build_query_bundle) when the concept is not in
        # the static map AND the LLM rewrite fails (GPU offline / Groq throttle).
        # Mixing Vietnamese tokens into a BM25 query over a 100%-English index
        # accent-folds into spurious matches (Turkish/Portuguese pages). Only use
        # query_en_semantic when it is actually ASCII (English).
        en_semantic = str(bundle.query_en_semantic or "").strip()
        if not en_semantic.isascii():
            en_semantic = ""
        en_parts = [
            " ".join(bundle.keywords_en),
            en_semantic,
        ]
        query_text_en = " ".join(p for p in en_parts if p).strip()
        if query_text_en:
            query_text = query_text_en
        else:
            query_text = " ".join(
                p for p in [
                    " ".join(bundle.keywords_vi),
                    str(bundle.query_vi_semantic or "").strip(),
                ] if p
            ).strip() or str(bundle.query_vi_original or "").strip()
        if not query_text:
            return None

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

        focused_definition_query = False
        if _is_definition_query(bundle.query_vi_original) and str(bundle.concept_target or "").strip():
            concept_terms = [
                term
                for term in _expand_definition_target_tokens(bundle.concept_target)
                if len(str(term or "").strip()) >= 3
            ]
            concept_terms = _dedupe_keep_order(concept_terms)[:12]
            if concept_terms:
                query_text = " ".join(concept_terms)
                focused_definition_query = True

        # Course-scoping: restrict to the Moodle course's curated books when matched.
        course = self._resolve_course_books(bundle.course_name) if self.tier2_course_scoped else None
        match_clause = {
            "multi_match": {
                "query": query_text,
                "type": "best_fields",
                "fields": ["text", "section_title^2", "chapter_title^2", "title"],
                "operator": "or",
                "minimum_should_match": "1<40%" if focused_definition_query else "2<70%"
            }
        }
        if course:
            query_block = {
                "bool": {
                    "must": [match_clause],
                    "filter": [{"terms": {"asset_uid": course["asset_uids"]}}],
                }
            }
        else:
            query_block = {"bool": {"must": [match_clause]}}

        if focused_definition_query and bundle.concept_target:
            concept_terms_en = [t for t in _expand_definition_target_tokens(bundle.concept_target) if t not in ["dao", "ham"]]
            if concept_terms_en:
                primary_concept_en = concept_terms_en[0]
                query_block["bool"].setdefault("should", [])
                query_block["bool"]["should"].append({
                    "multi_match": {
                        "query": primary_concept_en,
                        "fields": ["section_title^15", "text^5"],
                    }
                })

        if getattr(bundle, "document_title", None):
            query_block["bool"].setdefault("should", [])
            query_block["bool"]["should"].append({
                "match": {
                    "title": {
                        "query": bundle.document_title,
                        "boost": 10.0
                    }
                }
            })

        # Absolute BM25 minimum score: pages below this threshold are noise (e.g. CC license
        # pages that contain 'NoDerivatives' matching a math 'derivative' query). This is sent
        # to ES so it drops those hits at the index level before they are forwarded to the LLM.
        CROSSBOOK_MIN_SCORE = float(
            os.getenv("PAGEINDEX_CROSSBOOK_MIN_SCORE", "6.0")
        )
        es_size = max(1, int(self.tier2_crossbook_pages))
        if focused_definition_query:
            es_size = max(24, es_size * 3)
        body = {
            "size": es_size,
            "_source": ["asset_uid", "title", "page_no", "chapter_title", "section_title", "text"],
            "min_score": CROSSBOOK_MIN_SCORE,
            "query": query_block,
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
            return None
        if not hits:
            # Scoped to a course but nothing matched -> out-of-course refusal.
            if course:
                trace.append({"tool": "course_scope_guard", "course": course["name"],
                              "reason": "Khong co noi dung khop trong sach cua mon -> tu choi."})
                return self._crossbook_refusal(question, trace, document_result, answer_language, course["name"])
            return None

        # Relative score gate: drop pages whose BM25 score is below 30% of the top hit's score.
        # This removes borderline false-positive pages that only matched on stopwords or accidental
        # keyword overlaps (e.g. Creative Commons "NoDerivatives" page appearing for a math query).
        top_score = float(hits[0].get("_score") or 0.0)
        RELATIVE_SCORE_THRESHOLD = 0.30
        hits = [h for h in hits if float(h.get("_score") or 0.0) >= top_score * RELATIVE_SCORE_THRESHOLD]
        trace.append({
            "tool": "crossbook_score_filter",
            "min_score_absolute": CROSSBOOK_MIN_SCORE,
            "top_score": top_score,
            "relative_threshold": RELATIVE_SCORE_THRESHOLD,
            "hits_after_filter": len(hits),
            "focused_definition_query": focused_definition_query,
            "query_text": query_text[:240],
        })
        if not hits:
            if course:
                return self._crossbook_refusal(question, trace, document_result, answer_language, course["name"])
            return None

        docs_by_uid = {str(d.get("asset_uid") or ""): d for d in (documents or [])}
        contexts: List[Dict[str, Any]] = []
        course_profile = _build_course_scope_profile(bundle.course_name)
        definition_target_terms = _expand_definition_target_tokens(bundle.concept_target) if focused_definition_query else []
        for hit in hits:
            src = hit.get("_source") or {}
            text = _strip_surrogate_chars(str(src.get("text") or "")).strip()
            if not text:
                continue

            # --- Structural Noise Page Detection (TOC, Index, Glossary, Short Page Filter) ---
            text_lower = text.lower()
            ch_title = str(src.get("chapter_title") or "").lower()
            sec_title = str(src.get("section_title") or "").lower()
            page_no = src.get("page_no")

            # Word count threshold
            words = text_lower.split()
            word_count = len(words)

            # Skip pages that have under 40 words but dense matches (indicates index/TOC snippet or cover page)
            if word_count < 40:
                continue

            # Flag words for Table of Contents, Indexes, Glossary, Bibliographies
            noise_indicators = {
                "table of contents", "mục lục", "index", "glossary", 
                "bibliography", "references", "chương trình đào tạo",
                "tài liệu tham khảo", "danh mục", "từ điển thuật ngữ"
            }
            
            is_noise_page = False
            for indicator in noise_indicators:
                if indicator in ch_title or indicator in sec_title:
                    is_noise_page = True
                    break

            # If it's page 1, 2, 3 or the last few pages, and contains "contents" or "index" in the text body
            if page_no is not None and (int(page_no) <= 5 or int(page_no) >= 300):
                if any(ind in text_lower for ind in ["table of contents", "contents", "mục lục", "index"]):
                    is_noise_page = True

            # Detect dense dots or index pattern (e.g. "Chapter 1 .... 12")
            dot_lines = text_lower.count("...") + text_lower.count(" . . ") + text_lower.count("···")
            if dot_lines >= 4:
                is_noise_page = True

            if is_noise_page:
                continue

            # --- Exercise/Problem Page Detection ---
            is_exercise_page = False
            exercise_indicators = [
                "exercises", "problems", "review exercises", 
                "chapter review", "practice problems", "analytical exercises",
                "multiple-choice questions", "review questions"
            ]
            if (
                any(ind in text_lower for ind in exercise_indicators) or
                any(ind in sec_title for ind in exercise_indicators) or
                any(ind in ch_title for ind in exercise_indicators) or
                text_lower.count("[t]") >= 2 or
                re.search(r"\b\d+\s*\.\s*\[t\]", text_lower)
            ):
                is_exercise_page = True

            if focused_definition_query and is_exercise_page:
                continue
            # ---------------------------------------------------------------------------------

            uid = str(src.get("asset_uid") or "")
            meta = docs_by_uid.get(uid) or {}
            scope_text = " ".join(
                [
                    str(src.get("title") or meta.get("title") or ""),
                    str(src.get("chapter_title") or ""),
                    str(src.get("section_title") or ""),
                    text[:1200],
                ]
            )
            scope_eval = _evaluate_course_scope_text(scope_text, course_profile)
            if bool(scope_eval.get("mismatch")):
                continue
            concept_overlap = _overlap_score(scope_text, definition_target_terms) if definition_target_terms else 0.0
            definition_bonus = 0.0
            if focused_definition_query:
                if concept_overlap <= 0.0:
                    continue
                definition_bonus += concept_overlap * 4.0
                if _has_targeted_definition_cue(scope_text, definition_target_terms):
                    definition_bonus += 12.0
                elif _has_definition_cue(scope_text):
                    definition_bonus += 4.0
                if int(src.get("page_no") or 0) <= 120:
                    definition_bonus += 2.0
                if "volume 1" in _ascii_fold(scope_text):
                    definition_bonus += 8.0
            score = float(hit.get("_score") or 0.0) + definition_bonus
            if is_exercise_page:
                score -= 10.0
            es_page_no = int(src.get("page_no") or 0)
            physical_page_no = es_page_no + 1
            contexts.append({
                "text": text,
                "page_no": physical_page_no,
                "title": src.get("title") or meta.get("title"),
                "section_title": src.get("section_title"),
                "chapter_title": src.get("chapter_title"),
                "asset_uid": uid,
                "chunk_id": f"{uid}::page::{physical_page_no}",
                "retrieval_score": score,
            })
        if not contexts:
            return None
        if focused_definition_query:
            contexts.sort(key=lambda item: float(item.get("retrieval_score") or 0.0), reverse=True)
        contexts = contexts[:max(1, int(self.tier2_crossbook_pages))]

        # Primary document = book owning the top-ranked page (for attribution).
        top_uid = contexts[0]["asset_uid"]
        primary_doc = docs_by_uid.get(top_uid) or {
            "asset_uid": top_uid,
            "title": contexts[0].get("title"),
        }
        book_count = len({c["asset_uid"] for c in contexts})
        trace.append({
            "tool": "crossbook_es",
            "reason": "Phuong an 2: 1 truy van BM25 tren oer_pages_tier2, lay top pages.",
            "query": query_text[:160],
            "pages": len(contexts),
            "books_spanned": book_count,
            "top_book": primary_doc.get("title"),
            "course_scoped": course["name"] if course else None,
        })

        # Semantic scope gate: BM25 always finds *some* matching page, so reject
        # out-of-scope questions here before answering (restores OOS detection).
        remaining = remaining_time_fn()
        scope_timeout = max(1, min(self.llm_json_timeout, remaining)) if remaining > 0 else 1
        if not self._crossbook_relevance_ok(question, contexts, llm_timeout=scope_timeout,
                                            course_name=course["name"] if course else None):
            trace.append({"tool": "crossbook_scope_guard", "relevant": False,
                          "course_scoped": course["name"] if course else None,
                          "reason": "Cau hoi ngoai pham vi mon hoc / hoc lieu -> tu choi."})
            return {
                "question": question,
                "answer": _message_no_relevant(answer_language, primary_doc.get("title") or ""),
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

        remaining = remaining_time_fn()
        if remaining <= 1 and not self.disable_fallback:
            answer = self._fallback_answer(question, contexts, "medium", answer_language=answer_language)
        else:
            answer_timeout = max(1, min(self.llm_answer_timeout, remaining)) if remaining > 0 else 1
            answer = self._generate_answer(
                question, primary_doc, contexts, "medium",
                answer_language=answer_language, llm_timeout=answer_timeout,
            )

        return {
            "question": question,
            "answer": answer,
            # Trả lời dùng toàn bộ contexts (8 trang) để tổng hợp, nhưng chỉ HIỂN THỊ
            # top-N nguồn cho gọn (tránh 1 câu định nghĩa kèm 8 nguồn).
            "contexts": contexts[: self.tier2_crossbook_show],
            "confidence": "medium",
            "search_mode": "pageindex",
            "pageindex_trace": trace,
            "query_bundle": document_result.get("query_bundle"),
            "document": {
                "asset_uid": primary_doc.get("asset_uid"),
                "resource_uid": primary_doc.get("resource_uid"),
                "title": primary_doc.get("title"),
                "source_system": primary_doc.get("source_system"),
            },
            "metrics": self._build_ask_metrics(
                document_result=document_result,
                selected_document=primary_doc,
                pages_loaded_total=len(contexts),
                pages_hit_total=len(contexts),
                contexts=contexts,
                answer=answer,
                found_relevant_evidence=True,
            ),
        }
