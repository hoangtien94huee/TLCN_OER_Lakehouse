"""Book recommendation, find-material, history/summary resolution.

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


class _RecommendMixin:
    """Book recommendation, find-material, history/summary resolution."""

    def _build_recommendation_profile(self, question: str, preferred_subject_name: str, bundle: Optional[QueryBundle] = None) -> Dict[str, Any]:
        user_text = _strip_moodle_context(question)
        combined = " ".join([user_text, preferred_subject_name]).strip()
        folded = _ascii_fold(combined)
        generic_terms = _recommendation_generic_terms()

        if preferred_subject_name.strip():
            subject_focus_terms = _extract_subject_focus_terms(preferred_subject_name)
        else:
            recommendation_stop = {
                "goi", "tai", "lieu", "sach", "tham", "khao", "cho", "mon",
                "nay", "hoc", "recommend", "suggest", "book", "books",
                "resource", "resources", "reference", "materials", "about",
                "for", "the", "and", "cua", "cac", "nhung", "mot", "nao",
                "nen", "doc", "xem", "gioi", "thieu", "lam", "quen",
                "nhap", "ve", "voi", "toi", "minh", "ban", "giup",
                "hay", "xin", "duoc", "khong", "vai",
            }
            topic_tokens = [
                tok for tok in _tokenize(user_text)
                if len(tok) >= 3 and tok not in generic_terms and tok not in recommendation_stop
            ]
            subject_focus_terms = _dedupe_keep_order(topic_tokens)

        if bundle:
            en_keywords = [
                tok for tok in (bundle.keywords_en or [])
                if len(tok) >= 3 and tok not in generic_terms
            ]
            subject_focus_terms = _dedupe_keep_order(subject_focus_terms + en_keywords)

        if preferred_subject_name.strip():
            subject_focus_phrases = _extract_subject_focus_phrases(preferred_subject_name, subject_focus_terms)
        else:
            subject_focus_phrases = _extract_subject_focus_phrases(user_text, subject_focus_terms)

        db_markers = [
            "co so du lieu",
            "database",
            "dbms",
            "sql",
            "relational",
            "transaction",
            "normalization",
            "data modeling",
        ]
        is_database_domain = any(marker in folded for marker in db_markers)
        anchor_terms = [
            "database",
            "databases",
            "dbms",
            "sql",
            "relational",
            "transaction",
            "normalization",
            "schema",
            "co so du lieu",
            "he quan tri co so du lieu",
            "truy van",
            "chuan hoa",
            "giao dich",
        ] if is_database_domain else []
        anchor_phrases = [
            "co so du lieu",
            "he quan tri co so du lieu",
            "database systems",
            "database management",
            "relational database",
            "relational model",
            "sql",
            "normalization",
            "transaction",
            "data modeling",
            "schema design",
        ] if is_database_domain else []
        positive_terms = _dedupe_keep_order(
            subject_focus_terms
            + _tokenize(preferred_subject_name)
            + _tokenize(user_text)
            + anchor_terms
        )
        if not is_database_domain:
            anchor_terms = _dedupe_keep_order(subject_focus_terms)
            anchor_phrases = _dedupe_keep_order(subject_focus_phrases)
        offdomain_terms = [
            "economics",
            "labor",
            "macroeconomics",
            "microeconomics",
            "biology",
            "chemistry",
            "medicine",
            "anatomy",
            "finance",
            "accounting",
            "marketing",
            "law",
            "politics",
            "kinh",
            "sinh",
            "duoc",
            "psychology",
            "sociology",
            "philosophy",
            "geography",
            "geology",
            "astronomy",
        ]
        offdomain_phrases = [
            "labor economics",
            "macroeconomics",
            "microeconomics",
            "economic growth",
            "biology",
            "biological",
            "materials and structures",
            "mechanics of materials",
            "mechanical engineering",
            "structural engineering",
            "civil engineering",
            "chemical engineering",
            "electrical engineering",
            "aerospace engineering",
            "anatomy",
            "medicine",
            "pharmacology",
            "organic chemistry",
            "inorganic chemistry",
            "molecular biology",
            "political science",
        ]
        if subject_focus_terms:
            focus_set = set(subject_focus_terms)
            offdomain_terms = [t for t in offdomain_terms if t not in focus_set]
        return {
            "domain": "database" if is_database_domain else "generic",
            "positive_terms": positive_terms,
            "anchor_terms": anchor_terms,
            "anchor_phrases": anchor_phrases,
            "subject_focus_terms": subject_focus_terms,
            "subject_focus_phrases": subject_focus_phrases,
            "generic_terms": generic_terms,
            "offdomain_terms": offdomain_terms,
            "offdomain_phrases": offdomain_phrases,
        }

    def _build_recommendation_query(
        self,
        question: str,
        preferred_subject_name: str,
        profile: Dict[str, Any],
    ) -> str:
        if preferred_subject_name.strip():
            topic_label = preferred_subject_name.strip()
        else:
            user_text = _strip_moodle_context(question).strip()
            rec_noise = re.compile(
                r"\b(goi y|gợi ý|de xuat|đề xuất|gioi thieu|giới thiệu|"
                r"tai lieu|tài liệu|sach|sách|tham khao|tham khảo|"
                r"reference|recommend|suggest|book|books|resource|resources|materials|"
                r"cho mon|cho môn|mon nay|môn này|nay|về|ve|"
                r"cho|toi|minh|ban|giup|hay|xin|di|duoc|khong|mot|vai|nao)\b",
                flags=re.IGNORECASE,
            )
            topic_label = rec_noise.sub("", _ascii_fold(user_text)).strip()
            topic_label = re.sub(r"\s+", " ", topic_label).strip()
            if not topic_label:
                topic_label = user_text

        query_parts: List[str] = [topic_label]
        focus_terms = [str(term) for term in (profile.get("subject_focus_terms") or []) if str(term).strip()]
        if focus_terms:
            query_parts.append(" ".join(focus_terms[:6]))
        if profile.get("domain") == "database":
            query_parts.extend(
                [
                    "sach co so du lieu dbms sql mo hinh quan he chuan hoa transaction",
                    "database textbook dbms sql relational model normalization transaction",
                    "reference materials for database systems and sql practice",
                ]
            )
        else:
            query_parts.extend(
                [
                    f"tai lieu tham khao {topic_label}".strip(),
                    f"reference materials {topic_label}".strip(),
                ]
            )
        return " ".join([part for part in _dedupe_keep_order(query_parts) if part]).strip()

    def _dedupe_recommendations(self, documents: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        kept: Dict[str, Dict[str, Any]] = {}
        duplicates_removed = 0
        for item in documents:
            resource_uid = str(item.get("resource_uid") or "").strip().lower()
            source_url = str(item.get("source_url") or "").strip().lower()
            title_fold = _ascii_fold(item.get("title"))
            dedupe_key = resource_uid or source_url or title_fold
            if not dedupe_key:
                dedupe_key = f"fallback::{len(kept)}"
            prev = kept.get(dedupe_key)
            if not prev:
                kept[dedupe_key] = item
                continue
            prev_score = float(prev.get("recommendation_rank_score") or 0.0)
            curr_score = float(item.get("recommendation_rank_score") or 0.0)
            if curr_score > prev_score:
                kept[dedupe_key] = item
            duplicates_removed += 1
        deduped = list(kept.values())
        deduped.sort(key=lambda x: float(x.get("recommendation_rank_score") or 0.0), reverse=True)
        return deduped, duplicates_removed

    def _derive_recommendation_reason(self, doc_text: str, profile: Dict[str, Any]) -> str:
        folded = _ascii_fold(doc_text)
        tokens = set(_tokenize(doc_text))
        if profile.get("domain") == "database":
            if "sql" in tokens or "query" in tokens or "truy van" in folded:
                return "Phù hợp để luyện thực hành truy vấn SQL."
            if "normalization" in tokens or "chuan hoa" in folded:
                return "Phù hợp vì có nội dung về chuẩn hóa cơ sở dữ liệu."
            if "transaction" in tokens or "giao dich" in folded:
                return "Phù hợp vì đề cập transaction và toàn vẹn dữ liệu."
            if (
                "relational" in tokens
                or "schema" in tokens
                or "relational model" in folded
                or "data modeling" in folded
            ):
                return "Phù hợp vì bao quát mô hình quan hệ và thiết kế lược đồ."
            if "dbms" in tokens or "database" in tokens or "co so du lieu" in folded:
                return "Phù hợp cho học phần nhập môn và nền tảng DBMS."
            return "Phù hợp ở mức liên quan cơ sở dữ liệu theo metadata; nên kiểm tra mục lục trước khi học sâu."
        focus_terms = [str(t) for t in (profile.get("subject_focus_terms") or []) if str(t).strip()]
        if focus_terms:
            matched = [t for t in focus_terms if t in tokens or t in folded]
            if matched:
                return f"Nội dung liên quan đến: {', '.join(matched[:4])}."
        return "Phù hợp dựa trên từ khóa tìm kiếm và metadata tài liệu."

    def _rank_recommendation_candidates(
        self,
        documents: Sequence[Dict[str, Any]],
        preferred_subject_name: str,
        profile: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        ranked: List[Dict[str, Any]] = []
        dropped_offdomain = 0
        dropped_weak = 0
        preferred_subject_fold = _ascii_fold(preferred_subject_name)
        subject_focus_terms = [str(term) for term in (profile.get("subject_focus_terms") or []) if str(term).strip()]
        subject_focus_phrases = [str(term) for term in (profile.get("subject_focus_phrases") or []) if str(term).strip()]
        generic_terms = [str(term) for term in (profile.get("generic_terms") or []) if str(term).strip()]

        for item in documents:
            tier1_doc = item.get("tier1") or {}
            breakdown = item.get("score_breakdown") or {}
            title = str(item.get("title") or tier1_doc.get("title") or "")
            description = str(tier1_doc.get("description") or "")
            subject_text = " ".join(
                [
                    " ".join([str(x) for x in (tier1_doc.get("subject_names_vi") or [])]),
                    " ".join([str(x) for x in (tier1_doc.get("subject_names_en") or [])]),
                    " ".join([str(x) for x in (tier1_doc.get("subject_codes") or [])]),
                ]
            )
            text_for_match = " ".join([title, description, subject_text])
            core_text_for_match = " ".join([title, description])
            title_anchor_overlap = _overlap_score(title, profile.get("anchor_terms") or [])
            title_anchor_phrase_hits = _phrase_overlap(title, profile.get("anchor_phrases") or [])
            subject_anchor_overlap = _overlap_score(subject_text, profile.get("anchor_terms") or [])
            subject_anchor_phrase_hits = _phrase_overlap(subject_text, profile.get("anchor_phrases") or [])
            has_title_anchor = bool(title_anchor_overlap > 0.0 or title_anchor_phrase_hits > 0.0)
            has_subject_anchor = bool(subject_anchor_overlap > 0.0 or subject_anchor_phrase_hits > 0.0)

            positive_overlap = _overlap_score(text_for_match, profile.get("positive_terms") or [])
            anchor_token_overlap = _overlap_score(text_for_match, profile.get("anchor_terms") or [])
            anchor_phrase_hits = _phrase_overlap(text_for_match, profile.get("anchor_phrases") or [])
            anchor_overlap = anchor_token_overlap + (anchor_phrase_hits * 2.0)
            subject_focus_overlap = _overlap_score(text_for_match, subject_focus_terms)
            subject_focus_phrase_hits = _phrase_overlap(text_for_match, subject_focus_phrases)
            title_focus_overlap = _overlap_score(title, subject_focus_terms)
            core_focus_overlap = _overlap_score(core_text_for_match, subject_focus_terms)
            core_focus_phrase_hits = _phrase_overlap(core_text_for_match, subject_focus_phrases)
            generic_title_hits = _overlap_score(title, generic_terms)
            offdomain_hits = int(
                _overlap_score(text_for_match, profile.get("offdomain_terms") or [])
                + (_phrase_overlap(text_for_match, profile.get("offdomain_phrases") or []) * 2.0)
            )
            subject_score = float(breakdown.get("subject_score") or 0.0)
            program_score = float(breakdown.get("program_score") or 0.0)
            bm25_score = float((tier1_doc.get("bm25_score") or breakdown.get("bm25_score") or 0.0))
            base_score = float(item.get("score") or 0.0)

            course_match_bonus = 0.0
            folded_text = _ascii_fold(text_for_match)
            if preferred_subject_fold and preferred_subject_fold in folded_text:
                course_match_bonus = 6.0

            rank_score = (
                base_score
                + (positive_overlap * 1.6)
                + (anchor_overlap * 4.5)
                + (2.2 if (subject_score > 0 and anchor_overlap > 0.0) else 0.0)
                + (0.4 if (program_score > 0 and anchor_overlap > 0.0) else 0.0)
                + (0.4 if bm25_score >= self.tier1_min_bm25 else 0.0)
                + course_match_bonus
                + (subject_focus_overlap * 3.8)
                + (subject_focus_phrase_hits * 6.0)
                + (2.5 if title_focus_overlap > 0.0 else 0.0)
                + (core_focus_overlap * 4.2)
                + (core_focus_phrase_hits * 7.0)
            )
            if offdomain_hits > 0:
                rank_score -= float(offdomain_hits) * 7.5
            if subject_focus_overlap <= 0.0 and subject_focus_phrase_hits <= 0.0 and generic_title_hits > 0.0:
                rank_score -= float(generic_title_hits) * 4.0

            domain_relevant = bool(anchor_overlap > 0 or positive_overlap >= 2.0)
            if profile.get("domain") == "database":
                domain_relevant = bool(
                    (anchor_overlap > 0.0)
                    and (
                        has_title_anchor
                        or has_subject_anchor
                    )
                )
                # For recommendation list quality: prefer explicit DB titles over generic titles.
                if not has_title_anchor:
                    domain_relevant = False
            elif subject_focus_terms:
                domain_relevant = bool(
                    core_focus_overlap > 0.0
                    or core_focus_phrase_hits > 0.0
                    or title_focus_overlap > 0.0
                )
                if generic_title_hits > 0.0 and core_focus_overlap <= 0.0 and title_focus_overlap <= 0.0:
                    domain_relevant = False

            if profile.get("domain") == "database" and anchor_overlap <= 0.0:
                dropped_weak += 1
                continue
            if profile.get("domain") == "database" and offdomain_hits > 0 and anchor_overlap < 2.0:
                dropped_offdomain += 1
                continue
            if offdomain_hits > 0 and anchor_overlap <= 0 and subject_score <= 0:
                dropped_offdomain += 1
                continue
            if not domain_relevant and rank_score <= 2.0:
                dropped_weak += 1
                continue

            ranked_item = dict(item)
            ranked_item["recommendation_rank_score"] = rank_score
            ranked_item["recommendation_domain_relevant"] = domain_relevant
            ranked_item["recommendation_anchor_overlap"] = float(anchor_overlap)
            ranked_item["recommendation_anchor_phrase_hits"] = float(anchor_phrase_hits)
            ranked_item["recommendation_subject_match"] = bool(subject_score > 0)
            ranked_item["recommendation_offdomain_hits"] = offdomain_hits
            ranked_item["recommendation_subject_focus_overlap"] = float(subject_focus_overlap)
            ranked_item["recommendation_subject_focus_phrase_hits"] = float(subject_focus_phrase_hits)
            ranked_item["recommendation_title_focus_overlap"] = float(title_focus_overlap)
            ranked_item["recommendation_core_focus_overlap"] = float(core_focus_overlap)
            ranked_item["recommendation_core_focus_phrase_hits"] = float(core_focus_phrase_hits)
            ranked_item["recommendation_reason"] = self._derive_recommendation_reason(text_for_match, profile)
            ranked.append(ranked_item)

        ranked.sort(key=lambda x: float(x.get("recommendation_rank_score") or 0.0), reverse=True)
        return ranked, {
            "dropped_offdomain": dropped_offdomain,
            "dropped_weak": dropped_weak,
        }

    def _estimate_recommendation_confidence(
        self,
        documents: Sequence[Dict[str, Any]],
        stats: Dict[str, int],
    ) -> str:
        if not documents:
            return "low"
        all_domain_relevant = all(bool(item.get("recommendation_domain_relevant")) for item in documents)
        any_offdomain_selected = any(int(item.get("recommendation_offdomain_hits") or 0) > 0 for item in documents)
        subject_match_count = sum(1 for item in documents if bool(item.get("recommendation_subject_match")))
        anchor_hit_count = sum(1 for item in documents if float(item.get("recommendation_anchor_overlap") or 0.0) > 0.0)
        anchor_phrase_hit_count = sum(1 for item in documents if float(item.get("recommendation_anchor_phrase_hits") or 0.0) > 0.0)
        subject_focus_hit_count = sum(
            1
            for item in documents
            if (
                float(item.get("recommendation_subject_focus_overlap") or 0.0) > 0.0
                or float(item.get("recommendation_subject_focus_phrase_hits") or 0.0) > 0.0
                or float(item.get("recommendation_title_focus_overlap") or 0.0) > 0.0
                or float(item.get("recommendation_core_focus_overlap") or 0.0) > 0.0
                or float(item.get("recommendation_core_focus_phrase_hits") or 0.0) > 0.0
            )
        )
        dropped_offdomain = int(stats.get("dropped_offdomain") or 0)
        domain = str(stats.get("domain") or "generic").strip().lower()

        if domain == "database":
            if (
                len(documents) >= 3
                and all_domain_relevant
                and not any_offdomain_selected
                and subject_match_count >= 1
                and anchor_hit_count >= 2
                and anchor_phrase_hit_count >= 1
                and dropped_offdomain == 0
            ):
                return "high"
            if all_domain_relevant and not any_offdomain_selected and anchor_hit_count >= 1:
                return "medium"
            return "low"

        if (
            len(documents) >= 3
            and all_domain_relevant
            and not any_offdomain_selected
            and subject_focus_hit_count >= len(documents)
            and anchor_hit_count >= 1
            and dropped_offdomain <= 1
        ):
            return "high"
        if (
            all_domain_relevant
            and not any_offdomain_selected
            and (subject_focus_hit_count >= 1 or anchor_hit_count >= 1)
        ):
            return "medium"
        return "low"

    def _build_recommendation_contexts(self, documents: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        contexts: List[Dict[str, Any]] = []
        for item in documents:
            tier1_doc = item.get("tier1") or {}
            title = str(item.get("title") or tier1_doc.get("title") or "Tai lieu")
            description = str(tier1_doc.get("description") or "").strip()
            reason = str(item.get("recommendation_reason") or "").strip()
            text = reason if reason else (description if description else title)
            contexts.append(
                {
                    "text": text,
                    "page_no": None,
                    "title": title,
                    "section_title": None,
                    "chapter_title": None,
                    "source_url": item.get("source_url") or tier1_doc.get("source_url"),
                    "minio_url": None,
                    "asset_uid": item.get("asset_uid"),
                    "chunk_id": f"{item.get('asset_uid')}::recommendation",
                    "retrieval_score": float(item.get("recommendation_rank_score") or item.get("score") or 0.0),
                }
            )
        return contexts

    def _format_recommendation_answer(
        self,
        documents: Sequence[Dict[str, Any]],
        preferred_subject_name: str,
        confidence: str,
        answer_language: str = "vi",
    ) -> str:
        if not documents:
            return _message_no_document(answer_language, preferred_subject_name)
        confidence_label_vi = "Cao" if confidence == "high" else ("Trung bình" if confidence == "medium" else "Thấp")
        confidence_label_en = "High" if confidence == "high" else ("Medium" if confidence == "medium" else "Low")
        has_subject = bool(preferred_subject_name.strip())

        lines: List[str] = []
        for idx, item in enumerate(documents, start=1):
            tier1_doc = item.get("tier1") or {}
            title = str(item.get("title") or tier1_doc.get("title") or f"Tài liệu {idx}").strip()
            default_reason = "Relevant to the current course." if answer_language == "en" else "Phù hợp với nội dung đang tìm."
            reason = str(item.get("recommendation_reason") or default_reason).strip()
            if answer_language == "en":
                line = f"{idx}. {title}\n- Why: {reason}"
            else:
                line = f"{idx}. {title}\n- {reason}"
            lines.append(line)
        body = "\n\n".join(lines)

        limited_note_en = "\nNote: limited high-quality matches were found." if len(documents) < 3 else ""
        limited_note_vi = "\nLưu ý: hệ thống chỉ tìm được số ít tài liệu thật sự sát nội dung." if len(documents) < 3 else ""

        if answer_language == "en":
            if has_subject:
                intro = f"You are currently in \"{preferred_subject_name.strip()}\". I suggest {len(documents)} relevant materials:"
            else:
                intro = f"Based on your question, I suggest {len(documents)} relevant materials:"
            return f"{intro}\n\n{body}\n\nConfidence: {confidence_label_en}{limited_note_en}"

        if has_subject:
            intro = f"Bạn đang ở môn \"{preferred_subject_name.strip()}\". Mình gợi ý {len(documents)} tài liệu phù hợp:"
        else:
            intro = f"Dựa trên câu hỏi của bạn, mình gợi ý {len(documents)} tài liệu liên quan:"
        return f"{intro}\n\n{body}\n\nĐộ tin cậy: {confidence_label_vi}{limited_note_vi}"

    def _search_book_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        if not self.tier1_es_host or not self.tier1_es_index:
            return None
        import re
        query_title = re.sub(r"[^\w\s]", " ", title).strip()
        if not query_title:
            return None
        body = {
            "size": 1,
            "_source": [
                "resource_uid",
                "asset_uid",
                "title",
                "description",
                "source_system",
                "source_url"
            ],
            "query": {
                "match": {
                    "title": {
                        "query": query_title,
                        "operator": "and"
                    }
                }
            }
        }
        request_url = f"{self.tier1_es_host}/{self.tier1_es_index}/_search"
        auth = None
        if self.tier1_es_username and self.tier1_es_password:
            auth = (self.tier1_es_username, self.tier1_es_password)
        try:
            import requests
            response = requests.post(
                request_url,
                json=body,
                timeout=2,
                auth=auth
            )
            response.raise_for_status()
            hits = response.json().get("hits", {}).get("hits", [])
            if hits:
                hit = hits[0]
                src = hit["_source"]
                return {
                    "resource_uid": src.get("resource_uid"),
                    "asset_uid": src.get("asset_uid"),
                    "title": src.get("title"),
                    "description": src.get("description"),
                    "source_system": src.get("source_system"),
                    "source_url": src.get("source_url")
                }
        except Exception as exc:
            logger.warning("Error searching book by title: %s", exc)
        return None

    def _book_from_course_map_entry(self, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        asset_uid = str(entry.get("asset_uid") or "").strip()
        title = str(entry.get("title") or "").strip()
        if not asset_uid and not title:
            return None

        book: Dict[str, Any] = {
            "resource_uid": entry.get("resource_uid"),
            "asset_uid": asset_uid or None,
            "title": title or "Tài liệu",
            "description": entry.get("description") or "",
            "source_system": entry.get("source_system"),
            "source_url": entry.get("source_url"),
        }

        if asset_uid:
            try:
                meta = self._get_document_meta(asset_uid)
                if meta:
                    book.update(
                        {
                            "resource_uid": meta.get("resource_uid") or book.get("resource_uid"),
                            "title": meta.get("title") or book.get("title"),
                            "description": meta.get("description") or book.get("description"),
                            "source_system": meta.get("source_system") or book.get("source_system"),
                            "source_url": meta.get("source_url") or book.get("source_url"),
                        }
                    )
            except Exception as exc:
                logger.warning("Could not enrich course-map book %s from metadata: %s", asset_uid, exc)

        return book if book.get("asset_uid") or book.get("title") else None

    def _resolve_book_from_history(self, question: str, history: Optional[List[Dict[str, str]]] = None) -> Optional[Dict[str, Any]]:
        import re
        q_clean = _ascii_fold(_strip_moodle_context(question))
        question_with_context = question
        
        # Check if user refers to a book by index
        idx_match = re.search(
            r"\b(quyen|cuon|sach|book|tailieu|tai lieu)\b(?:\s+(?:so|số|thu|thứ|number|no\.?|#))?\s*(\d+|mot|hai|ba|bon|nam|thu nhat|thu hai|thu ba|thu tu|thu nam|#\d+)\b",
            q_clean,
            re.IGNORECASE
        )
        if idx_match:
            idx_str = idx_match.group(2).lower()
            idx = None
            if idx_str in {"1", "mot", "thu nhat", "#1"}:
                idx = 0
            elif idx_str in {"2", "hai", "thu hai", "#2"}:
                idx = 1
            elif idx_str in {"3", "ba", "thu ba", "#3"}:
                idx = 2
            elif idx_str in {"4", "bon", "thu tu", "#4"}:
                idx = 3
            elif idx_str in {"5", "nam", "thu nam", "#5"}:
                idx = 4
            else:
                digit_match = re.search(r"\d+", idx_str)
                if digit_match:
                    idx = int(digit_match.group(0)) - 1
            
            if idx is not None and idx >= 0:
                # 1. If Moodle course context is present, resolve by the same ranked course list
                # used for recommendations. This keeps "cuốn số 1" aligned with the UI list
                # even when titles are generic or duplicated.
                course_books: List[Dict[str, Any]] = []
                moodle_context = _parse_moodle_context(question_with_context)
                cname = str(moodle_context.get("course_name") or "").strip()
                if cname:
                    course_data = self._resolve_course_books(cname)
                    if course_data:
                        ranked_books, _, _ = self._rank_course_map_books(course_data, top_k=max(5, idx + 1))
                        if idx < len(ranked_books):
                            course_book = self._book_from_course_map_entry(ranked_books[idx])
                            if course_book:
                                return course_book
                        if "books" in course_data:
                            course_books = [b for b in (course_data.get("books") or []) if isinstance(b, dict)]

                # 2. Try to resolve the book from recommendation responses in conversation history
                for msg in reversed(history or []):
                    if msg.get("role") in {"assistant", "bot"}:
                        text = msg.get("text") or ""
                        text_lower = _ascii_fold(text)
                        # Ensure this message actually looks like a book suggestion list
                        if any(kw in text_lower for kw in ["goi y", "tai lieu", "sach", "suggest", "material", "reference"]):
                            lines = text.split("\n")
                            temp_titles = []
                            for line in lines:
                                line_trimmed = line.strip()
                                num_match = re.match(r"^(\d+)\.\s*(.*)$", line_trimmed)
                                if num_match:
                                    temp_titles.append(num_match.group(2).strip())
                            if idx < len(temp_titles):
                                target_title = temp_titles[idx]
                                target_title = re.sub(r"\*\*|\*", "", target_title).strip()
                                book = self._search_book_by_title(target_title)
                                if book:
                                    return book

                # 3. Last course fallback: raw course map order, for legacy maps with no ranking metadata.
                if idx < len(course_books):
                    target_title = str(course_books[idx].get("title") or "")
                    target_title = re.sub(r"\*\*|\*", "", target_title).strip()
                    if target_title:
                        book = self._search_book_by_title(target_title)
                        if book:
                            return book
                    course_book = self._book_from_course_map_entry(course_books[idx])
                    if course_book:
                        return course_book

        # Check if question contains any book name from history
        for msg in reversed(history or []):
            if msg.get("role") in {"assistant", "bot"}:
                text = msg.get("text") or ""
                lines = text.split("\n")
                for line in lines:
                    line_trimmed = line.strip()
                    num_match = re.match(r"^(\d+)\.\s*(.*)$", line_trimmed)
                    if num_match:
                        title = re.sub(r"\*\*|\*", "", num_match.group(2)).strip()
                        if len(title) > 5 and _ascii_fold(title) in q_clean:
                            book = self._search_book_by_title(title)
                            if book:
                                return book

        # Fallback: extract title directly from question
        summary_keywords = ["tom tat sach", "tom tat cuon", "tom tat quyen", "tom tat", "summarize book", "summarize"]
        for kw in summary_keywords:
            if kw in q_clean:
                parts = q_clean.split(kw)
                if len(parts) > 1:
                    candidate = parts[1].strip()
                    if len(candidate) > 3:
                        book = self._search_book_by_title(candidate)
                        if book:
                            return book
        return None

    def _extract_active_book_title_from_history(self, history: Optional[List[Dict[str, str]]]) -> str:
        if not history:
            return ""
        import re
        for msg in reversed(history):
            text = msg.get("text") or ""
            match = re.search(r"\*\*Tóm tắt sách:\s*(.*?)\*\*", text)
            if match:
                return match.group(1).strip()
        return ""

    def _is_summary_request(self, question: str) -> bool:
        q = _ascii_fold(question)
        keywords = [
            "tom tat", "tóm tắt", "muc luc", "mục lục", "chuong trinh", "chương trình",
            "summarize", "summary", "toc", "chapters", "outline"
        ]
        return any(kw in q for kw in keywords)

    def _generate_toc_summary(self, book: Dict[str, Any], answer_language: str = "vi") -> Dict[str, Any]:
        asset_uid = book.get("asset_uid")
        book_title = book.get("title")
        description = book.get("description") or ""
        clean_description_raw = re.sub(r"\{\{%.*?%\}\}", " ", str(description or ""), flags=re.DOTALL)
        clean_description_raw = re.sub(r"\[[^\]]{1,180}\]\([^)]*\)", " ", clean_description_raw)
        clean_description_raw = re.sub(r"\[[^\]]{1,180}\]_/courses/\S+\)?", " ", clean_description_raw)
        clean_description_raw = re.sub(
            r"(?is)\b(compared with|so với)\b.*?(?=\.|\n|$)",
            " ",
            clean_description_raw,
        )
        clean_description = _normalize_pdf_text(clean_description_raw)
        clean_description = re.sub(r"https?://\S+|/courses/\S+", " ", clean_description)
        clean_description = re.sub(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]", "", clean_description)
        clean_description = re.sub(r"\s+", " ", clean_description).strip()
        
        structure = self.get_document_structure(asset_uid)
        sections = structure.get("sections") or []
        summary_mode = "toc"
        sampled_pages: List[Dict[str, Any]] = []
        sampled_page_count = 0
        
        toc_lines = []
        # Generic/boilerplate section labels to always skip
        _GENERIC_SECTIONS = {
            "introduction", "chapter review", "review", "preface", "contents",
            "table of contents", "index", "glossary", "answers", "bibliography",
            "references", "key terms", "key concepts", "exercises", "problems",
        }

        def _is_generic_section(sec_t: str, ch_t: str) -> bool:
            """Return True if a section title should be skipped (generic or redundant)."""
            sl = sec_t.lower().strip()
            cl = ch_t.lower().strip()
            # Exact generic labels
            if sl in _GENERIC_SECTIONS:
                return True
            # Starts with generic word
            if re.match(r'^(introduction|chapter review|key (terms|concepts|equations)|section (summary|exercises))$', sl):
                return True
            # sec_title is a substring of ch_title (e.g. "Chapter 1" inside "Chapter 1 Integration")
            if sl and cl and (sl in cl or cl in sl):
                return True
            # Bare chapter/appendix labels like "Chapter 1", "Appendix A"
            if re.match(r'^(chapter|appendix|chuong|phu luc)\s+[\dA-Za-z]+$', sl, re.IGNORECASE):
                return True
            return False

        if sections:
            # Deduplicate chapter keys and filter sub-sections
            raw_chapters = {}
            for sec in sections:
                ch_title = (sec.get("chapter_title") or "Khác").strip()
                sec_title = (sec.get("section_title") or "").strip()
                if not ch_title:
                    continue
                if ch_title not in raw_chapters:
                    raw_chapters[ch_title] = []
                if sec_title and sec_title != ch_title and not _is_generic_section(sec_title, ch_title) and sec_title not in raw_chapters[ch_title]:
                    raw_chapters[ch_title].append(sec_title)

            sorted_ch_keys = sorted(raw_chapters.keys(), key=len, reverse=True)
            filtered_ch_keys = []
            for ch in sorted_ch_keys:
                is_duplicate_prefix = False
                for longer_ch in filtered_ch_keys:
                    if longer_ch.startswith(ch) or ch.startswith(longer_ch):
                        is_duplicate_prefix = True
                        break
                if not is_duplicate_prefix:
                    filtered_ch_keys.append(ch)

            chapters = {}
            for sec in sections:
                ch_title = (sec.get("chapter_title") or "Khác").strip()
                sec_title = (sec.get("section_title") or "").strip()
                if ch_title in filtered_ch_keys:
                    if ch_title not in chapters:
                        chapters[ch_title] = []
                    if sec_title and sec_title != ch_title and not _is_generic_section(sec_title, ch_title) and sec_title not in chapters[ch_title]:
                        chapters[ch_title].append(sec_title)
            for ch, secs in chapters.items():
                toc_lines.append(f"- **{ch}**")
                for s in secs[:3]:
                    toc_lines.append(f"  * {s}")
        toc_str = "\n".join(toc_lines)
        if not toc_str:
            sampled = self._sample_summary_pages_from_es(str(asset_uid or ""), max_pages=8)
            sampled_pages = list(sampled.get("pages") or [])
            sampled_page_count = int(sampled.get("page_count") or 0)
            summary_mode = str(sampled.get("mode") or "no_toc_metadata_only")
            page_blocks: List[str] = []
            per_page_limit = 1200
            for page in sampled_pages:
                pno = int(page.get("page_no") or 0)
                section_label = str(page.get("section_title") or page.get("chapter_title") or "").strip()
                text = _normalize_pdf_text(str(page.get("text") or ""))
                if len(text) > per_page_limit:
                    text = f"{text[:per_page_limit]}\n...[truncated]..."
                label = f"[Trang {pno}]"
                if section_label:
                    label += f" {section_label}"
                page_blocks.append(f"{label}\n{text}")
            if page_blocks:
                toc_str = "\n\n".join(page_blocks)
            else:
                toc_str = clean_description if clean_description else "Không có TOC hoặc trang mẫu đủ thông tin."
            
        if summary_mode == "toc":
            evidence_label = "Mục lục"
            scope_note = (
                "Hãy liệt kê đầy đủ danh sách các chương chính và dịch toàn bộ tiêu đề chương sang tiếng Việt "
                "BẮT BUỘC dịch 100% tiêu đề các chương (dạng: Chương 1, Chương 2,...) và tiêu đề các mục con sang tiếng Việt tự nhiên. "
                "Không giữ nguyên tiếng Anh cho tiêu đề. Trình bày dưới dạng danh sách phân cấp thụt lề rõ ràng."
            )
        elif summary_mode == "no_toc_sampled_pages":
            evidence_label = "Mô tả và các trang mẫu"
            scope_note = (
                "Tài liệu không có TOC khả dụng. Hãy tóm tắt dựa trên mô tả và các trang mẫu được trích xuất "
                "(ưu tiên introduction/overview/preface/contents và một số trang đầu-giữa-cuối). "
                "Không khẳng định đây là tóm tắt đầy đủ tuyệt đối của toàn bộ sách; hãy diễn đạt là tóm tắt dựa trên mẫu trang."
            )
        else:
            evidence_label = "Mô tả metadata"
            scope_note = (
                "Tài liệu không có TOC và không lấy được trang mẫu. Chỉ tóm tắt ở mức khái quát dựa trên metadata; "
                "không khẳng định bao quát đầy đủ toàn bộ sách."
            )

        prompt = (
            "Bạn là một trợ lý học tập OER thông thái. Hãy viết một tóm tắt ngắn gọn và có cấu trúc "
            "về nội dung chính chương trình học của cuốn sách dưới đây dựa trên bằng chứng được cung cấp.\n\n"
            f"Tên sách: {book_title}\n"
            f"Mô tả sơ lược: {clean_description}\n"
            f"Chế độ tóm tắt: {summary_mode}\n"
            f"Bằng chứng ({evidence_label}):\n{toc_str}\n\n"
            "Yêu cầu:\n"
            f"1. {scope_note}\n"
            "2. Bắt buộc trả lời 100% bằng tiếng Việt tự nhiên; không trộn tiếng Trung, tiếng Anh hoặc ngôn ngữ khác trừ thuật ngữ chuyên ngành cần giữ nguyên.\n"
            "3. Giữ câu trả lời cô đọng, dễ đọc dưới dạng bullet points.\n"
            "4. Chỉ dùng thông tin có trong mô tả/bằng chứng; không bịa thêm chương, tác giả, ví dụ hoặc công thức không có trong bằng chứng.\n"
            "5. Tuyệt đối không đưa URL, đường dẫn /courses/..., mã học phần nội bộ, markdown link lỗi, hoặc câu so sánh kiểu 'So với [tài liệu]...'.\n"
            "6. Với Đại số tuyến tính, dịch 'diagonalization' là 'chéo hóa' và 'singular value decomposition' là 'phân rã giá trị kỳ dị'.\n"
            "Hãy trả lời trực tiếp phần tóm tắt, không thêm lời chào mở đầu hay kết bài."
        )
        try:
            summary = self._call_local_llm(prompt, request_timeout=180)
        except Exception as exc:
            logger.warning("Local LLM call for summary failed, fallback to raw list. detail=%s", exc)
            if summary_mode == "toc":
                summary = "Dưới đây là cấu trúc các chương chính:\n\n" + toc_str
            elif sampled_pages:
                summary = "Tài liệu không có TOC khả dụng. Dưới đây là các điểm chính suy ra từ mô tả và trang mẫu:\n\n" + toc_str
            else:
                summary = "Tài liệu không có TOC hoặc trang mẫu đủ thông tin. Tóm tắt khái quát dựa trên metadata:\n\n" + toc_str

        summary_lines = []
        for line in str(summary or "").splitlines():
            folded_line = _ascii_fold(line)
            if "/courses/" in line or "http://" in line or "https://" in line:
                continue
            if "so voi [" in folded_line or "compared with [" in folded_line:
                continue
            line = re.sub(r"\[[^\]]{1,120}\]\([^)]*\)", "", line)
            line = re.sub(r"\[[^\]]{1,120}\]_/courses/\S+\)?", "", line)
            line = re.sub(r"/courses/\S+", "", line)
            line = line.replace("。", ".")
            line = line.replace("chẩn đoán hóa", "chéo hóa").replace("Chẩn đoán hóa", "Chéo hóa")
            line = re.sub(r"[\u3000-\u303f\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]", "", line)
            line = re.sub(r"\s+", " ", line).rstrip()
            if line.strip():
                summary_lines.append(line)
        summary = "\n".join(summary_lines).strip() or str(summary or "").strip()

        # Post-process TOC mode: ensure chapter-level lines (starting with "- ") are bold
        if summary_mode == "toc":
            bolded_lines = []
            for line in summary.splitlines():
                stripped = line.lstrip()
                # Top-level bullet (chapter) but not already bold and not a sub-item
                if stripped.startswith("- ") and not line.startswith("  ") and not line.startswith("\t"):
                    content = stripped[2:].strip()
                    # Add bold if not already bold
                    if content and not content.startswith("**"):
                        line = f"- **{content}**"
                bolded_lines.append(line)
            summary = "\n".join(bolded_lines)
            
        answer = f"**Tóm tắt sách: {book_title}**\n\n{summary}"
        contexts = []
        if sampled_pages:
            for page in sampled_pages[:5]:
                contexts.append(
                    {
                        "text": _normalize_pdf_text(str(page.get("text") or ""))[:900],
                        "page_no": int(page.get("page_no") or 0) or None,
                        "title": book_title,
                        "section_title": page.get("section_title"),
                        "chapter_title": page.get("chapter_title"),
                        "source_url": book.get("source_url"),
                        "minio_url": None,
                        "asset_uid": asset_uid,
                        "chunk_id": f"{asset_uid}::summary::p{page.get('page_no')}",
                        "retrieval_score": float(page.get("score") or 1.0),
                    }
                )
        if not contexts:
            contexts = [{
                "text": clean_description or book_title,
                "page_no": None,
                "title": book_title,
                "section_title": None,
                "chapter_title": None,
                "source_url": book.get("source_url"),
                "minio_url": None,
                "asset_uid": asset_uid,
                "chunk_id": f"{asset_uid}::summary",
                "retrieval_score": 1.0,
            }]
        return {
            "question": f"Tóm tắt sách {book_title}",
            "answer": answer,
            "contexts": contexts,
            "sources": self._build_sources(contexts),
            "confidence": "high",
            "search_mode": "pageindex",
            "pageindex_trace": [
                {
                    "tool": "toc_summarizer",
                    "book": book_title,
                    "sections": len(sections),
                    "summary_mode": summary_mode,
                    "sampled_pages": [int(p.get("page_no") or 0) for p in sampled_pages],
                    "page_count": sampled_page_count,
                }
            ],
            "query_bundle": None,
            "metrics": {
                "tier1_recall_at_k": 1.0,
                "tier1_recall_at_k_type": "toc_summary",
                "tier1_k": 1,
                "evidence_hit_rate": 1.0,
                "grounded_answer_rate": 1.0,
                "pages_loaded_total": 0,
                "pages_hit_total": 0,
            }
        }

    def recommend_books(
        self,
        question: str,
        top_k: int = 5,
        source_system: Optional[str] = None,
        language: Optional[str] = None,
        bundle: Optional[QueryBundle] = None,
    ) -> Dict[str, Any]:
        answer_language = _resolve_answer_language(language, question)
        moodle_context = _parse_moodle_context(question)
        preferred_subject_name = str(moodle_context.get("course_name") or "").strip()
        profile = self._build_recommendation_profile(question, preferred_subject_name, bundle)
        retrieval_query = self._build_recommendation_query(question, preferred_subject_name, profile)

        context_lines: List[str] = []
        for key in ["course_id", "course_name", "section_id", "section_name", "activity_id", "activity_name", "role", "page_url"]:
            value = str(moodle_context.get(key) or "").strip()
            if value:
                context_lines.append(f"- {key}={value}")
        retrieval_question = retrieval_query
        if context_lines:
            retrieval_question = f"{retrieval_query}\n\n[Moodle context]\n" + "\n".join(context_lines)

        document_result = self.get_document(
            retrieval_question,
            top_k=max(12, min(30, max(12, int(top_k or 5) * 6))),
            source_system=source_system,
            language=None,
            reason="Recommendation pipeline: ưu tiên metadata môn học hiện tại và lọc lệch domain.",
        )
        documents = document_result.get("documents") or []
        if documents:
            uids = list({doc["asset_uid"] for doc in documents if doc.get("asset_uid")})
            page_counts = self._get_page_counts(uids)
            filtered_docs = []
            for doc in documents:
                uid = doc.get("asset_uid")
                pcount = page_counts.get(uid, 0)
                if pcount >= 10:
                    filtered_docs.append(doc)
            if len(filtered_docs) >= 3 or (len(filtered_docs) > 0 and len(documents) <= 5):
                documents = filtered_docs

        ranked, rank_stats = self._rank_recommendation_candidates(
            documents=documents,
            preferred_subject_name=preferred_subject_name,
            profile=profile,
        )
        deduped, duplicates_removed = self._dedupe_recommendations(ranked)

        target_k = max(3, min(5, int(top_k or 5)))
        if profile.get("domain") == "database":
            selected = [
                item
                for item in deduped
                if bool(item.get("recommendation_domain_relevant"))
                and int(item.get("recommendation_offdomain_hits") or 0) == 0
                and (
                    float(item.get("recommendation_anchor_overlap") or 0.0) >= 1.0
                    or bool(item.get("recommendation_subject_match"))
                )
            ][:target_k]
        else:
            selected = [
                item
                for item in deduped
                if bool(item.get("recommendation_domain_relevant"))
                and int(item.get("recommendation_offdomain_hits") or 0) == 0
                and (
                    float(item.get("recommendation_subject_focus_overlap") or 0.0) >= 1.0
                    or float(item.get("recommendation_subject_focus_phrase_hits") or 0.0) >= 1.0
                    or float(item.get("recommendation_title_focus_overlap") or 0.0) >= 1.0
                    or float(item.get("recommendation_core_focus_overlap") or 0.0) >= 1.0
                    or float(item.get("recommendation_core_focus_phrase_hits") or 0.0) >= 1.0
                )
            ][:target_k]
        if profile.get("domain") != "database" and len(selected) < 3:
            for item in deduped:
                if item in selected:
                    continue
                if int(item.get("recommendation_offdomain_hits") or 0) > 0:
                    continue
                if not bool(item.get("recommendation_domain_relevant")):
                    continue
                if (
                    float(item.get("recommendation_subject_focus_overlap") or 0.0) <= 0.0
                    and float(item.get("recommendation_subject_focus_phrase_hits") or 0.0) <= 0.0
                    and float(item.get("recommendation_title_focus_overlap") or 0.0) <= 0.0
                    and float(item.get("recommendation_core_focus_overlap") or 0.0) <= 0.0
                    and float(item.get("recommendation_core_focus_phrase_hits") or 0.0) <= 0.0
                ):
                    continue
                if float(item.get("recommendation_rank_score") or 0.0) < 4.0:
                    continue
                selected.append(item)
                if len(selected) >= target_k:
                    break
        if not selected and deduped:
            fallback_subject_match = [
                item
                for item in deduped
                if int(item.get("recommendation_offdomain_hits") or 0) == 0
                and (
                    bool(item.get("recommendation_subject_match"))
                    or float(item.get("recommendation_anchor_overlap") or 0.0) >= 1.0
                )
            ]
            if fallback_subject_match:
                selected = fallback_subject_match[: min(2, target_k, len(fallback_subject_match))]
            else:
                fallback_safe = [
                    item
                    for item in deduped
                    if int(item.get("recommendation_offdomain_hits") or 0) == 0
                    and float(item.get("recommendation_rank_score") or 0.0) >= 4.0
                ]
                selected = fallback_safe[: min(2, target_k, len(fallback_safe))]
            if not selected and preferred_subject_name.strip():
                selected = deduped[:1]

        confidence = self._estimate_recommendation_confidence(
            selected,
            {
                "domain": str(profile.get("domain") or "generic"),
                "dropped_offdomain": int(rank_stats.get("dropped_offdomain") or 0),
                "dropped_weak": int(rank_stats.get("dropped_weak") or 0),
            },
        )
        answer = self._format_recommendation_answer(
            documents=selected,
            preferred_subject_name=preferred_subject_name,
            confidence=confidence,
            answer_language=answer_language,
        )
        contexts = self._build_recommendation_contexts(selected)
        selected_document = selected[0] if selected else None
        trace = [
            {
                "tool": "recommend_books",
                "reason": "Luồng recommendation riêng: không đọc page PDF.",
                "preferred_subject_name": preferred_subject_name,
                "retrieval_query": retrieval_query,
            },
            {
                "tool": "recommendation_ranking",
                "candidates_before_rank": len(documents),
                "candidates_after_rank": len(ranked),
                "duplicates_removed": duplicates_removed,
                "offdomain_dropped": int(rank_stats.get("dropped_offdomain") or 0),
                "weak_dropped": int(rank_stats.get("dropped_weak") or 0),
                "documents_used": len(selected),
                "confidence": confidence,
            },
        ]
        return {
            "question": question,
            "answer": answer,
            "contexts": contexts,
            "sources": self._build_sources(contexts),
            "confidence": confidence,
            "search_mode": "pageindex",
            "pageindex_trace": trace,
            "query_bundle": document_result.get("query_bundle"),
            "document": {
                "asset_uid": (selected_document or {}).get("asset_uid"),
                "resource_uid": (selected_document or {}).get("resource_uid"),
                "title": (selected_document or {}).get("title"),
                "source_system": (selected_document or {}).get("source_system"),
            },
            "metrics": self._build_ask_metrics(
                document_result=document_result,
                selected_document=selected_document,
                pages_loaded_total=0,
                pages_hit_total=0,
                contexts=contexts,
                answer=answer,
                found_relevant_evidence=bool(selected),
            ),
        }

    def _answer_find_material(
        self, contexts: List[Dict[str, Any]], answer_language: str = "vi"
    ) -> str:
        """Câu "Tài liệu nào nói về X?": trả về TÊN tài liệu nguồn (theo thứ tự truy hồi),
        KHÔNG giải thích nội dung. Retrieval đã tìm đúng sách → chỉ cần nêu tên."""
        seen: List[str] = []
        for c in contexts:
            t = str(c.get("title") or "").strip()
            if t and t not in seen:
                seen.append(t)
            if len(seen) >= 3:
                break
        if not seen:
            return _message_no_relevant(answer_language, "")
        top, others = seen[0], seen[1:]
        src = ", ".join(f"[Nguồn {i+1}]" if answer_language == "vi" else f"[Source {i+1}]"
                        for i in range(len(seen)))
        if answer_language == "vi":
            a = f"1) Trả lời: Tài liệu phù hợp nhất là \"{top}\"."
            a += ("\n2) Chi tiết: Các tài liệu liên quan khác: "
                  + "; ".join(f"\"{t}\"" for t in others) + "."
                  if others else
                  "\n2) Chi tiết: Đây là tài liệu chứa nội dung liên quan trực tiếp đến câu hỏi.")
            a += f"\n3) Nguồn: {src}"
        else:
            a = f"1) Answer: The most relevant material is \"{top}\"."
            a += ("\n2) Details: Other related materials: "
                  + "; ".join(f"\"{t}\"" for t in others) + "."
                  if others else
                  "\n2) Details: This is the document that directly covers the topic in question.")
            a += f"\n3) Sources: {src}"
        return a
