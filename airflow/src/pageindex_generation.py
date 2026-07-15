"""Answer generation, citation/source building, metrics.

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


class _GenerationMixin:
    """Answer generation, citation/source building, metrics."""

    def _generate_answer(
        self,
        question: str,
        document: Dict[str, Any],
        contexts: List[Dict[str, Any]],
        confidence: str,
        answer_language: str = "vi",
        llm_timeout: Optional[int] = None,
    ) -> str:
        if not contexts:
            cn = str(_parse_moodle_context(question).get("course_name") or "").strip()
            return _message_no_relevant(answer_language, cn)

        parts = []
        per_context_limit = max(800, min(3200, self.llm_context_max_chars // max(1, len(contexts))))
        for index, ctx in enumerate(contexts, start=1):
            label = f"[Nguồn {index}]"
            if ctx.get("page_no"):
                label += f" (Page {ctx['page_no']})"
            section_label = ""
            if ctx.get("section_title"):
                section_label = f" | Section: {ctx['section_title']}"
            ctx_text = str(ctx.get("text") or "")
            if len(ctx_text) > per_context_limit:
                ctx_text = f"{ctx_text[:per_context_limit]}\n...[truncated]..."
            parts.append(f"{label}{section_label}\n{ctx_text}")
        context_text = "\n\n".join(parts)
        if len(context_text) > self.llm_context_max_chars:
            context_text = f"{context_text[: self.llm_context_max_chars]}\n...[truncated]..."

        if not self._local_llm_enabled():
            if self.disable_fallback:
                detail = self._local_llm_last_error or "Local LLM unavailable."
                return f"[LLM_DEBUG] {detail}"
            return self._fallback_answer(question, contexts, confidence, answer_language=answer_language)

        definition_target = _extract_requested_concept(question) if _is_definition_query(question) else ""
        query_intent = _detect_query_intent(question)
        # We let the LLM generate the answer for find_material to provide a detailed, reasoned recommendation.
        moodle_ctx = _parse_moodle_context(question)
        section_hint = str(moodle_ctx.get("section_name") or "").strip()
        course_name = str(moodle_ctx.get("course_name") or "").strip()
        
        context_metadata = f"Document title: {document.get('title')}"
        if course_name:
            context_metadata += f"\nMoodle course context: {course_name}"
        if section_hint:
            context_metadata += f"\nMoodle section: {section_hint}"
        if definition_target:
            context_metadata += f"\nDefinition target: {definition_target}"

        is_listing = query_intent == "listing"

        course_instruction_en = ""
        course_instruction_vi = ""
        if course_name:
            course_instruction_en = f"IMPORTANT COURSE CONTEXT: The user is asking within the context of the course '{course_name}'. You MUST adapt the general concepts from the context to fit the specific syntax, paradigm, and characteristics of this course. If the context is generic computer science, adjust it to match {course_name}."
            course_instruction_vi = f"QUAN TRỌNG: Người dùng đang hỏi trong ngữ cảnh môn học '{course_name}'. Bạn PHẢI đối chiếu và điều chỉnh các khái niệm chung chung từ context cho phù hợp tuyệt đối với đặc thù, cú pháp và quy tắc của môn '{course_name}'. Nếu tài liệu nói kiến thức đại cương, hãy áp dụng chuẩn xác cho {course_name}."

        base_rules_en = (
            "You are a PageIndex OER learning assistant. Answer ONLY academic topics from VALID_CONTEXT.\n"
            "SCOPE RULE: If the query is non-academic (cooking, sports, entertainment, health, shopping, travel, etc.), "
            "you MUST reply ONLY with: 'This question is outside the scope of the OER academic library. I can only help with academic and educational topics.'\n"
            "COPYRIGHT WARNING: If the query is about a math concept but VALID_CONTEXT only has license terms (like 'NoDerivatives'/'ND') or index page, "
            "reply exactly: 'The provided documents do not contain information to answer this question.'\n"
            "MATH FORMULA: Format all math in LaTeX: \\( ... \\) for inline, $$ ... $$ for block. Convert ascii formulas from context to LaTeX. Do not use plain ascii.\n"
            f"{course_instruction_en}\n"
            "Answer in English. Never invent facts. Do not use circular definitions.\n"
            "Provide detailed step-by-step explanations, breaking down concepts. Synthesis and paraphrase clearly for students.\n"
        )
        base_rules_vi = (
            "Bạn là trợ lý học tập PageIndex cho học liệu mở (OER). Chỉ trả lời câu hỏi học thuật từ VALID_CONTEXT.\n"
            "QUY TẮC PHẠM VI: Nếu câu hỏi phi học thuật (nấu ăn, thể thao, giải trí, sức khỏe, tin tức, mua sắm...), "
            "bạn PHẢI trả lời DUY NHẤT: 'Câu hỏi này nằm ngoài phạm vi thư viện học liệu mở OER. Mình chỉ hỗ trợ các câu hỏi về học thuật và giáo dục.'\n"
            "CẢNH BÁO BẢN QUYỀN: Nếu câu hỏi về Toán nhưng VALID_CONTEXT chỉ chứa từ khóa giấy phép (như 'NoDerivatives'/'ND') hoặc trang Mục lục (Index), "
            "bạn PHẢI trả lời chính xác: 'Tài liệu của khóa học này không có thông tin để trả lời câu hỏi này.'\n"
            "CÔNG THỨC TOÁN: Dùng LaTeX tiêu chuẩn: \\( ... \\) cho inline, $$ ... $$ cho block (Ví dụ: \\( f'(x) = \\lim_{h \\to 0} \\frac{f(x+h) - f(x)}{h} \\)). "
            "Chuyển công thức dạng ascii trong context sang LaTeX. Không dùng text ascii rời rạc.\n"
            "CẢNH BÁO PHÂN SỐ PDF: Văn bản PDF thường mất gạch ngang phân số đứng (ví dụ '6  x2' phải khôi phục thành '\\( \\frac{6}{x^2} \\)' rồi giải thích viết dưới dạng mũ âm '\\( 6x^{-2} \\)'). Tránh viết trùng lặp hàm số ban đầu và sau khi viết lại.\n"
            f"{course_instruction_vi}\n"
            "LƯU Ý NGÔN NGỮ: Bắt buộc trả lời 100% bằng TIẾNG VIỆT. TUYỆT ĐỐI KHÔNG dùng chữ Hán (Hanzi), tiếng Trung, hoặc tiếng Nhật. "
            "Dịch chính xác context tiếng Anh sang tiếng Việt tự nhiên, giữ nguyên thuật ngữ khó dịch nếu cần.\n"
            "TỔNG HỢP KIẾN THỨC: Giải thích chi tiết, giải ví dụ và công thức cụ thể từng bước từ context. Không bịa thêm thông tin ngoài context. "
            "Dùng ký hiệu chuẩn thông dụng thay vì các ký hiệu dẫn dắt trung gian của sách (như m_sec).\n"
        )


        if is_listing:
            intent_instruction_en = (
                "The question asks for a LIST. Extract ALL relevant items from the context.\n"
                "At '1) Answer:', you MUST provide a bullet list with ALL items found.\n"
            )
            intent_instruction_vi = (
                "Câu hỏi yêu cầu LIỆT KÊ. Trích xuất TẤT CẢ các mục liên quan từ context.\n"
                "Tại mục '1) Trả lời:' PHẢI chứa danh sách gạch đầu dòng với TẤT CẢ mục tìm được.\n"
            )
        elif query_intent == "explanation" and definition_target:
            intent_instruction_en = (
                "The question asks for a DEFINITION or EXPLANATION of a concept.\n"
                "At '1) Answer:': give a precise, clear general definition of the concept. You MAY phrase a standard definition for readability, but do NOT introduce specific facts, numbers, or formulas that are not in the context.\n"
                "At '2) Details:': include ONLY properties, examples, formulas, or applications that ACTUALLY APPEAR in the context. Do NOT invent or add examples/numbers/formulas (for instance, do NOT add '\\( f(x)=x^2 \\)', '\\( f'(x)=2x \\)', or '\\( f'(3)=6 \\)' unless they literally appear in the context).\n"
                "If the context lacks specific details, say so instead of inventing them.\n"
                "If the context is in English, translate accurately into the answer language without adding content.\n"
            )
            intent_instruction_vi = (
                "Câu hỏi yêu cầu ĐỊNH NGHĨA hoặc GIẢI THÍCH khái niệm.\n"
                "Tại mục '1) Trả lời:': Trình bày định nghĩa bao quát, chuẩn xác TỪ CONTEXT. NẾU context cung cấp quá chi tiết kỹ thuật hoặc KHÔNG CÓ định nghĩa cơ bản, bạn PHẢI TRẢ LỜI RÕ: 'Tài liệu của khóa học này không định nghĩa khái quát về khái niệm này, mà tập trung vào các chi tiết như: [tóm tắt chi tiết]'. TUYỆT ĐỐI KHÔNG dùng kiến thức ngoài để bịa ra định nghĩa.\n"
                "Tại mục '2) Chi tiết:': Bổ sung các ví dụ, tính chất, hoặc chi tiết cụ thể CÓ THẬT TỪ CONTEXT để minh họa. TUYỆT ĐỐI KHÔNG tự bịa ra ví dụ, số liệu, hoặc công thức mới. NẾU trong ngữ cảnh (VALID_CONTEXT) có sẵn ví dụ cụ thể (như bài toán tính đạo hàm của một hàm cụ thể), bạn NÊN trích xuất chính xác ví dụ đó để minh họa.\n"
                "Hãy chứng minh bạn là một hệ thống RAG nghiêm ngặt: Chỉ nói những gì tài liệu có.\n"
            )
        elif query_intent == "find_material":
            intent_instruction_en = (
                "The user is asking which book/material is suitable to learn about a specific topic.\n"
                "Based on the contexts, identify which book(s) contain the most relevant content, chapters, or index for the topic.\n"
                "At '1) Answer:': state the most relevant book title(s) from the context.\n"
                "At '2) Details:': explain which chapters, sections, or pages cover the topic, based strictly on the context.\n"
            )
            intent_instruction_vi = (
                "Người dùng đang hỏi cuốn sách/tài liệu nào phù hợp để học về một chủ đề cụ thể.\n"
                "Dựa trên ngữ cảnh (VALID_CONTEXT), hãy xác định cuốn sách nào chứa chương học hoặc nội dung giải thích trực tiếp nhất về chủ đề đó.\n"
                "Tại mục '1) Trả lời:': Nêu tên cuốn sách phù hợp nhất để học chủ đề này.\n"
                "Tại mục '2) Chi tiết:': Giải thích rõ chương hoặc phần nào trong cuốn sách đó nói về chủ đề này (ví dụ: Chương 3 của cuốn sách chứa định nghĩa đạo hàm...), dựa hoàn toàn trên ngữ cảnh.\n"
            )
        else:
            intent_instruction_en = (
                "Answer the question directly and clearly based STRICTLY on the context.\n"
                "If the context does not contain the answer, you MUST state at '1) Answer:': 'The provided documents do not contain information to answer this question.'\n"
                "At '2) Details:', add useful details extracted ONLY from context.\n"
            )
            intent_instruction_vi = (
                "Trả lời câu hỏi trực tiếp, tự nhiên dựa HOÀN TOÀN vào context.\n"
                "Nếu context không chứa thông tin để trả lời, bạn PHẢI nói ở mục '1) Trả lời:': 'Tài liệu của khóa học này không có thông tin để trả lời câu hỏi này.'\n"
                "Mục '2) Chi tiết:' bổ sung các thông tin liên quan từ context.\n"
                "TUYỆT ĐỐI KHÔNG dùng kiến thức ngoài để bịa ra câu trả lời hay ví dụ.\n"
            )

        format_block_en = (
            "MANDATORY FORMAT (exactly 3 sections):\n"
            "1) Answer: <direct answer to the question>\n"
            "2) Details: <supporting details, examples, or bullet list FROM THE CONTEXT ONLY — do NOT invent formulas or examples>\n"
            "3) Sources: [Source 1]\n\n"
            "Example:\n"
            "1) Answer: A derivative measures the instantaneous rate of change of a function.\n"
            "2) Details: The formal definition uses the limit of the difference quotient. Only include formulas or examples if they literally appear in the VALID_CONTEXT.\n"
            "3) Sources: [Source 1]\n"
        )
        format_block_vi = (
            "ĐỊNH DẠNG BẮT BUỘC (đúng 3 phần):\n"
            "1) Trả lời: <câu trả lời trực tiếp, ngắn gọn, KHÔNG lặp lại từ khóa trong câu hỏi để định nghĩa chính nó>\n"
            "2) Chi tiết: <thông tin bổ sung, công thức, ví dụ lấy TRỰC TIẾP TỪ CONTEXT — TUYỆT ĐỐI KHÔNG bịa ví dụ hay công thức mới>\n"
            "3) Nguồn: [Nguồn 1]\n\n"
            "Ví dụ câu hỏi: 'Thế năng là gì?'\n"
            "1) Trả lời: Thế năng là năng lượng mà vật có được do vị trí của nó trong trường lực.\n"
            "2) Chi tiết: Theo tài liệu, thế năng trọng trường tính theo công thức \\( W_p = mgh \\) (nếu công thức đó có trong context).\n"
            "3) Nguồn: [Nguồn 1]\n"
        )

        if answer_language == "en":
            prompt = (
                f"{base_rules_en}\n"
                f"{intent_instruction_en}\n"
                f"{format_block_en}\n"
                f"{context_metadata}\n"
                f"Question: {question}\n"
                f"VALID_CONTEXT:\n{context_text}\n\n"
                "Answer in English:"
            )
        else:
            prompt = (
                f"{base_rules_vi}\n"
                f"{intent_instruction_vi}\n"
                f"{format_block_vi}\n"
                f"{context_metadata}\n"
                f"Câu hỏi: {question}\n"
                f"VALID_CONTEXT:\n{context_text}\n\n"
                "TRẢ LỜI 100% BẰNG TIẾNG VIỆT (VIETNAMESE):"
            )
        try:
            answer_text = self._call_local_llm(
                prompt,
                json_mode=False,
                request_timeout=llm_timeout if llm_timeout is not None else self.llm_answer_timeout,
            )
            if answer_language == "vi":
                answer_text = answer_text.replace("tia tiếp tuyến", "độ dốc của tiếp tuyến")
                answer_text = answer_text.replace("Tia tiếp tuyến", "Độ dốc của tiếp tuyến")
                answer_text = answer_text.replace("thương hiệu hiệu", "thương sai phân")
                answer_text = answer_text.replace("giới hạn của thương hiệu", "giới hạn của thương sai phân")
            return answer_text
        except Exception as exc:
            logger.warning("Local answer generation failed: %s", exc)
            if isinstance(exc, requests.RequestException):
                self._local_llm_last_error = str(exc)
                self._local_llm_checked = False
                self._local_llm_unavailable = False
            if self.disable_fallback:
                return f"[LLM_DEBUG_ERROR] {str(exc)}"
            return self._fallback_answer(question, contexts, confidence, answer_language=answer_language)

    def _repair_answer_format(
        self,
        raw_answer: str,
        contexts: List[Dict[str, Any]],
        answer_language: str = "vi",
    ) -> str:
        """Wrap an LLM answer that lacks 1)/2)/3) structure into the required format.

        Called only when _validate_generated_answer returns missing_required_sections
        and the raw answer is non-empty. Splits the raw text into a main answer and
        supporting details rather than discarding the LLM output.
        """
        text = str(raw_answer or "").strip()
        if not text:
            return ""
        insufficient_markers = [
            "khong du bang chung",
            "no context with sufficient relevance",
            "not enough evidence",
            "insufficient evidence",
        ]
        if any(marker in _ascii_fold(text) for marker in insufficient_markers):
            return ""

        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+|\n+", text) if s.strip()]
        if not sentences:
            return ""

        answer_sentence = sentences[0]
        detail_sentences = sentences[1:]
        details = " ".join(detail_sentences[:4]).strip()
        if not details:
            details = answer_sentence

        source_count = max(1, len(contexts))

        if answer_language == "en":
            sources_str = ", ".join(f"[Source {i}]" for i in range(1, source_count + 1))
            return (
                f"1) Answer: {answer_sentence}\n"
                f"2) Details: {details}\n"
                f"3) Sources: {sources_str}"
            )

        sources_str = ", ".join(f"[Nguồn {i}]" for i in range(1, source_count + 1))
        return (
            f"1) Trả lời: {answer_sentence}\n"
            f"2) Chi tiết: {details}\n"
            f"3) Nguồn: {sources_str}"
        )

    def _build_sources(self, contexts: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build structured source list with MinIO PDF URLs from contexts."""
        sources: List[Dict[str, Any]] = []
        seen: set = set()
        for ctx in contexts:
            title = str(ctx.get("title") or "").strip()
            page_no = ctx.get("page_no")
            minio_url = str(ctx.get("minio_url") or "").strip() or None
            source_url = str(ctx.get("source_url") or "").strip() or None
            asset_uid = str(ctx.get("asset_uid") or "").strip()

            pdf_url = self._resolve_pdf_url(
                minio_url=minio_url,
                source_url=source_url,
                asset_path=str(ctx.get("asset_path") or "").strip() or None,
            )
            if pdf_url and page_no:
                pdf_url_with_page = f"{pdf_url}#page={page_no}"
            else:
                pdf_url_with_page = pdf_url

            dedupe_key = (asset_uid, page_no)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            snippet = ""
            raw_text = str(ctx.get("text") or "")
            if raw_text:
                clean = re.sub(r"\s+", " ", raw_text).strip()
                snippet = clean[:200] + ("..." if len(clean) > 200 else "")

            # --- Section title accuracy fix ---
            # The indexed section_title can be stale (inherited from the first chunk
            # of a section and not updated when the page crosses into a new section).
            # Detect the actual section heading from the first line of the page text
            # and prefer it over the metadata when they clearly differ.
            metadata_section = str(ctx.get("section_title") or "").strip()
            actual_section = metadata_section
            if raw_text:
                # Look for numbered heading pattern at the start of the text (e.g. "3.2 | The Derivative as a Function")
                first_lines = raw_text.strip()[:300]
                heading_match = re.search(
                    r"^(\d+\.\d+(?:\.\d+)?)\s*[\|\-]?\s*([A-Z][^\n]{5,80})",
                    first_lines,
                    re.MULTILINE,
                )
                if heading_match:
                    detected = f"{heading_match.group(1)}: {heading_match.group(2).strip()}"
                    # Only override if metadata section is different section number
                    meta_num_match = re.search(r"(\d+\.\d+)", metadata_section)
                    detected_num = heading_match.group(1)
                    if meta_num_match and meta_num_match.group(1) != detected_num:
                        actual_section = detected

            sources.append({
                "title": title or "Không rõ tài liệu",
                "url": pdf_url_with_page or "",
                "file": minio_url or "",
                "source_url": source_url,
                "asset_uid": asset_uid or None,
                "page": int(page_no) if page_no else None,
                "section": actual_section or None,
                "snippet": snippet,
            })
        return sources

    def _resolve_pdf_url(
        self,
        minio_url: Optional[str],
        source_url: Optional[str],
        asset_path: Optional[str] = None,
    ) -> Optional[str]:
        """Return a browser-openable PDF URL, preferring presigned MinIO links."""
        if not minio_url:
            return source_url
        object_name = str(asset_path or "").strip().lstrip("/")
        if not object_name:
            parsed = urlparse(str(minio_url).strip())
            raw_path = (parsed.path or "").lstrip("/")
            bucket_prefix = f"{self.bucket}/"
            if raw_path.startswith(bucket_prefix):
                object_name = raw_path[len(bucket_prefix) :]
        if not object_name:
            return minio_url
        try:
            signer_client = self._get_minio_signer_client()
            return signer_client.presigned_get_object(
                self.bucket,
                object_name,
                expires=timedelta(seconds=self.minio_presigned_expiry_seconds),
            )
        except Exception as exc:
            logger.warning("Unable to build presigned MinIO URL for %s: %s", object_name, exc)
            return minio_url or source_url

    def _build_ask_metrics(
        self,
        document_result: Dict[str, Any],
        selected_document: Optional[Dict[str, Any]],
        pages_loaded_total: int,
        pages_hit_total: int,
        contexts: Sequence[Dict[str, Any]],
        answer: str,
        found_relevant_evidence: bool,
    ) -> Dict[str, Any]:
        tier1 = document_result.get("tier1") or {}
        tier1_docs = tier1.get("documents") or []
        selected_asset_uid = str((selected_document or {}).get("asset_uid") or "").strip()
        selected_resource_uid = str((selected_document or {}).get("resource_uid") or "").strip()
        tier1_topk_assets = [str(item.get("asset_uid") or "").strip() for item in tier1_docs[: self.max_document_candidates]]
        tier1_topk_resources = [str(item.get("resource_uid") or "").strip() for item in tier1_docs[: self.max_document_candidates]]
        tier1_hit = bool(
            selected_asset_uid and selected_asset_uid in tier1_topk_assets
        ) or bool(selected_resource_uid and selected_resource_uid in tier1_topk_resources)

        evidence_hit_rate = float(pages_hit_total) / float(max(1, pages_loaded_total))
        grounded_answer_rate = 0.0
        answer_text = str(answer or "")
        no_answer_markers = [
            "Không tìm thấy thông tin phù hợp",
            "Không tìm thấy tài liệu phù hợp",
            "No relevant information found",
            "No suitable document found",
        ]
        if contexts and not any(marker in answer_text for marker in no_answer_markers):
            grounded_answer_rate = 1.0

        return {
            "tier1_recall_at_k": 1.0 if (tier1_hit or found_relevant_evidence) else 0.0,
            "tier1_recall_at_k_type": "proxy",
            "tier1_k": int(self.max_document_candidates),
            "evidence_hit_rate": round(evidence_hit_rate, 4),
            "grounded_answer_rate": grounded_answer_rate,
            "pages_loaded_total": int(pages_loaded_total),
            "pages_hit_total": int(pages_hit_total),
        }
