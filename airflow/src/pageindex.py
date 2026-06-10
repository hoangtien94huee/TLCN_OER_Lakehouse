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


def _strip_surrogate_chars(value: Optional[str]) -> str:
    text = str(value or "")
    return "".join(ch for ch in text if not (0xD800 <= ord(ch) <= 0xDFFF))


def _normalize_pdf_text(text: str) -> str:
    text = _strip_surrogate_chars(text)
    text = re.sub(r"-\s*\n\s*", "", text)
    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _ascii_fold(value: Optional[str]) -> str:
    if not value:
        return ""
    normalized = str(value).replace("đ", "d").replace("Đ", "D")
    text = unicodedata.normalize("NFKD", normalized)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", text.lower()).strip()


def _tokenize(value: Optional[str]) -> List[str]:
    text = _ascii_fold(value)
    tokens = re.findall(r"[a-z0-9]+", text)
    stopwords = {
        "la",
        "gi",
        "giai",
        "thich",
        "khai",
        "niem",
        "cho",
        "ve",
        "trong",
        "cua",
        "the",
        "what",
        "is",
        "explain",
        "about",
        "and",
        "for",
        "with",
        "that",
        "this",
        "from",
        "into",
        "mot",
        "cac",
        "nhung",
        "tai",
        "lieu",
        "hoc",
        "tap",
        "bai",
        "nay",
        "chinh",
        "noi",
        "dung",
        "chu",
        "de",
        "toan",
        "math",
        "mathematics",
    }
    return [tok for tok in tokens if len(tok) > 1 and tok not in stopwords]


def _derive_en_keywords_from_vi(text: str, base_tokens: Sequence[str]) -> List[str]:
    norm = _ascii_fold(text)
    keywords: List[str] = []

    phrase_map = [
        ("dai so tuyen tinh", ["linear", "algebra"]),
        ("hoi quy tuyen tinh", ["linear", "regression"]),
        ("hoc may", ["machine", "learning"]),
        ("thong ke", ["statistics"]),
        ("xac suat", ["probability"]),
        ("dao ham", ["derivative"]),
        ("tich phan", ["integral"]),
        ("co so du lieu", ["database", "dbms", "sql", "relational", "transaction", "normalization"]),
        ("cau truc du lieu", ["data", "structure"]),
        ("giai thuat", ["algorithm"]),
        ("mang no ron", ["neural", "network"]),
        ("phan tich du lieu", ["data", "analysis"]),
        ("vi du", ["example"]),
    ]
    for phrase, mapped in phrase_map:
        if phrase in norm:
            keywords.extend(mapped)

    token_map = {
        "hoi": ["regression"],
        "quy": ["regression"],
        "tuyen": ["linear"],
        "tinh": ["linear"],
        "thong": ["statistics"],
        "ke": ["statistics"],
        "xac": ["probability"],
        "suat": ["probability"],
        "giai": ["explain"],
        "thich": ["explain"],
        "khai": ["concept"],
        "niem": ["concept"],
    }
    for tok in base_tokens:
        keywords.extend(token_map.get(tok, []))

    seen = set()
    return [k for k in keywords if k and not (k in seen or seen.add(k))]


def _derive_vi_keywords_from_en(text: str, base_tokens: Sequence[str]) -> List[str]:
    norm = _ascii_fold(text)
    keywords: List[str] = []

    phrase_map = [
        ("linear algebra", ["dai", "so", "tuyen", "tinh"]),
        ("linear regression", ["hoi", "quy", "tuyen", "tinh"]),
        ("machine learning", ["hoc", "may"]),
        ("statistics", ["thong", "ke"]),
        ("probability", ["xac", "suat"]),
        ("data structure", ["cau", "truc", "du", "lieu"]),
        ("database", ["co", "so", "du", "lieu"]),
        ("algorithm", ["giai", "thuat"]),
        ("neural network", ["mang", "no", "ron"]),
        ("definition", ["dinh", "nghia"]),
        ("concept", ["khai", "niem"]),
    ]
    for phrase, mapped in phrase_map:
        if phrase in norm:
            keywords.extend(mapped)

    token_map = {
        "linear": ["tuyen", "tinh"],
        "algebra": ["dai", "so"],
        "regression": ["hoi", "quy"],
        "statistics": ["thong", "ke"],
        "probability": ["xac", "suat"],
        "data": ["du", "lieu"],
        "database": ["co", "so", "du", "lieu"],
        "algorithm": ["giai", "thuat"],
        "definition": ["dinh", "nghia"],
        "concept": ["khai", "niem"],
        "example": ["vi", "du"],
    }
    for tok in base_tokens:
        keywords.extend(token_map.get(tok, []))

    seen = set()
    return [k for k in keywords if k and not (k in seen or seen.add(k))]


def _detect_query_language(text: str) -> str:
    q = _ascii_fold(text)
    tokens = re.findall(r"[a-z0-9]+", q)
    vi_markers = {
        "la",
        "gi",
        "dinh",
        "nghia",
        "khai",
        "niem",
        "giai",
        "thich",
        "vi",
        "du",
        "tai",
        "sao",
        "nhu",
        "nao",
        "trang",
        "chuong",
        "muc",
    }
    en_markers = {
        "what",
        "is",
        "define",
        "definition",
        "explain",
        "example",
        "how",
        "why",
        "section",
        "chapter",
        "page",
        "about",
    }
    vi_score = sum(1 for tok in tokens if tok in vi_markers)
    en_score = sum(1 for tok in tokens if tok in en_markers)
    vi_phrase_markers = [
        "dai so",
        "tuyen tinh",
        "hoi quy",
        "xac suat",
        "thong ke",
        "giai thuat",
    ]
    if any(marker in q for marker in vi_phrase_markers):
        vi_score += 2

    vi_chars = set("àáâãèéêìíòóôõùúýăđơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ")
    has_vi_diacritic = any(ch in vi_chars for ch in (text or "").lower())
    if has_vi_diacritic:
        vi_score += 2

    if vi_score > 0 and en_score > 0:
        return "mixed"
    if vi_score > 0:
        return "vi"
    return "en"


def _detect_lang(text: str) -> str:
    detected = _detect_query_language(text)
    if detected == "mixed":
        return "vi"
    return detected


def _parse_moodle_context(question: str) -> Dict[str, str]:
    context: Dict[str, str] = {}
    for line in str(question or "").splitlines():
        match = re.match(r"^\s*-\s*([a-z_]+)\s*=\s*(.+?)\s*$", line)
        if not match:
            continue
        key = str(match.group(1) or "").strip().lower()
        value = str(match.group(2) or "").strip()
        if key and value:
            context[key] = value
    return context


def _strip_moodle_context(question: str) -> str:
    text = str(question or "")
    marker = "\n\n[Moodle context]"
    if marker in text:
        return text.split(marker, 1)[0].strip()
    return text.strip()


def _detect_recommendation_intent(question: str) -> bool:
    q = _ascii_fold(_strip_moodle_context(question))
    moodle_ctx = _parse_moodle_context(question)
    exact_markers = [
        "goi y sach",
        "goi y tai lieu",
        "tai lieu tham khao",
        "sach tham khao",
        "de xuat sach",
        "de xuat tai lieu",
        "tai lieu cho mon nay",
        "sach cho mon nay",
        "recommend book",
        "recommend books",
        "recommend resource",
        "recommend resources",
        "suggest book",
        "suggest books",
        "suggest resource",
        "suggest resources",
        "reference material",
        "reading list",
    ]
    if any(marker in q for marker in exact_markers):
        return True

    rec_verbs = ["goi y", "de xuat", "gioi thieu", "recommend", "suggest", "tim"]
    rec_nouns = [
        "tai lieu", "sach", "book", "books", "resource", "resources",
        "material", "materials", "textbook", "reference",
    ]
    has_verb = any(v in q for v in rec_verbs)
    has_noun = any(n in q for n in rec_nouns)
    if has_verb and has_noun:
        return True

    affirmative = {"co", "co nhe", "ok", "yes", "yes please", "duoc"}
    if q in affirmative and str(moodle_ctx.get("course_name") or "").strip():
        return True
    return False


def _is_recommendation_query(question: str) -> bool:
    return _detect_recommendation_intent(question)


def _detect_query_intent(question: str) -> str:
    if _is_recommendation_query(question):
        return "recommendation"
    q = _strip_moodle_context(question).lower()
    q_folded = _ascii_fold(q)
    listing_markers = [
        "tính chất", "tinh chat",
        "đặc điểm", "dac diem",
        "gồm những gì", "gom nhung gi",
        "bao gồm", "bao gom",
        "có mấy loại", "co may loai",
        "các loại", "cac loai",
        "liệt kê", "liet ke",
        "những thành phần", "nhung thanh phan",
        "properties", "characteristics", "components",
        "types of", "list the", "what are the",
    ]
    if any(marker in q or marker in q_folded for marker in listing_markers):
        return "listing"
    if any(k in q for k in ["là gì", "la gi", "giải thích", "giai thich", "định nghĩa", "dinh nghia", "khái niệm", "khai niem", "what is", "explain"]):
        return "explanation"
    if any(k in q for k in ["ví dụ", "example", "minh họa"]):
        return "example"
    if any(k in q for k in ["code", "mã", "python", "java", "sql", "implement"]):
        return "code_example"
    if any(k in q for k in ["công thức", "formula", "equation"]):
        return "formula"
    return "explanation"


def _is_definition_query(question: str) -> bool:
    q = _ascii_fold(_strip_moodle_context(question))
    markers = [
        "what is",
        "define",
        "definition",
        "la gi",
        "dinh nghia",
        "khai niem",
        "giai thich",
    ]
    return any(marker in q for marker in markers)


def _extract_definition_target(question: str) -> str:
    q = _ascii_fold(_strip_moodle_context(question))
    patterns = [
        r"(?:what is|define|definition of|explain)\s+(.+)$",
        r"(.+?)\s+(?:la gi|dinh nghia|khai niem)\??$",
        r"(?:giai thich)\s+(.+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, q)
        if not match:
            continue
        target = (match.group(1) or "").strip(" ?!.,:;\"'()[]{}")
        target = re.sub(r"\b(ve|about|for|of|the|mot|moi|khai|niem)\b", " ", target)
        target = re.sub(r"\s+", " ", target).strip()
        if target:
            return target[:120]
    return ""


def _contains_unresolved_placeholder(question: str) -> bool:
    text = _strip_moodle_context(question)
    folded = _ascii_fold(text)
    if not folded:
        return False
    if re.search(r"\[[^\]]{1,80}\]", text) or re.search(r"<[^>]{1,80}>", text):
        placeholder_markers = [
            "khai niem cu the",
            "concept",
            "placeholder",
            "insert",
            "dien vao",
            "replace",
        ]
        return any(marker in folded for marker in placeholder_markers)
    direct_markers = [
        "khai niem cu the",
        "concept here",
        "replace concept",
        "dien khai niem",
    ]
    return any(marker in folded for marker in direct_markers)


def _is_implicit_concept_placeholder(text: str) -> bool:
    folded = _ascii_fold(text)
    if not folded:
        return False
    patterns = [
        r"\b(khai niem|concept)\s+(chinh|co ban|tong quan)?\s*(cua|ve|of|for)?\s*(bai|chu de|noi dung|topic|lesson|course)\s*(nay|do|this|current)?\b",
        r"\b(main|core|key)\s+concept\s+(of|for)\s+(this|current)\s+(lesson|topic|course)\b",
    ]
    if any(re.search(pattern, folded) for pattern in patterns):
        return True

    raw_tokens = re.findall(r"[a-z0-9]+", folded)
    generic = {
        "khai",
        "niem",
        "chinh",
        "bai",
        "nay",
        "chu",
        "de",
        "noi",
        "dung",
        "mon",
        "hoc",
        "cua",
        "ve",
        "do",
        "cac",
        "nhung",
        "chuong",
        "phan",
        "tom",
        "tat",
        "nhanh",
        "can",
        "nho",
        "this",
        "current",
        "lesson",
        "topic",
        "course",
        "main",
        "core",
        "key",
        "concept",
        "section",
        "chapter",
        "summary",
        "overview",
    }
    informative = [tok for tok in raw_tokens if tok not in {"giai", "thich", "la", "gi", "what", "is", "explain"}]
    return bool(informative) and all(tok in generic for tok in informative)


def _extract_course_name_hint(question: str) -> str:
    moodle_context = _parse_moodle_context(question)
    course_name = str(moodle_context.get("course_name") or "").strip()
    if course_name:
        return course_name

    core = _strip_moodle_context(question)
    patterns = [
        r"\b(calculus\s*(?:i{1,3}|[1-3]))\b",
        r"\b(giai\s*tich\s*(?:i{1,3}|[1-3]))\b",
        r"\b(toan\s*(?:i{1,3}|[1-3]))\b",
        r"\b(mathematics\s*(?:i{1,3}|[1-3]))\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, _ascii_fold(core))
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _extract_section_name_hint(question: str) -> str:
    moodle_context = _parse_moodle_context(question)
    return str(moodle_context.get("section_name") or "").strip()


def _build_course_scope_profile(course_name: str) -> Dict[str, Any]:
    folded = _ascii_fold(course_name)
    if not folded:
        return {}

    def _pack(
        name: str,
        allow: Sequence[str],
        deny: Sequence[str],
        hard_deny: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        return {
            "name": name,
            "allow_terms": _dedupe_keep_order([_ascii_fold(x) for x in allow if _ascii_fold(x)]),
            "deny_terms": _dedupe_keep_order([_ascii_fold(x) for x in deny if _ascii_fold(x)]),
            "hard_deny_terms": _dedupe_keep_order([_ascii_fold(x) for x in (hard_deny or []) if _ascii_fold(x)]),
        }

    if any(marker in folded for marker in ["calculus i", "calculus 1", "giai tich 1", "toan 1", "mathematics 1"]):
        return _pack(
            "calculus_i",
            [
                "calculus i",
                "calculus 1",
                "calculus volume 1",
                "volume 1",
                "limit",
                "continuity",
                "derivative",
                "differentiation",
                "single variable",
                "function",
                "tangent",
                "integral",
                "dao ham",
                "gioi han",
                "ham so",
                "giai tich",
            ],
            [
                "vector calculus",
                "vector field",
                "multivariable",
                "analysis ii",
                "analysis 2",
                "calculus ii",
                "calculus 2",
                "giai tich ii",
                "giai tich 2",
                "partial derivative",
                "gradient",
                "divergence",
                "curl",
                "line integral",
                "surface integral",
                "stokes theorem",
                "gauss theorem",
                "green theorem",
            ],
            [
                "calculus volume 3",
                "volume 3",
                "vector calculus",
                "multivariable calculus",
                "analysis ii",
                "analysis 2",
                "calculus ii",
                "calculus 2",
                "giai tich ii",
                "giai tich 2",
                "stokes theorem",
                "gauss theorem",
            ],
        )
    return {}


def _evaluate_course_scope_text(text: Optional[str], profile: Dict[str, Any]) -> Dict[str, Any]:
    haystack = _ascii_fold(text)
    if not haystack or not profile:
        return {
            "profile_name": str(profile.get("name") or "") if isinstance(profile, dict) else "",
            "allow_hits": 0.0,
            "deny_hits": 0.0,
            "mismatch": False,
            "alignment": 1.0,
        }
    allow_hits = _phrase_overlap(haystack, profile.get("allow_terms") or [])
    deny_hits = _phrase_overlap(haystack, profile.get("deny_terms") or [])
    hard_deny_hits = _phrase_overlap(haystack, profile.get("hard_deny_terms") or [])
    mismatch = bool(hard_deny_hits >= 1.0 or (deny_hits >= 1.0 and deny_hits >= (allow_hits + 0.5)))
    alignment = 1.0
    if mismatch:
        alignment = 0.0
    elif allow_hits <= 0.0 and profile.get("allow_terms"):
        alignment = 0.55
    return {
        "profile_name": str(profile.get("name") or ""),
        "allow_hits": float(allow_hits),
        "deny_hits": float(deny_hits),
        "hard_deny_hits": float(hard_deny_hits),
        "mismatch": mismatch,
        "alignment": float(alignment),
    }


def _extract_requested_concept(question: str) -> str:
    core = _strip_moodle_context(question)
    target = _extract_definition_target(core)
    if not target and _is_definition_query(core):
        match = re.search(r"^\s*(.+?)\s*(?:\?|$)", _ascii_fold(core))
        if match:
            target = str(match.group(1) or "").strip()
    cleaned = _ascii_fold(target)
    if not cleaned:
        return ""
    cleaned = re.sub(
        r"\b(trong|in|for)\s+(calculus|giai tich|toan|mathematics)\s*(?:i{1,3}|[1-3])\b",
        " ",
        cleaned,
    )
    cleaned = re.sub(
        r"\b(neu|nêu|hay|please|va|and)\s+(dinh nghia|definition|vi du|example).*$",
        " ",
        cleaned,
    )
    cleaned = re.sub(r"\b(la gi|what is)\b", " ", cleaned).strip()
    cleaned = re.sub(r"\[[^\]]*\]|\<[^>]*\>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ?!.,:;\"'()[]{}")
    if cleaned in {"khai niem cu the", "concept", "placeholder"}:
        return ""
    if _is_implicit_concept_placeholder(cleaned):
        return ""
    return cleaned[:120]


def _has_example_cue(text: Optional[str]) -> bool:
    content = _ascii_fold(text)
    if not content:
        return False
    markers = [
        "for example",
        "e.g",
        "example",
        "vi du",
        "chang han",
        "minh hoa",
    ]
    return any(marker in content for marker in markers)


def _has_definition_cue(text: Optional[str]) -> bool:
    content = _ascii_fold(text)
    if not content:
        return False
    patterns = [
        r"\bis (?:a|an|the)\b",
        r"\bdefined as\b",
        r"\brefers to\b",
        r"\bis the study of\b",
        r"\bla (?:mot|he)\b",
        r"\bduoc dinh nghia\b",
        r"\bkhai niem\b",
    ]
    return any(re.search(pattern, content) for pattern in patterns)


def _has_targeted_definition_cue(text: Optional[str], target_terms: Sequence[str]) -> bool:
    content = _ascii_fold(text)
    if not content:
        return False
    meaningful_terms = [term for term in target_terms if len(str(term or "").strip()) >= 3][:8]
    if not meaningful_terms:
        return _has_definition_cue(content)
    for term in meaningful_terms:
        needle = re.escape(_ascii_fold(term))
        patterns = [
            # forward: "derivative is ...", "derivative defined as ..."
            rf"\b{needle}\b.{{0,48}}\b(is|are|defined as|refers to|means|la|duoc dinh nghia|duoc dinh nghia la|la mot)\b",
            # "definition of derivative", "định nghĩa của đạo hàm"
            rf"\b(definition of|dinh nghia cua)\s+{needle}\b",
            # "define derivative as ..."
            rf"\bdefine\s+{needle}\s+(as|la)\b",
            # "derivative is called ..."
            rf"\b{needle}\b.{{0,32}}\b(is called|duoc goi la)\b",
            # reversed passive: "is a difference quotient", "called a derivative"
            rf"\b(is|are)\s+(?:a|an|the)\s+{needle}\b",
            rf"\bcalled\s+(?:a|an|the)?\s*{needle}\b",
            # section/chapter title contains the concept directly
            rf"\b(defining|definition)\b.{{0,24}}{needle}\b",
        ]
        if any(re.search(pattern, content) for pattern in patterns):
            return True
    return False


def _is_english_dominant_text(text: Optional[str]) -> bool:
    content = _ascii_fold(text)
    if not content:
        return False
    tokens = re.findall(r"[a-z]+", content)
    if len(tokens) < 6:
        return False
    en_markers = {
        "the",
        "and",
        "is",
        "are",
        "of",
        "to",
        "that",
        "with",
        "for",
        "however",
        "note",
        "used",
        "define",
        "integral",
    }
    vi_markers = {
        "la",
        "duoc",
        "dinh",
        "nghia",
        "khai",
        "niem",
        "dao",
        "ham",
        "tich",
        "phan",
        "gioi",
        "han",
        "vi",
        "du",
    }
    en_hits = sum(1 for tok in tokens if tok in en_markers)
    vi_hits = sum(1 for tok in tokens if tok in vi_markers)
    return en_hits >= 2 and en_hits >= (vi_hits + 2)


def _estimate_transcript_noise(text: Optional[str]) -> float:
    content = _ascii_fold(text)
    if not content:
        return 0.0
    token_count = max(1, len(re.findall(r"\w+", content)))
    noise_hits = 0
    noise_patterns = [
        r"\byou know\b",
        r"\bright\??\b",
        r"\blet us\b|\blet's\b",
        r"\bokay\b",
        r"\bwell\b",
        r"\bcan we\b",
        r"\bnow i can\b",
    ]
    for pattern in noise_patterns:
        noise_hits += len(re.findall(pattern, content))
    return float(noise_hits) / float(token_count)


def _resolve_answer_language(preferred: Optional[str], question: str) -> str:
    pref = str(preferred or "").strip().lower()
    if pref in {"vi", "en"}:
        return pref
    # Project policy: default final answer language is Vietnamese.
    # Retrieval can still use bilingual (VI + EN) query signals.
    return "vi"


def _message_no_relevant(answer_language: str, course_name: str = "") -> str:
    cn = course_name.strip()
    if answer_language == "en":
        if cn:
            return (
                f"No relevant information found for your question within \"{cn}\".\n"
                "You can try:\n"
                "- Asking about a more specific concept\n"
                "- Requesting resource recommendations with \"Suggest books for this course\""
            )
        return (
            "No relevant information found in the document repository.\n"
            "You can try:\n"
            "- Navigating to a specific course for more accurate results\n"
            "- Asking about a specific concept, e.g. \"What is a derivative?\""
        )
    if cn:
        return (
            f"Mình chưa tìm thấy thông tin phù hợp cho câu hỏi này trong phạm vi môn \"{cn}\".\n"
            "Bạn có thể thử:\n"
            "- Hỏi cụ thể hơn về khái niệm cần tìm\n"
            "- Yêu cầu gợi ý tài liệu bằng \"Gợi ý tài liệu cho môn này\""
        )
    return (
        "Mình chưa tìm thấy thông tin phù hợp trong kho tài liệu.\n"
        "Bạn có thể thử:\n"
        "- Vào một môn học cụ thể để mình tìm chính xác hơn\n"
        "- Hỏi cụ thể hơn, ví dụ: \"đạo hàm là gì\", \"cơ sở dữ liệu quan hệ là gì\""
    )


def _message_unresolved_concept(answer_language: str) -> str:
    if answer_language == "en":
        return (
            "Your question references a general concept without specifying which one.\n"
            "Please specify, for example: \"What is a derivative?\", \"Explain normalization\""
        )
    return (
        "Câu hỏi của bạn đề cập đến khái niệm chung mà chưa nêu rõ khái niệm nào.\n"
        "Vui lòng nêu cụ thể, ví dụ: \"Đạo hàm là gì?\", \"Giải thích chuẩn hóa CSDL\""
    )


def _message_insufficient_scope(answer_language: str, course_name: str = "") -> str:
    cn = course_name.strip()
    if answer_language == "en":
        scope = f" within \"{cn}\"" if cn else ""
        return (
            f"The retrieved context is not sufficiently relevant to your question{scope}.\n"
            "You can try:\n"
            "- Rephrasing with more specific terms\n"
            "- Checking if the concept belongs to a different section or course"
        )
    scope_vi = f" trong phạm vi môn \"{cn}\"" if cn else ""
    return (
        f"Ngữ cảnh tìm được chưa đủ sát với câu hỏi của bạn{scope_vi}.\n"
        "Bạn có thể thử:\n"
        "- Diễn đạt lại với từ khóa cụ thể hơn\n"
        "- Kiểm tra xem khái niệm có thuộc chương hoặc môn khác không"
    )


def _is_obviously_out_of_scope(question: str) -> bool:
    """
    Fast keyword check for questions that are clearly non-academic.
    Returns True → skip PageIndex entirely, saves 5-9s latency.
    Intentionally conservative: only blocks obvious lifestyle/entertainment/personal questions.
    """
    import re
    q = question.lower()
    # Non-academic topic signals
    _OOS_PATTERNS = [
        # Food / cooking / calories
        r"\b(recipe|nấu|công thức nấu|món ăn|calo trong)\b",
        r"(calories?\s+(are\s+)?in|how\s+many\s+calories|big\s+mac|mcdonald)",
        # Sports scores / entertainment news
        r"\b(score of|kết quả trận|vô địch .{0,20}năm|fifa|world cup|v-league|aff cup)\b",
        r"\b(best.*series|netflix|phim.*hay|gợi ý phim)\b",
        # Personal lifestyle / shopping
        r"\b(lose weight|tăng cân|giảm cân nhanh|bằng lái xe|visa du lịch|đặt vé)\b",
        r"\b(best.*laptop.*buy|laptop gaming.*giá|stock price of|giá vàng hôm nay)\b",
        r"\b(birthday poem|thiệp chúc mừng|lyrics to|lời bài hát)\b",
        # Current events / personal advice
        r"\b(current president|thủ tướng.*hiện tại|thủ tướng.*là ai)\b",
        r"\b(tiktok account|tạo tài khoản tiktok)\b",
        r"\b(symptoms of.*cold|triệu chứng.*cảm|điều trị tại nhà)\b",
        r"\b(invest.*crypto|bitcoin|cryptocurrency|ethereum.*năm nay)\b",
        r"\b(tourist attractions|địa điểm du lịch)\b",
        r"\b(dog.*sit|train.*dog|chăm sóc cây|mai vàng)\b",
        # Travel / personal documents
        r"\b(visa\s+requirements?|travel\s+visa|tourist\s+visa|du\s+lịch\s+\S+\s+visa)",
        r"(driver.s\s+licen|driving\s+licen|\bbằng\s+lái\s+xe\b)",
        # Creative writing / entertainment
        r"\b(write\s+(me\s+)?(a\s+)?(lyrics?|poem|song|story|joke)|lyrics?\s+for)",
        r"\b(book\s+(a\s+)?flight|flight\s+to\s+\w+\s+(from|booking)|đặt\s+vé\s+máy\s+bay)\b",
        # Commodity / real-time prices
        r"\b(price\s+of\s+(gold|silver|oil)\s+(today|now|hôm nay))\b",
        # Personal fitness / bodybuilding
        r"\b(build\s+muscle|muscle\s+(gain|building)|diet\s+(plan\s+)?to\s+(lose|build|gain))\b",
        # Current leaders (English)
        r"\b(current\s+(prime\s+minister|president|chancellor)\s+of\b)",
    ]
    for pattern in _OOS_PATTERNS:
        if re.search(pattern, q):
            return True
    return False


def _message_no_document(answer_language: str, course_name: str = "") -> str:
    cn = course_name.strip()
    if answer_language == "en":
        if cn:
            return (
                f"No suitable document found for \"{cn}\" in the repository.\n"
                "You can try:\n"
                "- Asking about a specific concept from this course\n"
                "- Requesting resource recommendations with \"Suggest books for this course\""
            )
        return (
            "No suitable document found in the repository.\n"
            "You can try:\n"
            "- Navigating to a specific course for better results\n"
            "- Asking about a specific topic, e.g. \"What is a relational database?\""
        )
    if cn:
        return (
            f"Mình chưa tìm thấy tài liệu phù hợp cho môn \"{cn}\" trong kho dữ liệu.\n"
            "Bạn có thể thử:\n"
            "- Hỏi về một khái niệm cụ thể trong môn này\n"
            "- Yêu cầu gợi ý tài liệu bằng \"Gợi ý tài liệu cho môn này\""
        )
    return (
        "Mình chưa tìm thấy tài liệu phù hợp trong kho dữ liệu.\n"
        "Bạn có thể thử:\n"
        "- Vào một môn học cụ thể để mình tìm chính xác hơn\n"
        "- Hỏi cụ thể hơn, ví dụ: \"đạo hàm là gì\", \"cơ sở dữ liệu quan hệ là gì\""
    )


def _message_time_budget(answer_language: str) -> str:
    if answer_language == "en":
        return (
            "The search took too long and could not complete in time.\n"
            "Please try again — shorter or more specific questions usually get faster results."
        )
    return (
        "Tìm kiếm mất quá nhiều thời gian và không kịp hoàn thành.\n"
        "Vui lòng thử lại — câu hỏi ngắn gọn và cụ thể thường cho kết quả nhanh hơn."
    )


def _overlap_score(text: Optional[str], terms: Sequence[str]) -> float:
    haystack = set(_tokenize(text))
    if not haystack or not terms:
        return 0.0
    hits = sum(1 for term in terms if term in haystack)
    return float(hits)


def _estimate_formula_density(text: Optional[str]) -> float:
    raw = str(text or "")
    if not raw.strip():
        return 1.0
    token_count = max(1, len(re.findall(r"\S+", raw)))
    symbol_hits = len(re.findall(r"[=+\-*/^∑∫√<>[\]{}()|]", raw))
    digit_hits = len(re.findall(r"\d", raw))
    return float(symbol_hits + (0.5 * digit_hits)) / float(token_count)


def _estimate_garbled_text_ratio(text: Optional[str]) -> float:
    raw = str(text or "")
    if not raw.strip():
        return 1.0
    token_count = max(1, len(re.findall(r"\S+", raw)))
    long_alpha_runs = len(re.findall(r"[A-Za-z]{14,}", raw))
    return float(long_alpha_runs) / float(token_count)


def _dedupe_keep_order(values: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _detect_default_gateway_ipv4() -> Optional[str]:
    route_path = "/proc/net/route"
    try:
        with open(route_path, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except Exception:
        return None

    for line in lines[1:]:
        parts = line.strip().split()
        if len(parts) < 3:
            continue
        destination = parts[1]
        gateway_hex = parts[2]
        if destination != "00000000":
            continue
        try:
            return socket.inet_ntoa(struct.pack("<L", int(gateway_hex, 16)))
        except Exception:
            continue
    return None


def _safe_json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _to_python(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {k: _to_python(v) for k, v in value.asDict(recursive=True).items()}
    if isinstance(value, dict):
        return {k: _to_python(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_python(v) for v in value]
    return value


def _clamp_page_range(start_page: int, end_page: int, total_pages: int) -> Tuple[int, int]:
    total = max(int(total_pages or 0), 1)
    start = max(1, min(int(start_page), total))
    end = max(start, min(int(end_page), total))
    return start, end


def _range_to_expr(start_page: int, end_page: int) -> str:
    return str(start_page) if start_page == end_page else f"{start_page}-{end_page}"


def _parse_pages_expr(expr: str) -> List[int]:
    pages: List[int] = []
    for part in [p.strip() for p in str(expr or "").split(",") if p.strip()]:
        if "-" in part:
            left, right = part.split("-", 1)
            if left.isdigit() and right.isdigit():
                start, end = int(left), int(right)
                if end >= start:
                    pages.extend(list(range(start, end + 1)))
        elif part.isdigit():
            pages.append(int(part))
    seen = set()
    return [p for p in pages if not (p in seen or seen.add(p))]


def _collapse_pages(pages: Sequence[int]) -> str:
    if not pages:
        return ""
    ordered = sorted(set(int(p) for p in pages))
    ranges: List[str] = []
    start = ordered[0]
    prev = ordered[0]
    for page in ordered[1:]:
        if page == prev + 1:
            prev = page
            continue
        ranges.append(_range_to_expr(start, prev))
        start = prev = page
    ranges.append(_range_to_expr(start, prev))
    return ",".join(ranges)


def _phrase_overlap(text: Optional[str], phrases: Sequence[str]) -> float:
    haystack = _ascii_fold(text)
    if not haystack or not phrases:
        return 0.0
    hits = 0
    for phrase in phrases:
        needle = _ascii_fold(phrase)
        if needle and needle in haystack:
            hits += 1
    return float(hits)


def _recommendation_generic_terms() -> List[str]:
    return [
        "introduction",
        "intro",
        "fundamentals",
        "fundamental",
        "basic",
        "basics",
        "overview",
        "project",
        "projects",
        "topics",
        "topic",
        "course",
        "courses",
        "mon",
        "hoc",
        "nhap",
        "co",
        "ban",
        "do",
        "an",
        "thuc",
        "nghiem",
        "ly",
        "thuyet",
        "systems",
        "system",
        "engineering",
        "science",
        "technology",
        "technical",
        "techniques",
        "method",
        "methods",
        "analysis",
        "organization",
        "management",
        "leadership",
        "business",
        "history",
        "multiple",
        "advanced",
    ]


def _extract_subject_focus_terms(subject_name: str) -> List[str]:
    generic = set(_recommendation_generic_terms())
    terms: List[str] = []
    for token in _tokenize(subject_name):
        if len(token) < 3:
            continue
        if token in generic:
            continue
        terms.append(token)
    return _dedupe_keep_order(terms)


def _extract_subject_focus_phrases(subject_name: str, focus_terms: Sequence[str]) -> List[str]:
    phrases: List[str] = []
    folded_subject = _ascii_fold(subject_name)
    if folded_subject:
        phrases.append(folded_subject)

    ordered_terms = [str(term).strip() for term in focus_terms if str(term).strip()]
    for ngram_size in (3, 2):
        if len(ordered_terms) < ngram_size:
            continue
        for idx in range(0, len(ordered_terms) - ngram_size + 1):
            phrase = " ".join(ordered_terms[idx : idx + ngram_size]).strip()
            if len(phrase) >= 5:
                phrases.append(phrase)
    return _dedupe_keep_order(phrases)


def _expand_definition_target_tokens(target: str) -> List[str]:
    folded_target = _ascii_fold(target)
    expanded_terms: List[str] = list(_tokenize(target))
    concept_map: Dict[str, List[str]] = {
        "co so du lieu": ["database", "databases", "dbms", "database system", "relational database"],
        "he quan tri co so du lieu": ["dbms", "database management system"],
        "ngon ngu truy van": ["sql", "query language"],
        "chuan hoa": ["normalization"],
        "giao dich": ["transaction"],
        "database": ["co so du lieu", "he quan tri co so du lieu"],
        "dbms": ["co so du lieu", "he quan tri co so du lieu"],
        "sql": ["truy van", "ngon ngu truy van"],
        "dao ham": [
            "derivative",
            "derivatives",
            "differentiate",
            "differentiation",
            "differentiable",
            "differentiability",
            "difference quotient",
        ],
        "derivative": [
            "dao ham",
            "differentiation",
            "differentiate",
            "differentiable",
            "differentiability",
            "derivatives",
            "difference quotient",
        ],
        "tich phan": ["integral", "integrals", "integration", "antiderivative"],
        "integral": ["tich phan", "integration", "antiderivative", "integrals"],
        "gioi han": ["limit", "limits", "approaches", "limiting value"],
        "limit": ["gioi han", "limits"],
        "ham so": ["function", "functions"],
        "function": ["ham so", "functions"],
    }
    for concept, mapped in concept_map.items():
        if concept in folded_target:
            expanded_terms.extend(_tokenize(" ".join(mapped)))
    singular_plural_pairs = [
        ("derivative", "derivatives"),
        ("integral", "integrals"),
        ("function", "functions"),
        ("limit", "limits"),
    ]
    for singular, plural in singular_plural_pairs:
        if singular in expanded_terms and plural not in expanded_terms:
            expanded_terms.append(plural)
        if plural in expanded_terms and singular not in expanded_terms:
            expanded_terms.append(singular)
    return _dedupe_keep_order(expanded_terms)


@dataclass
class QueryBundle:
    query_vi_original: str
    query_en_semantic: str
    query_vi_semantic: str
    keywords_en: List[str]
    keywords_vi: List[str]
    intent: str
    language: str
    query_mode: str
    course_name: str = ""
    section_name: str = ""
    concept_target: str = ""
    has_unresolved_placeholder: bool = False


class PageIndexError(RuntimeError):
    pass


class PageIndexEngine:
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
        self.local_llm_timeout = int(os.getenv("LOCAL_LLM_TIMEOUT", "120"))
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

    def _create_spark_session(self) -> SparkSession:
        if not SPARK_AVAILABLE:
            raise PageIndexError("PySpark chưa sẵn sàng cho PageIndex.")

        java_home = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
        os.environ.setdefault("JAVA_HOME", java_home)
        os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.pop("JAVA_TOOL_OPTIONS", None)

        endpoint = self.minio_endpoint
        if not endpoint.startswith(("http://", "https://")):
            endpoint = f"http://{endpoint}"

        builder = SparkSession.builder.appName("OER-PageIndex-API").master(self.spark_master)
        spark_jars = os.getenv("SPARK_JARS")
        use_local_jars = False
        if spark_jars:
            jar_paths = [p.strip() for p in spark_jars.split(",") if p.strip()]
            use_local_jars = bool(jar_paths) and all(Path(p).exists() for p in jar_paths)
            if use_local_jars:
                builder = (
                    builder
                    .config("spark.jars", spark_jars)
                    .config("spark.driver.extraClassPath", spark_jars)
                    .config("spark.executor.extraClassPath", spark_jars)
                )
        if not use_local_jars:
            builder = (
                builder
                .config(
                    "spark.jars.packages",
                    ",".join(
                        [
                            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
                            "org.apache.hadoop:hadoop-aws:3.3.4",
                            "com.amazonaws:aws-java-sdk-bundle:1.12.565",
                        ]
                    ),
                )
                .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2"))
            )

        return (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.catalog_name}.warehouse", f"s3a://{self.bucket}/silver/")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.driver.host", self.spark_driver_host)
            .config("spark.driver.bindAddress", self.spark_driver_bind_address)
            .config("spark.driver.memory", os.getenv("CHATBOT_SPARK_DRIVER_MEMORY", "2g"))
            .config("spark.executor.memory", os.getenv("CHATBOT_SPARK_EXECUTOR_MEMORY", "2g"))
            .config("spark.driver.maxResultSize", os.getenv("CHATBOT_SPARK_DRIVER_MAXRESULTSIZE", "1g"))
            .config("spark.sql.shuffle.partitions", os.getenv("CHATBOT_SPARK_SHUFFLE_PARTITIONS", "8"))
            .getOrCreate()
        )

    def _get_spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = self._create_spark_session()
        return self._spark

    def _get_minio_client(self) -> Minio:
        if not MINIO_AVAILABLE:
            raise PageIndexError("MinIO client chưa sẵn sàng cho PageIndex.")
        if self._minio_client is None:
            endpoint = self.minio_endpoint
            secure = self.minio_secure
            if endpoint.startswith("http://"):
                endpoint = endpoint[7:]
            elif endpoint.startswith("https://"):
                endpoint = endpoint[8:]
                secure = True
            self._minio_client = Minio(
                endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=secure,
            )
        return self._minio_client

    def _get_minio_signer_client(self) -> Minio:
        """Create a MinIO client for presigning against public host."""
        if not MINIO_AVAILABLE:
            raise PageIndexError("MinIO client chưa sẵn sàng cho PageIndex.")
        if self._minio_signer_client is not None:
            return self._minio_signer_client
        public_base = str(self.minio_public_base_url or "").strip()
        if not public_base:
            return self._get_minio_client()
        parsed = urlparse(public_base)
        endpoint = parsed.netloc or parsed.path
        if not endpoint:
            return self._get_minio_client()
        secure = (parsed.scheme or "").lower() == "https"
        self._minio_signer_client = Minio(
            endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=secure,
            region="us-east-1",
        )
        return self._minio_signer_client

    def _table_exists(self, table_name: str) -> bool:
        try:
            self._get_spark().table(table_name)
            return True
        except Exception:
            return False

    def _load_reference_cache(self) -> Dict[str, Any]:
        if self._reference_cache is not None:
            return self._reference_cache

        spark = self._get_spark()
        subjects: List[Dict[str, Any]] = []
        links: List[Dict[str, Any]] = []
        if self._table_exists(self.reference_subjects_table):
            subjects = [_to_python(row) for row in spark.table(self.reference_subjects_table).collect()]
        if self._table_exists(self.reference_program_subject_links_table):
            links = [_to_python(row) for row in spark.table(self.reference_program_subject_links_table).collect()]

        programs_by_subject: Dict[int, List[int]] = {}
        for link in links:
            try:
                subject_id = int(link.get("subject_id"))
                program_id = int(link.get("program_id"))
            except Exception:
                continue
            programs_by_subject.setdefault(subject_id, [])
            if program_id not in programs_by_subject[subject_id]:
                programs_by_subject[subject_id].append(program_id)

        self._reference_cache = {
            "subjects": subjects,
            "programs_by_subject": programs_by_subject,
        }
        return self._reference_cache

    def _cache_get(self, cache: "OrderedDict[str, Any]", key: str) -> Any:
        if not self.cache_enabled:
            return None
        cache_key = str(key or "")
        if cache_key not in cache:
            return None
        value = cache.pop(cache_key)
        cache[cache_key] = value
        return value

    def _cache_set(self, cache: "OrderedDict[str, Any]", key: str, value: Any) -> None:
        if not self.cache_enabled:
            return
        cache_key = str(key or "")
        if cache_key in cache:
            cache.pop(cache_key)
        cache[cache_key] = value
        while len(cache) > self.cache_max_items:
            cache.popitem(last=False)

    def _local_llm_enabled(self) -> bool:
        return bool(self.local_llm_base_url and self.local_llm_model) and not self._local_llm_unavailable

    def _openai_chat_completions_url(self) -> str:
        if self.local_llm_api_url:
            return self.local_llm_api_url.rstrip("/")
        base = self.local_llm_base_url.rstrip("/")
        if base.endswith("/chat/completions"):
            return base
        if base.endswith("/v1"):
            return f"{base}/chat/completions"
        return f"{base}/v1/chat/completions"

    def _openai_models_url(self, base_url: str) -> str:
        base = base_url.rstrip("/")
        if base.endswith("/models"):
            return base
        if base.endswith("/v1"):
            return f"{base}/models"
        return f"{base}/v1/models"

    def _gemini_generate_content_url(self, base_url: str) -> str:
        if self.local_llm_api_url:
            return self.local_llm_api_url.rstrip("/")
        base = base_url.rstrip("/")
        return f"{base}/models/{self.local_llm_model}:generateContent"

    def _probe_local_llm_base_url(self, base_url: str) -> Tuple[bool, str]:
        base = str(base_url or "").strip().rstrip("/")
        if not base:
            return False, "empty base url"
        timeout = (self.local_llm_connect_timeout, self.local_llm_probe_timeout)
        try:
            if self.local_llm_health_url:
                health_url = self.local_llm_health_url
                if "{base_url}" in health_url:
                    endpoint = health_url.replace("{base_url}", base)
                elif health_url.startswith("http://") or health_url.startswith("https://"):
                    endpoint = health_url
                elif health_url.startswith("/"):
                    endpoint = f"{base}{health_url}"
                else:
                    endpoint = f"{base}/{health_url}"
                headers = {}
                if self.local_llm_api_key:
                    headers["Authorization"] = f"Bearer {self.local_llm_api_key}"
                response = requests.get(endpoint, headers=headers, timeout=timeout)
            elif self.local_llm_backend == "gemini":
                endpoint = f"{base}/models"
                params: Dict[str, str] = {}
                if self.local_llm_api_key:
                    params["key"] = self.local_llm_api_key
                response = requests.get(endpoint, params=params, timeout=timeout)
            elif self.local_llm_backend in {"vllm", "openai", "openai_compat", "api", "api_openai", "groq"}:
                endpoint = self._openai_models_url(base)
                headers = {}
                if self.local_llm_api_key:
                    headers["Authorization"] = f"Bearer {self.local_llm_api_key}"
                response = requests.get(endpoint, headers=headers, timeout=timeout)
            else:
                endpoint = f"{base}/api/tags"
                response = requests.get(endpoint, timeout=timeout)
            if response.ok:
                return True, ""
            return False, f"HTTP {response.status_code}"
        except requests.RequestException as exc:
            return False, str(exc)

    def _ensure_local_llm_endpoint(self) -> None:
        if self._local_llm_checked:
            return
        self._local_llm_checked = True
        candidates = _dedupe_keep_order([self.local_llm_base_url] + self.local_llm_fallback_base_urls)
        last_error = ""
        for candidate in candidates:
            ok, error = self._probe_local_llm_base_url(candidate)
            if ok:
                if candidate != self.local_llm_base_url:
                    logger.warning("Switch LOCAL_LLM_BASE_URL to reachable endpoint: %s", candidate)
                self.local_llm_base_url = candidate
                self._local_llm_unavailable = False
                self._local_llm_last_error = ""
                return
            last_error = f"{candidate} -> {error}"

        self._local_llm_last_error = last_error or "No reachable local LLM endpoint"
        if self.local_llm_probe_required:
            self._local_llm_unavailable = True
        else:
            # Non-strict probe: allow direct call even when health/models probe is unavailable.
            self._local_llm_unavailable = False
            logger.warning("Local LLM probe failed, will try direct API call. detail=%s", self._local_llm_last_error)

    def _call_local_llm(
        self,
        prompt: str,
        json_mode: bool = False,
        request_timeout: Optional[int] = None,
    ) -> str:
        self._ensure_local_llm_endpoint()
        if not self._local_llm_enabled():
            raise PageIndexError("Local LLM chưa được cấu hình cho PageIndex.")

        read_timeout = self.local_llm_timeout
        if request_timeout is not None:
            read_timeout = max(1, min(int(request_timeout), self.local_llm_timeout))
        timeout = (self.local_llm_connect_timeout, read_timeout)
        if self.local_llm_backend in {"vllm", "openai", "openai_compat", "api", "api_openai", "groq"}:
            payload: Dict[str, Any] = {
                "model": self.local_llm_model,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}],
            }
            if json_mode:
                payload["response_format"] = {"type": "json_object"}
            headers = {"Content-Type": "application/json"}
            if self.local_llm_api_key:
                headers["Authorization"] = f"Bearer {self.local_llm_api_key}"
            response = requests.post(
                self._openai_chat_completions_url(),
                headers=headers,
                json=payload,
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"].strip()

        if self.local_llm_backend == "ollama":
            payload = {
                "model": self.local_llm_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0},
            }
            if json_mode:
                payload["format"] = "json"
            response = requests.post(
                f"{self.local_llm_base_url}/api/generate",
                json=payload,
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
            return str(data.get("response") or "").strip()

        if self.local_llm_backend == "gemini":
            headers = {"Content-Type": "application/json"}
            params: Dict[str, str] = {}
            if self.local_llm_api_key:
                params["key"] = self.local_llm_api_key
            payload: Dict[str, Any] = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0},
            }
            if json_mode:
                payload["generationConfig"]["response_mime_type"] = "application/json"
            response = requests.post(
                self._gemini_generate_content_url(self.local_llm_base_url),
                headers=headers,
                params=params,
                json=payload,
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return ""
            content = candidates[0].get("content") or {}
            parts = content.get("parts") or []
            if not parts:
                return ""
            return str(parts[0].get("text") or "").strip()

        raise PageIndexError(
            f"LOCAL_LLM_BACKEND='{self.local_llm_backend}' không hợp lệ. "
            "Giá trị hợp lệ: ollama | vllm | api | groq | gemini."
        )

    def _call_local_llm_json(
        self,
        prompt: str,
        default: Dict[str, Any],
        request_timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        try:
            raw = self._call_local_llm(prompt, json_mode=True, request_timeout=request_timeout)
            data = _safe_json_loads(raw, default)
            if isinstance(data, dict):
                return data
            return default
        except Exception as exc:
            logger.warning("Local LLM JSON call failed: %s", exc)
            if isinstance(exc, requests.RequestException):
                self._local_llm_last_error = str(exc)
                # Allow subsequent calls to re-probe fallback endpoints.
                self._local_llm_checked = False
                self._local_llm_unavailable = False
            return default

    def _build_query_bundle(self, question: str, llm_timeout: Optional[int] = None) -> QueryBundle:
        question_core = _strip_moodle_context(question) or str(question or "")
        intent = _detect_query_intent(question_core)
        query_mode = _detect_query_language(question_core)
        language = _detect_lang(question_core)
        course_name = _extract_course_name_hint(question)
        section_name = _extract_section_name_hint(question)
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
            data = self._call_local_llm_json(
                prompt,
                {
                    "query_en_semantic": query_en_semantic,
                    "query_vi_semantic": query_vi_semantic,
                    "keywords_en": keywords_en,
                    "keywords_vi": keywords_vi,
                },
                request_timeout=llm_timeout if llm_timeout is not None else self.llm_json_timeout,
            )
            query_en_semantic = str(data.get("query_en_semantic") or question_core).strip()
            query_vi_semantic = str(data.get("query_vi_semantic") or question_core).strip()
            kw_en = [str(x).strip() for x in data.get("keywords_en") or [] if str(x).strip()]
            kw_vi = [str(x).strip() for x in data.get("keywords_vi") or [] if str(x).strip()]
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

    def _build_recommendation_profile(self, question: str, preferred_subject_name: str) -> Dict[str, Any]:
        user_text = _strip_moodle_context(question)
        combined = " ".join([user_text, preferred_subject_name]).strip()
        folded = _ascii_fold(combined)
        generic_terms = _recommendation_generic_terms()

        if preferred_subject_name.strip():
            subject_focus_terms = _extract_subject_focus_terms(preferred_subject_name)
            subject_focus_phrases = _extract_subject_focus_phrases(preferred_subject_name, subject_focus_terms)
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
            source_url = str(item.get("source_url") or tier1_doc.get("source_url") or "").strip()
            default_reason = "Relevant to the current course." if answer_language == "en" else "Phù hợp với nội dung đang tìm."
            reason = str(item.get("recommendation_reason") or default_reason).strip()
            if answer_language == "en":
                line = f"{idx}. {title}"
                if source_url:
                    line += f"\n- Link: {source_url}"
                line += f"\n- Why: {reason}"
            else:
                line = f"{idx}. {title}"
                if source_url:
                    line += f"\n- Link: {source_url}"
                line += f"\n- {reason}"
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

    def recommend_books(
        self,
        question: str,
        top_k: int = 5,
        source_system: Optional[str] = None,
        language: Optional[str] = None,
    ) -> Dict[str, Any]:
        answer_language = _resolve_answer_language(language, question)
        moodle_context = _parse_moodle_context(question)
        preferred_subject_name = str(moodle_context.get("course_name") or "").strip()
        profile = self._build_recommendation_profile(question, preferred_subject_name)
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
        moodle_ctx = _parse_moodle_context(question)
        section_hint = str(moodle_ctx.get("section_name") or "").strip()
        context_metadata = f"Document title: {document.get('title')}"
        if section_hint:
            context_metadata += f"\nMoodle section: {section_hint}"
        if definition_target:
            context_metadata += f"\nDefinition target: {definition_target}"

        is_listing = query_intent == "listing"

        base_rules_en = (
            "You are a PageIndex learning assistant for open educational resources (OER).\n"
            "You ONLY answer questions about academic subjects: mathematics, science, engineering, "
            "computer science, economics, history, literature, and other university-level disciplines.\n"
            "SCOPE RULE: If the question is about cooking, sports scores, entertainment, personal health "
            "advice, news, weather, shopping, travel tips, or ANY non-academic topic, you MUST respond "
            "ONLY with: 'This question is outside the scope of the OER academic library. "
            "I can only help with academic and educational topics.'\n"
            "You must answer strictly from the provided VALID_CONTEXT extracted from document pages.\n"
            "Answer in English.\n"
            "If context is in another language, translate accurately without adding external facts.\n"
            "NEVER invent facts not found in the context.\n"
            "NEVER use circular definitions (e.g. 'an integral is... integrating').\n"
            "Be concise. Do NOT repeat yourself. Each idea should appear ONCE.\n"
        )
        base_rules_vi = (
            "Bạn là trợ lý học tập PageIndex cho học liệu mở (OER).\n"
            "Bạn CHỈ trả lời các câu hỏi về học thuật: toán học, khoa học, kỹ thuật, công nghệ thông tin, "
            "kinh tế, lịch sử, văn học và các môn học đại học khác.\n"
            "QUY TẮC PHẠM VI: Nếu câu hỏi về nấu ăn, kết quả thể thao, giải trí, sức khỏe cá nhân, "
            "tin tức, thời tiết, mua sắm, du lịch hoặc BẤT KỲ chủ đề phi học thuật nào, "
            "bạn PHẢI trả lời DUY NHẤT: 'Câu hỏi này nằm ngoài phạm vi thư viện học liệu mở OER. "
            "Mình chỉ hỗ trợ các câu hỏi về học thuật và giáo dục.'\n"
            "Chỉ được trả lời dựa trên VALID_CONTEXT đã lấy trực tiếp từ các trang tài liệu.\n"
            "Trả lời bằng tiếng Việt.\n"
            "Nếu context gốc là tiếng Anh thì DỊCH NGHĨA chính xác sang tiếng Việt, diễn đạt tự nhiên như giáo trình Việt.\n"
            "TUYỆT ĐỐI KHÔNG bịa thêm nội dung không có trong context.\n"
            "KHÔNG dùng câu đồng nghĩa lặp lại (ví dụ: 'tích phân là... tích phân').\n"
            "Ngắn gọn, súc tích, mỗi ý chỉ nêu MỘT lần.\n"
        )

        if is_listing:
            intent_instruction_en = (
                "The question asks for a LIST. Extract ALL relevant items from the context.\n"
                "Section 1 MUST contain a bullet list with ALL items found.\n"
            )
            intent_instruction_vi = (
                "Câu hỏi yêu cầu LIỆT KÊ. Trích xuất TẤT CẢ các mục liên quan từ context.\n"
                "Phần 1 PHẢI chứa danh sách gạch đầu dòng với TẤT CẢ mục tìm được.\n"
            )
        elif query_intent == "explanation" and definition_target:
            intent_instruction_en = (
                "The question asks for a DEFINITION or EXPLANATION of a specific concept.\n"
                "Section 1 MUST give a precise, non-circular definition from the context.\n"
                "A non-circular definition explains the concept using DIFFERENT words, not the concept itself.\n"
                "Section 2 should include: key properties, examples, formulas, or applications from the context.\n"
                "If the context is in English, translate the definition accurately into the answer language.\n"
            )
            intent_instruction_vi = (
                "Câu hỏi yêu cầu ĐỊNH NGHĨA hoặc GIẢI THÍCH một khái niệm cụ thể.\n"
                "Phần 1 PHẢI đưa ra định nghĩa chính xác, KHÔNG lặp vòng (không dùng chính từ đó để giải thích).\n"
                "Ví dụ SAI: 'Tích phân là phép tích phân các hàm số'. Ví dụ ĐÚNG: 'Tích phân là phép tính tìm diện tích dưới đường cong'.\n"
                "Phần 2 nên bổ sung: tính chất quan trọng, ví dụ, công thức, hoặc ứng dụng từ context.\n"
                "Nếu context tiếng Anh, dịch chính xác sang tiếng Việt với thuật ngữ chuẩn.\n"
            )
        else:
            intent_instruction_en = (
                "Answer the question directly and clearly.\n"
                "If the question asks for a definition, give the definition first.\n"
                "Section 2 should add useful details: examples, formulas, or elaboration from context.\n"
            )
            intent_instruction_vi = (
                "Trả lời câu hỏi trực tiếp và rõ ràng.\n"
                "Nếu câu hỏi hỏi định nghĩa, đưa định nghĩa trước.\n"
                "Phần 2 nên bổ sung chi tiết hữu ích: ví dụ, công thức, hoặc giải thích thêm từ context.\n"
            )

        format_block_en = (
            "MANDATORY FORMAT (exactly 3 sections):\n"
            "1) Answer: <direct answer to the question>\n"
            "2) Details: <supporting details, examples, or bullet list from context>\n"
            "3) Sources: [Source 1]\n\n"
            "Example:\n"
            "1) Answer: The derivative of f at a is the limit of the difference quotient as h approaches 0.\n"
            "2) Details: It measures instantaneous rate of change. For f(x)=x², f'(x)=2x, so at x=3 the rate is 6.\n"
            "3) Sources: [Source 1]\n"
        )
        format_block_vi = (
            "ĐỊNH DẠNG BẮT BUỘC (đúng 3 phần):\n"
            "1) Trả lời: <câu trả lời trực tiếp, ngắn gọn, KHÔNG lặp lại từ khóa trong câu hỏi để định nghĩa chính nó>\n"
            "2) Chi tiết: <thông tin bổ sung, ví dụ, công thức, hoặc danh sách gạch đầu dòng từ context>\n"
            "3) Nguồn: [Nguồn 1]\n\n"
            "Ví dụ câu hỏi: 'Đạo hàm là gì?'\n"
            "1) Trả lời: Đạo hàm của hàm f tại điểm a là giới hạn của tỷ số giữa độ biến thiên của hàm số và độ biến thiên của biến số khi độ biến thiên biến số tiến đến 0.\n"
            "2) Chi tiết: Đạo hàm đo tốc độ thay đổi tức thời. Ví dụ: với f(x) = x², thì f'(x) = 2x; tại x = 3, tốc độ thay đổi bằng 6. Công thức: f'(a) = lim[h→0] (f(a+h) - f(a))/h.\n"
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
                "Trả lời bằng tiếng Việt:"
            )
        try:
            return self._call_local_llm(
                prompt,
                json_mode=False,
                request_timeout=llm_timeout if llm_timeout is not None else self.llm_answer_timeout,
            )
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

            sources.append({
                "title": title or "Không rõ tài liệu",
                "url": pdf_url_with_page or "",
                "file": minio_url or "",
                "asset_uid": asset_uid or None,
                "page": int(page_no) if page_no else None,
                "section": str(ctx.get("section_title") or "").strip() or None,
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

    def ask(
        self,
        question: str,
        top_k: int = 5,
        source_system: Optional[str] = None,
        language: Optional[str] = None,
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

        prebundle = self._build_query_bundle(question)
        detected_intent = prebundle.intent
        if detected_intent == "recommendation":
            return self.recommend_books(
                question=question,
                top_k=top_k,
                source_system=source_system,
                language=language,
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

        # Fast OOS pre-filter: skip PageIndex tree traversal for obviously non-academic questions.
        # Saves ~5-9s latency by returning immediately before get_document().
        if _is_obviously_out_of_scope(question):
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
                "query_bundle": {
                    "intent": "off_topic",
                    "language": answer_language,
                },
                "metrics": {
                    "tier1_recall_at_k": 0.0,
                    "tier1_recall_at_k_type": "proxy",
                    "tier1_k": 0,
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
            return {
                "question": question,
                "answer": _message_no_document(answer_language, prebundle.course_name),
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
            concept_target=str(bundle_data.get("concept_target") or prebundle.concept_target or ""),
            has_unresolved_placeholder=bool(
                bundle_data.get("has_unresolved_placeholder")
                if "has_unresolved_placeholder" in bundle_data
                else prebundle.has_unresolved_placeholder
            ),
        )

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
                pages_expr = _range_to_expr(current_range[0], current_range[1])
                page_result = self.get_page_content(
                    str(document.get("asset_uid") or ""),
                    pages_expr,
                    reason="Doc mot vung trang hep de tim bang chung truc tiep.",
                )
                trace.append(
                    {
                        "tool": "get_page_content",
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
                    next_range = self._expand_range(current_range, structure, document, round_index)
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
                    next_range = self._expand_range(current_range, structure, document, round_index)
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
                    next_range = self._expand_range(current_range, structure, document, round_index)
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
                    next_range = self._expand_range(current_range, structure, document, round_index)
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
