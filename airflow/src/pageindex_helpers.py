"""Pure helper functions extracted from pageindex.py (behaviour-preserving split).

Chua cac ham thuan (text normalization, language/keyword derivation, intent &
scope detection, message builders, scoring, page-range utils). Khong tham chieu
PageIndexEngine/QueryBundle nen import mot chieu, khong circular.
"""
from __future__ import annotations

import json
import re
import socket
import struct
import time
import unicodedata
from typing import Any, Dict, List, Optional, Sequence, Tuple

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
        ("tuyen tinh", ["linear"]),
        ("giai tich", ["calculus"]),
        ("hoi quy tuyen tinh", ["linear", "regression"]),
        ("hoi quy", ["regression"]),
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
        ("phan phoi chuan", ["normal", "distribution"]),
        ("kiem dinh gia thuyet", ["hypothesis", "testing", "test"]),
        ("dinh nghia", ["definition"]),
        ("giai thich", ["explanation"]),
        ("khai niem", ["concept"]),
        ("sinh hoc", ["biology", "biological"]),
        ("sinh", ["biology"]),
        ("vat ly", ["physics", "physical"]),
        ("ly", ["physics"]),
        ("hoa hoc", ["chemistry", "chemical"]),
        ("hoa", ["chemistry"]),
        ("triet hoc", ["philosophy", "philosophical"]),
        ("triet", ["philosophy"]),
        ("tin hoc", ["computer", "programming"]),
        ("lap trinh", ["computer", "programming"]),
        ("giao duc", ["education", "educational"]),
    ]
    for phrase, mapped in phrase_map:
        if phrase in norm:
            keywords.extend(mapped)

    token_map = {
        "tuyen": ["linear"],
        "thong": ["statistics"],
        "ke": ["statistics"],
        "xac": ["probability"],
        "suat": ["probability"],
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
        ("calculus", ["giai", "tich"]),
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
    """3-tier intent detection for book recommendation:
    Tier 1: High-confidence exact phrase matches.
    Tier 2: Course-scope signals (user asks about resources for this course/subject in general).
    Tier 3: Ambiguous — defer to LLM (see _recommendation_intent_ambiguous).
    """
    q = _ascii_fold(_strip_moodle_context(question))
    moodle_ctx = _parse_moodle_context(question)

    # --- Tier 1: High-confidence exact phrases ---
    exact_markers = [
        # Vietnamese explicit recommendation
        "goi y sach",
        "goi y tai lieu",
        "goi y giao trinh",
        "tai lieu tham khao",
        "sach tham khao",
        "de xuat sach",
        "de xuat tai lieu",
        "gioi thieu sach",
        "gioi thieu tai lieu",
        # Explicit course-scoped
        "tai lieu cho mon nay",
        "sach cho mon nay",
        "tai lieu cua mon",
        "sach cua mon",
        "tai lieu hoc phan",
        "tai lieu mon hoc",
        "sach mon hoc",
        "sach giao khoa",
        # English
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
        "course material",
        "course book",
    ]
    if any(marker in q for marker in exact_markers):
        return True

    # --- Tier 2: Course-scope signals (no specific topic → user wants whole-course resources) ---
    # e.g. "tài liệu nào phù hợp cho môn học này", "sách nào thích hợp cho môn"
    course_scope_patterns = [
        # "phù hợp/thích hợp/dùng cho môn [này/đó]"
        r"(phu hop|thich hop|duoc dung|dung)\s+(cho|trong)\s+(mon|hoc phan|khoa hoc)",
        r"(tai lieu|sach|giao trinh|cuon).{0,20}(mon hoc|mon nay|khoa hoc nay|hoc phan)",
        r"(mon hoc|mon nay|khoa hoc nay|hoc phan).{0,30}(tai lieu|sach|giao trinh)",
        r"(co|dang dung|su dung|hoc).{0,20}(tai lieu|sach|giao trinh).{0,20}(nao|gi)\b",
    ]
    for pat in course_scope_patterns:
        if re.search(pat, q):
            return True

    # --- Tier 1b: verb + noun combination ---
    rec_verbs = ["goi y", "de xuat", "gioi thieu", "recommend", "suggest", "tim"]
    rec_nouns = [
        "tai lieu", "sach", "book", "books", "resource", "resources",
        "material", "materials", "textbook", "reference", "giao trinh",
    ]
    has_verb = any(v in q for v in rec_verbs)
    has_noun = any(n in q for n in rec_nouns)
    if has_verb and has_noun:
        return True

    # --- Affirmative follow-up response ---
    affirmative = {"co", "co nhe", "ok", "yes", "yes please", "duoc"}
    if q in affirmative and str(moodle_ctx.get("course_name") or "").strip():
        return True

    return False


def _recommendation_intent_ambiguous(question: str) -> bool:
    """Returns True when the question is asking about documents/books without a
    specific topic — suggesting course-level recommendation — but pattern matching
    alone isn't definitive. Use this to trigger LLM intent classification.

    Example ambiguous queries:
      - "tài liệu nào cho môn học này?"
      - "sách học nào tốt?"
      - "có tài liệu nào không?"
    """
    q = _ascii_fold(_strip_moodle_context(question))
    # Already high-confidence, no need for LLM
    if _detect_recommendation_intent(question):
        return False
    # Check if any doc/book noun is present without a specific topic keyword
    doc_nouns = ["tai lieu", "sach", "giao trinh", "textbook", "book", "resource", "material"]
    has_doc_noun = any(n in q for n in doc_nouns)
    if not has_doc_noun:
        return False
    # Presence of question words without specific topic → ambiguous
    question_words = ["nao", "gi", "nhu the nao", "co khong", "phu hop", "thich hop",
                      "which", "what", "any", "suitable"]
    has_question_word = any(w in q for w in question_words)
    # Topic-specific indicators (if present → not ambiguous, it's find_material)
    topic_indicators = ["ve", "giai thich", "noi ve", "chua", "bao gom", "co chua", "about",
                        "explain", "cover", "contain", "regarding", "on the topic"]
    has_topic = any(t in q for t in topic_indicators)
    return has_question_word and not has_topic


def _is_recommendation_query(question: str) -> bool:
    return _detect_recommendation_intent(question)


def _detect_find_material_intent(question: str) -> bool:
    """Câu hỏi "Tài liệu/Sách NÀO nói/giải thích về <chủ đề>?" — người dùng muốn
    biết TÊN tài liệu chứa chủ đề, KHÔNG phải giải thích nội dung. Khác với
    recommendation (gợi ý sách cho cả môn) ở chỗ có chủ đề cụ thể + từ để hỏi "nào"."""
    q = _ascii_fold(_strip_moodle_context(question))
    # If course-scope patterns matched (no specific topic), this is recommendation not find_material
    course_scope_patterns = [
        r"(tai lieu|sach|giao trinh).{0,20}(mon hoc|mon nay|khoa hoc nay|hoc phan)",
        r"(mon hoc|mon nay|khoa hoc nay|hoc phan).{0,30}(tai lieu|sach|giao trinh)",
        r"(phu hop|thich hop).{0,30}(mon|hoc phan|khoa hoc)",
    ]
    for pat in course_scope_patterns:
        if re.search(pat, q):
            return False  # This is recommendation, not find_material
    vi_markers = ["tai lieu nao", "sach nao", "giao trinh nao", "cuon nao", "tai lieu gi", "sach gi"]
    en_markers = ["which document", "which book", "which material", "which textbook",
                  "which resource", "what document", "what book", "what material"]
    return any(m in q for m in vi_markers + en_markers)

def _detect_query_intent(question: str) -> str:
    if _detect_find_material_intent(question):
        return "find_material"
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
        r"(?:giai thich|dinh nghia|khai niem)\s+(.+)$",
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
            explicit_name = str(match.group(1) or "").strip()
            if explicit_name and (not course_name or not re.search(r"\b(?:i{1,3}|[1-3])\b", _ascii_fold(course_name))):
                return explicit_name
    if course_name:
        return course_name
    return ""


def _extract_document_title_hint(question: str) -> str:
    moodle_context = _parse_moodle_context(question)
    doc_title = str(moodle_context.get("document_title") or "").strip()
    return doc_title


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
    
    known_concepts = {
        "dao ham cua mot ham so": "dao ham ham so",
        "dao ham cua ham so": "dao ham ham so",
        "derivative of a function": "derivative function",
        "gioi han cua ham so": "gioi han ham so",
        "limit of a function": "limit function",
        "database management system": "co so du lieu",
        "database": "co so du lieu",
        "dbms": "co so du lieu",
        "sql": "co so du lieu"
    }
    
    cleaned = known_concepts.get(cleaned, cleaned)
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


def _extract_find_material_target(question: str) -> str:
    core = _strip_moodle_context(question)
    q = _ascii_fold(core)
    
    # Common Vietnamese patterns for find_material
    patterns = [
        r"(?:hoc ve|tim hieu ve|noi ve|viet ve|giai thich ve|day ve|huong dan ve|cover|explain|about|on the topic of|on|regarding)\s+(.+?)(?:\s+(?:thi|cho|thi dung|nao|gi|de|phu hop|tot)\b|$)",
        r"(?:sach|tai lieu|giao trinh|cuon)\s+(?:nao|gi)\s+(?:hoc|ve|noi ve|giai thich ve)\s+(.+)$",
        r"(?:sach|tai lieu|giao trinh|cuon)\s+(?:nao|gi)\s+(?:phu hop cho|phu hop de hoc)\s+(.+)$",
        r"(?:sach|tai lieu|giao trinh|cuon)\s+(?:nao|gi)\s+(?:ve)\s+(.+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, q)
        if match:
            target = match.group(1).strip()
            # Clean up target (remove trailing/leading noise words)
            target = re.sub(r"\b(thi|dung|nao|gi|tot|phu hop|de|co|duoc|nhat)\b", "", target).strip()
            if target:
                return target
    # Fallback to the requested concept extraction if patterns didn't match
    return _extract_requested_concept(question)



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
            rf"\b{needle}\b.{{0,80}}\b(is|are|defined as|refers to|means|la|duoc dinh nghia|duoc dinh nghia la|la mot)\b",
            # "definition of derivative", "định nghĩa của đạo hàm"
            rf"\b(definition of|dinh nghia cua)\s+{needle}\b",
            # "define derivative as ..."
            rf"\bdefine\s+{needle}\s+(as|la)\b",
            # "derivative is called ..."
            rf"\b{needle}\b.{{0,60}}\b(is called|duoc goi la)\b",
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
        # Games / gaming / esports
        r"\b(game|trò chơi|video game|gaming|esport)\b",
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
        # Current leaders (English & Vietnamese)
        r"\b(current\s+(prime\s+minister|president|chancellor)\s+of\b)",
        r"\b(tổng thống|chủ tịch nước|tổng bí thư)\b",
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
        "phan phoi chuan": ["normal distribution", "normal distributions"],
        "normal distribution": ["phan phoi chuan", "normal distributions"],
        "kiem dinh gia thuyet": ["hypothesis testing", "hypothesis test"],
        "hypothesis testing": ["kiem dinh gia thuyet", "hypothesis test"],
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


__all__ = [
    '_strip_surrogate_chars',
    '_normalize_pdf_text',
    '_ascii_fold',
    '_tokenize',
    '_derive_en_keywords_from_vi',
    '_derive_vi_keywords_from_en',
    '_detect_query_language',
    '_detect_lang',
    '_parse_moodle_context',
    '_strip_moodle_context',
    '_detect_recommendation_intent',
    '_recommendation_intent_ambiguous',
    '_is_recommendation_query',
    '_detect_find_material_intent',
    '_detect_query_intent',
    '_is_definition_query',
    '_extract_definition_target',
    '_contains_unresolved_placeholder',
    '_is_implicit_concept_placeholder',
    '_extract_course_name_hint',
    '_extract_document_title_hint',
    '_extract_section_name_hint',
    '_build_course_scope_profile',
    '_evaluate_course_scope_text',
    '_extract_requested_concept',
    '_has_example_cue',
    '_has_definition_cue',
    '_has_targeted_definition_cue',
    '_is_english_dominant_text',
    '_estimate_transcript_noise',
    '_resolve_answer_language',
    '_message_no_relevant',
    '_message_unresolved_concept',
    '_message_insufficient_scope',
    '_is_obviously_out_of_scope',
    '_message_no_document',
    '_message_time_budget',
    '_overlap_score',
    '_estimate_formula_density',
    '_estimate_garbled_text_ratio',
    '_dedupe_keep_order',
    '_detect_default_gateway_ipv4',
    '_safe_json_loads',
    '_to_python',
    '_clamp_page_range',
    '_range_to_expr',
    '_parse_pages_expr',
    '_collapse_pages',
    '_phrase_overlap',
    '_recommendation_generic_terms',
    '_extract_subject_focus_terms',
    '_extract_subject_focus_phrases',
    '_expand_definition_target_tokens',
]
