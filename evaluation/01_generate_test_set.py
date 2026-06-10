"""
Script 1 — Generate test set (100 questions).

Approach:
  1. Sample diverse documents from Elasticsearch oer_resources_tier1
  2. For each candidate doc: pull page content via /api/debug/get_page_content
  3. Use Groq to generate Q&A pairs (definition / comparison / multi_step)
  4. Generate find_material questions from doc titles/descriptions
  5. Add 20 hardcoded out-of-scope questions
  6. Stratify to exactly TARGET_COUNTS per type → save test_set.json

Bilingual support:
  --lang en   (default) generate English questions from English PDFs
  --lang vi             generate Vietnamese questions from English PDFs
  --lang mixed          50% English + 50% Vietnamese
  --import-vi           seed test set with chatbot_qa_testset_vi.md (10 questions)

Usage:
  python3 01_generate_test_set.py
  python3 01_generate_test_set.py --lang vi --import-vi
  python3 01_generate_test_set.py --force --lang mixed
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import time
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    API_BASE, ES_HOST, ES_INDEX,
    GROQ_API_KEY, GROQ_BASE_URL, GROQ_MODEL,
    GEMINI_API_KEY, GEMINI_BASE_URL, GEMINI_MODEL,
    TARGET_COUNTS, ES_SAMPLE_PER_SOURCE,
    DEBUG_TIMEOUT_SEC, GROQ_TIMEOUT_SEC, GEMINI_TIMEOUT_SEC,
    MAX_RETRIES, GROQ_RETRY_DELAY, GEMINI_RETRY_DELAY, INTER_CALL_DELAY,
    TEST_SET_PATH, LOGS_DIR,
)

_CHECKPOINT_PATH = TEST_SET_PATH.parent / "_test_set_checkpoint.json"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "01_generate_test_set.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

random.seed(77)

# ---------------------------------------------------------------------------
# Hardcoded out-of-scope questions — bilingual pool (pick by --lang flag)
# ---------------------------------------------------------------------------
_OUT_OF_SCOPE_EN: list[dict] = [
    {"question": "What is the best recipe for making pasta carbonara?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "Who won the FIFA World Cup in 2022?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "How do I fix a leaking kitchen faucet?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "What is the latest iPhone model released by Apple?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "Can you recommend a good Netflix series to watch this weekend?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "How do I train my dog to sit and stay?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "What are the visa requirements for travelling to Japan as a tourist?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "What is the current stock price of Tesla?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "How do I lose weight quickly in 2 weeks?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "What are the lyrics to Bohemian Rhapsody by Queen?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "Who is the current president of the United States?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "How do I create a TikTok account and start posting videos?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "What are the best tourist attractions in Paris?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "Can you write a birthday poem for my friend named Anna?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "What are the symptoms of a common cold and how do I treat it?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "How do I invest in cryptocurrency safely?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "What is the best gaming laptop to buy under $1000?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "How many calories are in a Big Mac from McDonald's?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "What is the score of last night's basketball game?",
     "ground_truth": "N/A — out of scope for OER academic library."},
    {"question": "How do I apply for a driver's licence in California?",
     "ground_truth": "N/A — out of scope for OER academic library."},
]

_OUT_OF_SCOPE_VI: list[dict] = [
    {"question": "Công thức nấu phở bò chuẩn vị Hà Nội là gì?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Đội tuyển bóng đá Việt Nam vô địch AFF Cup năm nào gần nhất?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Làm thế nào để tăng cân nhanh trong 1 tháng?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "iPhone 16 Pro Max có những tính năng gì mới?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Bạn có thể gợi ý phim Việt Nam hay xem cuối tuần không?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Cách chăm sóc cây mai vàng trổ bông đúng dịp Tết?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Thủ tục xin visa du lịch Nhật Bản mất bao lâu?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Giá vàng hôm nay là bao nhiêu?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Cách đặt vé tàu Tết trên ứng dụng VNRailway?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Lời bài hát Trống Cơm của dân ca quan họ Bắc Ninh?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Thủ tướng Việt Nam hiện tại là ai?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Cách tạo tài khoản TikTok và đăng video đầu tiên?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Những địa điểm du lịch nổi tiếng ở Đà Lạt là gì?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Bạn có thể viết thiệp chúc mừng sinh nhật cho tôi không?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Triệu chứng của cảm cúm thông thường và cách điều trị tại nhà?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Nên đầu tư Bitcoin hay Ethereum trong năm nay?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Laptop gaming tốt nhất trong tầm giá 20 triệu đồng?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Một bát bún bò Huế có bao nhiêu calo?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Kết quả trận đấu V-League tối qua là bao nhiêu?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
    {"question": "Thủ tục làm bằng lái xe ô tô hạng B2 cần những gì?",
     "ground_truth": "N/A — ngoài phạm vi thư viện học liệu mở OER."},
]


def _get_oos_pool(lang: str) -> list[dict]:
    """Return out-of-scope questions for the given language."""
    if lang == "vi":
        return _OUT_OF_SCOPE_VI
    if lang == "mixed":
        # interleave: 10 EN + 10 VI
        return [q for pair in zip(_OUT_OF_SCOPE_EN[:10], _OUT_OF_SCOPE_VI[:10]) for q in pair]
    return _OUT_OF_SCOPE_EN  # default: en


# ---------------------------------------------------------------------------
# Gemini helper (primary LLM)
# ---------------------------------------------------------------------------
def call_gemini(messages: list[dict], max_tokens: int = 900, temperature: float = 0.3) -> str:
    """Call Google Gemini generateContent API with retry logic.

    `messages` follows OpenAI role/content format; we map it to Gemini's
    `contents` format (user/model alternation).
    """
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set.")

    # Convert OpenAI-style messages → Gemini contents
    contents = []
    for m in messages:
        role = "user" if m["role"] in ("user", "system") else "model"
        contents.append({"role": role, "parts": [{"text": m["content"]}]})

    payload = {
        "contents": contents,
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": temperature,
        },
    }
    url = f"{GEMINI_BASE_URL}/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(url, json=payload, timeout=GEMINI_TIMEOUT_SEC)
            resp.raise_for_status()
            data = resp.json()
            # Response: candidates[0].content.parts[0].text
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status == 429:
                wait = GEMINI_RETRY_DELAY * (2 ** attempt)
                log.warning("Gemini rate-limited. Waiting %ds ...", wait)
                time.sleep(wait)
            else:
                log.warning("Gemini HTTP error (attempt %d): %s", attempt + 1, exc)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(GEMINI_RETRY_DELAY)
        except Exception as exc:
            log.warning("Gemini error (attempt %d): %s", attempt + 1, exc)
            if attempt < MAX_RETRIES - 1:
                time.sleep(GEMINI_RETRY_DELAY)

    raise RuntimeError("Gemini API failed after all retries.")


# ---------------------------------------------------------------------------
# Groq helper (fallback)
# ---------------------------------------------------------------------------
def call_groq(messages: list[dict], max_tokens: int = 900, temperature: float = 0.3) -> str:
    """Call Groq chat completion with retry logic (fallback when Gemini unavailable)."""
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not set. Check your .env file.")

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
                log.warning("Groq rate-limited. Waiting %ds ...", wait)
                time.sleep(wait)
            else:
                log.warning("Groq HTTP error (attempt %d): %s", attempt + 1, exc)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(GROQ_RETRY_DELAY)
        except Exception as exc:
            log.warning("Groq error (attempt %d): %s", attempt + 1, exc)
            if attempt < MAX_RETRIES - 1:
                time.sleep(GROQ_RETRY_DELAY)

    raise RuntimeError("Groq API failed after all retries.")


def call_llm(messages: list[dict], max_tokens: int = 900, temperature: float = 0.3) -> str:
    """Call Gemini (primary); fall back to Groq if Gemini key is absent."""
    if GEMINI_API_KEY:
        return call_gemini(messages, max_tokens=max_tokens, temperature=temperature)
    return call_groq(messages, max_tokens=max_tokens, temperature=temperature)


def _parse_json_from_llm(text: str) -> Any:
    """Extract and parse JSON from LLM output that may contain extra prose."""
    text = text.strip()
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Find first '[' or '{' and last ']' or '}'
    for start_char, end_char in [("[", "]"), ("{", "}")]:
        start = text.find(start_char)
        end = text.rfind(end_char)
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass
    raise ValueError(f"Cannot parse JSON from: {text[:200]}")


# ---------------------------------------------------------------------------
# Elasticsearch sampling
# ---------------------------------------------------------------------------
def sample_from_es(source_system: str, n: int) -> list[dict]:
    """Return up to n documents from a given source_system that have a valid asset_uid."""
    query = {
        "size": min(n * 3, 500),
        "query": {
            "bool": {
                "must": [{"term": {"source_system": source_system}}],
                # Only docs where asset_uid field exists and is not null
                "filter": [{"exists": {"field": "asset_uid"}}],
            }
        },
        "_source": ["asset_uid", "title", "description",
                    "source_system", "subject_names_en", "source_url"],
    }
    try:
        resp = requests.get(
            f"{ES_HOST}/{ES_INDEX}/_search",
            json=query,
            timeout=15,
        )
        resp.raise_for_status()
        hits = resp.json()["hits"]["hits"]
    except Exception as exc:
        log.error("ES query failed for %s: %s", source_system, exc)
        return []

    # Double-check: filter out any remaining None values at Python level
    docs = [h["_source"] for h in hits
            if h["_source"].get("asset_uid") and h["_source"]["asset_uid"] != "None"]
    random.shuffle(docs)
    # Deduplicate by title prefix to avoid very similar docs
    seen_titles: set[str] = set()
    unique: list[dict] = []
    for doc in docs:
        title_key = (doc.get("title") or "")[:30].lower()
        if title_key not in seen_titles:
            seen_titles.add(title_key)
            unique.append(doc)
    return unique[:n]


# ---------------------------------------------------------------------------
# Debug API helpers
# ---------------------------------------------------------------------------
def get_page_content(asset_uid: str, pages: str) -> list[dict]:
    """Return list of {page_no, text, chapter_title, section_title} via debug API."""
    try:
        resp = requests.post(
            f"{API_BASE}/debug/get_page_content",
            json={"asset_uid": asset_uid, "pages": pages,
                  "reason": "evaluation test-set generation"},
            timeout=DEBUG_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        data = resp.json()
        # API returns {"tool":..., "found": bool, "content": [...]}
        if isinstance(data, dict):
            content = data.get("content") or data.get("pages") or []
            if not data.get("found", True):   # found=False → PDF not indexed
                return []
            return content if isinstance(content, list) else []
        # Fallback: API returned a list directly
        if isinstance(data, list):
            return data
        return []
    except Exception as exc:
        log.debug("get_page_content failed for %s pages=%s: %s", asset_uid, pages, exc)
        return []


def get_doc_total_pages(asset_uid: str) -> int | None:
    """Return total page count from document structure endpoint."""
    try:
        resp = requests.post(
            f"{API_BASE}/debug/get_document_structure",
            json={"asset_uid": asset_uid,
                  "reason": "evaluation: check total pages"},
            timeout=DEBUG_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("total_pages")
    except Exception:
        return None


def _is_real_prose(text: str) -> bool:
    """Return True only if text looks like real paragraph content, not ToC or headers."""
    import re
    if not text or len(text) < 200:
        return False
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return False
    # Table of contents: many dot-leaders like "Chapter 1 ........... 12"
    toc_line_count = sum(1 for l in lines if len(re.findall(r'\.{4,}', l)) > 0)
    if toc_line_count > len(lines) * 0.25:
        return False
    # Header-only pages: average line is very short
    avg_line_len = sum(len(l) for l in lines) / len(lines)
    if avg_line_len < 35:
        return False
    # Must have at least 2 long sentences (real prose)
    sentences = re.split(r'[.!?]+\s+', text)
    long_sentences = [s for s in sentences if len(s.strip()) > 40]
    if len(long_sentences) < 2:
        return False
    return True


def fetch_usable_content(doc: dict, min_chars: int = 400) -> str:
    """
    Pull meaningful prose from a document, skipping ToC and header pages.
    Strategy: try pages 2-6, 7-12, 13-18, 19-25, 26-35.
    Returns first batch of pages that contains real prose content.
    """
    asset_uid = doc.get("asset_uid")
    if not asset_uid or asset_uid == "None":
        return ""
    # Extended ranges to find content pages even when early pages are ToC/preface
    for page_range in ["2-6", "7-12", "13-18", "19-25", "26-35"]:
        pages = get_page_content(asset_uid, page_range)
        text_parts: list[str] = []
        for p in pages:
            if isinstance(p, dict):
                text = (p.get("text") or "").strip()
            elif isinstance(p, str):
                text = p.strip()
            else:
                continue
            # Only include pages that look like real content
            if text and _is_real_prose(text):
                text_parts.append(text)
        combined = "\n\n".join(text_parts).strip()
        if len(combined) >= min_chars:
            return combined[:4000]
        time.sleep(0.3)
    return ""


# ---------------------------------------------------------------------------
# Q&A generation with Groq — bilingual prompt templates
# ---------------------------------------------------------------------------
_CONTENT_QA_PROMPT_EN = """\
You are an expert at creating evaluation questions for an educational chatbot \
that answers questions about academic textbooks and open educational resources.

Given content from a PDF titled "{title}" (subject area: {subject}):

--- CONTENT ---
{content}
--- END ---

Generate exactly {n} question-answer pairs in JSON.
Rules:
- Each answer must be grounded ONLY in the provided content above.
- Use these types:
    "definition"  → "What is X?", "Define Y.", "What does Z mean?"
    "comparison"  → "What is the difference between A and B?"
    "multi_step"  → requires combining info from 2+ parts of the content
- Difficulty guidelines (do NOT make all questions easy):
    "easy"   → answer is directly stated in one sentence of the content
    "medium" → requires understanding a concept or full paragraph
    "hard"   → requires synthesizing information from multiple parts
  Aim for at least {hard_count} medium or hard question(s) per set.

Return ONLY a valid JSON array, no extra text:
[
  {{
    "question": "...",
    "ground_truth": "short factual answer (1-3 sentences)",
    "question_type": "definition|comparison|multi_step",
    "difficulty": "easy|medium|hard"
  }}
]"""

_CONTENT_QA_PROMPT_VI = """\
Bạn là chuyên gia tạo câu hỏi đánh giá cho chatbot hỏi đáp về giáo trình và học liệu mở.

Nội dung từ tài liệu PDF có tiêu đề "{title}" (lĩnh vực: {subject}):

--- NỘI DUNG ---
{content}
--- KẾT THÚC ---

Tạo chính xác {n} cặp câu hỏi - câu trả lời bằng tiếng Việt dưới dạng JSON.
Quy tắc:
- Câu trả lời phải dựa HOÀN TOÀN vào nội dung ở trên, không bịa đặt.
- Sử dụng các loại sau:
    "definition"  → "X là gì?", "Định nghĩa Y.", "Z có nghĩa là gì?"
    "comparison"  → "Sự khác nhau giữa A và B là gì?"
    "multi_step"  → cần kết hợp thông tin từ 2+ phần của nội dung
- Hướng dẫn độ khó (KHÔNG tạo tất cả câu hỏi easy):
    "easy"   → câu trả lời nằm trực tiếp trong một câu của nội dung
    "medium" → cần hiểu một khái niệm hoặc cả đoạn văn
    "hard"   → cần tổng hợp thông tin từ nhiều phần khác nhau
  Mục tiêu: ít nhất {hard_count} câu hỏi medium hoặc hard.

Chỉ trả về mảng JSON hợp lệ, không có văn bản thêm:
[
  {{
    "question": "câu hỏi bằng tiếng Việt",
    "ground_truth": "câu trả lời ngắn gọn (1-3 câu) bằng tiếng Việt",
    "question_type": "definition|comparison|multi_step",
    "difficulty": "easy|medium|hard"
  }}
]"""

_FIND_MATERIAL_PROMPT_EN = """\
A student is searching for academic course materials.
Here is an excerpt from a resource titled "{title}" (subject: {subject}):

--- EXCERPT ---
{content}
--- END ---

Write ONE "find materials" question referencing a specific concept or topic from this excerpt.
The student does NOT know the resource name — phrase the question as if searching for it.
Good formats: "What textbook explains [concept]?", "Where can I find course material about [specific topic]?", \
"What academic resource covers [concept from the text]?"

Return ONLY valid JSON (no extra text):
{{
  "question": "...",
  "ground_truth": "The resource '{title}' covers this topic."
}}"""

_FIND_MATERIAL_PROMPT_VI = """\
Một sinh viên đang tìm kiếm tài liệu học tập.
Đây là đoạn trích từ tài liệu có tiêu đề "{title}" (lĩnh vực: {subject}):

--- ĐOẠN TRÍCH ---
{content}
--- KẾT THÚC ---

Viết MỘT câu hỏi tìm tài liệu tham chiếu đến một khái niệm cụ thể trong đoạn trích này.
Sinh viên CHƯA biết tên tài liệu — đặt câu hỏi như thể đang tìm kiếm.
Ví dụ: "Giáo trình nào giải thích [khái niệm]?", "Tìm tài liệu về [chủ đề cụ thể]?"

Chỉ trả về JSON hợp lệ (không có văn bản thêm):
{{
  "question": "câu hỏi tìm tài liệu bằng tiếng Việt",
  "ground_truth": "Tài liệu phù hợp là: {title}"
}}"""


def generate_content_qa(doc: dict, content: str, n: int = 3, lang: str = "en") -> list[dict]:
    """Generate definition/comparison/multi_step pairs from page content."""
    subject = ", ".join((doc.get("subject_names_en") or [])[:3]) or "general education"
    prompt_template = _CONTENT_QA_PROMPT_VI if lang == "vi" else _CONTENT_QA_PROMPT_EN
    hard_count = max(1, n // 2)  # at least half should be medium/hard
    prompt = prompt_template.format(
        title=doc.get("title", "Unknown"),
        subject=subject,
        content=content,
        n=n,
        hard_count=hard_count,
    )
    time.sleep(INTER_CALL_DELAY)
    raw = call_llm([{"role": "user", "content": prompt}], max_tokens=1200)
    try:
        pairs = _parse_json_from_llm(raw)
        if isinstance(pairs, list):
            return pairs
    except ValueError as exc:
        log.warning("JSON parse failed for %s: %s", doc.get("asset_uid", "?")[:16], exc)
    return []


def generate_find_material_qa(doc: dict, content: str, lang: str = "en") -> dict | None:
    """Generate one find_material question from actual page content (not just metadata).

    Using content ensures PageIndex can find page-level evidence, improving evidence_hit_rate.
    """
    subject = ", ".join((doc.get("subject_names_en") or [])[:2]) or "general education"
    prompt_template = _FIND_MATERIAL_PROMPT_VI if lang == "vi" else _FIND_MATERIAL_PROMPT_EN
    # Use first 1500 chars of content so the question references a specific concept
    content_snippet = content[:1500].strip()
    prompt = prompt_template.format(
        title=doc.get("title", "Unknown"),
        subject=subject,
        content=content_snippet,
    )
    time.sleep(INTER_CALL_DELAY)
    try:
        raw = call_llm([{"role": "user", "content": prompt}], max_tokens=200)
        pair = _parse_json_from_llm(raw)
        if isinstance(pair, dict) and "question" in pair:
            return pair
    except Exception as exc:
        log.debug("find_material generation failed: %s", exc)
    return None


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------
def _save_checkpoint(buckets: dict, question_counter: int, processed_uids: set) -> None:
    try:
        _CHECKPOINT_PATH.write_text(json.dumps({
            "buckets": {k: v for k, v in buckets.items() if k != "out_of_scope"},
            "question_counter": question_counter,
            "processed_uids": list(processed_uids),
        }, ensure_ascii=False))
    except Exception as exc:
        log.debug("Checkpoint save failed: %s", exc)


def _load_checkpoint(buckets: dict) -> tuple[int, set]:
    """Load checkpoint into buckets in-place. Returns (question_counter, processed_uids)."""
    if not _CHECKPOINT_PATH.exists():
        return 0, set()
    try:
        data = json.loads(_CHECKPOINT_PATH.read_text())
        for k, v in data.get("buckets", {}).items():
            buckets[k] = v
        total = sum(len(v) for v in buckets.values())
        log.info("Resumed from checkpoint: %d questions across %d types",
                 total, len(buckets))
        return data.get("question_counter", 0), set(data.get("processed_uids", []))
    except Exception as exc:
        log.warning("Failed to load checkpoint (%s) — starting fresh", exc)
        return 0, set()


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
def build_test_set(lang: str = "en", resume: bool = False) -> list[dict]:
    log.info("=" * 60)
    log.info("Phase 1: Sampling documents from Elasticsearch")
    log.info("=" * 60)

    all_docs: list[dict] = []
    for source, n in ES_SAMPLE_PER_SOURCE.items():
        docs = sample_from_es(source, n)
        log.info("  %s: %d documents sampled", source, len(docs))
        all_docs.extend(docs)

    random.shuffle(all_docs)
    log.info("Total candidate documents: %d", len(all_docs))

    # Buckets for collected Q&A
    buckets: dict[str, list[dict]] = {k: [] for k in TARGET_COUNTS}

    # Load checkpoint if resuming
    if resume:
        question_counter, processed_uids = _load_checkpoint(buckets)
    else:
        question_counter, processed_uids = 0, set()

    log.info("")
    log.info("Phase 2: Generating Q&A pairs (content + find_material combined)")
    log.info("=" * 60)

    content_targets = {"definition", "comparison", "multi_step"}
    doc_index = 0

    for doc in all_docs:
        content_full = all(len(buckets[t]) >= TARGET_COUNTS[t] for t in content_targets)
        fm_full = len(buckets["find_material"]) >= TARGET_COUNTS["find_material"]

        if content_full and fm_full:
            break

        asset_uid = doc["asset_uid"]
        title = doc.get("title", "Unknown")
        doc_index += 1

        # Skip already processed in a resumed run
        if asset_uid in processed_uids:
            continue

        log.info("[doc %d] %s | %s", doc_index, asset_uid[:16], title[:50])

        content = fetch_usable_content(doc)
        if not content:
            log.info("  → No usable content, skipping")
            processed_uids.add(asset_uid)
            continue

        log.info("  → Content: %d chars", len(content))
        made_progress = False

        # --- Content-based Q&A (definition / comparison / multi_step) ---
        if not content_full:
            still_needed = sum(
                max(0, TARGET_COUNTS[t] - len(buckets[t])) for t in content_targets
            )
            n_pairs = min(4, still_needed)
            if n_pairs > 0:
                try:
                    pairs = generate_content_qa(doc, content, n=n_pairs, lang=lang)
                except Exception as exc:
                    log.warning("  → Content Q&A error: %s", exc)
                    pairs = []

                for pair in pairs:
                    qtype = pair.get("question_type", "")
                    if qtype not in content_targets:
                        continue
                    if len(buckets[qtype]) >= TARGET_COUNTS[qtype]:
                        continue
                    question_counter += 1
                    buckets[qtype].append({
                        "id": f"Q{question_counter:03d}",
                        "question": pair.get("question", "").strip(),
                        "ground_truth": pair.get("ground_truth", "").strip(),
                        "source_asset_uid": asset_uid,
                        "source_title": title,
                        "source_system": doc.get("source_system", ""),
                        "source_url": doc.get("source_url", ""),
                        "question_type": qtype,
                        "difficulty": pair.get("difficulty", "medium"),
                        "language": lang,
                    })
                    made_progress = True

                filled = {t: len(buckets[t]) for t in content_targets}
                log.info("  → Content buckets: %s", filled)

        # --- Find_material from the same content (NOT metadata) ---
        if not fm_full:
            try:
                fm_pair = generate_find_material_qa(doc, content, lang=lang)
            except Exception as exc:
                log.warning("  → find_material error: %s", exc)
                fm_pair = None

            if fm_pair:
                question_counter += 1
                diff = fm_pair.get("difficulty", "medium")
                buckets["find_material"].append({
                    "id": f"Q{question_counter:03d}",
                    "question": fm_pair.get("question", "").strip(),
                    "ground_truth": fm_pair.get("ground_truth", "").strip(),
                    "source_asset_uid": asset_uid,
                    "source_title": title,
                    "source_system": doc.get("source_system", ""),
                    "source_url": doc.get("source_url", ""),
                    "question_type": "find_material",
                    "difficulty": diff,
                    "language": lang,
                })
                made_progress = True
                log.info("  → find_material [%d/%d]: %s",
                         len(buckets["find_material"]), TARGET_COUNTS["find_material"],
                         fm_pair.get("question", "")[:60])

        processed_uids.add(asset_uid)
        # Checkpoint after each doc so Ctrl+C doesn't lose all progress
        _save_checkpoint(buckets, question_counter, processed_uids)

    # --- Out-of-scope questions ---
    log.info("")
    log.info("Phase 3: Adding out-of-scope questions (%s)", lang)
    oos_pool = _get_oos_pool(lang)
    for oos in oos_pool[:TARGET_COUNTS["out_of_scope"]]:
        question_counter += 1
        buckets["out_of_scope"].append({
            "id": f"Q{question_counter:03d}",
            "question": oos["question"],
            "ground_truth": oos["ground_truth"],
            "source_asset_uid": None,
            "source_title": None,
            "source_system": None,
            "source_url": None,
            "question_type": "out_of_scope",
            "difficulty": "easy",
            "language": "vi" if "N/A —" not in oos["ground_truth"] or "ngoài" in oos["ground_truth"] else lang,
        })

    # --- Assemble final list ---
    log.info("")
    log.info("Phase 4: Assembling final test set")
    final: list[dict] = []
    for qtype, items in buckets.items():
        available = items[: TARGET_COUNTS[qtype]]
        if len(available) < TARGET_COUNTS[qtype]:
            log.warning("  %s: only %d/%d questions available",
                        qtype, len(available), TARGET_COUNTS[qtype])
        final.extend(available)

    # Re-number IDs sequentially
    for idx, q in enumerate(final, 1):
        q["id"] = f"Q{idx:03d}"

    log.info("Total questions in test set: %d", len(final))
    for qtype in TARGET_COUNTS:
        count = sum(1 for q in final if q["question_type"] == qtype)
        log.info("  %-14s : %d", qtype, count)

    # Clean up checkpoint once fully assembled
    if _CHECKPOINT_PATH.exists():
        _CHECKPOINT_PATH.unlink()

    return final


# ---------------------------------------------------------------------------
# Import from existing Vietnamese test set (chatbot_qa_testset_vi.md)
# ---------------------------------------------------------------------------
# Maps Vietnamese question-type labels to standard keys
_VI_TYPE_MAP = {
    "định nghĩa": "definition",
    "giải thích": "definition",
    "liệt kê":    "multi_step",
    "công thức":  "multi_step",
    "so sánh":    "comparison",
}

# Known asset_uids from chatbot_qa_testset_vi.md
_VI_ASSET_UIDS = {
    "Linear Algebra (2016)":                          "08c79d5c3d5df1d072e084ad931523d282f87be5057c31b2840ddc6f19ab3507",
    "Introduction to Python Programming – OpenStax":  "b9307858c8b74a9552de02ff0be7e28c8642104d9ba6f0d5f96b838ccb23ae32",
    "Optimal, Integral, Likely (OIL)":                "f0fcae8162a7c1c656d74b253aedc6b1d516f503cbc93c5f656a9717698e3179",
    "Direct Energy":                                  "ff4f7cf5c3c369bdfe9f63ef40f5fe5f1f61cabdd9b26ad087b3f71b8842d49d",
    "Linear Algebra (Hefferon)":                      "57f45f296e648404ee0af0fec22fdfb31ce6a844555e1dbad24b6f5caf8a9e7a",
    "Multivariable Calculus with Theory":             "18738220d76e1f45d1a9e9989726b526ecdef3ef71d7d3ddc1bffd401fd7c2d4",
}


def import_vi_testset(md_path: Path) -> list[dict]:
    """
    Parse chatbot_qa_testset_vi.md and convert to test_set.json format.
    Returns a list of question dicts ready to be seeded into the test set.
    """
    import re
    text = md_path.read_text(encoding="utf-8")

    # Split on ### headers (each section = one Q&A pair)
    sections = re.split(r"^### .+$", text, flags=re.MULTILINE)[1:]

    questions: list[dict] = []
    for section in sections:
        def _field(label: str) -> str:
            m = re.search(rf"- {label}: `([^`]+)`", section)
            return m.group(1).strip() if m else ""

        question     = _field("Câu hỏi")
        ground_truth = _field("Đáp án kỳ vọng")
        source_title = _field("Tài liệu nguồn")
        qtype_vi     = _field("Loại câu hỏi")

        if not question or not ground_truth:
            continue

        # Resolve asset_uid from title
        asset_uid = None
        for title_key, uid in _VI_ASSET_UIDS.items():
            if title_key.lower() in source_title.lower():
                asset_uid = uid
                break

        # Map Vietnamese type label to standard key
        qtype = _VI_TYPE_MAP.get(qtype_vi.lower(), "definition")

        questions.append({
            "question":         question,
            "ground_truth":     ground_truth,
            "source_asset_uid": asset_uid,
            "source_title":     source_title,
            "source_system":    "otl" if asset_uid else "",
            "source_url":       "",
            "question_type":    qtype,
            "difficulty":       "medium",
            "language":         "vi",
        })

    log.info("Imported %d Vietnamese questions from %s", len(questions), md_path.name)
    return questions


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate OER chatbot evaluation test set")
    parser.add_argument("--force", action="store_true",
                        help="Regenerate from scratch even if test_set.json already exists")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint (created automatically after each document)")
    parser.add_argument("--lang", choices=["en", "vi", "mixed"], default="en",
                        help="Language for generated questions (default: en)")
    parser.add_argument("--import-vi", action="store_true",
                        help="Seed test set with pre-built Vietnamese questions from chatbot_qa_testset_vi.md")
    args = parser.parse_args()

    # --force clears checkpoint and overwrites test_set.json
    if args.force and _CHECKPOINT_PATH.exists():
        _CHECKPOINT_PATH.unlink()
        log.info("Checkpoint cleared (--force)")

    # --resume requires a checkpoint or existing partial test set
    if args.resume and not _CHECKPOINT_PATH.exists():
        log.warning("--resume specified but no checkpoint found; starting fresh")
        args.resume = False

    if TEST_SET_PATH.exists() and not args.force and not args.resume:
        existing = json.loads(TEST_SET_PATH.read_text())
        log.info("test_set.json already exists with %d questions. "
                 "Use --force to regenerate or --resume to continue from checkpoint.",
                 len(existing))
        return

    # Sanity check: API reachable?
    try:
        r = requests.get(f"{API_BASE}/health", timeout=10)
        r.raise_for_status()
        log.info("API health check: OK")
    except Exception as exc:
        log.error("Cannot reach API at %s: %s", API_BASE, exc)
        sys.exit(1)

    # Sanity check: at least one LLM key configured
    if not GEMINI_API_KEY and not GROQ_API_KEY:
        log.error("Neither GEMINI_API_KEY nor GROQ_API_KEY is configured. Check .env file.")
        sys.exit(1)

    if GEMINI_API_KEY:
        log.info("LLM backend: Google Gemini (%s)", GEMINI_MODEL)
    else:
        log.info("LLM backend: Groq (%s) — GEMINI_API_KEY not set", GROQ_MODEL)

    log.info("Language mode: %s", args.lang)

    # Optional: seed with pre-built Vietnamese questions
    vi_seed: list[dict] = []
    if args.import_vi:
        vi_md = Path(__file__).parent.parent / "chatbot_qa_testset_vi.md"
        if vi_md.exists():
            vi_seed = import_vi_testset(vi_md)
            log.info("Seeding %d Vietnamese questions from existing test set", len(vi_seed))
        else:
            log.warning("--import-vi: file not found at %s", vi_md)

    test_set = build_test_set(lang=args.lang, resume=args.resume)

    # Merge seeded Vietnamese questions (prepend, deduplicate by question text)
    if vi_seed:
        existing_questions = {q["question"] for q in test_set}
        new_vi = [q for q in vi_seed if q["question"] not in existing_questions]
        # Assign IDs and merge at the beginning
        for i, q in enumerate(new_vi, 1):
            q["id"] = f"VI{i:03d}"
        test_set = new_vi + test_set
        log.info("Final test set: %d questions (%d seeded VI + %d generated)",
                 len(test_set), len(new_vi), len(test_set) - len(new_vi))

    # Re-number all IDs sequentially
    for idx, q in enumerate(test_set, 1):
        q["id"] = f"Q{idx:03d}"

    TEST_SET_PATH.write_text(
        json.dumps(test_set, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("Saved %d questions to %s", len(test_set), TEST_SET_PATH)


if __name__ == "__main__":
    main()
