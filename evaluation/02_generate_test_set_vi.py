"""
Script 2 — Generate Vietnamese test set (test_set_vi.json).

Tạo 100 câu hỏi tiếng Việt hoàn toàn mới từ cùng kho tài liệu OER,
mô phỏng người dùng Việt Nam đặt câu hỏi tự nhiên.
So sánh kết quả với test_set.json (tiếng Anh) để đánh giá ảnh hưởng ngôn ngữ.

Cấu trúc tương tự 01_generate_test_set.py nhưng:
  - Mặc định lang="vi" → prompt tiếng Việt, câu hỏi tiếng Việt
  - Random seed khác (seed=42) → sampling tài liệu khác
  - Output: test_set_vi.json (không ghi đè test_set.json)
  - Checkpoint riêng: _test_set_vi_checkpoint.json

Usage:
  python3 02_generate_test_set_vi.py
  python3 02_generate_test_set_vi.py --force
  python3 02_generate_test_set_vi.py --resume
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
    GROQ_API_KEY, GROQ_API_KEY_2, GROQ_BASE_URL, GROQ_MODEL,
    GEMINI_API_KEY, GEMINI_BASE_URL, GEMINI_MODEL,
    TARGET_COUNTS, ES_SAMPLE_PER_SOURCE,
    DEBUG_TIMEOUT_SEC, GROQ_TIMEOUT_SEC, GEMINI_TIMEOUT_SEC,
    MAX_RETRIES, GROQ_RETRY_DELAY, GEMINI_RETRY_DELAY, INTER_CALL_DELAY,
    LOGS_DIR,
)

# ---------------------------------------------------------------------------
# Paths — riêng cho bản tiếng Việt
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).parent
TEST_SET_VI_PATH   = _ROOT / "test_set_vi.json"
_CHECKPOINT_PATH   = _ROOT / "_test_set_vi_checkpoint.json"

random.seed(42)  # seed khác script 01 → sampling tài liệu khác

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "02_generate_test_set_vi.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Câu hỏi out-of-scope tiếng Việt (hardcoded — 20 câu)
# Chủ đề gần gũi với người dùng Việt Nam, không liên quan OER
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Prompt tiếng Việt — content Q&A
# ---------------------------------------------------------------------------
_CONTENT_QA_PROMPT_VI = """\
Bạn là chuyên gia tạo câu hỏi đánh giá cho chatbot hỏi đáp về giáo trình và \
học liệu mở dành cho người dùng Việt Nam.

Nội dung từ tài liệu PDF có tiêu đề "{title}" (lĩnh vực: {subject}):

--- NỘI DUNG ---
{content}
--- KẾT THÚC ---

Tạo chính xác {n} cặp câu hỏi - câu trả lời bằng tiếng Việt tự nhiên, \
như thể một sinh viên Việt Nam đang thực sự hỏi.
Quy tắc:
- Câu trả lời phải dựa HOÀN TOÀN vào nội dung ở trên.
- Câu hỏi phải tự nhiên, đúng ngữ pháp tiếng Việt, KHÔNG dịch máy.
- Sử dụng các loại sau:
    "definition"  → "X là gì?", "Định nghĩa của Y là gì?", "Thuật ngữ Z có nghĩa là gì?"
    "comparison"  → "Sự khác nhau giữa A và B là gì?", "A và B khác nhau như thế nào?"
    "multi_step"  → cần kết hợp thông tin từ 2+ phần khác nhau của nội dung
- Hướng dẫn độ khó (KHÔNG tạo tất cả câu hỏi easy):
    "easy"   → câu trả lời nằm trực tiếp trong một câu của nội dung
    "medium" → cần hiểu một khái niệm hoặc cả đoạn văn
    "hard"   → cần tổng hợp thông tin từ nhiều phần
  Mục tiêu: ít nhất {hard_count} câu hỏi medium hoặc hard trong tập này.

Chỉ trả về mảng JSON hợp lệ, không có văn bản thêm:
[
  {{
    "question": "câu hỏi tiếng Việt tự nhiên",
    "ground_truth": "câu trả lời ngắn gọn (1-3 câu) bằng tiếng Việt",
    "question_type": "definition|comparison|multi_step",
    "difficulty": "easy|medium|hard"
  }}
]"""

# ---------------------------------------------------------------------------
# Prompt tiếng Việt — find_material
# ---------------------------------------------------------------------------
_FIND_MATERIAL_PROMPT_VI = """\
Một sinh viên Việt Nam đang tìm kiếm tài liệu học tập bằng tiếng Việt.
Đây là đoạn trích từ tài liệu có tiêu đề "{title}" (lĩnh vực: {subject}):

--- ĐOẠN TRÍCH ---
{content}
--- KẾT THÚC ---

Viết MỘT câu hỏi tìm tài liệu tham chiếu đến một khái niệm hoặc chủ đề \
cụ thể trong đoạn trích này. Sinh viên CHƯA biết tên tài liệu.
Câu hỏi phải:
- Bằng tiếng Việt tự nhiên (không dịch máy)
- Hỏi về một nội dung CỤ THỂ trong đoạn trích (không hỏi chung chung)
- Ví dụ đúng: "Tài liệu nào giải thích về [khái niệm cụ thể]?",
              "Tôi muốn tìm giáo trình về [chủ đề từ đoạn trích]?",
              "Có tài liệu nào đề cập đến [nội dung X] không?"

Chỉ trả về JSON hợp lệ (không có văn bản thêm):
{{
  "question": "câu hỏi tìm tài liệu bằng tiếng Việt",
  "ground_truth": "Tài liệu phù hợp là: '{title}'"
}}"""

# ---------------------------------------------------------------------------
# LLM helpers (Gemini primary, Groq fallback)
# ---------------------------------------------------------------------------
def _call_gemini(messages: list[dict], max_tokens: int = 900, temperature: float = 0.3) -> str:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set.")
    contents = []
    for m in messages:
        role = "user" if m["role"] in ("user", "system") else "model"
        contents.append({"role": role, "parts": [{"text": m["content"]}]})
    payload = {
        "contents": contents,
        "generationConfig": {"maxOutputTokens": max_tokens, "temperature": temperature},
    }
    url = f"{GEMINI_BASE_URL}/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(url, json=payload, timeout=GEMINI_TIMEOUT_SEC)
            resp.raise_for_status()
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            wait = GEMINI_RETRY_DELAY * (2 ** attempt)
            if status == 429:
                log.warning("Gemini rate-limited. Chờ %ds ...", wait)
                time.sleep(wait)
            else:
                log.warning("Gemini HTTP error (lần %d): %s", attempt + 1, exc)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(GEMINI_RETRY_DELAY)
        except Exception as exc:
            log.warning("Gemini error (lần %d): %s", attempt + 1, exc)
            if attempt < MAX_RETRIES - 1:
                time.sleep(GEMINI_RETRY_DELAY)
    raise RuntimeError("Gemini API thất bại sau tất cả lần thử.")


def _call_groq_with_key(api_key: str, messages: list[dict],
                        max_tokens: int, temperature: float,
                        model: str = GROQ_MODEL) -> str:
    """Gọi Groq API với một key cụ thể. Raise nếu rate-limited hoặc lỗi."""
    resp = requests.post(
        f"{GROQ_BASE_URL}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}",
                 "Content-Type": "application/json"},
        json={"model": model, "messages": messages,
              "max_tokens": max_tokens, "temperature": temperature},
        timeout=GROQ_TIMEOUT_SEC,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def _call_groq(messages: list[dict], max_tokens: int = 900,
               temperature: float = 0.3, model: str = GROQ_MODEL) -> str:
    """Gọi Groq với key 1, tự động fallback sang key 2 khi hết quota (429)."""
    keys = [k for k in [GROQ_API_KEY, GROQ_API_KEY_2] if k]
    if not keys:
        raise RuntimeError("Chưa cấu hình GROQ_API_KEY.")

    for key_idx, api_key in enumerate(keys):
        key_label = f"key{key_idx + 1}"
        for attempt in range(MAX_RETRIES):
            try:
                result = _call_groq_with_key(api_key, messages, max_tokens,
                                             temperature, model)
                return result
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status == 429:
                    if attempt < MAX_RETRIES - 1:
                        wait = GROQ_RETRY_DELAY * (2 ** attempt)
                        log.warning("Groq %s rate-limited. Chờ %ds ...", key_label, wait)
                        time.sleep(wait)
                    else:
                        log.warning("Groq %s hết quota — thử key tiếp theo.", key_label)
                        break  # sang key tiếp theo
                else:
                    log.warning("Groq %s HTTP error (lần %d): %s", key_label, attempt + 1, exc)
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(GROQ_RETRY_DELAY)
            except Exception as exc:
                log.warning("Groq %s error (lần %d): %s", key_label, attempt + 1, exc)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(GROQ_RETRY_DELAY)

    raise RuntimeError("Tất cả Groq API keys đều thất bại.")


def _call_llm(messages: list[dict], max_tokens: int = 900, temperature: float = 0.3) -> str:
    if GEMINI_API_KEY:
        return _call_gemini(messages, max_tokens=max_tokens, temperature=temperature)
    return _call_groq(messages, max_tokens=max_tokens, temperature=temperature)


def _parse_json(text: str) -> Any:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    for start_char, end_char in [("[", "]"), ("{", "}")]:
        s = text.find(start_char)
        e = text.rfind(end_char)
        if s != -1 and e != -1 and e > s:
            try:
                return json.loads(text[s:e + 1])
            except json.JSONDecodeError:
                pass
    raise ValueError(f"Không parse được JSON từ: {text[:200]}")

# ---------------------------------------------------------------------------
# Elasticsearch sampling
# ---------------------------------------------------------------------------
def _sample_from_es(source_system: str, n: int) -> list[dict]:
    query = {
        "size": min(n * 3, 500),
        "query": {
            "bool": {
                "must": [{"term": {"source_system": source_system}}],
                "filter": [{"exists": {"field": "asset_uid"}}],
            }
        },
        "_source": ["asset_uid", "title", "description",
                    "source_system", "subject_names_en", "source_url"],
    }
    try:
        resp = requests.get(f"{ES_HOST}/{ES_INDEX}/_search", json=query, timeout=15)
        resp.raise_for_status()
        hits = resp.json()["hits"]["hits"]
    except Exception as exc:
        log.error("ES query thất bại cho %s: %s", source_system, exc)
        return []
    docs = [h["_source"] for h in hits
            if h["_source"].get("asset_uid") and h["_source"]["asset_uid"] != "None"]
    random.shuffle(docs)
    seen: set[str] = set()
    unique: list[dict] = []
    for doc in docs:
        key = (doc.get("title") or "")[:30].lower()
        if key not in seen:
            seen.add(key)
            unique.append(doc)
    return unique[:n]

# ---------------------------------------------------------------------------
# Dịch nội dung tiếng Anh → tiếng Việt trước khi gen câu hỏi
# ---------------------------------------------------------------------------
_TRANSLATE_CONTENT_PROMPT = """\
Translate the following English academic text to Vietnamese.
Rules:
- Keep technical/academic terms that are widely used in English as-is \
  (e.g. "machine learning", "database", "regression", "entropy", proper nouns).
- Maintain academic tone and accuracy.
- Return ONLY the Vietnamese translation, no explanation.

English text:
{text}"""

# Dùng model nhỏ hơn để dịch content — tiết kiệm quota cho bước gen câu hỏi
_TRANSLATE_GROQ_MODEL = "llama-3.1-8b-instant"
_TRANSLATE_DELAY      = 3.0   # giây — tránh hit 30 RPM của Groq free tier


def _translate_content_to_vi(english_text: str) -> str:
    """Translate an English document excerpt to Vietnamese.
    Uses llama-3.1-8b-instant (faster/cheaper) for Groq, Gemini otherwise.
    """
    prompt = _TRANSLATE_CONTENT_PROMPT.format(text=english_text[:2500])
    time.sleep(_TRANSLATE_DELAY)

    # Gemini là primary nếu có — rate limit cao hơn
    if GEMINI_API_KEY:
        try:
            return _call_gemini([{"role": "user", "content": prompt}],
                                max_tokens=1800, temperature=0.1)
        except Exception as exc:
            log.warning("Gemini translate thất bại (%s) — thử Groq.", exc)

    # Groq fallback dùng model nhỏ để tiết kiệm quota — có fallback key 2
    if GROQ_API_KEY or GROQ_API_KEY_2:
        try:
            return _call_groq([{"role": "user", "content": prompt}],
                              max_tokens=1800, temperature=0.1,
                              model=_TRANSLATE_GROQ_MODEL)
        except Exception as exc:
            log.warning("Groq translate thất bại (%s) — dùng bản EN gốc.", exc)

    return english_text


# ---------------------------------------------------------------------------
# Lấy nội dung trang từ debug API
# ---------------------------------------------------------------------------
def _get_page_content(asset_uid: str, pages: str) -> list[dict]:
    try:
        resp = requests.post(
            f"{API_BASE}/debug/get_page_content",
            json={"asset_uid": asset_uid, "pages": pages,
                  "reason": "vi test-set generation"},
            timeout=DEBUG_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            if not data.get("found", True):
                return []
            content = data.get("content") or data.get("pages") or []
            return content if isinstance(content, list) else []
        if isinstance(data, list):
            return data
        return []
    except Exception as exc:
        log.debug("get_page_content thất bại %s pages=%s: %s", asset_uid, pages, exc)
        return []


def _is_real_prose(text: str) -> bool:
    import re
    if not text or len(text) < 200:
        return False
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return False
    toc_lines = sum(1 for l in lines if len(re.findall(r'\.{4,}', l)) > 0)
    if toc_lines > len(lines) * 0.25:
        return False
    avg_len = sum(len(l) for l in lines) / len(lines)
    if avg_len < 35:
        return False
    sentences = re.split(r'[.!?]+\s+', text)
    if sum(1 for s in sentences if len(s.strip()) > 40) < 2:
        return False
    return True


def _fetch_content(doc: dict, min_chars: int = 400) -> str:
    asset_uid = doc.get("asset_uid")
    if not asset_uid or asset_uid == "None":
        return ""
    for page_range in ["2-6", "7-12", "13-18", "19-25", "26-35"]:
        pages = _get_page_content(asset_uid, page_range)
        parts: list[str] = []
        for p in pages:
            text = (p.get("text") if isinstance(p, dict) else p or "").strip()
            if text and _is_real_prose(text):
                parts.append(text)
        combined = "\n\n".join(parts).strip()
        if len(combined) >= min_chars:
            return combined[:4000]
        time.sleep(0.3)
    return ""

# ---------------------------------------------------------------------------
# Tạo câu hỏi Q&A từ nội dung
# ---------------------------------------------------------------------------
def _generate_content_qa(doc: dict, content: str, n: int = 3) -> list[dict]:
    subject = ", ".join((doc.get("subject_names_en") or [])[:3]) or "giáo dục đại cương"
    hard_count = max(1, n // 2)
    prompt = _CONTENT_QA_PROMPT_VI.format(
        title=doc.get("title", "Unknown"),
        subject=subject,
        content=content,
        n=n,
        hard_count=hard_count,
    )
    time.sleep(INTER_CALL_DELAY)
    raw = _call_llm([{"role": "user", "content": prompt}], max_tokens=1200)
    try:
        pairs = _parse_json(raw)
        if isinstance(pairs, list):
            return pairs
    except ValueError as exc:
        log.warning("Parse JSON thất bại cho %s: %s", doc.get("asset_uid", "?")[:16], exc)
    return []


def _generate_find_material_qa(doc: dict, content: str) -> dict | None:
    subject = ", ".join((doc.get("subject_names_en") or [])[:2]) or "giáo dục"
    prompt = _FIND_MATERIAL_PROMPT_VI.format(
        title=doc.get("title", "Unknown"),
        subject=subject,
        content=content[:1500].strip(),
    )
    time.sleep(INTER_CALL_DELAY)
    try:
        raw = _call_llm([{"role": "user", "content": prompt}], max_tokens=200)
        pair = _parse_json(raw)
        if isinstance(pair, dict) and "question" in pair:
            return pair
    except Exception as exc:
        log.debug("find_material thất bại: %s", exc)
    return None

# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------
def _save_checkpoint(buckets: dict, counter: int, processed: set) -> None:
    try:
        _CHECKPOINT_PATH.write_text(json.dumps({
            "buckets": {k: v for k, v in buckets.items() if k != "out_of_scope"},
            "question_counter": counter,
            "processed_uids": list(processed),
        }, ensure_ascii=False))
    except Exception as exc:
        log.debug("Lưu checkpoint thất bại: %s", exc)


def _load_checkpoint(buckets: dict) -> tuple[int, set]:
    if not _CHECKPOINT_PATH.exists():
        return 0, set()
    try:
        data = json.loads(_CHECKPOINT_PATH.read_text())
        for k, v in data.get("buckets", {}).items():
            buckets[k] = v
        total = sum(len(v) for v in buckets.values())
        log.info("Tiếp tục từ checkpoint: %d câu hỏi", total)
        return data.get("question_counter", 0), set(data.get("processed_uids", []))
    except Exception as exc:
        log.warning("Load checkpoint thất bại (%s) — bắt đầu mới", exc)
        return 0, set()

# ---------------------------------------------------------------------------
# Orchestration chính
# ---------------------------------------------------------------------------
def build_vi_test_set(resume: bool = False) -> list[dict]:
    log.info("=" * 60)
    log.info("Phase 1: Sampling tài liệu từ Elasticsearch (seed=42)")
    log.info("=" * 60)

    all_docs: list[dict] = []
    for source, n in ES_SAMPLE_PER_SOURCE.items():
        docs = _sample_from_es(source, n)
        log.info("  %s: %d tài liệu", source, len(docs))
        all_docs.extend(docs)
    random.shuffle(all_docs)
    log.info("Tổng tài liệu ứng viên: %d", len(all_docs))

    buckets: dict[str, list[dict]] = {k: [] for k in TARGET_COUNTS}

    if resume:
        counter, processed = _load_checkpoint(buckets)
    else:
        counter, processed = 0, set()

    log.info("")
    log.info("Phase 2: Tạo câu hỏi tiếng Việt")
    log.info("=" * 60)

    content_targets = {"definition", "comparison", "multi_step"}

    for doc_idx, doc in enumerate(all_docs, 1):
        content_done = all(len(buckets[t]) >= TARGET_COUNTS[t] for t in content_targets)
        fm_done = len(buckets["find_material"]) >= TARGET_COUNTS["find_material"]
        if content_done and fm_done:
            break

        asset_uid = doc["asset_uid"]
        title = doc.get("title", "Unknown")

        if asset_uid in processed:
            continue

        log.info("[doc %d] %s | %s", doc_idx, asset_uid[:16], title[:50])
        content = _fetch_content(doc)
        if not content:
            log.info("  → Không có nội dung dùng được, bỏ qua")
            processed.add(asset_uid)
            continue

        log.info("  → Nội dung EN: %d ký tự", len(content))

        # Dịch nội dung sang tiếng Việt để gen câu hỏi bám sát thuật ngữ gốc
        content_vi = _translate_content_to_vi(content)
        log.info("  → Nội dung VI: %d ký tự", len(content_vi))

        # Content-based Q&A
        if not content_done:
            needed = sum(max(0, TARGET_COUNTS[t] - len(buckets[t])) for t in content_targets)
            n_pairs = min(4, needed)
            if n_pairs > 0:
                try:
                    pairs = _generate_content_qa(doc, content_vi, n=n_pairs)
                except Exception as exc:
                    log.warning("  → Content Q&A error: %s", exc)
                    pairs = []
                for pair in pairs:
                    qtype = pair.get("question_type", "")
                    if qtype not in content_targets:
                        continue
                    if len(buckets[qtype]) >= TARGET_COUNTS[qtype]:
                        continue
                    counter += 1
                    buckets[qtype].append({
                        "id": f"QVI{counter:03d}",
                        "question": pair.get("question", "").strip(),
                        "ground_truth": pair.get("ground_truth", "").strip(),
                        "source_asset_uid": asset_uid,
                        "source_title": title,
                        "source_system": doc.get("source_system", ""),
                        "source_url": doc.get("source_url", ""),
                        "question_type": qtype,
                        "difficulty": pair.get("difficulty", "medium"),
                        "language": "vi",
                    })
                log.info("  → Content buckets: %s",
                         {t: len(buckets[t]) for t in content_targets})

        # Find material
        if not fm_done:
            try:
                fm = _generate_find_material_qa(doc, content_vi)
            except Exception as exc:
                log.warning("  → find_material error: %s", exc)
                fm = None
            if fm:
                counter += 1
                buckets["find_material"].append({
                    "id": f"QVI{counter:03d}",
                    "question": fm.get("question", "").strip(),
                    "ground_truth": fm.get("ground_truth", "").strip(),
                    "source_asset_uid": asset_uid,
                    "source_title": title,
                    "source_system": doc.get("source_system", ""),
                    "source_url": doc.get("source_url", ""),
                    "question_type": "find_material",
                    "difficulty": fm.get("difficulty", "medium"),
                    "language": "vi",
                })
                log.info("  → find_material [%d/%d]: %s",
                         len(buckets["find_material"]), TARGET_COUNTS["find_material"],
                         fm.get("question", "")[:70])

        processed.add(asset_uid)
        _save_checkpoint(buckets, counter, processed)

    # Out-of-scope
    log.info("")
    log.info("Phase 3: Thêm câu hỏi ngoài phạm vi (tiếng Việt)")
    for oos in _OUT_OF_SCOPE_VI[:TARGET_COUNTS["out_of_scope"]]:
        counter += 1
        buckets["out_of_scope"].append({
            "id": f"QVI{counter:03d}",
            "question": oos["question"],
            "ground_truth": oos["ground_truth"],
            "source_asset_uid": None,
            "source_title": None,
            "source_system": None,
            "source_url": None,
            "question_type": "out_of_scope",
            "difficulty": "easy",
            "language": "vi",
        })

    # Kết hợp và đánh số lại
    log.info("")
    log.info("Phase 4: Kết hợp test set cuối cùng")
    final: list[dict] = []
    for qtype, items in buckets.items():
        available = items[:TARGET_COUNTS[qtype]]
        if len(available) < TARGET_COUNTS[qtype]:
            log.warning("  %s: chỉ có %d/%d câu hỏi",
                        qtype, len(available), TARGET_COUNTS[qtype])
        final.extend(available)

    for idx, q in enumerate(final, 1):
        q["id"] = f"QVI{idx:03d}"

    log.info("Tổng câu hỏi: %d", len(final))
    for qtype in TARGET_COUNTS:
        log.info("  %-14s : %d", qtype, sum(1 for q in final if q["question_type"] == qtype))

    if _CHECKPOINT_PATH.exists():
        _CHECKPOINT_PATH.unlink()

    return final

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Tạo bộ câu hỏi đánh giá tiếng Việt (test_set_vi.json)"
    )
    parser.add_argument("--force", action="store_true",
                        help="Tạo lại từ đầu dù test_set_vi.json đã tồn tại")
    parser.add_argument("--resume", action="store_true",
                        help="Tiếp tục từ checkpoint")
    args = parser.parse_args()

    if args.force and _CHECKPOINT_PATH.exists():
        _CHECKPOINT_PATH.unlink()
        log.info("Checkpoint đã xóa (--force)")

    if args.resume and not _CHECKPOINT_PATH.exists():
        log.warning("--resume nhưng không có checkpoint; bắt đầu mới")
        args.resume = False

    if TEST_SET_VI_PATH.exists() and not args.force and not args.resume:
        existing = json.loads(TEST_SET_VI_PATH.read_text())
        log.info("test_set_vi.json đã tồn tại với %d câu hỏi. "
                 "Dùng --force để tạo lại.", len(existing))
        return

    # Kiểm tra API
    try:
        r = requests.get(f"{API_BASE}/health", timeout=10)
        r.raise_for_status()
        log.info("API health check: OK")
    except Exception as exc:
        log.error("Không thể kết nối API tại %s: %s", API_BASE, exc)
        sys.exit(1)

    if not GEMINI_API_KEY and not GROQ_API_KEY:
        log.error("Chưa cấu hình GEMINI_API_KEY hoặc GROQ_API_KEY.")
        sys.exit(1)

    backend = f"Gemini ({GEMINI_MODEL})" if GEMINI_API_KEY else f"Groq ({GROQ_MODEL})"
    log.info("LLM backend: %s | Ngôn ngữ: vi | Seed: 42", backend)

    test_set = build_vi_test_set(resume=args.resume)

    TEST_SET_VI_PATH.write_text(
        json.dumps(test_set, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("Đã lưu %d câu hỏi vào %s", len(test_set), TEST_SET_VI_PATH)


if __name__ == "__main__":
    main()