"""
Chatbot API for OER RAG.

Key behavior:
- Hybrid retrieval (BM25 + kNN) with multilingual E5 embeddings.
- Two-stage hierarchical retrieval (tier 1-2 -> tier 3 expansion).
- Rescue branch that searches tier 3 globally to avoid missed answers.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

_embedding_model = None

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200").rstrip("/")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-base")
USE_E5_PREFIXES = True

STAGE1_MULTIPLIER = int(os.getenv("HIER_STAGE1_MULTIPLIER", "6"))
STAGE2_CHAPTER_LIMIT = int(os.getenv("HIER_STAGE2_CHAPTER_LIMIT", "4"))
STAGE2_DETAIL_PER_CHAPTER = int(os.getenv("HIER_STAGE2_DETAIL_PER_CHAPTER", "8"))
RESCUE_TIER3_K = int(os.getenv("HIER_RESCUE_TIER3_K", "10"))

app = FastAPI(title="OER Chatbot API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AskRequest(BaseModel):
    question: str = Field(..., min_length=3)
    top_k: int = Field(5, ge=1, le=12)
    source_system: Optional[str] = None
    language: Optional[str] = None
    use_hybrid: bool = Field(True)
    use_hierarchical: bool = Field(True)


VI_STOPWORDS = {
    "à",
    "ạ",
    "ấy",
    "bạn",
    "bị",
    "cho",
    "chứ",
    "có",
    "cái",
    "của",
    "cùng",
    "cũng",
    "dạ",
    "để",
    "đi",
    "đó",
    "được",
    "gì",
    "hả",
    "hay",
    "hỏi",
    "là",
    "làm",
    "lại",
    "mà",
    "mình",
    "muốn",
    "này",
    "như",
    "nhỉ",
    "nhé",
    "nào",
    "nè",
    "nha",
    "ơi",
    "rồi",
    "sao",
    "tôi",
    "thì",
    "thế",
    "trên",
    "trong",
    "từ",
    "và",
    "vậy",
    "về",
    "với",
    "ê",
    "uhm",
    "um",
    "ừ",
    "ờ",
    "vâng",
    "xin",
    "giúp",
    "biết",
    "tui",
}
EN_STOPWORDS = {
    "a",
    "an",
    "the",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "have",
    "has",
    "had",
    "do",
    "does",
    "did",
    "will",
    "would",
    "could",
    "should",
    "may",
    "might",
    "must",
    "to",
    "of",
    "in",
    "for",
    "on",
    "with",
    "at",
    "by",
    "from",
    "as",
    "into",
    "through",
    "during",
    "before",
    "after",
    "about",
    "all",
    "each",
    "few",
    "more",
    "most",
    "other",
    "some",
    "such",
    "no",
    "nor",
    "not",
    "only",
    "own",
    "same",
    "so",
    "than",
    "too",
    "very",
    "i",
    "me",
    "my",
    "we",
    "our",
    "you",
    "your",
    "he",
    "she",
    "it",
    "they",
    "them",
    "what",
    "which",
    "who",
    "this",
    "that",
    "these",
    "those",
    "please",
    "help",
    "want",
    "know",
    "tell",
    "hey",
    "hi",
    "hello",
}


def _get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        from sentence_transformers import SentenceTransformer

        _embedding_model = SentenceTransformer(EMBEDDING_MODEL)
    return _embedding_model


def _detect_lang(text: str) -> str:
    vi_chars = set("àáâãèéêìíòóôõùúýăđơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ")
    lowered = (text or "").lower()
    return "vi" if any(ch in vi_chars for ch in lowered) else "en"


def _extract_keywords(query: str) -> str:
    lang = _detect_lang(query)
    stopwords = VI_STOPWORDS if lang == "vi" else EN_STOPWORDS
    tokens = re.findall(r"[\w\u00C0-\u024F\u1E00-\u1EFF]+", query.lower())
    keywords = [t for t in tokens if t not in stopwords and len(t) > 1]
    if len(keywords) < 2:
        return query.strip()
    return " ".join(keywords)


def _translate_query_for_search(query: str) -> str:
    if _detect_lang(query) != "vi":
        return query
    if LLM_PROVIDER == "groq" and not GROQ_API_KEY:
        return query
    if LLM_PROVIDER != "groq" and not GEMINI_API_KEY:
        return query

    prompt = (
        "Translate this Vietnamese question to natural English for search retrieval.\n"
        "Output only the translated question.\n\n"
        f"Vietnamese: {query}\nEnglish:"
    )
    try:
        if LLM_PROVIDER == "groq":
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": GROQ_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0,
                    "max_tokens": 96,
                },
                timeout=6,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()

        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=6,
        )
        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        return query


def _embed_query(query: str) -> List[float]:
    model = _get_embedding_model()
    text = _extract_keywords(query)
    if USE_E5_PREFIXES and "e5" in EMBEDDING_MODEL.lower():
        text = f"query: {text}"
    vector = model.encode(text, convert_to_numpy=True, normalize_embeddings=True)
    return vector.tolist()


def _build_filters(
    source_system: Optional[str],
    language: Optional[str],
    tiers: Optional[List[int]] = None,
    chapter_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    filters: List[Dict[str, Any]] = []
    if source_system:
        filters.append({"term": {"source_system": source_system}})
    if language:
        filters.append({"term": {"lang": language}})
    if tiers:
        filters.append({"terms": {"chunk_tier": tiers}})
    if chapter_ids:
        filters.append({"terms": {"chapter_id": chapter_ids}})
    return filters


def _build_should_clauses(question: str, translated: str, keywords: str) -> List[Dict[str, Any]]:
    clauses: List[Dict[str, Any]] = []
    queries = []
    if question.strip():
        queries.append((question.strip(), 1.2))
    if translated.strip() and translated.strip() != question.strip():
        queries.append((translated.strip(), 1.0))
    if keywords.strip():
        queries.append((keywords.strip(), 0.8))

    for q, boost in queries:
        clauses.append(
            {
                "multi_match": {
                    "query": q,
                    "fields": ["chunk_text^1.0", "chapter_title^1.8", "section_title^1.5"],
                    "fuzziness": "AUTO",
                    "boost": boost,
                }
            }
        )
    return clauses


def _es_hybrid_search(
    question: str,
    top_k: int,
    filters: List[Dict[str, Any]],
    use_hybrid: bool = True,
) -> List[Dict[str, Any]]:
    query_lang = _detect_lang(question)
    translated = _translate_query_for_search(question) if query_lang == "vi" else question
    keywords = _extract_keywords(translated if translated != question else question)

    should_clauses = _build_should_clauses(question, translated, keywords)
    bool_query = {
        "bool": {
            "filter": filters,
            "should": should_clauses,
            "minimum_should_match": 1 if should_clauses else 0,
        }
    }

    if not use_hybrid:
        body = {
            "size": top_k,
            "_source": True,
            "query": bool_query,
        }
        resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=body, timeout=25)
        resp.raise_for_status()
        return resp.json().get("hits", {}).get("hits", [])

    try:
        query_vector = _embed_query(question)
        knn_k = max(top_k * 3, 20)
        knn_candidates = 120 if query_lang == "vi" else 80
        body: Dict[str, Any] = {
            "size": top_k,
            "_source": True,
            "query": bool_query,
            "knn": {
                "field": "embedding",
                "query_vector": query_vector,
                "k": knn_k,
                "num_candidates": knn_candidates,
            },
            "rank": {"rrf": {"window_size": 120 if query_lang == "vi" else 80, "rank_constant": 20}},
        }
        if filters:
            body["knn"]["filter"] = filters

        resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=body, timeout=35)
        resp.raise_for_status()
        return resp.json().get("hits", {}).get("hits", [])
    except Exception:
        body = {"size": top_k, "_source": True, "query": bool_query}
        resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=body, timeout=25)
        resp.raise_for_status()
        return resp.json().get("hits", {}).get("hits", [])


def _extract_relevant_chapters(hits: List[Dict[str, Any]], limit: int = STAGE2_CHAPTER_LIMIT) -> List[str]:
    chapters: List[Tuple[str, float]] = []
    seen = set()
    for hit in hits:
        src = hit.get("_source", {})
        chapter_id = src.get("chapter_id")
        if not chapter_id or chapter_id in seen:
            continue
        seen.add(chapter_id)
        chapters.append((chapter_id, float(hit.get("_score", 0.0))))
    chapters.sort(key=lambda x: x[1], reverse=True)
    return [cid for cid, _ in chapters[:limit]]


def _search_hierarchical(
    question: str,
    top_k: int,
    source_system: Optional[str],
    language: Optional[str],
    use_hybrid: bool = True,
) -> List[Dict[str, Any]]:
    stage1_size = max(top_k * STAGE1_MULTIPLIER, 24)
    stage1_filters = _build_filters(source_system, language, tiers=[1, 2])
    stage1_hits = _es_hybrid_search(question, stage1_size, stage1_filters, use_hybrid=use_hybrid)

    # Old flat indices may not have tier field; fallback if stage1 returns empty.
    if not stage1_hits:
        flat_filters = _build_filters(source_system, language, tiers=None)
        stage1_hits = _es_hybrid_search(question, stage1_size, flat_filters, use_hybrid=use_hybrid)

    chapter_ids = _extract_relevant_chapters(stage1_hits)
    stage2_hits: List[Dict[str, Any]] = []
    if chapter_ids:
        stage2_filters = _build_filters(source_system, language, tiers=[3], chapter_ids=chapter_ids)
        stage2_size = max(top_k * STAGE2_DETAIL_PER_CHAPTER, 16)
        stage2_hits = _es_hybrid_search(question, stage2_size, stage2_filters, use_hybrid=use_hybrid)

    rescue_filters = _build_filters(source_system, language, tiers=[3])
    rescue_hits = _es_hybrid_search(question, RESCUE_TIER3_K, rescue_filters, use_hybrid=use_hybrid)

    return _dedupe_hits(stage1_hits + stage2_hits + rescue_hits)


def _search_flat(
    question: str,
    top_k: int,
    source_system: Optional[str],
    language: Optional[str],
    use_hybrid: bool = True,
) -> List[Dict[str, Any]]:
    filters = _build_filters(source_system, language, tiers=None)
    size = max(top_k * 4, 20)
    return _es_hybrid_search(question, size, filters, use_hybrid=use_hybrid)


def _dedupe_hits(hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique: Dict[str, Dict[str, Any]] = {}
    for hit in hits:
        src = hit.get("_source", {})
        key = src.get("chunk_id") or hit.get("_id")
        if not key:
            continue
        existing = unique.get(key)
        if existing is None or float(hit.get("_score", 0.0)) > float(existing.get("_score", 0.0)):
            unique[key] = hit
    return list(unique.values())


def _rank_hits_by_es_score(hits: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    if not hits:
        return []
    ranked = sorted(hits, key=lambda h: float(h.get("_score", 0.0)), reverse=True)
    return ranked[:top_k]


def _build_context(question: str, hits: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    contexts: List[Dict[str, Any]] = []
    for hit in hits:
        src = hit.get("_source", {})
        text = src.get("chunk_text", "")
        if not text:
            continue
        contexts.append(
            {
                "text": text,
                "chunk_id": src.get("chunk_id"),
                "resource_uid": src.get("resource_uid"),
                "page_no": src.get("page_no"),
                "lang": src.get("lang"),
                "chunk_tier": src.get("chunk_tier", 3),
                "chapter_id": src.get("chapter_id"),
                "chapter_title": src.get("chapter_title"),
                "chapter_number": src.get("chapter_number"),
                "section_id": src.get("section_id"),
                "section_title": src.get("section_title"),
                "section_number": src.get("section_number"),
                "source_system": src.get("source_system"),
            }
        )
        if len(contexts) >= top_k:
            break
    return contexts


def _build_prompt(question: str, contexts: List[Dict[str, Any]]) -> str:
    lang = _detect_lang(question)
    refs = []
    for idx, c in enumerate(contexts, start=1):
        tier = c.get("chunk_tier")
        chapter = c.get("chapter_title")
        section = c.get("section_title")
        page = c.get("page_no")
        label = f"[Source {idx}]"
        meta = []
        if tier is not None:
            meta.append(f"tier={tier}")
        if chapter:
            meta.append(f"chapter={chapter}")
        if section:
            meta.append(f"section={section}")
        if page:
            meta.append(f"page={page}")
        refs.append(f"{label} ({', '.join(meta)})\n{c['text']}")
    context = "\n\n".join(refs)

    if lang == "vi":
        return (
            "Bạn là trợ lý học tập trong hệ thống thư viện OER.\n"
            "Hãy trả lời chính xác theo tài liệu cung cấp, diễn giải rõ ràng, không chép nguyên văn dài.\n"
            "Nếu thiếu dữ liệu, nêu rõ phần chưa đủ.\n\n"
            f"--- TÀI LIỆU ---\n{context}\n--- HẾT ---\n\n"
            f"Câu hỏi: {question}\n\n"
            "Trả lời bằng tiếng Việt, có trích nguồn [Source X]."
        )

    return (
        "You are a learning assistant for an OER library system.\n"
        "Answer based on the provided sources, paraphrase clearly, avoid long verbatim copying.\n"
        "If evidence is insufficient, explicitly say what is missing.\n\n"
        f"--- SOURCES ---\n{context}\n--- END ---\n\n"
        f"Question: {question}\n\n"
        "Answer in English and cite sources as [Source X]."
    )


def _generate_answer(prompt: str) -> str:
    provider = (LLM_PROVIDER or "gemini").lower()
    if provider == "groq":
        if not GROQ_API_KEY:
            return "Missing GROQ_API_KEY."
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,
                "max_tokens": 2048,
            },
            timeout=90,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()

    if not GEMINI_API_KEY:
        return "Missing GEMINI_API_KEY."
    resp = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
        json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2048},
        },
        timeout=90,
    )
    resp.raise_for_status()
    cands = resp.json().get("candidates", [])
    return cands[0]["content"]["parts"][0]["text"].strip() if cands else "No answer generated."


def _search(
    question: str,
    top_k: int,
    source_system: Optional[str],
    language: Optional[str],
    use_hybrid: bool,
    use_hierarchical: bool,
) -> List[Dict[str, Any]]:
    if use_hierarchical:
        return _search_hierarchical(question, top_k, source_system, language, use_hybrid=use_hybrid)
    return _search_flat(question, top_k, source_system, language, use_hybrid=use_hybrid)


def _search_expand_chapter(
    chapter_id: str,
    top_k: int = 20,
    source_system: Optional[str] = None,
    language: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = _build_filters(source_system, language, tiers=[3], chapter_ids=[chapter_id])
    body = {
        "size": top_k,
        "_source": True,
        "query": {"bool": {"filter": filters, "must": [{"match_all": {}}]}},
        "sort": [
            {"section_page_start": {"order": "asc", "missing": "_last", "unmapped_type": "integer"}},
            {"chunk_order": {"order": "asc", "missing": "_last", "unmapped_type": "integer"}},
        ],
    }
    resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=body, timeout=20)
    resp.raise_for_status()
    return resp.json().get("hits", {}).get("hits", [])


@app.get("/api/health")
async def health() -> Dict[str, Any]:
    try:
        resp = requests.get(f"{ES_HOST}/_cluster/health", timeout=5)
        resp.raise_for_status()
        es_health = resp.json().get("status", "unknown")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Elasticsearch unavailable: {exc}") from exc
    return {
        "status": "ok",
        "elasticsearch": es_health,
        "index": ES_INDEX,
        "llm_provider": LLM_PROVIDER,
        "hierarchical_search": True,
    }


@app.get("/api/expand/{chapter_id}")
async def expand_chapter(chapter_id: str, top_k: int = 20) -> Dict[str, Any]:
    try:
        hits = _search_expand_chapter(chapter_id, top_k=max(1, min(top_k, 100)))
        contexts = _build_context(chapter_id, hits, top_k=max(1, min(top_k, 100)))
        return {"chapter_id": chapter_id, "results": contexts, "count": len(contexts)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/ask")
async def ask_api(payload: AskRequest) -> Dict[str, Any]:
    try:
        hits = _search(
            question=payload.question,
            top_k=payload.top_k,
            source_system=payload.source_system,
            language=payload.language,
            use_hybrid=payload.use_hybrid,
            use_hierarchical=payload.use_hierarchical,
        )
        ranked = _rank_hits_by_es_score(hits, top_k=max(payload.top_k * 3, payload.top_k))
        contexts = _build_context(payload.question, ranked, payload.top_k)
        if not contexts:
            lang = _detect_lang(payload.question)
            msg = "Không tìm thấy ngữ cảnh phù hợp trong kho dữ liệu." if lang == "vi" else "No relevant context found in the database."
            return {
                "question": payload.question,
                "answer": msg,
                "contexts": [],
                "search_mode": "hierarchical" if payload.use_hierarchical else "flat",
            }

        prompt = _build_prompt(payload.question, contexts)
        answer = _generate_answer(prompt)
        return {
            "question": payload.question,
            "answer": answer,
            "contexts": contexts,
            "sources": contexts,
            "search_mode": "hierarchical" if payload.use_hierarchical else "flat",
        }
    except requests.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Upstream HTTP error: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
