"""
Chatbot-only API for OER RAG.
Hybrid Search: BM25 (text) + kNN (vector) with keyword extraction.
"""

import os
import re
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Lazy load embedding model
_embedding_model = None

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200").rstrip("/")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "oer-lakehouse")
MINIO_PUBLIC_BASE_URL = os.getenv("MINIO_PUBLIC_BASE_URL", "http://localhost:19000").rstrip("/")

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# Embedding config (same model as Silver layer) - E5-Base for cross-lingual
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-base")
EMBEDDING_DIM = 768
USE_E5_PREFIXES = True  # E5 models need query: prefix

# Hybrid search weights - adjusted per language
# Vietnamese queries: rely more on kNN (multilingual embedding handles cross-language)
# English queries: BM25 works well for exact text match
BM25_WEIGHT_EN = float(os.getenv("BM25_WEIGHT_EN", "0.5"))
KNN_WEIGHT_EN = float(os.getenv("KNN_WEIGHT_EN", "0.5"))
BM25_WEIGHT_VI = float(os.getenv("BM25_WEIGHT_VI", "0.2"))  # Lower for cross-language
KNN_WEIGHT_VI = float(os.getenv("KNN_WEIGHT_VI", "0.8"))    # Higher for semantic match

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
    use_hybrid: bool = Field(True, description="Use hybrid search (BM25 + kNN)")


def _get_embedding_model():
    """Lazy load embedding model."""
    global _embedding_model
    if _embedding_model is None:
        from sentence_transformers import SentenceTransformer
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL)
    return _embedding_model


def _detect_lang(text: str) -> str:
    vi_chars = set("àáâãèéêìíòóôõùúýăđơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ")
    return "vi" if any(ch in vi_chars for ch in text.lower()) else "en"


# Stopwords for keyword extraction
VI_STOPWORDS = {
    "à", "ạ", "ấy", "bạn", "bị", "cho", "chứ", "có", "cái", "của", "cùng", "cũng",
    "dạ", "để", "đi", "đó", "được", "gì", "hả", "hay", "hỏi", "là", "làm", "lại",
    "mà", "mình", "muốn", "này", "như", "nhỉ", "nhé", "nào", "nè", "nha", "ơi",
    "rồi", "sao", "tôi", "thì", "thế", "trên", "trong", "từ", "và", "vậy", "về",
    "với", "ê", "uhm", "um", "ừ", "ờ", "vâng", "dạ", "xin", "giúp", "biết", "tui",
}
EN_STOPWORDS = {
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being", "have",
    "has", "had", "do", "does", "did", "will", "would", "could", "should", "may",
    "might", "must", "shall", "can", "need", "dare", "ought", "used", "to", "of",
    "in", "for", "on", "with", "at", "by", "from", "as", "into", "through", "during",
    "before", "after", "above", "below", "between", "under", "again", "further",
    "then", "once", "here", "there", "when", "where", "why", "how", "all", "each",
    "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only",
    "own", "same", "so", "than", "too", "very", "just", "also", "now", "i", "me",
    "my", "myself", "we", "our", "you", "your", "he", "him", "his", "she", "her",
    "it", "its", "they", "them", "their", "what", "which", "who", "whom", "this",
    "that", "these", "those", "am", "like", "um", "uh", "well", "please", "help",
    "want", "know", "tell", "about", "hey", "hi", "hello",
}


def _extract_keywords(query: str) -> str:
    """Extract meaningful keywords from query, removing filler words."""
    # Skip keyword extraction for short queries (keep natural language)
    if len(query.split()) <= 5:
        return query
    
    lang = _detect_lang(query)
    stopwords = VI_STOPWORDS if lang == "vi" else EN_STOPWORDS
    
    # Normalize and tokenize
    text = query.lower().strip()
    # Keep alphanumeric and Vietnamese chars
    tokens = re.findall(r'[\w\u00C0-\u024F\u1E00-\u1EFF]+', text)
    
    # Filter stopwords and short tokens
    keywords = [t for t in tokens if t not in stopwords and len(t) > 1]
    
    # If too aggressive, fallback to original
    if len(keywords) < 2:
        return query
    
    return " ".join(keywords)


def _extract_source_url_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"Saylor URL:\s*(https?://[^\s]+)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"(https?://[^\s)]+)", text)
    return m.group(1).strip() if m else None


def _build_public_minio_url(asset_path: Optional[str]) -> Optional[str]:
    if not asset_path:
        return None
    key = str(asset_path).strip().lstrip("/")
    if not key:
        return None
    return f"{MINIO_PUBLIC_BASE_URL}/{MINIO_BUCKET}/{key}"


def _embed_query(query: str) -> List[float]:
    """Embed query using sentence-transformers.
    
    E5 models require 'query:' prefix for search queries.
    See: https://huggingface.co/intfloat/multilingual-e5-base
    """
    model = _get_embedding_model()
    # Extract keywords before embedding for better semantic match
    clean_query = _extract_keywords(query)
    
    # Add E5 query prefix if using E5 model
    if USE_E5_PREFIXES and "e5" in EMBEDDING_MODEL.lower():
        clean_query = f"query: {clean_query}"
    
    embedding = model.encode(clean_query, convert_to_numpy=True, normalize_embeddings=True)
    return embedding.tolist()


def _es_hybrid_search(
    question: str,
    top_k: int,
    source_system: Optional[str],
    language: Optional[str],
    use_hybrid: bool = True,
) -> List[Dict[str, Any]]:
    """Hybrid search on flat chunk index.

    - BM25 branch: lexical match on chunk text/title/chapter title.
    - Vector branch: cosine similarity on embedding.
    - Fusion: weighted score merge in application layer (no RRF license dependency).
    """
    # Query enhancement: extract subject keywords
    search_query = question
    if re.search(r'\b(book|textbook|sách|giáo trình|tài liệu)\b', question.lower()):
        # Extract subject from "book of X" or "X textbook"
        subject_match = re.search(r'\b(of|về|cho)\s+(\w+)', question.lower())
        if subject_match:
            subject = subject_match.group(2)
            # Expand common subjects
            expansions = {
                'math': 'mathematics algebra calculus',
                'toán': 'toán học mathematics algebra calculus',
                'database': 'database SQL data management',
            }
            search_query = expansions.get(subject, question)
    
    query_lang = _detect_lang(question)
    bm25_weight = BM25_WEIGHT_VI if query_lang == "vi" else BM25_WEIGHT_EN
    knn_weight = KNN_WEIGHT_VI if query_lang == "vi" else KNN_WEIGHT_EN

    source_fields = [
        "chunk_id", "resource_uid", "asset_uid", "chunk_text", "page_no", "lang",
        "title", "source_url", "minio_url", "asset_path", "chapter_title", "source_system",
    ]

    filters: List[Dict[str, Any]] = []
    if language:
        filters.append({"term": {"lang": language}})
    if source_system:
        filters.append({"term": {"source_system": source_system}})

    bm25_query: Dict[str, Any] = {
        "bool": {
            "filter": filters,
            "should": [
                {"match": {"chunk_text": {"query": search_query, "fuzziness": "AUTO"}}},
                {"match": {"title": {"query": search_query, "boost": 10.0}}},
                {"match": {"chapter_title": {"query": search_query, "boost": 5.0}}},
            ],
            "minimum_should_match": 1,
        }
    }

    bm25_body = {"size": max(top_k * 3, top_k), "_source": source_fields, "query": bm25_query}
    bm25_resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=bm25_body, timeout=20)
    bm25_resp.raise_for_status()
    bm25_hits = bm25_resp.json().get("hits", {}).get("hits", [])

    if not use_hybrid:
        return bm25_hits[:top_k]

    query_vector = _embed_query(question)
    vector_base_query: Dict[str, Any] = {"match_all": {}}
    if filters:
        vector_base_query = {"bool": {"filter": filters}}

    vector_body = {
        "size": max(top_k * 3, top_k),
        "_source": source_fields,
        "query": {
            "script_score": {
                "query": vector_base_query,
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                    "params": {"query_vector": query_vector},
                },
            }
        },
    }
    vector_resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=vector_body, timeout=30)
    vector_resp.raise_for_status()
    vector_hits = vector_resp.json().get("hits", {}).get("hits", [])

    def _normalized_score_map(hits: List[Dict[str, Any]]) -> Dict[str, float]:
        pairs: List[tuple[str, float]] = []
        for h in hits:
            doc_id = h.get("_id") or str(h.get("_source", {}).get("chunk_id") or "")
            if not doc_id:
                continue
            raw = h.get("_score")
            score = float(raw) if raw is not None else 0.0
            pairs.append((doc_id, score))
        if not pairs:
            return {}
        max_score = max(score for _, score in pairs)
        if max_score <= 0:
            return {doc_id: 0.0 for doc_id, _ in pairs}
        return {doc_id: score / max_score for doc_id, score in pairs}

    bm25_norm = _normalized_score_map(bm25_hits)
    vector_norm = _normalized_score_map(vector_hits)

    merged: Dict[str, Dict[str, Any]] = {}
    for hit in bm25_hits + vector_hits:
        doc_id = hit.get("_id") or str(hit.get("_source", {}).get("chunk_id") or "")
        if not doc_id:
            continue
        if doc_id not in merged:
            merged[doc_id] = hit
        fused_score = (bm25_weight * bm25_norm.get(doc_id, 0.0)) + (knn_weight * vector_norm.get(doc_id, 0.0))
        merged[doc_id]["_score"] = fused_score

    ranked = sorted(merged.values(), key=lambda h: h.get("_score", 0.0), reverse=True)
    return ranked[:top_k]


def _build_context(question: str, hits: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    """Build context from flat chunk-level search results."""
    contexts: List[Dict[str, Any]] = []
    noise_patterns = []
    
    # Generic titles to skip (metadata quality filter)
    generic_titles = {"part 1", "part 2", "part 3", "part 4", "part 5", "part 6", 
                      "chapter 1", "chapter 2", "section 1", "unknown", "unknown book"}
    
    for hit in hits:
        src = hit.get("_source", {})
        
        # Flat structure: each hit is a chunk
        text = src.get("chunk_text", "")
        if not text:
            continue
        text_norm = text.strip().lower()
        title_norm = str(src.get("title") or src.get("chapter_title") or "").strip().lower()

        # Skip generic/low-quality titles
        if title_norm in generic_titles:
            continue
        
        # Skip code-heavy chunks (>50% looks like code)
        code_indicators = len(re.findall(r'\b(do|enddo|call|function|return|sum=|def |class |import |#include)', text_norm))
        if code_indicators > 5 or (code_indicators > 0 and len(text_norm) < 300):
            continue

        # Skip noisy/boilerplate chunks.
        if len(text_norm) < 60:
            continue
        if any(re.search(p, text_norm) for p in noise_patterns):
            continue
        if title_norm and any(re.search(p, title_norm) for p in noise_patterns):
            continue

        source_url = (
            src.get("source_url")
            or src.get("minio_url")
            or _build_public_minio_url(src.get("asset_path"))
            or _extract_source_url_from_text(text)
        )
        title = (
            src.get("title")
            or src.get("chapter_title")
            or src.get("source_system")
            or src.get("resource_uid")
            or "Unknown"
        )
            
        contexts.append({
            "text": text,
            "chunk_id": src.get("chunk_id"),
            "resource_uid": src.get("resource_uid"),
            "page_no": src.get("page_no"),
            "lang": src.get("lang"),
            "title": title,
            "source_url": source_url,
            "asset_uid": src.get("asset_uid"),
            "minio_url": src.get("minio_url"),
            "retrieval_score": hit.get("_score"),
        })
        
        if len(contexts) >= top_k:
            break
    
    return contexts


def _build_prompt(question: str, contexts: List[Dict[str, Any]]) -> str:
    lang = _detect_lang(question)
    parts = []
    for i, c in enumerate(contexts, 1):
        # Simplified citation (no title/URL in flat structure yet)
        cite = f"[Chunk {c.get('chunk_id', i)}]"
        if c.get("page_no"):
            cite += f" (Page {c['page_no']})"
        parts.append(f"{cite}\n{c['text']}")
    context = "\n\n".join(parts)
    if lang == "vi":
        return (
            "Bạn là trợ lý thư viện chỉ trả lời dựa HOÀN TOÀN trên tài liệu được cung cấp.\n\n"

            "QUY TẮC BẮT BUỘC:\n"
            "1. CHỈ sử dụng thông tin có trong CONTEXT dưới đây.\n"
            "2. NẾU CONTEXT không liên quan đến câu hỏi, BẮT BUỘC trả lời ĐÚNG NGUYÊN VĂN:\n"
            "   'Không tìm thấy thông tin phù hợp trong kho tài liệu. Vui lòng thử câu hỏi khác.'\n"
            "3. KHÔNG suy đoán, KHÔNG thêm thông tin từ kiến thức chung.\n"
            "4. KHÔNG cố gắng kết nối câu hỏi với context không liên quan.\n\n"

            "CÁCH ĐÁNH GIÁ CONTEXT:\n"
            "- Context CÓ LIÊN QUAN: Đề cập trực tiếp đến chủ đề câu hỏi\n"
            "- Context KHÔNG LIÊN QUAN: Đề cập chủ đề khác hoàn toàn\n"
            "  → Nếu không liên quan, dừng ngay và trả lời 'Không tìm thấy thông tin...'\n\n"

            "NẾU CONTEXT CÓ LIÊN QUAN, hãy:\n"
            "1. Tóm tắt thông tin chính từ context\n"
            "2. Giải thích rõ ràng, dễ hiểu\n"
            "3. Trích dẫn nguồn bằng [Nguồn X]\n\n"

            f"--- TÀI LIỆU ---\n{context}\n--- HẾT ---\n\n"
            f"Câu hỏi: {question}\n\n"
            "Trả lời (kiểm tra liên quan trước khi trả lời):"
        )
    else:
        return (
            "You are an intelligent learning assistant in an Open Educational Resources (OER) library system.\n"
            "Your goal is to help learners understand concepts clearly, simply, and in a structured way.\n\n"

            "ANSWERING PRINCIPLES:\n"
            "- Prioritize information from the provided documents\n"
            "- Do NOT copy text verbatim from documents - paraphrase and synthesize in your own words\n"
            "- STRICTLY do NOT add external knowledge beyond the provided documents\n"
            "- If the documents are insufficient or not directly relevant, you MUST answer exactly: \"No relevant context found in the database.\"\n"
            "- Ignore document fragments that are cut off mid-sentence or lack context\n\n"

            "HOW TO RESPOND:\n"
            "1. START with a general overview sentence about the topic\n"
            "2. Explain concepts clearly as if teaching a beginner\n"
            "3. Combine relevant information from multiple sources when available\n"
            "4. Structure your answer logically with clear paragraphs\n"
            "5. Include examples when helpful\n"
            "6. Cite sources using [Source X] (do not include page numbers unless explicitly available)\n\n"

            f"--- REFERENCE DOCUMENTS ---\n{context}\n--- END OF DOCUMENTS ---\n\n"
            f"Question: {question}\n\n"
            "Provide a detailed, clear answer in English (do NOT copy verbatim from documents):"
        )


def _generate_answer(prompt: str) -> str:
    provider = (LLM_PROVIDER or "gemini").lower()
    
    # Try Groq first, fallback to Gemini on rate limit
    if provider == "groq":
        if not GROQ_API_KEY:
            return "Missing GROQ_API_KEY."
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={"model": GROQ_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 2048},
                timeout=90,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
        except requests.HTTPError as e:
            # Fallback to Gemini on rate limit (429) or server errors (5xx)
            if e.response.status_code == 429 or e.response.status_code >= 500:
                if GEMINI_API_KEY:
                    provider = "gemini"  # Switch to Gemini
                else:
                    return f"Groq API rate limit exceeded. Please wait a moment and try again."
            else:
                raise  # Re-raise other errors
    
    # Gemini API (default or fallback)
    if not GEMINI_API_KEY:
        return "Missing GEMINI_API_KEY."
    resp = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
        json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2048},
        },
        timeout=90,
    )
    resp.raise_for_status()
    cands = resp.json().get("candidates", [])
    return cands[0]["content"]["parts"][0]["text"].strip() if cands else "No answer generated."


@app.get("/api/health")
async def health() -> Dict[str, Any]:
    try:
        resp = requests.get(f"{ES_HOST}/_cluster/health", timeout=5)
        resp.raise_for_status()
        health = resp.json().get("status", "unknown")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Elasticsearch unavailable: {exc}") from exc
    return {"status": "ok", "elasticsearch": health, "index": ES_INDEX, "llm_provider": LLM_PROVIDER, "hybrid_search": True}


@app.post("/api/ask")
async def ask_api(payload: AskRequest) -> Dict[str, Any]:
    try:
        # Use hybrid search (BM25 + kNN) by default
        hits = _es_hybrid_search(
            payload.question,
            payload.top_k,
            payload.source_system,
            payload.language,
            use_hybrid=payload.use_hybrid,
        )
        contexts = _build_context(payload.question, hits, payload.top_k)
        
        # Check if retrieval quality is too low
        if not contexts:
            lang = _detect_lang(payload.question)
            no_result_msg = "Không tìm thấy ngữ cảnh phù hợp trong kho dữ liệu." if lang == "vi" else "No relevant context found in the database."
            return {"question": payload.question, "answer": no_result_msg, "contexts": [], "search_mode": "hybrid" if payload.use_hybrid else "bm25"}
        
        # Check average retrieval score - reject if too low (likely irrelevant)
        # Note: scores are normalized (0-1 range) after hybrid fusion
        avg_score = sum(c.get("retrieval_score", 0) for c in contexts) / len(contexts)
        if avg_score < 0.15:  # Threshold for minimum relevance (normalized score)
            lang = _detect_lang(payload.question)
            low_quality_msg = (
                "Không tìm thấy tài liệu liên quan đến câu hỏi của bạn. Vui lòng thử câu hỏi cụ thể hơn hoặc sử dụng từ khóa tiếng Anh."
                if lang == "vi" else
                "No relevant documents found for your question. Please try a more specific query or use English keywords."
            )
            return {"question": payload.question, "answer": low_quality_msg, "contexts": contexts, "search_mode": "low_quality_retrieval"}
        
        prompt = _build_prompt(payload.question, contexts)
        answer = _generate_answer(prompt)
        return {
            "question": payload.question,
            "answer": answer,
            "contexts": contexts,
            "search_mode": "hybrid" if payload.use_hybrid else "bm25",
        }
    except requests.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Upstream HTTP error: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
