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


def _translate_query_for_search(query: str) -> str:
    """Use LLM to translate Vietnamese query to English for better search.
    
    Only translates if query is Vietnamese and LLM is available.
    Returns original query if translation fails.
    """
    if _detect_lang(query) != "vi":
        return query
    
    # Skip if no API key
    if LLM_PROVIDER == "groq" and not GROQ_API_KEY:
        return query
    if LLM_PROVIDER == "gemini" and not GEMINI_API_KEY:
        return query
    
    try:
        prompt = f"""Translate this Vietnamese question to English. Only output the English translation, nothing else.

Vietnamese: {query}
English:"""
        
        if LLM_PROVIDER == "groq":
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
                json={
                    "model": GROQ_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 100,
                    "temperature": 0,
                },
                timeout=5,
            )
            resp.raise_for_status()
            translated = resp.json()["choices"][0]["message"]["content"].strip()
        else:  # gemini
            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=5,
            )
            resp.raise_for_status()
            translated = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        
        print(f"[Query Translation] '{query}' -> '{translated}'")
        return translated
        
    except Exception as e:
        print(f"[Query Translation] Failed: {e}")
        return query


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
    """Hybrid search: BM25 + kNN with RRF fusion (SIMPLIFIED for flat index).
    
    New architecture: Direct chunk-level search (no nested structure).
    - Each document = 1 chunk (chunk_id as _id)
    - Faster queries, simpler mapping
    """
    query_lang = _detect_lang(question)
    
    # Translate Vietnamese queries to English for better BM25 matching
    search_query = _translate_query_for_search(question) if query_lang == "vi" else question
    
    filters: List[Dict[str, Any]] = []
    if language:
        filters.append({"term": {"lang": language}})

    # BM25 query (text search on chunk_text)
    bm25_query = {
        "bool": {
            "filter": filters,
            "should": [
                {
                    "match": {
                        "chunk_text": {
                            "query": search_query,
                            "fuzziness": "AUTO",
                        }
                    }
                },
            ],
            "minimum_should_match": 1,
        }
    }

    # If not using hybrid, use BM25 only
    if not use_hybrid:
        body = {
            "size": top_k,
            "_source": ["chunk_id", "resource_uid", "chunk_text", "page_no", "lang"],
            "query": bm25_query,
        }
        resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=body, timeout=20)
        resp.raise_for_status()
        return resp.json().get("hits", {}).get("hits", [])

    # Hybrid: Use ES RRF (Reciprocal Rank Fusion) - Flat index structure
    try:
        query_vector = _embed_query(question)
        
        # For cross-language queries, increase kNN candidates
        knn_k = top_k * 3 if query_lang == "vi" else top_k * 2
        knn_candidates = 100 if query_lang == "vi" else 50
        
        # ES 8.8+ RRF with flat index (direct chunk search)
        body = {
            "size": top_k,
            "_source": ["chunk_id", "resource_uid", "chunk_text", "page_no", "lang"],
            "query": bm25_query,
            "knn": {
                "field": "embedding",
                "query_vector": query_vector,
                "k": knn_k,
                "num_candidates": knn_candidates,
                "filter": filters if filters else None,
            },
            "rank": {
                "rrf": {
                    "window_size": 100 if query_lang == "vi" else 50,
                    "rank_constant": 20,
                }
            },
        }
        
        # Remove None filter
        if body["knn"]["filter"] is None:
            del body["knn"]["filter"]
        
        resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=body, timeout=30)
        resp.raise_for_status()
        return resp.json().get("hits", {}).get("hits", [])
        
    except Exception as e:
        # Fallback to BM25 if hybrid fails
        print(f"[Hybrid Search] Fallback to BM25: {e}")
        body = {
            "size": top_k,
            "_source": ["chunk_id", "resource_uid", "chunk_text", "page_no", "lang"],
            "query": bm25_query,
        }
        resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=body, timeout=20)
        resp.raise_for_status()
        return resp.json().get("hits", {}).get("hits", [])


def _build_context(question: str, hits: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    """Build context from flat chunk-level search results."""
    contexts: List[Dict[str, Any]] = []
    for hit in hits:
        src = hit.get("_source", {})
        
        # Flat structure: each hit is a chunk
        text = src.get("chunk_text", "")
        if not text:
            continue
            
        contexts.append({
            "text": text,
            "chunk_id": src.get("chunk_id"),
            "resource_uid": src.get("resource_uid"),
            "page_no": src.get("page_no"),
            "lang": src.get("lang"),
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
            "Bạn là một trợ lý học tập thông minh trong hệ thống thư viện học liệu mở (OER).\n"
            "Bạn giúp người học hiểu bài một cách rõ ràng, dễ tiếp cận và có hệ thống.\n\n"

            "NGUYÊN TẮC TRẢ LỜI:\n"
            "- Ưu tiên sử dụng thông tin từ tài liệu được cung cấp\n"
            "- KHÔNG copy nguyên văn từ tài liệu - hãy diễn giải và tổng hợp lại bằng lời của bạn\n"
            "- Không tự ý thêm kiến thức ngoài nếu tài liệu không đề cập (trừ khi cần giải thích thêm, và phải nói rõ)\n"
            "- Nếu tài liệu không đủ, hãy nói rõ phần thiếu\n"
            "- Bỏ qua các đoạn text bị cắt giữa chừng hoặc không có ngữ cảnh\n\n"

            "CÁCH TRẢ LỜI:\n"
            "1. BẮT ĐẦU bằng câu giới thiệu tổng quan về chủ đề\n"
            "2. Giải thích rõ ràng, dễ hiểu như đang dạy cho người mới học\n"
            "3. Kết hợp thông tin từ nhiều nguồn nếu có\n"
            "4. Trình bày logic, chia đoạn hợp lý\n"
            "5. Nếu có thể, đưa ví dụ minh họa\n"
            "6. Khi trích dẫn, dùng dạng [Nguồn X] (không cần ghi trang nếu không có)\n\n"

            f"--- TÀI LIỆU ---\n{context}\n--- HẾT ---\n\n"
            f"Câu hỏi: {question}\n\n"
            "Hãy trả lời chi tiết, rõ ràng bằng tiếng Việt (KHÔNG copy nguyên văn từ tài liệu):"
        )
    else:
        return (
            "You are an intelligent learning assistant in an Open Educational Resources (OER) library system.\n"
            "Your goal is to help learners understand concepts clearly, simply, and in a structured way.\n\n"

            "ANSWERING PRINCIPLES:\n"
            "- Prioritize information from the provided documents\n"
            "- Do NOT copy text verbatim from documents - paraphrase and synthesize in your own words\n"
            "- Do NOT add external knowledge unless necessary for clarification (and clearly state when you do)\n"
            "- If the documents are insufficient, explicitly mention what is missing\n"
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
    if provider == "groq":
        if not GROQ_API_KEY:
            return "Missing GROQ_API_KEY."
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": GROQ_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 2048},
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
        if not contexts:
            lang = _detect_lang(payload.question)
            no_result_msg = "Không tìm thấy ngữ cảnh phù hợp trong kho dữ liệu." if lang == "vi" else "No relevant context found in the database."
            return {"question": payload.question, "answer": no_result_msg, "contexts": [], "search_mode": "hybrid" if payload.use_hybrid else "bm25"}
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
