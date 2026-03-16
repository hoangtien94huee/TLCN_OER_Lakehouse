"""
Simple OER Search API using Elasticsearch.
Includes Search and Recommendations with PDF support.
"""

import os
from typing import Any, Dict, List, Optional, Tuple
from datetime import timedelta

import requests
from fastapi import FastAPI, Query, Request, Form, Response, HTTPException, Body
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from minio import Minio

from reviews import get_review_store, ReviewStore

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200").rstrip("/")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
RESULT_SIZE = int(os.getenv("SEARCH_RESULT_SIZE", "20"))

# LLM config for RAG Q&A  (LLM_PROVIDER=gemini | groq | ollama)
LLM_PROVIDER   = os.getenv("LLM_PROVIDER", "gemini")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY", "")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL     = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
OLLAMA_HOST    = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL   = os.getenv("OLLAMA_MODEL", "llama3.2")

# MinIO config for PDF access
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_PUBLIC_ENDPOINT = os.getenv("MINIO_PUBLIC_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "oer-lakehouse")

app = FastAPI(title="OER Search & Recommendations")

# Add CORS middleware for DSpace Angular frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4000",
        "http://localhost:4200",
        "http://127.0.0.1:4000",
        "http://127.0.0.1:4200",
        "http://dspace-angular:4000",
        "*"  # Allow all origins in development - restrict in production
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

_minio_client: Optional[Minio] = None


def get_minio_client() -> Optional[Minio]:
    """Get or create MinIO client."""
    global _minio_client
    if _minio_client is None:
        try:
            endpoint = MINIO_PUBLIC_ENDPOINT.replace("http://", "").replace("https://", "")
            _minio_client = Minio(
                endpoint,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_PUBLIC_ENDPOINT.startswith("https")
            )
        except Exception as e:
            print(f"MinIO client error: {e}")
    return _minio_client


def generate_presigned_url(path: str) -> Optional[str]:
    """Generate presigned URL for PDF file."""
    if not path:
        return None
    
    # Parse s3a:// or s3:// path
    normalized = path.replace("s3a://", "").replace("s3://", "")
    if "/" not in normalized:
        return None
    
    bucket, key = normalized.split("/", 1)
    
    try:
        client = get_minio_client()
        if client:
            url = client.presigned_get_object(bucket, key, expires=timedelta(hours=1))
            return url
    except Exception as e:
        print(f"Presigned URL error: {e}")
    
    return None


def search_elasticsearch(query: str) -> List[Dict[str, Any]]:
    """Search OER resources in Elasticsearch with PDF chunk search."""
    if not query:
        return []
    
    es_query = {
        "size": RESULT_SIZE,
        "query": {
            "bool": {
                "should": [
                    # Search in main fields
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["title^3", "description^2", "search_text"],
                            "type": "best_fields",
                            "fuzziness": "AUTO"
                        }
                    },
                    # Search in PDF chunks (nested)
                    {
                        "nested": {
                            "path": "pdf_chunks",
                            "query": {
                                "match": {
                                    "pdf_chunks.text": {
                                        "query": query,
                                        "fuzziness": "AUTO"
                                    }
                                }
                            },
                            "score_mode": "max",
                            "inner_hits": {
                                "size": 3,  # Return top 3 matching chunks
                                "highlight": {
                                    "fields": {
                                        "pdf_chunks.text": {
                                            "fragment_size": 150,
                                            "number_of_fragments": 1,
                                            "pre_tags": ["<mark>"],
                                            "post_tags": ["</mark>"]
                                        }
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        },
        "highlight": {
            "fields": {
                "title": {},
                "description": {"fragment_size": 200}
            }
        }
    }
    
    try:
        resp = requests.post(
            f"{ES_HOST}/{ES_INDEX}/_search",
            json=es_query,
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        
        results = []
        for hit in data.get("hits", {}).get("hits", []):
            source = hit.get("_source", {})
            highlight = hit.get("highlight", {})
            inner_hits = hit.get("inner_hits", {})
            
            # Extract PDF chunks with snippets from inner_hits (if search matched PDF content)
            pdf_snippets = []
            if "pdf_chunks" in inner_hits:
                for chunk_hit in inner_hits["pdf_chunks"].get("hits", {}).get("hits", [])[:3]:
                    chunk_source = chunk_hit.get("_source", {})
                    chunk_highlight = chunk_hit.get("highlight", {})
                    
                    # Get highlighted snippet or fallback to raw text
                    snippet_text = None
                    if "pdf_chunks.text" in chunk_highlight:
                        snippet_text = chunk_highlight["pdf_chunks.text"][0]
                    else:
                        snippet_text = chunk_source.get("text", "")[:150] + "..."
                    
                    if snippet_text:
                        pdf_path = chunk_source.get("file_uri")
                        url = generate_presigned_url(pdf_path) if pdf_path else None
                        
                        pdf_snippets.append({
                            "file": chunk_source.get("file", "PDF"),
                            "page": chunk_source.get("page"),
                            "snippet": snippet_text,
                            "url": url
                        })
            
            # Generate presigned URLs for PDF files
            # Ưu tiên dùng pdf_files_with_pages (có page number), fallback sang pdf_files cũ
            pdf_links = []
            pdf_links_limit = 5  # Limit số PDF links trả về (UI sẽ hiển thị top 2)
            
            # If we have PDF snippets from search, use those first
            if pdf_snippets:
                # Use snippets as primary PDF links (they have context)
                for snippet in pdf_snippets[:pdf_links_limit]:
                    if snippet.get("url"):
                        pdf_links.append({
                            "name": snippet["file"],
                            "url": snippet["url"],
                            "page": snippet.get("page"),
                            "snippet": snippet.get("snippet")
                        })
            
            # Also add other PDF files (without snippets) if we have space
            if len(pdf_links) < pdf_links_limit:
                pdf_files_with_pages = source.get("pdf_files_with_pages", [])
                if pdf_files_with_pages:
                    for pdf_item in pdf_files_with_pages[:pdf_links_limit - len(pdf_links)]:
                        if isinstance(pdf_item, dict):
                            pdf_path = pdf_item.get("path")
                            page = pdf_item.get("page")
                            name = pdf_item.get("name")
                            
                            url = generate_presigned_url(pdf_path) if pdf_path else None
                            if url:
                                filename = name or (pdf_path.split("/")[-1] if pdf_path else "PDF")
                                link = {"name": filename, "url": url}
                                if page is not None:
                                    link["page"] = page
                                pdf_links.append(link)
                else:
                    # Fallback: legacy pdf_files format (list of strings or objects)
                    pdf_files = source.get("pdf_files", [])
                    for pdf_item in pdf_files[:pdf_links_limit - len(pdf_links)]:
                        pdf_path = None
                        page = None

                        # Nếu ES lưu dạng object: {"path": "...", "page": 5, ...}
                        if isinstance(pdf_item, dict):
                            pdf_path = (
                                pdf_item.get("path")
                                or pdf_item.get("file")
                                or pdf_item.get("url")
                            )
                            page = pdf_item.get("page")
                        else:
                            # Dạng cũ: chỉ là string path
                            pdf_path = pdf_item

                        url = generate_presigned_url(pdf_path) if pdf_path else None
                        if url:
                            # Extract filename from path
                            filename = pdf_path.split("/")[-1] if pdf_path else "PDF"
                            link = {"name": filename, "url": url}
                            if page is not None:
                                link["page"] = page
                            pdf_links.append(link)
            
            results.append({
                "title": source.get("title", "Untitled"),
                "description": source.get("description", ""),
                "url": source.get("source_url", "#"),
                "source": source.get("source_system", ""),
                "subjects": source.get("subjects", []),
                "score": hit.get("_score", 0),
                "pdf_links": pdf_links,
                "highlight": {
                    "title": highlight.get("title", []),
                    "description": highlight.get("description", [])
                }
            })
        
        return results
    except Exception as e:
        print(f"Elasticsearch error: {e}")
        return []


def validate_student(student_id: str, password: str) -> Optional[str]:
    """Validate student login. Accepts any non-empty student ID."""
    if not student_id or not student_id.strip():
        return None
    return student_id.strip()


def get_student_name(student_id: str) -> Optional[str]:
    """Return student ID as display name."""
    return student_id


def get_popular_resources(limit: int = 10, source: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get popular/high-quality OER resources sorted by data_quality_score."""
    query: Dict[str, Any] = {
        "size": limit,
        "query": {"bool": {"must": [{"exists": {"field": "title"}}]}},
        "sort": [
            {"data_quality_score": {"order": "desc"}},
            {"_score": {"order": "desc"}}
        ]
    }
    if source:
        query["query"]["bool"]["filter"] = [{"term": {"source_system.keyword": source}}]
    try:
        resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=query, timeout=10)
        resp.raise_for_status()
        results = []
        for hit in resp.json().get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            subjects = [s.get("subject_name", "") for s in src.get("matched_subjects", []) if isinstance(s, dict)]
            results.append({
                "resource_id": src.get("resource_id", ""),
                "title": src.get("title", "Untitled"),
                "description": src.get("description", "")[:200],
                "url": src.get("source_url", "#"),
                "source": src.get("source_system", ""),
                "subjects": subjects[:3],
                "quality_score": src.get("data_quality_score", 0),
            })
        return results
    except Exception as e:
        print(f"Popular resources error: {e}")
        return []


# ============ ROUTES ============

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Redirect to login page."""
    student_id = request.cookies.get("student_id")
    if student_id:
        return RedirectResponse(url="/dashboard", status_code=302)
    return RedirectResponse(url="/login", status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Login page."""
    student_id = request.cookies.get("student_id")
    if student_id:
        return RedirectResponse(url="/dashboard", status_code=302)
    
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": None,
        "username": ""
    })


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    """Handle login form submission."""
    student_name = validate_student(username, password)
    
    if student_name is None:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Vui lòng nhập mã sinh viên.",
            "username": username
        })
    
    # Set cookie and redirect to dashboard
    response = RedirectResponse(url="/dashboard", status_code=302)
    response.set_cookie(key="student_id", value=username, max_age=86400)  # 1 day
    return response


@app.get("/logout")
async def logout():
    """Logout and clear session."""
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(key="student_id")
    return response


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, q: Optional[str] = Query(None)):
    """Main dashboard with search."""
    student_id = request.cookies.get("student_id")
    if not student_id:
        return RedirectResponse(url="/login", status_code=302)

    student_name = get_student_name(student_id)

    # Search results if query provided
    search_results = []
    if q:
        search_results = search_elasticsearch(q)

    # Show popular resources when not searching
    popular_resources = get_popular_resources(limit=10) if not q else []

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "student_id": student_id,
        "student_name": student_name,
        "search_results": search_results,
        "popular_resources": popular_resources,
        "query": q or ""
    })


# ============ API ENDPOINTS FOR DSPACE INTEGRATION ============

@app.get("/api/search")
async def api_search(q: str = Query(..., description="Search query")):
    """REST API endpoint for search."""
    results = search_elasticsearch(q)
    return {
        "query": q,
        "total": len(results),
        "results": results
    }


@app.get("/api/recommend/by-subject/{subject}")
async def api_recommend_by_subject(
    subject: str,
    limit: int = Query(10, ge=1, le=50)
):
    """Get OER recommendations for a specific subject/topic.
    
    DSpace Integration: Use this to show related resources on item pages.
    
    Example: GET /api/recommend/by-subject/General%20principles%20of%20mathematics?limit=5
    """
    results = get_popular_resources(limit=limit, source=None)
    # Further filter by subject keyword via ES multi_match
    try:
        resp = requests.post(
            f"{ES_HOST}/{ES_INDEX}/_search",
            json={
                "size": limit,
                "_source": ["title", "description", "source_url", "source_system", "matched_subjects", "resource_id"],
                "query": {
                    "multi_match": {
                        "query": subject,
                        "fields": ["title^2", "matched_subjects.subject_name^3", "description"],
                        "type": "best_fields"
                    }
                }
            },
            timeout=5
        )
        resp.raise_for_status()
        results = []
        for hit in resp.json().get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            subjects = [s.get("subject_name", "") for s in src.get("matched_subjects", []) if isinstance(s, dict)]
            results.append({
                "resource_id": src.get("resource_id", ""),
                "title": src.get("title", "Untitled"),
                "description": src.get("description", "")[:200],
                "url": src.get("source_url", "#"),
                "source": src.get("source_system", ""),
                "subjects": subjects[:5],
                "score": hit.get("_score", 0),
            })
    except Exception as e:
        print(f"by-subject error: {e}")
        results = []
    return {
        "subject": subject,
        "total": len(results),
        "results": results
    }


@app.get("/api/recommend/by-item/{item_id}")
async def api_recommend_by_item(
    item_id: str,
    limit: int = Query(10, ge=1, le=50)
):
    """Get similar OER resources based on an item's subjects.
    
    DSpace Integration: Show "Related Resources" on item detail page.
    
    Example: GET /api/recommend/by-item/mit_ocw_18-06sc?limit=5
    """
    # First, get the item from Elasticsearch
    try:
        resp = requests.post(
            f"{ES_HOST}/{ES_INDEX}/_search",
            json={
                "size": 1,
                "query": {
                    "bool": {
                        "should": [
                            {"term": {"resource_id.keyword": item_id}},
                            {"term": {"resource_id": item_id}},
                            {"match": {"resource_id": item_id}}
                        ]
                    }
                }
            },
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        
        if not hits:
            return {
                "item_id": item_id,
                "total": 0,
                "results": [],
                "message": "Item not found"
            }
        
        source = hits[0].get("_source", {})
        item_title = source.get("title", "")
        
        # Get subjects from matched_subjects array
        subjects = []
        for s in source.get("matched_subjects", []):
            if isinstance(s, dict):
                subjects.append(s.get("subject_name", ""))
            else:
                subjects.append(str(s))
        
        # Also use title for similarity
        search_text = f"{item_title} {' '.join(subjects)}"
        
        # Search for similar items, excluding the original
        similar_query = {
            "size": limit + 1,  # Get one extra to exclude self
            "query": {
                "bool": {
                    "must": {
                        "multi_match": {
                            "query": search_text,
                            "fields": ["title^2", "description", "matched_subjects.subject_name^3"],
                            "type": "best_fields",
                            "fuzziness": "AUTO"
                        }
                    },
                    "must_not": {
                        "term": {"resource_id.keyword": item_id}
                    }
                }
            }
        }
        
        resp = requests.post(
            f"{ES_HOST}/{ES_INDEX}/_search",
            json=similar_query,
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        
        results = []
        for hit in data.get("hits", {}).get("hits", [])[:limit]:
            src = hit.get("_source", {})
            subj_list = [s.get("subject_name", "") for s in src.get("matched_subjects", []) if isinstance(s, dict)]
            
            results.append({
                "resource_id": src.get("resource_id", ""),
                "title": src.get("title", "Untitled"),
                "description": src.get("description", ""),
                "url": src.get("source_url", "#"),
                "source": src.get("source_system", ""),
                "subjects": subj_list,
                "score": hit.get("_score", 0),
            })
        
        return {
            "item_id": item_id,
            "item_title": item_title,
            "item_subjects": subjects,
            "total": len(results),
            "results": results
        }
        
    except Exception as e:
        print(f"Similar items error: {e}")
        return {
            "item_id": item_id,
            "total": 0,
            "results": [],
            "error": str(e)
        }


@app.get("/api/recommend/popular")
async def api_popular_resources(
    limit: int = Query(10, ge=1, le=50),
    source: Optional[str] = Query(None, description="Filter by source: mit_ocw, openstax, otl")
):
    """Get popular/trending OER resources.
    
    DSpace Integration: Show on homepage or sidebar.
    
    Example: GET /api/recommend/popular?limit=10&source=openstax
    """
    query = {
        "size": limit,
        "query": {
            "bool": {
                "must": [
                    {"exists": {"field": "title"}}
                ]
            }
        },
        "sort": [
            {"data_quality_score": {"order": "desc"}},
            {"_score": {"order": "desc"}}
        ]
    }
    
    if source:
        query["query"]["bool"]["filter"] = [
            {"term": {"source_system.keyword": source}}
        ]
    
    try:
        resp = requests.post(
            f"{ES_HOST}/{ES_INDEX}/_search",
            json=query,
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        
        results = []
        for hit in data.get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            subj_list = [s.get("subject_name", "") for s in src.get("matched_subjects", []) if isinstance(s, dict)]
            
            results.append({
                "resource_id": src.get("resource_id", ""),
                "title": src.get("title", "Untitled"),
                "description": src.get("description", "")[:200] + "..." if len(src.get("description", "")) > 200 else src.get("description", ""),
                "url": src.get("source_url", "#"),
                "source": src.get("source_system", ""),
                "subjects": subj_list[:3],  # Limit subjects for display
                "quality_score": src.get("data_quality_score", 0),
            })
        
        return {
            "filter": {"source": source} if source else None,
            "total": len(results),
            "results": results
        }
        
    except Exception as e:
        print(f"Popular resources error: {e}")
        return {"total": 0, "results": [], "error": str(e)}


# ============ Q&A / RAG API ============

class AskRequest(BaseModel):
    question: str = Field(..., min_length=3, description="Question to ask about OER content")
    top_k: int = Field(5, ge=1, le=10, description="Number of context chunks to retrieve")


# --- Encoding fix for PDF text extracted with wrong codec (Latin-1 read as UTF-8) ---
_ENCODING_MAP = [
    ("â€œ", '"'), ("â€", '"'), ("â€™", "'"), ("â€˜", "'"),
    ("â€¦", "..."), ("â€"", "—"), ("â€"", "–"),
    ("Â·", "·"), ("Â©", "©"), ("Â®", "®"), ("Â°", "°"),
    ("Â½", "½"), ("Â¼", "¼"), ("Â¾", "¾"),
    ("â¢", "•"), ("â¢ ", "• "),
    ("Ã©", "é"), ("Ã¨", "è"), ("Ã ", "à"), ("Ã¢", "â"),
    ("Ã¯", "ï"), ("Ã®", "î"), ("Ã´", "ô"), ("Ã¹", "ù"),
    ("Ã±", "ñ"), ("Ã¼", "ü"), ("Ã¶", "ö"), ("Ã¤", "ä"),
    ("â ", " "),
]

def _fix_encoding(text: str) -> str:
    """Fix mojibake characters from PDF Latin-1/Windows-1252 decoded as UTF-8."""
    if not text:
        return text
    for bad, good in _ENCODING_MAP:
        text = text.replace(bad, good)
    return text


# --- Multilingual Embedding Model (singleton, lazy-loaded) ---
_EMBED_MODEL = None

def _get_embedding_model():
    """Lazy-load distiluse-base-multilingual-cased-v2 once, reuse forever.

    Model specs: 512 dimensions, 50+ languages (VI + EN in same vector space),
    ~135MB, CPU-friendly. No API calls required — runs fully local.
    """
    global _EMBED_MODEL
    if _EMBED_MODEL is not None:
        return _EMBED_MODEL
    try:
        from sentence_transformers import SentenceTransformer
        print("[Embedding] Loading distiluse-base-multilingual-cased-v2 ...")
        _EMBED_MODEL = SentenceTransformer("distiluse-base-multilingual-cased-v2")
        print("[Embedding] Model ready")
    except Exception as e:
        print(f"[Embedding] Could not load model ({e}) — falling back to BM25 only")
        _EMBED_MODEL = None
    return _EMBED_MODEL


def retrieve_context(question: str, top_k: int = 5) -> List[Dict[str, Any]]:
    """Retrieve top-k relevant chunks using multilingual KNN + BM25 fallback.

    Strategy:
      1. KNN search on doc-level `embedding` field (handles VI↔EN cross-lingual)
         → for each matched doc, re-rank its pdf_chunks by chunk embedding cosine sim
      2. Fallback: BM25 multi_match on search_text/description (for docs without embeddings)
    """
    import numpy as np

    chunks: List[Dict[str, Any]] = []
    model = _get_embedding_model()

    # ── Path 1: Multilingual KNN ──────────────────────────────────────────────
    if model:
        try:
            query_vec = model.encode(question, normalize_embeddings=True)
            query_list = query_vec.tolist()

            knn_body = {
                "size": top_k * 3,   # Fetch extra docs to extract best chunks
                "_source": ["title", "source_url", "resource_id", "description", "pdf_chunks"],
                "knn": {
                    "field": "embedding",
                    "query_vector": query_list,
                    "k": top_k * 3,
                    "num_candidates": max(200, top_k * 40),
                    # Only search docs that have actual PDF chunks indexed
                    "filter": {
                        "nested": {
                            "path": "pdf_chunks",
                            "query": {"exists": {"field": "pdf_chunks.text"}}
                        }
                    }
                }
            }
            resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=knn_body, timeout=15)
            resp.raise_for_status()

            for hit in resp.json().get("hits", {}).get("hits", []):
                if len(chunks) >= top_k:
                    break
                src = hit.get("_source", {})
                doc_chunks = src.get("pdf_chunks") or []

                if doc_chunks:
                    # Re-rank chunks by cosine similarity with query vector
                    scored: List[tuple] = []
                    for ch in doc_chunks:
                        ch_emb = ch.get("embedding")
                        if ch_emb:
                            score = float(np.dot(query_vec, np.array(ch_emb)))
                        else:
                            score = 0.0  # No chunk embedding — rank below any real match
                        scored.append((score, ch))
                    scored.sort(key=lambda x: x[0], reverse=True)

                    # Take top-3 best chunks per doc for richer context
                    for _, ch in scored[:3]:
                        if len(chunks) >= top_k:
                            break
                        if ch.get("text"):
                            chunks.append({
                                "text": _fix_encoding(ch["text"]),
                                "title": src.get("title", ""),
                                "source_url": src.get("source_url", "#"),
                                "resource_id": src.get("resource_id", ""),
                                "file": ch.get("file", ""),
                                "page": ch.get("page"),
                            })
                else:
                    # Doc matched but no chunks → use description as context
                    desc = src.get("description", "")
                    if desc:
                        chunks.append({
                            "text": _fix_encoding(desc[:800]),
                            "title": src.get("title", ""),
                            "source_url": src.get("source_url", "#"),
                            "resource_id": src.get("resource_id", ""),
                            "file": "",
                            "page": None,
                        })

            if chunks:
                print(f"[RAG] KNN retrieved {len(chunks)} chunks for: '{question[:60]}'")

        except Exception as e:
            print(f"[RAG] KNN failed ({e}), falling back to BM25")

    # ── Path 2: BM25 fallback (no model, or KNN returned nothing) ─────────────
    if not chunks:
        # Try pdf_chunks nested BM25 first
        chunk_query = {
            "size": 5,
            "_source": ["title", "source_url", "resource_id"],
            "query": {
                "nested": {
                    "path": "pdf_chunks",
                    "query": {"match": {"pdf_chunks.text": {"query": question, "fuzziness": "AUTO"}}},
                    "score_mode": "max",
                    "inner_hits": {
                        "size": top_k,
                        "_source": ["pdf_chunks.text", "pdf_chunks.file", "pdf_chunks.page"],
                        "highlight": {
                            "fields": {"pdf_chunks.text": {"fragment_size": 600, "number_of_fragments": 1}}
                        }
                    }
                }
            }
        }
        try:
            resp = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=chunk_query, timeout=10)
            resp.raise_for_status()
            for hit in resp.json().get("hits", {}).get("hits", []):
                src = hit.get("_source", {})
                inner_hits = (
                    hit.get("inner_hits", {}).get("pdf_chunks", {}).get("hits", {}).get("hits", [])
                )
                for ch in inner_hits:
                    ch_src = ch.get("_source", {})
                    ch_hl = ch.get("highlight", {})
                    text = (
                        ch_hl.get("pdf_chunks.text", [""])[0]
                        if ch_hl.get("pdf_chunks.text")
                        else ch_src.get("text", "")
                    )
                    if text:
                        chunks.append({
                            "text": text,
                            "title": src.get("title", ""),
                            "source_url": src.get("source_url", "#"),
                            "resource_id": src.get("resource_id", ""),
                            "file": ch_src.get("file", ""),
                            "page": ch_src.get("page"),
                        })
        except Exception as e:
            print(f"[RAG] BM25 chunk retrieval error: {e}")

        # Final fallback: description-based multi_match
        if not chunks:
            text_query = {
                "size": 5,
                "_source": ["title", "source_url", "resource_id", "description"],
                "query": {
                    "multi_match": {
                        "query": question,
                        "fields": ["title^3", "description^2", "search_text"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                }
            }
            try:
                resp2 = requests.post(f"{ES_HOST}/{ES_INDEX}/_search", json=text_query, timeout=10)
                resp2.raise_for_status()
                for hit in resp2.json().get("hits", {}).get("hits", []):
                    src = hit.get("_source", {})
                    desc = src.get("description", "")
                    if desc:
                        chunks.append({
                            "text": desc[:800],
                            "title": src.get("title", ""),
                            "source_url": src.get("source_url", "#"),
                            "resource_id": src.get("resource_id", ""),
                            "file": "",
                            "page": None,
                        })
            except Exception as e:
                print(f"[RAG] Description fallback error: {e}")

        if chunks:
            print(f"[RAG] BM25 retrieved {len(chunks)} chunks for: '{question[:60]}'")

    return chunks[:top_k]


_VIETNAMESE_CHARS = set("àáâãèéêìíòóôõùúýăđơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ"
                        "ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝĂĐƠƯẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼẾỀỂỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỴỶỸ")


def _detect_language(text: str) -> str:
    """Return 'vi' if text contains Vietnamese characters, else 'en'."""
    for ch in text:
        if ch in _VIETNAMESE_CHARS:
            return "vi"
    return "en"


def _build_rag_prompt(question: str, chunks: List[Dict[str, Any]]) -> str:
    """Build the RAG prompt from question + retrieved context chunks.

    Automatically matches the response language to the question language,
    and includes page-level citations for each context chunk.
    """
    lang = _detect_language(question)

    context_parts = []
    for i, c in enumerate(chunks, 1):
        title = c.get("title", "Tài liệu không rõ tên")
        page = c.get("page")
        file_name = c.get("file", "")
        text = c.get("text", "")

        if lang == "vi":
            cite = f"[Nguồn {i}: {title}"
            if page:
                cite += f", trang {page}"
            if file_name:
                cite += f" ({file_name})"
            cite += "]"
        else:
            cite = f"[Source {i}: {title}"
            if page:
                cite += f", page {page}"
            if file_name:
                cite += f" ({file_name})"
            cite += "]"

        context_parts.append(f"{cite}\n{text}")

    context = "\n\n".join(context_parts)

    if lang == "vi":
        return (
            "Bạn là trợ lý học tập thông minh của hệ thống thư viện học liệu mở (OER).\n"
            "Hãy trả lời câu hỏi CHỈ dựa trên các đoạn tài liệu được cung cấp bên dưới — "
            "không suy luận thêm thông tin ngoài tài liệu.\n"
            "Trả lời bằng TIẾNG VIỆT vì câu hỏi được đặt bằng tiếng Việt.\n"
            "Khi trích dẫn thông tin, ghi rõ nguồn theo dạng [Nguồn X, trang Y].\n"
            "Nếu tài liệu không đủ để trả lời, hãy nêu rõ và tóm tắt nội dung liên quan nhất.\n\n"
            f"--- TÀI LIỆU THAM KHẢO ---\n{context}\n--- HẾT TÀI LIỆU ---\n\n"
            f"Câu hỏi: {question}\n\n"
            "Trả lời (bằng tiếng Việt, có trích dẫn nguồn):"
        )
    else:
        return (
            "You are an intelligent learning assistant for an Open Educational Resources (OER) library.\n"
            "Answer the question ONLY based on the document excerpts provided below — "
            "do not infer or add information beyond what is in the sources.\n"
            "Respond in ENGLISH because the question is in English.\n"
            "When citing information, reference the source as [Source X, page Y].\n"
            "If the documents are insufficient to answer, say so clearly and summarize the most relevant content.\n\n"
            f"--- REFERENCE DOCUMENTS ---\n{context}\n--- END OF DOCUMENTS ---\n\n"
            f"Question: {question}\n\n"
            "Answer (in English, with source citations):"
        )


def _generate_with_gemini(prompt: str) -> str:
    """Call Google Gemini REST API to generate an answer."""
    if not GEMINI_API_KEY:
        return "Chưa cấu hình GEMINI_API_KEY. Vui lòng thêm biến môi trường GEMINI_API_KEY."
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.3,
            "maxOutputTokens": 1024,
        }
    }
    try:
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        candidates = resp.json().get("candidates", [])
        if candidates:
            return candidates[0]["content"]["parts"][0]["text"].strip()
        return "Không thể tạo câu trả lời lúc này."
    except requests.exceptions.Timeout:
        return "Gemini API phản hồi quá lâu. Vui lòng thử lại sau ít phút."
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else ""
        if status == 429:
            return "Đã vượt quá giới hạn API. Vui lòng thử lại sau."
        print(f"[RAG] Gemini HTTP error {status}: {e}")
        return "Lỗi khi gọi Gemini API. Vui lòng thử lại sau."
    except Exception as e:
        print(f"[RAG] Gemini error: {e}")
        return "Đã xảy ra lỗi khi gọi Gemini. Vui lòng thử lại sau."


def _generate_with_ollama(prompt: str) -> str:
    """Call local Ollama API to generate an answer."""
    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": 1024}
            },
            timeout=120
        )
        resp.raise_for_status()
        return resp.json().get("response", "Không thể tạo câu trả lời lúc này.").strip()
    except requests.exceptions.Timeout:
        return "Hệ thống AI đang xử lý quá lâu. Vui lòng thử lại sau ít phút."
    except requests.exceptions.ConnectionError:
        return "Không thể kết nối đến Ollama. Vui lòng đảm bảo dịch vụ đang chạy."
    except Exception as e:
        print(f"[RAG] Ollama error: {e}")
        return "Đã xảy ra lỗi khi gọi Ollama. Vui lòng thử lại sau."


def _generate_with_groq(prompt: str) -> str:
    """Call Groq API (OpenAI-compatible) to generate an answer."""
    if not GROQ_API_KEY:
        return "Chưa cấu hình GROQ_API_KEY. Vui lòng thêm biến môi trường GROQ_API_KEY."
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 1024,
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except requests.exceptions.Timeout:
        return "Groq API phản hồi quá lâu. Vui lòng thử lại sau ít phút."
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else ""
        if status == 429:
            return "Đã vượt quá giới hạn Groq API. Vui lòng thử lại sau."
        print(f"[RAG] Groq HTTP error {status}: {e}")
        return "Lỗi khi gọi Groq API. Vui lòng thử lại sau."
    except Exception as e:
        print(f"[RAG] Groq error: {e}")
        return "Đã xảy ra lỗi khi gọi Groq. Vui lòng thử lại sau."


def generate_answer(question: str, chunks: List[Dict[str, Any]]) -> str:
    """Generate an answer via Gemini (default) or Ollama, using retrieved RAG context."""
    if not chunks:
        return (
            "Xin lỗi, tôi không tìm thấy tài liệu nào liên quan đến câu hỏi của bạn "
            "trong kho học liệu mở. Hãy thử diễn đạt câu hỏi theo cách khác."
        )
    prompt = _build_rag_prompt(question, chunks)
    provider = LLM_PROVIDER.lower()
    if provider == "groq":
        return _generate_with_groq(prompt)
    if provider == "ollama":
        return _generate_with_ollama(prompt)
    return _generate_with_gemini(prompt)


@app.post("/api/ask")
async def ask_question(request: AskRequest):
    """RAG-based Q&A: retrieve relevant PDF chunks from ES, generate answer via Gemini (or Ollama).

    Set LLM_PROVIDER=gemini (default) or LLM_PROVIDER=ollama.

    Example:
    POST /api/ask
    { "question": "Đạo hàm là gì và cách tính đạo hàm hàm số mũ?" }
    """
    question = request.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    # retrieve_context internally translates to English for BM25 search;
    # generate_answer receives the ORIGINAL Vietnamese question so LLM answers in Vietnamese.
    chunks = retrieve_context(question, top_k=request.top_k)
    answer = generate_answer(question, chunks)

    # Deduplicate sources by resource_id
    sources: List[Dict[str, Any]] = []
    seen: set = set()
    for c in chunks:
        rid = c.get("resource_id", "") or c.get("source_url", "")
        if rid and rid not in seen:
            seen.add(rid)
            snippet = c["text"]
            sources.append({
                "title": c["title"],
                "url": c["source_url"],
                "file": c.get("file", ""),
                "page": c.get("page"),
                "snippet": snippet[:250] + "..." if len(snippet) > 250 else snippet,
            })

    return {
        "question": question,
        "answer": answer,
        "sources": sources,
        "context_count": len(chunks),
    }


@app.get("/api/health")
async def health_check():
    """Health check endpoint for monitoring."""
    es_ok = False
    try:
        resp = requests.get(f"{ES_HOST}/_cluster/health", timeout=5)
        es_ok = resp.status_code == 200
    except:
        pass
    
    return {
        "status": "healthy" if es_ok else "degraded",
        "elasticsearch": "connected" if es_ok else "disconnected",
    }


# ============ RATING & REVIEW API ENDPOINTS ============

class ReviewCreate(BaseModel):
    """Request model for creating/updating a review."""
    resource_id: str = Field(..., description="OER resource ID")
    email: str = Field(..., description="User email (from DSpace eperson)")
    rating: int = Field(..., ge=1, le=5, description="Rating from 1 to 5 stars")
    comment: str = Field("", description="Optional review comment")


class ReviewCreateByUUID(BaseModel):
    """Request model for creating/updating a review using eperson UUID."""
    resource_id: str = Field(..., description="OER resource ID")
    eperson_id: str = Field(..., description="DSpace eperson UUID")
    rating: int = Field(..., ge=1, le=5, description="Rating from 1 to 5 stars")
    comment: str = Field("", description="Optional review comment")


@app.post("/api/reviews")
async def create_review(review: ReviewCreate):
    """Submit or update a review for a resource.
    
    DSpace Integration: Call when user submits a rating/review on item page.
    Uses user's email to link to DSpace eperson table.
    Each user can only have one review per resource - submitting again updates it.
    
    Example:
    POST /api/reviews
    {
        "resource_id": "mit_ocw_18-06sc",
        "email": "22142278@gmail.com",
        "rating": 5,
        "comment": "Excellent course for linear algebra!"
    }
    """
    try:
        store = get_review_store()
        result = store.add_review_by_email(
            resource_id=review.resource_id,
            email=review.email,
            rating=review.rating,
            comment=review.comment
        )
        
        return {
            "success": True,
            "message": "Review submitted successfully",
            "review": result
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Create review error: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit review")


@app.post("/api/reviews/by-uuid")
async def create_review_by_uuid(review: ReviewCreateByUUID):
    """Submit or update a review using eperson UUID directly.
    
    Alternative to /api/reviews when you have the UUID.
    
    Example:
    POST /api/reviews/by-uuid
    {
        "resource_id": "mit_ocw_18-06sc",
        "eperson_id": "550e8400-e29b-41d4-a716-446655440000",
        "rating": 5,
        "comment": "Excellent course!"
    }
    """
    try:
        store = get_review_store()
        result = store.add_review(
            resource_id=review.resource_id,
            eperson_id=review.eperson_id,
            rating=review.rating,
            comment=review.comment
        )
        
        return {
            "success": True,
            "message": "Review submitted successfully",
            "review": result
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Create review error: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit review")


@app.get("/api/reviews/{resource_id}")
async def get_reviews(
    resource_id: str,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("created_at", description="Sort by: created_at, rating, helpful_count"),
    order: str = Query("desc", description="Order: asc or desc")
):
    """Get all reviews for a resource.
    
    DSpace Integration: Display on item detail page.
    
    Example: GET /api/reviews/mit_ocw_18-06sc?limit=10&sort_by=helpful_count&order=desc
    """
    store = get_review_store()
    return store.get_reviews(
        resource_id=resource_id,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        order=order
    )


@app.get("/api/reviews/{resource_id}/stats")
async def get_review_stats(resource_id: str):
    """Get rating statistics for a resource.
    
    DSpace Integration: Show star rating summary on item cards and detail pages.
    
    Example: GET /api/reviews/mit_ocw_18-06sc/stats
    
    Response:
    {
        "resource_id": "mit_ocw_18-06sc",
        "total_reviews": 42,
        "average_rating": 4.3,
        "distribution": {"5": 20, "4": 12, "3": 5, "2": 3, "1": 2}
    }
    """
    store = get_review_store()
    return store.get_resource_stats(resource_id)


@app.get("/api/reviews/{resource_id}/user")
async def get_user_review_by_email(
    resource_id: str,
    email: str = Query(..., description="User email")
):
    """Get a specific user's review for a resource by email.
    
    DSpace Integration: Check if user has already reviewed, pre-fill edit form.
    
    Example: GET /api/reviews/mit_ocw_18-06sc/user?email=22142278@gmail.com
    """
    store = get_review_store()
    review = store.get_user_review_by_email(resource_id, email)
    
    if review:
        return {"has_review": True, "review": review}
    else:
        return {"has_review": False, "review": None}


@app.get("/api/reviews/{resource_id}/user/{eperson_id}")
async def get_user_review(resource_id: str, eperson_id: str):
    """Get a specific user's review for a resource by eperson UUID.
    
    Example: GET /api/reviews/mit_ocw_18-06sc/user/550e8400-e29b-41d4-a716-446655440000
    """
    store = get_review_store()
    review = store.get_user_review(resource_id, eperson_id)
    
    if review:
        return {"has_review": True, "review": review}
    else:
        return {"has_review": False, "review": None}


@app.delete("/api/reviews/{resource_id}/user/{eperson_id}")
async def delete_review(resource_id: str, eperson_id: str):
    """Delete a user's review.
    
    DSpace Integration: Allow users to delete their own reviews.
    
    Example: DELETE /api/reviews/mit_ocw_18-06sc/user/550e8400-e29b-41d4-a716-446655440000
    """
    store = get_review_store()
    deleted = store.delete_review(resource_id, eperson_id)
    
    if deleted:
        return {"success": True, "message": "Review deleted"}
    else:
        raise HTTPException(status_code=404, detail="Review not found")


@app.post("/api/reviews/{review_id}/helpful")
async def mark_helpful(
    review_id: int, 
    eperson_id: str = Query(..., description="eperson UUID marking as helpful")
):
    """Mark a review as helpful.
    
    DSpace Integration: "Was this review helpful?" button.
    Each user can only mark a review as helpful once.
    
    Example: POST /api/reviews/123/helpful?eperson_id=550e8400-e29b-41d4-a716-446655440000
    """
    store = get_review_store()
    marked = store.mark_helpful(review_id, eperson_id)
    
    if marked:
        return {"success": True, "message": "Marked as helpful"}
    else:
        return {"success": False, "message": "Already marked as helpful"}


@app.get("/api/users/{eperson_id}/reviews")
async def get_user_reviews(eperson_id: str, limit: int = Query(20, ge=1, le=100)):
    """Get all reviews by a user (by eperson UUID).
    
    DSpace Integration: Show on user profile page.
    
    Example: GET /api/users/550e8400-e29b-41d4-a716-446655440000/reviews?limit=10
    """
    store = get_review_store()
    reviews = store.get_user_reviews(eperson_id, limit)
    
    return {
        "eperson_id": eperson_id,
        "total": len(reviews),
        "reviews": reviews
    }


@app.get("/api/reviews/top-rated")
async def get_top_rated_resources(
    limit: int = Query(10, ge=1, le=50),
    min_reviews: int = Query(3, ge=1, description="Minimum number of reviews to qualify")
):
    """Get top rated resources based on user reviews.
    
    DSpace Integration: Show "Top Rated" section on homepage.
    
    Example: GET /api/reviews/top-rated?limit=10&min_reviews=5
    """
    store = get_review_store()
    top_rated = store.get_top_rated(limit, min_reviews)
    
    # Enrich with resource details from Elasticsearch
    enriched_results = []
    for item in top_rated:
        resource_id = item['resource_id']
        
        # Get resource info from ES
        try:
            resp = requests.post(
                f"{ES_HOST}/{ES_INDEX}/_search",
                json={
                    "size": 1,
                    "_source": ["title", "source_url", "source_system", "matched_subjects"],
                    "query": {
                        "bool": {
                            "should": [
                                {"term": {"resource_id.keyword": resource_id}},
                                {"term": {"resource_id": resource_id}}
                            ]
                        }
                    }
                },
                timeout=5
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", {}).get("hits", [])
            
            if hits:
                source = hits[0].get("_source", {})
                subjects = [s.get("subject_name", "") for s in source.get("matched_subjects", []) if isinstance(s, dict)]
                
                enriched_results.append({
                    "resource_id": resource_id,
                    "title": source.get("title", resource_id),
                    "url": source.get("source_url", "#"),
                    "source": source.get("source_system", ""),
                    "subjects": subjects[:3],
                    "average_rating": round(item['avg_rating'], 1),
                    "review_count": item['review_count']
                })
            else:
                # Resource not found in ES, include basic info
                enriched_results.append({
                    "resource_id": resource_id,
                    "title": resource_id,
                    "average_rating": round(item['avg_rating'], 1),
                    "review_count": item['review_count']
                })
        except Exception as e:
            print(f"Error enriching top rated: {e}")
            enriched_results.append({
                "resource_id": resource_id,
                "average_rating": round(item['avg_rating'], 1),
                "review_count": item['review_count']
            })
    
    return {
        "total": len(enriched_results),
        "min_reviews": min_reviews,
        "results": enriched_results
    }
