"""
Simple OER Search API using Elasticsearch.
Includes Search and Recommendations with PDF support.
"""

import os
import time
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple
from datetime import timedelta
from functools import lru_cache

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
USER_DATA_PATH = os.getenv("USER_DATA_PATH", "/opt/airflow/database/user_final.csv")

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

# Cache user data
_user_df: Optional[pd.DataFrame] = None
_minio_client: Optional[Minio] = None

# Cache for recommendations: {student_id: (timestamp, results)}
_recommend_cache: Dict[str, Tuple[float, Dict]] = {}
RECOMMEND_CACHE_TTL = 300  # 5 minutes

# Cache for user topics: {student_id: (timestamp, topics)}
_topics_cache: Dict[str, Tuple[float, Dict[str, int]]] = {}
TOPICS_CACHE_TTL = 600  # 10 minutes


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


def get_user_data() -> Optional[pd.DataFrame]:
    """Load and cache user borrowing history."""
    global _user_df
    if _user_df is None:
        try:
            _user_df = pd.read_csv(USER_DATA_PATH)
            print(f"Loaded {len(_user_df)} user records")
        except Exception as e:
            print(f"Could not load user data: {e}")
            _user_df = pd.DataFrame()
    return _user_df


def search_elasticsearch(query: str) -> List[Dict[str, Any]]:
    """Search OER resources in Elasticsearch."""
    if not query:
        return []
    
    es_query = {
        "size": RESULT_SIZE,
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["title^3", "description^2", "search_text"],
                "type": "best_fields",
                "fuzziness": "AUTO"
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
            
            # Generate presigned URLs for PDF files
            pdf_links = []
            for pdf_path in source.get("pdf_files", []):
                url = generate_presigned_url(pdf_path)
                if url:
                    # Extract filename from path
                    filename = pdf_path.split("/")[-1] if pdf_path else "PDF"
                    pdf_links.append({"name": filename, "url": url})
            
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


def get_user_topics(student_id: str) -> Dict[str, int]:
    """Get topics and frequency for a user from borrowing history.
    Uses cache to avoid repeated CSV parsing.
    
    Args:
        student_id: Mã sinh viên (VD: 22142278, 23142367)
    """
    global _topics_cache
    
    # Check cache first
    if student_id in _topics_cache:
        cached_time, cached_topics = _topics_cache[student_id]
        if time.time() - cached_time < TOPICS_CACHE_TTL:
            return cached_topics
    
    df = get_user_data()
    if df is None or df.empty:
        return {}
    
    # Tìm theo mã sinh viên (So_the)
    user_history = df[df['So_the'].astype(str) == str(student_id)]
    if user_history.empty:
        return {}
    
    topic_counts = user_history['Chu_de'].dropna().value_counts().to_dict()
    
    # Cache result
    _topics_cache[student_id] = (time.time(), topic_counts)
    
    return topic_counts


def get_recommendations(topic_counts: Dict[str, int], limit: int = 10, top_n_topics: int = 3) -> List[Dict[str, Any]]:
    """Get recommendations with DIVERSIFIED results across all top topics.
    
    Instead of just boosting by topic frequency (which causes all results to come from 
    the highest-count topic), this fetches results separately for each topic and 
    interleaves them to ensure representation from all user interests.
    """
    if not topic_counts:
        return []
    
    # Only use top N topics
    sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)
    top_topics = sorted_topics[:top_n_topics]
    
    if not top_topics:
        return []
    
    # Calculate how many results to fetch per topic (fetch extra for deduplication)
    per_topic_limit = max(4, (limit // len(top_topics)) + 2)
    
    # Fetch results separately for each topic
    topic_results: Dict[str, List[Dict[str, Any]]] = {}
    
    for topic, count in top_topics:
        es_query = {
            "size": per_topic_limit,
            "_source": ["title", "description", "source_url", "source_system", "matched_subjects", "resource_id"],
            "query": {
                "multi_match": {
                    "query": topic,
                    "fields": ["title^2", "matched_subjects.subject_name^3", "description"],
                    "type": "best_fields"
                }
            }
        }
        
        try:
            resp = requests.post(
                f"{ES_HOST}/{ES_INDEX}/_search",
                json=es_query,
                timeout=5
            )
            resp.raise_for_status()
            data = resp.json()
            
            results = []
            for hit in data.get("hits", {}).get("hits", []):
                source = hit.get("_source", {})
                subjects = [s.get("subject_name", "") for s in source.get("matched_subjects", []) if isinstance(s, dict)]
                
                results.append({
                    "resource_id": source.get("resource_id", ""),
                    "title": source.get("title", "Untitled"),
                    "description": source.get("description", "")[:200] if source.get("description") else "",
                    "url": source.get("source_url", "#"),
                    "source": source.get("source_system", ""),
                    "subjects": subjects[:5],
                    "score": hit.get("_score", 0),
                    "matched_topics": [topic],  # Tag with the topic this was fetched for
                })
            
            topic_results[topic] = results
            
        except Exception as e:
            print(f"Recommendation error for topic '{topic}': {e}")
            topic_results[topic] = []
    
    # Interleave results from all topics (round-robin) to ensure diversity
    final_results = []
    seen_ids = set()
    topic_iterators = {topic: iter(results) for topic, results in topic_results.items()}
    topic_order = [t[0] for t in top_topics]  # Maintain priority order
    
    while len(final_results) < limit:
        added_any = False
        
        for topic in topic_order:
            if len(final_results) >= limit:
                break
                
            try:
                result = next(topic_iterators[topic])
                resource_id = result.get("resource_id", "")
                
                # Skip duplicates (same resource might match multiple topics)
                if resource_id and resource_id not in seen_ids:
                    seen_ids.add(resource_id)
                    final_results.append(result)
                    added_any = True
                    
            except StopIteration:
                # This topic has no more results
                continue
        
        # If no topic added any result, we're done
        if not added_any:
            break
    
    return final_results


def validate_student(student_id: str, password: str) -> Optional[str]:
    """Validate student login. Returns student name if valid, None otherwise."""
    if student_id != password:
        return None
    
    df = get_user_data()
    if df is None or df.empty:
        return None
    
    match = df[df['So_the'].astype(str) == str(student_id)]
    if match.empty:
        return None
    
    return match.iloc[0].get('Ho_ten', student_id)


def get_student_name(student_id: str) -> Optional[str]:
    """Get student name by ID."""
    df = get_user_data()
    if df is None or df.empty:
        return None
    
    match = df[df['So_the'].astype(str) == str(student_id)]
    if match.empty:
        return None
    
    return match.iloc[0].get('Ho_ten', '')


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
            "error": "Mã sinh viên không tồn tại hoặc mật khẩu không đúng.",
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
    """Main dashboard with search and recommendations."""
    student_id = request.cookies.get("student_id")
    if not student_id:
        return RedirectResponse(url="/login", status_code=302)
    
    # Get student info and topics
    student_name = get_student_name(student_id)
    topics = get_user_topics(student_id)
    
    # Get top 2 topics for highlighting
    sorted_topics = sorted(topics.items(), key=lambda x: x[1], reverse=True)
    top_topics = [t[0] for t in sorted_topics[:2]]
    
    # Get recommendations (limit 10, prioritize top 2 topics)
    recommendations = get_recommendations(topics, limit=10, top_n_topics=2)
    
    # Search results if query provided
    search_results = []
    if q:
        search_results = search_elasticsearch(q)
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "student_id": student_id,
        "student_name": student_name,
        "topics": topics,
        "top_topics": top_topics,
        "recommendations": recommendations,
        "search_results": search_results,
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


@app.get("/api/recommend/{student_id}")
async def api_recommend(student_id: str, limit: int = Query(6, ge=1, le=20)):
    """
    Example: GET /api/recommend/22142278?limit=6
    """
    global _recommend_cache
    
    # Check cache first
    cache_key = f"{student_id}:{limit}"
    if cache_key in _recommend_cache:
        cached_time, cached_result = _recommend_cache[cache_key]
        if time.time() - cached_time < RECOMMEND_CACHE_TTL:
            return cached_result
    
    topics = get_user_topics(student_id)
    if not topics:
        result = {
            "student_id": student_id,
            "topics": {},
            "total": 0,
            "results": [],
            "message": "No borrowing history found for this student"
        }
        return result
    
    results = get_recommendations(topics, limit=limit)
    result = {
        "student_id": student_id,
        "topics": dict(list(topics.items())[:5]),  # Only return top 5 topics
        "total": len(results),
        "results": results
    }
    
    # Cache result
    _recommend_cache[cache_key] = (time.time(), result)
    
    return result


@app.get("/api/recommend/by-subject/{subject}")
async def api_recommend_by_subject(
    subject: str,
    limit: int = Query(10, ge=1, le=50)
):
    """Get OER recommendations for a specific subject/topic.
    
    DSpace Integration: Use this to show related resources on item pages.
    
    Example: GET /api/recommend/by-subject/General%20principles%20of%20mathematics?limit=5
    """
    results = get_recommendations({subject: 1}, limit=limit)
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


@app.get("/api/health")
async def health_check():
    """Health check endpoint for monitoring."""
    es_ok = False
    try:
        resp = requests.get(f"{ES_HOST}/_cluster/health", timeout=5)
        es_ok = resp.status_code == 200
    except:
        pass
    
    user_data_ok = get_user_data() is not None and not get_user_data().empty
    
    return {
        "status": "healthy" if es_ok else "degraded",
        "elasticsearch": "connected" if es_ok else "disconnected",
        "user_data": "loaded" if user_data_ok else "not_loaded",
        "user_records": len(get_user_data()) if user_data_ok else 0
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
