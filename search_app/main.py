"""
Simple OER Search API using Elasticsearch.
Includes Search and Recommendations with PDF support.
"""

import os
import pandas as pd
from typing import Any, Dict, List, Optional
from datetime import timedelta

import requests
from fastapi import FastAPI, Query, Request, Form, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from minio import Minio

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
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Cache user data
_user_df: Optional[pd.DataFrame] = None
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
    
    Args:
        student_id: Mã sinh viên (VD: 22142278, 23142367)
    """
    df = get_user_data()
    if df is None or df.empty:
        return {}
    
    # Tìm theo mã sinh viên (So_the)
    user_history = df[df['So_the'].astype(str) == str(student_id)]
    if user_history.empty:
        return {}
    
    topic_counts = user_history['Chu_de'].dropna().value_counts().to_dict()
    return topic_counts


def get_recommendations(topic_counts: Dict[str, int], limit: int = 10, top_n_topics: int = 2) -> List[Dict[str, Any]]:
    """Get OER recommendations based on user's top topics.
    
    Args:
        topic_counts: Dict of topic -> borrow count
        limit: Max number of results (default 10)
        top_n_topics: Number of top topics to prioritize (default 2)
    """
    if not topic_counts:
        return []
    
    # Get top N topics by count
    sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)
    top_topics = [t[0] for t in sorted_topics[:top_n_topics]]
    
    should_clauses = []
    for topic, count in topic_counts.items():
        # Higher boost for top topics
        if topic in top_topics:
            boost_val = min(count, 5) * 2  # Double boost for top topics
        else:
            boost_val = min(count, 5)
        
        should_clauses.append({
            "multi_match": {
                "query": topic,
                "fields": ["title^3", "description", "matched_subjects.subject_name^2"],
                "fuzziness": "AUTO",
                "boost": boost_val
            }
        })
    
    es_query = {
        "size": limit,
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1
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
            subjects = [s.get("subject_name", "") for s in source.get("matched_subjects", [])]
            
            # Find which top topics this resource matches
            matched_topics = [t for t in top_topics if any(t.lower() in s.lower() for s in subjects)]
            
            results.append({
                "title": source.get("title", "Untitled"),
                "description": source.get("description", ""),
                "url": source.get("source_url", "#"),
                "source": source.get("source_system", ""),
                "subjects": subjects,
                "score": hit.get("_score", 0),
                "matched_topics": matched_topics,
            })
        
        return results
    except Exception as e:
        print(f"Recommendation error: {e}")
        return []


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


# ============ API ENDPOINTS ============

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
async def api_recommend(student_id: str, limit: int = Query(10, ge=1, le=50)):
    """REST API endpoint for recommendations by student ID (mã sinh viên)."""
    topics = get_user_topics(student_id)
    results = get_recommendations(topics, limit=limit)
    return {
        "student_id": student_id,
        "topics": topics,
        "total": len(results),
        "results": results
    }
