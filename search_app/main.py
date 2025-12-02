"""
Simple OER Search API using Elasticsearch.
Includes Search and Recommendations with PDF support.
"""

import os
import pandas as pd
from typing import Any, Dict, List, Optional
from datetime import timedelta

import requests
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from minio import Minio

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200").rstrip("/")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
RESULT_SIZE = int(os.getenv("SEARCH_RESULT_SIZE", "20"))
USER_DATA_PATH = os.getenv("USER_DATA_PATH", "/app/data/user_final.csv")

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


def get_recommendations(topic_counts: Dict[str, int], limit: int = 10) -> List[Dict[str, Any]]:
    """Get OER recommendations based on user topics."""
    if not topic_counts:
        return []
    
    should_clauses = []
    for topic, count in topic_counts.items():
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
            results.append({
                "title": source.get("title", "Untitled"),
                "description": source.get("description", ""),
                "url": source.get("source_url", "#"),
                "source": source.get("source_system", ""),
                "subjects": [s.get("subject_name", "") for s in source.get("matched_subjects", [])],
                "score": hit.get("_score", 0),
            })
        
        return results
    except Exception as e:
        print(f"Recommendation error: {e}")
        return []


@app.get("/", response_class=HTMLResponse)
async def search_page(request: Request, q: Optional[str] = Query(None)):
    """Main search page."""
    results = search_elasticsearch(q) if q else []
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "query": q or "",
        "results": results,
        "total": len(results)
    })


@app.get("/recommend", response_class=HTMLResponse)
async def recommend_page(request: Request, student_id: Optional[str] = Query(None)):
    """Recommendation page for a student."""
    topics = {}
    results = []
    student_name = None
    
    if student_id:
        topics = get_user_topics(student_id)
        results = get_recommendations(topics, limit=15)
        # Lấy tên sinh viên
        df = get_user_data()
        if df is not None and not df.empty:
            match = df[df['So_the'].astype(str) == str(student_id)]
            if not match.empty:
                student_name = match.iloc[0].get('Ho_ten', '')
    
    return templates.TemplateResponse("recommend.html", {
        "request": request,
        "student_id": student_id,
        "student_name": student_name,
        "topics": topics,
        "results": results,
        "total": len(results)
    })


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
