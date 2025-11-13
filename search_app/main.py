from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200").rstrip("/")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
RESULT_SIZE = int(os.getenv("SEARCH_APP_RESULT_SIZE", "20"))
MINIO_BROWSER = os.getenv("MINIO_BROWSER_ENDPOINT", "http://localhost:9000/browser")
BUCKET = os.getenv("MINIO_BUCKET", "oer-lakehouse")

app = FastAPI(title="OER Search Portal")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


def build_query(
    query: Optional[str],
    source_system: Optional[str],
    program_id: Optional[int],
    language: Optional[str],
) -> Dict[str, Any]:
    must: List[Dict[str, Any]] = []
    filter_clause: List[Dict[str, Any]] = []

    if query:
        must.append(
            {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "description^2", "search_text", "pdf_text"],
                }
            }
        )
    else:
        must.append({"match_all": {}})

    if source_system:
        filter_clause.append({"term": {"source_system": source_system}})

    if program_id is not None:
        filter_clause.append({"term": {"program_ids": program_id}})

    if language:
        filter_clause.append({"term": {"language": language}})

    return {
        "size": RESULT_SIZE,
        "query": {
            "bool": {
                "must": must,
                "filter": filter_clause,
            }
        },
        "highlight": {
            "fields": {
                "description": {},
                "pdf_text": {"fragment_size": 150, "number_of_fragments": 2},
            }
        },
    }


def build_pdf_url(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    normalized = path.lstrip("/")
    if normalized.startswith("s3a://") or normalized.startswith("s3://"):
        parts = normalized.split("/", 3)
        if len(parts) >= 4:
            bucket = parts[2]
            key = parts[3]
        else:
            return None
    else:
        bucket = BUCKET
        key = normalized
    return f"{MINIO_BROWSER}/{bucket}/{key}"


@app.get("/", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: Optional[str] = Query(None, description="Search query"),
    source_system: Optional[str] = Query(None),
    program_id: Optional[str] = Query(None),
    language: Optional[str] = Query(None),
):
    program_id_value: Optional[int]
    if program_id is None or program_id == "":
        program_id_value = None
    else:
        try:
            program_id_value = int(program_id)
        except (TypeError, ValueError):
            program_id_value = None

    query = build_query(q, source_system, program_id_value, language)
    url = f"{ES_HOST}/{ES_INDEX}/_search"
    try:
        response = requests.post(url, json=query, timeout=30)
        response.raise_for_status()
        payload = response.json()
        hits = payload.get("hits", {}).get("hits", [])
    except Exception as exc:  # pragma: no cover - purely UI feedback
        hits = []
        payload = {"error": str(exc)}

    results: List[Dict[str, Any]] = []
    for hit in hits:
        source = hit.get("_source", {})
        highlight = hit.get("highlight", {})
        pdf_files = source.get("pdf_files") or []
        pdf_links = [build_pdf_url(item) for item in pdf_files]
        pdf_titles = source.get("pdf_titles") or []
        pdf_bundle = list(zip(pdf_titles, pdf_links, pdf_files))
        results.append(
            {
                "title": source.get("title"),
                "url": source.get("source_url"),
                "score": hit.get("_score"),
                "source_system": source.get("source_system"),
                "language": source.get("language"),
                "matched_subjects": source.get("matched_subjects", []),
                "programs": source.get("programs", []),
                "publisher": source.get("publisher_name"),
                "pdf_assets": pdf_bundle,
                "description": source.get("description"),
                "highlight": highlight,
            }
        )

    context = {
        "request": request,
        "query": q or "",
        "source_system": source_system or "",
        "language": language or "",
        "program_id": program_id or "",
        "results": results,
        "total": len(results),
        "payload": payload,
    }
    return templates.TemplateResponse("index.html", context)
