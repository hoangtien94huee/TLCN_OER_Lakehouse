from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from minio import Minio
from urllib.parse import urlparse, urlunparse

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200").rstrip("/")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "oer_resources")
RESULT_SIZE = int(os.getenv("SEARCH_APP_RESULT_SIZE", "20"))
MIN_SCORE = float(os.getenv("SEARCH_APP_MIN_SCORE", "30"))
MINIO_BROWSER = os.getenv("MINIO_BROWSER_ENDPOINT", "http://localhost:9000/browser")
BUCKET = os.getenv("MINIO_BUCKET", "oer-lakehouse")
REF_DIR = Path(os.getenv("REFERENCE_DIR", "/app/reference"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "0").lower() in {"1", "true", "yes"}
MINIO_PUBLIC_ENDPOINT = os.getenv("MINIO_PUBLIC_ENDPOINT", "http://localhost:9000")
# Signing should use the internal service host (e.g., http://minio:9000) so the container can reach it
MINIO_SIGNING_ENDPOINT = os.getenv("MINIO_SIGNING_ENDPOINT", "http://minio:9000")

app = FastAPI(title="OER Search Portal")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


def es_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{ES_HOST}/{path.lstrip('/')}"
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_minio_client(endpoint: str) -> Minio:
    clean = endpoint.replace("http://", "").replace("https://", "")
    return Minio(
        clean,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=endpoint.startswith("https"),
    )


def build_query(
    query: Optional[str],
    program_id: Optional[int],
    subject_id: Optional[int],
) -> Dict[str, Any]:
    must: List[Dict[str, Any]] = []
    filter_clause: List[Dict[str, Any]] = []

    # Program/subject filters disabled (not present in index)

    should_clause: List[Dict[str, Any]] = []
    if query:
        should_clause.append(
            {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "title^6",
                        "title.autocomplete",
                        "description^3",
                        "search_text",
                        "pdf_text",
                    ],
                    "type": "best_fields",
                    "operator": "and",
                    "fuzziness": "AUTO",
                    "minimum_should_match": "2<75%",
                }
            }
        )
        should_clause.append({
            "match_phrase_prefix": {
                "title": {
                    "query": query,
                    "boost": 2
                }
            }
        })
        # Nested query for PDF chunks (page-level search)
        should_clause.append({
            "nested": {
                "path": "pdf_chunks",
                "query": {
                    "match": {
                        "pdf_chunks.text": {
                            "query": query,
                            "operator": "and"
                        }
                    }
                },
                "boost": 3,  # Boost nested hits significantly
                "inner_hits": {
                    "size": 3,  # Return top 3 matching pages
                    "highlight": {
                        "fields": {
                            "pdf_chunks.text": {}
                        }
                    }
                }
            }
        })
    else:
        must.append({"match_all": {}})

    base_bool = {
        "must": must,
        "filter": filter_clause,
        "should": should_clause,
        "minimum_should_match": 1 if should_clause else 0,
    }

    function_score = {
        "function_score": {
            "query": {"bool": base_bool},
            "functions": [
                {
                    "gauss": {
                        "publication_date": {
                            "origin": "now",
                            "scale": "3650d",  # 10 years decay to 0.5
                            "decay": 0.5,
                        }
                    },
                    "weight": 1.5  # Significant impact from recency
                }
            ],
            "score_mode": "multiply",
            "boost_mode": "sum",
        }
    }

    return {
        "size": RESULT_SIZE,
        "query": function_score,
        "min_score": MIN_SCORE,
        "highlight": {
            "fields": {
                "description": {},
                "pdf_text": {"fragment_size": 150, "number_of_fragments": 2},
            }
        },
    }


def fetch_faculty_programs() -> List[Dict[str, Any]]:
    """
    Build faculty -> programs -> subjects tree from local reference JSON files.
    """
    faculties_path = REF_DIR / "faculties.json"
    programs_path = REF_DIR / "programs.json"
    subjects_path = REF_DIR / "subjects.json"
    links_path = REF_DIR / "program_subject_links.json"

    if not (faculties_path.exists() and programs_path.exists() and subjects_path.exists() and links_path.exists()):
        return []

    faculties = json.loads(faculties_path.read_text(encoding="utf-8"))
    programs = json.loads(programs_path.read_text(encoding="utf-8"))
    subjects = json.loads(subjects_path.read_text(encoding="utf-8"))
    links = json.loads(links_path.read_text(encoding="utf-8"))

    subjects_by_id = {s["subject_id"]: s for s in subjects if "subject_id" in s}

    prog_subjects: Dict[int, List[Dict[str, Any]]] = {}
    for link in links:
        pid = link.get("program_id")
        sid = link.get("subject_id")
        subj = subjects_by_id.get(sid)
        if pid is None or subj is None:
            continue
        prog_subjects.setdefault(pid, []).append(subj)

    faculty_map = {f["faculty_id"]: f.get("faculty_name") for f in faculties if "faculty_id" in f}
    programs_by_faculty: Dict[int, List[Dict[str, Any]]] = {}
    for p in programs:
        fid = p.get("faculty_id")
        if fid is None:
            continue
        programs_by_faculty.setdefault(fid, []).append(p)

    tree: List[Dict[str, Any]] = []
    for fid, fname in faculty_map.items():
        prog_list = []
        for prog in sorted(programs_by_faculty.get(fid, []), key=lambda x: x.get("program_name", "")):
            pid = prog.get("program_id")
            prog_list.append(
                {
                    "program_id": pid,
                    "program_name": prog.get("program_name"),
                    "program_code": prog.get("program_code"),
                    "subjects": sorted(
                        prog_subjects.get(pid, []),
                        key=lambda s: s.get("subject_name_en") or s.get("subject_name") or "",
                    ),
                }
            )
        tree.append({"faculty_id": fid, "faculty_name": fname, "programs": prog_list})

    return sorted(tree, key=lambda x: x.get("faculty_name") or "")


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


def generate_presigned_url(path: Optional[str]) -> Optional[str]:
    import sys
    if not path:
        print(f"[PRESIGNED] Empty path provided", file=sys.stderr, flush=True)
        return None
    normalized = path.replace("s3a://", "").replace("s3://", "")
    if "/" not in normalized:
        print(f"[PRESIGNED] Invalid path format (no bucket): {normalized}", file=sys.stderr, flush=True)
        return None
    bucket, key = normalized.split("/", 1)
    try:
        # Use MINIO_PUBLIC_ENDPOINT to generate presigned URL
        # With extra_hosts in docker-compose, container can reach localhost via host-gateway
        public_endpoint = os.getenv("MINIO_PUBLIC_ENDPOINT", "http://localhost:9000")
        print(f"[PRESIGNED] Connecting to {public_endpoint}", file=sys.stderr, flush=True)
        client = get_minio_client(public_endpoint)
        presigned_url = client.presigned_get_object(bucket, key)
        
        print(f"[PRESIGNED] Success: {presigned_url[:100]}...", file=sys.stderr, flush=True)
        return presigned_url
    except Exception as e:
        print(f"[PRESIGNED] Error for {bucket}/{key}: {e}", file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return None


@app.get("/", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: Optional[str] = Query(None, description="Search query"),
    source_system: Optional[str] = Query(None),  # kept for compatibility, not shown in UI
    program_id: Optional[str] = Query(None),
    language: Optional[str] = Query(None),  # kept for compatibility, not shown in UI
):
    program_id_value: Optional[int] = None  # UI no longer uses program filter

    subject_id_value: Optional[int] = None  # subject filter removed

    # Try strict -> medium -> loose to avoid zero results
    query = build_query(q, program_id_value, subject_id_value)
    url = f"{ES_HOST}/{ES_INDEX}/_search"
    fallback_used = False
    fallback_mode = "strict"
    try:
        response = requests.post(url, json=query, timeout=30)
        response.raise_for_status()
        payload = response.json()
        hits = payload.get("hits", {}).get("hits", [])
        if not hits:
            # Loose: keyword only
            fallback_used = True
            fallback_mode = "keyword_only"
            query_loose = build_query(q, program_id_value, None)
            resp3 = requests.post(url, json=query_loose, timeout=30)
            resp3.raise_for_status()
            payload = resp3.json()
            hits = payload.get("hits", {}).get("hits", [])
    except Exception as exc:  # pragma: no cover - purely UI feedback
        hits = []
        payload = {"error": str(exc)}

    results: List[Dict[str, Any]] = []
    for hit in hits:
        score_val = hit.get("_score") or 0
        try:
            score_val = float(score_val)
        except Exception:
            score_val = 0
        if score_val < MIN_SCORE:
            continue
        source = hit.get("_source", {})
        highlight = hit.get("highlight", {})
        inner_hits = hit.get("inner_hits", {}).get("pdf_chunks", {}).get("hits", {}).get("hits", [])
        
        pdf_files = source.get("pdf_files") or []
        pdf_links = []
        for item in pdf_files:
            presigned = generate_presigned_url(item)
            pdf_links.append(presigned)
        pdf_titles = source.get("pdf_titles") or []
        pdf_bundle = list(zip(pdf_titles, pdf_links, pdf_files))
        is_textbook = source.get("source_system") == "textbook"

        # Process nested inner_hits for page-level matches
        page_hits = []
        for inner in inner_hits:
            inner_source = inner.get("_source", {})
            inner_highlight = inner.get("highlight", {})
            page_num = inner_source.get("page")
            page_text_highlight = inner_highlight.get("pdf_chunks.text", [])
            if page_text_highlight:
                page_snippet = " ... ".join(page_text_highlight)
            else:
                page_snippet = (inner_source.get("text") or "")[:150] + "..."
            
            page_hits.append({
                "page": page_num,
                "snippet": page_snippet
            })

        # Prioritize page-level snippets for PDFs
        if page_hits:
            # Show up to 2 page matches
            hit_strs = []
            for ph in page_hits[:2]:
                hit_strs.append(f"Page {ph['page']}: {ph['snippet']}")
            snippet = " | ".join(hit_strs)
        elif highlight:
            if "pdf_text" in highlight:
                snippet = " ... ".join(highlight["pdf_text"][:2])
            elif "description" in highlight:
                snippet = " ... ".join(highlight["description"][:2])

        if not snippet:
            text_source = source.get("pdf_text") or source.get("description") or ""
            if isinstance(text_source, str):
                snippet = (text_source[:300] + "...") if len(text_source) > 300 else text_source

        results.append(
            {
                "title": source.get("title"),
                "url": source.get("source_url"),
                "score": score_val,
                "source_system": source.get("source_system"),
                "language": source.get("language"),
                "matched_subjects": source.get("matched_subjects", []),
                "programs": source.get("programs", []),
                "publisher": source.get("publisher_name"),
                "publish_year": source.get("publish_year"),
                "is_textbook": is_textbook,
                "pdf_assets": pdf_bundle,
                "pdf_text": source.get("pdf_text"),
                "description": source.get("description"),
                "snippet": snippet,
                "highlight": highlight,
                "page_hits": page_hits,
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
        "reference_tree": fetch_faculty_programs(),
        "fallback_used": fallback_used,
        "fallback_mode": fallback_mode,
    }
    return templates.TemplateResponse("index.html", context)


@app.get("/facets")
async def facets():
    try:
        faculties = fetch_faculty_programs()
        return {"faculties": faculties}
    except Exception as exc:  # pragma: no cover
        return {"faculties": [], "error": str(exc)}
