"""
Chatbot API - PageIndex only mode.

This service intentionally disables vector database / embedding retrieval.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel, Field

try:
    from pageindex import PageIndexEngine, PageIndexError
except ImportError:
    try:
        from src.pageindex import PageIndexEngine, PageIndexError  # type: ignore
    except ImportError:
        PageIndexEngine = None  # type: ignore

        class PageIndexError(RuntimeError):
            pass


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_pageindex_engine = None


app = FastAPI(title="OER Chatbot API (PageIndex)")
allowed_origins_env = os.getenv("CHATBOT_API_ALLOWED_ORIGINS", "*").strip()
api_key_required = os.getenv("CHATBOT_API_KEY", "").strip()
allowed_origins: List[str]
if allowed_origins_env == "*":
    allowed_origins = ["*"]
else:
    allowed_origins = [origin.strip() for origin in allowed_origins_env.split(",") if origin.strip()]
    if not allowed_origins:
        allowed_origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=allowed_origins != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1)
    top_k: int = Field(5, ge=1, le=12)
    source_system: Optional[str] = None
    language: Optional[str] = None
    course_id: Optional[int] = None
    section_id: Optional[int] = None
    activity_id: Optional[int] = None
    role: Optional[str] = None
    course_name: Optional[str] = None
    section_name: Optional[str] = None
    activity_name: Optional[str] = None
    page_url: Optional[str] = None


def _contextualize_question(payload: AskRequest) -> str:
    context_lines: List[str] = []
    if payload.course_id is not None:
        context_lines.append(f"course_id={payload.course_id}")
    if payload.course_name:
        context_lines.append(f"course_name={payload.course_name}")
    if payload.section_id is not None:
        context_lines.append(f"section_id={payload.section_id}")
    if payload.section_name:
        context_lines.append(f"section_name={payload.section_name}")
    if payload.activity_id is not None:
        context_lines.append(f"activity_id={payload.activity_id}")
    if payload.activity_name:
        context_lines.append(f"activity_name={payload.activity_name}")
    if payload.role:
        context_lines.append(f"role={payload.role}")
    if payload.page_url:
        context_lines.append(f"page_url={payload.page_url}")

    if not context_lines:
        return payload.question
    return (
        f"{payload.question}\n\n"
        "[Moodle context]\n"
        + "\n".join(f"- {line}" for line in context_lines)
    )


class DebugGetDocumentRequest(BaseModel):
    question: str = Field(..., min_length=3)
    top_k: int = Field(5, ge=1, le=12)
    source_system: Optional[str] = None
    language: Optional[str] = None
    reason: str = Field("Debug retrieval: chọn tài liệu ứng viên từ metadata và TOC.")


class DebugGetDocumentStructureRequest(BaseModel):
    asset_uid: str = Field(..., min_length=8)
    reason: str = Field("Debug retrieval: đọc cấu trúc tài liệu để khoanh vùng section.")


class DebugGetPageContentRequest(BaseModel):
    asset_uid: str = Field(..., min_length=8)
    pages: str = Field(..., description='Page expression: "x-y" | "x,y" | "x"')
    reason: str = Field("Debug retrieval: đọc range trang hẹp để lấy bằng chứng trực tiếp.")


def _get_pageindex_engine():
    global _pageindex_engine
    if _pageindex_engine is None:
        if PageIndexEngine is None:
            raise PageIndexError("PageIndex engine chưa khả dụng trong môi trường hiện tại.")
        _pageindex_engine = PageIndexEngine()
    return _pageindex_engine


def _api_base_url() -> str:
    return os.getenv("CHATBOT_PUBLIC_BASE_URL", "http://localhost:18088").rstrip("/")


def _build_proxy_pdf_url(asset_uid: str, page: Optional[int] = None) -> str:
    base = f"{_api_base_url()}/api/pdf/{asset_uid}"
    if page is not None and page > 0:
        return f"{base}#page={int(page)}"
    return base


@app.get("/api/health")
async def health() -> Dict[str, Any]:
    if PageIndexEngine is None:
        raise HTTPException(status_code=503, detail="PageIndex engine unavailable in current environment.")
    return {
        "status": "ok",
        "retrieval_mode": "pageindex",
        "vector_db_enabled": False,
        "engine_initialized": _pageindex_engine is not None,
        "api_key_required": bool(api_key_required),
    }


@app.post("/api/ask")
async def ask_api(payload: AskRequest, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")) -> Dict[str, Any]:
    try:
        if api_key_required and x_api_key != api_key_required:
            raise HTTPException(status_code=401, detail="Invalid or missing API key.")
        engine = _get_pageindex_engine()
        result = engine.ask(
            question=_contextualize_question(payload),
            top_k=payload.top_k,
            source_system=payload.source_system,
            language=payload.language,
        )
        result["moodle_context"] = {
            "course_id": payload.course_id,
            "section_id": payload.section_id,
            "activity_id": payload.activity_id,
            "role": payload.role,
            "course_name": payload.course_name,
            "section_name": payload.section_name,
            "activity_name": payload.activity_name,
            "page_url": payload.page_url,
        }
        if "sources" not in result:
            result["sources"] = engine._build_sources(result.get("contexts") or [])
        normalized_sources: List[Dict[str, Any]] = []
        for src in result.get("sources") or []:
            src_obj = dict(src or {})
            asset_uid = str(src_obj.get("asset_uid") or "").strip()
            page_no = src_obj.get("page")
            if asset_uid:
                src_obj["url"] = _build_proxy_pdf_url(asset_uid, page_no if isinstance(page_no, int) else None)
            normalized_sources.append(src_obj)
        result["sources"] = normalized_sources
        return result
    except requests.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Upstream HTTP error: {exc}") from exc
    except PageIndexError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/pdf/{asset_uid}")
async def pdf_proxy_api(asset_uid: str, page: Optional[int] = None) -> Response:
    try:
        engine = _get_pageindex_engine()
        meta = engine._get_document_meta(asset_uid)  # pylint: disable=protected-access
        if not meta:
            raise HTTPException(status_code=404, detail="asset_uid not found.")
        asset_path = str(meta.get("asset_path") or "").strip()
        if not asset_path:
            raise HTTPException(status_code=404, detail="asset_path missing for asset_uid.")
        pdf_bytes = engine._get_pdf_bytes(asset_path)  # pylint: disable=protected-access
        filename = os.path.basename(asset_path) or f"{asset_uid}.pdf"
        headers = {
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "private, max-age=300",
        }
        if page is not None and page > 0:
            headers["X-PDF-Page"] = str(int(page))
        return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)
    except HTTPException:
        raise
    except PageIndexError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/debug/get_document")
async def debug_get_document_api(payload: DebugGetDocumentRequest) -> Dict[str, Any]:
    try:
        engine = _get_pageindex_engine()
        return engine.get_document(
            question=payload.question,
            top_k=payload.top_k,
            source_system=payload.source_system,
            language=payload.language,
            reason=payload.reason,
        )
    except PageIndexError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/debug/tier1_candidates")
async def debug_tier1_candidates_api(payload: DebugGetDocumentRequest) -> Dict[str, Any]:
    try:
        engine = _get_pageindex_engine()
        result = engine.get_document(
            question=payload.question,
            top_k=payload.top_k,
            source_system=payload.source_system,
            language=payload.language,
            reason=payload.reason,
        )
        return {
            "tool": "tier1_candidates",
            "reason": payload.reason,
            "query_bundle": result.get("query_bundle"),
            "subject_hints": result.get("subject_hints"),
            "tier1": result.get("tier1"),
            "documents": result.get("documents"),
        }
    except PageIndexError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/debug/get_document_structure")
async def debug_get_document_structure_api(payload: DebugGetDocumentStructureRequest) -> Dict[str, Any]:
    try:
        engine = _get_pageindex_engine()
        return engine.get_document_structure(
            asset_uid=payload.asset_uid,
            reason=payload.reason,
        )
    except PageIndexError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/debug/get_page_content")
async def debug_get_page_content_api(payload: DebugGetPageContentRequest) -> Dict[str, Any]:
    try:
        engine = _get_pageindex_engine()
        return engine.get_page_content(
            asset_uid=payload.asset_uid,
            pages=payload.pages,
            reason=payload.reason,
        )
    except PageIndexError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/debug/pageindex_config")
async def debug_pageindex_config_api() -> Dict[str, Any]:
    try:
        engine = _get_pageindex_engine()
        return engine.get_runtime_config()
    except PageIndexError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/debug/local_llm")
async def debug_local_llm_api() -> Dict[str, Any]:
    try:
        engine = _get_pageindex_engine()
        return engine.debug_local_llm()
    except PageIndexError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
