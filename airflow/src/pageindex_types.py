"""Shared datatypes for the PageIndex engine (extracted from pageindex.py)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class QueryBundle:
    query_vi_original: str
    query_en_semantic: str
    query_vi_semantic: str
    keywords_en: List[str]
    keywords_vi: List[str]
    intent: str
    language: str
    query_mode: str
    course_name: str = ""
    section_name: str = ""
    document_title: str = ""
    concept_target: str = ""
    concept_target_en: str = ""
    has_unresolved_placeholder: bool = False


class PageIndexError(RuntimeError):
    pass
