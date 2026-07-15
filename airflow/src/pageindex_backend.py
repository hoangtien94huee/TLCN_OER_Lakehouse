"""Spark / MinIO / cache / LLM backend.

Mixin tach tu pageindex.py (behaviour-preserving). Cac method van thao tac tren `self`
cua PageIndexEngine; config/state duoc khoi tao o PageIndexEngine.__init__.
"""
from __future__ import annotations

from collections import OrderedDict
from datetime import timedelta
import json
import logging
import os
import re
import socket
import struct
import time
import unicodedata
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


try:
    from minio import Minio

    MINIO_AVAILABLE = True
except ImportError:
    Minio = None  # type: ignore
    MINIO_AVAILABLE = False

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    SPARK_AVAILABLE = True
except ImportError:
    SparkSession = None  # type: ignore
    F = None  # type: ignore
    SPARK_AVAILABLE = False

try:
    import pdfplumber

    PDFPLUMBER_AVAILABLE = True
except ImportError:
    pdfplumber = None  # type: ignore
    PDFPLUMBER_AVAILABLE = False

try:
    from pypdf import PdfReader as PyPDFReader

    PYPDF_AVAILABLE = True
except ImportError:
    try:
        import pypdf

        PyPDFReader = pypdf.PdfReader
        PYPDF_AVAILABLE = True
    except ImportError:
        PyPDFReader = None  # type: ignore
        PYPDF_AVAILABLE = False


logger = logging.getLogger(__name__)

from pageindex_types import PageIndexError, QueryBundle
from pageindex_helpers import (
    _strip_surrogate_chars,
    _normalize_pdf_text,
    _ascii_fold,
    _tokenize,
    _derive_en_keywords_from_vi,
    _derive_vi_keywords_from_en,
    _detect_query_language,
    _detect_lang,
    _parse_moodle_context,
    _strip_moodle_context,
    _detect_recommendation_intent,
    _is_recommendation_query,
    _detect_find_material_intent,
    _detect_query_intent,
    _is_definition_query,
    _extract_definition_target,
    _contains_unresolved_placeholder,
    _is_implicit_concept_placeholder,
    _extract_course_name_hint,
    _extract_document_title_hint,
    _extract_section_name_hint,
    _build_course_scope_profile,
    _evaluate_course_scope_text,
    _extract_requested_concept,
    _has_example_cue,
    _has_definition_cue,
    _has_targeted_definition_cue,
    _is_english_dominant_text,
    _estimate_transcript_noise,
    _resolve_answer_language,
    _message_no_relevant,
    _message_unresolved_concept,
    _message_insufficient_scope,
    _is_obviously_out_of_scope,
    _message_no_document,
    _message_time_budget,
    _overlap_score,
    _estimate_formula_density,
    _estimate_garbled_text_ratio,
    _dedupe_keep_order,
    _detect_default_gateway_ipv4,
    _safe_json_loads,
    _to_python,
    _clamp_page_range,
    _range_to_expr,
    _parse_pages_expr,
    _collapse_pages,
    _phrase_overlap,
    _recommendation_generic_terms,
    _extract_subject_focus_terms,
    _extract_subject_focus_phrases,
    _expand_definition_target_tokens,
)


class _BackendMixin:
    """Spark / MinIO / cache / LLM backend."""

    def _create_spark_session(self) -> SparkSession:
        if not SPARK_AVAILABLE:
            raise PageIndexError("PySpark chưa sẵn sàng cho PageIndex.")

        java_home = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
        os.environ.setdefault("JAVA_HOME", java_home)
        os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.pop("JAVA_TOOL_OPTIONS", None)

        endpoint = self.minio_endpoint
        if not endpoint.startswith(("http://", "https://")):
            endpoint = f"http://{endpoint}"

        builder = SparkSession.builder.appName("OER-PageIndex-API").master(self.spark_master)
        spark_jars = os.getenv("SPARK_JARS")
        use_local_jars = False
        if spark_jars:
            jar_paths = [p.strip() for p in spark_jars.split(",") if p.strip()]
            use_local_jars = bool(jar_paths) and all(Path(p).exists() for p in jar_paths)
            if use_local_jars:
                builder = (
                    builder
                    .config("spark.jars", spark_jars)
                    .config("spark.driver.extraClassPath", spark_jars)
                    .config("spark.executor.extraClassPath", spark_jars)
                )
        if not use_local_jars:
            builder = (
                builder
                .config(
                    "spark.jars.packages",
                    ",".join(
                        [
                            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
                            "org.apache.hadoop:hadoop-aws:3.3.4",
                            "com.amazonaws:aws-java-sdk-bundle:1.12.565",
                        ]
                    ),
                )
                .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/.ivy2"))
            )

        return (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.catalog_name}.warehouse", f"s3a://{self.bucket}/silver/")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.driver.host", self.spark_driver_host)
            .config("spark.driver.bindAddress", self.spark_driver_bind_address)
            .config("spark.driver.memory", os.getenv("CHATBOT_SPARK_DRIVER_MEMORY", "2g"))
            .config("spark.executor.memory", os.getenv("CHATBOT_SPARK_EXECUTOR_MEMORY", "2g"))
            .config("spark.driver.maxResultSize", os.getenv("CHATBOT_SPARK_DRIVER_MAXRESULTSIZE", "1g"))
            .config("spark.sql.shuffle.partitions", os.getenv("CHATBOT_SPARK_SHUFFLE_PARTITIONS", "8"))
            .getOrCreate()
        )

    def _get_spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = self._create_spark_session()
        return self._spark

    def _get_minio_client(self) -> Minio:
        if not MINIO_AVAILABLE:
            raise PageIndexError("MinIO client chưa sẵn sàng cho PageIndex.")
        if self._minio_client is None:
            endpoint = self.minio_endpoint
            secure = self.minio_secure
            if endpoint.startswith("http://"):
                endpoint = endpoint[7:]
            elif endpoint.startswith("https://"):
                endpoint = endpoint[8:]
                secure = True
            self._minio_client = Minio(
                endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=secure,
            )
        return self._minio_client

    def _get_minio_signer_client(self) -> Minio:
        """Create a MinIO client for presigning against public host."""
        if not MINIO_AVAILABLE:
            raise PageIndexError("MinIO client chưa sẵn sàng cho PageIndex.")
        if self._minio_signer_client is not None:
            return self._minio_signer_client
        public_base = str(self.minio_public_base_url or "").strip()
        if not public_base:
            return self._get_minio_client()
        parsed = urlparse(public_base)
        endpoint = parsed.netloc or parsed.path
        if not endpoint:
            return self._get_minio_client()
        secure = (parsed.scheme or "").lower() == "https"
        self._minio_signer_client = Minio(
            endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=secure,
            region="us-east-1",
        )
        return self._minio_signer_client

    def _table_exists(self, table_name: str) -> bool:
        try:
            self._get_spark().table(table_name)
            return True
        except Exception:
            return False

    def _load_reference_cache(self) -> Dict[str, Any]:
        if self._reference_cache is not None:
            return self._reference_cache

        spark = self._get_spark()
        subjects: List[Dict[str, Any]] = []
        links: List[Dict[str, Any]] = []
        if self._table_exists(self.reference_subjects_table):
            subjects = [_to_python(row) for row in spark.table(self.reference_subjects_table).collect()]
        if self._table_exists(self.reference_program_subject_links_table):
            links = [_to_python(row) for row in spark.table(self.reference_program_subject_links_table).collect()]

        programs_by_subject: Dict[int, List[int]] = {}
        for link in links:
            try:
                subject_id = int(link.get("subject_id"))
                program_id = int(link.get("program_id"))
            except Exception:
                continue
            programs_by_subject.setdefault(subject_id, [])
            if program_id not in programs_by_subject[subject_id]:
                programs_by_subject[subject_id].append(program_id)

        self._reference_cache = {
            "subjects": subjects,
            "programs_by_subject": programs_by_subject,
        }
        return self._reference_cache

    def _cache_get(self, cache: "OrderedDict[str, Any]", key: str) -> Any:
        if not self.cache_enabled:
            return None
        cache_key = str(key or "")
        if cache_key not in cache:
            return None
        value = cache.pop(cache_key)
        cache[cache_key] = value
        return value

    def _cache_set(self, cache: "OrderedDict[str, Any]", key: str, value: Any) -> None:
        if not self.cache_enabled:
            return
        cache_key = str(key or "")
        if cache_key in cache:
            cache.pop(cache_key)
        cache[cache_key] = value
        while len(cache) > self.cache_max_items:
            cache.popitem(last=False)

    def _local_llm_enabled(self) -> bool:
        return bool(self.local_llm_base_url and self.local_llm_model) and not self._local_llm_unavailable

    def _openai_chat_completions_url(self) -> str:
        if self.local_llm_api_url:
            return self.local_llm_api_url.rstrip("/")
        base = self.local_llm_base_url.rstrip("/")
        if base.endswith("/chat/completions"):
            return base
        if base.endswith("/v1"):
            return f"{base}/chat/completions"
        return f"{base}/v1/chat/completions"

    def _openai_models_url(self, base_url: str) -> str:
        base = base_url.rstrip("/")
        if base.endswith("/models"):
            return base
        if base.endswith("/v1"):
            return f"{base}/models"
        return f"{base}/v1/models"

    def _gemini_generate_content_url(self, base_url: str) -> str:
        if self.local_llm_api_url:
            return self.local_llm_api_url.rstrip("/")
        base = base_url.rstrip("/")
        return f"{base}/models/{self.local_llm_model}:generateContent"

    def _probe_local_llm_base_url(self, base_url: str) -> Tuple[bool, str]:
        base = str(base_url or "").strip().rstrip("/")
        if not base:
            return False, "empty base url"
        timeout = (self.local_llm_connect_timeout, self.local_llm_probe_timeout)
        try:
            headers = {"ngrok-skip-browser-warning": "1"}
            if self.local_llm_health_url:
                health_url = self.local_llm_health_url
                if "{base_url}" in health_url:
                    endpoint = health_url.replace("{base_url}", base)
                elif health_url.startswith("http://") or health_url.startswith("https://"):
                    endpoint = health_url
                elif health_url.startswith("/"):
                    endpoint = f"{base}{health_url}"
                else:
                    endpoint = f"{base}/{health_url}"
                if self.local_llm_api_key:
                    headers["Authorization"] = f"Bearer {self.local_llm_api_key}"
                response = requests.get(endpoint, headers=headers, timeout=timeout, verify=False)
            elif self.local_llm_backend == "gemini":
                endpoint = f"{base}/models"
                params: Dict[str, str] = {}
                if self.local_llm_api_key:
                    params["key"] = self.local_llm_api_key
                response = requests.get(endpoint, headers=headers, params=params, timeout=timeout, verify=False)
            elif self.local_llm_backend in {"vllm", "openai", "openai_compat", "api", "api_openai", "groq"}:
                endpoint = self._openai_models_url(base)
                if self.local_llm_api_key:
                    headers["Authorization"] = f"Bearer {self.local_llm_api_key}"
                response = requests.get(endpoint, headers=headers, timeout=timeout, verify=False)
            else:
                endpoint = f"{base}/api/tags"
                response = requests.get(endpoint, headers=headers, timeout=timeout, verify=False)
            if response.ok:
                return True, ""
            return False, f"HTTP {response.status_code}"
        except requests.RequestException as exc:
            return False, str(exc)

    def _ensure_local_llm_endpoint(self) -> None:
        if self._local_llm_checked:
            return
        self._local_llm_checked = True
        candidates = _dedupe_keep_order([self.local_llm_base_url] + self.local_llm_fallback_base_urls)
        last_error = ""
        for candidate in candidates:
            ok, error = self._probe_local_llm_base_url(candidate)
            if ok:
                if candidate != self.local_llm_base_url:
                    logger.warning("Switch LOCAL_LLM_BASE_URL to reachable endpoint: %s", candidate)
                self.local_llm_base_url = candidate
                self._local_llm_unavailable = False
                self._local_llm_last_error = ""
                return
            last_error = f"{candidate} -> {error}"

        self._local_llm_last_error = last_error or "No reachable local LLM endpoint"
        if self.local_llm_probe_required:
            self._local_llm_unavailable = True
        else:
            # Non-strict probe: allow direct call even when health/models probe is unavailable.
            self._local_llm_unavailable = False
            logger.warning("Local LLM probe failed, will try direct API call. detail=%s", self._local_llm_last_error)

    def _classify_intent_with_llm(self, question: str, timeout: int = 6) -> str:
        """Zero-shot intent classifier using the local LLM.
        Returns one of: recommendation | definition | find_material | listing | general | out_of_scope.
        Called when pattern-based detection is ambiguous.

        Intent definitions:
          - recommendat ion: user asks for book/document suggestions for a course/subject (no specific topic)
          - definition: user asks what a concept means (is, explain, define)
          - find_material: user asks which document covers a specific topic
          - listing: user asks to enumerate items/properties/types
          - general: academic question not matching above categories
          - out_of_scope: NOT an academic/educational question (food, games, sports, shopping, travel, personal advice, etc.)
        """
        if not self._local_llm_enabled():
            return "general"
        prompt = (
            "You are an intent classifier for an OER (Open Educational Resources) academic chatbot.\n"
            "Classify the user question into EXACTLY ONE intent label.\n\n"
            "Intent labels:\n"
            "- recommendation: asking for book, document, or learning resource suggestions for a general course or subject as a whole (e.g., 'cho tôi tài liệu học môn sinh học', 'gợi ý tài liệu giải tích', 'tìm sách triết học')\n"
            "- definition: asking what a concept/term means or to explain it\n"
            "- find_material: asking which specific document/book covers a particular topic, concept, or chapter (e.g., 'tài liệu nào nói về đạo hàm', 'sách nào có chương về tích phân')\n"
            "- listing: asking to list properties, types, examples, or components\n"
            "- general: an academic question not matching above\n"
            "- out_of_scope: NOT academic — food, cooking, games, sports, entertainment, shopping, travel, personal advice, current events, etc.\n\n"
            "Examples:\n"
            "Q: gợi ý tài liệu cho môn học này → recommendation\n"
            "Q: tài liệu nào phù hợp cho môn học này → recommendation\n"
            "Q: sách nào thích hợp cho khóa học này → recommendation\n"
            "Q: cho tôi tài liệu học môn sinh học → recommendation\n"
            "Q: tôi muốn tìm tài liệu tự học triết học → recommendation\n"
            "Q: đạo hàm là gì → definition\n"
            "Q: explain what a derivative is → definition\n"
            "Q: tài liệu nào nói về tích phân → find_material\n"
            "Q: which book covers linear regression → find_material\n"
            "Q: tính chất của ma trận là gì → listing\n"
            "Q: list the types of joins in SQL → listing\n"
            "Q: tôi nên học gì trước → general\n"
            "Q: công thức nấu phở → out_of_scope\n"
            "Q: gợi ý phim hay → out_of_scope\n"
            "Q: game nào hay nhất 2024 → out_of_scope\n"
            "Q: kết quả bóng đá hôm nay → out_of_scope\n"
            "Q: giá vàng hôm nay → out_of_scope\n"
            "Q: làm sao để giảm cân → out_of_scope\n\n"
            f"Question: {question}\n"
            "Answer with the intent label only (one word, no explanation):"
        )
        try:
            raw = self._call_local_llm(prompt, request_timeout=timeout)
            raw = raw.strip().lower().split()[0] if raw.strip() else ""
            # Normalize common model quirks
            raw = raw.rstrip(".,;:!?\"'")
            valid = {"recommendation", "definition", "find_material", "listing", "general", "out_of_scope"}
            if raw in valid:
                return raw
            # Fuzzy match fallbacks
            if any(k in raw for k in ["recommend", "suggest", "sach", "goi y"]):
                return "recommendation"
            if any(k in raw for k in ["defin", "explain", "la gi"]):
                return "definition"
            if any(k in raw for k in ["find", "material", "which"]):
                return "find_material"
            if any(k in raw for k in ["list", "liet"]):
                return "listing"
            if any(k in raw for k in ["scope", "off", "ngoai"]):
                return "out_of_scope"
            return "general"
        except Exception as exc:
            logger.warning("LLM intent classification failed, defaulting to 'general'. detail=%s", exc)
            return "general"

    def _call_local_llm(
        self,
        prompt: str,
        json_mode: bool = False,
        request_timeout: Optional[int] = None,
    ) -> str:
        self._ensure_local_llm_endpoint()
        if not self._local_llm_enabled():
            raise PageIndexError("Local LLM chưa được cấu hình cho PageIndex.")

        read_timeout = self.local_llm_timeout
        if request_timeout is not None:
            read_timeout = max(1, min(int(request_timeout), self.local_llm_timeout))
        timeout = (self.local_llm_connect_timeout, read_timeout)
        if self.local_llm_backend in {"vllm", "openai", "openai_compat", "api", "api_openai", "groq"}:
            payload: Dict[str, Any] = {
                "model": self.local_llm_model,
                "temperature": 0,
                "messages": [{"role": "user", "content": prompt}],
            }
            if json_mode:
                payload["response_format"] = {"type": "json_object"}
            headers = {"Content-Type": "application/json", "ngrok-skip-browser-warning": "1"}
            if self.local_llm_api_key:
                headers["Authorization"] = f"Bearer {self.local_llm_api_key}"
            response = requests.post(
                self._openai_chat_completions_url(),
                headers=headers,
                json=payload,
                timeout=timeout,
                verify=False,
            )
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"].strip()

        if self.local_llm_backend == "ollama":
            payload = {
                "model": self.local_llm_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0},
            }
            if json_mode:
                payload["format"] = "json"
            response = requests.post(
                f"{self.local_llm_base_url}/api/generate",
                headers={"ngrok-skip-browser-warning": "1"},
                json=payload,
                timeout=timeout,
                verify=False,
            )
            response.raise_for_status()
            data = response.json()
            return str(data.get("response") or "").strip()

        if self.local_llm_backend == "gemini":
            headers = {"Content-Type": "application/json", "ngrok-skip-browser-warning": "1"}
            params: Dict[str, str] = {}
            if self.local_llm_api_key:
                params["key"] = self.local_llm_api_key
            payload: Dict[str, Any] = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0},
            }
            if json_mode:
                payload["generationConfig"]["response_mime_type"] = "application/json"
            response = requests.post(
                self._gemini_generate_content_url(self.local_llm_base_url),
                headers=headers,
                params=params,
                json=payload,
                timeout=timeout,
                verify=False,
            )
            response.raise_for_status()
            data = response.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return ""
            content = candidates[0].get("content") or {}
            parts = content.get("parts") or []
            if not parts:
                return ""
            return str(parts[0].get("text") or "").strip()

        raise PageIndexError(
            f"LOCAL_LLM_BACKEND='{self.local_llm_backend}' không hợp lệ. "
            "Giá trị hợp lệ: ollama | vllm | api | groq | gemini."
        )

    def _call_local_llm_json(
        self,
        prompt: str,
        default: Dict[str, Any],
        request_timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        try:
            raw = self._call_local_llm(prompt, json_mode=True, request_timeout=request_timeout)
            data = _safe_json_loads(raw, default)
            if isinstance(data, dict):
                return data
            return default
        except Exception as exc:
            logger.warning("Local LLM JSON call failed: %s", exc)
            if isinstance(exc, requests.RequestException):
                self._local_llm_last_error = str(exc)
                # Allow subsequent calls to re-probe fallback endpoints.
                self._local_llm_checked = False
                self._local_llm_unavailable = False
            return default
