from __future__ import annotations

import os
import re
from typing import Any, Dict, List

try:
    import numpy as np
    from sklearn.feature_extraction.text import TfidfVectorizer

    SKLEARN_AVAILABLE = True
except ImportError:
    np = None  # type: ignore
    TfidfVectorizer = None  # type: ignore
    SKLEARN_AVAILABLE = False


class MultilingualExtractiveSummarizer:
    """Fast extractive summarization that works for both Vietnamese and English."""

    def __init__(self) -> None:
        self.min_sentence_words = 5
        self.min_sentence_chars = 24

    def split_sentences(self, text: str) -> List[str]:
        if not text:
            return []
        normalized = re.sub(r"\s+", " ", text.replace("\r", "\n")).strip()
        if not normalized:
            return []

        parts = re.split(r"(?<=[\.\!\?])\s+|\n+", normalized)
        sentences: List[str] = []
        for part in parts:
            s = part.strip(" -\t")
            if not s:
                continue
            if len(s) < self.min_sentence_chars:
                continue
            if len(re.findall(r"\w+", s, flags=re.UNICODE)) < self.min_sentence_words:
                continue
            if self._is_noisy_sentence(s):
                continue
            sentences.append(s)
        return sentences

    def _is_noisy_sentence(self, sentence: str) -> bool:
        # Drop OCR noise / table fragments where alpha coverage is too low.
        if not sentence:
            return True
        alpha = len(re.findall(r"[A-Za-zÀ-ỹ]", sentence))
        return alpha / max(1, len(sentence)) < 0.45

    def generate_extractive_summary(
        self,
        text: str,
        max_sentences: int = 5,
        max_chars: int = 800,
    ) -> str:
        sentences = self.split_sentences(text)
        if not sentences:
            return ""
        if len(sentences) <= max_sentences:
            return self._truncate(" ".join(sentences), max_chars)

        if not SKLEARN_AVAILABLE:
            return self._truncate(" ".join(sentences[:max_sentences]), max_chars)

        try:
            # Char n-grams are language-agnostic and work better for VI/EN mixed text.
            vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=1)
            matrix = vectorizer.fit_transform(sentences)
            scores = matrix.sum(axis=1).A1
            top_indices = list(np.argsort(scores)[::-1][:max_sentences])  # type: ignore[arg-type]
            top_indices.sort()
            selected = [sentences[i] for i in top_indices]
            return self._truncate(" ".join(selected), max_chars)
        except Exception:
            return self._truncate(" ".join(sentences[:max_sentences]), max_chars)

    def generate_chapter_summary(
        self,
        chapter_text: str,
        chapter_title: str,
        max_chars: int = 800,
    ) -> str:
        summary = self.generate_extractive_summary(
            chapter_text,
            max_sentences=4,
            max_chars=max_chars,
        )
        if not summary:
            return chapter_title[:max_chars]
        return self._truncate(f"{chapter_title}: {summary}", max_chars)

    def generate_document_summary(
        self,
        resource: Dict[str, Any],
        toc: List[Dict[str, Any]],
        max_chars: int = 1000,
    ) -> str:
        parts: List[str] = []
        asset_title = self._derive_asset_title(resource)
        title = str(resource.get("title") or "").strip()
        description = str(resource.get("description") or "").strip()

        if asset_title:
            parts.append(asset_title)
        if title:
            if not asset_title or self._normalize_label(asset_title) != self._normalize_label(title):
                parts.append(f"Course: {title}")
        if description:
            parts.append(description[:350])
        if toc:
            chapter_titles = [str(ch.get("chapter_title") or "").strip() for ch in toc[:10]]
            chapter_titles = [c for c in chapter_titles if c]
            if chapter_titles:
                parts.append(f"Contents: {'; '.join(chapter_titles)}")

        matched_subjects = resource.get("matched_subjects") or []
        if isinstance(matched_subjects, list) and matched_subjects:
            topics: List[str] = []
            for item in matched_subjects[:6]:
                if isinstance(item, dict):
                    topic = str(item.get("subject_name") or item.get("subject_name_en") or "").strip()
                    if topic:
                        topics.append(topic)
            if topics:
                parts.append(f"Topics: {', '.join(topics)}")

        joined = ". ".join([p for p in parts if p]).strip()
        return self._truncate(joined, max_chars)

    def _derive_asset_title(self, resource: Dict[str, Any]) -> str:
        file_name = str(resource.get("file_name") or "").strip()
        asset_path = str(resource.get("asset_path") or "").strip()

        candidate = file_name or os.path.basename(asset_path)
        candidate = re.sub(r"\.[A-Za-z0-9]+$", "", candidate).strip()
        candidate = candidate.replace("_", " ").replace("-", " ")
        candidate = re.sub(r"%20", " ", candidate, flags=re.IGNORECASE)
        candidate = re.sub(r"\s+", " ", candidate).strip(" .-_")
        return candidate[:220]

    def _normalize_label(self, value: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
        return re.sub(r"\s+", " ", normalized).strip()

    def _truncate(self, text: str, max_chars: int) -> str:
        if len(text) <= max_chars:
            return text
        truncated = text[:max_chars].rstrip()
        # Prefer sentence boundary when possible.
        boundary = max(truncated.rfind(". "), truncated.rfind("? "), truncated.rfind("! "))
        if boundary > max_chars // 2:
            return truncated[: boundary + 1].strip()
        return truncated
