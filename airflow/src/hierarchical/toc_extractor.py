from __future__ import annotations

import re
import unicodedata
from io import BytesIO
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

try:
    import PyPDF2

    PYPDF2_AVAILABLE = True
except ImportError:
    PyPDF2 = None  # type: ignore
    PYPDF2_AVAILABLE = False

try:
    import pdfplumber

    PDFPLUMBER_AVAILABLE = True
except ImportError:
    pdfplumber = None  # type: ignore
    PDFPLUMBER_AVAILABLE = False


CHAPTER_PATTERNS = [
    re.compile(r"^(chapter|chương|chuong|phần|phan)\s+([0-9]+|[ivxlcdm]+)\b", re.IGNORECASE),
    re.compile(r"^([0-9]+)\.\s+[A-ZÀ-Ỹ]", re.IGNORECASE),
    re.compile(r"^([IVXLCDM]+)\.\s+", re.IGNORECASE),
]

SECTION_PATTERNS = [
    re.compile(r"^\d+\.\d+(\.\d+)?\b"),
    re.compile(r"^(section|mục|muc)\s+\d+", re.IGNORECASE),
]


class TOCExtractor:
    """Multi-method TOC extractor with quality scoring and safe fallbacks."""

    def __init__(
        self,
        fallback_chapter_size: int = 50,
        toc_scan_pages: int = 12,
    ) -> None:
        self.fallback_chapter_size = max(10, fallback_chapter_size)
        self.toc_scan_pages = max(5, toc_scan_pages)

    def extract(self, pdf_bytes: bytes, max_pages: Optional[int] = None) -> Dict[str, Any]:
        total_pages = self._get_total_pages(pdf_bytes)
        if total_pages <= 0:
            return {
                "method": "flat",
                "confidence": 0.0,
                "toc": [],
                "total_pages": 0,
                "structure_valid": False,
            }

        process_pages = min(total_pages, max_pages) if max_pages else total_pages

        toc = self._extract_pdf_outline(pdf_bytes, process_pages)
        if self._validate_toc(toc, process_pages):
            return self._finalize("pdf_outline", 0.95, toc, process_pages)

        toc = self._extract_with_pdfplumber(pdf_bytes, process_pages)
        if self._validate_toc(toc, process_pages):
            return self._finalize("pdfplumber", 0.85, toc, process_pages)

        toc = self._extract_with_regex(pdf_bytes, process_pages)
        if self._validate_toc(toc, process_pages):
            return self._finalize("regex", 0.70, toc, process_pages)

        toc = self._generate_flat_toc(process_pages)
        return self._finalize("flat", 0.50, toc, process_pages)

    def _finalize(self, method: str, confidence: float, toc: List[Dict[str, Any]], total_pages: int) -> Dict[str, Any]:
        self._fill_end_pages(toc, total_pages)
        return {
            "method": method,
            "confidence": confidence,
            "toc": toc,
            "total_pages": total_pages,
            "structure_valid": self._validate_toc(toc, total_pages),
        }

    def _get_total_pages(self, pdf_bytes: bytes) -> int:
        if not PYPDF2_AVAILABLE:
            return 0
        try:
            reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
            return len(reader.pages)
        except Exception:
            return 0

    def _extract_pdf_outline(self, pdf_bytes: bytes, total_pages: int) -> List[Dict[str, Any]]:
        if not PYPDF2_AVAILABLE:
            return []
        try:
            reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
            outline = getattr(reader, "outline", None)
            if not outline:
                return []

            flat: List[Tuple[int, str, Optional[int]]] = []

            def flatten(items: Any, level: int = 0) -> None:
                if isinstance(items, list):
                    for item in items:
                        flatten(item, level + 1 if isinstance(item, list) else level)
                    return
                title = str(getattr(items, "title", "") or "").strip()
                if not title:
                    return
                page_num: Optional[int] = None
                try:
                    page_num = reader.get_destination_page_number(items) + 1
                except Exception:
                    page_num = None
                flat.append((level, title, page_num))

            flatten(outline)
            if not flat:
                return []

            chapters: List[Dict[str, Any]] = []
            current_chapter: Optional[Dict[str, Any]] = None
            chapter_counter = 0

            for level, title, page_num in flat:
                if page_num is None or page_num < 1 or page_num > total_pages:
                    continue
                cleaned_title = self._clean_title(title)
                if not cleaned_title:
                    continue
                chapter_like = self._is_chapter_title(cleaned_title) or current_chapter is None or level == 0

                if chapter_like:
                    chapter_counter += 1
                    chapter_number = self._extract_leading_number(cleaned_title) or chapter_counter
                    current_chapter = {
                        "chapter_id": f"ch{chapter_counter:02d}",
                        "chapter_number": chapter_number,
                        "chapter_title": cleaned_title,
                        "page_start": page_num,
                        "page_end": None,
                        "sections": [],
                    }
                    chapters.append(current_chapter)
                    continue

                if current_chapter is None:
                    continue
                section_idx = len(current_chapter["sections"]) + 1
                current_chapter["sections"].append(
                    {
                        "section_id": f"{current_chapter['chapter_id']}_sec{section_idx:02d}",
                        "section_number": f"{current_chapter['chapter_number']}.{section_idx}",
                        "section_title": cleaned_title,
                        "page_start": page_num,
                        "page_end": None,
                    }
                )

            return chapters
        except Exception:
            return []

    def _extract_with_pdfplumber(self, pdf_bytes: bytes, total_pages: int) -> List[Dict[str, Any]]:
        if not PDFPLUMBER_AVAILABLE:
            return []
        try:
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                toc_lines: List[str] = []
                scan_pages = min(total_pages, self.toc_scan_pages)
                for page_idx in range(scan_pages):
                    page_text = (pdf.pages[page_idx].extract_text() or "").strip()
                    if not page_text:
                        continue
                    if self._is_toc_page(page_text):
                        toc_lines.extend([line.strip() for line in page_text.splitlines() if line.strip()])
                if not toc_lines:
                    return []

                toc = self._parse_toc_lines(toc_lines)
                if not toc:
                    return []
                offset = self._estimate_page_offset(pdf, toc, total_pages)
                self._apply_page_offset(toc, offset, total_pages)
                return toc
        except Exception:
            return []

    def _extract_with_regex(self, pdf_bytes: bytes, total_pages: int) -> List[Dict[str, Any]]:
        if not PDFPLUMBER_AVAILABLE:
            return []
        try:
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                chapters: List[Dict[str, Any]] = []
                chapter_counter = 0
                scan_pages = min(total_pages, len(pdf.pages))
                for page_idx in range(scan_pages):
                    page_text = (pdf.pages[page_idx].extract_text() or "").strip()
                    if not page_text:
                        continue
                    page_no = page_idx + 1
                    for line in page_text.splitlines():
                        normalized = self._clean_title(line)
                        if not normalized:
                            continue
                        if self._is_chapter_title(normalized):
                            chapter_counter += 1
                            chapters.append(
                                {
                                    "chapter_id": f"ch{chapter_counter:02d}",
                                    "chapter_number": chapter_counter,
                                    "chapter_title": normalized,
                                    "page_start": page_no,
                                    "page_end": None,
                                    "sections": [],
                                }
                            )
                            break
                return chapters
        except Exception:
            return []

    def _generate_flat_toc(self, total_pages: int) -> List[Dict[str, Any]]:
        toc: List[Dict[str, Any]] = []
        chapter_num = 0
        for start_page in range(1, total_pages + 1, self.fallback_chapter_size):
            chapter_num += 1
            end_page = min(start_page + self.fallback_chapter_size - 1, total_pages)
            toc.append(
                {
                    "chapter_id": f"ch{chapter_num:02d}",
                    "chapter_number": chapter_num,
                    "chapter_title": f"Part {chapter_num}",
                    "page_start": start_page,
                    "page_end": end_page,
                    "sections": [],
                }
            )
        return toc

    def _is_toc_page(self, text: str) -> bool:
        if not text:
            return False
        lowered = self._norm_ascii(text)
        toc_terms = ["table of contents", "contents", "muc luc", "noi dung"]
        if any(term in lowered for term in toc_terms):
            return True
        dotted_lines = len(re.findall(r"\.{2,}\s*\d+\s*$", text, flags=re.MULTILINE))
        return dotted_lines >= 3

    def _parse_toc_lines(self, lines: List[str]) -> List[Dict[str, Any]]:
        toc: List[Dict[str, Any]] = []
        current_chapter: Optional[Dict[str, Any]] = None
        chapter_counter = 0

        for raw_line in lines:
            line = re.sub(r"\s+", " ", raw_line).strip()
            if not line:
                continue
            page_match = re.search(r"(\d{1,4})\s*$", line)
            if not page_match:
                continue
            page_no = int(page_match.group(1))
            head = line[: page_match.start()].rstrip(" .-\t")
            if not head:
                continue
            head = self._clean_title(head)

            if self._is_chapter_title(head):
                chapter_counter += 1
                chapter_number = self._extract_leading_number(head) or chapter_counter
                current_chapter = {
                    "chapter_id": f"ch{chapter_counter:02d}",
                    "chapter_number": chapter_number,
                    "chapter_title": head,
                    "page_start": page_no,
                    "page_end": None,
                    "sections": [],
                }
                toc.append(current_chapter)
                continue

            if self._is_section_title(head) and current_chapter is not None:
                section_idx = len(current_chapter["sections"]) + 1
                current_chapter["sections"].append(
                    {
                        "section_id": f"{current_chapter['chapter_id']}_sec{section_idx:02d}",
                        "section_number": self._extract_section_number(head) or f"{current_chapter['chapter_number']}.{section_idx}",
                        "section_title": head,
                        "page_start": page_no,
                        "page_end": None,
                    }
                )
        return toc

    def _estimate_page_offset(self, pdf: Any, toc: List[Dict[str, Any]], total_pages: int) -> int:
        offsets: List[int] = []
        scan_limit = min(total_pages, 140)
        scanned_pages: List[Tuple[int, str]] = []
        for page_idx in range(scan_limit):
            page_text = (pdf.pages[page_idx].extract_text() or "").strip()
            if not page_text:
                continue
            scanned_pages.append((page_idx + 1, self._norm_ascii(page_text)))

        for chapter in toc[:6]:
            title = self._norm_ascii(str(chapter.get("chapter_title") or ""))
            title_tokens = [t for t in re.findall(r"[a-z0-9]+", title) if len(t) > 3][:4]
            if not title_tokens:
                continue
            logical_page = int(chapter.get("page_start") or 0)
            if logical_page <= 0:
                continue
            for pdf_page, page_text in scanned_pages:
                matches = sum(1 for tok in title_tokens if tok in page_text)
                if matches >= max(1, len(title_tokens) - 1):
                    offsets.append(pdf_page - logical_page)
                    break

        if not offsets:
            return 0
        return int(round(median(offsets)))

    def _apply_page_offset(self, toc: List[Dict[str, Any]], offset: int, total_pages: int) -> None:
        for chapter in toc:
            chapter["page_start"] = self._clamp_page((chapter.get("page_start") or 1) + offset, total_pages)
            for section in chapter.get("sections", []):
                section["page_start"] = self._clamp_page((section.get("page_start") or chapter["page_start"]) + offset, total_pages)

    def _validate_toc(self, toc: List[Dict[str, Any]], total_pages: int) -> bool:
        if not toc:
            return False
        chapter_starts: List[int] = []
        for chapter in toc:
            page_start = chapter.get("page_start")
            if not isinstance(page_start, int):
                return False
            if page_start < 1 or page_start > total_pages:
                return False
            chapter_starts.append(page_start)
        if chapter_starts != sorted(chapter_starts):
            return False
        return True

    def _fill_end_pages(self, toc: List[Dict[str, Any]], total_pages: int) -> None:
        for idx, chapter in enumerate(toc):
            next_start = toc[idx + 1]["page_start"] if idx < len(toc) - 1 else total_pages + 1
            chapter_end = self._clamp_page(next_start - 1, total_pages)
            chapter["page_end"] = max(chapter.get("page_start", 1), chapter_end)

            sections = chapter.get("sections", []) or []
            for s_idx, section in enumerate(sections):
                next_sec_start = sections[s_idx + 1]["page_start"] if s_idx < len(sections) - 1 else chapter["page_end"] + 1
                sec_end = self._clamp_page(next_sec_start - 1, total_pages)
                section["page_end"] = max(section.get("page_start", chapter["page_start"]), sec_end)

    def _is_chapter_title(self, title: str) -> bool:
        stripped = title.strip()
        return any(pattern.match(stripped) for pattern in CHAPTER_PATTERNS)

    def _is_section_title(self, title: str) -> bool:
        stripped = title.strip()
        return any(pattern.match(stripped) for pattern in SECTION_PATTERNS)

    def _extract_leading_number(self, text: str) -> Optional[int]:
        m = re.search(r"\b(\d{1,3})\b", text)
        if m:
            return int(m.group(1))
        roman = re.search(r"\b([IVXLCDM]{1,8})\b", text, flags=re.IGNORECASE)
        if roman:
            return self._roman_to_int(roman.group(1))
        return None

    def _extract_section_number(self, text: str) -> Optional[str]:
        m = re.search(r"(\d+\.\d+(?:\.\d+)?)", text)
        return m.group(1) if m else None

    def _clean_title(self, title: str) -> str:
        cleaned = re.sub(r"\s+", " ", title or "").strip(" .-\t")
        return cleaned[:220]

    def _clamp_page(self, value: int, total_pages: int) -> int:
        return max(1, min(int(value), total_pages))

    def _norm_ascii(self, text: str) -> str:
        lowered = (text or "").lower()
        nfkd = unicodedata.normalize("NFKD", lowered)
        normalized = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
        normalized = re.sub(r"[^a-z0-9\s]+", " ", normalized)
        return re.sub(r"\s+", " ", normalized).strip()

    def _roman_to_int(self, roman: str) -> int:
        values = {"i": 1, "v": 5, "x": 10, "l": 50, "c": 100, "d": 500, "m": 1000}
        total = 0
        prev = 0
        for ch in roman.lower()[::-1]:
            val = values.get(ch, 0)
            if val < prev:
                total -= val
            else:
                total += val
            prev = val
        return max(1, total)

