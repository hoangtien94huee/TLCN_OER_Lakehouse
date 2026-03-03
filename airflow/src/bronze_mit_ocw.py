#!/usr/bin/env python3
"""
MIT OCW Scraper - Bronze Layer
Scrapes course metadata AND PDF resources from MIT OpenCourseWare.

PDF Storage structure in MinIO:
  bronze/mit_ocw/pdfs/{course-slug}/lecture-notes/Lecture_01_Introduction.pdf
  bronze/mit_ocw/pdfs/{course-slug}/assignments/Problem_Set_01_Questions.pdf
  bronze/mit_ocw/pdfs/{course-slug}/exams/Quiz_01.pdf
  bronze/mit_ocw/pdfs/{course-slug}/recitations/Recitation_01.pdf
  bronze/mit_ocw/pdfs/{course-slug}/other/Syllabus.pdf
"""

import os
import json
import time
import hashlib
import requests
import re
import io
from datetime import datetime
from typing import List, Dict, Any, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# MinIO imports
try:
    from minio import Minio
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False


class MITOCWScraper:
    """MIT OCW scraper for bronze layer - course metadata only"""
    
    # PDF type classification keywords → (folder_name, priority)
    # priority: lower = more valuable for chatbot (lecture notes = 0, exams = 2, etc.)
    _PDF_TYPE_RULES: List[Tuple[List[str], str, int]] = [
        (['lecture', 'lec'],                          'lecture-notes',  0),
        (['recitation', 'rec'],                       'recitations',    1),
        (['problem set', 'pset', 'assignment', 'hw'], 'assignments',    2),
        (['exam', 'quiz', 'final', 'midterm', 'test'],'exams',          3),
        (['solution', 'sol', 'answer', 'key'],        'solutions',      4),
        (['syllabus'],                                 'other',          5),
        (['reading', 'note'],                          'lecture-notes',  0),
    ]

    def __init__(self, delay=2, output_dir="scraped_data/mit_ocw",
                 batch_size=25, max_documents=None,
                 download_pdfs=True, max_pdfs_per_course=30,
                 pdf_types=None, **kwargs):
        # Default: chỉ lấy lecture notes (bài giảng) cho chatbot Q&A
        # Truyền pdf_types=['lecture-notes','assignments'] nếu muốn mở rộng
        if pdf_types is None:
            pdf_types = ['lecture-notes']
        self.base_url = "https://ocw.mit.edu"
        self.source = "mit_ocw"
        self.delay = delay
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.max_documents = max_documents
        self.driver = None

        # PDF config
        self.download_pdfs = download_pdfs
        self.max_pdfs_per_course = max_pdfs_per_course
        # Which types to keep: None = all; e.g. ['lecture-notes', 'assignments']
        self.pdf_types_filter: Optional[Set[str]] = set(pdf_types) if pdf_types else None

        # HTTP Session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # Setup
        self._setup_minio()
        self._setup_selenium()

        # Load existing courses for deduplication
        self.existing_course_ids = self._load_existing_course_ids()
        if self.existing_course_ids:
            print(f"[MIT OCW] Loaded {len(self.existing_course_ids)} existing courses")

        print(f"[MIT OCW] Scraper initialized - Max docs: {self.max_documents}, "
              f"Download PDFs: {self.download_pdfs}, "
              f"PDF types: {list(self.pdf_types_filter) if self.pdf_types_filter else 'all'}")
    
    def _setup_minio(self):
        """Setup MinIO client"""
        self.minio_client = None
        self.minio_bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        self.minio_enable = os.getenv('MINIO_ENABLE', '0').lower() in ('1', 'true', 'yes')
        
        if self.minio_enable and MINIO_AVAILABLE:
            try:
                self.minio_client = Minio(
                    endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
                    access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                    secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
                    secure=os.getenv('MINIO_SECURE', '0').lower() in ('1', 'true', 'yes')
                )
                if not self.minio_client.bucket_exists(self.minio_bucket):
                    self.minio_client.make_bucket(self.minio_bucket)
                print("[MinIO] Client initialized successfully")
            except Exception as e:
                print(f"[MinIO] Error: {e}")
                self.minio_enable = False
    
    def _setup_selenium(self):
        """Setup Selenium WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-images")
            
            chromedriver_paths = ['/usr/local/bin/chromedriver', '/usr/bin/chromedriver', 'chromedriver']
            
            for path in chromedriver_paths:
                try:
                    if os.path.exists(path) or path == 'chromedriver':
                        service = Service(path)
                        self.driver = webdriver.Chrome(service=service, options=chrome_options)
                        print(f"[Selenium] Initialized with: {path}")
                        return
                except Exception:
                    continue
            
            raise Exception("Could not initialize ChromeDriver")
                    
        except Exception as e:
            raise Exception(f"Selenium WebDriver required but failed: {e}")
    
    def cleanup(self):
        """Cleanup resources"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                print(f"Error closing WebDriver: {e}")
    
    # =========================================================================
    # MAIN SCRAPING
    # =========================================================================
    
    def scrape(self) -> List[Dict[str, Any]]:
        """Main scraping method"""
        print("[MIT OCW] Starting scraper...")
        
        try:
            # Get course URLs from sitemap
            course_urls = list(self._get_course_urls())
            print(f"[MIT OCW] Found {len(course_urls)} courses")
            
            if self.max_documents:
                course_urls = course_urls[:self.max_documents]
                print(f"[MIT OCW] Limited to {len(course_urls)} courses")
            
            # Scrape courses
            documents = []
            skipped = 0
            
            for i, url in enumerate(course_urls, 1):
                course_hash = self._create_document_id(url)
                
                # Skip if already scraped
                if course_hash in self.existing_course_ids:
                    skipped += 1
                    continue
                
                print(f"[{i}/{len(course_urls)}] Scraping: {url}")
                
                course_data = self._scrape_course(url)
                if course_data:
                    documents.append(course_data)
                    self.existing_course_ids.add(course_data['id'])
                    pdf_count = course_data.get('pdf_count', 0)
                    print(f"  ✓ {course_data['title'][:50]}... | PDFs: {pdf_count}")
                
                time.sleep(self.delay)
            
            print(f"\n[MIT OCW] Completed: {len(documents)} new, {skipped} skipped")
            return documents
            
        except Exception as e:
            print(f"[MIT OCW] Error: {e}")
            return []
        finally:
            self.cleanup()
    
    def scrape_with_selenium(self) -> List[Dict[str, Any]]:
        """Legacy method for DAG compatibility"""
        return self.scrape()
    
    # =========================================================================
    # URL EXTRACTION
    # =========================================================================
    
    def _get_course_urls(self) -> Set[str]:
        """Get course URLs from sitemap (faster than scrolling)"""
        course_urls = set()
        
        try:
            sitemap_url = "https://ocw.mit.edu/sitemap.xml"
            print(f"[MIT OCW] Fetching sitemap: {sitemap_url}")
            
            response = self.session.get(sitemap_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            for loc in soup.find_all('loc'):
                url = loc.get_text(strip=True)
                if '/sitemap.xml' in url:
                    url = url.replace('/sitemap.xml', '/')
                
                if self._is_valid_course_url(url):
                    course_urls.add(url)
            
            print(f"[MIT OCW] Found {len(course_urls)} courses from sitemap")
            return course_urls
            
        except Exception as e:
            print(f"[MIT OCW] Sitemap failed: {e}, trying fallback...")
            return self._get_course_urls_fallback()
    
    def _get_course_urls_fallback(self) -> Set[str]:
        """Fallback: Get URLs by scrolling search page"""
        try:
            search_url = "https://ocw.mit.edu/search/?s=department_course_numbers.sort_coursenum&type=course"
            self.driver.get(search_url)
            time.sleep(3)
            
            # Scroll to load more
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            for _ in range(5):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            # Extract URLs
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            course_urls = set()
            
            for link in soup.find_all('a', href=lambda x: x and '/courses/' in x):
                href = link.get('href', '')
                if self._is_valid_course_url(href):
                    course_urls.add(urljoin(self.base_url, href))
            
            return course_urls
            
        except Exception as e:
            print(f"[MIT OCW] Fallback failed: {e}")
            return set()
    
    def _is_valid_course_url(self, url: str) -> bool:
        """Check if URL is a valid course URL"""
        if not url or '/courses/' not in url:
            return False
        
        skip_patterns = ['/about/', '/instructor-insights/', '/download/', 
                        '/calendar/', '/sitemap.xml', '.xml', '.json']
        return not any(p in url for p in skip_patterns)
    
    # =========================================================================
    # COURSE SCRAPING
    # =========================================================================
    
    def _scrape_course(self, url: str) -> Optional[Dict[str, Any]]:
        """Scrape individual course metadata + download PDFs"""
        try:
            soup = self._get_page(url)
            if not soup:
                return None

            course_data = self._extract_course_info(soup, url)

            # Download PDFs after metadata extraction
            if self.download_pdfs and self.minio_client:
                course_slug = self._extract_course_slug(url)
                pdf_results = self._download_course_pdfs(url, course_slug)
                course_data['pdf_count'] = pdf_results['downloaded']
                course_data['pdf_paths'] = pdf_results['paths']
                course_data['pdf_types_found'] = pdf_results['types_found']
            else:
                course_data['pdf_count'] = 0
                course_data['pdf_paths'] = []
                course_data['pdf_types_found'] = []

            return course_data

        except Exception as e:
            print(f"  ✗ Error: {e}")
            return None
    
    def _get_page(self, url: str, max_retries=3) -> Optional[BeautifulSoup]:
        """Get page content with Selenium"""
        for attempt in range(max_retries):
            try:
                self.driver.get(url)
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(1)
                return BeautifulSoup(self.driver.page_source, 'html.parser')
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  ✗ Failed to load page: {e}")
                    return None
    
    # =========================================================================
    # PDF DOWNLOAD
    # =========================================================================

    def _extract_course_slug(self, course_url: str) -> str:
        """Extract clean course slug from URL for folder naming.
        e.g. https://ocw.mit.edu/courses/6-006-introduction-to-algorithms-spring-2020/
             → 6-006-introduction-to-algorithms-spring-2020
        """
        path = urlparse(course_url).path.rstrip('/')
        return path.split('/courses/')[-1].split('/')[0] or 'unknown'

    def _classify_pdf(self, link_text: str, href: str) -> Tuple[str, int]:
        """Return (folder_name, priority) based on link text / filename."""
        combined = f"{link_text} {href}".lower()
        for keywords, folder, priority in self._PDF_TYPE_RULES:
            if any(kw in combined for kw in keywords):
                return folder, priority
        return 'other', 99

    def _sanitize_filename(self, name: str) -> str:
        """Convert a human-readable label to a safe filename."""
        # Remove characters that are unsafe in filenames
        name = re.sub(r'[\\/:*?"<>|]', '', name)
        # Collapse whitespace → underscore
        name = re.sub(r'\s+', '_', name.strip())
        # Remove duplicate underscores
        name = re.sub(r'_+', '_', name)
        return name[:120]  # cap length

    def _collect_pdf_links(self, course_url: str) -> List[Dict[str, Any]]:
        """
        Collect all PDF links for a course by scanning:
          1. /pages/resource-index/  (comprehensive table)
          2. /pages/lecture-notes/   (if resource-index missing)
          3. /pages/assignments/
          4. /pages/exams/
          5. Course home page itself

        Returns list of dicts: {url, label, folder, priority}
        """
        base = course_url.rstrip('/')
        scan_pages = [
            f"{base}/pages/resource-index/",
            f"{base}/pages/lecture-notes/",
            f"{base}/pages/assignments/",
            f"{base}/pages/exams/",
            f"{base}/pages/readings/",
            base + '/',
        ]

        seen_urls: Set[str] = set()
        pdf_entries: List[Dict[str, Any]] = []

        for page_url in scan_pages:
            try:
                soup = self._get_page(page_url)
                if not soup:
                    continue

                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    # Keep only direct .pdf links or /resources/ resource pages
                    is_pdf_direct = href.lower().endswith('.pdf')
                    is_resource_page = '/resources/' in href and not href.endswith('/')

                    if not (is_pdf_direct or is_resource_page):
                        continue

                    # Resolve to absolute URL
                    abs_url = urljoin(self.base_url, href)
                    if abs_url in seen_urls:
                        continue
                    seen_urls.add(abs_url)

                    label = a_tag.get_text(strip=True) or Path(urlparse(abs_url).path).stem
                    # Strip trailing "notes (PDF)", "(PDF)" etc. from label
                    label = re.sub(r'\s*\(pdf\)\s*$', '', label, flags=re.I).strip()
                    label = re.sub(r'\s+notes\s*$', '', label, flags=re.I).strip()
                    label = label or Path(urlparse(abs_url).path).stem

                    folder, priority = self._classify_pdf(label, abs_url)

                    pdf_entries.append({
                        'url': abs_url,
                        'label': label,
                        'folder': folder,
                        'priority': priority,
                        'is_resource_page': is_resource_page,
                    })

                # Resource-index usually has everything; stop early if found enough
                if '/resource-index/' in page_url and len(pdf_entries) > 5:
                    break

            except Exception as e:
                print(f"  [PDF] Scan error on {page_url}: {e}")

        # Sort: lecture-notes first, then others; within same folder keep order
        pdf_entries.sort(key=lambda x: (x['priority'], x['label']))
        return pdf_entries

    def _resolve_pdf_url(self, entry: Dict[str, Any]) -> Optional[str]:
        """If entry is a /resources/ page, resolve it to the actual .pdf download URL."""
        if not entry.get('is_resource_page'):
            return entry['url']

        try:
            soup = self._get_page(entry['url'])
            if not soup:
                return None
            # The resource page has a "Download file" link pointing to the actual PDF
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if href.lower().endswith('.pdf'):
                    return urljoin(self.base_url, href)
        except Exception:
            pass
        return None

    def _download_course_pdfs(self, course_url: str, course_slug: str) -> Dict[str, Any]:
        """
        Download all PDFs for a course and upload to MinIO.

        MinIO path structure:
          bronze/mit_ocw/pdfs/{course_slug}/{folder}/{label}.pdf

        Returns:
          {'downloaded': int, 'paths': [minio_path, ...], 'types_found': [folder, ...]}
        """
        result = {'downloaded': 0, 'paths': [], 'types_found': []}

        if not self.minio_client:
            return result

        # Check if PDFs already downloaded for this course
        existing_prefix = f"bronze/mit_ocw/pdfs/{course_slug}/"
        try:
            existing = list(self.minio_client.list_objects(
                self.minio_bucket, prefix=existing_prefix, recursive=True
            ))
            if existing:
                existing_paths = [o.object_name for o in existing]
                types_found = list({p.split('/')[4] for p in existing_paths
                                    if len(p.split('/')) > 4})
                print(f"  [PDF] Already have {len(existing_paths)} PDFs for {course_slug}, skipping")
                result['downloaded'] = len(existing_paths)
                result['paths'] = existing_paths
                result['types_found'] = types_found
                return result
        except Exception:
            pass

        # Collect PDF links from course pages
        pdf_entries = self._collect_pdf_links(course_url)
        print(f"  [PDF] Found {len(pdf_entries)} PDF links")

        if not pdf_entries:
            return result

        # Apply type filter
        if self.pdf_types_filter:
            pdf_entries = [e for e in pdf_entries if e['folder'] in self.pdf_types_filter]

        # Limit per course
        pdf_entries = pdf_entries[:self.max_pdfs_per_course]

        # Track filename collisions per folder
        used_names: Dict[str, Set[str]] = {}
        types_seen: Set[str] = set()

        for entry in pdf_entries:
            try:
                # Resolve actual PDF URL (handle resource pages)
                pdf_url = self._resolve_pdf_url(entry)
                if not pdf_url:
                    continue

                # Build a clean, human-readable filename
                label = self._sanitize_filename(entry['label'])
                folder = entry['folder']
                if not label:
                    label = Path(urlparse(pdf_url).path).stem
                    label = self._sanitize_filename(label)

                # Handle duplicates within same folder by appending counter
                used_names.setdefault(folder, set())
                base_label = label
                counter = 1
                while label in used_names[folder]:
                    label = f"{base_label}_{counter}"
                    counter += 1
                used_names[folder].add(label)

                minio_path = f"bronze/mit_ocw/pdfs/{course_slug}/{folder}/{label}.pdf"

                # Skip if already exists in MinIO
                try:
                    self.minio_client.stat_object(self.minio_bucket, minio_path)
                    result['paths'].append(minio_path)
                    types_seen.add(folder)
                    result['downloaded'] += 1
                    continue
                except Exception:
                    pass  # Doesn't exist, proceed to download

                # Download PDF bytes
                pdf_bytes = self._fetch_pdf(pdf_url)
                if not pdf_bytes:
                    continue

                # Upload to MinIO
                self.minio_client.put_object(
                    self.minio_bucket,
                    minio_path,
                    io.BytesIO(pdf_bytes),
                    length=len(pdf_bytes),
                    content_type='application/pdf',
                )

                result['paths'].append(minio_path)
                types_seen.add(folder)
                result['downloaded'] += 1
                print(f"  [PDF] ✓ {folder}/{label}.pdf")

                time.sleep(0.5)

            except Exception as e:
                print(f"  [PDF] ✗ {entry.get('label', '?')}: {e}")

        result['types_found'] = list(types_seen)
        print(f"  [PDF] Course {course_slug}: {result['downloaded']} PDFs uploaded "
              f"({', '.join(types_seen) or 'none'})")
        return result

    def _fetch_pdf(self, url: str, max_retries: int = 3) -> Optional[bytes]:
        """Download PDF bytes with retry logic."""
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=60, stream=True)
                resp.raise_for_status()
                content_type = resp.headers.get('Content-Type', '')
                if 'pdf' not in content_type.lower() and not url.lower().endswith('.pdf'):
                    # Not a PDF - skip silently
                    return None
                return resp.content
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  [PDF] Download failed {url}: {e}")
        return None

    # =========================================================================
    # DATA EXTRACTION
    # =========================================================================

    def _extract_course_info(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Extract course metadata"""
        course_number = self._extract_course_number(url)
        _, year = self._extract_semester_year(course_number, soup)
        
        license_name, license_url = self._extract_license(soup)
        return {
            'id': self._create_document_id(url),
            'title': self._extract_title(soup),
            'url': url,
            'description': self._extract_description(soup),
            'instructors': self._extract_instructors(soup),
            'year': year if year != 'Unknown' else None,
            'source': self.source,
            'language': 'en',
            'license': license_name,
            'license_url': license_url,
            'scraped_at': datetime.now().isoformat()
        }
    
    def _extract_license(self, soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        """Extract license by finding any creativecommons.org link on the page.
        Returns (license_name, license_url), e.g. ('CC BY-NC-SA 4.0', 'https://...')
        """
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if 'creativecommons.org/licenses/' in href or 'creativecommons.org/publicdomain/' in href:
                return self._cc_url_to_name(href), href.rstrip('/')
        return None, None

    def _cc_url_to_name(self, url: str) -> str:
        """Convert a creativecommons.org URL to a short human-readable name.
        e.g. https://creativecommons.org/licenses/by-nc-sa/4.0/ → 'CC BY-NC-SA 4.0'
        """
        url = url.rstrip('/')
        m = re.search(r'creativecommons\.org/licenses/([^/]+)/([^/]+)', url)
        if m:
            return f"CC {m.group(1).upper()} {m.group(2)}"
        m = re.search(r'creativecommons\.org/publicdomain/([^/]+)/([^/]+)', url)
        if m:
            return f"CC0 {m.group(2)}"
        return 'Creative Commons'

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract course title"""
        for selector in ['h1', 'title']:
            elem = soup.find(selector)
            if elem:
                title = elem.get_text().strip()
                if ' | ' in title:
                    title = title.split(' | ')[0].strip()
                if title and title != 'MIT OpenCourseWare':
                    return title
        return 'Unknown Course'
    
    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Extract course description"""
        # Try meta description
        meta = soup.find('meta', {'name': 'description'})
        if meta and meta.get('content'):
            desc = meta.get('content').strip()
            if len(desc) > 50:
                return desc[:1000]
        
        # Try description sections
        for section in soup.find_all(['div', 'section', 'p'], 
            class_=lambda x: x and any(t in str(x).lower() for t in ['description', 'intro', 'about'])):
            text = section.get_text().strip()
            if len(text) > 100:
                return text[:1000]
        
        return ""
    
    def _extract_instructors(self, soup: BeautifulSoup) -> List[str]:
        """Extract instructor names"""
        instructors = []
        
        # Look for professor patterns in page text
        page_text = soup.get_text()
        patterns = [
            r'Prof\.\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
            r'Dr\.\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
            r'Professor\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'
        ]
        
        seen = set()
        for pattern in patterns:
            for match in re.findall(pattern, page_text):
                name = f'Prof. {match}'
                if name not in seen:
                    instructors.append(name)
                    seen.add(name)
        
        return instructors[:5]
    
    def _extract_course_number(self, url: str) -> str:
        """Extract course number from URL"""
        match = re.search(r'courses/([^/]+)', url)
        return match.group(1) if match else 'unknown'
    
    def _extract_semester_year(self, course_number: str, soup: BeautifulSoup) -> tuple:
        """Extract semester and year"""
        # From URL pattern
        match = re.search(r'-(fall|spring|summer|winter)-(\d{4})', course_number.lower())
        if match:
            return match.group(1).title(), match.group(2)
        
        # From page text
        page_text = soup.get_text()
        match = re.search(r'(Fall|Spring|Summer|Winter)\s+(\d{4})', page_text)
        if match:
            return match.group(1), match.group(2)
        
        return 'Unknown', 'Unknown'
    
    # =========================================================================
    # STORAGE & DEDUPLICATION
    # =========================================================================
    
    def _create_document_id(self, url: str) -> str:
        """Create unique document ID from URL"""
        return hashlib.md5(f"{self.source}_{url}".encode()).hexdigest()
    
    def save_to_minio(self, data: List[Dict[str, Any]], logical_date: str = None) -> Optional[str]:
        """Save scraped data to MinIO"""
        if not data:
            return None
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        filename = f"mit_ocw_bronze_{timestamp}.json"
        
        # Save locally first
        output_file = Path(self.output_dir) / filename
        output_file.parent.mkdir(exist_ok=True, parents=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"[MIT OCW] Saved locally: {output_file}")
        
        # Upload to MinIO
        if self.minio_client:
            try:
                minio_path = f"bronze/mit_ocw/json/{filename}"
                self.minio_client.fput_object(self.minio_bucket, minio_path, str(output_file))
                print(f"[MinIO] Uploaded: {minio_path}")
                return minio_path
            except Exception as e:
                print(f"[MinIO] Upload failed: {e}")
        
        return None
    
    def _load_existing_course_ids(self) -> Set[str]:
        """Load existing course IDs from MinIO for deduplication"""
        existing = set()
        
        if not self.minio_client:
            return existing
        
        try:
            objects = self.minio_client.list_objects(
                self.minio_bucket,
                prefix='bronze/mit_ocw/json/',
                recursive=True
            )
            
            for obj in objects:
                if not obj.object_name.endswith('.json'):
                    continue
                
                try:
                    response = self.minio_client.get_object(self.minio_bucket, obj.object_name)
                    content = response.read().decode('utf-8').strip()
                    response.close()
                    response.release_conn()
                    
                    if not content:
                        continue
                    
                    records = json.loads(content)
                    if not isinstance(records, list):
                        records = [records]
                    
                    for record in records:
                        if isinstance(record, dict):
                            course_id = record.get('id')
                            if not course_id and record.get('url'):
                                course_id = self._create_document_id(record['url'])
                            if course_id:
                                existing.add(course_id)
                                
                except Exception as e:
                    print(f"[MIT OCW] Skipping {obj.object_name}: {e}")
                    
        except Exception as e:
            print(f"[MIT OCW] Could not scan MinIO: {e}")
        
        return existing


def run_mit_ocw_scraper(**kwargs) -> List[Dict[str, Any]]:
    """Run MIT OCW scraper - entry point for DAG"""
    scraper = MITOCWScraper(**kwargs)
    try:
        return scraper.scrape()
    finally:
        scraper.cleanup()


if __name__ == "__main__":
    import sys
    
    max_docs = int(sys.argv[1]) if len(sys.argv) > 1 else None
    documents = run_mit_ocw_scraper(max_documents=max_docs)
    print(f"Completed! Scraped {len(documents)} courses.")
