
"""
Open Textbook Library Scraper - Bronze Layer - FAST MODE
=========================================================

Optimized version with parallel processing and speed improvements.
Saves as JSON Array matching the exact format.
"""

import os
import json
import time
import hashlib
import requests
import random
import re
from datetime import datetime
from typing import List, Dict, Any
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Warning: Selenium library not found")

# MinIO imports
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    print("Warning: MinIO library not found")

# Configuration constants
BASE_URL = "https://open.umn.edu"

# FAST MODE Configuration
FAST_MODE = os.getenv('OTL_FAST_MODE', 'true').lower() in ['1', 'true', 'yes']
PARALLEL_MODE = os.getenv('OTL_PARALLEL', 'true').lower() in ['1', 'true', 'yes']
MAX_WORKERS = int(os.getenv('OTL_MAX_WORKERS', '4'))
SKIP_PDF_DOWNLOAD = os.getenv('OTL_SKIP_PDF', 'false').lower() in ['1', 'true', 'yes']

# Speed optimizations
if FAST_MODE:
    DEFAULT_DELAY_BASE_SEC = 0.2
    DEFAULT_DELAY_JITTER_SEC = 0.1
    PER_BOOK_DELAY_SEC = 0.2
    SUBJECT_COOLDOWN_SEC = 2.0
    SCROLL_WAIT_SEC = 0.5
    PAGE_LOAD_WAIT_SEC = 0.5
else:
    DEFAULT_DELAY_BASE_SEC = float(os.getenv('OTL_DELAY_BASE_SEC', '0.8'))
    DEFAULT_DELAY_JITTER_SEC = float(os.getenv('OTL_DELAY_JITTER_SEC', '0.4'))
    PER_BOOK_DELAY_SEC = float(os.getenv('OTL_PER_BOOK_DELAY_SEC', '0.6'))
    SUBJECT_COOLDOWN_SEC = float(os.getenv('OTL_SUBJECT_COOLDOWN_SEC', '5.0'))
    SCROLL_WAIT_SEC = 2.0
    PAGE_LOAD_WAIT_SEC = 1.0

BOOK_MAX_ATTEMPTS = 2 if FAST_MODE else 3

# PDF Download Configuration
DOWNLOAD_PDFS = not SKIP_PDF_DOWNLOAD
PDF_PATH = '/opt/airflow/scraped_pdfs/otl'

# Helper functions
def _sleep(base_sec: float = DEFAULT_DELAY_BASE_SEC, jitter_sec: float = DEFAULT_DELAY_JITTER_SEC) -> None:
    """Sleep with jitter"""
    delay = max(0.0, base_sec) + max(0.0, jitter_sec) * random.random()
    time.sleep(delay)

def _pick_user_agent() -> str:
    """Return modern Chrome user agent"""
    return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.127 Safari/537.36'

def _md5_id(url: str, title: str) -> str:
    """Generate MD5 hash ID"""
    return hashlib.md5(f"{url}_{title}".encode("utf-8")).hexdigest()

def _download_pdf(pdf_url: str, book_id: str, title: str, minio_client=None, bucket: str = 'oer-lakehouse') -> str:
    """Download PDF file and upload to MinIO. Returns MinIO path or empty string."""
    if SKIP_PDF_DOWNLOAD:
        return ""
        
    try:
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_title = safe_title.replace(' ', '_')[:50]
        filename = f"{book_id}_{safe_title}.pdf"
        
        os.makedirs(PDF_PATH, exist_ok=True)
        filepath = os.path.join(PDF_PATH, filename)
        
        if FAST_MODE:
            print(f"[PDF] Downloading: {filename[:30]}...")
        else:
            print(f"[OTL] [PDF] Downloading: {filename}")
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': _pick_user_agent(),
            'Accept': 'application/pdf,application/octet-stream,*/*',
            'Referer': BASE_URL
        })
        
        max_attempts = 2 if FAST_MODE else 3
        for attempt in range(max_attempts):
            try:
                response = session.get(pdf_url, stream=True, timeout=60 if FAST_MODE else 120, allow_redirects=True)
                response.raise_for_status()
                
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                file_size = os.path.getsize(filepath) / (1024 * 1024)
                
                if file_size < 0.01:
                    os.remove(filepath)
                    if attempt < max_attempts - 1:
                        time.sleep(1)
                        continue
                    return ""
                
                if FAST_MODE:
                    print(f"[PDF] ✓ {filename[:30]}... ({file_size:.1f}MB)")
                else:
                    print(f"[OTL] [PDF] ✓ Downloaded: {filename} ({file_size:.2f} MB)")
                
                if minio_client:
                    minio_path = f"bronze/otl/otl-pdfs/{filename}"
                    try:
                        minio_client.fput_object(bucket, minio_path, filepath, content_type='application/pdf')
                        if not FAST_MODE:
                            print(f"[OTL] [PDF] ✓ Uploaded to MinIO: {minio_path}")
                        return f"s3a://{bucket}/{minio_path}"
                    except S3Error as e:
                        print(f"[PDF] MinIO upload failed: {e}")
                        return ""
                
                return ""
                
            except requests.exceptions.RequestException as e:
                if attempt < max_attempts - 1:
                    time.sleep(1)
                continue
        
        return False
        
    except Exception as e:
        if not FAST_MODE:
            print(f"[OTL] [PDF] Download failed for {pdf_url}: {e}")
        return False

class OTLScraperStandalone:
    """Standalone Open Textbook Library scraper for bronze layer"""
    
    def __init__(self):
        self.base_url = BASE_URL + "/opentextbooks"
        self.source = "otl"
        self.delay = DEFAULT_DELAY_BASE_SEC
        self.max_books = int(os.getenv('MAX_DOCUMENTS', '100'))  # Total limit across all subjects
        self.use_selenium = SELENIUM_AVAILABLE
        
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        self.minio_client = self._setup_minio() if MINIO_AVAILABLE else None
        
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': _pick_user_agent()})
        
        self.pdf_downloaded_count = 0
        self.pdf_failed_count = 0
        
        # Load existing book IDs to avoid duplicates
        self.existing_book_ids = self._load_existing_book_ids()
        self.total_scraped = 0  # Track total books scraped in this run
        
        if self.existing_book_ids:
            print(f"[OTL] Loaded {len(self.existing_book_ids)} existing books; duplicates will be skipped.")
        
        # Get subjects dynamically
        self.subjects = self._get_subjects()
        
        mode_str = "FAST" if FAST_MODE else "NORMAL"
        parallel_str = f"PARALLEL ({MAX_WORKERS} workers)" if PARALLEL_MODE else "SEQUENTIAL"
        
        print(f"OTL Scraper initialized - {mode_str} MODE - {parallel_str}")
        print(f"Max books TOTAL: {self.max_books if self.max_books < 999999 else 'UNLIMITED'}")
        print(f"Selenium: {self.use_selenium}")
        print(f"Download PDFs: {DOWNLOAD_PDFS}")
        print(f"Subjects to scrape: {len(self.subjects)}")
    
    def _get_subjects(self) -> List[Dict[str, str]]:
        """Get list of ALL subjects dynamically from OTL website"""
        subjects = []
        
        print("[OTL] Fetching subjects list from website...")
        
        try:
            driver = self._init_driver(headless=True)
            if not driver:
                print("[OTL] Selenium not available, using fallback subjects")
                return self._get_subjects_fallback()
            
            driver.get(self.base_url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(2 if FAST_MODE else 3)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            seen_urls = set()
            
            # Find "Textbooks by Subject" section
            subject_section = soup.find(string=re.compile(r'Textbooks by Subject', re.I))
            if subject_section:
                parent = subject_section.find_parent(['div', 'section'])
                if parent:
                    for link in parent.find_all('a', href=True):
                        href = link.get('href', '')
                        if '/subjects/' in href:
                            full_url = urljoin(BASE_URL, href)
                            subject_name = link.get_text(strip=True)
                            
                            if full_url not in seen_urls and subject_name:
                                subjects.append({
                                    'name': subject_name,
                                    'url': full_url
                                })
                                seen_urls.add(full_url)
                                if not FAST_MODE:
                                    print(f"[OTL] Found subject: {subject_name}")
            
            # Fallback: All links with /subjects/
            if not subjects:
                for link in soup.find_all('a', href=re.compile(r'/subjects/[a-z-]+')):
                    href = link.get('href', '')
                    full_url = urljoin(BASE_URL, href)
                    subject_name = link.get_text(strip=True)
                    
                    if any(x in full_url for x in ['/subjects/all', '/subjects?', '/subjects#']):
                        continue
                    
                    if full_url not in seen_urls and subject_name:
                        subjects.append({
                            'name': subject_name,
                            'url': full_url
                        })
                        seen_urls.add(full_url)
            
            driver.quit()
            
            if subjects:
                print(f"[OTL] ✓ Total {len(subjects)} subjects found")
                return subjects
            else:
                print("[OTL] No subjects found, using fallback list")
                return self._get_subjects_fallback()
                
        except Exception as e:
            print(f"[OTL] Error fetching subjects: {e}")
            return self._get_subjects_fallback()
    
    def _get_subjects_fallback(self) -> List[Dict[str, str]]:
        """Fallback comprehensive subject list"""
        base = self.base_url
        return [
            {'name': 'Accounting', 'url': f"{base}/subjects/accounting"},
            {'name': 'Finance', 'url': f"{base}/subjects/finance"},
            {'name': 'Human Resources', 'url': f"{base}/subjects/human-resources"},
            {'name': 'Management', 'url': f"{base}/subjects/management"},
            {'name': 'Marketing', 'url': f"{base}/subjects/marketing"},
            {'name': 'Business', 'url': f"{base}/subjects/business"},
            {'name': 'Computer Science', 'url': f"{base}/subjects/computer-science"},
            {'name': 'Databases', 'url': f"{base}/subjects/databases"},
            {'name': 'Information Systems', 'url': f"{base}/subjects/information-systems"},
            {'name': 'Programming Languages', 'url': f"{base}/subjects/programming-languages"},
            {'name': 'Education', 'url': f"{base}/subjects/education"},
            {'name': 'Engineering Technology', 'url': f"{base}/subjects/engineering-technology"},
            {'name': 'Civil Engineering', 'url': f"{base}/subjects/civil-engineering"},
            {'name': 'Electrical Engineering', 'url': f"{base}/subjects/electrical-engineering"},
            {'name': 'Mechanical Engineering', 'url': f"{base}/subjects/mechanical-engineering"},
            {'name': 'Humanities', 'url': f"{base}/subjects/humanities"},
            {'name': 'History', 'url': f"{base}/subjects/history"},
            {'name': 'Philosophy', 'url': f"{base}/subjects/philosophy"},
            {'name': 'Languages', 'url': f"{base}/subjects/languages"},
            {'name': 'Literature', 'url': f"{base}/subjects/literature"},
            {'name': 'Journalism Media Communications', 'url': f"{base}/subjects/journalism-media-studies-communications"},
            {'name': 'Law', 'url': f"{base}/subjects/law"},
            {'name': 'Mathematics', 'url': f"{base}/subjects/mathematics"},
            {'name': 'Statistics', 'url': f"{base}/subjects/statistics"},
            {'name': 'Medicine', 'url': f"{base}/subjects/medicine"},
            {'name': 'Nursing', 'url': f"{base}/subjects/nursing"},
            {'name': 'Health Sciences', 'url': f"{base}/subjects/health-sciences"},
            {'name': 'Natural Sciences', 'url': f"{base}/subjects/natural-sciences"},
            {'name': 'Biology', 'url': f"{base}/subjects/biology"},
            {'name': 'Chemistry', 'url': f"{base}/subjects/chemistry"},
            {'name': 'Physics', 'url': f"{base}/subjects/physics"},
            {'name': 'Astronomy', 'url': f"{base}/subjects/astronomy"},
            {'name': 'Social Sciences', 'url': f"{base}/subjects/social-sciences"},
            {'name': 'Economics', 'url': f"{base}/subjects/economics"},
            {'name': 'Psychology', 'url': f"{base}/subjects/psychology"},
            {'name': 'Sociology', 'url': f"{base}/subjects/sociology"},
            {'name': 'Political Science', 'url': f"{base}/subjects/political-science"},
            {'name': 'Anthropology', 'url': f"{base}/subjects/anthropology"},
        ]
    
    def _setup_minio(self):
        """Setup MinIO client"""
        try:
            endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
            access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
            secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
            secure = os.getenv('MINIO_SECURE', '0').lower() in ['1', 'true', 'yes']
            
            client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
            
            if not client.bucket_exists(self.bucket):
                client.make_bucket(self.bucket)
                print(f"[MinIO] Created bucket: {self.bucket}")
            
            return client
        except Exception as e:
            print(f"[MinIO] Setup failed: {e}")
            return None
    
    def _load_existing_book_ids(self) -> set:
        """Load existing book IDs from previous scrapes to avoid duplicates"""
        existing_ids = set()
        
        # Note: OTL saves to MinIO directly, not local files
        # We'll check MinIO for existing JSON files
        if not self.minio_client:
            return existing_ids
        
        try:
            # List all OTL JSON files in bronze layer
            objects = self.minio_client.list_objects(
                self.bucket,
                prefix='bronze/otl/json/',
                recursive=True
            )
            
            for obj in objects:
                if obj.object_name.endswith('.json'):
                    try:
                        # Download and parse JSON
                        response = self.minio_client.get_object(self.bucket, obj.object_name)
                        content = response.read().decode('utf-8')
                        response.close()
                        response.release_conn()
                        
                        # Parse JSON Array format (OTL uses array, not lines)
                        records = json.loads(content)
                        
                        if isinstance(records, list):
                            for record in records:
                                if isinstance(record, dict):
                                    book_id = record.get('id')
                                    if book_id:
                                        existing_ids.add(book_id)
                    
                    except Exception as e:
                        print(f"[OTL] Warning: Could not read {obj.object_name}: {e}")
                        continue
        
        except Exception as e:
            print(f"[OTL] Warning: Error scanning existing files: {e}")
        
        return existing_ids
    
    def _init_driver(self, headless: bool = True):
        """Initialize Selenium Chrome driver"""
        if not SELENIUM_AVAILABLE:
            return None
            
        try:
            options = Options()
            if headless:
                options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--blink-settings=imagesEnabled=false")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option('excludeSwitches', ['enable-automation'])
            options.add_experimental_option('useAutomationExtension', False)
            
            # FAST MODE: Disable more features
            if FAST_MODE:
                options.add_argument("--disable-extensions")
                options.add_argument("--disable-gpu")
                options.add_argument("--disable-software-rasterizer")

            # Set user agent
            options.add_argument(f"--user-agent={_pick_user_agent()}")
            
            try:
                prefs = {
                    "profile.managed_default_content_settings.images": 2,
                    "profile.default_content_setting_values.notifications": 2
                }
                options.add_experimental_option("prefs", prefs)
            except Exception:
                pass
            
            chromedriver_paths = ['/usr/local/bin/chromedriver', '/usr/bin/chromedriver', 'chromedriver']
            
            driver = None
            for path in chromedriver_paths:
                try:
                    if os.path.exists(path) or path == 'chromedriver':
                        service = Service(path)
                        driver = webdriver.Chrome(service=service, options=options)
                        break
                except Exception:
                    continue
            
            if not driver:
                driver = webdriver.Chrome(options=options)
            
            try:
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                    "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                })
            except Exception:
                pass
            
            return driver
        except Exception as e:
            print(f"Selenium setup failed: {e}")
            return None
    
    def parse_book_detail_html(self, html: str, book_url: str, subjects: List[str]) -> Dict[str, Any]:
        """Parse book detail HTML - EXACT FORMAT MATCH"""
        soup = BeautifulSoup(html, 'html.parser')

        # Title
        title = ''
        h1 = soup.select_one('#info h1') or soup.find('h1')
        if h1:
            title = h1.get_text(strip=True)
        if not title:
            title_tag = soup.find('title')
            title = (title_tag.get_text(strip=True) if title_tag else '').strip()

        # Description
        description = ''
        about = soup.select_one('#AboutBook')
        if about:
            p = about.find('p')
            if p:
                description = p.get_text('\n', strip=True)

        # Authors - parse as list
        authors: List[str] = []
        for p in soup.find_all('p'):
            txt = (p.get_text(' ', strip=True) or '')
            if txt.lower().startswith('contributors:'):
                raw = txt.split(':', 1)[1].strip()
                parts = [re.sub(r'\s+', ' ', x).strip() for x in re.split(r',| and ', raw) if x.strip()]
                for a in parts:
                    if a and a not in authors:
                        authors.append(a)
                break
        
        if not authors:
            for meta_name in ['author', 'book:author']:
                for m in soup.find_all('meta', attrs={'name': meta_name}):
                    val = (m.get('content') or '').strip()
                    if val and val not in authors:
                        authors.append(val)
        
        if not authors:
            info_div = soup.select_one('#info')
            if info_div:
                started_block = False
                for p in info_div.find_all('p'):
                    txt = (p.get_text(' ', strip=True) or '')
                    if not txt:
                        continue
                    ltxt = txt.lower()
                    if 'review' in ltxt:
                        continue
                    meta_prefixes = (
                        'copyright year', 'publisher', 'language', 'isbn', 'license',
                        'format', 'pages', 'doi', 'edition'
                    )
                    if any(ltxt.startswith(pref) for pref in meta_prefixes) or ':' in txt:
                        if started_block:
                            break
                        else:
                            continue
                    started_block = True
                    parts = [re.sub(r'\s+', ' ', x).strip() for x in re.split(r',| and ', txt) if x.strip()]
                    for a in parts:
                        if a and a not in authors:
                            authors.append(a)

        # Extract PDF URL
        pdf_url = ''
        for a in soup.select('#book-types a[href]'):
            label = (a.get_text(strip=True) or '').lower()
            href = a.get('href', '')
            if 'pdf' in label or '.pdf' in href.lower():
                pdf_url = urljoin(BASE_URL, href)
                break
        
        if not pdf_url:
            for a in soup.select('a[href*=".pdf"]'):
                pdf_url = urljoin(BASE_URL, a.get('href', ''))
                break
        
        if not pdf_url:
            for a in soup.select('a[data-format="pdf"]'):
                pdf_url = urljoin(BASE_URL, a.get('href', ''))
                break

        # EXACT FORMAT MATCH - Simple object
        doc = {
            "id": _md5_id(book_url, title or book_url),
            "title": title,
            "description": description,
            "authors": authors,  # Always list
            "subject": subjects,  # Always list
            "source": "Open Textbook Library",
            "url": book_url,
            "url_pdf": pdf_url or "",
            "pdf_downloaded": False,
            "scraped_at": datetime.now().isoformat()
        }
        return doc
    
    def get_textbooks_list_selenium(self, subject_name: str = "All", subject_url: str = None) -> List[Dict[str, Any]]:
        """Get ALL textbooks using Selenium with infinite scroll - OPTIMIZED"""
        results = []
        driver = self._init_driver(headless=True)
        
        if not driver:
            print("Selenium not available")
            return results
        
        try:
            if not subject_url:
                subject_url = self.base_url
            
            if FAST_MODE:
                print(f"[{subject_name}] Loading...")
            else:
                print(f"[OTL] Loading: {subject_url}")
            
            driver.get(subject_url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(1 if FAST_MODE else 3)
            
            # FAST SCROLLING
            if FAST_MODE:
                print(f"[{subject_name}] Fast scrolling...")
            else:
                print("[OTL] Scrolling to load all books...")
            
            last_height = driver.execute_script("return document.body.scrollHeight")
            urls_found = 0
            no_new_count = 0
            max_no_new = 3 if FAST_MODE else 5
            scroll_attempt = 0
            max_scrolls = 50 if FAST_MODE else 100
            
            while True:
                scroll_attempt += 1
                
                # AGGRESSIVE SCROLL in fast mode
                if FAST_MODE:
                    for _ in range(3):
                        driver.execute_script("window.scrollBy(0, 2000);")
                    time.sleep(SCROLL_WAIT_SEC)
                else:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(SCROLL_WAIT_SEC)
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                current_books = len(soup.select('a[href*="/opentextbooks/textbooks/"]'))
                
                if current_books > urls_found:
                    if FAST_MODE:
                        if scroll_attempt % 5 == 0:
                            print(f"[{subject_name}] Scroll {scroll_attempt}: {current_books} books")
                    else:
                        print(f"[OTL] Scroll {scroll_attempt}: {current_books} books (+{current_books - urls_found})")
                    urls_found = current_books
                    no_new_count = 0
                else:
                    no_new_count += 1
                
                new_height = driver.execute_script("return document.body.scrollHeight")
                
                if no_new_count >= max_no_new or scroll_attempt >= max_scrolls:
                    if not FAST_MODE:
                        print(f"[OTL] ✓ Reached end after {scroll_attempt} scrolls")
                    break
                
                last_height = new_height
            
            # COLLECT ALL BOOK URLs
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            urls = []
            
            selectors = [
                'a[href*="/opentextbooks/textbooks/"]',
                'div.short-description h2 > a',
            ]
            
            for selector in selectors:
                elements = soup.select(selector)
                
                for elem in elements:
                    href = elem.get('href', '')
                    if not href:
                        continue
                    
                    full_url = urljoin(BASE_URL, href)
                    
                    if '/opentextbooks/textbooks/' in full_url and full_url not in urls:
                        if any(x in full_url for x in ['/submit', '/newest', '/in_development']):
                            continue
                        urls.append(full_url)
            
            urls = list(dict.fromkeys(urls))
            
            print(f"[{subject_name}] ✓ {len(urls)} books found")

            # VISIT EACH BOOK
            skipped_count = 0
            for idx, url in enumerate(urls, 1):
                # Check if we've reached the TOTAL limit across all subjects
                if self.total_scraped >= self.max_books:
                    print(f"[{subject_name}] Reached max books limit ({self.max_books}), stopping")
                    break
                
                # Generate book ID to check if already scraped
                book_id = hashlib.md5(f"otl_{url}".encode('utf-8')).hexdigest()
                
                if book_id in self.existing_book_ids:
                    skipped_count += 1
                    if not FAST_MODE:
                        print(f"[{subject_name}] [{idx}/{len(urls)}] Skipping already scraped: {url}")
                    continue
                
                if FAST_MODE:
                    if idx % 10 == 0 or idx == 1 or idx == len(urls):
                        print(f"[{subject_name}] [{idx}/{len(urls)}]")
                else:
                    print(f"[OTL] [{idx}/{len(urls)}] {url}")
                
                time.sleep(PER_BOOK_DELAY_SEC)
                
                try:
                    driver.get(url)
                    WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                    time.sleep(PAGE_LOAD_WAIT_SEC)
                    
                    html = driver.page_source
                    doc = self.parse_book_detail_html(html, url, [subject_name])
                    
                    # Download PDF
                    if doc and DOWNLOAD_PDFS and doc.get('url_pdf'):
                        pdf_path = _download_pdf(
                            doc['url_pdf'],
                            doc['id'],
                            doc.get('title', 'unknown'),
                            self.minio_client,
                            self.bucket
                        )
                        doc['pdf_downloaded'] = bool(pdf_path)
                        doc['pdf_path'] = pdf_path if pdf_path else None
                        
                        if pdf_path:
                            self.pdf_downloaded_count += 1
                        else:
                            self.pdf_failed_count += 1
                    
                    if doc:
                        results.append(doc)
                        self.total_scraped += 1  # Track total across all subjects
                        # Add to existing set to prevent duplicates in this run
                        self.existing_book_ids.add(doc.get('id', book_id))
                        
                        if not FAST_MODE:
                            print(f"[OTL] ✓ {doc.get('title', 'N/A')}")
                        
                except Exception as e:
                    if not FAST_MODE:
                        print(f"[OTL] ✗ Failed: {e}")
                    continue

            if skipped_count > 0:
                print(f"[{subject_name}] Skipped {skipped_count} books already scraped")
            print(f"[{subject_name}] ✓ Complete: {len(results)} new books (total scraped: {self.total_scraped})")
            return results
            
        except Exception as e:
            print(f"[{subject_name}] ERROR: {e}")
            import traceback
            traceback.print_exc()
            return results
        finally:
            try:
                driver.quit()
            except Exception:
                pass
    
    def scrape_single_subject(self, subject: Dict[str, str]) -> List[Dict[str, Any]]:
        """Scrape a single subject - for parallel execution"""
        return self.get_textbooks_list_selenium(
            subject_name=subject['name'],
            subject_url=subject['url']
        )
    
    def save_to_minio(self, documents: List[Dict[str, Any]], source: str = "otl", logical_date: str = None):
        """Save data to local file and upload to MinIO (matching MIT OCW pattern)"""
        if not documents:
            print("No documents to save")
            return ""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Match exact filename format: otl_bronze_merged_YYYYMMDD_HHMMSS.json
        filename = f"{source}_bronze_merged_{timestamp}.json"
        
        # Save to local file first (same as MIT OCW)
        local_dir = f"/opt/airflow/scraped_data/{source}"
        os.makedirs(local_dir, exist_ok=True)
        output_file = os.path.join(local_dir, filename)
        
        try:
            # SAVE AS PURE JSON ARRAY - NO WRAPPER METADATA
            # Format: [ {...}, {...}, {...} ]
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(documents, f, ensure_ascii=False, indent=2)
            
            print(f"\n[Local] Saved: {output_file}")
            print(f"[Local] Books: {len(documents)}")
            
            # Upload to MinIO if available
            if self.minio_client:
                object_name = f"bronze/{source}/json/{filename}"
                self.minio_client.fput_object(
                    self.bucket, 
                    object_name, 
                    output_file, 
                    content_type='application/json'
                )
                print(f"[MinIO] Uploaded: s3://{self.bucket}/{object_name}")
                if DOWNLOAD_PDFS:
                    print(f"[MinIO] PDFs Downloaded: {self.pdf_downloaded_count}")
                    print(f"[MinIO] PDFs Failed: {self.pdf_failed_count}")
                return object_name
            else:
                print("[MinIO] Not available, using local file only")
                return output_file
            
        except Exception as e:
            print(f"[Save Error] {e}")
            return ""
    
    def run_parallel(self):
        """Main execution - PARALLEL MODE"""
        print("="*70)
        print(f"OTL SCRAPER - FAST MODE - PARALLEL ({MAX_WORKERS} workers)")
        print("="*70)
        print(f"PDF Download: {DOWNLOAD_PDFS}")
        print(f"Subjects: {len(self.subjects)}")
        print("="*70)
        
        start_time = time.time()
        all_documents = []
        
        try:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_subject = {
                    executor.submit(self.scrape_single_subject, subject): subject 
                    for subject in self.subjects
                }
                
                completed = 0
                for future in as_completed(future_to_subject):
                    subject = future_to_subject[future]
                    completed += 1
                    
                    try:
                        documents = future.result()
                        all_documents.extend(documents)
                        print(f"\n[Progress] {completed}/{len(self.subjects)} subjects | {len(all_documents)} total books")
                    except Exception as e:
                        print(f"[Error] Subject '{subject['name']}' failed: {e}")
            
            if all_documents:
                print(f"\n{'='*70}")
                print("SAVING TO MINIO...")
                self.save_to_minio(all_documents, "otl", datetime.now().strftime("%Y-%m-%d"))
            
        except Exception as e:
            print(f"\n=== ERROR: {e} ===")
            if all_documents:
                self.save_to_minio(all_documents, "otl", datetime.now().strftime("%Y-%m-%d"))
            raise
        
        end_time = time.time()
        print(f"\n{'='*70}")
        print("FINAL STATISTICS")
        print(f"{'='*70}")
        print(f"Time: {end_time - start_time:.1f}s ({(end_time - start_time)/60:.1f}m)")
        print(f"Subjects: {len(self.subjects)}")
        print(f"Books: {len(all_documents)}")
        if DOWNLOAD_PDFS:
            print(f"PDFs Downloaded: {self.pdf_downloaded_count}")
        print(f"Speedup: ~{len(self.subjects)/MAX_WORKERS:.1f}x faster")
        print(f"{'='*70}")
        
        return {
            'status': 'success',
            'mode': 'parallel',
            'workers': MAX_WORKERS,
            'subjects_scraped': len(self.subjects),
            'total_books': len(all_documents),
            'pdf_downloaded': self.pdf_downloaded_count,
            'execution_time': end_time - start_time
        }
    
    def run(self):
        """Main execution wrapper - automatically chooses parallel or sequential mode"""
        if PARALLEL_MODE:
            return self.run_parallel()
        else:
            return self.run_sequential()
    
    def run_sequential(self):
        """Main execution - SEQUENTIAL MODE"""
        print("="*70)
        print(f"OTL SCRAPER - {'FAST' if FAST_MODE else 'NORMAL'} MODE - SEQUENTIAL")
        print("="*70)
        print(f"PDF Download: {DOWNLOAD_PDFS}")
        print(f"Subjects: {len(self.subjects)}")
        print("="*70)
        
        start_time = time.time()
        all_documents = []
        
        try:
            for idx, subject in enumerate(self.subjects, 1):
                print(f"\n{'='*70}")
                print(f"SUBJECT [{idx}/{len(self.subjects)}]: {subject['name']}")
                print(f"{'='*70}")
                
                subject_start = time.time()
                
                documents = self.get_textbooks_list_selenium(
                    subject_name=subject['name'],
                    subject_url=subject['url']
                )
                
                all_documents.extend(documents)
                
                subject_end = time.time()
                print(f"[Done] {subject['name']}: {len(documents)} books ({subject_end - subject_start:.1f}s)")
                print(f"[Total] {len(all_documents)} books")
                
                if idx < len(self.subjects) and SUBJECT_COOLDOWN_SEC > 0:
                    if not FAST_MODE:
                        print(f"[Cooldown] {SUBJECT_COOLDOWN_SEC}s...")
                    time.sleep(SUBJECT_COOLDOWN_SEC)
            
            if all_documents:
                print(f"\n{'='*70}")
                print("SAVING TO MINIO...")
                self.save_to_minio(all_documents, "otl", datetime.now().strftime("%Y-%m-%d"))
            
        except KeyboardInterrupt:
            print("\n=== INTERRUPTED ===")
            if all_documents:
                self.save_to_minio(all_documents, "otl", datetime.now().strftime("%Y-%m-%d"))
            raise
            
        except Exception as e:
            print(f"\n=== ERROR: {e} ===")
            if all_documents:
                self.save_to_minio(all_documents, "otl", datetime.now().strftime("%Y-%m-%d"))
            raise
        
        end_time = time.time()
        print(f"\n{'='*70}")
        print("FINAL STATISTICS")
        print(f"{'='*70}")
        print(f"Time: {end_time - start_time:.1f}s ({(end_time - start_time)/60:.1f}m)")
        print(f"Subjects: {len(self.subjects)}")
        print(f"Books: {len(all_documents)}")
        if DOWNLOAD_PDFS:
            print(f"PDFs Downloaded: {self.pdf_downloaded_count}")
        print(f"{'='*70}")
        
        return {
            'status': 'success',
            'mode': 'sequential',
            'subjects_scraped': len(self.subjects),
            'total_books': len(all_documents),
            'pdf_downloaded': self.pdf_downloaded_count,
            'execution_time': end_time - start_time
        }

def main():
    """Entry point"""
    scraper = OTLScraperStandalone()
    
    if PARALLEL_MODE:
        result = scraper.run_parallel()
    else:
        result = scraper.run_sequential()
    
    print(f"\n{json.dumps(result, indent=2)}")

if __name__ == "__main__":
    main()