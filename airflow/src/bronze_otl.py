#!/usr/bin/env python3
"""
Open Textbook Library Scraper - Bronze Layer
Scrapes textbook metadata from open.umn.edu/opentextbooks
"""

import os
import json
import time
import hashlib
import requests
import re
from datetime import datetime
from typing import List, Dict, Any, Set, Optional
from urllib.parse import urljoin
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

# MinIO imports
try:
    from minio import Minio
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False


class OTLScraper:
    """Open Textbook Library scraper for bronze layer - textbook metadata only"""
    
    BASE_URL = "https://open.umn.edu"
    
    # Default subjects fallback
    DEFAULT_SUBJECTS = [
        'accounting', 'finance', 'management', 'marketing', 'business',
        'computer-science', 'information-systems', 'programming-languages',
        'education', 'engineering-technology', 'civil-engineering',
        'electrical-engineering', 'mechanical-engineering',
        'humanities', 'history', 'philosophy', 'languages', 'literature',
        'law', 'mathematics', 'statistics',
        'medicine', 'nursing', 'health-sciences',
        'biology', 'chemistry', 'physics', 'astronomy',
        'economics', 'psychology', 'sociology', 'political-science'
    ]
    
    def __init__(self, delay: float = 0.5, max_documents: int = None,
                 parallel: bool = True, max_workers: int = 2,
                 output_dir: str = "/opt/airflow/scraped_data/otl",
                 download_pdfs: bool = None):
        self.source = "otl"
        self.delay = delay
        self.max_documents = max_documents or int(os.getenv('MAX_DOCUMENTS', '999999'))
        self.parallel = parallel
        self.max_workers = max_workers
        self.output_dir = output_dir
        # Read from env if not explicitly passed
        if download_pdfs is None:
            download_pdfs = os.getenv('OTL_DOWNLOAD_PDFS', '1').lower() in ('1', 'true', 'yes')
        self.download_pdfs = download_pdfs
        self.driver = None
        
        # HTTP Session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Setup
        self._setup_minio()
        
        # Load existing books for deduplication
        self.existing_book_ids = self._load_existing_book_ids()
        if self.existing_book_ids:
            print(f"[OTL] Loaded {len(self.existing_book_ids)} existing books")
        
        # Get subjects
        self.subjects = self._get_subjects()
        
        # Stats
        self.total_scraped = 0
        
        mode = "PARALLEL" if parallel else "SEQUENTIAL"
        print(f"[OTL] Scraper initialized - {mode} mode, Max docs: {self.max_documents}")
        print(f"[OTL] Subjects: {len(self.subjects)}")
    
    def _setup_minio(self):
        """Setup MinIO client"""
        self.minio_client = None
        self.minio_bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        self.minio_enable = os.getenv('MINIO_ENABLE', '1').lower() in ('1', 'true', 'yes')
        
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
    
    def _setup_selenium(self) -> Optional[webdriver.Chrome]:
        """Setup Selenium WebDriver with minimal child-process footprint."""
        if not SELENIUM_AVAILABLE:
            return None

        try:
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1280,800")
            options.add_argument("--blink-settings=imagesEnabled=false")
            # --- reduce child-process spawning ---
            options.add_argument("--no-zygote")                       # skip zygote helper
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-background-networking")
            options.add_argument("--disable-default-apps")
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-renderer-backgrounding")
            options.add_argument("--disable-backgrounding-occluded-windows")
            options.add_argument("--disable-client-side-phishing-detection")
            options.add_argument("--disable-sync")
            options.add_argument("--metrics-recording-only")
            options.add_argument("--mute-audio")

            for path in ['/usr/local/bin/chromedriver', '/usr/bin/chromedriver', 'chromedriver']:
                try:
                    if os.path.exists(path) or path == 'chromedriver':
                        service = Service(path)
                        return webdriver.Chrome(service=service, options=options)
                except Exception:
                    continue

            return webdriver.Chrome(options=options)

        except Exception as e:
            print(f"[Selenium] Setup failed: {e}")
            return None

    def _quit_driver(self, driver) -> None:
        """Quit WebDriver and forcefully reap any remaining Chrome child processes."""
        if driver is None:
            return
        # Grab chromedriver PID before quit so we can SIGKILL its process group
        service_pid: Optional[int] = None
        try:
            service_pid = driver.service.process.pid if driver.service and driver.service.process else None
        except Exception:
            pass

        try:
            driver.quit()
        except Exception:
            pass

        # Give Chrome 1 s to exit cleanly, then force-kill the whole process group
        if service_pid:
            import signal
            time.sleep(1)
            try:
                os.killpg(os.getpgid(service_pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass  # already dead — that's fine
    
    # =========================================================================
    # SUBJECTS
    # =========================================================================
    
    def _get_subjects(self) -> List[Dict[str, str]]:
        """Get list of subjects dynamically or use fallback"""
        subjects = []
        base_url = f"{self.BASE_URL}/opentextbooks"
        
        driver = self._setup_selenium()
        if not driver:
            return self._get_subjects_fallback()
        
        try:
            driver.get(base_url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(2)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            seen_urls = set()
            
            # Find subject links
            for link in soup.find_all('a', href=re.compile(r'/subjects/[a-z-]+')):
                href = link.get('href', '')
                name = link.get_text(strip=True)
                
                if any(x in href for x in ['/subjects/all', '/subjects?', '/subjects#']):
                    continue
                
                full_url = urljoin(self.BASE_URL, href)
                if full_url not in seen_urls and name:
                    subjects.append({'name': name, 'url': full_url})
                    seen_urls.add(full_url)
            
            self._quit_driver(driver)

            if subjects:
                print(f"[OTL] Found {len(subjects)} subjects dynamically")
                return subjects

        except Exception as e:
            print(f"[OTL] Error fetching subjects: {e}")
            self._quit_driver(driver)
        
        return self._get_subjects_fallback()
    
    def _get_subjects_fallback(self) -> List[Dict[str, str]]:
        """Fallback subject list"""
        base_url = f"{self.BASE_URL}/opentextbooks"
        return [
            {'name': s.replace('-', ' ').title(), 'url': f"{base_url}/subjects/{s}"}
            for s in self.DEFAULT_SUBJECTS
        ]
    
    # =========================================================================
    # MAIN SCRAPING
    # =========================================================================
    
    def scrape(self) -> List[Dict[str, Any]]:
        """Main scraping method"""
        print(f"[OTL] Starting scraper...")
        start_time = time.time()
        
        all_documents = []
        
        try:
            if self.parallel and self.max_workers > 1:
                all_documents = self._scrape_parallel()
            else:
                all_documents = self._scrape_sequential()
            
            elapsed = time.time() - start_time
            print(f"\n[OTL] Completed: {len(all_documents)} books in {elapsed:.1f}s")
            return all_documents
            
        except Exception as e:
            print(f"[OTL] Error: {e}")
            return all_documents
    
    def _scrape_parallel(self) -> List[Dict[str, Any]]:
        """Scrape subjects in parallel"""
        all_documents = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._scrape_subject, subj): subj
                for subj in self.subjects
            }
            
            for i, future in enumerate(as_completed(futures), 1):
                subject = futures[future]
                try:
                    docs = future.result()
                    all_documents.extend(docs)
                    print(f"[Progress] {i}/{len(self.subjects)} subjects | {len(all_documents)} total books")
                except Exception as e:
                    print(f"[Error] {subject['name']}: {e}")
        
        return all_documents
    
    def _scrape_sequential(self) -> List[Dict[str, Any]]:
        """Scrape subjects sequentially"""
        all_documents = []
        
        for i, subject in enumerate(self.subjects, 1):
            print(f"\n[{i}/{len(self.subjects)}] Subject: {subject['name']}")
            
            docs = self._scrape_subject(subject)
            all_documents.extend(docs)
            
            print(f"[{subject['name']}] {len(docs)} books | Total: {len(all_documents)}")
            
            if i < len(self.subjects):
                time.sleep(1)
        
        return all_documents
    
    def _scrape_subject(self, subject: Dict[str, str]) -> List[Dict[str, Any]]:
        """Scrape all books from a subject"""
        documents = []
        driver = self._setup_selenium()
        
        if not driver:
            print(f"[{subject['name']}] Selenium not available")
            return documents
        
        try:
            # Get book URLs
            book_urls = self._get_book_urls(driver, subject)
            print(f"[{subject['name']}] Found {len(book_urls)} books")
            
            # Scrape each book
            skipped = 0
            for idx, url in enumerate(book_urls, 1):
                # Check limit
                if self.total_scraped >= self.max_documents:
                    print(f"[{subject['name']}] Reached max limit ({self.max_documents})")
                    break
                
                book_id = self._create_document_id(url)
                
                # Skip if already scraped
                if book_id in self.existing_book_ids:
                    skipped += 1
                    continue
                
                # Scrape book
                book_data = self._scrape_book(driver, url, subject['name'])
                if book_data:
                    documents.append(book_data)
                    self.existing_book_ids.add(book_data['id'])
                    self.total_scraped += 1
                
                if idx % 10 == 0:
                    print(f"[{subject['name']}] {idx}/{len(book_urls)}")
                
                time.sleep(self.delay)
            
            if skipped > 0:
                print(f"[{subject['name']}] Skipped {skipped} already scraped")
            
        except Exception as e:
            print(f"[{subject['name']}] Error: {e}")
        finally:
            self._quit_driver(driver)

        return documents
    
    # =========================================================================
    # URL EXTRACTION
    # =========================================================================
    
    def _get_book_urls(self, driver, subject: Dict[str, str]) -> List[str]:
        """Get all book URLs from subject page with infinite scroll"""
        urls = []
        
        try:
            driver.get(subject['url'])
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(1)
            
            # Scroll to load all books
            self._scroll_to_bottom(driver)
            
            # Extract URLs
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            for link in soup.select('a[href*="/opentextbooks/textbooks/"]'):
                href = link.get('href', '')
                if any(x in href for x in ['/submit', '/newest', '/in_development']):
                    continue
                
                full_url = urljoin(self.BASE_URL, href)
                if full_url not in urls:
                    urls.append(full_url)
            
        except Exception as e:
            print(f"[{subject['name']}] URL extraction error: {e}")
        
        return urls
    
    def _scroll_to_bottom(self, driver, max_scrolls: int = 50):
        """Scroll page to load all content"""
        last_height = driver.execute_script("return document.body.scrollHeight")
        no_change_count = 0
        
        for i in range(max_scrolls):
            # Scroll down
            driver.execute_script("window.scrollBy(0, 2000);")
            time.sleep(0.5)
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                no_change_count += 1
                if no_change_count >= 3:
                    break
            else:
                no_change_count = 0
            
            last_height = new_height
    
    # =========================================================================
    # BOOK SCRAPING
    # =========================================================================
    
    def _scrape_book(self, driver, url: str, subject_name: str) -> Optional[Dict[str, Any]]:
        """Scrape individual book metadata"""
        try:
            driver.get(url)
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(0.5)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            title = self._extract_title(soup)
            book_id = self._create_document_id(url)
            
            # Extract license and PDF format URLs from the book page
            page_info = self._extract_license_and_formats(soup)
            
            # Download PDF if enabled
            pdf_result = {'downloaded': False, 'paths': []}
            if self.download_pdfs and page_info['pdf_format_urls']:
                pdf_result = self._download_book_pdf(book_id, title, page_info['pdf_format_urls'])
            
            return {
                'id': book_id,
                'title': title,
                'description': self._extract_description(soup),
                'authors': self._extract_authors(soup),
                'subject': [subject_name],
                'url': url,
                'source': self.source,   # "otl" — consistent with mit_ocw / openstax
                'language': 'en',
                'license': page_info['license_name'],
                'license_url': page_info['license_url'],
                'scraped_at': datetime.now().isoformat(),
                'pdf_count': len(pdf_result['paths']),
                'pdf_paths': pdf_result['paths']
            }
            
        except Exception as e:
            print(f"  ✗ Error scraping {url}: {e}")
            return None
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract book title"""
        h1 = soup.select_one('#info h1') or soup.find('h1')
        if h1:
            return h1.get_text(strip=True)
        
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text(strip=True)
        
        return 'Unknown Book'
    
    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Extract book description"""
        about = soup.select_one('#AboutBook')
        if about:
            p = about.find('p')
            if p:
                return p.get_text('\n', strip=True)[:500]
        return ""
    
    def _extract_authors(self, soup: BeautifulSoup) -> List[str]:
        """Extract author names"""
        authors = []
        
        # Method 1: Contributors paragraph
        for p in soup.find_all('p'):
            txt = p.get_text(' ', strip=True)
            if txt.lower().startswith('contributors:'):
                raw = txt.split(':', 1)[1].strip()
                parts = [re.sub(r'\s+', ' ', x).strip() for x in re.split(r',| and ', raw)]
                for a in parts:
                    if a and a not in authors:
                        authors.append(a)
                return authors[:10]
        
        # Method 2: Meta tags
        for meta_name in ['author', 'book:author']:
            for m in soup.find_all('meta', attrs={'name': meta_name}):
                val = (m.get('content') or '').strip()
                if val and val not in authors:
                    authors.append(val)
        
        return authors[:10]

    # =========================================================================
    # LICENSE & PDF HELPERS
    # =========================================================================

    def _cc_url_to_name(self, url: str) -> str:
        """Convert a Creative Commons URL to a short human-readable name.
        e.g. https://creativecommons.org/licenses/by-nc-sa/4.0/ -> 'CC BY-NC-SA 4.0'
        """
        if not url:
            return 'Creative Commons'
        if 'publicdomain/zero' in url:
            return 'CC0 1.0'
        m = re.search(r'/licenses/([a-z-]+)/([\d.]+)', url)
        if m:
            return f"CC {m.group(1).upper()} {m.group(2)}"
        m = re.search(r'/licenses/([a-z-]+)', url)
        if m:
            return f"CC {m.group(1).upper()}"
        return 'Creative Commons'

    def _sanitize_filename(self, name: str) -> str:
        """Create a filesystem-safe filename, max 100 chars."""
        safe = re.sub(r'[^\w\s-]', '', name)
        safe = re.sub(r'\s+', '_', safe.strip())
        return safe[:100] or 'document'

    def _extract_license_and_formats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract license info and PDF format links from an OTL book page."""
        result: Dict[str, Any] = {
            'license_name': None,
            'license_url': None,
            'pdf_format_urls': []
        }

        # --- License: find creativecommons.org or gnu.org FDL link ---
        for a in soup.find_all('a', href=True):
            href = a.get('href', '')
            # Normalise to full URL
            if href.startswith('//'):
                full = 'https:' + href
            elif href.startswith('http'):
                full = href
            else:
                full = None

            if full and 'creativecommons.org/licenses/' in full:
                result['license_url'] = full
                result['license_name'] = self._cc_url_to_name(full)
                break
            if full and 'creativecommons.org/publicdomain/' in full:
                result['license_url'] = full
                result['license_name'] = self._cc_url_to_name(full)
                break
            if full and 'gnu.org/licenses/fdl' in full:
                result['license_url'] = full
                result['license_name'] = 'GNU FDL'
                break

        # Fallback: CC badge image alt text (e.g. alt="CC BY-NC-SA")
        if not result['license_name']:
            for img in soup.find_all('img', alt=True):
                alt = img.get('alt', '').strip()
                if alt.upper().startswith('CC '):
                    parent = img.find_parent('a', href=True)
                    if parent:
                        href = parent.get('href', '')
                        if 'creativecommons.org' in href:
                            full = href if href.startswith('http') else 'https:' + href
                            result['license_url'] = full
                            result['license_name'] = self._cc_url_to_name(full)
                    else:
                        result['license_name'] = alt
                    break

        # --- PDF formats: links with text == 'PDF' pointing to /formats/ ---
        for a in soup.find_all('a', href=True):
            href = a.get('href', '')
            text = a.get_text(strip=True).upper()
            if '/opentextbooks/formats/' in href and text == 'PDF':
                full_url = urljoin(self.BASE_URL, href)
                if full_url not in result['pdf_format_urls']:
                    result['pdf_format_urls'].append(full_url)

        return result

    def _resolve_format_url(self, format_url: str) -> Optional[str]:
        """Follow the OTL format redirect and return an actual downloadable PDF URL.

        OTL /formats/{id} links redirect to external sources which may be:
        - A direct .pdf URL
        - An HTML page from which we must extract a .pdf link
        """
        try:
            # HEAD first (cheap), fall back to streaming GET if HEAD fails
            final_url = None
            content_type = ''
            try:
                resp = self.session.head(format_url, allow_redirects=True, timeout=15)
                final_url = resp.url
                content_type = resp.headers.get('Content-Type', '')
            except Exception:
                pass

            if not final_url:
                resp = self.session.get(format_url, allow_redirects=True,
                                        timeout=20, stream=True)
                final_url = resp.url
                content_type = resp.headers.get('Content-Type', '')
                resp.close()

            url_lower = final_url.lower()

            # Direct PDF indicators
            if url_lower.endswith('.pdf'):
                return final_url
            if 'application/pdf' in content_type:
                return final_url
            if 'type=pdf' in url_lower:
                return final_url

            # HTML landing page — try to scrape a .pdf link from it
            resp2 = self.session.get(final_url, timeout=20)
            if resp2.status_code == 200 and 'text/html' in resp2.headers.get('Content-Type', ''):
                page_soup = BeautifulSoup(resp2.text, 'html.parser')
                for a in page_soup.find_all('a', href=True):
                    link = a['href']
                    if link.lower().endswith('.pdf'):
                        return urljoin(final_url, link)

            return None

        except Exception as e:
            print(f"  [OTL PDF] Could not resolve {format_url}: {e}")
            return None

    def _fetch_pdf(self, url: str, max_retries: int = 3) -> Optional[bytes]:
        """Download PDF bytes with retry logic (max 200 MB)."""
        MAX_SIZE = 200 * 1024 * 1024  # 200 MB

        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, stream=True, timeout=30)
                resp.raise_for_status()

                # Reject accidental HTML responses
                ct = resp.headers.get('Content-Type', '')
                if 'text/html' in ct and '.pdf' not in url.lower():
                    return None

                size = 0
                chunks = []
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        size += len(chunk)
                        if size > MAX_SIZE:
                            print(f"  [OTL PDF] File exceeds {MAX_SIZE // 1024 // 1024} MB, skipping")
                            return None
                        chunks.append(chunk)

                data = b''.join(chunks)
                if len(data) < 1024:   # too small to be a real PDF
                    return None
                return data

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  [OTL PDF] Download failed ({url[:60]}...): {e}")

        return None

    def _download_book_pdf(self, book_id: str, book_title: str,
                           format_urls: List[str]) -> Dict[str, Any]:
        """Resolve, download, and upload the first available PDF for an OTL book.

        MinIO path: bronze/otl/otl-pdfs/{book_id}/{safe_title}.pdf
        (Compatible with elasticsearch_sync.py which looks up bronze/otl/otl-pdfs/{normalized_id})
        """
        import io
        result: Dict[str, Any] = {'downloaded': False, 'paths': []}

        if not self.minio_client:
            return result

        safe_title = self._sanitize_filename(book_title)
        minio_path = f"bronze/otl/otl-pdfs/{book_id}/{safe_title}.pdf"

        # Deduplication — skip if already uploaded
        try:
            self.minio_client.stat_object(self.minio_bucket, minio_path)
            print(f"  [OTL PDF] Already exists, skipping: {minio_path}")
            result['paths'].append(minio_path)
            result['downloaded'] = True
            return result
        except Exception:
            pass  # Not found — proceed

        # Try each format URL in order until one succeeds
        for format_url in format_urls:
            actual_url = self._resolve_format_url(format_url)
            if not actual_url:
                continue

            print(f"  [OTL PDF] Downloading: {actual_url[:80]}...")
            pdf_bytes = self._fetch_pdf(actual_url)
            if not pdf_bytes:
                continue

            try:
                self.minio_client.put_object(
                    self.minio_bucket,
                    minio_path,
                    io.BytesIO(pdf_bytes),
                    length=len(pdf_bytes),
                    content_type='application/pdf'
                )
                print(f"  [OTL PDF] ✓ Uploaded: {minio_path}")
                result['paths'].append(minio_path)
                result['downloaded'] = True
                return result
            except Exception as e:
                print(f"  [OTL PDF] MinIO upload error: {e}")

        if not result['downloaded']:
            print(f"  [OTL PDF] No downloadable PDF found for '{book_title}'")

        return result

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
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"otl_bronze_{timestamp}.json"
        
        # Save locally first
        output_file = os.path.join(self.output_dir, filename)
        os.makedirs(self.output_dir, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"[OTL] Saved locally: {output_file}")
        
        # Upload to MinIO
        if self.minio_client:
            try:
                minio_path = f"bronze/otl/json/{filename}"
                self.minio_client.fput_object(
                    self.minio_bucket, minio_path, output_file,
                    content_type='application/json'
                )
                print(f"[MinIO] Uploaded: {minio_path}")
                return minio_path
            except Exception as e:
                print(f"[MinIO] Upload failed: {e}")
        
        return output_file
    
    def _load_existing_book_ids(self) -> Set[str]:
        """Load existing book IDs from MinIO for deduplication"""
        existing = set()
        
        if not self.minio_client:
            return existing
        
        try:
            objects = self.minio_client.list_objects(
                self.minio_bucket,
                prefix='bronze/otl/json/',
                recursive=True
            )
            
            for obj in objects:
                if not obj.object_name.endswith('.json'):
                    continue
                
                try:
                    response = self.minio_client.get_object(self.minio_bucket, obj.object_name)
                    content = response.read().decode('utf-8')
                    response.close()
                    response.release_conn()
                    
                    records = json.loads(content)
                    if isinstance(records, list):
                        for record in records:
                            if isinstance(record, dict) and record.get('id'):
                                existing.add(record['id'])
                                
                except Exception as e:
                    print(f"[OTL] Skipping {obj.object_name}: {e}")
                    
        except Exception as e:
            print(f"[OTL] Could not scan MinIO: {e}")
        
        return existing


    def run(self):
        """Main execution - for DAG compatibility"""
        documents = self.scrape()
        if documents:
            self.save_to_minio(documents)
        return {
            'status': 'success',
            'total_books': len(documents),
            'subjects_scraped': len(self.subjects)
        }


# Legacy class name for DAG compatibility
OTLScraperStandalone = OTLScraper


def run_otl_scraper(**kwargs) -> List[Dict[str, Any]]:
    """Run OTL scraper - entry point for DAG"""
    scraper = OTLScraper(**kwargs)
    try:
        return scraper.scrape()
    finally:
        pass  # Cleanup handled in scrape method


if __name__ == "__main__":
    import sys
    
    max_docs = int(sys.argv[1]) if len(sys.argv) > 1 else None
    parallel = '--sequential' not in sys.argv
    
    scraper = OTLScraper(max_documents=max_docs, parallel=parallel)
    documents = scraper.scrape()
    
    if documents:
        scraper.save_to_minio(documents)
    
    print(f"Completed! Scraped {len(documents)} books.")
