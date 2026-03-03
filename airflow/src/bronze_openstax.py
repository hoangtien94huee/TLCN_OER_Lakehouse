#!/usr/bin/env python3
"""
OpenStax Scraper - Bronze Layer
Scrapes textbook metadata AND full-book PDFs from OpenStax.org

PDF Storage structure in MinIO:
  bronze/openstax/pdfs/{book-slug}/College_Physics_2e.pdf
  (OpenStax provides a single full-book PDF per title; no sub-folders needed
   unless multiple editions/languages exist)
"""

import os
import json
import time
import hashlib
import requests
import re
import io
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Set, Optional, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

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


class OpenStaxScraper:
    """OpenStax scraper for bronze layer - textbook metadata only"""
    
    BASE_URL = "https://openstax.org"
    
    def __init__(self, delay: float = 2.0, use_selenium: bool = True,
                 max_documents: int = None, output_dir: str = "/opt/airflow/scraped_data/openstax",
                 download_pdfs: bool = True):
        self.source = "openstax"
        self.delay = delay
        self.max_documents = max_documents
        self.output_dir = output_dir
        self.driver = None

        # PDF config
        self.download_pdfs = download_pdfs

        # HTTP Session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # Setup
        self._setup_minio()

        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        if self.use_selenium:
            self._setup_selenium()

        # Load existing books for deduplication
        self.existing_book_ids = self._load_existing_book_ids()
        if self.existing_book_ids:
            print(f"[OpenStax] Loaded {len(self.existing_book_ids)} existing books")

        print(f"[OpenStax] Scraper initialized - Selenium: {self.use_selenium}, "
              f"Max docs: {self.max_documents or 'ALL'}, Download PDFs: {self.download_pdfs}")
    
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
    
    def _setup_selenium(self):
        """Setup Selenium WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            for path in ['/usr/local/bin/chromedriver', '/usr/bin/chromedriver', 'chromedriver']:
                try:
                    if os.path.exists(path) or path == 'chromedriver':
                        service = Service(path)
                        self.driver = webdriver.Chrome(service=service, options=chrome_options)
                        print(f"[Selenium] Initialized with: {path}")
                        return
                except Exception:
                    continue
            
            print("[Selenium] Could not initialize, falling back to requests")
            self.use_selenium = False
            
        except Exception as e:
            print(f"[Selenium] Setup failed: {e}")
            self.use_selenium = False
    
    def cleanup(self):
        """Cleanup resources"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
    
    # =========================================================================
    # PAGE FETCHING
    # =========================================================================
    
    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Get page content - prefer Selenium for JS-heavy pages"""
        if self.use_selenium and self.driver:
            return self._get_page_selenium(url)
        return self._get_page_requests(url)
    
    def _get_page_selenium(self, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        """Get page with Selenium"""
        for attempt in range(max_retries):
            try:
                if not self._is_driver_alive():
                    self._setup_selenium()
                    if not self.driver:
                        return None
                
                self.driver.get(url)
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(2)
                
                # Scroll to trigger lazy loading
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                
                return BeautifulSoup(self.driver.page_source, 'html.parser')
                
            except Exception as e:
                if "disconnected" in str(e).lower():
                    if self.driver:
                        try:
                            self.driver.quit()
                        except:
                            pass
                        self.driver = None
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                break
        
        return None
    
    def _get_page_requests(self, url: str) -> Optional[BeautifulSoup]:
        """Get page with requests (fallback)"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"[Requests] Error: {e}")
            return None
    
    def _is_driver_alive(self) -> bool:
        """Check if Selenium driver is still alive"""
        try:
            self.driver.current_url
            return True
        except:
            return False
    
    # =========================================================================
    # MAIN SCRAPING
    # =========================================================================
    
    def scrape(self) -> List[Dict[str, Any]]:
        """Main scraping method"""
        print("[OpenStax] Starting scraper...")
        
        try:
            # Get book URLs
            book_urls = self._get_book_urls()
            print(f"[OpenStax] Found {len(book_urls)} books")
            
            if self.max_documents:
                book_urls = list(book_urls)[:self.max_documents]
                print(f"[OpenStax] Limited to {len(book_urls)} books")
            
            # Scrape books
            documents = []
            skipped = 0
            
            for i, url in enumerate(book_urls, 1):
                book_hash = self._create_document_id(url)
                
                # Skip if already scraped
                if book_hash in self.existing_book_ids:
                    skipped += 1
                    continue
                
                print(f"[{i}/{len(book_urls)}] Scraping: {url}")
                
                book_data = self._scrape_book(url)
                if book_data:
                    documents.append(book_data)
                    self.existing_book_ids.add(book_data['id'])
                    pdf_count = book_data.get('pdf_count', 0)
                    print(f"  ✓ {book_data['title'][:50]}... | PDFs: {pdf_count}")
                
                time.sleep(self.delay)
            
            print(f"\n[OpenStax] Completed: {len(documents)} new, {skipped} skipped")
            return documents
            
        except Exception as e:
            print(f"[OpenStax] Error: {e}")
            return []
        finally:
            self.cleanup()
    
    # =========================================================================
    # URL EXTRACTION
    # =========================================================================
    
    def _get_book_urls(self) -> Set[str]:
        """Get all book URLs from subject pages"""
        book_urls = set()
        
        # Get subject URLs
        subject_urls = self._get_subject_urls()
        print(f"[OpenStax] Found {len(subject_urls)} subjects")
        
        # Extract book URLs from each subject
        for i, subject_url in enumerate(subject_urls, 1):
            print(f"  [{i}/{len(subject_urls)}] {subject_url}")
            soup = self._get_page(subject_url)
            if not soup:
                continue
            
            # Find book links
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                if '/books/' in href and self._is_valid_book_link(text):
                    full_url = urljoin(self.BASE_URL, href).split('?')[0].split('#')[0]
                    if full_url not in book_urls:
                        book_urls.add(full_url)
            
            time.sleep(0.5)
        
        return book_urls
    
    def _get_subject_urls(self) -> List[str]:
        """Get subject category URLs"""
        subject_urls = []
        
        soup = self._get_page(f"{self.BASE_URL}/subjects")
        if soup:
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if '/subjects/' in href and href != '/subjects' and '#' not in href:
                    full_url = urljoin(self.BASE_URL, href).split('#')[0]
                    if full_url not in subject_urls:
                        subject_urls.append(full_url)
        
        # Fallback if no subjects found
        if not subject_urls:
            subject_urls = [
                f"{self.BASE_URL}/subjects/math",
                f"{self.BASE_URL}/subjects/science",
                f"{self.BASE_URL}/subjects/social-sciences",
                f"{self.BASE_URL}/subjects/humanities",
                f"{self.BASE_URL}/subjects/business",
            ]
        
        return subject_urls
    
    def _is_valid_book_link(self, link_text: str) -> bool:
        """Check if link text looks like a book title"""
        if not link_text or len(link_text) < 10 or len(link_text) > 100:
            return False
        
        skip_words = ['view', 'read', 'access', 'download', 'get', 'click', 
                      'more', 'resources', 'instructor', 'student', 'errata']
        text_lower = link_text.lower()
        return not any(word in text_lower for word in skip_words)
    
    # =========================================================================
    # BOOK SCRAPING
    # =========================================================================
    
    def _scrape_book(self, url: str) -> Optional[Dict[str, Any]]:
        """Scrape individual book metadata + download PDF"""
        soup = self._get_page(url)
        if not soup:
            return None

        try:
            pub_year = self._extract_publication_year(soup)
            book_data = {
                'id': self._create_document_id(url),
                'title': self._extract_title(soup, url),
                'description': self._extract_description(soup),
                'authors': self._extract_authors(soup),
                'subject': self._extract_subject(soup),
                'url': url,
                'source': self.source,
                'language': 'en',
                # OpenStax publishes under CC BY 4.0
                'license': 'CC BY 4.0',
                'license_url': 'https://creativecommons.org/licenses/by/4.0/',
                'publication_date': pub_year,
                'scraped_at': datetime.now().isoformat()
            }

            # Download PDF
            if self.download_pdfs and self.minio_client:
                book_slug = self._extract_book_slug(url)
                pdf_result = self._download_book_pdf(url, book_slug, book_data['title'])
                book_data['pdf_count'] = pdf_result['downloaded']
                book_data['pdf_paths'] = pdf_result['paths']
            else:
                book_data['pdf_count'] = 0
                book_data['pdf_paths'] = []

            return book_data
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return None
    
    def _extract_title(self, soup: BeautifulSoup, url: str) -> str:
        """Extract book title"""
        # Try hero title
        elem = soup.select_one('h1.hero-title, h1[data-testid="hero-title"]')
        if elem:
            title = elem.get_text(strip=True)
            if title and 'OpenStax' not in title:
                return title
        
        # Try meta tag
        meta = soup.find('meta', {'property': 'og:title'})
        if meta and meta.get('content'):
            title = meta.get('content', '')
            if title and 'OpenStax' not in title:
                return title
        
        # Try page title
        title_elem = soup.find('title')
        if title_elem:
            title = title_elem.get_text(strip=True).replace(' | OpenStax', '')
            if title:
                return title
        
        # Extract from URL
        if '/books/' in url:
            return url.split('/books/')[-1].replace('-', ' ').title()
        
        return 'Unknown Book'
    
    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Extract book description"""
        selectors = [
            ('meta', {'name': 'description'}),
            ('meta', {'property': 'og:description'}),
        ]
        
        for tag, attrs in selectors:
            elem = soup.find(tag, attrs)
            if elem and elem.get('content'):
                desc = elem.get('content', '').strip()
                if len(desc) > 50:
                    return desc[:500] if len(desc) > 500 else desc
        
        # Try description divs
        for selector in ['.book-description', '.description', '.hero-subtitle']:
            elem = soup.select_one(selector)
            if elem:
                desc = elem.get_text(strip=True)
                if len(desc) > 50:
                    return desc[:500] if len(desc) > 500 else desc
        
        return ""
    
    def _extract_authors(self, soup: BeautifulSoup) -> List[str]:
        """Extract author names"""
        authors = []
        
        selectors = ['.loc-senior-author', '.book-authors .author', 
                     '.authors .author', '.contributor']
        
        for selector in selectors:
            for elem in soup.select(selector):
                text = elem.get_text(strip=True).strip('"').strip("'")
                if not text:
                    continue
                
                # Parse author (handle "Name, University" format)
                parts = [p.strip() for p in text.split(',')]
                name = parts[0] if parts else text
                
                # Skip if looks like institution
                institution_words = ['university', 'college', 'institute', 'school']
                if any(w in name.lower() for w in institution_words):
                    continue
                
                if name and name not in authors and len(name) > 2:
                    authors.append(name)
        
        return authors[:10]  # Limit to 10 authors
    
    def _fetch_details_page_info(self, book_slug: str) -> Dict[str, Any]:
        """
        Fetch /details/books/{slug} once and extract:
          - license_name  e.g. 'CC BY 4.0'
          - license_url   e.g. 'https://creativecommons.org/licenses/by/4.0'
          - publish_date  e.g. '2022'
          - pdf_url       direct .pdf download link

        The details page contains all of these in one place, avoiding extra fetches.
        """
        result: Dict[str, Any] = {}
        details_url = f"{self.BASE_URL}/details/books/{book_slug}"

        try:
            soup = self._get_page(details_url)
            if not soup:
                return result

            # --- License: find CC link anywhere on the page ---
            # The details page has both:
            #   '... licensed under Creative Commons Attribution License v4.0'
            #   and a footer link to creativecommons.org
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if 'creativecommons.org/licenses/' in href or 'creativecommons.org/publicdomain/' in href:
                    result['license_url'] = href.rstrip('/')
                    result['license_name'] = self._cc_url_to_name(href)
                    break

            # --- Publish Date: look for "Publish Date:" label ---
            page_text = soup.get_text(separator='\n')
            m = re.search(
                r'[Pp]ublish(?:ed)?\s*[Dd]ate[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{4})',
                page_text
            )
            if m:
                raw = m.group(1).strip()
                # Extract just the year
                yr = re.search(r'(\d{4})', raw)
                if yr:
                    result['publish_date'] = yr.group(1)

            # --- PDF URL: direct .pdf link on the page ---
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if href.lower().endswith('.pdf') and 'assets.openstax.org' in href:
                    result['pdf_url'] = href
                    break

        except Exception as e:
            print(f"  [Details] Could not fetch {details_url}: {e}")

        return result

    def _cc_url_to_name(self, url: str) -> str:
        """Convert a creativecommons.org URL to a short human-readable name.
        e.g. https://creativecommons.org/licenses/by/4.0/ → 'CC BY 4.0'
        """
        url = url.rstrip('/')
        m = re.search(r'creativecommons\.org/licenses/([^/]+)/([^/]+)', url)
        if m:
            return f"CC {m.group(1).upper()} {m.group(2)}"
        m = re.search(r'creativecommons\.org/publicdomain/([^/]+)/([^/]+)', url)
        if m:
            return f"CC0 {m.group(2)}"
        return 'Creative Commons'

    def _extract_publication_year(self, soup: BeautifulSoup) -> Optional[str]:
        """Fallback year extraction from book/preface page (used if details page fails)."""
        page_text = soup.get_text()
        for pattern in [
            r'[Rr]evised\s+(\d{4})',
            r'©\s*(\d{4})',
            r'[Cc]opyright\s+(\d{4})',
        ]:
            m = re.search(pattern, page_text)
            if m:
                year = m.group(1)
                if 1990 <= int(year) <= 2030:
                    return year
        return None

    def _extract_subject(self, soup: BeautifulSoup) -> str:
        """Extract book subject/category"""
        # Try breadcrumb
        breadcrumb = soup.find('nav', {'aria-label': 'breadcrumb'})
        if breadcrumb:
            links = breadcrumb.find_all('a')
            if len(links) >= 2:
                subject = links[1].get_text(strip=True)
                if subject:
                    return subject.title()
        
        # Try category elements
        for selector in ['.book-category', '.subject', '.category']:
            elem = soup.select_one(selector)
            if elem:
                subject = elem.get_text(strip=True)
                if subject:
                    return subject.title()
        
        return ""
    
    # =========================================================================
    # PDF DOWNLOAD
    # =========================================================================

    def _extract_book_slug(self, url: str) -> str:
        """Extract book slug for folder naming.
        e.g. https://openstax.org/books/college-physics-2e/pages/preface
             → college-physics-2e
        """
        path = urlparse(url).path
        m = re.search(r'/books/([^/]+)', path)
        return m.group(1) if m else hashlib.md5(url.encode()).hexdigest()[:12]

    def _sanitize_filename(self, name: str) -> str:
        """Convert title to a safe, human-readable filename."""
        name = re.sub(r'[\\/:*?"<>|]', '', name)
        name = re.sub(r'\s+', '_', name.strip())
        name = re.sub(r'_+', '_', name)
        return name[:120]

    def _find_pdf_download_url(self, book_url: str, book_slug: str) -> Optional[str]:
        """
        Find the direct PDF download URL for an OpenStax book.

        Strategy (in order):
        1. OpenStax details page has a predictable direct PDF URL pattern:
               https://openstax.org/details/books/{slug}
           which contains an <a href="...pdf"> download link.
        2. Attempt the CDN pattern directly:
               https://assets.openstax.org/oscms-prodcms/media/documents/{slug}.pdf
               https://openstax.org/books/{slug}/pdf   (some books)
        3. Parse the book page itself for any .pdf link.
        """
        # --- Strategy 1: details page ---
        details_url = f"{self.BASE_URL}/details/books/{book_slug}"
        try:
            soup = self._get_page(details_url)
            if soup:
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    if href.lower().endswith('.pdf') and book_slug.replace('-', '') in href.replace('-', '').lower():
                        return urljoin(self.BASE_URL, href)
                # Broader fallback: any PDF link on the details page
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    if href.lower().endswith('.pdf'):
                        return urljoin(self.BASE_URL, href)
        except Exception:
            pass

        # --- Strategy 2: CDN patterns ---
        cdn_candidates = [
            f"https://assets.openstax.org/oscms-prodcms/media/documents/{book_slug}.pdf",
            f"https://d3bxy9euw4e147.cloudfront.net/oscms-prodcms/media/documents/{book_slug}.pdf",
        ]
        for cdn_url in cdn_candidates:
            try:
                resp = self.session.head(cdn_url, timeout=15, allow_redirects=True)
                if resp.status_code == 200 and 'pdf' in resp.headers.get('Content-Type', '').lower():
                    return cdn_url
            except Exception:
                pass

        # --- Strategy 3: book page itself ---
        try:
            soup = self._get_page(book_url)
            if soup:
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    if href.lower().endswith('.pdf'):
                        return urljoin(self.BASE_URL, href)
        except Exception:
            pass

        return None

    def _download_book_pdf(self, book_url: str, book_slug: str,
                           book_title: str,
                           known_pdf_url: Optional[str] = None) -> Dict[str, Any]:
        """
        Download the full-book PDF and upload to MinIO.
        If known_pdf_url is provided (from details page), skip the discovery step.
        """
        result = {'downloaded': 0, 'paths': []}

        if not self.minio_client:
            return result

        safe_title = self._sanitize_filename(book_title) or book_slug
        minio_path = f"bronze/openstax/pdfs/{book_slug}/{safe_title}.pdf"

        # Skip if already exists
        try:
            self.minio_client.stat_object(self.minio_bucket, minio_path)
            print(f"  [PDF] Already exists: {minio_path}")
            result['downloaded'] = 1
            result['paths'] = [minio_path]
            return result
        except Exception:
            pass

        # Use known URL if provided, otherwise discover
        pdf_url = known_pdf_url or self._find_pdf_download_url(book_url, book_slug)
        if not pdf_url:
            print(f"  [PDF] No PDF found for {book_slug}")
            return result

        print(f"  [PDF] Downloading: {pdf_url}")
        pdf_bytes = self._fetch_pdf(pdf_url)
        if not pdf_bytes:
            return result

        try:
            self.minio_client.put_object(
                self.minio_bucket,
                minio_path,
                io.BytesIO(pdf_bytes),
                length=len(pdf_bytes),
                content_type='application/pdf',
            )
            print(f"  [PDF] ✓ Uploaded {minio_path} ({len(pdf_bytes) // 1024 / 1024:.1f} MB)")
            result['downloaded'] = 1
            result['paths'] = [minio_path]
        except Exception as e:
            print(f"  [PDF] Upload failed: {e}")

        return result

    def _fetch_pdf(self, url: str, max_retries: int = 3) -> Optional[bytes]:
        """Download PDF bytes with retry and size guard (max 200 MB)."""
        max_size_bytes = 200 * 1024 * 1024  # 200 MB safety cap

        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=120, stream=True)
                resp.raise_for_status()

                content_type = resp.headers.get('Content-Type', '')
                if 'pdf' not in content_type.lower() and not url.lower().endswith('.pdf'):
                    return None

                # Stream download with size guard
                chunks = []
                total = 0
                for chunk in resp.iter_content(chunk_size=65536):
                    total += len(chunk)
                    if total > max_size_bytes:
                        print(f"  [PDF] Skipping {url}: exceeds 200 MB limit")
                        return None
                    chunks.append(chunk)
                return b''.join(chunks)

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"  [PDF] Download failed {url}: {e}")
        return None

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
        filename = f"openstax_bronze_{timestamp}.json"
        
        # Save locally first
        output_file = Path(self.output_dir) / filename
        output_file.parent.mkdir(exist_ok=True, parents=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"[OpenStax] Saved locally: {output_file}")
        
        # Upload to MinIO
        if self.minio_client:
            try:
                minio_path = f"bronze/openstax/json/{filename}"
                self.minio_client.fput_object(self.minio_bucket, minio_path, str(output_file))
                print(f"[MinIO] Uploaded: {minio_path}")
                return minio_path
            except Exception as e:
                print(f"[MinIO] Upload failed: {e}")
        
        return str(output_file)
    
    def _load_existing_book_ids(self) -> Set[str]:
        """Load existing book IDs from MinIO for deduplication"""
        existing = set()
        
        if not self.minio_client:
            return existing
        
        try:
            objects = self.minio_client.list_objects(
                self.minio_bucket,
                prefix='bronze/openstax/json/',
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
                            book_id = record.get('id')
                            if not book_id and record.get('url'):
                                book_id = self._create_document_id(record['url'])
                            if book_id:
                                existing.add(book_id)
                                
                except Exception as e:
                    print(f"[OpenStax] Skipping {obj.object_name}: {e}")
                    
        except Exception as e:
            print(f"[OpenStax] Could not scan MinIO: {e}")
        
        return existing


    def run(self):
        """Main execution - for DAG compatibility"""
        documents = self.scrape()
        if documents:
            self.save_to_minio(documents)
        return documents


# Legacy class name for DAG compatibility
OpenStaxScraperStandalone = OpenStaxScraper


def run_openstax_scraper(**kwargs) -> List[Dict[str, Any]]:
    """Run OpenStax scraper - entry point for DAG"""
    scraper = OpenStaxScraper(**kwargs)
    try:
        return scraper.scrape()
    finally:
        scraper.cleanup()


if __name__ == "__main__":
    import sys
    
    max_docs = int(sys.argv[1]) if len(sys.argv) > 1 else None
    
    scraper = OpenStaxScraper(max_documents=max_docs)
    documents = scraper.scrape()
    
    if documents:
        scraper.save_to_minio(documents)
    
    print(f"Completed! Scraped {len(documents)} books.")
