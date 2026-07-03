#!/usr/bin/env python3
"""
Open Textbook Library Scraper - Bronze Layer
Scrapes textbook metadata AND PDF resources from open.umn.edu/opentextbooks

PDF Storage structure in MinIO:
  bronze/otl/pdfs/{book-slug}/textbook/Book_Title.pdf
  bronze/otl/pdfs/{book-slug}/ancillary/Instructor_Guide.pdf
  bronze/otl/pdfs/{book-slug}/ancillary/Student_Solutions.pdf
"""

import os
import json
import time
import hashlib
import requests
import re
import io
import warnings
import threading
from datetime import datetime
from typing import List, Dict, Any, Set, Optional, Tuple
from urllib.parse import urljoin, urlparse
from pathlib import Path
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Suppress SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    """Open Textbook Library scraper for bronze layer - textbook metadata + PDFs"""
    
    BASE_URL = "https://open.umn.edu"
    
    # PDF type classification keywords → (folder_name, priority)
    # priority: lower = more valuable for chatbot (textbook = 0, ancillary = 1)
    _PDF_TYPE_RULES: List[Tuple[List[str], str, int]] = [
        (['textbook', 'book', 'full text', 'complete', 'main text'], 'textbook', 0),
        (['instructor', 'teaching', 'teacher', 'faculty'], 'ancillary', 1),
        (['student', 'solution', 'answer key', 'answers'], 'ancillary', 2),
        (['guide', 'manual', 'supplement', 'workbook'], 'ancillary', 3),
        (['slide', 'presentation', 'powerpoint', 'ppt'], 'ancillary', 4),
        (['test bank', 'quiz', 'exam', 'assessment'], 'ancillary', 5),
    ]
    
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
    
    def __init__(self, delay: float = 1.5, max_documents: int = None,
                 parallel: bool = False, max_workers: int = 2,
                 output_dir: str = "/opt/airflow/scraped_data/otl",
                 download_pdfs: bool = True, max_pdfs_per_book: int = 20,
                 pdf_types: Optional[List[str]] = None,
                 batch_size: int = 50,
                 subjects_filter: Optional[List[str]] = None,
                 **kwargs):
        """
        Args:
            delay: Delay giữa các requests (giây) - default 1.5s
            max_documents: Giới hạn tổng số books cào
            parallel: False = sequential (ổn định hơn), True = parallel
            max_workers: Số workers cho parallel mode (default 2)
            batch_size: Số books tối đa mỗi subject (default 50)
            subjects_filter: List tên subjects cụ thể để cào (None = all)
                           VD: ['Business', 'Computer Science', 'Mathematics']
            pdf_types: ['textbook'] hoặc ['textbook', 'ancillary']
        """
        # Default: lấy textbook chính cho chatbot Q&A
        if pdf_types is None:
            pdf_types = ['textbook']
        
        self.source = "otl"
        self.delay = delay
        self.max_documents = max_documents or int(os.getenv('MAX_DOCUMENTS', '999999'))
        self.parallel = parallel
        self.max_workers = max_workers
        self.output_dir = output_dir
        self.driver = None
        self.batch_size = batch_size
        self.subjects_filter = subjects_filter
        
        # PDF config
        self.download_pdfs = download_pdfs
        self.max_pdfs_per_book = max_pdfs_per_book
        # Which types to keep: None = all; e.g. ['textbook', 'ancillary']
        self.pdf_types_filter: Optional[Set[str]] = set(pdf_types) if pdf_types else None
        
        # HTTP Session with retry strategy
        self.session = requests.Session()

        # Setup retry strategy for better stability
        retry_strategy = Retry(
            total=3,  # Increased from 2 to 3 retries
            backoff_factor=2,  # Increased from 1 to 2 (2, 4, 8 seconds)
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            raise_on_status=False  # Don't raise on retry errors
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy, 
            pool_connections=10,  # Increased from 5
            pool_maxsize=20,  # Increased from 10
            pool_block=False
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.session.headers.update({
            'User-Agent': 'OER-Educational-Bot/2.0 (Research; +https://github.com/oer-lakehouse)'
        })
        
        # Setup
        self._setup_minio()
        
        # Load existing books for deduplication
        self.existing_book_ids = self._load_existing_book_ids()
        if self.existing_book_ids:
            print(f"[OTL] Loaded {len(self.existing_book_ids)} existing books")
        
        # Get subjects and apply filter
        all_subjects = self._get_subjects()
        if self.subjects_filter:
            self.subjects = [s for s in all_subjects if s['name'] in self.subjects_filter]
            print(f"[OTL] Filtered to {len(self.subjects)} subjects: {[s['name'] for s in self.subjects]}")
        else:
            self.subjects = all_subjects

        # Stats
        self.total_scraped = 0
        
        mode = "PARALLEL" if parallel else "SEQUENTIAL"
        print(f"[OTL] Scraper initialized - {mode} mode, Max docs: {self.max_documents}, "
              f"Batch size: {self.batch_size} books/subject, "
              f"Download PDFs: {self.download_pdfs}, "
              f"PDF types: {list(self.pdf_types_filter) if self.pdf_types_filter else 'all'}")
        print(f"[OTL] Subjects to scrape: {len(self.subjects)}")
    
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
        """Setup Selenium WebDriver with optimized timeouts"""
        if not SELENIUM_AVAILABLE:
            return None
        
        try:
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--blink-settings=imagesEnabled=false")
            
            # Add page load strategy for better timeout handling
            options.page_load_strategy = 'normal'
            
            for path in ['/usr/local/bin/chromedriver', '/usr/bin/chromedriver', 'chromedriver']:
                try:
                    if os.path.exists(path) or path == 'chromedriver':
                        service = Service(path)
                        driver = webdriver.Chrome(service=service, options=options)
                        
                        # Set explicit timeouts (increased from 15s to 30s)
                        driver.set_page_load_timeout(30)
                        driver.set_script_timeout(30)
                        driver.implicitly_wait(10)
                        
                        return driver
                except Exception:
                    continue
            
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
            driver.implicitly_wait(10)
            return driver
            
        except Exception as e:
            print(f"[Selenium] Setup failed: {e}")
            return None
    
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
            
            driver.quit()
            
            if subjects:
                print(f"[OTL] Found {len(subjects)} subjects dynamically")
                return subjects
                
        except Exception as e:
            print(f"[OTL] Error fetching subjects: {e}")
            if driver:
                driver.quit()
        
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
        """Scrape books from a specific subject page.
        
        Each subject will scrape its own books from the subject URL.
        Limited by batch_size parameter.
        """
        documents = []

        driver = self._setup_selenium()

        if not driver:
            print(f"[{subject['name']}] Selenium not available")
            return documents

        try:
            # Get book URLs for this specific subject
            book_urls = self._get_book_urls_from_subject_page(driver, subject)
            
            # Apply batch size limit
            if len(book_urls) > self.batch_size:
                print(f"[{subject['name']}] Found {len(book_urls)} books, limiting to {self.batch_size}")
                book_urls = book_urls[:self.batch_size]
            else:
                print(f"[{subject['name']}] Found {len(book_urls)} books")

            # Scrape each book
            skipped_metadata = 0
            skipped_pdfs = 0
            scraped_new = 0
            scraped_pdfs_only = 0

            for idx, url in enumerate(book_urls, 1):
                # Check global limit
                if self.total_scraped >= self.max_documents:
                    print(f"[{subject['name']}] Reached max limit ({self.max_documents})")
                    break

                book_id = self._create_document_id(url)
                book_slug = self._extract_book_slug(url)

                # Check if we should skip this book
                has_metadata = book_id in self.existing_book_ids
                has_pdfs = False

                if has_metadata and self.download_pdfs and self.minio_client:
                    # Check if PDFs already exist
                    try:
                        existing_prefix = f"bronze/otl/pdfs/{book_slug}/"
                        existing = list(self.minio_client.list_objects(
                            self.minio_bucket, prefix=existing_prefix, recursive=True
                        ))
                        has_pdfs = len(existing) > 0
                    except:
                        has_pdfs = False

                # Skip only if BOTH metadata AND PDFs exist
                if has_metadata and (not self.download_pdfs or has_pdfs):
                    skipped_metadata += 1
                    continue

                # Scrape book (either new or needs PDF update)
                if has_metadata and not has_pdfs:
                    # Has metadata but missing PDFs - scrape for PDFs only
                    book_data = self._scrape_book(driver, url, subject['name'])
                    if book_data and book_data.get('pdf_count', 0) > 0:
                        scraped_pdfs_only += 1
                        documents.append(book_data)
                else:
                    # New book - scrape everything
                    book_data = self._scrape_book(driver, url, subject['name'])
                    if book_data:
                        scraped_new += 1
                        documents.append(book_data)
                        self.existing_book_ids.add(book_data['id'])
                        self.total_scraped += 1

                # Progress indicator
                if idx % 10 == 0 or idx == len(book_urls):
                    print(f"[{subject['name']}] Progress: {idx}/{len(book_urls)} | "
                          f"New: {scraped_new} | PDFs added: {scraped_pdfs_only} | "
                          f"Skipped: {skipped_metadata}")

                time.sleep(self.delay)

            if scraped_new > 0 or scraped_pdfs_only > 0:
                print(f"[{subject['name']}] Summary: New books: {scraped_new} | "
                      f"PDFs added: {scraped_pdfs_only} | Skipped (complete): {skipped_metadata}")

        except Exception as e:
            print(f"[{subject['name']}] Error: {e}")
        finally:
            try:
                driver.quit()
            except:
                pass

        return documents
    
    # =========================================================================
    # URL EXTRACTION
    # =========================================================================

    def _get_book_urls_from_subject_page(self, driver, subject: Dict[str, str]) -> List[str]:
        """Get book URLs from a specific subject page by scrolling and loading all books.
        
        Each subject page has its own list of books that dynamically loads on scroll.
        Uses improved scroll detection to load ALL books.
        """
        book_urls = []
        seen_urls = set()
        
        try:
            # Navigate to subject page
            print(f"[{subject['name']}] Loading subject page...")
            driver.get(subject['url'])
            
            # Wait for page to fully load with book cards
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/opentextbooks/textbooks/"]'))
                )
                print(f"[{subject['name']}] ✓ Page loaded, found book cards")
            except:
                print(f"[{subject['name']}] ⚠ No book cards detected on initial load")
            
            time.sleep(4)  # Initial page load - increased from 3s to 4s
            
            # Scroll aggressively to load all books
            print(f"[{subject['name']}] Scrolling to load all books...")
            self._scroll_to_load_all_books(driver)
            
            # Parse page and extract book links
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Find all book links on this subject page
            # Pattern: /opentextbooks/textbooks/123 or /opentextbooks/textbooks/book-slug
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                
                # Check if this is a book detail link
                if '/opentextbooks/textbooks/' in href and 'submit' not in href:
                    full_url = urljoin(self.BASE_URL, href)
                    
                    # Clean URL (remove query params)
                    parsed = urlparse(full_url)
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    
                    if clean_url not in seen_urls:
                        book_urls.append(clean_url)
                        seen_urls.add(clean_url)
            
            print(f"[{subject['name']}] ✓ Found {len(book_urls)} total books")
            
        except Exception as e:
            print(f"[{subject['name']}] ✗ Error loading books: {e}")
        
        return book_urls
    
    def _scroll_to_load_all_books(self, driver):
        """Aggressive scrolling to load ALL books on subject page.
        
        OTL uses infinite scroll - need to scroll many times and wait for content.
        Uses improved strategy: scroll slower, wait for DOM changes, check book count.
        """
        scroll_count = 0
        max_scrolls = 200  # Increased limit for pages with many books
        no_change_count = 0
        last_book_count = 0
        
        while scroll_count < max_scrolls:
            # Get current book count before scrolling
            current_book_count = driver.execute_script("""
                return document.querySelectorAll('a[href*="/opentextbooks/textbooks/"]').length;
            """)
            
            # Scroll to bottom in multiple steps for smoother loading
            driver.execute_script("""
                window.scrollTo({
                    top: document.body.scrollHeight,
                    behavior: 'smooth'
                });
            """)
            scroll_count += 1
            
            # Wait longer for lazy loading to trigger (critical for OTL)
            time.sleep(3.5)  # Increased from 2.5s to 3.5s
            
            # Wait for any loading indicators to disappear
            try:
                WebDriverWait(driver, 3).until_not(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".loading, .spinner, [class*='loading']"))
                )
            except:
                pass  # No loading indicator found
            
            # Additional wait for new DOM elements to render
            time.sleep(1.5)
            
            # Get new book count after scroll
            new_book_count = driver.execute_script("""
                return document.querySelectorAll('a[href*="/opentextbooks/textbooks/"]').length;
            """)
            
            # Check if new books were loaded
            if new_book_count == last_book_count:
                no_change_count += 1
                # Give it 7 tries before stopping (was 5)
                if no_change_count >= 7:
                    print(f"    → Stopped after {scroll_count} scrolls ({new_book_count} books found, no new content)")
                    break
            else:
                # New content detected!
                no_change_count = 0
                if scroll_count % 5 == 0:  # Report more frequently
                    print(f"    → Scrolled {scroll_count}x | Books loaded: {new_book_count}")
            
            last_book_count = new_book_count
            
            # Early exit if we have a lot of books already
            if new_book_count > 500:
                print(f"    → Loaded {new_book_count} books, stopping scroll")
                break
        
        if scroll_count >= max_scrolls:
            print(f"    → Reached max scrolls ({max_scrolls}) with {last_book_count} books")
        
        # Final wait for any pending renders
        time.sleep(3)

    def _fetch_all_book_urls_from_atom(self) -> List[str]:
        """Fetch all book URLs from Atom feed (legacy method, not used anymore)"""
        all_urls = []
        page = 1
        max_pages = 200  # Safety limit

        while page <= max_pages:
            try:
                feed_url = f"{self.BASE_URL}/opentextbooks/textbooks?page={page}"
                resp = self.session.get(feed_url, timeout=15)

                if resp.status_code != 200:
                    print(f"  Page {page}: status {resp.status_code}, stopping")
                    break

                # Parse Atom feed (XML)
                soup = BeautifulSoup(resp.text, 'xml')
                entries = soup.find_all('entry')

                if not entries:
                    break

                # Extract book URLs
                found = 0
                for entry in entries:
                    link_elem = entry.find('link', rel='alternate')
                    if link_elem:
                        href = link_elem.get('href', '')
                        if '/opentextbooks/textbooks/' in href and 'submit' not in href:
                            full_url = urljoin(self.BASE_URL, href)
                            if full_url not in all_urls:
                                all_urls.append(full_url)
                                found += 1

                # Check for next page
                has_next = bool(soup.find('link', rel='next'))
                if not found or not has_next:
                    break

                page += 1

                # Progress indicator every 50 pages
                if page % 50 == 0:
                    print(f"  ... fetched {len(all_urls)} books so far")

            except Exception as e:
                print(f"  Error on page {page}: {e}")
                break

        return all_urls

    def _get_book_urls(self, driver, subject: Dict[str, str]) -> List[str]:
        """Get book URLs for a subject (delegates to new method)"""
        return self._get_book_urls_from_subject_page(driver, subject)
    
    def _scroll_to_bottom(self, driver, max_scrolls: int = 30):
        """Scroll page to load all dynamically loaded content (legacy method).
        
        OTL subject pages load books dynamically on scroll.
        """
        last_height = driver.execute_script("return document.body.scrollHeight")
        no_change_count = 0
        
        for i in range(max_scrolls):
            # Scroll down
            driver.execute_script("window.scrollBy(0, 1500);")
            time.sleep(1.5)  # Wait for content to load
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                no_change_count += 1
                if no_change_count >= 3:
                    # No new content after 3 attempts
                    break
            else:
                no_change_count = 0
            
            last_height = new_height
    
    # =========================================================================
    # BOOK SCRAPING
    # =========================================================================
    
    def _scrape_book(self, driver, url: str, subject_name: str) -> Optional[Dict[str, Any]]:
        """Scrape individual book metadata + download PDFs"""
        try:
            driver.get(url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(1.5)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            title = self._extract_title(soup)
            
            book_data = {
                'id': self._create_document_id(url),
                'title': title,
                'description': self._extract_description(soup),
                'authors': self._extract_authors(soup),
                'subject': [subject_name],
                'url': url,
                'source': 'Open Textbook Library',
                'scraped_at': datetime.now().isoformat()
            }
            
            # Download PDFs after metadata extraction
            if self.download_pdfs and self.minio_client:
                book_slug = self._extract_book_slug(url)
                pdf_results = self._download_book_pdfs(soup, url, book_slug)
                book_data['pdf_count'] = pdf_results['downloaded']
                book_data['pdf_paths'] = pdf_results['paths']
                book_data['pdf_types_found'] = pdf_results['types_found']
            else:
                book_data['pdf_count'] = 0
                book_data['pdf_paths'] = []
                book_data['pdf_types_found'] = []
            
            return book_data
            
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
    # PDF DOWNLOAD
    # =========================================================================

    def _extract_book_slug(self, book_url: str) -> str:
        """Extract clean book slug from URL for folder naming.
        e.g. https://open.umn.edu/opentextbooks/textbooks/123
             → textbooks-123
        """
        path = urlparse(book_url).path.rstrip('/')
        # Extract the last parts of the path
        parts = [p for p in path.split('/') if p]
        if len(parts) >= 2:
            return f"{parts[-2]}-{parts[-1]}"
        elif len(parts) == 1:
            return parts[0]
        return 'unknown'

    def _classify_pdf(self, link_text: str, href: str) -> Tuple[str, int]:
        """Return (folder_name, priority) based on link text / filename."""
        combined = f"{link_text} {href}".lower()
        for keywords, folder, priority in self._PDF_TYPE_RULES:
            if any(kw in combined for kw in keywords):
                return folder, priority
        return 'ancillary', 99

    def _sanitize_filename(self, name: str) -> str:
        """Convert a human-readable label to a safe filename."""
        # Remove characters that are unsafe in filenames
        name = re.sub(r'[\\/:*?"<>|]', '', name)
        # Collapse whitespace → underscore
        name = re.sub(r'\s+', '_', name.strip())
        # Remove duplicate underscores
        name = re.sub(r'_+', '_', name)
        return name[:120]  # cap length

    def _collect_pdf_links(self, soup: BeautifulSoup, book_url: str) -> List[Dict[str, Any]]:
        """Collect all PDF links for a book from its page.

        OTL books typically have:
        - "View on the Web" or "Download PDF" links
        - Multiple formats (PDF, EPUB, etc.)
        - Ancillary resources section
        - Links to intermediate hosts (e.g., saylor.org, pressbooks)

        Returns list of dicts: {url, label, folder, priority}
        """
        seen_urls: Set[str] = set()
        pdf_entries: List[Dict[str, Any]] = []

        # Known intermediate hosts that may contain PDFs
        INTERMEDIATE_HOSTS = {
            'saylor.org', 'resources.saylor.org',
            'archive.org', 'wayback.archive.org',
            'libretexts.org', 'pressbooks', 'opentextbc.ca',
            'open.lib.umn.edu', 'openoregon', 'oer.hawaii.edu'
        }

        # Look for PDF download links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']

            # Skip obvious non-PDF links
            if any(x in href for x in ['javascript:', 'mailto:', '#', 'void(0)']):
                continue

            # Resolve to absolute URL
            abs_url = urljoin(self.BASE_URL, href)
            if abs_url in seen_urls:
                continue
            seen_urls.add(abs_url)

            # Extract label from link text or nearby text
            label = a_tag.get_text(strip=True)
            aria_label = a_tag.get('aria-label', '')
            title_attr = a_tag.get('title', '')

            # Try to get more context from parent elements
            if not label or len(label) < 3:
                parent = a_tag.find_parent(['li', 'div', 'p'])
                if parent:
                    label = parent.get_text(strip=True)

            # Clean label
            label = re.sub(r'\s*\(pdf\)\s*$', '', label, flags=re.I).strip()
            label = re.sub(r'\s*download\s*$', '', label, flags=re.I).strip()

            if not label:
                label = Path(urlparse(abs_url).path).stem or 'document'

            # Strategy 1: Direct .pdf links
            if abs_url.lower().endswith('.pdf'):
                folder, priority = self._classify_pdf(label, abs_url)
                pdf_entries.append({
                    'url': abs_url,
                    'label': label,
                    'folder': folder,
                    'priority': priority,
                })
                continue

            # Strategy 2: OTL format links (e.g., /opentextbooks/formats/123)
            parsed = urlparse(abs_url)
            is_otl_format = (parsed.netloc in ('open.umn.edu', '') and
                           '/opentextbooks/formats/' in parsed.path)

            # Check if link text/attributes mention PDF
            mentions_pdf = re.search(r'\bpdf\b', label + aria_label + title_attr, re.I)

            if is_otl_format or mentions_pdf:
                # Try to follow this link to find actual PDF
                discovered = self._follow_link_for_pdf(abs_url, label)
                for entry in discovered:
                    if entry['url'] not in {e['url'] for e in pdf_entries}:
                        pdf_entries.append(entry)
                continue

            # Strategy 3: Links to known intermediate hosts
            host = parsed.netloc.lower().lstrip('www.')
            if any(h in host for h in INTERMEDIATE_HOSTS):
                discovered = self._follow_link_for_pdf(abs_url, label)
                for entry in discovered:
                    if entry['url'] not in {e['url'] for e in pdf_entries}:
                        pdf_entries.append(entry)

        # Sort: textbook first, then ancillary materials
        pdf_entries.sort(key=lambda x: (x['priority'], x['label']))
        return pdf_entries

    def _follow_link_for_pdf(self, url: str, parent_label: str) -> List[Dict[str, Any]]:
        """Follow a link to discover PDF URLs (handles redirects and intermediate pages)."""
        results: List[Dict[str, Any]] = []

        try:
            # Increased timeout from 30s to 60s for slow external sites
            resp = self.session.get(url, timeout=60, allow_redirects=True, verify=False)

            # Check if redirected to PDF directly
            final_url = resp.url
            if final_url.lower().endswith('.pdf'):
                # Verify it's actually a PDF via magic bytes
                if self._is_pdf_content(resp.content[:1024]):
                    folder, priority = self._classify_pdf(parent_label, final_url)
                    results.append({
                        'url': final_url,
                        'label': parent_label,
                        'folder': folder,
                        'priority': priority
                    })
                    return results

            # Check content-type
            content_type = resp.headers.get('Content-Type', '').lower()
            if 'pdf' in content_type:
                if self._is_pdf_content(resp.content[:1024]):
                    folder, priority = self._classify_pdf(parent_label, final_url)
                    results.append({
                        'url': final_url,
                        'label': parent_label,
                        'folder': folder,
                        'priority': priority
                    })
                    return results

            # If HTML, parse for PDF links
            if 'html' in content_type or self._looks_like_html(resp.content[:500]):
                soup = BeautifulSoup(resp.text, 'html.parser')

                # Find all .pdf links
                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    if not href.lower().endswith('.pdf'):
                        continue

                    pdf_url = urljoin(final_url, href)
                    label = a_tag.get_text(strip=True) or parent_label
                    label = re.sub(r'\s*\(pdf\)\s*', '', label, flags=re.I).strip() or parent_label

                    folder, priority = self._classify_pdf(label, pdf_url)
                    results.append({
                        'url': pdf_url,
                        'label': label,
                        'folder': folder,
                        'priority': priority
                    })

        except Exception:
            # Silent fail - expected for many links
            pass

        return results

    def _is_pdf_content(self, data: bytes) -> bool:
        """Check if bytes content is actually a PDF via magic bytes."""
        if not data or len(data) < 5:
            return False
        return data[:4] == b'%PDF' or data[:5] == b'%PDF-'

    def _looks_like_html(self, data: bytes) -> bool:
        """Quick check if response is HTML."""
        if not data or len(data) < 10:
            return False
        start = data[:200].lower()
        return b'<!doctype' in start or b'<html' in start or b'<head' in start


    def _download_book_pdfs(self, soup: BeautifulSoup, book_url: str, book_slug: str) -> Dict[str, Any]:
        """Download all PDFs for a book and upload to MinIO.

        MinIO path structure:
          bronze/otl/pdfs/{book_slug}/{folder}/{label}.pdf

        Returns:
          {'downloaded': int, 'paths': [minio_path, ...], 'types_found': [folder, ...]}
        """
        result = {'downloaded': 0, 'paths': [], 'types_found': []}

        if not self.minio_client:
            return result

        # Check if PDFs already downloaded for this book
        existing_prefix = f"bronze/otl/pdfs/{book_slug}/"
        try:
            existing = list(self.minio_client.list_objects(
                self.minio_bucket, prefix=existing_prefix, recursive=True
            ))
            if existing:
                existing_paths = [o.object_name for o in existing]
                types_found = list({p.split('/')[4] for p in existing_paths
                                    if len(p.split('/')) > 4})
                print(f"  [PDF] Already have {len(existing_paths)} PDFs for {book_slug}, skipping")
                result['downloaded'] = len(existing_paths)
                result['paths'] = existing_paths
                result['types_found'] = types_found
                return result
        except Exception:
            pass

        # Collect PDF links from book page
        pdf_entries = self._collect_pdf_links(soup, book_url)

        if not pdf_entries:
            print(f"  [PDF] ⚠ No PDF links found for {book_slug}")
            return result

        print(f"  [PDF] Found {len(pdf_entries)} potential PDF links")

        # Apply type filter
        if self.pdf_types_filter:
            pdf_entries = [e for e in pdf_entries if e['folder'] in self.pdf_types_filter]

        # Limit per book
        pdf_entries = pdf_entries[:self.max_pdfs_per_book]

        # Track filename collisions per folder
        used_names: Dict[str, Set[str]] = {}
        types_seen: Set[str] = set()

        for entry in pdf_entries:
            try:
                pdf_url = entry['url']
                
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

                minio_path = f"bronze/otl/pdfs/{book_slug}/{folder}/{label}.pdf"

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
                    print(f"  [PDF] ✗ Failed to fetch or validate: {label[:40]}")
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

                size_mb = len(pdf_bytes) / (1024 * 1024)
                print(f"  [PDF] ✓ {folder}/{label}.pdf ({size_mb:.2f}MB)")

                time.sleep(0.5)

            except Exception as e:
                print(f"  [PDF] ✗ {entry.get('label', '?')}: {e}")

        result['types_found'] = list(types_seen)
        print(f"  [PDF] Book {book_slug}: {result['downloaded']} PDFs uploaded "
              f"({', '.join(types_seen) or 'none'})")
        return result

    def _fetch_pdf(self, url: str, max_retries: int = 3) -> Optional[bytes]:
        """Download PDF bytes with retry logic and magic bytes validation."""
        for attempt in range(max_retries):
            try:
                # Disable SSL verification to avoid certificate errors with external sites
                resp = self.session.get(url, timeout=60, stream=True, verify=False)
                resp.raise_for_status()

                # Download content
                content = resp.content

                # Verify it's actually a PDF using magic bytes
                if not self._is_pdf_content(content):
                    # Might be HTML error page
                    if self._looks_like_html(content):
                        # Silent skip - not a PDF
                        return None
                    # Or wrong content type - skip
                    return None

                # Valid PDF
                return content

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    # Only log on final failure
                    pass
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