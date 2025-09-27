#!/usr/bin/env python3
"""
Open Textbook Library Scraper - Bronze Layer
=============================================

Standalone script to scrape Open Textbook Library and store to MinIO bronze layer.
Based on building-lakehouse pattern with advanced Selenium support.
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

# Selenium imports for live mode
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

# Throttling/backoff configuration via ENV (safe defaults)
DEFAULT_DELAY_BASE_SEC = float(os.getenv('OTL_DELAY_BASE_SEC', '0.8'))
DEFAULT_DELAY_JITTER_SEC = float(os.getenv('OTL_DELAY_JITTER_SEC', '0.4'))
RETRY_BACKOFF_BASE_SEC = float(os.getenv('OTL_RETRY_BACKOFF_BASE_SEC', '2.0'))
RETRY_BACKOFF_MAX_SEC = float(os.getenv('OTL_RETRY_BACKOFF_MAX_SEC', '10.0'))
WAIT_UNTIL_CLEAR = (os.getenv('OTL_WAIT_UNTIL_CLEAR', 'false').lower() in ['1','true','yes'])
WAIT_MAX_MINUTES = int(os.getenv('OTL_WAIT_MAX_MINUTES', '15'))
PER_BOOK_DELAY_SEC = float(os.getenv('OTL_PER_BOOK_DELAY_SEC', '0.6'))
SUBJECT_COOLDOWN_SEC = float(os.getenv('OTL_SUBJECT_COOLDOWN_SEC', '3.0'))
FORCE_RANDOM_UA = (os.getenv('OTL_FORCE_RANDOM_UA', 'true').lower() in ['1','true','yes'])
CUSTOM_UA = os.getenv('OTL_USER_AGENT', '').strip()
PROXY_SERVER = os.getenv('OTL_PROXY', '').strip()
BOOK_MAX_ATTEMPTS = int(os.getenv('OTL_BOOK_MAX_ATTEMPTS', '4'))
RETRY_PDF_ON_MISS = (os.getenv('OTL_RETRY_PDF_ON_MISS', 'true').lower() in ['1','true','yes'])

# Helper functions
def _sleep(base_sec: float = DEFAULT_DELAY_BASE_SEC, jitter_sec: float = DEFAULT_DELAY_JITTER_SEC) -> None:
    delay = max(0.0, base_sec) + max(0.0, jitter_sec) * random.random()
    time.sleep(delay)

def _backoff_sleep(attempt_index: int) -> None:
    delay = RETRY_BACKOFF_BASE_SEC * (2 ** max(0, attempt_index - 1))
    time.sleep(min(delay, RETRY_BACKOFF_MAX_SEC))

def _with_cache_bust(url: str) -> str:
    if not url:
        return url
    ts = str(int(time.time() * 1000))
    return f"{url}&ts={ts}" if ('?' in url) else f"{url}?ts={ts}"

def _pick_user_agent() -> str:
    if CUSTOM_UA:
        return CUSTOM_UA
    ua_pool = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.127 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.127 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    ]
    return random.choice(ua_pool)

def _md5_id(url: str, title: str) -> str:
    return hashlib.md5(f"{url}_{title}".encode("utf-8")).hexdigest()

def _ensure_not_retry_later(driver) -> BeautifulSoup:
    """Ensure current page is not a 'Retry later' placeholder"""
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    text = soup.get_text()
    if 'Retry later' not in text:
        return soup
    
    if WAIT_UNTIL_CLEAR:
        print("[OTL] [Wait] 'Retry later' detected — waiting until it clears...")
        start_ts = time.time()
        attempt = 1
        while True: 
            if time.time() - start_ts > WAIT_MAX_MINUTES * 60:
                print("[OTL] [Wait] Max wait time reached; proceeding.")
                return BeautifulSoup(driver.page_source, 'html.parser')
            _backoff_sleep(attempt)
            attempt += 1
            try:
                driver.refresh()
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            except Exception:
                pass
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            if 'Retry later' not in soup.get_text():
                print("[OTL] [Wait] Cleared; continue.")
                return soup
    else:
        print("[OTL] [Leaf] Hit 'Retry later', applying backoff...")
        for attempt in range(1, 4):
            _backoff_sleep(attempt)
            try:
                driver.refresh()
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            except Exception:
                pass
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            if 'Retry later' not in soup.get_text():
                break
        return soup

class OTLScraperStandalone:
    """Standalone Open Textbook Library scraper for bronze layer"""
    
    def __init__(self):
        self.base_url = BASE_URL + "/opentextbooks"
        self.source = "otl"
        self.delay = DEFAULT_DELAY_BASE_SEC
        self.max_books = int(os.getenv('MAX_DOCUMENTS', 50))
        self.use_selenium = SELENIUM_AVAILABLE
        
        # MinIO setup
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        self.minio_client = self._setup_minio() if MINIO_AVAILABLE else None
        
        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': _pick_user_agent()
        })
        
        print(f"OTL Scraper initialized - Max books: {self.max_books}, Selenium: {self.use_selenium}")
    
    def _setup_minio(self):
        """Setup MinIO client"""
        try:
            endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
            access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
            secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
            secure = os.getenv('MINIO_SECURE', '0') == '1'
            
            client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
            
            # Ensure bucket exists
            if not client.bucket_exists(self.bucket):
                client.make_bucket(self.bucket)
                print(f"Created bucket: {self.bucket}")
            
            return client
        except Exception as e:
            print(f"MinIO setup failed: {e}")
            return None
    
    def _init_driver(self, headless: bool = True):
        """Initialize Selenium Chrome driver with anti-detection features"""
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

            ua = _pick_user_agent() if FORCE_RANDOM_UA or CUSTOM_UA else None
            if ua:
                options.add_argument(f"--user-agent={ua}")

            if PROXY_SERVER:
                options.add_argument(f"--proxy-server={PROXY_SERVER}")
            
            try:
                prefs = {"profile.managed_default_content_settings.images": 2}
                options.add_experimental_option("prefs", prefs)
            except Exception:
                pass
            
            # Try ChromeDriver paths
            chromedriver_paths = ['/usr/local/bin/chromedriver', '/usr/bin/chromedriver', 'chromedriver']
            
            driver = None
            for path in chromedriver_paths:
                try:
                    if os.path.exists(path) or path == 'chromedriver':
                        service = Service(path)
                        driver = webdriver.Chrome(service=service, options=options)
                        print(f"Selenium ready with: {path}")
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
        """Parse a book detail HTML into the required output schema"""
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

        # Authors
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
        
        # Heuristic: authors appear as plain <p> lines inside #info above metadata fields
        if not authors:
            info_div = soup.select_one('#info')
            if info_div:
                started_block = False
                for p in info_div.find_all('p'):
                    txt = (p.get_text(' ', strip=True) or '')
                    if not txt:
                        continue
                    ltxt = txt.lower()
                    # Skip reviews and star rows
                    if 'review' in ltxt:
                        continue
                    # If this looks like a metadata field, stop after we started authors block
                    meta_prefixes = (
                        'copyright year', 'publisher', 'language', 'isbn', 'license',
                        'format', 'pages', 'doi', 'edition'
                    )
                    if any(ltxt.startswith(pref) for pref in meta_prefixes) or ':' in txt:
                        if started_block:
                            break
                        else:
                            continue
                    # Treat as author line(s)
                    started_block = True
                    parts = [re.sub(r'\s+', ' ', x).strip() for x in re.split(r',| and ', txt) if x.strip()]
                    for a in parts:
                        if a and a not in authors:
                            authors.append(a)

        # Extract PDF URL if present
        pdf_url = ''
        for a in soup.select('#book-types a[href]'):
            label = (a.get_text(strip=True) or '').lower()
            if 'pdf' in label:
                pdf_url = urljoin(BASE_URL, a['href'])
                break

        doc = {
            'id': _md5_id(book_url, title or book_url),
            'title': title,
            'description': description,
            'authors': authors,
            'subject': subjects or [],
            'source': 'Open Textbook Library',
            'url': book_url,
            'url_pdf': pdf_url or '',
            'scraped_at': datetime.now().isoformat()
        }
        return doc
    
    def get_textbooks_list_selenium(self, subject_name: str = "All", subject_url: str = None) -> List[Dict[str, Any]]:
        """Get list of textbooks using Selenium with advanced features"""
        results = []
        driver = self._init_driver(headless=True)
        
        if not driver:
            print("Selenium not available, falling back to requests method")
            return self.get_textbooks_list_fallback()
        
        try:
            if not subject_url:
                subject_url = f"{self.base_url}/subjects"
            
            print(f"[OTL] Starting scrape for subject: {subject_name}")
            
            # Collect listing URLs with pagination first, fallback to scroll
            def _with_scroll(u: str) -> str:
                if 'scroll=true' in (u or ''):
                    return u
                return f"{u}&scroll=true" if ('?' in u) else f"{u}?scroll=true"

            current_url = _with_scroll(subject_url)
            urls: List[str] = []
            visited_pages: set[str] = set()
            page_no = 1
            
            while len(urls) < self.max_books:
                driver.get(current_url)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                soup = _ensure_not_retry_later(driver)
                
                # Find textbook URLs
                for h2 in soup.select('div.row.short-description h2 > a[href]'):
                    href = (h2.get('href') or '').strip()
                    if not href:
                        continue
                    full = urljoin(BASE_URL, href)
                    if '/opentextbooks/textbooks/' in full and full not in urls:
                        urls.append(full)
                        if len(urls) >= self.max_books:
                            break
                
                print(f"[OTL] Page {page_no} listing count so far: {len(urls)}")
                
                # Check for next page
                next_a = soup.select_one('#pagination a[rel="next"][href]')
                if not next_a or len(urls) >= self.max_books:
                    break
                    
                next_url = _with_scroll(urljoin(BASE_URL, next_a.get('href') or ''))
                next_url = _with_cache_bust(next_url)
                if not next_url or next_url in visited_pages:
                    break
                    
                visited_pages.add(next_url)
                page_no += 1
                current_url = next_url
                _sleep()

            # Fallback to scroll if pagination failed
            if not urls:
                print("[OTL] Pagination failed, using scroll fallback")
                last_count = 0
                stable_rounds = 0
                while len(urls) < self.max_books:
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    for h2 in soup.select('div.row.short-description h2 > a[href]'):
                        href = (h2.get('href') or '').strip()
                        if not href:
                            continue
                        full = urljoin(BASE_URL, href)
                        if '/opentextbooks/textbooks/' in full and full not in urls:
                            urls.append(full)
                            if len(urls) >= self.max_books:
                                break
                    
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                    _sleep(0.4, 0.4)
                    
                    if len(urls) == last_count:
                        stable_rounds += 1
                    else:
                        stable_rounds = 0
                        last_count = len(urls)
                    
                    if stable_rounds >= 3:
                        break

            print(f"[OTL] Found {len(urls)} book URLs for subject '{subject_name}'")

            # Visit each book and parse
            for idx, url in enumerate(urls, start=1):
                if len(results) >= self.max_books:
                    break
                    
                print(f"[OTL] Scraping book {idx}/{len(urls)}: {url}")
                
                # Per-book delay and cache-busting
                if PER_BOOK_DELAY_SEC > 0:
                    time.sleep(PER_BOOK_DELAY_SEC + DEFAULT_DELAY_JITTER_SEC * random.random())
                
                attempts = 0
                doc = None
                while attempts < max(1, BOOK_MAX_ATTEMPTS):
                    attempts += 1
                    driver.get(_with_cache_bust(url))
                    try:
                        WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                    except Exception:
                        pass
                    
                    # Ensure not stuck on retry page
                    soup = _ensure_not_retry_later(driver)
                    
                    # Give the page a moment for dynamic blocks
                    _sleep(0.4, 0.4)
                    
                    # Scroll to trigger lazy loads
                    try:
                        driver.execute_script("var el=document.getElementById('Formats'); if(el){el.scrollIntoView({behavior:'instant',block:'center'});} else {window.scrollTo(0, document.body.scrollHeight);} ")
                        WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                    except Exception:
                        pass
                    
                    html = driver.page_source
                    doc_try = self.parse_book_detail_html(html, url, [subject_name])
                    missing_title = not (doc_try.get('title') or '').strip()
                    missing_pdf = not bool(doc_try.get('url_pdf'))
                    
                    if not missing_title and (not RETRY_PDF_ON_MISS or not missing_pdf):
                        doc = doc_try
                        break
                    
                    # If missing title or pdf, backoff and retry
                    _backoff_sleep(attempts)
                
                if not doc:
                    doc = self.parse_book_detail_html(driver.page_source, url, [subject_name])
                
                if doc:
                    results.append(doc)
                    has_pdf = 'url_pdf' in doc and bool(doc['url_pdf'])
                    print(f"[OTL] Parsed book {idx}/{len(urls)} | title='{doc.get('title','')}' | pdf={has_pdf}")

            print(f"[OTL] Selenium scraping completed: {len(results)} textbooks")
            return results
            
        except Exception as e:
            print(f"[OTL] Error in Selenium scraping: {e}")
            return results
        finally:
            try:
                driver.quit()
            except Exception:
                pass
    
    def get_textbooks_list_fallback(self) -> List[Dict[str, Any]]:
        """Fallback method using requests"""
        try:
            print("Fetching OTL textbooks list with requests fallback...")
            
            textbooks = []
            page = 1
            
            while len(textbooks) < self.max_books:
                browse_url = f"{self.base_url}/browse"
                if page > 1:
                    browse_url += f"?page={page}"
                
                print(f"Fetching page {page}...")
                response = self.session.get(browse_url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find textbook cards/items
                book_elements = soup.find_all('div', class_='textbook-item') or \
                               soup.find_all('article', class_='textbook') or \
                               soup.find_all('a', href=lambda x: x and '/textbook/' in str(x))
                
                if not book_elements:
                    print("No more textbooks found")
                    break
                
                page_books = 0
                for elem in book_elements:
                    if len(textbooks) >= self.max_books:
                        break
                    
                    try:
                        # Extract basic info
                        title_elem = elem.find('h3') or elem.find('h2') or elem.find('a')
                        link_elem = elem.find('a') or elem
                        
                        if not title_elem or not link_elem:
                            continue
                        
                        title = title_elem.get_text().strip()
                        href = link_elem.get('href', '')
                        
                        if not href or not title:
                            continue
                        
                        # Build full URL
                        if href.startswith('/'):
                            full_url = f"https://open.umn.edu{href}"
                        elif not href.startswith('http'):
                            full_url = urljoin(self.base_url, href)
                        else:
                            full_url = href
                        
                        textbook = {
                            'title': title,
                            'url': full_url,
                            'source': self.source,
                            'found_at': datetime.now().isoformat()
                        }
                        
                        textbooks.append(textbook)
                        page_books += 1
                        
                    except Exception as e:
                        print(f"Error extracting textbook: {e}")
                        continue
                
                if page_books == 0:
                    break
                
                page += 1
                time.sleep(self.delay)
            
            print(f"Found {len(textbooks)} textbooks")
            return textbooks
            
        except Exception as e:
            print(f"Error fetching textbooks list: {e}")
            return []
    
    def save_to_minio(self, documents: List[Dict[str, Any]], source: str = "otl", logical_date: str = None, file_type: str = "textbooks"):
        """Save data to MinIO with organized path structure"""
        if not self.minio_client or not documents:
            print("MinIO not available or no documents to save")
            return ""
        
        if logical_date is None:
            logical_date = datetime.now().strftime("%Y-%m-%d")
        
        # Create organized path for OTL
        timestamp = int(time.time())
        object_name = f"bronze/{source}/{logical_date}/{file_type}_{timestamp}.jsonl"
        
        # Create temporary file
        os.makedirs('/tmp', exist_ok=True) if os.name != 'nt' else os.makedirs('temp', exist_ok=True)
        tmp_dir = '/tmp' if os.name != 'nt' else 'temp'
        tmp_path = os.path.join(tmp_dir, f"{source}_{file_type}_{timestamp}.jsonl")
        
        try:
            # Write data to temp file
            with open(tmp_path, 'w', encoding='utf-8') as f:
                for doc in documents:
                    f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            
            # Upload to MinIO with organized path
            self.minio_client.fput_object(self.bucket, object_name, tmp_path)
            os.remove(tmp_path)
            
            print(f"[MinIO] OTL saved: s3://{self.bucket}/{object_name}")
            print(f"[MinIO] Total {len(documents)} textbooks saved")
            
            return object_name
            
        except Exception as e:
            print(f"[MinIO] Error saving to MinIO: {e}")
            # Still try to remove temp file
            try:
                os.remove(tmp_path)
            except:
                pass
            return ""
    
    def run(self):
        """Main execution function for bronze layer scraping"""
        print("Starting OTL bronze layer scraping...")
        
        start_time = time.time()
        documents = []
        
        try:
            # Use Selenium scraping if available, otherwise fallback
            if self.use_selenium:
                documents = self.get_textbooks_list_selenium("All")
            else:
                documents = self.get_textbooks_list_fallback()
            
            # Save to MinIO bronze layer
            if documents:
                print("Saving to bronze layer...")
                self.save_to_minio(documents, "otl", datetime.now().strftime("%Y-%m-%d"))
            
        except KeyboardInterrupt:
            print("\n=== INTERRUPTED BY USER ===")
            # Save current data to MinIO
            if documents:
                print(f"Saving emergency backup: {len(documents)} textbooks scraped")
                self.save_to_minio(documents, "otl", datetime.now().strftime("%Y-%m-%d"), "emergency")
            print("Data saved before exit.")
            raise
            
        except Exception as e:
            print(f"\n=== CRITICAL ERROR: {e} ===")
            # Save current data to MinIO
            if documents:
                print(f"Saving emergency backup: {len(documents)} textbooks scraped")
                self.save_to_minio(documents, "otl", datetime.now().strftime("%Y-%m-%d"), "error")
            print("Data saved before error report.")
            raise
        
        # Statistics
        end_time = time.time()
        print(f"\nSTATISTICS")
        print("=" * 30)
        print(f"Time: {end_time - start_time:.1f} seconds")
        print(f"OTL: {len(documents)} textbooks")
        print(f"Bronze layer scraping completed!")

def main():
    """Entry point for standalone execution"""
    scraper = OTLScraperStandalone()
    scraper.run()

if __name__ == "__main__":
    main()

