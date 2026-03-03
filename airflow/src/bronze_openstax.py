#!/usr/bin/env python3
"""
OpenStax Scraper - Bronze Layer
Scrapes textbook metadata from OpenStax.org
"""

import os
import json
import time
import hashlib
import requests
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Set, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin

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
                 max_documents: int = None, output_dir: str = "/opt/airflow/scraped_data/openstax"):
        self.source = "openstax"
        self.delay = delay
        self.max_documents = max_documents
        self.output_dir = output_dir
        self.driver = None
        
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
        
        print(f"[OpenStax] Scraper initialized - Selenium: {self.use_selenium}, Max docs: {self.max_documents or 'ALL'}")
    
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
                    print(f"  ✓ {book_data['title'][:50]}...")
                
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
        """Scrape individual book metadata"""
        soup = self._get_page(url)
        if not soup:
            return None
        
        try:
            return {
                'id': self._create_document_id(url),
                'title': self._extract_title(soup, url),
                'description': self._extract_description(soup),
                'authors': self._extract_authors(soup),
                'subject': self._extract_subject(soup),
                'url': url,
                'source': self.source,
                'scraped_at': datetime.now().isoformat()
            }
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
