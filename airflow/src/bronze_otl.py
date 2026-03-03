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
                 parallel: bool = True, max_workers: int = 4,
                 output_dir: str = "/opt/airflow/scraped_data/otl"):
        self.source = "otl"
        self.delay = delay
        self.max_documents = max_documents or int(os.getenv('MAX_DOCUMENTS', '999999'))
        self.parallel = parallel
        self.max_workers = max_workers
        self.output_dir = output_dir
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
        """Setup Selenium WebDriver"""
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
            try:
                driver.quit()
            except:
                pass
        
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
            
            return {
                'id': self._create_document_id(url),
                'title': title,
                'description': self._extract_description(soup),
                'authors': self._extract_authors(soup),
                'subject': [subject_name],
                'url': url,
                'source': 'Open Textbook Library',
                'scraped_at': datetime.now().isoformat()
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
