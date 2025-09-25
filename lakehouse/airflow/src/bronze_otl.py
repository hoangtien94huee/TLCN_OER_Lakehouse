#!/usr/bin/env python3
"""
Open Textbook Library Scraper - Bronze Layer
=============================================

Standalone script to scrape Open Textbook Library and store to MinIO bronze layer.
Based on building-lakehouse pattern.
"""

import os
import json
import time
import hashlib
import requests
from datetime import datetime
from typing import List, Dict, Any
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# MinIO imports
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    print("Warning: MinIO library not found")

class OTLScraperStandalone:
    """Standalone Open Textbook Library scraper for bronze layer"""
    
    def __init__(self):
        self.base_url = "https://open.umn.edu/opentextbooks"
        self.source = "otl"
        self.delay = float(os.getenv('SCRAPING_DELAY_BASE', 1.5))
        self.max_books = int(os.getenv('MAX_DOCUMENTS', 50))
        
        # MinIO setup
        self.minio_client = self._setup_minio() if MINIO_AVAILABLE else None
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        
        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        print(f"🚀 OTL Scraper initialized - Max books: {self.max_books}")
    
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
                print(f"✅ Created bucket: {self.bucket}")
            
            return client
        except Exception as e:
            print(f"❌ MinIO setup failed: {e}")
            return None
    
    def get_textbooks_list(self) -> List[Dict[str, Any]]:
        """Get list of textbooks from OTL browse page"""
        try:
            print("🔍 Fetching OTL textbooks list...")
            
            textbooks = []
            page = 1
            
            while len(textbooks) < self.max_books:
                browse_url = f"{self.base_url}/browse"
                if page > 1:
                    browse_url += f"?page={page}"
                
                print(f"📄 Fetching page {page}...")
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
                        print(f"⚠️ Error extracting textbook: {e}")
                        continue
                
                if page_books == 0:
                    break
                
                page += 1
                time.sleep(self.delay)
            
            print(f"✅ Found {len(textbooks)} textbooks")
            return textbooks
            
        except Exception as e:
            print(f"❌ Error fetching textbooks list: {e}")
            return []
    
    def scrape_textbook_details(self, textbook: Dict[str, Any]) -> Dict[str, Any]:
        """Scrape detailed information for a textbook"""
        try:
            time.sleep(self.delay)
            
            print(f"📖 Scraping: {textbook['title']}")
            
            response = self.session.get(textbook['url'], timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract detailed information
            textbook_details = {
                'id': hashlib.md5(textbook['url'].encode()).hexdigest(),
                'title': textbook['title'],
                'url': textbook['url'],
                'source': self.source,
                'description': self._extract_description(soup),
                'authors': self._extract_authors(soup),
                'subjects': self._extract_subjects(soup),
                'isbn': self._extract_isbn(soup),
                'publisher': self._extract_publisher(soup),
                'publication_date': self._extract_publication_date(soup),
                'language': self._extract_language(soup),
                'format': 'textbook',
                'license': self._extract_license(soup),
                'download_links': self._extract_download_links(soup),
                'reviews': self._extract_reviews(soup),
                'raw_data': {
                    'scraped_at': datetime.now().isoformat(),
                    'scraper_version': '1.0'
                }
            }
            
            return textbook_details
            
        except Exception as e:
            print(f"⚠️ Error scraping textbook {textbook.get('title', 'Unknown')}: {e}")
            return None
    
    def _extract_description(self, soup) -> str:
        """Extract textbook description"""
        desc_elem = soup.find('div', class_='description') or \
                   soup.find('div', class_='summary') or \
                   soup.find('div', id='description')
        if desc_elem:
            return desc_elem.get_text().strip()
        return ""
    
    def _extract_authors(self, soup) -> List[str]:
        """Extract author names"""
        authors = []
        
        # Try different selectors
        author_elems = soup.find_all('span', class_='author') or \
                      soup.find_all('div', class_='author') or \
                      soup.find_all('a', href=lambda x: x and '/author/' in str(x))
        
        for elem in author_elems:
            author_name = elem.get_text().strip()
            if author_name and author_name not in authors:
                authors.append(author_name)
        
        # Try metadata approach
        if not authors:
            meta_elem = soup.find('meta', {'name': 'author'})
            if meta_elem:
                authors.append(meta_elem.get('content', '').strip())
        
        return authors
    
    def _extract_subjects(self, soup) -> List[str]:
        """Extract subject areas"""
        subjects = []
        
        # Look for subject tags/categories
        subject_elems = soup.find_all('a', href=lambda x: x and '/subject/' in str(x)) or \
                       soup.find_all('span', class_='subject') or \
                       soup.find_all('div', class_='subject')
        
        for elem in subject_elems:
            subject = elem.get_text().strip()
            if subject and subject not in subjects:
                subjects.append(subject)
        
        return subjects
    
    def _extract_isbn(self, soup) -> str:
        """Extract ISBN if available"""
        # Look for ISBN in various places
        isbn_elem = soup.find(string=lambda x: x and 'ISBN' in str(x))
        if isbn_elem:
            import re
            isbn_match = re.search(r'ISBN[:\s]*([0-9\-]+)', isbn_elem)
            if isbn_match:
                return isbn_match.group(1)
        return ""
    
    def _extract_publisher(self, soup) -> str:
        """Extract publisher information"""
        pub_elem = soup.find('span', class_='publisher') or \
                  soup.find('div', class_='publisher')
        if pub_elem:
            return pub_elem.get_text().strip()
        return ""
    
    def _extract_publication_date(self, soup) -> str:
        """Extract publication date"""
        date_elem = soup.find('time') or \
                   soup.find('span', class_='date') or \
                   soup.find('div', class_='date')
        if date_elem:
            return date_elem.get('datetime') or date_elem.get_text().strip()
        return ""
    
    def _extract_language(self, soup) -> str:
        """Extract language"""
        lang_elem = soup.find('span', class_='language')
        if lang_elem:
            return lang_elem.get_text().strip().lower()
        return "en"  # Default to English
    
    def _extract_license(self, soup) -> str:
        """Extract license information"""
        license_elem = soup.find('a', href=lambda x: x and 'creativecommons.org' in str(x)) or \
                      soup.find('span', class_='license')
        if license_elem:
            return license_elem.get_text().strip()
        return "Open License"
    
    def _extract_download_links(self, soup) -> List[Dict[str, str]]:
        """Extract download links"""
        downloads = []
        
        # Look for download links
        download_elems = soup.find_all('a', href=lambda x: x and any(ext in str(x) for ext in ['.pdf', '.epub', '.mobi', '.zip']))
        
        for elem in download_elems:
            href = elem.get('href', '')
            if href:
                # Determine format
                format_type = 'pdf'
                if '.epub' in href:
                    format_type = 'epub'
                elif '.mobi' in href:
                    format_type = 'mobi'
                elif '.zip' in href:
                    format_type = 'archive'
                
                downloads.append({
                    'format': format_type,
                    'url': href if href.startswith('http') else urljoin(self.base_url, href),
                    'text': elem.get_text().strip()
                })
        
        return downloads
    
    def _extract_reviews(self, soup) -> List[Dict[str, str]]:
        """Extract reviews if available"""
        reviews = []
        
        review_elems = soup.find_all('div', class_='review') or \
                      soup.find_all('article', class_='review')
        
        for elem in review_elems:
            review_text = elem.get_text().strip()
            if review_text:
                reviews.append({
                    'text': review_text,
                    'date': datetime.now().isoformat()
                })
        
        return reviews
    
    def save_to_bronze(self, data: List[Dict[str, Any]]):
        """Save data to bronze layer (MinIO)"""
        if not data:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bronze/otl/batch_{timestamp}.json"
        
        # Save locally first
        local_path = f"/tmp/{filename.replace('/', '_')}"
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # Upload to MinIO if available
        if self.minio_client:
            try:
                self.minio_client.fput_object(self.bucket, filename, local_path)
                print(f"💾 Saved to MinIO: {filename} ({len(data)} records)")
                os.remove(local_path)
            except Exception as e:
                print(f"❌ MinIO upload failed: {e}")
                print(f"📁 Data saved locally: {local_path}")
        else:
            print(f"📁 Data saved locally: {local_path}")
    
    def run(self):
        """Main execution function"""
        print("🚀 Starting OTL scraping...")
        
        # Get textbooks list
        textbooks = self.get_textbooks_list()
        if not textbooks:
            print("❌ No textbooks found")
            return
        
        # Scrape textbook details
        scraped_data = []
        for idx, textbook in enumerate(textbooks):
            details = self.scrape_textbook_details(textbook)
            if details:
                scraped_data.append(details)
        
        # Save all data
        if scraped_data:
            self.save_to_bronze(scraped_data)
        
        print(f"✅ OTL scraping completed! Scraped {len(scraped_data)} textbooks")

def main():
    """Entry point for standalone execution"""
    scraper = OTLScraperStandalone()
    scraper.run()

if __name__ == "__main__":
    main()

