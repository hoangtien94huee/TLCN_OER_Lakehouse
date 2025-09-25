#!/usr/bin/env python3
"""
OpenStax Scraper - Bronze Layer
===============================

Standalone script to scrape OpenStax textbooks and store to MinIO bronze layer.
Based on building-lakehouse pattern.
"""

import os
import json
import time
import hashlib
import requests
from datetime import datetime
from typing import List, Dict, Any
from bs4 import BeautifulSoup

# MinIO imports
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    print("Warning: MinIO library not found")

class OpenStaxScraperStandalone:
    """Standalone OpenStax scraper for bronze layer"""
    
    def __init__(self):
        self.base_url = "https://openstax.org"
        self.api_url = "https://openstax.org/apps/cms/api/v2"
        self.source = "openstax"
        self.delay = float(os.getenv('SCRAPING_DELAY_BASE', 1.0))
        self.max_books = int(os.getenv('MAX_DOCUMENTS', 50))
        
        # MinIO setup
        self.minio_client = self._setup_minio() if MINIO_AVAILABLE else None
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        
        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        print(f"🚀 OpenStax Scraper initialized - Max books: {self.max_books}")
    
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
    
    def get_books_list(self) -> List[Dict[str, Any]]:
        """Get list of all OpenStax books"""
        try:
            print("🔍 Fetching OpenStax books list...")
            
            # First get books from API
            api_url = f"{self.api_url}/pages/?type=books.Book&fields=*"
            response = self.session.get(api_url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            books = []
            
            for item in data.get('items', []):
                if len(books) >= self.max_books:
                    break
                
                book = {
                    'id': str(item.get('id')),
                    'title': item.get('title', ''),
                    'slug': item.get('meta', {}).get('slug', ''),
                    'book_state': item.get('book_state', ''),
                    'subjects': [subj.get('subject_name') for subj in item.get('book_subjects', [])],
                    'api_data': item  # Store full API response
                }
                
                books.append(book)
            
            print(f"✅ Found {len(books)} books")
            return books
            
        except Exception as e:
            print(f"❌ Error fetching books list: {e}")
            return []
    
    def scrape_book_details(self, book: Dict[str, Any]) -> Dict[str, Any]:
        """Scrape detailed information for a book"""
        try:
            time.sleep(self.delay)
            
            # Build book URL
            book_url = f"{self.base_url}/details/books/{book['slug']}"
            
            print(f"📖 Scraping: {book['title']}")
            
            response = self.session.get(book_url, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract detailed information
            book_details = {
                'id': hashlib.md5(book_url.encode()).hexdigest(),
                'title': book['title'],
                'url': book_url,
                'source': self.source,
                'description': self._extract_description(soup),
                'subjects': book.get('subjects', []),
                'authors': self._extract_authors(soup),
                'isbn': self._extract_isbn(soup),
                'language': 'en',
                'format': 'textbook',
                'license': self._extract_license(soup),
                'download_links': self._extract_download_links(soup),
                'publication_date': self._extract_publication_date(soup),
                'raw_data': {
                    'api_data': book.get('api_data', {}),
                    'scraped_at': datetime.now().isoformat(),
                    'scraper_version': '1.0'
                }
            }
            
            return book_details
            
        except Exception as e:
            print(f"⚠️ Error scraping book {book.get('title', 'Unknown')}: {e}")
            return None
    
    def _extract_description(self, soup) -> str:
        """Extract book description"""
        desc_elem = soup.find('div', class_='description') or soup.find('div', {'data-testid': 'book-description'})
        if desc_elem:
            return desc_elem.get_text().strip()
        return ""
    
    def _extract_authors(self, soup) -> List[str]:
        """Extract author names"""
        authors = []
        
        # Try different selectors for authors
        author_elems = soup.find_all('span', class_='author-name') or \
                      soup.find_all('div', class_='author') or \
                      soup.find_all('a', href=lambda x: x and '/authors/' in str(x))
        
        for elem in author_elems:
            author_name = elem.get_text().strip()
            if author_name and author_name not in authors:
                authors.append(author_name)
        
        return authors
    
    def _extract_isbn(self, soup) -> str:
        """Extract ISBN if available"""
        isbn_elem = soup.find('span', string=lambda x: x and 'ISBN' in x)
        if isbn_elem:
            isbn_text = isbn_elem.parent.get_text() if isbn_elem.parent else isbn_elem.get_text()
            # Extract numbers from ISBN text
            import re
            isbn_match = re.search(r'ISBN[:\s]*([0-9\-]+)', isbn_text)
            if isbn_match:
                return isbn_match.group(1)
        return ""
    
    def _extract_license(self, soup) -> str:
        """Extract license information"""
        license_elem = soup.find('a', href=lambda x: x and 'creativecommons.org' in str(x))
        if license_elem:
            return license_elem.get_text().strip()
        return "CC BY"  # OpenStax default
    
    def _extract_download_links(self, soup) -> List[Dict[str, str]]:
        """Extract download links for different formats"""
        downloads = []
        
        # Look for download buttons/links
        download_elems = soup.find_all('a', href=lambda x: x and any(ext in str(x) for ext in ['.pdf', '.epub', '.zip']))
        
        for elem in download_elems:
            href = elem.get('href', '')
            if href:
                # Determine format from URL or text
                format_type = 'pdf'
                if '.epub' in href:
                    format_type = 'epub'
                elif '.zip' in href:
                    format_type = 'archive'
                
                downloads.append({
                    'format': format_type,
                    'url': href if href.startswith('http') else f"{self.base_url}{href}",
                    'text': elem.get_text().strip()
                })
        
        return downloads
    
    def _extract_publication_date(self, soup) -> str:
        """Extract publication or last updated date"""
        date_elem = soup.find('time') or soup.find('span', string=lambda x: x and any(word in str(x).lower() for word in ['published', 'updated', 'revised']))
        if date_elem:
            return date_elem.get('datetime') or date_elem.get_text().strip()
        return ""
    
    def save_to_bronze(self, data: List[Dict[str, Any]]):
        """Save data to bronze layer (MinIO)"""
        if not data:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bronze/openstax/batch_{timestamp}.json"
        
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
        print("🚀 Starting OpenStax scraping...")
        
        # Get books list
        books = self.get_books_list()
        if not books:
            print("❌ No books found")
            return
        
        # Scrape book details
        scraped_data = []
        for idx, book in enumerate(books):
            details = self.scrape_book_details(book)
            if details:
                scraped_data.append(details)
        
        # Save all data
        if scraped_data:
            self.save_to_bronze(scraped_data)
        
        print(f"✅ OpenStax scraping completed! Scraped {len(scraped_data)} books")

def main():
    """Entry point for standalone execution"""
    scraper = OpenStaxScraperStandalone()
    scraper.run()

if __name__ == "__main__":
    main()

