#!/usr/bin/env python3
"""
MIT OCW Scraper - Bronze Layer
==============================

Standalone script to scrape MIT OpenCourseWare and store to MinIO bronze layer.
Based on building-lakehouse pattern.
"""

import os
import json
import time
import random
import hashlib
import requests
from datetime import datetime
from typing import List, Dict, Any
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# MinIO imports
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    print("Warning: MinIO library not found")

class MITOCWScraperStandalone:
    """Standalone MIT OCW scraper for bronze layer"""
    
    def __init__(self):
        self.base_url = "https://ocw.mit.edu"
        self.source = "mit_ocw"
        self.delay = float(os.getenv('SCRAPING_DELAY_BASE', 2.0))
        self.batch_size = int(os.getenv('BATCH_SIZE', 25))
        self.max_documents = int(os.getenv('MAX_DOCUMENTS', 100))
        
        # MinIO setup
        self.minio_client = self._setup_minio() if MINIO_AVAILABLE else None
        self.bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        
        # Data storage
        self.current_batch = []
        
        print(f"🚀 MIT OCW Scraper initialized - Max docs: {self.max_documents}")
    
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
    
    def _setup_selenium(self):
        """Setup Selenium Chrome driver"""
        try:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            
            driver = webdriver.Chrome(options=options)
            driver.implicitly_wait(10)
            return driver
        except Exception as e:
            print(f"❌ Selenium setup failed: {e}")
            return None
    
    def scrape_course_list(self):
        """Scrape course list from MIT OCW"""
        driver = self._setup_selenium()
        if not driver:
            return []
        
        try:
            print("🔍 Fetching course list...")
            driver.get(f"{self.base_url}/search/")
            
            # Wait for courses to load
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".course-tile"))
            )
            
            courses = []
            course_elements = driver.find_elements(By.CSS_SELECTOR, ".course-tile")
            
            for idx, element in enumerate(course_elements):
                if self.max_documents and idx >= self.max_documents:
                    break
                
                try:
                    title_elem = element.find_element(By.CSS_SELECTOR, ".course-title")
                    link_elem = element.find_element(By.CSS_SELECTOR, "a")
                    
                    course = {
                        'title': title_elem.text.strip(),
                        'url': urljoin(self.base_url, link_elem.get_attribute('href')),
                        'source': self.source,
                        'scraped_at': datetime.now().isoformat()
                    }
                    courses.append(course)
                    
                except Exception as e:
                    print(f"⚠️ Error extracting course {idx}: {e}")
                    continue
            
            print(f"✅ Found {len(courses)} courses")
            return courses
            
        except Exception as e:
            print(f"❌ Error scraping course list: {e}")
            return []
        finally:
            driver.quit()
    
    def scrape_course_details(self, course: Dict[str, Any]) -> Dict[str, Any]:
        """Scrape detailed info for a single course"""
        try:
            time.sleep(random.uniform(self.delay * 0.5, self.delay * 1.5))
            
            response = requests.get(course['url'], timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract course details
            course_details = {
                'id': hashlib.md5(course['url'].encode()).hexdigest(),
                'title': course['title'],
                'url': course['url'],
                'source': self.source,
                'description': self._extract_description(soup),
                'subjects': self._extract_subjects(soup),
                'level': self._extract_level(soup),
                'instructors': self._extract_instructors(soup),
                'language': 'en',
                'format': 'course',
                'license': 'CC BY-NC-SA',
                'raw_data': {
                    'scraped_at': datetime.now().isoformat(),
                    'scraper_version': '1.0'
                }
            }
            
            return course_details
            
        except Exception as e:
            print(f"⚠️ Error scraping course details {course.get('title', 'Unknown')}: {e}")
            return None
    
    def _extract_description(self, soup) -> str:
        """Extract course description"""
        desc_elem = soup.find('div', class_='course-description')
        if desc_elem:
            return desc_elem.get_text().strip()
        return ""
    
    def _extract_subjects(self, soup) -> List[str]:
        """Extract course subjects/topics"""
        subjects = []
        subject_elems = soup.find_all('a', href=lambda x: x and '/courses/subject/' in x)
        for elem in subject_elems:
            subjects.append(elem.text.strip())
        return subjects
    
    def _extract_level(self, soup) -> str:
        """Extract course level"""
        level_elem = soup.find('span', class_='course-level')
        if level_elem:
            return level_elem.text.strip().lower()
        return "undergraduate"
    
    def _extract_instructors(self, soup) -> List[str]:
        """Extract instructor names"""
        instructors = []
        instructor_elems = soup.find_all('a', href=lambda x: x and '/instructor/' in x)
        for elem in instructor_elems:
            instructors.append(elem.text.strip())
        return instructors
    
    def save_to_bronze(self, data: List[Dict[str, Any]]):
        """Save data to bronze layer (MinIO)"""
        if not data:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bronze/mit_ocw/batch_{timestamp}.json"
        
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
        print("🚀 Starting MIT OCW scraping...")
        
        # Get course list
        courses = self.scrape_course_list()
        if not courses:
            print("❌ No courses found")
            return
        
        # Scrape course details
        scraped_data = []
        for idx, course in enumerate(courses):
            print(f"📚 Scraping course {idx+1}/{len(courses)}: {course['title']}")
            
            details = self.scrape_course_details(course)
            if details:
                scraped_data.append(details)
            
            # Save in batches
            if len(scraped_data) >= self.batch_size:
                self.save_to_bronze(scraped_data)
                scraped_data = []
        
        # Save remaining data
        if scraped_data:
            self.save_to_bronze(scraped_data)
        
        print(f"✅ MIT OCW scraping completed!")

def main():
    """Entry point for standalone execution"""
    scraper = MITOCWScraperStandalone()
    scraper.run()

if __name__ == "__main__":
    main()

