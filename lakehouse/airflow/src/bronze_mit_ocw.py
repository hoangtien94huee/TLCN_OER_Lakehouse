#!/usr/bin/env python3
"""
MIT OCW Scraper - Bronze Layer
==============================

Standalone script to scrape MIT OpenCourseWare and store to MinIO bronze layer.
Based on building-lakehouse pattern with multimedia support.
"""

import os
import json
import time
import random
import hashlib
import requests
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin, urlparse
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

# Video processing imports
try:
    import yt_dlp
    from youtube_transcript_api import YouTubeTranscriptApi
    VIDEO_PROCESSING_AVAILABLE = True
except ImportError:
    VIDEO_PROCESSING_AVAILABLE = False
    print("Warning: Video processing libraries not found")

# PDF processing imports
try:
    import PyPDF2
    import pdfplumber
    PDF_PROCESSING_AVAILABLE = True
except ImportError:
    PDF_PROCESSING_AVAILABLE = False
    print("Warning: PDF processing libraries not found")

# MinIO imports
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False
    print("Warning: MinIO library not found")

class MITOCWScraper:
    """MIT OCW scraper with multimedia support for bronze layer"""
    
    def __init__(self, delay=2, use_selenium=True, output_dir="scraped_data/mit_ocw", batch_size=25, max_documents=None, **kwargs):
        self.base_url = "https://ocw.mit.edu"
        self.source = "mit_ocw"
        self.delay = delay
        self.use_selenium = use_selenium
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.max_documents = max_documents
        self.driver = None
        
        # Session for requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.82 Safari/537.36'
        })
        
        # Multimedia settings
        self.enable_video_scraping = os.getenv('ENABLE_VIDEO_SCRAPING', '1') == '1'
        self.enable_pdf_scraping = os.getenv('ENABLE_PDF_SCRAPING', '1') == '1'
        self.max_pdf_size_mb = int(os.getenv('MAX_PDF_SIZE_MB', 50))
        self.download_pdfs = os.getenv('DOWNLOAD_PDFS', '1') == '1'
        
        # Batch processing
        self.current_batch = []
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create local directories for multimedia content
        self.local_storage = Path("/tmp/mit_ocw_scrape")
        self.local_storage.mkdir(exist_ok=True)
        (self.local_storage / "pdfs").mkdir(exist_ok=True)
        (self.local_storage / "transcripts").mkdir(exist_ok=True)
        
        # MinIO setup
        self.minio_client = None
        self.minio_bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
        self.bucket = self.minio_bucket 
        self.minio_enable = str(os.getenv('MINIO_ENABLE', '0')).lower() in {'1', 'true', 'yes'}
        if self.minio_enable and MINIO_AVAILABLE:
            try:
                self.minio_client = Minio(
                    endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
                    access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                    secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
                    secure=str(os.getenv('MINIO_SECURE', '0')).lower() in {'1', 'true', 'yes'}
                )
                if not self.minio_client.bucket_exists(self.minio_bucket):
                    self.minio_client.make_bucket(self.minio_bucket)
                print("MinIO client initialized successfully")
            except Exception as e:
                print(f"[MinIO] Error initializing MinIO client: {e}")
                self.minio_enable = False
        elif self.minio_enable and not MINIO_AVAILABLE:
            print("[MinIO] MinIO library not available, disabling MinIO features")
            self.minio_enable = False
        
        # Setup Selenium if needed
        if self.use_selenium:
            self._setup_selenium()
        
        print(f"MIT OCW Scraper initialized - Max docs: {self.max_documents}")
        print(f"Video scraping: {'ENABLED' if self.enable_video_scraping else 'DISABLED'}")
        print(f"PDF scraping: {'ENABLED' if self.enable_pdf_scraping else 'DISABLED'}")
    
    def _setup_selenium(self):
        """Setup Selenium WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.82 Safari/537.36")
            
            # Try ChromeDriver paths
            chromedriver_paths = ['/usr/local/bin/chromedriver', '/usr/bin/chromedriver', 'chromedriver']
            
            for path in chromedriver_paths:
                try:
                    if os.path.exists(path) or path == 'chromedriver':
                        service = Service(path)
                        self.driver = webdriver.Chrome(service=service, options=chrome_options)
                        print(f"Selenium ready with: {path}")
                        break
                except Exception:
                    continue
            
        except Exception as e:
            print(f"Selenium setup failed: {e}")
            self.use_selenium = False
            self.driver = None
    
    def cleanup(self):
        """Cleanup resources"""
        if self.driver:
            try:
                self.driver.quit()
                print("Đã đóng Selenium WebDriver")
            except Exception as e:
                print(f"Lỗi đóng WebDriver: {e}")
    
    def create_document_id(self, source: str, url: str) -> str:
        """Tạo ID unique cho document"""
        unique_string = f"{source}_{url}"
        return hashlib.md5(unique_string.encode()).hexdigest()
    
    def get_page(self, url: str, max_retries=3) -> BeautifulSoup:
        """Lấy nội dung trang web với retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                return soup
                
            except Exception as e:
                print(f"Lỗi lấy trang {url} (lần {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    return None
    
    def add_to_batch(self, document):
        """Add document to in-memory batch; writes occur at crawl completion"""
        self.current_batch.append(document)

    
    def save_batch(self):
        """Lưu batch hiện tại"""
        if not self.current_batch:
            return
        
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"mit_ocw_batch_{timestamp}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.current_batch, f, ensure_ascii=False, indent=2)
            
            print(f"Đã lưu batch {len(self.current_batch)} documents vào {filename}")
            self.current_batch = []
            
        except Exception as e:
            print(f"Lỗi lưu batch: {e}")
    
    def _smart_delay(self):
        """Smart delay với random để tránh bị block"""
        delay_time = self.delay + random.uniform(0.5, 2.0)
        time.sleep(delay_time)
    
    def scrape_with_selenium(self) -> List[Dict[str, Any]]:
        """Cào MIT OCW courses với logic advanced"""
        print("Bắt đầu cào MIT OCW courses với logic advanced...")
        all_documents = []
        
        try:
            # Lấy course URLs với method advanced
            course_urls = self._get_all_course_urls_advanced()
            print(f"Tổng số courses tìm thấy: {len(course_urls)}")
            
            if not course_urls:
                print("Không tìm thấy course URLs nào!")
                return []
            
            success_count = 0
            error_count = 0
            course_list = list(course_urls)
            
            for i, course_url in enumerate(course_list):
                try:
                    # Kiểm tra giới hạn số lượng tài liệu
                    if self.max_documents and len(all_documents) >= self.max_documents:
                        print(f"\nĐã đạt giới hạn {self.max_documents} tài liệu. Dừng cào.")
                        break
                    
                    # Smart delay
                    self._smart_delay()
                    
                    print(f"[{i+1}/{len(course_list)}] Đang cào: {course_url}")
                    
                    # Cào course với retry
                    course_data = self._scrape_course_with_retry(course_url)
                    if course_data:
                        self.add_to_batch(course_data)
                        all_documents.append(course_data)
                        success_count += 1
                        print(f"Thành công: {course_data['title'][:60]}...")
                        
                        # Kiểm tra lại giới hạn sau khi thêm document
                        if self.max_documents and len(all_documents) >= self.max_documents:
                            print(f"\nĐã cào đủ {self.max_documents} tài liệu. Dừng cào.")
                            break
                    else:
                        error_count += 1
                        print(f"Không thể cào course: {course_url}")
                    
                    # Progress report
                    if (i + 1) % 25 == 0:
                        print(f"Progress: {i+1}/{len(course_list)} - Success: {success_count}, Errors: {error_count}")
                        
                except KeyboardInterrupt:
                    print(f"\nTạm dừng bởi người dùng...")
                    self.save_batch()
                    break
                    
                except Exception as e:
                    error_count += 1
                    print(f"Lỗi cào course {course_url}: {e}")
                    continue
            
            # Lưu batch cuối cùng
            if self.current_batch:
                self.save_batch()
            
            print(f"\nKết quả: {len(all_documents)} courses | Success: {success_count} | Errors: {error_count}")
            
            # Lưu kết quả cuối cùng
            self._save_final_data(all_documents)
            
        except Exception as e:
            print(f"Lỗi nghiêm trọng: {e}")
            if self.current_batch:
                self.save_batch()
        
        return all_documents
    
    def _get_all_course_urls_advanced(self) -> set:
        """Lấy tất cả course URLs từ MIT OCW search page"""
        course_urls = set()
        
        if not self.driver:
            return self._get_course_urls_fallback()
        
        try:
            # Chỉ dùng URL hiệu quả nhất
            search_url = "https://ocw.mit.edu/search/?s=department_course_numbers.sort_coursenum&type=course"
            print(f"Đang cào courses từ: {search_url}")
            
            self.driver.get(search_url)
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
            
            # Progressive scroll để load courses
            self._progressive_scroll()
            
            # Extract URLs
            course_urls = self._extract_course_urls()
            print(f"Tìm thấy {len(course_urls)} courses")
            
        except Exception as e:
            print(f"Lỗi lấy course URLs: {e}")
            return self._get_course_urls_fallback()
        
        return course_urls
    
    def _get_course_urls_fallback(self) -> set:
        """Fallback method với requests"""
        print("Sử dụng fallback method...")
        course_urls = set()
        
        try:
            # Sử dụng cùng URL như method chính
            search_url = "https://ocw.mit.edu/search/?s=department_course_numbers.sort_coursenum&type=course"
            print(f"Fallback: đang cào từ {search_url}")
            
            soup = self.get_page(search_url)
            if soup:
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    if '/courses/' in href and self._is_valid_course_url(href):
                        full_url = urljoin(self.base_url, href)
                        course_urls.add(full_url)
                
                print(f"Fallback: tìm thấy {len(course_urls)} URLs")
            
        except Exception as e:
            print(f"Lỗi fallback method: {e}")
        
        return course_urls
    
    def _progressive_scroll(self):
        """Progressive scroll để load content"""
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            scroll_attempts = 0
            max_scrolls = 5
            
            while scroll_attempts < max_scrolls:
                # Scroll down
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Check if content loaded
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                    
                last_height = new_height
                scroll_attempts += 1
                
                # Try load more buttons
                try:
                    for selector in ['.load-more', '.show-more', 'button[contains(text(), "Load")]']:
                        try:
                            button = self.driver.find_element(By.CSS_SELECTOR, selector)
                            if button.is_displayed():
                                button.click()
                                time.sleep(2)
                                break
                        except:
                            continue
                except:
                    pass
            
            print(f"Scroll completed after {scroll_attempts} attempts")
            
        except Exception as e:
            print(f"Lỗi scroll: {e}")
    
    def _extract_course_urls(self) -> set:
        """Extract course URLs - đơn giản hiệu quả"""
        course_urls = set()
        
        try:
            # Wait for content
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "a"))
            )
            time.sleep(2)
            
            # Get all links and filter
            all_links = self.driver.find_elements(By.TAG_NAME, 'a')
            
            for link in all_links:
                try:
                    href = link.get_attribute('href')
                    if href and '/courses/' in href and self._is_valid_course_url(href):
                        if href.startswith('/'):
                            full_url = urljoin(self.base_url, href)
                        else:
                            full_url = href
                        
                        if 'ocw.mit.edu' in full_url or full_url.startswith(self.base_url):
                            course_urls.add(full_url)
                            
                except Exception:
                    continue
            
            print(f"Found {len(course_urls)} course URLs")
            
        except Exception as e:
            print(f"Lỗi extract URLs: {e}")
        
        return course_urls
    
    def _is_valid_course_url(self, url: str) -> bool:
        """Kiểm tra URL course hợp lệ"""
        if not url or '/courses/' not in url:
            return False
        
        if 'ocw.mit.edu' not in url:
            return False
            
        skip_patterns = ['?page=', '&page=', '#', 'search', 'filter', 'download', 'print']
        for pattern in skip_patterns:
            if pattern in url:
                return False
                
        return True
    
    def _scrape_course_with_retry(self, url: str, max_retries=3) -> Dict[str, Any]:
        """Cào course với retry logic"""
        for attempt in range(max_retries):
            try:
                course_data = self._scrape_course(url)
                if course_data:
                    return course_data
                else:
                    if attempt < max_retries - 1:
                        delay = (2 ** attempt) + random.uniform(0.5, 1.0)
                        time.sleep(delay)
                        continue
                    
            except Exception as e:
                print(f"Lỗi cào course {url} (lần {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    delay = (2 ** attempt) + random.uniform(0.5, 1.0)
                    time.sleep(delay)
                    continue
                else:
                    return None
        
        return None
    
    def _scrape_course(self, url: str) -> Dict[str, Any]:
        """Cào thông tin course với format JSON yêu cầu và multimedia"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        try:
            # Title
            title_selectors = [
                'h1[data-testid="course-title"]',
                'h1.course-title',
                'h1.course-header-title',
                '.course-info h1',
                'h1'
            ]
            title = self._extract_text_by_selectors(soup, title_selectors)
            
            # Clean title
            if title and '|' in title:
                title = title.split('|')[0].strip()
            
            if not title or len(title) < 3:
                return None
            
            # Description
            desc_selectors = [
                '[data-testid="course-description"]',
                '.course-description',
                '.course-intro',
                '.course-summary',
                'meta[name="description"]'
            ]
            description = self._extract_text_by_selectors(soup, desc_selectors, 'content')
            
            # Authors
            authors = self._extract_instructors_advanced(soup)
            
            # Subject
            subject = self._extract_subject(soup)
            
            # PDF URL
            url_pdf = self._find_pdf_url(soup, url)
            
            # Extract multimedia content (enhanced to explore sub-sections)
            videos = self.extract_videos_and_transcripts(soup, url)
            pdfs = self.extract_and_download_pdfs(soup, url)
            
            # Also explore sub-sections for additional content
            section_content = self._explore_course_sections(soup, url)
            videos.extend(section_content.get('videos', []))
            pdfs.extend(section_content.get('pdfs', []))
            
            # Calculate multimedia statistics
            total_videos = len(videos)
            total_pdfs = len(pdfs)
            total_size_mb = sum(pdf.get('size_mb', 0) for pdf in pdfs)
            videos_with_transcripts = len([v for v in videos if v.get('transcript')])
            downloaded_pdfs = len([p for p in pdfs if p.get('downloaded', False)])
            
            # Log multimedia findings
            if total_videos > 0 or total_pdfs > 0:
                print(f"   Videos: {total_videos} (transcripts: {videos_with_transcripts})")
                print(f"   PDFs: {total_pdfs} (downloaded: {downloaded_pdfs}, size: {total_size_mb:.1f}MB)")
                if total_pdfs > 0:
                    # Log PDF categories for better tracking
                    pdf_categories = {}
                    for pdf in pdfs:
                        cat = pdf.get('category', 'unknown')
                        pdf_categories[cat] = pdf_categories.get(cat, 0) + 1
                    categories_str = ', '.join(f"{k}: {v}" for k, v in pdf_categories.items())
                    print(f"   PDF Categories: {categories_str}")
            
            return {
                'id': self.create_document_id(self.source, url),
                'title': title,
                'description': description or '',
                'authors': authors,
                'subject': subject,
                'source': 'MIT',
                'url': url,
                'url_pdf': url_pdf,
                'scraped_at': datetime.now().isoformat(),
                
                # Multimedia content (from original)
                'videos': videos,
                'pdfs': pdfs,
                'multimedia_stats': {
                    'total_videos': total_videos,
                    'total_pdfs': total_pdfs,
                    'videos_with_transcripts': videos_with_transcripts,
                    'downloaded_pdfs': downloaded_pdfs,
                    'total_pdf_size_mb': round(total_size_mb, 2),
                    'has_multimedia': total_videos > 0 or total_pdfs > 0
                },
                'raw_data': {
                    'scraped_at': datetime.now().isoformat(),
                    'scraper_version': '3.0',  # Updated version
                    'multimedia_enabled': {
                        'videos': self.enable_video_scraping,
                        'pdfs': self.enable_pdf_scraping,
                        'pdf_downloads': self.download_pdfs
                    }
                }
            }
            
        except Exception as e:
            print(f"Lỗi parse course {url}: {e}")
            return None
    
    def _extract_text_by_selectors(self, soup, selectors, attribute=None):
        """Extract text từ HTML bằng CSS selectors"""
        for selector in selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    if attribute:
                        return element.get(attribute, '').strip()
                    else:
                        return element.get_text(strip=True)
            except Exception:
                continue
        return ''
    
    def _extract_instructors_advanced(self, soup):
        """Extract instructors với logic advanced cho MIT OCW"""
        instructors = []
        
        try:
            # Method 1: course-info-instructor class
            instructor_links = soup.find_all('a', class_='course-info-instructor')
            for link in instructor_links:
                name = link.get_text(strip=True)
                if name and name not in instructors:
                    instructors.append(name)
            
            # Method 2: Tìm trong course-info section
            course_info_sections = soup.find_all('div', class_='course-info')
            for section in course_info_sections:
                instructor_heading = section.find(string=lambda text: text and 'instructor' in text.lower())
                if instructor_heading:
                    parent = instructor_heading.find_parent()
                    if parent:
                        next_content = parent.find_next('div', class_='course-info-content')
                        if next_content:
                            links = next_content.find_all('a')
                            for link in links:
                                name = link.get_text(strip=True)
                                if name and len(name) > 2 and name not in instructors:
                                    if not any(word in name.lower() for word in ['view', 'profile', 'search']):
                                        instructors.append(name)
                            
                            if not links:
                                text_content = next_content.get_text(strip=True)
                                if text_content and len(text_content) > 2:
                                    text_content = ' '.join(text_content.split())
                                    if text_content not in instructors:
                                        instructors.append(text_content)
            
            # Method 3: Pattern matching
            course_info_text = soup.find('div', class_='course-info')
            if course_info_text:
                all_text = course_info_text.get_text()
                name_pattern = r'\b[A-Z][a-z]+ [A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b'
                potential_names = re.findall(name_pattern, all_text)
                for name in potential_names:
                    if name not in instructors and len(name) > 5:
                        if not any(word in name.lower() for word in ['course info', 'electrical engineering']):
                            instructors.append(name)
                    
        except Exception as e:
            print(f"Lỗi extract instructors: {e}")
        
        # Clean up
        cleaned_instructors = []
        noise_words = [
            'electrical engineering', 'computer science', 'mathematics', 'physics',
            'as taught', 'fall', 'spring', 'course info', 'undergraduate', 'graduate'
        ]
        
        for instructor in instructors:
            if len(instructor) < 100:
                instructor_lower = instructor.lower()
                is_noise = any(noise in instructor_lower for noise in noise_words)
                
                has_multiple_words = len(instructor.split()) >= 2
                has_capitals = any(c.isupper() for c in instructor)
                
                if not is_noise and has_multiple_words and has_capitals:
                    is_duplicate = False
                    for existing in cleaned_instructors:
                        if instructor in existing or existing in instructor:
                            is_duplicate = True
                            break
                    
                    if not is_duplicate:
                        cleaned_instructors.append(instructor)
        
        return cleaned_instructors[:3]
    
    def _extract_subject(self, soup):
        """Extract subject/topic classification - bao gồm cả topics và departments"""
        topics = []
        departments = []
        
        try:
            # Method 1: Extract từ course-info sections (tìm TOPICS)
            course_info_sections = soup.find_all('div', class_='course-info')
            for section in course_info_sections:
                # Tìm text "TOPICS" trong section
                topics_text = section.find(string=lambda text: text and 'topics' in text.lower())
                if topics_text:
                    # Tìm content sau TOPICS
                    parent = topics_text.find_parent()
                    if parent:
                        # Tìm next sibling hoặc content div
                        next_content = parent.find_next_sibling()
                        if not next_content:
                            next_content = parent.find_next('div', class_='course-info-content')
                        
                        if next_content:
                            # Extract topic links
                            topic_links = next_content.find_all('a')
                            for link in topic_links:
                                topic_text = link.get_text(strip=True)
                                if topic_text and len(topic_text) > 1:
                                    topics.append(topic_text)
            
            # Method 2: Extract từ course-info sections (tìm DEPARTMENTS)
            for section in course_info_sections:
                # Tìm text "DEPARTMENTS" trong section
                dept_text_elem = section.find(string=lambda text: text and 'department' in text.lower())
                if dept_text_elem:
                    # Tìm content sau DEPARTMENTS
                    parent = dept_text_elem.find_parent()
                    if parent:
                        next_content = parent.find_next_sibling()
                        if not next_content:
                            next_content = parent.find_next('div', class_='course-info-content')
                        
                        if next_content:
                            dept_text = next_content.get_text(strip=True)
                            if dept_text and len(dept_text) > 1:
                                dept_text = ' '.join(dept_text.split())
                                # Filter ra instructor names
                                if not any(name in dept_text for name in ['Patrick', 'Winston', 'Professor']):
                                    departments.append(dept_text)
            
            # Method 3: Backup - guess từ course code
            try:
                current_url = soup.find('link', {'rel': 'canonical'})
                if current_url:
                    url = current_url.get('href', '')
                    if '/courses/6-' in url and not departments:
                        departments.append('Electrical Engineering and Computer Science')
                    elif '/courses/18-' in url and not departments:
                        departments.append('Mathematics')
                    elif '/courses/8-' in url and not departments:
                        departments.append('Physics')
                    elif '/courses/14-' in url and not departments:
                        departments.append('Economics')
            except:
                pass
            
            # Method 4: Fallback - tìm topics từ text patterns
            if not topics:
                # Tìm text có pattern "Engineering", "Computer Science", "Artificial Intelligence"
                possible_topics = [
                    'Engineering', 'Computer Science', 'Artificial Intelligence', 
                    'Machine Learning', 'Algorithms and Data Structures', 'Theory of Computation',
                    'Mathematics', 'Calculus', 'Linear Algebra', 'Statistics', 'Probability'
                ]
                
                page_text = soup.get_text().lower()
                for topic in possible_topics:
                    if topic.lower() in page_text:
                        topics.append(topic)
            
            # Method 5: Guess từ title nếu vẫn không có topics
            if not topics:
                title_element = soup.find('h1')
                if title_element:
                    title = title_element.get_text(strip=True)
                    subject_from_title = self._guess_subject_from_title(title)
                    if subject_from_title:
                        topics.append(subject_from_title)
            
        except Exception as e:
            print(f"Lỗi extract subject: {e}")
        
        # Combine topics và departments thành array
        subject_array = []
        
        # Add department first (primary classification)
        if departments:
            dept = departments[0]
            if dept and len(dept) > 2:
                subject_array.append(dept)
        
        # Add topics (specific areas)
        if topics:
            # Clean topics
            for topic in topics:
                if topic and len(topic) > 2:
                    topic_lower = topic.lower()
                    is_valid = not any(invalid in topic_lower for invalid in [
                        'course', 'mit', 'patrick', 'winston', 'henry', 'professor', 'instructor'
                    ])
                    if is_valid and topic not in subject_array:
                        subject_array.append(topic)
        
        # Return array of subjects
        return subject_array
    
    def _guess_subject_from_title(self, title: str) -> str:
        """Guess subject từ title"""
        title_lower = title.lower()
        
        subject_keywords = {
            'Computer Science': ['computer', 'programming', 'algorithm', 'artificial intelligence', 'machine learning'],
            'Mathematics': ['mathematics', 'calculus', 'linear algebra', 'statistics'],
            'Physics': ['physics', 'quantum', 'mechanics'],
            'Chemistry': ['chemistry', 'organic', 'chemical'],
            'Biology': ['biology', 'biological', 'genetics'],
            'Engineering': ['engineering', 'design', 'systems'],
            'Economics': ['economics', 'finance', 'market']
        }
        
        for subject, keywords in subject_keywords.items():
            if any(keyword in title_lower for keyword in keywords):
                return subject
        
        return ""
    
    def _find_pdf_url(self, soup, course_url: str) -> str:
        """Tìm PDF URL thực sự"""
        try:
            pdf_selectors = [
                'a[href$=".pdf"]',
                'a[href*=".pdf?"]',
                'a[href*=".pdf#"]'
            ]
            
            for selector in pdf_selectors:
                pdf_links = soup.select(selector)
                for link in pdf_links:
                    href = link.get('href', '')
                    if href and self._is_valid_pdf_url(href):
                        if href.startswith('http'):
                            return href
                        else:
                            return urljoin(course_url, href)
        
        except Exception as e:
            print(f"Lỗi tìm PDF: {e}")
        
        return ""
    
    def _is_valid_pdf_url(self, url: str) -> bool:
        """Check PDF URL"""
        if not url:
            return False
        url_lower = url.lower()
        return (url_lower.endswith('.pdf') or '.pdf?' in url_lower or '.pdf#' in url_lower)
    
    def _save_final_data(self, documents):
        """Lưu dữ liệu cuối cùng"""
        try:
            output_data = {
                'total_found': len(documents),
                'scraped_at': datetime.now().isoformat(),
                'source': 'MIT OCW',
                'documents': documents
            }
            
            filename = f"mit_ocw_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            print(f"Đã lưu kết quả cuối cùng vào {filename}")
            
        except Exception as e:
            print(f"Lỗi lưu dữ liệu cuối cùng: {e}")
    
    def scrape_fallback(self) -> List[Dict[str, Any]]:
        """Fallback method"""
        print("Sử dụng requests fallback...")
        return self.scrape_with_selenium()
    
    def extract_videos_and_transcripts(self, soup, course_url: str) -> List[Dict[str, Any]]:
        """Extract video URLs and their transcripts from course page"""
        if not self.enable_video_scraping:
            return []
        
        videos = []
        
        try:
            # First, look for direct transcript download links (faster and more reliable)
            transcript_links = self._find_transcript_download_links(soup, course_url)
            
            # Find YouTube videos (embedded iframes)
            youtube_iframes = soup.find_all('iframe', src=lambda x: x and 'youtube.com' in x)
            for iframe in youtube_iframes:
                video_data = self._process_youtube_video(iframe, transcript_links)
                if video_data:
                    videos.append(video_data)
            
            # Find MIT OCW native videos
            video_elements = soup.find_all(['video', 'source'])
            for video_elem in video_elements:
                video_data = self._process_native_video(video_elem, course_url, transcript_links)
                if video_data:
                    videos.append(video_data)
            
            # Look for video links in the page (including different video types)
            video_extensions = ['.mp4', '.mov', '.avi', '.wmv', '.flv', '.webm']
            video_links = soup.find_all('a', href=lambda x: x and any(ext in x.lower() for ext in video_extensions))
            for link in video_links:
                video_data = self._process_video_link(link, course_url, transcript_links)
                if video_data:
                    videos.append(video_data)
            
            # Look for links to actual video pages (NOT lecture notes which are PDFs)
            video_page_links = soup.find_all('a', href=lambda x: x and any(term in x.lower() for term in [
                'video-lectures', 'video-gallery', 'videos/', 'watch', 'player', 'media'
            ]) and not any(exclude in x.lower() for exclude in ['notes', 'note', 'pdf', 'handout']))
            
            # Also look for actual video content indicators in link text
            for link in soup.find_all('a'):
                link_text = link.get_text().strip().lower()
                href = link.get('href', '').lower()
                
                # Only consider as video if it explicitly mentions video content
                if any(term in link_text for term in ['video lecture', 'watch video', 'video player', 'streaming']) and \
                   not any(exclude in link_text for exclude in ['notes', 'transcript', 'handout', 'pdf']):
                    video_page_links.append(link)
            
            for link in video_page_links:
                # Extract video type from context
                link_text = link.get_text().strip().lower()
                video_type = self._classify_video_type(link_text)
                if video_type:
                    video_data = self._process_video_page_link(link, course_url, video_type, transcript_links)
                    if video_data:
                        videos.append(video_data)
            
            # Look for embedded MIT OCW video players
            ocw_video_divs = soup.find_all('div', class_=lambda x: x and 'video' in x.lower())
            for div in ocw_video_divs:
                video_data = self._process_ocw_video_div(div, course_url, transcript_links)
                if video_data:
                    videos.append(video_data)
            
            # Also look for structured video content in tables (common in MIT OCW)
            table_videos = self._extract_table_videos(soup, course_url)
            videos.extend(table_videos)
            
            print(f"Found {len(videos)} videos")
            return videos
            
        except Exception as e:
            print(f"WARNING: Error extracting videos: {e}")
            return []
    
    def _find_transcript_download_links(self, soup, course_url: str) -> Dict[str, str]:
        """Find all transcript download links on the page (usually PDF files)"""
        transcript_links = {}
        
        try:
            # Look for "Download transcript" links (usually PDF files)
            download_links = soup.find_all('a', string=lambda text: text and 'transcript' in text.lower())
            
            # Look for PDF files that might be transcripts
            pdf_links = soup.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))
            
            # Filter PDF links for transcript-related ones
            transcript_pdf_keywords = ['transcript', 'captions', 'subtitles', 'script', 'text']
            for pdf_link in pdf_links:
                link_text = pdf_link.get_text().lower()
                href = pdf_link.get('href', '').lower()
                
                # Check if PDF link contains transcript-related keywords
                if any(keyword in link_text or keyword in href for keyword in transcript_pdf_keywords):
                    download_links.append(pdf_link)
            
            # Also look for links with transcript-related text
            transcript_patterns = ['transcript', 'captions', 'subtitles']
            for pattern in transcript_patterns:
                # Look for any links with transcript keywords
                links_by_text = soup.find_all('a', string=lambda text: text and pattern in text.lower())
                download_links.extend(links_by_text)
                
                # Look for links near transcript-related text
                transcript_text_elements = soup.find_all(string=lambda text: text and pattern in text.lower())
                for text_elem in transcript_text_elements:
                    # Find nearby links
                    parent = text_elem.parent if hasattr(text_elem, 'parent') else None
                    if parent:
                        nearby_links = parent.find_all('a', href=True)
                        download_links.extend(nearby_links)
            
            # Extract and map transcript URLs
            for link in download_links:
                href = link.get('href', '')
                if href:
                    transcript_url = urljoin(course_url, href)
                    
                    # Try to associate with video (use video title or position)
                    video_context = self._get_video_context_for_transcript(link)
                    transcript_links[video_context] = transcript_url
            
            if transcript_links:
                print(f"Found {len(transcript_links)} transcript download links")
            
            return transcript_links
            
        except Exception as e:
            print(f"WARNING: Error finding transcript links: {e}")
            return {}
    
    def _get_video_context_for_transcript(self, transcript_link) -> str:
        """Get context/identifier for which video this transcript belongs to"""
        try:
            # Look for video title in nearby elements
            parent = transcript_link.parent
            if parent:
                # Check for video title in same container
                title_elem = parent.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                if title_elem:
                    return title_elem.get_text().strip()
                
                # Check for video links in same container
                video_link = parent.find('a', href=lambda x: x and any(ext in x.lower() for ext in ['.mp4', '.mov', '.avi']))
                if video_link:
                    return video_link.get_text().strip()
            
            # Fallback: use link text or href
            link_text = transcript_link.get_text().strip()
            if link_text and link_text.lower() != 'download transcript':
                return link_text
            
            # Use href as identifier
            href = transcript_link.get('href', '')
            if href:
                return href
            
            return 'unknown'
            
        except Exception:
            return 'unknown'
    
    def _download_transcript_file(self, transcript_url: str, video_id: str) -> Optional[str]:
        """Download transcript file from MIT OCW (usually PDF)"""
        try:
            response = requests.get(transcript_url, timeout=30)
            response.raise_for_status()
            
            # Handle different file formats
            if transcript_url.lower().endswith('.pdf'):
                # Save PDF transcript file
                transcript_pdf_file = self.local_storage / "transcripts" / f"{video_id}_transcript.pdf"
                with open(transcript_pdf_file, 'wb') as f:
                    f.write(response.content)
                
                # Extract text from PDF
                if PDF_PROCESSING_AVAILABLE:
                    content = self._extract_pdf_text(transcript_pdf_file)
                else:
                    print(f"WARNING: PDF processing not available for transcript {video_id}")
                    return None
                
                # Save extracted text
                transcript_text_file = self.local_storage / "transcripts" / f"{video_id}_transcript.txt"
                if content:
                    with open(transcript_text_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                
            elif transcript_url.lower().endswith('.vtt'):
                # Handle VTT files
                content = self._parse_vtt_content(response.text)
                transcript_file = self.local_storage / "transcripts" / f"{video_id}_transcript.txt"
                with open(transcript_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                    
            elif transcript_url.lower().endswith('.srt'):
                # Handle SRT files
                content = self._parse_srt_content(response.text)
                transcript_file = self.local_storage / "transcripts" / f"{video_id}_transcript.txt"
                with open(transcript_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                    
            else:
                # Handle plain text files
                content = response.text
                transcript_file = self.local_storage / "transcripts" / f"{video_id}_transcript.txt"
                with open(transcript_file, 'w', encoding='utf-8') as f:
                    f.write(content)
            
            if content:
                print(f"Downloaded transcript from MIT OCW: {video_id} (PDF)" if transcript_url.lower().endswith('.pdf') else f"Downloaded transcript from MIT OCW: {video_id}")
                
                # Upload to MinIO if available
                if self.minio_client:
                    try:
                        if transcript_url.lower().endswith('.pdf'):
                            # Upload both PDF and text versions
                            minio_pdf_path = f"bronze/mit_ocw/transcripts/{video_id}_transcript.pdf"
                            minio_text_path = f"bronze/mit_ocw/transcripts/{video_id}_transcript.txt"
                            
                            self.minio_client.fput_object(self.bucket, minio_pdf_path, str(transcript_pdf_file))
                            if transcript_text_file.exists():
                                self.minio_client.fput_object(self.bucket, minio_text_path, str(transcript_text_file))
                            
                            print(f"Uploaded transcript PDF and text to MinIO: {video_id}")
                        else:
                            minio_path = f"bronze/mit_ocw/transcripts/{video_id}_transcript.txt"
                            self.minio_client.fput_object(self.bucket, minio_path, str(transcript_file))
                            print(f"Uploaded transcript to MinIO: {video_id}")
                    except Exception as e:
                        print(f"WARNING: Failed to upload transcript to MinIO: {e}")
                
                return content
            else:
                print(f"WARNING: No content extracted from transcript {video_id}")
                return None
            
        except Exception as e:
            print(f"WARNING: Error downloading transcript {video_id}: {e}")
            return None
    
    def _parse_vtt_content(self, vtt_content: str) -> str:
        """Parse VTT file and extract just the text"""
        lines = vtt_content.split('\n')
        text_lines = []
        
        for line in lines:
            line = line.strip()
            # Skip VTT headers, timestamps, and empty lines
            if (line and 
                not line.startswith('WEBVTT') and 
                not line.startswith('NOTE') and
                not '-->' in line and
                not line.isdigit()):
                text_lines.append(line)
        
        return ' '.join(text_lines)
    
    def _parse_srt_content(self, srt_content: str) -> str:
        """Parse SRT file and extract just the text"""
        lines = srt_content.split('\n')
        text_lines = []
        
        for line in lines:
            line = line.strip()
            # Skip SRT sequence numbers, timestamps, and empty lines
            if (line and 
                not line.isdigit() and 
                not '-->' in line):
                text_lines.append(line)
        
        return ' '.join(text_lines)
    
    def _classify_video_type(self, link_text: str) -> str:
        """Classify video type based on link text"""
        video_types = {
            'lecture': ['lecture', 'class', 'session'],
            'tutorial': ['tutorial', 'how-to', 'guide'],
            'workshop': ['workshop', 'lab', 'hands-on'],
            'simulation': ['simulation', 'demo', 'model'],
            'demonstration': ['demonstration', 'demo', 'example'],
            'problem_solving': ['problem', 'solution', 'solving'],
            'competition': ['competition', 'contest', 'challenge']
        }
        
        for video_type, keywords in video_types.items():
            if any(keyword in link_text for keyword in keywords):
                return video_type
        
        return 'other'
    
    def _process_video_page_link(self, link, course_url: str, video_type: str, transcript_links: Dict[str, str] = None) -> Optional[Dict[str, Any]]:
        """Process link to video page"""
        try:
            href = link.get('href', '')
            if not href:
                return None
            
            video_url = urljoin(course_url, href)
            video_id = hashlib.md5(video_url.encode()).hexdigest()[:12]
            title = link.text.strip() or f"{video_type.title()} Video {video_id}"
            
            # Try to get transcript from MIT OCW direct download
            transcript = None
            transcript_source = None
            
            if transcript_links:
                # Try to match transcript by title or video type
                for context, transcript_url in transcript_links.items():
                    if (title.lower() in context.lower() or 
                        context.lower() in title.lower() or
                        video_type in context.lower() or
                        video_id in context):
                        transcript = self._download_transcript_file(transcript_url, video_id)
                        transcript_source = 'mit_ocw_download'
                        break
            
            video_data = {
                'type': f'{video_type}_video_page',
                'video_id': video_id,
                'url': video_url,
                'title': title,
                'video_category': video_type,
                'transcript': transcript,
                'transcript_source': transcript_source,
                'duration': None,
                'source': 'mit_ocw_page'
            }
            
            return video_data
            
        except Exception as e:
            print(f"WARNING: Error processing video page link: {e}")
            return None
    
    def _process_ocw_video_div(self, div, course_url: str, transcript_links: Dict[str, str] = None) -> Optional[Dict[str, Any]]:
        """Process MIT OCW video div elements"""
        try:
            # Look for video data attributes or embedded players
            video_src = div.get('data-video-src') or div.get('data-src')
            if not video_src:
                # Look for nested video elements
                video_elem = div.find('video') or div.find('iframe')
                if video_elem:
                    video_src = video_elem.get('src')
            
            if not video_src:
                return None
            
            video_url = urljoin(course_url, video_src)
            video_id = hashlib.md5(video_url.encode()).hexdigest()[:12]
            title = div.get('title', '').strip() or f"OCW Video {video_id}"
            
            # Try to find transcript in the same div or nearby
            transcript_elem = div.find('div', class_=lambda x: x and 'transcript' in x.lower())
            transcript_text = None
            transcript_source = None
            
            if transcript_elem:
                transcript_text = transcript_elem.get_text().strip()
                transcript_source = 'embedded'
            
            # If no embedded transcript, try MIT OCW direct download
            if not transcript_text and transcript_links:
                for context, transcript_url in transcript_links.items():
                    if (title.lower() in context.lower() or 
                        context.lower() in title.lower() or
                        video_id in context):
                        transcript_text = self._download_transcript_file(transcript_url, video_id)
                        transcript_source = 'mit_ocw_download'
                        break
            
            video_data = {
                'type': 'mit_ocw_player',
                'video_id': video_id,
                'url': video_url,
                'title': title,
                'transcript': transcript_text,
                'transcript_source': transcript_source,
                'duration': None,
                'source': 'mit_ocw',
                'has_embedded_transcript': transcript_text is not None
            }
            
            return video_data
            
        except Exception as e:
            print(f"WARNING: Error processing OCW video div: {e}")
            return None
    
    def _process_youtube_video(self, iframe, transcript_links: Dict[str, str] = None) -> Optional[Dict[str, Any]]:
        """Process YouTube video iframe"""
        try:
            src = iframe.get('src', '')
            video_id = self._extract_youtube_id(src)
            if not video_id:
                return None
            
            title = iframe.get('title', '').strip() or f"Video {video_id}"
            
            # First try to get transcript from MIT OCW direct download
            transcript = None
            transcript_source = None
            
            if transcript_links:
                # Try to match transcript by title or context
                for context, transcript_url in transcript_links.items():
                    if title.lower() in context.lower() or context.lower() in title.lower():
                        transcript = self._download_transcript_file(transcript_url, video_id)
                        transcript_source = 'mit_ocw_download'
                        break
            
            # Fallback to YouTube API if no MIT OCW transcript found
            if not transcript and VIDEO_PROCESSING_AVAILABLE:
                transcript = self._get_youtube_transcript(video_id)
                transcript_source = 'youtube_api'
            
            # Get video info
            video_data = {
                'type': 'youtube_video',
                'video_id': video_id,
                'url': f"https://www.youtube.com/watch?v={video_id}",
                'title': title,
                'transcript': transcript,
                'transcript_source': transcript_source,
                'duration': None,  # Could be fetched with yt-dlp if needed
                'source': 'youtube'
            }
            
            return video_data
            
        except Exception as e:
            print(f"WARNING: Error processing YouTube video: {e}")
            return None
    
    def _extract_youtube_id(self, url: str) -> Optional[str]:
        """Extract YouTube video ID from URL"""
        patterns = [
            r'youtube\.com/embed/([^/?]+)',
            r'youtube\.com/watch\?v=([^&]+)',
            r'youtu\.be/([^/?]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None
    
    def _get_youtube_transcript(self, video_id: str) -> Optional[str]:
        """Get transcript for YouTube video"""
        try:
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['en'])
            transcript_text = ' '.join([item['text'] for item in transcript_list])
            
            # Save transcript to local file
            transcript_file = self.local_storage / "transcripts" / f"{video_id}_transcript.txt"
            with open(transcript_file, 'w', encoding='utf-8') as f:
                f.write(transcript_text)
            
            return transcript_text
            
        except Exception as e:
            print(f"WARNING: Could not get transcript for video {video_id}: {e}")
            return None
    
    def _process_native_video(self, video_elem, course_url: str, transcript_links: Dict[str, str] = None) -> Optional[Dict[str, Any]]:
        """Process MIT OCW native video element"""
        try:
            src = video_elem.get('src', '')
            if not src:
                return None
            
            video_url = urljoin(course_url, src)
            video_id = hashlib.md5(video_url.encode()).hexdigest()[:12]
            title = video_elem.get('title', '').strip() or f"Video {video_id}"
            
            # Try to get transcript from MIT OCW direct download
            transcript = None
            transcript_source = None
            
            if transcript_links:
                # Try to match transcript by title or video URL
                for context, transcript_url in transcript_links.items():
                    if (title.lower() in context.lower() or 
                        context.lower() in title.lower() or
                        video_id in context):
                        transcript = self._download_transcript_file(transcript_url, video_id)
                        transcript_source = 'mit_ocw_download'
                        break
            
            video_data = {
                'type': 'native_video',
                'video_id': video_id,
                'url': video_url,
                'title': title,
                'transcript': transcript,
                'transcript_source': transcript_source,
                'duration': None,
                'source': 'mit_ocw'
            }
            
            return video_data
            
        except Exception as e:
            print(f"WARNING: Error processing native video: {e}")
            return None
    
    def _process_video_link(self, link, course_url: str, transcript_links: Dict[str, str] = None) -> Optional[Dict[str, Any]]:
        """Process video file link"""
        try:
            href = link.get('href', '')
            video_url = urljoin(course_url, href)
            video_id = hashlib.md5(video_url.encode()).hexdigest()[:12]
            title = link.text.strip() or f"Video {video_id}"
            
            # Try to get transcript from MIT OCW direct download
            transcript = None
            transcript_source = None
            
            if transcript_links:
                # Try to match transcript by title or video URL
                for context, transcript_url in transcript_links.items():
                    if (title.lower() in context.lower() or 
                        context.lower() in title.lower() or
                        video_id in context):
                        transcript = self._download_transcript_file(transcript_url, video_id)
                        transcript_source = 'mit_ocw_download'
                        break
            
            video_data = {
                'type': 'video_file',
                'video_id': video_id,
                'url': video_url,
                'title': title,
                'transcript': transcript,
                'transcript_source': transcript_source,
                'duration': None,
                'source': 'mit_ocw',
                'file_extension': Path(href).suffix.lower()
            }
            
            return video_data
            
        except Exception as e:
            print(f"WARNING: Error processing video link: {e}")
            return None
    
    def extract_and_download_pdfs(self, soup, course_url: str) -> List[Dict[str, Any]]:
        """Extract and download PDF files from course page"""
        if not self.enable_pdf_scraping or not PDF_PROCESSING_AVAILABLE:
            return []
        
        pdfs = []
        
        try:
            # Find all PDF links
            pdf_links = soup.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))
            
            # Also look for lecture notes sections specifically
            lecture_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(term in x.lower() for term in [
                'lecture-notes', 'notes', 'readings', 'handouts', 'materials'
            ]))
            
            for section in lecture_sections:
                section_pdf_links = section.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))
                pdf_links.extend(section_pdf_links)
            
            # Look for links that mention lecture notes but link to PDFs
            text_pdf_links = soup.find_all('a', string=lambda x: x and any(term in x.lower() for term in [
                'lecture', 'notes', 'handout', 'reading', 'material'
            ]) and x.lower().endswith('.pdf') == False)  # Text doesn't end with .pdf
            
            for link in text_pdf_links:
                href = link.get('href', '')
                if href and href.lower().endswith('.pdf'):
                    pdf_links.append(link)
            
            # Remove duplicates
            unique_pdf_links = []
            seen_hrefs = set()
            for link in pdf_links:
                href = link.get('href', '')
                if href and href not in seen_hrefs:
                    unique_pdf_links.append(link)
                    seen_hrefs.add(href)
            
            for link in unique_pdf_links:
                pdf_data = self._process_pdf_link(link, course_url)
                if pdf_data:
                    pdfs.append(pdf_data)
            
            print(f"Found {len(pdfs)} PDFs")
            return pdfs
            
        except Exception as e:
            print(f"WARNING: Error extracting PDFs: {e}")
            return []
    
    def _process_pdf_link(self, link, course_url: str) -> Optional[Dict[str, Any]]:
        """Process individual PDF link"""
        try:
            href = link.get('href', '')
            if not href:
                return None
            
            pdf_url = urljoin(course_url, href)
            
            # Get title from link text, or try to extract from context
            pdf_title = self._extract_meaningful_pdf_title(link, href)
            
            # Generate PDF ID and filename
            pdf_id = hashlib.md5(pdf_url.encode()).hexdigest()[:12]
            pdf_filename = self._generate_pdf_filename(pdf_title, pdf_id)
            
            # Classify PDF type based on context
            pdf_type = self._classify_pdf_type(link, pdf_title, href)
            
            # Check file size first
            file_size_mb = self._get_pdf_size(pdf_url)
            if file_size_mb > self.max_pdf_size_mb:
                print(f"WARNING: PDF too large ({file_size_mb}MB): {pdf_title}")
                return {
                    'type': 'pdf',
                    'category': pdf_type,
                    'pdf_id': pdf_id,
                    'title': pdf_title,
                    'url': pdf_url,
                    'size_mb': file_size_mb,
                    'downloaded': False,
                    'local_path': None,
                    'text_content': None,
                    'error': f"File too large ({file_size_mb}MB)"
                }
            
            pdf_data = {
                'type': 'pdf',
                'category': pdf_type,
                'pdf_id': pdf_id,
                'title': pdf_title,
                'filename': pdf_filename,
                'url': pdf_url,
                'size_mb': file_size_mb,
                'downloaded': False,
                'local_path': None,
                'text_content': None
            }
            
            # Download PDF if enabled
            if self.download_pdfs:
                local_path, text_content = self._download_and_extract_pdf(pdf_url, pdf_id, pdf_data)
                pdf_data.update({
                    'downloaded': local_path is not None,
                    'local_path': str(local_path) if local_path else None,
                    'text_content': text_content
                })
            
            return pdf_data
            
        except Exception as e:
            print(f"WARNING: Error processing PDF link: {e}")
            return None
    
    def _classify_pdf_type(self, link_element, title: str, href: str) -> str:
        """Classify PDF type based on context and content"""
        # Get surrounding context
        context = ""
        parent = link_element.parent
        for _ in range(3):  # Go up to 3 levels to find context
            if parent:
                context += parent.get_text().lower()
                parent = parent.parent
            else:
                break
        
        # Check URL and title patterns
        content_lower = (title + " " + href + " " + context).lower()
        
        # Classification rules
        if any(keyword in content_lower for keyword in ['lecture-note', 'lecture_note', 'notes']):
            return 'lecture_notes'
        elif any(keyword in content_lower for keyword in ['handout', 'worksheet']):
            return 'handout'
        elif any(keyword in content_lower for keyword in ['assignment', 'homework', 'pset', 'problem-set']):
            return 'assignment'
        elif any(keyword in content_lower for keyword in ['exam', 'quiz', 'test', 'midterm', 'final']):
            return 'exam'
        elif any(keyword in content_lower for keyword in ['syllabus', 'schedule']):
            return 'syllabus'
        elif any(keyword in content_lower for keyword in ['reading', 'textbook', 'reference']):
            return 'reading'
        else:
            return 'document'
    
    def _explore_course_sections(self, soup, course_url: str) -> Dict[str, List]:
        """Explore course sub-sections like Video Lectures, Lecture Notes to find more content"""
        additional_videos = []
        additional_pdfs = []
        processed_pdf_urls = set()  # Track processed PDFs to avoid duplicates
        
        try:
            # Find navigation links to different sections
            section_links = []
            
            # Look for common section names in MIT OCW
            section_keywords = [
                'video-lectures', 'video_lectures', 'videos',
                'lecture-notes', 'lecture_notes', 'notes', 
                'assignments', 'exams', 'readings', 'resources'
            ]
            
            # Find links that match section patterns
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                text = link.get_text().strip().lower()
                
                if any(keyword in href or keyword in text for keyword in section_keywords):
                    full_url = urljoin(course_url, link['href'])
                    section_info = {
                        'url': full_url,
                        'text': link.get_text().strip(),
                        'type': self._classify_section_type(href, text)
                    }
                    section_links.append(section_info)
            
            # Process each section (limit to avoid too many requests)
            for section in section_links[:5]:  # Limit to 5 sections
                try:
                    print(f"   Exploring section: {section['text']} ({section['type']})")
                    section_content = self._scrape_section_page(section['url'], section['type'])
                    
                    if section['type'] == 'videos':
                        additional_videos.extend(section_content)
                    elif section['type'] == 'pdfs':
                        # Deduplicate PDFs by URL
                        for pdf_item in section_content:
                            pdf_url = pdf_item.get('url', '')
                            if pdf_url and pdf_url not in processed_pdf_urls:
                                processed_pdf_urls.add(pdf_url)
                                additional_pdfs.append(pdf_item)
                            else:
                                print(f"   Skipping duplicate PDF: {pdf_item.get('title', 'Unknown')}")
                        
                except Exception as e:
                    print(f"   Warning: Failed to explore section {section['text']}: {e}")
                    
        except Exception as e:
            print(f"Warning: Error exploring course sections: {e}")
            
        return {
            'videos': additional_videos,
            'pdfs': additional_pdfs
        }
    
    def _classify_section_type(self, href: str, text: str) -> str:
        """Classify what type of content a section contains"""
        content = (href + " " + text).lower()
        
        if any(term in content for term in ['video', 'lecture', 'watch']):
            return 'videos'
        elif any(term in content for term in ['note', 'pdf', 'reading', 'resource']):
            return 'pdfs'
        elif any(term in content for term in ['assignment', 'homework', 'pset']):
            return 'pdfs'  # Assignments are usually PDFs
        elif any(term in content for term in ['exam', 'quiz', 'test']):
            return 'pdfs'  # Exams are usually PDFs
        else:
            return 'unknown'
    
    def _scrape_section_page(self, section_url: str, section_type: str) -> List[Dict]:
        """Scrape a specific section page for videos or PDFs"""
        try:
            self.driver.get(section_url)
            time.sleep(2)  # Wait for page load
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            if section_type == 'videos':
                # Extract videos from this section
                videos = self._extract_table_videos(soup, section_url)
                videos.extend(self.extract_videos_and_transcripts(soup, section_url))
                return videos
            elif section_type == 'pdfs':
                # Extract PDFs from this section  
                pdfs = self._extract_section_pdfs(soup, section_url)
                return pdfs
            else:
                return []
                
        except Exception as e:
            print(f"Error scraping section {section_url}: {e}")
            return []
    
    def _extract_table_videos(self, soup, course_url: str) -> List[Dict[str, Any]]:
        """Extract videos from table structures (common in MIT OCW video lectures)"""
        videos = []
        
        try:
            # Look for tables containing video content
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    
                    # Look for cells that contain video indicators
                    for cell in cells:
                        cell_text = cell.get_text().strip()
                        
                        # Check if this looks like a video entry
                        if self._is_video_table_entry(cell_text):
                            video_data = self._extract_video_from_table_cell(cell, cell_text, course_url)
                            if video_data:
                                videos.append(video_data)
                                
            # Also look for structured lists that might contain videos
            video_lists = soup.find_all(['ul', 'ol'])
            for video_list in video_lists:
                list_items = video_list.find_all('li')
                
                for item in list_items:
                    item_text = item.get_text().strip()
                    if self._is_video_table_entry(item_text):
                        video_data = self._extract_video_from_table_cell(item, item_text, course_url)
                        if video_data:
                            videos.append(video_data)
                            
        except Exception as e:
            print(f"Error extracting table videos: {e}")
            
        return videos
    
    def _is_video_table_entry(self, text: str) -> bool:
        """Check if a table cell or list item contains video content"""
        text_lower = text.lower()
        
        # Look for video indicators with duration patterns
        has_duration = bool(re.search(r'\(\d{1,2}:\d{2}\)|\(\d{1,3}:\d{2}\)', text))
        
        # Look for lecture/lesson patterns
        has_lecture_pattern = bool(re.search(r'l\d+\.\d+|lecture \d+|lesson \d+', text_lower))
        
        # Look for video keywords
        video_keywords = ['video', 'watch', 'play', 'stream', 'media']
        has_video_keywords = any(keyword in text_lower for keyword in video_keywords)
        
        # Must have duration OR (lecture pattern AND not be excluded terms)
        excluded_terms = ['notes', 'pdf', 'handout', 'assignment', 'reading']
        has_excluded = any(term in text_lower for term in excluded_terms)
        
        return (has_duration or (has_lecture_pattern and not has_excluded) or has_video_keywords) and not has_excluded
    
    def _extract_video_from_table_cell(self, cell_element, cell_text: str, course_url: str) -> Optional[Dict[str, Any]]:
        """Extract video information from a table cell or list item"""
        try:
            # Extract duration if present
            duration_match = re.search(r'\((\d{1,2}:\d{2}|\d{1,3}:\d{2})\)', cell_text)
            duration = duration_match.group(1) if duration_match else None
            
            # Extract title (remove duration)
            title = re.sub(r'\(\d{1,2}:\d{2}\)|\(\d{1,3}:\d{2}\)', '', cell_text).strip()
            
            # Look for video link in the cell
            video_link = cell_element.find('a')
            video_url = None
            
            if video_link and video_link.get('href'):
                video_url = urljoin(course_url, video_link['href'])
            
            # Generate video ID
            video_id = hashlib.md5((course_url + title).encode()).hexdigest()[:12]
            
            return {
                'type': 'video',
                'video_id': video_id,
                'title': title,
                'duration': duration,
                'url': video_url or course_url,
                'source': 'table_extraction',
                'transcript': None,
                'description': cell_text[:200] if len(cell_text) > len(title) else None
            }
            
        except Exception as e:
            print(f"Error extracting video from cell: {e}")
            return None
    
    def _extract_section_pdfs(self, soup, section_url: str) -> List[Dict[str, Any]]:
        """Extract PDFs from a section page (like lecture notes)"""
        pdfs = []
        
        try:
            # Look for direct PDF links
            pdf_links = soup.find_all('a', href=lambda x: x and ('.pdf' in x.lower() or 'resources/' in x.lower()))
            
            for link in pdf_links:
                href = link.get('href', '')
                if not href:
                    continue
                    
                pdf_data = self._process_pdf_link(link, section_url)
                if pdf_data:
                    pdfs.append(pdf_data)
                    
        except Exception as e:
            print(f"Error extracting section PDFs: {e}")
            
        return pdfs
    
    def _extract_meaningful_pdf_title(self, link, href: str) -> str:
        """Extract meaningful title for PDF from link and context"""
        # Try to get title from link text
        pdf_title = link.text.strip()
        
        if not pdf_title or len(pdf_title) < 3:
            # Try to get title from parent elements
            parent = link.parent
            for _ in range(3):  # Go up to 3 levels
                if parent:
                    parent_text = parent.get_text().strip()
                    # Clean up the text
                    parent_text = re.sub(r'\s+', ' ', parent_text)
                    if 5 < len(parent_text) < 100 and parent_text != pdf_title:
                        pdf_title = parent_text
                        break
                    parent = parent.parent
                else:
                    break
        
        # If still no title, try to extract from href
        if not pdf_title or len(pdf_title) < 3:
            # Extract filename from URL
            filename = href.split('/')[-1]
            if filename and '.pdf' in filename.lower():
                # Remove .pdf and clean up
                pdf_title = filename.replace('.pdf', '').replace('.PDF', '')
                pdf_title = re.sub(r'[_-]', ' ', pdf_title)
                pdf_title = ' '.join(word.capitalize() for word in pdf_title.split())
        
        # Final fallback
        if not pdf_title or len(pdf_title) < 3:
            pdf_title = "MIT OCW Document"
        
        # Clean up title
        pdf_title = re.sub(r'\s+', ' ', pdf_title.strip())
        # Remove common prefixes
        pdf_title = re.sub(r'^(PDF|Download|File):\s*', '', pdf_title, flags=re.IGNORECASE)
        
        return pdf_title[:80]  # Limit length
    
    def _generate_pdf_filename(self, title: str, pdf_id: str) -> str:
        """Generate meaningful filename for PDF"""
        # Clean title for filename
        clean_title = re.sub(r'[^\w\s-]', '', title)
        clean_title = re.sub(r'\s+', '_', clean_title.strip())
        clean_title = clean_title[:40]  # Limit length
        
        if clean_title and len(clean_title) > 3:
            return f"{clean_title}_{pdf_id}.pdf"
        else:
            return f"MIT_OCW_Document_{pdf_id}.pdf"
    
    def _get_pdf_size(self, pdf_url: str) -> float:
        """Get PDF file size in MB"""
        try:
            response = requests.head(pdf_url, timeout=10)
            if 'content-length' in response.headers:
                size_bytes = int(response.headers['content-length'])
                return round(size_bytes / (1024 * 1024), 2)
            return 0.0
        except Exception:
            return 0.0
    
    def _download_and_extract_pdf(self, pdf_url: str, pdf_id: str, pdf_data: Dict[str, Any]) -> Tuple[Optional[Path], Optional[str]]:
        """Download PDF and extract text content"""
        try:
            # Download PDF
            response = requests.get(pdf_url, timeout=30, stream=True)
            response.raise_for_status()
            
            # Use meaningful filename from pdf_data
            pdf_filename = pdf_data.get('filename', f'{pdf_id}.pdf')
            pdf_path = self.local_storage / "pdfs" / pdf_filename
            
            with open(pdf_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Extract text content
            text_content = self._extract_pdf_text(pdf_path)
            
            # Upload to MinIO if available
            if self.minio_client:
                try:
                    # Use meaningful filename from PDF data
                    pdf_filename = pdf_data.get('filename', f"{pdf_id}.pdf")
                    pdf_title = pdf_data.get('title', 'Unknown')
                    minio_path = f"bronze/mit_ocw/pdfs/{pdf_filename}"
                    self.minio_client.fput_object(self.bucket, minio_path, str(pdf_path))
                    print(f"Uploaded PDF to MinIO: {minio_path} ('{pdf_title}')")
                except Exception as e:
                    print(f"WARNING: Failed to upload PDF to MinIO: {e}")
            
            return pdf_path, text_content
            
        except Exception as e:
            print(f"WARNING: Error downloading/extracting PDF {pdf_id}: {e}")
            return None, None
    
    def _extract_pdf_text(self, pdf_path: Path) -> Optional[str]:
        """Extract text content from PDF file"""
        if not PDF_PROCESSING_AVAILABLE:
            print("PDF text extraction skipped - libraries not available")
            return None
            
        try:
            text_content = []
            
            # Try pdfplumber first (better for complex layouts)
            if pdfplumber:
                with pdfplumber.open(pdf_path) as pdf:
                    for page in pdf.pages[:3]:  # Limit to first 3 pages for efficiency
                        page_text = page.extract_text()
                        if page_text and page_text.strip():
                            text_content.append(page_text.strip())
            
            if text_content:
                full_text = '\n'.join(text_content)
                return full_text[:3000] if len(full_text) > 3000 else full_text
            
            # Fallback to PyPDF2
            if PyPDF2:
                with open(pdf_path, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    for page_num in range(min(3, len(pdf_reader.pages))):
                        page = pdf_reader.pages[page_num]
                        page_text = page.extract_text()
                        if page_text and page_text.strip():
                            text_content.append(page_text.strip())
                
                if text_content:
                    full_text = '\n'.join(text_content)
                    return full_text[:3000] if len(full_text) > 3000 else full_text
            
            return None
            
        except Exception as e:
            print(f"WARNING: Error extracting text from PDF: {e}")
            return None
    
    def save_to_bronze(self, data: List[Dict[str, Any]]):
        """Save data to bronze layer (MinIO) with multimedia content"""
        if not data:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Calculate statistics
        total_courses = len(data)
        total_videos = sum(len(course.get('videos', [])) for course in data)
        total_pdfs = sum(len(course.get('pdfs', [])) for course in data)
        total_transcripts = sum(len([v for v in course.get('videos', []) if v.get('transcript')]) for course in data)
        total_downloaded_pdfs = sum(len([p for p in course.get('pdfs', []) if p.get('downloaded', False)]) for course in data)
        
        print("\nScraping Summary:")
        print(f"   Courses: {total_courses}")
        print(f"   Videos: {total_videos} (transcripts: {total_transcripts})")
        print(f"   PDFs: {total_pdfs} (downloaded: {total_downloaded_pdfs})")
        
        # Save main course data
        filename = f"bronze/mit_ocw/courses_{timestamp}.json"
        local_path = f"/tmp/{filename.replace('/', '_')}"
        
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # Upload to MinIO if available
        if self.minio_client:
            try:
                self.minio_client.fput_object(self.bucket, filename, local_path)
                print(f"Saved courses to MinIO: {filename}")
                
                # Also save transcripts separately for easier access
                if total_transcripts > 0:
                    self._save_transcripts_separately(data, timestamp)
                
                # Save multimedia metadata
                self._save_multimedia_metadata(data, timestamp)
                
                os.remove(local_path)
            except Exception as e:
                print(f"ERROR: MinIO upload failed: {e}")
                print(f"📁 Data saved locally: {local_path}")
        else:
            print(f"📁 Data saved locally: {local_path}")
    
    def _save_transcripts_separately(self, data: List[Dict[str, Any]], timestamp: str):
        """Save video transcripts as separate files for easier search"""
        try:
            transcripts_data = []
            
            for course in data:
                course_id = course.get('id')
                course_title = course.get('title', 'Unknown')
                
                for video in course.get('videos', []):
                    if video.get('transcript'):
                        transcript_record = {
                            'course_id': course_id,
                            'course_title': course_title,
                            'course_url': course.get('url'),
                            'video_id': video.get('video_id'),
                            'video_title': video.get('title'),
                            'video_url': video.get('url'),
                            'video_type': video.get('type'),
                            'transcript': video.get('transcript'),
                            'scraped_at': datetime.now().isoformat()
                        }
                        transcripts_data.append(transcript_record)
            
            if transcripts_data:
                filename = f"bronze/mit_ocw/transcripts_{timestamp}.json"
                local_path = f"/tmp/{filename.replace('/', '_')}"
                
                with open(local_path, 'w', encoding='utf-8') as f:
                    json.dump(transcripts_data, f, ensure_ascii=False, indent=2)
                
                self.minio_client.fput_object(self.bucket, filename, local_path)
                print(f"Saved transcripts to MinIO: {filename} ({len(transcripts_data)} transcripts)")
                os.remove(local_path)
                
        except Exception as e:
            print(f"WARNING: Error saving transcripts: {e}")
    
    def _save_multimedia_metadata(self, data: List[Dict[str, Any]], timestamp: str):
        """Save multimedia metadata summary"""
        try:
            multimedia_summary = {
                'scrape_timestamp': timestamp,
                'total_courses': len(data),
                'courses_with_multimedia': len([c for c in data if c.get('multimedia_stats', {}).get('has_multimedia', False)]),
                'video_summary': {
                    'total_videos': sum(len(course.get('videos', [])) for course in data),
                    'youtube_videos': sum(len([v for v in course.get('videos', []) if v.get('type') == 'youtube_video']) for course in data),
                    'native_videos': sum(len([v for v in course.get('videos', []) if v.get('type') == 'native_video']) for course in data),
                    'videos_with_transcripts': sum(len([v for v in course.get('videos', []) if v.get('transcript')]) for course in data)
                },
                'pdf_summary': {
                    'total_pdfs': sum(len(course.get('pdfs', [])) for course in data),
                    'downloaded_pdfs': sum(len([p for p in course.get('pdfs', []) if p.get('downloaded', False)]) for course in data),
                    'total_size_mb': sum(sum(p.get('size_mb', 0) for p in course.get('pdfs', [])) for course in data),
                    'failed_downloads': sum(len([p for p in course.get('pdfs', []) if p.get('error')]) for course in data)
                },
                'scraper_config': {
                    'video_scraping_enabled': self.enable_video_scraping,
                    'pdf_scraping_enabled': self.enable_pdf_scraping,
                    'pdf_downloads_enabled': self.download_pdfs,
                    'max_pdf_size_mb': self.max_pdf_size_mb
                }
            }
            
            filename = f"bronze/mit_ocw/multimedia_summary_{timestamp}.json"
            local_path = f"/tmp/{filename.replace('/', '_')}"
            
            with open(local_path, 'w', encoding='utf-8') as f:
                json.dump(multimedia_summary, f, ensure_ascii=False, indent=2)
            
            self.minio_client.fput_object(self.bucket, filename, local_path)
            print(f"Saved multimedia summary to MinIO: {filename}")
            os.remove(local_path)
            
        except Exception as e:
            print(f"WARNING: Error saving multimedia summary: {e}")
    
    def run(self):
        """Main execution function with multimedia support"""
        print("Starting MIT OCW scraping with multimedia support...")
        
        try:
            # Get course list
            courses = self.scrape_course_list()
            if not courses:
                print("ERROR: No courses found")
                return
            
            # Scrape course details with multimedia
            scraped_data = []
            failed_courses = []
            
            for idx, course in enumerate(courses):
                print(f"\nScraping course {idx+1}/{len(courses)}: {course['title']}")
                
                try:
                    details = self.scrape_course_details(course)
                    if details:
                        scraped_data.append(details)
                    else:
                        failed_courses.append(course['title'])
                except Exception as e:
                    print(f"   ERROR: Failed: {e}")
                    failed_courses.append(course['title'])
            
            # Save all data at once
            if scraped_data:
                self.save_to_bronze(scraped_data)
            
            # Report results
            print("\nMIT OCW scraping completed!")
            print(f"   Successful: {len(scraped_data)}/{len(courses)} courses")
            if failed_courses:
                print(f"   Failed: {len(failed_courses)} courses")
                for failed in failed_courses[:5]:  # Show first 5 failures
                    print(f"      - {failed}")
                if len(failed_courses) > 5:
                    print(f"      ... and {len(failed_courses) - 5} more")
        
        except Exception as e:
            print(f"ERROR: Critical error during scraping: {e}")
        
        finally:
            # Cleanup local storage
            try:
                if self.local_storage.exists():
                    print("\nCleaning up local storage...")
                    import shutil
                    shutil.rmtree(self.local_storage, ignore_errors=True)
                    print("   Cleanup completed")
            except Exception as e:
                print(f"   WARNING: Cleanup warning: {e}")
    
    def get_multimedia_stats(self) -> Dict[str, Any]:
        """Get current multimedia processing capabilities"""
        return {
            'video_processing_available': VIDEO_PROCESSING_AVAILABLE,
            'pdf_processing_available': PDF_PROCESSING_AVAILABLE,
            'minio_available': MINIO_AVAILABLE,
            'capabilities': {
                'youtube_transcripts': VIDEO_PROCESSING_AVAILABLE,
                'pdf_text_extraction': PDF_PROCESSING_AVAILABLE,
                'pdf_downloads': self.download_pdfs,
                'video_detection': self.enable_video_scraping,
                'pdf_detection': self.enable_pdf_scraping
            },
            'limits': {
                'max_pdf_size_mb': self.max_pdf_size_mb,
                'max_documents': self.max_documents
            }
        }

    def save_to_minio(self, documents: List[Dict[str, Any]], source: str = "mit_ocw", logical_date: str = None, file_type: str = "courses"):
        """Lưu dữ liệu vào MinIO với đường dẫn có tổ chức"""
        if not self.minio_enable or not self.minio_client or not documents:
            print("MinIO không được bật hoặc không có dữ liệu để lưu")
            return ""
        
        if logical_date is None:
            logical_date = datetime.now().strftime("%Y-%m-%d")
        
        # Tạo đường dẫn có tổ chức cho MIT OCW
        timestamp = int(time.time())
        object_name = f"{source}/{logical_date}/{file_type}_{timestamp}.jsonl"
        
        # Tạo temporary file
        os.makedirs('/tmp', exist_ok=True) if os.name != 'nt' else os.makedirs('temp', exist_ok=True)
        tmp_dir = '/tmp' if os.name != 'nt' else 'temp'
        tmp_path = os.path.join(tmp_dir, f"{source}_{file_type}_{timestamp}.jsonl")
        
        try:
            # Ghi dữ liệu vào temp file
            with open(tmp_path, 'w', encoding='utf-8') as f:
                for doc in documents:
                    f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            
            # Upload lên MinIO với organized path
            self.minio_client.fput_object(self.minio_bucket, object_name, tmp_path)
            os.remove(tmp_path)
            print(f"[MinIO] MIT OCW saved: s3://{self.minio_bucket}/{object_name}")
            return object_name
        except Exception as e:
            print(f"[MinIO] Lỗi khi lưu MIT OCW: {e}")
            # Clean up temp file if upload failed
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return ""
    
    def list_minio_backups(self, source: str = "mit_ocw"):
        """Liệt kê các file backup trong MinIO"""
        if not self.minio_enable or not self.minio_client:
            print("MinIO không được bật")
            return []
        
        try:
            objects = self.minio_client.list_objects(self.minio_bucket, prefix=f"{source}/", recursive=True)
            backups = []
            for obj in objects:
                backup_info = {
                    'object_name': obj.object_name,
                    'size': obj.size,
                    'last_modified': obj.last_modified.isoformat() if obj.last_modified else None,
                    'etag': obj.etag
                }
                backups.append(backup_info)
                print(f"Backup: {obj.object_name} ({obj.size} bytes, {obj.last_modified})")
            
            print(f"Tìm thấy {len(backups)} backup files")
            return backups
        except Exception as e:
            print(f"[MinIO] Lỗi khi liệt kê backup: {e}")
            return []
    
    def restore_from_minio(self, object_name: str):
        """Khôi phục dữ liệu từ MinIO backup"""
        if not self.minio_enable or not self.minio_client:
            print("MinIO không được bật")
            return None
        
        try:
            # Tạo thư mục restore nếu chưa có
            restore_dir = os.path.join(self.output_dir, "restored")
            os.makedirs(restore_dir, exist_ok=True)
            
            # Download file từ MinIO
            local_path = os.path.join(restore_dir, os.path.basename(object_name))
            self.minio_client.fget_object(self.minio_bucket, object_name, local_path)
            
            # Đọc và parse JSON
            documents = []
            with open(local_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        documents.append(json.loads(line))
            
            print(f"[MinIO] Đã khôi phục {len(documents)} tài liệu từ {object_name}")
            return {
                'documents': documents,
                'restored_from': object_name,
                'restored_at': datetime.now().isoformat(),
                'local_path': local_path
            }
        except Exception as e:
            print(f"[MinIO] Lỗi khi khôi phục từ MinIO: {e}")
            return None

class MITOCWScraperStandalone:
    """Wrapper class for backward compatibility"""
    
    def __init__(self):
        self.scraper = MITOCWScraper(
            delay=float(os.getenv('SCRAPING_DELAY_BASE', 2.0)),
            use_selenium=True,
            output_dir="scraped_data/mit_ocw",
            batch_size=int(os.getenv('BATCH_SIZE', 25)),
            max_documents=int(os.getenv('MAX_DOCUMENTS', 100)) if os.getenv('MAX_DOCUMENTS') else None
        )
        
    def run(self):
        """Main execution function"""
        print("Starting MIT OCW scraping with multimedia support...")
        
        try:
            # Run scraping
            documents = self.scraper.scrape_with_selenium()
            
            if documents:
                # Save to MinIO if enabled
                if self.scraper.minio_enable:
                    self.scraper.save_to_minio(documents)
                
                print(f"\nMIT OCW scraping completed!")
                print(f"Total documents: {len(documents)}")
                
                # Show multimedia stats
                total_videos = sum(len(doc.get('videos', [])) for doc in documents)
                total_pdfs = sum(len(doc.get('pdfs', [])) for doc in documents)
                print(f"Multimedia content: {total_videos} videos, {total_pdfs} PDFs")
            else:
                print("No documents scraped")
                
        except Exception as e:
            print(f"ERROR: Critical error during scraping: {e}")
        
        finally:
            # Cleanup
            self.scraper.cleanup()
            
            # Cleanup local storage
            try:
                if self.scraper.local_storage.exists():
                    print("\nCleaning up local storage...")
                    import shutil
                    shutil.rmtree(self.scraper.local_storage, ignore_errors=True)
                    print("   Cleanup completed")
            except Exception as e:
                print(f"   WARNING: Cleanup warning: {e}")

def main():
    """Entry point for standalone execution"""
    scraper = MITOCWScraperStandalone()
    scraper.run()

if __name__ == "__main__":
    main()

