#!/usr/bin/env python3
"""
Optimized version of MIT OCW scraper with reduced redundancy.
"""

import os
import json
import time
import hashlib
import requests
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Video processing imports
try:
    import yt_dlp
    from youtube_transcript_api import YouTubeTranscriptApi
    VIDEO_PROCESSING_AVAILABLE = True
except ImportError:
    VIDEO_PROCESSING_AVAILABLE = False

# PDF processing imports
try:
    import PyPDF2
    try:
        import pdfplumber
    except ImportError:
        pdfplumber = None
    PDF_PROCESSING_AVAILABLE = True
except ImportError:
    PDF_PROCESSING_AVAILABLE = False
    PyPDF2 = None
    pdfplumber = None

# MinIO imports
try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

class MITOCWScraper:
    """Optimized MIT OCW scraper for bronze layer"""
    
    def __init__(self, delay=2, use_selenium=True, output_dir="scraped_data/mit_ocw", 
                 batch_size=25, max_documents=None, **kwargs):
        self.base_url = "https://ocw.mit.edu"
        self.source = "mit_ocw"
        self.delay = delay
        self.use_selenium = True  # Force Selenium for MIT OCW
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.max_documents = max_documents
        self.driver = None
        
        # Session for requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.82 Safari/537.36'
        })
        
        # Multimedia settings - chá»‰ focus vÃ o lecture notes
        self.enable_video_scraping = os.getenv('ENABLE_VIDEO_SCRAPING', '0') == '1'  # Táº¯t video máº·c Ä‘á»‹nh
        self.enable_pdf_scraping = os.getenv('ENABLE_PDF_SCRAPING', '0') == '1'
        self.max_pdf_size_mb = float(os.getenv('MAX_PDF_SIZE_MB', '50.0'))  # TÄƒng limit cho lecture notes
        self.download_pdfs = os.getenv('DOWNLOAD_PDFS', '1') == '1'
        
        # Smart download configuration 
        self.download_strategy = os.getenv('DOWNLOAD_STRATEGY', 'selective')
        self.important_categories = set(os.getenv('IMPORTANT_CATEGORIES', 'lecture_notes,textbook,handout,reading').split(','))
        self.skip_categories = set(os.getenv('SKIP_CATEGORIES', 'exam,assignment,quiz').split(','))
        
        # Temporary directory for processing (will be cleaned up)
        # Only used for temporary processing before uploading to MinIO
        self.temp_storage = Path("/tmp/mit_ocw_temp")
        self.temp_storage.mkdir(exist_ok=True, parents=True)

        self.output_path = Path(self.output_dir)
        self.output_path.mkdir(parents=True, exist_ok=True)

        # MinIO setup
        self._setup_minio()
        self.existing_course_ids = self._load_existing_course_ids()
        if self.existing_course_ids:
            print(f"[MIT OCW] Loaded {len(self.existing_course_ids)} existing courses; duplicates will be skipped.")

        # Báº¯t buá»™c setup Selenium
        self._setup_selenium()
        
        # Kiá»ƒm tra Selenium Ä‘Ã£ setup thÃ nh cÃ´ng
        if not self.driver:
            raise Exception("Selenium WebDriver is required for MIT OCW scraping but failed to initialize")
        
        print(f"MIT OCW Scraper initialized - Max docs: {self.max_documents}")
        print(f"Video: {'ON' if self.enable_video_scraping else 'OFF'}, PDF: {'ON' if self.enable_pdf_scraping else 'OFF'}")
    
    def _setup_minio(self):
        """Setup MinIO client"""
        self.minio_client = None
        self.minio_bucket = os.getenv('MINIO_BUCKET', 'oer-lakehouse')
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
                print(f"[MinIO] Error: {e}")
                self.minio_enable = False
    
    def _setup_selenium(self):
        """Setup Selenium WebDriver - Required for MIT OCW"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--disable-javascript")  # Try without JS first
            
            chromedriver_paths = ['/usr/local/bin/chromedriver', '/usr/bin/chromedriver', 'chromedriver']
            
            for path in chromedriver_paths:
                try:
                    print(f"Trying ChromeDriver at: {path}")
                    if os.path.exists(path) or path == 'chromedriver':
                        service = Service(path)
                        self.driver = webdriver.Chrome(service=service, options=chrome_options)
                        print(f"Selenium successfully initialized with: {path}")
                        return
                except Exception as e:
                    print(f"Failed with {path}: {e}")
                    continue
            
            # If all paths failed, raise exception
            raise Exception("Could not initialize ChromeDriver from any path")
                    
        except Exception as e:
            print(f"Selenium setup failed: {e}")
            # Do NOT set use_selenium = False, raise exception instead
            raise Exception(f"Selenium WebDriver is required for MIT OCW but failed to initialize: {e}")
    
    def cleanup(self):
        """Cleanup resources"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                print(f"Error closing WebDriver: {e}")
        
        # Clean up temporary files
        if hasattr(self, 'temp_storage') and self.temp_storage.exists():
            try:
                import shutil
                shutil.rmtree(self.temp_storage, ignore_errors=True)
                print("Cleaned up temporary storage")
            except Exception as e:
                print(f"Error cleaning temp storage: {e}")
    
    def scrape_with_selenium(self) -> List[Dict[str, Any]]:
        """Legacy method for compatibility with existing DAG"""
        return self.scrape()
    
    def scrape(self) -> List[Dict[str, Any]]:
        """Main scraping method"""
        print("[MIT OCW] Starting scraper...")
        
        try:
            # Get course URLs
            course_urls = list(self._get_course_urls())
            print(f"Found {len(course_urls)} courses")
            
            if self.max_documents:
                course_urls = course_urls[:self.max_documents]
                print(f"Limited to {len(course_urls)} courses for processing")
            
            # Scrape courses
            documents = []
            skipped_courses = 0
            total_candidates = len(course_urls)
            for i, url in enumerate(course_urls, 1):
                course_hash = self.create_document_id(self.source, url)
                if course_hash in self.existing_course_ids:
                    skipped_courses += 1
                    print(f"[{i}/{total_candidates}] Skipping already scraped course: {url}")
                    continue

                print(f"[{i}/{total_candidates}] Scraping: {url}")
                
                course_data = self._scrape_course(url)
                if course_data:
                    documents.append(course_data)
                    self.existing_course_ids.add(course_data.get('id', course_hash))
                    
                    # Log progress
                    videos = course_data.get('videos', [])
                    pdfs = course_data.get('pdfs', [])
                    videos_with_transcripts = [v for v in videos if v.get('transcript')]
                    
                    print(f"   Videos: {len(videos)} (transcripts: {len(videos_with_transcripts)})")
                    
                    if pdfs:
                        downloaded_pdfs = [p for p in pdfs if p.get('downloaded')]
                        total_size = sum(p.get('size_mb', 0) for p in downloaded_pdfs)
                        categories = {}
                        for pdf in pdfs:
                            cat = pdf.get('category', 'unknown')
                            categories[cat] = categories.get(cat, 0) + 1
                        
                        print(f"   PDFs: {len(pdfs)} (downloaded: {len(downloaded_pdfs)}, size: {total_size:.1f}MB)")
                        if categories:
                            cat_str = ", ".join(f"{k}: {v}" for k, v in categories.items())
                            print(f"   PDF Categories: {cat_str}")
                    
                    title = course_data.get('title', 'Unknown')
                    if len(title) > 50:
                        title = title[:47] + "..."
                    print(f"Success: {title}")
                else:
                    print(f"Failed to scrape course: {url}")
                time.sleep(self.delay)

            if skipped_courses:
                print(f"[MIT OCW] Skipped {skipped_courses} courses already processed.")
            
            # Print summary but don't save yet - let DAG handle saving
            total_courses = len(documents)
            total_videos = sum(len(course.get('videos', [])) for course in documents)
            total_pdfs = sum(len(course.get('pdfs', [])) for course in documents)
            videos_with_transcripts = sum(len([v for v in course.get('videos', []) if v.get('transcript')]) for course in documents)
            
            print(f"\n[BRONZE LAYER SUMMARY]")
            print(f"Courses: {total_courses}")
            print(f"Videos: {total_videos} (transcripts: {videos_with_transcripts})")
            print(f"PDFs: {total_pdfs}")
        
            
            print(f"[MIT OCW] Scraping completed. Total courses: {len(documents)}")
            return documents
            
        except Exception as e:
            print(f"[MIT OCW] Scraping error: {e}")
            return []
        finally:
            self.cleanup()
    
    def _get_course_urls(self) -> set:
        """Get course URLs using advanced method with fallback"""
        
        try:
            search_url = "https://ocw.mit.edu/search/?s=department_course_numbers.sort_coursenum&type=course"
            print(f"Getting courses from: {search_url}")
            
            self.driver.get(search_url)
            time.sleep(3)
            
            # Scroll to load more courses
            self._progressive_scroll()
            
            # Extract course URLs
            course_urls = self._extract_course_urls()
            print(f"Found {len(course_urls)} course URLs")
            
            return course_urls
            
        except Exception as e:
            print(f"Error getting course URLs: {e}")
            return set()
    
    def _progressive_scroll(self):
        """Scroll page to load all courses"""
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            scroll_attempts = 0
            max_scrolls = 200
            
            while scroll_attempts < max_scrolls:
                # Scroll down
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Check if new content loaded
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                    
                last_height = new_height
                scroll_attempts += 1
            
            print(f"Scroll completed after {scroll_attempts} attempts")
            
        except Exception as e:
            print(f"Error during scrolling: {e}")
    
    def _extract_course_urls(self) -> set:
        """Extract course URLs from search page"""
        course_urls = set()
        
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Find course links
            course_links = soup.find_all('a', href=lambda x: x and '/courses/' in x)
            
            for link in course_links:
                href = link.get('href', '')
                if self._is_valid_course_url(href):
                    full_url = urljoin(self.base_url, href)
                    course_urls.add(full_url)
            
            return course_urls
            
        except Exception as e:
            print(f"Error extracting course URLs: {e}")
            return set()
    
    def _is_valid_course_url(self, url: str) -> bool:
        """Check if URL is a valid course URL"""
        if not url or '/courses/' not in url:
            return False
        
        # Skip unwanted patterns
        skip_patterns = ['/about/', '/instructor-insights/', '/download/', '/calendar/']
        return not any(pattern in url for pattern in skip_patterns)
    
    def _scrape_course(self, url: str) -> Dict[str, Any]:
        """Scrape individual course with deep multimedia extraction"""
        try:
            soup = self.get_page(url)
            if not soup:
                return None
            
            # Extract basic course info
            course_info = self._extract_course_info(soup, url)
            
            # Extract multimedia content with deep navigation
            videos = []
            pdfs = []
            
            if self.enable_video_scraping or self.enable_pdf_scraping:
                videos, pdfs = self._extract_multimedia_deep(url, soup, course_info)
            
            # Update course info with multimedia content
            course_info['videos'] = videos
            course_info['pdfs'] = pdfs
            course_info['has_videos'] = len(videos) > 0
            course_info['has_pdfs'] = len(pdfs) > 0
            course_info['multimedia_count'] = len(videos) + len(pdfs)
            
            # Add summary info
            course_info['video_count'] = len(videos)
            course_info['pdf_count'] = len(pdfs)
            course_info['lecture_notes_count'] = len([p for p in pdfs if p.get('category') == 'lecture_notes'])
            
            return course_info
            
        except Exception as e:
            print(f"Error scraping course {url}: {e}")
            return None
    
    def _extract_multimedia_deep(self, course_url: str, main_soup: BeautifulSoup, course_info: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Extract multimedia content by navigating to resource pages"""
        videos = []
        pdfs = []
        
        try:
            # Find resource links from main course page
            resource_links = self._find_resource_links(main_soup, course_url)
            
            for link_type, links in resource_links.items():
                for link_url in links:
                    try:
                        print(f"  Checking {link_type}: {link_url}")
                        
                        # Get resource page content
                        resource_soup = self.get_page(link_url)
                        if not resource_soup:
                            continue
                        
                        # Extract content based on type
                        if self.enable_video_scraping and link_type in ['video_galleries', 'lectures']:
                            page_videos = self._extract_videos_from_page(resource_soup, link_url, course_info)
                            videos.extend(page_videos)
                        
                        # Láº¥y PDF tá»« lectures, lecture_notes, vÃ  materials (cÃ³ thá»ƒ chá»©a lecture notes)
                        if self.enable_pdf_scraping and link_type in ['lectures', 'lecture_notes', 'materials']:
                            page_pdfs = self._extract_pdfs_from_page(resource_soup, link_url, course_info)
                            pdfs.extend(page_pdfs)
                        
                        time.sleep(1)  # Be respectful to the server
                        
                    except Exception as e:
                        print(f"    Error processing {link_url}: {e}")
                        continue
            
            # Also check main page for any directly embedded content
            if self.enable_video_scraping:
                main_videos = self._extract_videos(main_soup, course_url, course_info)
                videos.extend(main_videos)
            
            if self.enable_pdf_scraping:
                main_pdfs = self._extract_pdfs(main_soup, course_url, course_info)
                pdfs.extend(main_pdfs)
            
        except Exception as e:
            print(f"Error in deep multimedia extraction: {e}")
        
        return videos, pdfs
    
    def _find_resource_links(self, soup: BeautifulSoup, course_url: str) -> Dict[str, List[str]]:
        """Find links to resource pages containing multimedia content"""
        resource_links = {
            'video_galleries': [],
            'lectures': [],
            'assignments': [],
            'readings': [],
            'lecture_notes': [],
            'materials': []
        }
        
        try:
            # Find all links
            all_links = soup.find_all('a', href=True)
            
            for link in all_links:
                href = link.get('href', '')
                text = link.get_text().lower().strip()
                
                if not href:
                    continue
                
                # Convert relative URLs to absolute
                full_url = urljoin(course_url, href)
                
                # Categorize links
                if 'video' in href.lower() or 'video' in text:
                    if 'gallery' in href.lower() or 'gallery' in text:
                        resource_links['video_galleries'].append(full_url)
                    else:
                        resource_links['lectures'].append(full_url)
                
                elif 'lecture' in href.lower() or 'lecture' in text:
                    resource_links['lectures'].append(full_url)
                
                elif 'assignment' in href.lower() or 'assignment' in text:
                    resource_links['assignments'].append(full_url)
                
                elif 'reading' in href.lower() or 'reading' in text:
                    resource_links['readings'].append(full_url)
                
                elif any(term in href.lower() for term in ['note', 'handout', 'material']):
                    resource_links['lecture_notes'].append(full_url)
                
                elif any(term in text for term in ['note', 'handout', 'material', 'download', 'resource']):
                    resource_links['materials'].append(full_url)
        
        except Exception as e:
            print(f"Error finding resource links: {e}")
        
        # Remove duplicates
        for key in resource_links:
            resource_links[key] = list(set(resource_links[key]))
        
        return resource_links
    
    def _extract_videos_from_page(self, soup: BeautifulSoup, page_url: str, course_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract videos from a video gallery or lecture page"""
        videos = []
        
        try:
            # Try different video extraction methods
            
            # Method 1: YouTube iframes
            youtube_iframes = soup.find_all('iframe', src=lambda x: x and 'youtube.com' in x)
            for iframe in youtube_iframes:
                video_data = self._process_video_element(iframe, 'youtube', page_url, {})
                if video_data:
                    videos.append(video_data)
            
            # Method 2: Video elements
            video_elements = soup.find_all(['video', 'source'])
            for elem in video_elements:
                video_data = self._process_video_element(elem, 'native', page_url, {})
                if video_data:
                    videos.append(video_data)
            
            # Method 3: Video file links
            video_extensions = ['.mp4', '.mov', '.avi', '.wmv', '.flv', '.webm']
            video_links = soup.find_all('a', href=lambda x: x and any(ext in x.lower() for ext in video_extensions))
            for link in video_links:
                video_data = self._process_video_element(link, 'link', page_url, {})
                if video_data:
                    videos.append(video_data)
            
            # Method 4: Look for embedded video players (data attributes, etc.)
            try:
                video_containers = soup.find_all(['div', 'section'])
                
                for container in video_containers:
                    if not hasattr(container, 'attrs') or not container.attrs:
                        continue
                    
                    # Look for data attributes that might contain video URLs
                    for attr, value in container.attrs.items():
                        if not isinstance(value, str):
                            continue
                        
                        if 'youtube.com' in value or any(ext in value.lower() for ext in video_extensions):
                            # Create a pseudo-element for processing
                            pseudo_element = type('Element', (), {
                                'get': lambda self, key, default='': value if key == 'src' or key == 'href' else default,
                                'get_text': lambda: container.get_text()[:50] if hasattr(container, 'get_text') else ''
                            })()
                            
                            element_type = 'youtube' if 'youtube.com' in value else 'link'
                            video_data = self._process_video_element(pseudo_element, element_type, page_url, {})
                            if video_data:
                                videos.append(video_data)
            except Exception as e:
                print(f"    Error in video container search: {e}")
        
        except Exception as e:
            print(f"Error extracting videos from page: {e}")
        
        return videos
    
    def _extract_pdfs_from_page(self, soup: BeautifulSoup, page_url: str, course_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract PDFs from an assignments or materials page"""
        pdfs = []
        seen_urls = set()
        
        try:
            # Find PDF links
            pdf_links = soup.find_all('a', href=lambda x: x and '.pdf' in x.lower())
            
            for link in pdf_links:
                href = link.get('href', '')
                if not href or href in seen_urls:
                    continue
                
                seen_urls.add(href)
                pdf_url = urljoin(page_url, href)
                
                # Process PDF link
                pdf_data = self._process_pdf_link(link, pdf_url, course_info)
                if pdf_data:
                    pdfs.append(pdf_data)
        
        except Exception as e:
            print(f"Error extracting PDFs from page: {e}")
        
        return pdfs
    
    def get_page(self, url: str, max_retries=3) -> BeautifulSoup:
        """Get page content with retry logic, using Selenium for JS-heavy pages"""
        for attempt in range(max_retries):
            try:
                if self.use_selenium and self.driver:
                    # Use Selenium for better JS rendering
                    self.driver.get(url)
                    # Wait for page to load
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    time.sleep(2)  # Additional wait for dynamic content
                    return BeautifulSoup(self.driver.page_source, 'html.parser')
                else:
                    # Fallback to requests
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                    return BeautifulSoup(response.content, 'html.parser')
                
            except Exception as e:
                print(f"Error getting page {url} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
    
    def _extract_course_info(self, soup, url: str) -> Dict[str, Any]:
        """Extract comprehensive course information for MIT OCW"""
        
        # Basic course identification
        course_number = self._extract_course_number_from_url(url)
        course_id = self.create_document_id(self.source, url)
        
        # Title extraction
        title = self._extract_title(soup)
        
        # Instructor extraction with multiple methods
        instructors = self._extract_instructors_comprehensive(soup)
        
        # Department/Subject extraction
        department = self._extract_department_comprehensive(soup, course_number)
        
        # Description extraction
        description = self._extract_description_comprehensive(soup)
        
        # Level determination
        level = self._extract_level_from_course_number(course_number)
        
        # Semester/Year extraction
        semester, year = self._extract_semester_year(course_number, soup)
        
        # Additional metadata
        tags = self._extract_course_tags(soup, title)
        difficulty = self._extract_difficulty_level(soup, course_number)
        prerequisites = self._extract_prerequisites(soup)
        
        return {
            # Core identifiers
            'id': course_id,
            'course_id': course_number,
            'course_number': course_number,
            'title': title,
            'url': url,
            
            # People
            'instructors': instructors,
            'instructor_count': len(instructors) if instructors else 0,
            
            # Classification
            'department': department,
            'subject': department,  # For compatibility with silver layer
            'level': level,
            'difficulty': difficulty,
            
            # Time information
            'semester': semester,
            'year': year,
            'academic_term': f"{semester} {year}" if semester != 'Unknown' and year != 'Unknown' else 'Unknown',
            
            # Content
            'description': description,
            'description_length': len(description) if description else 0,
            'prerequisites': prerequisites,
            'tags': tags,
            
            # Technical metadata
            'source': self.source,
            'scraped_at': datetime.now().isoformat(),
            'scraping_version': '2.0',
            
            # Placeholder for multimedia content
            'videos': [],
            'pdfs': [],
            'has_videos': False,
            'has_pdfs': False,
            'multimedia_count': 0
        }
    
    def _extract_course_number_from_url(self, url: str) -> str:
        """Extract course number from URL with better parsing"""
        import re
        match = re.search(r'courses/([^/]+)', url)
        if match:
            return match.group(1)
        return 'unknown-course'
    
    def _extract_title(self, soup) -> str:
        """Extract course title"""
        title_selectors = ['h1', 'title']
        for selector in title_selectors:
            element = soup.find(selector)
            if element:
                title = element.get_text().strip()
                # Clean title from page title format (e.g., "Course | Department | MIT OCW")
                if ' | ' in title:
                    title = title.split(' | ')[0].strip()
                if title and title != 'MIT OpenCourseWare':
                    return title
        return 'Unknown Course'
    
    def _extract_instructors_comprehensive(self, soup) -> List[str]:
        """Extract instructors using comprehensive methods"""
        instructors = []
        
        try:
            # Method 1: Look for instructor names in course info section
            course_info = soup.find('div', class_='course-info')
            if course_info:
                # Find prof/instructor links or text
                prof_elements = course_info.find_all(['a', 'div', 'span'], 
                    string=lambda x: x and any(prefix in x for prefix in ['Prof.', 'Dr.', 'Professor']))
                for elem in prof_elements:
                    name = elem.get_text().strip()
                    if name and len(name) < 100:  # Reasonable name length
                        instructors.append(name)
            
            # Method 2: Look for dedicated instructor sections
            instructor_headers = soup.find_all(['h2', 'h3', 'div'], 
                string=lambda x: x and 'instructor' in x.lower())
            for header in instructor_headers:
                # Look for names in siblings or children
                parent = header.parent if header.parent else header
                name_elements = parent.find_all(['a', 'span', 'div'])
                for elem in name_elements:
                    text = elem.get_text().strip()
                    if any(prefix in text for prefix in ['Prof.', 'Dr.']) and len(text) < 100:
                        instructors.append(text)
            
            # Method 3: Search page text for instructor patterns
            page_text = soup.get_text()
            import re
            patterns = [
                r'Prof\.\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                r'Dr\.\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                r'Professor\s+([A-Z][a-z]+\s+[A-Z][a-z]+)'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, page_text)
                for match in matches:
                    if match not in [i.replace('Prof. ', '').replace('Dr. ', '') for i in instructors]:
                        instructors.append(f'Prof. {match}')
            
            # Clean and deduplicate
            cleaned_instructors = []
            seen = set()
            for instructor in instructors:
                clean_name = instructor.strip()
                if clean_name and clean_name not in seen:
                    cleaned_instructors.append(clean_name)
                    seen.add(clean_name)
            
            return cleaned_instructors[:5]  # Limit to 5 instructors max
            
        except Exception as e:
            print(f"Error extracting instructors: {e}")
            return []
    
    def _extract_department_comprehensive(self, soup, course_number: str) -> str:
        """Extract department using multiple methods"""
        try:
            # Method 1: Department mapping from course number
            dept_map = {
                '1': 'Civil and Environmental Engineering',
                '2': 'Mechanical Engineering',
                '3': 'Materials Science and Engineering',
                '4': 'Architecture',
                '5': 'Chemistry',
                '6': 'Electrical Engineering and Computer Science',
                '7': 'Biology',
                '8': 'Physics',
                '9': 'Brain and Cognitive Sciences',
                '10': 'Chemical Engineering',
                '11': 'Urban Studies and Planning',
                '12': 'Earth, Atmospheric, and Planetary Sciences',
                '14': 'Economics',
                '15': 'Management',
                '16': 'Aeronautics and Astronautics',
                '17': 'Political Science',
                '18': 'Mathematics',
                '20': 'Biological Engineering',
                '21': 'Humanities',
                '22': 'Nuclear Science and Engineering',
                '24': 'Linguistics and Philosophy'
            }
            
            # Extract first number from course number
            import re
            match = re.search(r'^(\d+)', course_number)
            if match:
                dept_num = match.group(1)
                if dept_num in dept_map:
                    return dept_map[dept_num]
            
            # Method 2: Look for department in page title
            title_elem = soup.find('title')
            if title_elem:
                title_text = title_elem.get_text()
                parts = title_text.split(' | ')
                if len(parts) >= 2:
                    potential_dept = parts[1].strip()
                    if potential_dept not in ['MIT OpenCourseWare', 'MIT OCW']:
                        return potential_dept
            
            # Method 3: Look for department in course info
            course_info = soup.find('div', class_='course-info')
            if course_info:
                dept_text = course_info.get_text()
                for dept in dept_map.values():
                    if dept in dept_text:
                        return dept
            
            return 'General Studies'
            
        except Exception as e:
            print(f"Error extracting department: {e}")
            return 'General Studies'
    
    def _extract_description_comprehensive(self, soup) -> str:
        """Extract course description comprehensively"""
        try:
            # Method 1: Meta description
            meta_desc = soup.find('meta', {'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                desc = meta_desc.get('content').strip()
                if len(desc) > 50:
                    return desc
            
            # Method 2: Course description sections
            desc_sections = soup.find_all(['div', 'section', 'p'], 
                class_=lambda x: x and any(term in x.lower() for term in ['description', 'intro', 'about']))
            for section in desc_sections:
                text = section.get_text().strip()
                if len(text) > 100:
                    return text[:1000]  # Limit description length
            
            # Method 3: First substantial paragraph
            paragraphs = soup.find_all('p')
            for p in paragraphs:
                text = p.get_text().strip()
                if len(text) > 150 and any(word in text.lower() for word in 
                    ['course', 'subject', 'covers', 'introduces', 'study', 'analysis']):
                    return text[:1000]
            
            return ""
            
        except Exception as e:
            print(f"Error extracting description: {e}")
            return ""
    
    def _extract_level_from_course_number(self, course_number: str) -> str:
        """Extract course level from course number"""
        try:
            import re
            # Extract course number (e.g., 6-046j -> 046j)
            match = re.search(r'-(\d+[a-zA-Z]*)', course_number)
            if match:
                number_str = match.group(1)
                # Remove any letters (like 'j' for joint courses)
                number_clean = re.sub(r'[a-zA-Z]', '', number_str)
                if number_clean:
                    # Keep the original number as MIT intended (046 = 46 but represents 400-level)
                    # MIT course numbers: 046 means 400-level course
                    if number_clean.startswith('0') and len(number_clean) == 3:
                        # Convert 046 -> 460, 034 -> 340, etc.
                        actual_number = int(number_clean[1:]) * 10 + int(number_clean[0]) * 100
                    else:
                        actual_number = int(number_clean)
                    
                    if actual_number >= 700:
                        return 'Graduate'
                    elif actual_number >= 600:
                        return 'Graduate'  
                    elif actual_number >= 400:
                        return 'Advanced Undergraduate'
                    elif actual_number >= 100:
                        return 'Undergraduate'
                    else:
                        return 'Introductory'
            return 'Unknown'
        except Exception as e:
            print(f"Error extracting level: {e}")
            return 'Unknown'
    
    def _extract_semester_year(self, course_number: str, soup) -> Tuple[str, str]:
        """Extract semester and year"""
        try:
            import re
            # From course number (e.g., spring-2015)
            match = re.search(r'-(fall|spring|summer|winter)-(\d{4})', course_number.lower())
            if match:
                return match.group(1).title(), match.group(2)
            
            # From page content
            page_text = soup.get_text()
            semester_match = re.search(r'(Fall|Spring|Summer|Winter)\s+(\d{4})', page_text)
            if semester_match:
                return semester_match.group(1), semester_match.group(2)
            
            return 'Unknown', 'Unknown'
        except:
            return 'Unknown', 'Unknown'
    
    def _extract_course_tags(self, soup, title: str) -> List[str]:
        """Extract course tags/keywords"""
        tags = []
        try:
            # From title
            title_lower = title.lower()
            if 'algorithm' in title_lower:
                tags.append('Algorithms')
            if 'programming' in title_lower:
                tags.append('Programming')
            if 'mathematics' in title_lower or 'math' in title_lower:
                tags.append('Mathematics')
            if 'physics' in title_lower:
                tags.append('Physics')
            if 'engineering' in title_lower:
                tags.append('Engineering')
            
            # From keywords meta tag
            keywords_meta = soup.find('meta', {'name': 'keywords'})
            if keywords_meta:
                keywords = keywords_meta.get('content', '').split(',')
                tags.extend([k.strip().title() for k in keywords[:5]])
            
            return list(set(tags))[:10]  # Limit to 10 unique tags
        except:
            return []
    
    def _extract_difficulty_level(self, soup, course_number: str) -> str:
        """Extract difficulty level"""
        try:
            import re
            match = re.search(r'-(\d+)', course_number)
            if match:
                number_str = match.group(1)
                number_clean = re.sub(r'[a-zA-Z]', '', number_str)
                if number_clean:
                    # Handle leading zeros
                    if len(number_str) == 4 and len(number_clean) == 2:
                        number_clean = '0' + number_clean
                    
                    number = int(number_clean)
                    if number >= 800:
                        return 'Advanced Graduate'
                    elif number >= 600:
                        return 'Graduate'
                    elif number >= 400:
                        return 'Advanced Undergraduate'
                    elif number >= 200:
                        return 'Intermediate'
                    elif number >= 100:
                        return 'Introductory Undergraduate'
                    else:
                        return 'Introductory'
            return 'Unknown'
        except:
            return 'Unknown'
    
    def _extract_prerequisites(self, soup) -> List[str]:
        """Extract course prerequisites"""
        prereqs = []
        try:
            # Look for prerequisite sections
            prereq_sections = soup.find_all(['div', 'p'], 
                string=lambda x: x and 'prerequisite' in x.lower())
            
            for section in prereq_sections:
                parent = section.parent if section.parent else section
                text = parent.get_text()
                
                # Extract course numbers
                import re
                course_matches = re.findall(r'\b\d+\.\d+\w*\b', text)
                prereqs.extend(course_matches)
            
            return list(set(prereqs))[:5]  # Limit to 5 prerequisites
        except:
            return []

    def _extract_course_id_from_url(self, url: str) -> str:
        """Extract course ID from URL"""
        try:
            path_parts = urlparse(url).path.split('/')
            course_parts = [part for part in path_parts if part and 'courses' not in part]
            return course_parts[0] if course_parts else 'unknown-course'
        except:
            return 'unknown-course'
    
    def _extract_text_by_selectors(self, soup, selectors, attribute=None):
        """Extract text using multiple CSS selectors"""
        for selector in selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    return element.get(attribute) if attribute else element.get_text()
            except:
                continue
        return ""
    
    def _extract_subject(self, soup):
        """Extract subject/department"""
        subject_selectors = [
            '.breadcrumbs a',
            '.course-subject',
            '.department',
            'meta[name="subject"]'
        ]
        
        subject = self._extract_text_by_selectors(soup, subject_selectors, 'content')
        if subject and len(subject) > 5:
            return subject
        
        # Try breadcrumbs
        breadcrumbs = soup.find('nav', class_='breadcrumbs') or soup.find('ol', class_='breadcrumb')
        if breadcrumbs:
            links = breadcrumbs.find_all('a')
            for link in links:
                text = link.get_text().strip()
                if text and text.lower() not in ['home', 'courses', 'mit ocw']:
                    return text
        
        return "General"
    
    def _guess_subject_from_title(self, title: str) -> str:
        """Guess subject from course title"""
        subjects = {
            'mathematics': ['math', 'calculus', 'algebra', 'geometry', 'statistics'],
            'physics': ['physics', 'mechanics', 'quantum', 'thermodynamics'],
            'chemistry': ['chemistry', 'chemical', 'organic', 'inorganic'],
            'biology': ['biology', 'biological', 'bio', 'genetics', 'molecular'],
            'computer_science': ['computer', 'programming', 'algorithms', 'software'],
            'engineering': ['engineering', 'mechanical', 'electrical', 'civil'],
            'economics': ['economics', 'economic', 'finance', 'business'],
            'literature': ['literature', 'writing', 'english', 'poetry']
        }
        
        title_lower = title.lower()
        for subject, keywords in subjects.items():
            if any(keyword in title_lower for keyword in keywords):
                return subject.replace('_', ' ').title()
        
        return "General"
    
    def _extract_level(self, soup) -> str:
        """Extract course level"""
        level_indicators = soup.find_all(string=lambda text: text and any(
            level in text.lower() for level in ['undergraduate', 'graduate', 'level']))
        
        for indicator in level_indicators:
            text = indicator.lower()
            if 'undergraduate' in text:
                return 'Undergraduate'
            elif 'graduate' in text:
                return 'Graduate'
        
        return 'Unknown'
    
    def _extract_semester(self, soup) -> str:
        """Extract semester information"""
        semester_indicators = soup.find_all(string=lambda text: text and any(
            term in text.lower() for term in ['spring', 'fall', 'summer', 'winter']))
        
        for indicator in semester_indicators:
            text = indicator.strip()
            if any(term in text.lower() for term in ['spring', 'fall', 'summer', 'winter']):
                return text[:20]
        
        return 'Unknown'
    
    def _extract_videos(self, soup, course_url: str, course_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract videos and transcripts"""
        if not self.enable_video_scraping:
            return []
        
        videos = []
        
        try:
            # Find transcript links first
            transcript_links = self._find_transcript_links(soup, course_url)
            
            # YouTube videos
            youtube_iframes = soup.find_all('iframe', src=lambda x: x and 'youtube.com' in x)
            for iframe in youtube_iframes:
                video_data = self._process_video_element(iframe, 'youtube', course_url, transcript_links)
                if video_data:
                    videos.append(video_data)
            
            # Native videos
            video_elements = soup.find_all(['video', 'source'])
            for elem in video_elements:
                video_data = self._process_video_element(elem, 'native', course_url, transcript_links)
                if video_data:
                    videos.append(video_data)
            
            # Video file links
            video_extensions = ['.mp4', '.mov', '.avi', '.wmv', '.flv', '.webm']
            video_links = soup.find_all('a', href=lambda x: x and any(ext in x.lower() for ext in video_extensions))
            for link in video_links:
                video_data = self._process_video_element(link, 'link', course_url, transcript_links)
                if video_data:
                    videos.append(video_data)
            
            return videos
            
        except Exception as e:
            print(f"Error extracting videos: {e}")
            return []
    
    def _find_transcript_links(self, soup, course_url: str) -> Dict[str, str]:
        """Find transcript download links"""
        transcript_links = {}
        
        try:
            # Look for transcript download links
            download_links = soup.find_all('a', string=lambda text: text and 'transcript' in text.lower())
            
            for link in download_links:
                href = link.get('href', '')
                if href:
                    transcript_url = urljoin(course_url, href)
                    context = link.get_text().strip() or href
                    transcript_links[context] = transcript_url
            
            return transcript_links
            
        except Exception as e:
            print(f"Error finding transcript links: {e}")
            return {}
    
    def _process_video_element(self, element, element_type: str, course_url: str, transcript_links: Dict[str, str] = None) -> Optional[Dict[str, Any]]:
        """Process video element (unified method)"""
        try:
            if element_type == 'youtube':
                src = element.get('src', '')
                video_id = self._extract_youtube_id(src)
                if not video_id:
                    return None
                title = element.get('title', '').strip() or f"Video {video_id}"
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                video_type = 'youtube_video'
                
            elif element_type == 'native':
                src = element.get('src', '')
                if not src:
                    return None
                video_url = urljoin(course_url, src)
                video_id = hashlib.md5(video_url.encode()).hexdigest()[:12]
                title = element.get('title', '').strip() or f"Video {video_id}"
                video_type = 'native_video'
                
            elif element_type == 'link':
                href = element.get('href', '')
                if not href:
                    return None
                video_url = urljoin(course_url, href)
                video_id = hashlib.md5(video_url.encode()).hexdigest()[:12]
                title = element.text.strip() or f"Video {video_id}"
                video_type = 'video_file'
            else:
                return None
            
            # Try to get transcript
            transcript = None
            transcript_source = None
            
            if transcript_links:
                for context, transcript_url in transcript_links.items():
                    if title.lower() in context.lower() or context.lower() in title.lower():
                        transcript = self._download_transcript_content(transcript_url)
                        transcript_source = 'mit_ocw_download'
                        break
            
            # For YouTube videos, try YouTube API as fallback
            if not transcript and element_type == 'youtube' and VIDEO_PROCESSING_AVAILABLE:
                transcript = self._get_youtube_transcript(video_id)
                transcript_source = 'youtube_api'
            
            return {
                'type': video_type,
                'video_id': video_id,
                'url': video_url,
                'title': title,
                'transcript': transcript,
                'transcript_source': transcript_source,
                'source': 'mit_ocw'
            }
            
        except Exception as e:
            print(f"Error processing {element_type} video: {e}")
            return None
    
    def _extract_youtube_id(self, url: str) -> Optional[str]:
        """Extract YouTube video ID from URL"""
        try:
            patterns = [
                r'youtube\.com/embed/([a-zA-Z0-9_-]+)',
                r'youtube\.com/watch\?v=([a-zA-Z0-9_-]+)',
                r'youtu\.be/([a-zA-Z0-9_-]+)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    return match.group(1)
            return None
            
        except Exception:
            return None
    
    def _get_youtube_transcript(self, video_id: str) -> Optional[str]:
        """Get transcript for YouTube video"""
        try:
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['en'])
            transcript_text = ' '.join([item['text'] for item in transcript_list])
            return transcript_text
        except Exception as e:
            print(f"WARNING: Could not get YouTube transcript for {video_id}: {e}")
            return None
    
    def _download_transcript_content(self, transcript_url: str) -> Optional[str]:
        """Download and extract transcript content"""
        try:
            response = requests.get(transcript_url, timeout=30)
            response.raise_for_status()
            
            if transcript_url.lower().endswith('.pdf'):
                # Handle PDF transcripts
                if not PDF_PROCESSING_AVAILABLE:
                    return None
                
                # Save temporarily for processing only
                temp_file = self.temp_storage / "temp_transcript.pdf"
                with open(temp_file, 'wb') as f:
                    f.write(response.content)
                
                text = self._extract_pdf_text(temp_file)
                
                # Clean up temp file immediately
                try:
                    temp_file.unlink()
                except:
                    pass
                
                return text
            else:
                # Handle text-based transcripts
                return response.text
                
        except Exception as e:
            print(f"ERROR downloading transcript: {e}")
            return None
    
    def _extract_pdfs(self, soup, course_url: str, course_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract PDFs with smart filtering"""
        if not self.enable_pdf_scraping:
            return []
        
        pdfs = []
        seen_urls = set()
        
        try:
            # Find PDF links
            pdf_links = soup.find_all('a', href=lambda x: x and '.pdf' in x.lower())
            
            for link in pdf_links:
                href = link.get('href', '')
                if not href or href in seen_urls:
                    continue
                
                seen_urls.add(href)
                pdf_url = urljoin(course_url, href)
                
                # Extract PDF metadata
                pdf_data = self._process_pdf_link(link, pdf_url, course_info)
                if pdf_data:
                    pdfs.append(pdf_data)
            
            return pdfs
            
        except Exception as e:
            print(f"Error extracting PDFs: {e}")
            return []
    
    def _process_pdf_link(self, link, pdf_url: str, course_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process individual PDF link - chá»‰ xá»­ lÃ½ lecture notes"""
        try:
            # Extract title and classify type
            pdf_title = link.get_text().strip() or "MIT OCW Document"
            pdf_category = self._classify_pdf_type(link, pdf_title, pdf_url)
            
            # Bá» qua PDF khÃ´ng pháº£i educational materials ngay tá»« Ä‘áº§u
            if pdf_category == 'rejected':
                return None
            
            pdf_id = hashlib.md5(pdf_url.encode()).hexdigest()[:12]
            
            # Get file size
            file_size_mb = self._get_pdf_size(pdf_url)
            if file_size_mb > self.max_pdf_size_mb:
                return {
                    'type': 'pdf',
                    'pdf_id': pdf_id,
                    'title': pdf_title,
                    'url': pdf_url,
                    'category': pdf_category,
                    'size_mb': file_size_mb,
                    'downloaded': False,
                    'error': f"File too large ({file_size_mb}MB)"
                }
            
            # Create PDF data structure
            pdf_data = {
                'type': 'pdf',
                'pdf_id': pdf_id,
                'title': pdf_title[:80],
                'url': pdf_url,
                'category': pdf_category,
                'size_mb': file_size_mb,
                'course_id': course_info.get('course_id', 'unknown'),
                'scraped_at': datetime.now().isoformat(),
                'downloaded': False,
                'text_content': None
            }
            
            # Smart download decision
            should_download = self._should_download_pdf(pdf_data)
            
            if self.download_pdfs and should_download:
                local_path, text_content = self._download_and_extract_pdf(pdf_url, pdf_id, pdf_data)
                pdf_data.update({
                    'downloaded': local_path is not None,
                    'local_path': str(local_path) if local_path else None,
                    'text_content': text_content,
                    'text_length': len(text_content) if text_content else 0
                })
            
            return pdf_data
            
        except Exception as e:
            print(f"Error processing PDF: {e}")
            return None
    
    def _classify_pdf_type(self, link_element, title: str, url: str) -> str:
        """Classify PDF type - Æ°u tiÃªn lecture materials"""
        context = ""
        parent = link_element.parent
        for _ in range(2):
            if parent:
                context += parent.get_text().lower()
                parent = parent.parent
        
        combined_text = (title + " " + url + " " + context).lower()
        
        # Lecture notes vÃ  slides
        lecture_keywords = ['lecture', 'notes', 'slides', 'slide', 'presentation', 'handout']
        if any(keyword in combined_text for keyword in lecture_keywords):
            return 'lecture_notes'
        
        # Readings cÃ³ thá»ƒ cÃ³ giÃ¡ trá»‹
        reading_keywords = ['reading', 'textbook', 'book', 'chapter']
        if any(keyword in combined_text for keyword in reading_keywords):
            return 'reading_material'
        
        # Tá»« chá»‘i assignments vÃ  exams
        reject_keywords = ['assignment', 'homework', 'problem', 'pset', 'exam', 'test', 'quiz', 'midterm', 'final']
        if any(keyword in combined_text for keyword in reject_keywords):
            return 'rejected'
        
        # Máº·c Ä‘á»‹nh coi nhÆ° educational material
        return 'educational_material'
    
    def _should_download_pdf(self, pdf_data: Dict[str, Any]) -> bool:
        """Decide whether to download PDF - cháº¥p nháº­n lecture materials"""
        category = pdf_data.get('category', 'document')
        
        # Cháº¥p nháº­n cÃ¡c loáº¡i educational materials
        accepted_categories = ['lecture_notes', 'reading_material', 'educational_material']
        if category in accepted_categories:
            return True
        
        # Tá»« chá»‘i rejected category
        return category != 'rejected'
    
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
        """Download PDF and extract text"""
        try:
            # Download with proper headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/pdf,application/octet-stream,*/*'
            }
            response = requests.get(pdf_url, timeout=30, stream=True, headers=headers)
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get('content-type', '').lower()
            if 'html' in content_type:
                print(f"WARNING: URL returns HTML instead of PDF: {pdf_url}")
                return None, None
            
            # Save file temporarily for processing
            pdf_filename = f"{pdf_id}.pdf"
            temp_pdf_path = self.temp_storage / pdf_filename
            
            with open(temp_pdf_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Validate and extract text
            if not self._is_valid_pdf(temp_pdf_path):
                print(f"WARNING: Invalid PDF file: {pdf_url}")
                text_content = None
            else:
                text_content = self._extract_pdf_text(temp_pdf_path)
            
            # Upload to MinIO bronze layer immediately
            minio_path = None
            if self.minio_client:
                try:
                    course_id = pdf_data.get('course_id', 'unknown')
                    category = pdf_data.get('category', 'document')
                    minio_path = f"bronze/mit_ocw/pdfs/{course_id}/{category}/{pdf_filename}"
                    
                    self.minio_client.fput_object(self.minio_bucket, minio_path, str(temp_pdf_path))
                    
                    size_mb = pdf_data.get('size_mb', 0)
                    print(f"[BRONZE] PDF uploaded: {course_id}/{category}/{pdf_filename} ({size_mb:.1f}MB)")
                except Exception as e:
                    print(f"WARNING: Failed to upload PDF to MinIO: {e}")
            
            # Clean up temp file immediately after upload
            try:
                temp_pdf_path.unlink()
            except Exception as e:
                print(f"Warning: Could not delete temp file: {e}")
            
            return minio_path, text_content
            
        except Exception as e:
            print(f"ERROR downloading PDF {pdf_id}: {e}")
            return None, None
    
    def _is_valid_pdf(self, pdf_path: Path) -> bool:
        """Check if file is valid PDF"""
        try:
            with open(pdf_path, 'rb') as f:
                header = f.read(8)
                return header.startswith(b'%PDF-')
        except Exception:
            return False
    
    def _extract_pdf_text(self, pdf_path: Path) -> Optional[str]:
        """Extract text from PDF"""
        if not PDF_PROCESSING_AVAILABLE:
            return None
        
        try:
            text_content = []
            
            # Try pdfplumber first
            if pdfplumber:
                try:
                    with pdfplumber.open(pdf_path) as pdf:
                        for page in pdf.pages[:3]:  # First 3 pages only
                            page_text = page.extract_text()
                            if page_text and page_text.strip():
                                text_content.append(page_text.strip())
                except Exception as e:
                    if "No /Root object" in str(e):
                        print(f"WARNING: Invalid PDF structure: {e}")
                    else:
                        print(f"WARNING: pdfplumber failed: {e}")
                    text_content = []
            
            # Fallback to PyPDF2
            if not text_content and PyPDF2:
                try:
                    with open(pdf_path, 'rb') as file:
                        pdf_reader = PyPDF2.PdfReader(file)
                        
                        if len(pdf_reader.pages) == 0:
                            return None
                        
                        for page_num in range(min(3, len(pdf_reader.pages))):
                            page = pdf_reader.pages[page_num]
                            page_text = page.extract_text()
                            if page_text and page_text.strip():
                                text_content.append(page_text.strip())
                                
                except Exception as e:
                    if "EOF marker not found" in str(e):
                        print(f"WARNING: Corrupted PDF: {e}")
                    else:
                        print(f"WARNING: PyPDF2 failed: {e}")
                    return None
            
            if text_content:
                full_text = '\n'.join(text_content)
                return full_text[:3000] if len(full_text) > 3000 else full_text
            
            return None
            
        except Exception as e:
            print(f"ERROR extracting PDF text: {e}")
            return None
    
    def create_document_id(self, source: str, url: str) -> str:
        """Create unique document ID"""
        unique_string = f"{source}_{url}"
        return hashlib.md5(unique_string.encode()).hexdigest()
    
    def save_to_bronze(self, data: List[Dict[str, Any]]):
        """Save data to bronze layer"""
        if not data:
            return
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        
        # Calculate stats
        total_courses = len(data)
        total_videos = sum(len(course.get('videos', [])) for course in data)
        total_pdfs = sum(len(course.get('pdfs', [])) for course in data)
        videos_with_transcripts = sum(len([v for v in course.get('videos', []) if v.get('transcript')]) for course in data)
        
        print(f"\n[BRONZE LAYER SUMMARY]")
        print(f"Courses: {total_courses}")
        print(f"Videos: {total_videos} (transcripts: {videos_with_transcripts})")
        print(f"PDFs: {total_pdfs}")
        
        # Save to local file in JSON array format (like the old file)
        filename = f"mit_ocw_bronze_{timestamp}.json"
        output_file = Path(self.output_dir) / filename
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        for record in data:
            if isinstance(record, dict):
                course_id = record.get('id')
                if not course_id and record.get('url'):
                    course_id = self.create_document_id(self.source, record['url'])
                if course_id:
                    self.existing_course_ids.add(course_id)
        
        print(f"Saved to: {output_file}")
        
        # Upload to MinIO
        if self.minio_client:
            try:
                minio_path = f"bronze/mit_ocw/json/{filename}"
                self.minio_client.fput_object(self.minio_bucket, minio_path, str(output_file))
                print(f"Uploaded to MinIO: {minio_path}")
                return minio_path
            except Exception as e:
                print(f"WARNING: Failed to upload JSON to MinIO: {e}")
                return None
        
        return None
    
    def save_to_minio(self, data: List[Dict[str, Any]], logical_date: str = None) -> str:
        """Legacy method for compatibility with existing DAG"""
        if not data:
            return None
        
        # Use current date instead of logical_date for filename
        current_timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        filename = f"mit_ocw_bronze_{current_timestamp}.json"
        
        # Save to local file first in JSON array format
        output_file = Path(self.output_dir) / filename
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        for record in data:
            if isinstance(record, dict):
                course_id = record.get('id')
                if not course_id and record.get('url'):
                    course_id = self.create_document_id(self.source, record['url'])
                if course_id:
                    self.existing_course_ids.add(course_id)
        
        # Upload to MinIO
        if self.minio_client:
            try:
                minio_path = f"bronze/mit_ocw/json/{filename}"
                self.minio_client.fput_object(self.minio_bucket, minio_path, str(output_file))
                print(f"Uploaded to MinIO: {minio_path}")
                return minio_path
            except Exception as e:
                print(f"WARNING: Failed to upload JSON to MinIO: {e}")
                return None
        
        return None



    def _load_existing_course_ids(self) -> Set[str]:
        existing: Set[str] = set()
        try:
            if not self.output_path.exists():
                return existing
            for json_file in sorted(self.output_path.glob("mit_ocw_bronze_*.json")):
                try:
                    with open(json_file, "r", encoding="utf-8") as handle:
                        content = handle.read().strip()
                    if not content:
                        continue
                    records: List[Dict[str, Any]] = []
                    
                    # Check if content is JSON array (new format like old file)
                    if content.startswith("["):
                        try:
                            parsed = json.loads(content)
                            if isinstance(parsed, list):
                                records = [item for item in parsed if isinstance(item, dict)]
                        except json.JSONDecodeError:
                            records = []
                    else:
                        # Handle JSON Lines format (old new format)
                        for line in content.splitlines():
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                parsed_line = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if isinstance(parsed_line, dict):
                                records.append(parsed_line)
                    
                    for record in records:
                        course_id = record.get("id")
                        if not course_id and record.get("url"):
                            course_id = self.create_document_id(self.source, record["url"])
                        if course_id:
                            existing.add(course_id)
                except Exception as exc:
                    print(f"[MIT OCW] Skipping existing file {json_file.name}: {exc}")
                    continue
        except Exception as exc:
            print(f"[MIT OCW] Unable to scan existing bronze files: {exc}")
        return existing

    def _extract_instructors_advanced(self, soup) -> str:
        """Extract instructors using multiple methods"""
        try:
            # Method 1: Look for instructor-specific elements
            instructor_selectors = [
                '.instructors', '.faculty', '.instructor-name', 
                '[class*="instructor"]', '[class*="faculty"]'
            ]
            
            for selector in instructor_selectors:
                elements = soup.select(selector)
                if elements:
                    instructors = []
                    for elem in elements:
                        text = elem.get_text().strip()
                        if text and text.lower() not in ['unknown', 'n/a']:
                            instructors.append(text)
                    if instructors:
                        return '; '.join(instructors)
            
            # Method 2: Search for instructor patterns in text
            all_text = soup.get_text()
            import re
            
            # Look for "Taught by X" or "Instructor: X" patterns
            patterns = [
                r'Taught by[:\s]+([^\\n.;,]+)',
                r'Instructor[s]?[:\s]+([^\\n.;,]+)',
                r'Prof[essor]*[:\s]+([^\\n.;,]+)',
                r'Dr[:\s]+([^\\n.;,]+)'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, all_text, re.IGNORECASE)
                if matches:
                    instructors = []
                    for match in matches:
                        name = match.strip()
                        if len(name) > 2 and len(name) < 100:  # Reasonable name length
                            instructors.append(name)
                    if instructors:
                        return '; '.join(instructors[:3])  # Max 3 instructors
            
            # Method 3: Look in specific content areas
            content_areas = soup.find_all(['div', 'section'], class_=lambda x: x and any(
                keyword in x.lower() for keyword in ['course', 'about', 'instructor', 'faculty']
            ))
            
            for area in content_areas:
                text = area.get_text()
                for pattern in patterns:
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    if matches:
                        return matches[0].strip()
            
            return "Unknown"
            
        except Exception as e:
            print(f"Error extracting instructors: {e}")
            return "Unknown"
    
    def _extract_subject_advanced(self, soup, title: str) -> str:
        """Extract subject using advanced methods"""
        try:
            # Method 1: From URL department prefix (e.g., 1-89 = Civil Engineering)
            url_parts = soup.find('link', {'rel': 'canonical'})
            if url_parts:
                url = url_parts.get('href', '')
                dept_map = {
                    '1-': 'Civil and Environmental Engineering',
                    '2-': 'Mechanical Engineering', 
                    '3-': 'Materials Science and Engineering',
                    '6-': 'Electrical Engineering and Computer Science',
                    '8-': 'Physics',
                    '14-': 'Economics',
                    '15-': 'Management',
                    '16-': 'Aeronautics and Astronautics',
                    '18-': 'Mathematics'
                }
                
                for prefix, department in dept_map.items():
                    if prefix in url:
                        return department
            
            # Method 2: From page title breadcrumbs
            page_title = soup.find('title')
            if page_title:
                title_text = page_title.get_text()
                if ' | ' in title_text:
                    parts = title_text.split(' | ')
                    if len(parts) >= 2:
                        # Usually format: "Course | Department | MIT OCW"
                        department = parts[1].strip()
                        if department not in ['MIT OpenCourseWare', 'MIT OCW']:
                            return department
            
            # Method 3: Guess from course title keywords
            return self._guess_subject_from_title(title)
            
        except Exception as e:
            print(f"Error extracting subject: {e}")
            return "General"
    
    def _extract_description_advanced(self, soup) -> str:
        """Extract course description using multiple methods"""
        try:
            # Method 1: Meta description
            meta_desc = soup.find('meta', {'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                desc = meta_desc.get('content').strip()
                if len(desc) > 20:  # Reasonable description length
                    return desc
            
            # Method 2: Course description sections
            desc_selectors = [
                '.course-description', '.course-intro', '.description',
                '[class*="description"]', '[class*="intro"]', '[class*="about"]'
            ]
            
            for selector in desc_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text().strip()
                    if len(text) > 50:  # Substantial content
                        return text[:500]
            
            # Method 3: Look for description in first few paragraphs
            paragraphs = soup.find_all('p')
            for p in paragraphs[:5]:  # Check first 5 paragraphs
                text = p.get_text().strip()
                if len(text) > 100 and any(keyword in text.lower() for keyword in 
                    ['course', 'subject', 'covers', 'introduces', 'focuses', 'examines']):
                    return text[:500]
            
            return ""
            
        except Exception as e:
            print(f"Error extracting description: {e}")
            return ""
    
    def _extract_level_advanced(self, soup, title: str) -> str:
        """Extract course level with better detection"""
        try:
            # Check common level indicators
            text = soup.get_text().lower()
            
            if any(term in text for term in ['graduate', 'grad', 'phd', 'doctoral']):
                return 'Graduate'
            elif any(term in text for term in ['undergraduate', 'undergrad', 'bachelor']):
                return 'Undergraduate'
            
            # Check course number pattern (graduate courses usually 600+)
            import re
            course_num_match = re.search(r'(\\d+)\\.(\\d+)', title)
            if course_num_match:
                major_num = int(course_num_match.group(2))
                if major_num >= 600:
                    return 'Graduate'
                else:
                    return 'Undergraduate'
            
            return 'Unknown'
            
        except Exception as e:
            print(f"Error extracting level: {e}")
            return 'Unknown'
    
    def _extract_semester_advanced(self, soup) -> str:
        """Extract semester info with better parsing"""
        try:
            text = soup.get_text()
            
            # Look for semester patterns
            import re
            semester_patterns = [
                r'(Fall|Spring|Summer|Winter)\\s+(\\d{4})',
                r'(Fall|Spring|Summer|Winter)\\s+\\d{2}',
                r'(Fall|Spring|Summer|Winter)\\s+Semester'
            ]
            
            for pattern in semester_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    if isinstance(matches[0], tuple):
                        return ' '.join(matches[0])
                    else:
                        return matches[0]
            
            # Simple semester detection
            seasons = ['Fall', 'Spring', 'Summer', 'Winter']
            for season in seasons:
                if season.lower() in text.lower():
                    return season
            
            return 'Unknown'
            
        except Exception as e:
            print(f"Error extracting semester: {e}")
            return 'Unknown'


def run_mit_ocw_scraper(**kwargs):
    """Run MIT OCW scraper"""
    scraper = MITOCWScraper(**kwargs)
    try:
        return scraper.scrape()
    finally:
        scraper.cleanup()


if __name__ == "__main__":
    import sys
    
    # Parse max_documents from command line
    max_docs = None
    if len(sys.argv) > 1:
        try:
            max_docs = int(sys.argv[1])
        except ValueError:
            print("Usage: python bronze_mit_ocw_optimized.py [max_documents]")
            sys.exit(1)
    
    # Run scraper
    documents = run_mit_ocw_scraper(max_documents=max_docs)
    print(f"Completed! Scraped {len(documents)} courses.")


