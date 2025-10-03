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
        self.max_pdf_size_mb = float(os.getenv('MAX_PDF_SIZE_MB', '20.0'))
        self.download_pdfs = os.getenv('DOWNLOAD_PDFS', '1') == '1'
        
        # Smart download configuration 
        self.download_strategy = os.getenv('DOWNLOAD_STRATEGY', 'selective')
        self.important_categories = set(os.getenv('IMPORTANT_CATEGORIES', 'lecture_notes,textbook,handout,reading').split(','))
        self.skip_categories = set(os.getenv('SKIP_CATEGORIES', 'exam,assignment,quiz').split(','))
        
        # Create directories
        self.local_storage = Path("/tmp/mit_ocw_scrape")
        self.local_storage.mkdir(exist_ok=True)
        (self.local_storage / "pdfs").mkdir(exist_ok=True)
        (self.local_storage / "transcripts").mkdir(exist_ok=True)
        
        # MinIO setup
        self._setup_minio()
        
        # Setup Selenium
        if self.use_selenium:
            self._setup_selenium()
        
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
        """Setup Selenium WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
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
            except Exception as e:
                print(f"Error closing WebDriver: {e}")
    
    def scrape_with_selenium(self) -> List[Dict[str, Any]]:
        """Legacy method for compatibility with existing DAG"""
        return self.scrape()
    
    def scrape(self) -> List[Dict[str, Any]]:
        """Main scraping method"""
        print("[MIT OCW] Starting scraper...")
        
        try:
            # Get course URLs
            course_urls = self._get_course_urls()
            print(f"Found {len(course_urls)} courses")
            
            if self.max_documents:
                course_urls = list(course_urls)[:self.max_documents]
                print(f"Limited to {len(course_urls)} courses for processing")
            
            # Scrape courses
            documents = []
            for i, url in enumerate(course_urls, 1):
                print(f"[{i}/{len(course_urls)}] Scraping: {url}")
                
                course_data = self._scrape_course(url)
                if course_data:
                    documents.append(course_data)
                    
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
        """Get course URLs using advanced method"""
        if not self.driver:
            return set()
        
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
            max_scrolls = 10
            
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
            if self.enable_video_scraping or self.enable_pdf_scraping:
                videos, pdfs = self._extract_multimedia_deep(url, soup, course_info)
                course_info['videos'] = videos
                course_info['pdfs'] = pdfs
            
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
                        
                        if self.enable_pdf_scraping and link_type in ['assignments', 'readings', 'lecture_notes', 'materials']:
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
        """Get page content with retry logic"""
        for attempt in range(max_retries):
            try:
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
        """Extract basic course information"""
        course_id = self._extract_course_id_from_url(url)
        
        # Extract title
        title_selectors = ['h1.course-title', 'h1', '.course-header h1', 'title']
        title = self._extract_text_by_selectors(soup, title_selectors) or "Unknown Course"
        
        # Extract instructors
        instructor_selectors = ['.instructors', '.faculty', '.instructor-name']
        instructors = self._extract_text_by_selectors(soup, instructor_selectors) or "Unknown"
        
        # Extract subject/department
        subject = self._extract_subject(soup) or self._guess_subject_from_title(title)
        
        # Extract description
        desc_selectors = ['.course-description', '.course-intro', '.description', 'meta[name="description"]']
        description = self._extract_text_by_selectors(soup, desc_selectors, 'content') or ""
        
        return {
            'id': self.create_document_id(self.source, url),
            'course_id': course_id,
            'title': title.strip()[:200],
            'instructors': instructors.strip()[:100],
            'subject': subject.strip()[:50],
            'description': description.strip()[:500],
            'url': url,
            'source': self.source,
            'scraped_at': datetime.now().isoformat(),
            'level': self._extract_level(soup),
            'semester': self._extract_semester(soup)
        }
    
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
                
                # Save temporarily and extract text
                temp_file = self.local_storage / "transcripts" / "temp_transcript.pdf"
                with open(temp_file, 'wb') as f:
                    f.write(response.content)
                
                return self._extract_pdf_text(temp_file)
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
        """Process individual PDF link"""
        try:
            # Extract title and classify type
            pdf_title = link.get_text().strip() or "MIT OCW Document"
            pdf_id = hashlib.md5(pdf_url.encode()).hexdigest()[:12]
            pdf_category = self._classify_pdf_type(link, pdf_title, pdf_url)
            
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
        """Classify PDF type"""
        context = ""
        parent = link_element.parent
        for _ in range(2):
            if parent:
                context += parent.get_text().lower()
                parent = parent.parent
        
        combined_text = (title + " " + url + " " + context).lower()
        
        categories = {
            'lecture_notes': ['lecture', 'notes', 'slides'],
            'textbook': ['textbook', 'book', 'text'],
            'handout': ['handout', 'handouts'],
            'assignment': ['assignment', 'homework', 'problem', 'pset'],
            'exam': ['exam', 'test', 'quiz', 'midterm', 'final'],
            'reading': ['reading', 'readings'],
            'syllabus': ['syllabus', 'schedule']
        }
        
        for category, keywords in categories.items():
            if any(keyword in combined_text for keyword in keywords):
                return category
        
        return 'document'
    
    def _should_download_pdf(self, pdf_data: Dict[str, Any]) -> bool:
        """Decide whether to download PDF based on strategy"""
        if self.download_strategy == 'none':
            return False
        elif self.download_strategy == 'all':
            return True
        elif self.download_strategy == 'selective':
            category = pdf_data.get('category', 'document')
            
            # Skip categories we don't want
            if category in self.skip_categories:
                return False
            
            # Download important categories
            if category in self.important_categories:
                return True
            
            # Download small files
            size_mb = pdf_data.get('size_mb', 0)
            if size_mb > 0 and size_mb < 5:
                return True
        
        return False
    
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
            
            # Save file
            pdf_filename = f"{pdf_id}.pdf"
            pdf_path = self.local_storage / "pdfs" / pdf_filename
            
            with open(pdf_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Validate and extract text
            if not self._is_valid_pdf(pdf_path):
                print(f"WARNING: Invalid PDF file: {pdf_url}")
                text_content = None
            else:
                text_content = self._extract_pdf_text(pdf_path)
            
            # Upload to MinIO
            if self.minio_client:
                try:
                    course_id = pdf_data.get('course_id', 'unknown')
                    category = pdf_data.get('category', 'document')
                    minio_path = f"bronze/mit_ocw/pdfs/{course_id}/{category}/{pdf_filename}"
                    
                    self.minio_client.fput_object(self.minio_bucket, minio_path, str(pdf_path))
                    
                    size_mb = pdf_data.get('size_mb', 0)
                    print(f"[BRONZE] PDF uploaded: {course_id}/{category}/{pdf_filename} ({size_mb:.1f}MB)")
                except Exception as e:
                    print(f"WARNING: Failed to upload PDF to MinIO: {e}")
            
            return pdf_path, text_content
            
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
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Calculate stats
        total_courses = len(data)
        total_videos = sum(len(course.get('videos', [])) for course in data)
        total_pdfs = sum(len(course.get('pdfs', [])) for course in data)
        videos_with_transcripts = sum(len([v for v in course.get('videos', []) if v.get('transcript')]) for course in data)
        
        print(f"\n[BRONZE LAYER SUMMARY]")
        print(f"Courses: {total_courses}")
        print(f"Videos: {total_videos} (transcripts: {videos_with_transcripts})")
        print(f"PDFs: {total_pdfs}")
        
        # Save to local file
        filename = f"mit_ocw_bronze_{timestamp}.json"
        output_file = Path(self.output_dir) / filename
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
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
        
        timestamp = logical_date or datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mit_ocw_bronze_{timestamp}.json"
        
        # Save to local file first
        output_file = Path(self.output_dir) / filename
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
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