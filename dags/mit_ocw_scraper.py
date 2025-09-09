from typing import List, Dict, Any
from datetime import datetime
from urllib.parse import urljoin
import time
import json
import random
import os
import requests
import hashlib
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# MinIO imports
from minio import Minio

class MITOCWScraper:
    
    def __init__(self, delay=2, use_selenium=True, output_dir="scraped_data", batch_size=25, max_documents=None, **kwargs):
        self.base_url = "https://ocw.mit.edu"
        self.source = "mit_ocw"
        self.delay = delay
        self.use_selenium = use_selenium
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.max_documents = max_documents  # Giới hạn số lượng tài liệu cào (None = không giới hạn)
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Batch processing
        self.current_batch = []
        
        # Tạo output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # MinIO setup
        self.minio_client = None
        self.minio_bucket = os.getenv('MINIO_BUCKET', 'oer-raw')
        self.minio_enable = str(os.getenv('MINIO_ENABLE', '0')).lower() in {'1', 'true', 'yes'}
        if self.minio_enable:
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
        
        # Setup Selenium nếu cần
        if self.use_selenium:
            self._setup_selenium()
    
    def _setup_selenium(self):
        """Setup Selenium WebDriver - sử dụng cùng logic với OpenStax"""
        try:
            print("Đang thiết lập Selenium...")
            
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Chạy không hiển thị browser cho container
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-software-rasterizer")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
            chrome_options.add_argument("--remote-debugging-port=9222")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            
            # Luôn sử dụng ChromeDriver đã cài sẵn trong container
            chromedriver_paths = [
                '/usr/local/bin/chromedriver',
                '/usr/bin/chromedriver',
                'chromedriver'
            ]

            driver_service = None
            for path in chromedriver_paths:
                try:
                    if os.path.exists(path) or path == 'chromedriver':
                        driver_service = Service(path)
                        self.driver = webdriver.Chrome(service=driver_service, options=chrome_options)
                        print(f"Sử dụng ChromeDriver tại: {path}")
                        break
                except Exception as e:
                    print(f"Không thể sử dụng ChromeDriver tại {path}: {e}")
                    continue
            
            if not self.driver:
                # Fallback sử dụng webdriver-manager
                print("Đang thử sử dụng webdriver-manager...")
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                print("Sử dụng ChromeDriver từ webdriver-manager")
                
            print("Selenium đã sẵn sàng!")
            
        except Exception as e:
            print(f"Lỗi thiết lập Selenium: {e}")
            print("Sẽ sử dụng requests thay thế")
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
        """Thêm document vào batch"""
        self.current_batch.append(document)
        if len(self.current_batch) >= self.batch_size:
            self.save_batch()
    
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
                        print(f"\nPROGRESS REPORT:")
                        print(f"   Thành công: {success_count}")
                        print(f"   Lỗi: {error_count}")
                        print(f"   Tiến độ: {i+1}/{len(course_list)}")
                        print(f"   Tỷ lệ thành công: {success_count/(i+1)*100:.1f}%\n")
                        if self.max_documents:
                            print(f"   Giới hạn: {len(all_documents)}/{self.max_documents}")
                        
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
            
            print(f"\nKẾT QUẢ CUỐI CÙNG:")
            print(f"   Tổng courses: {len(all_documents)}")
            print(f"   Thành công: {success_count}")
            print(f"   Lỗi: {error_count}")
            if self.max_documents:
                print(f"   Giới hạn đặt: {self.max_documents}")
                print(f"   Đã đạt giới hạn: {'Có' if len(all_documents) >= self.max_documents else 'Không'}")
            else:
                print(f"   Giới hạn: Không giới hạn")
            
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
            print("Selenium không khả dụng, chuyển sang method fallback")
            return self._get_course_urls_fallback()
        
        try:
            # Chỉ lấy từ search page với sort theo course number
            search_url = "https://ocw.mit.edu/search/?s=department_course_numbers.sort_coursenum&type=course"
            print(f"Đang cào courses từ: {search_url}")
            
            self.driver.get(search_url)
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)
            
            # Progressive scroll để load tất cả courses
            self._progressive_scroll()
            
            # Extract URLs
            course_urls = self._extract_course_urls()
            print(f"Tìm thấy {len(course_urls)} courses từ search page")
            
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
            print("Đang scroll để load content...")
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            scroll_attempts = 0
            max_scrolls = 20
            
            while scroll_attempts < max_scrolls:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                    
                last_height = new_height
                scroll_attempts += 1
            
            print(f"Hoàn thành scroll sau {scroll_attempts} lần")
            
        except Exception as e:
            print(f"Lỗi scroll: {e}")
    
    def _extract_course_urls(self) -> set:
        """Extract course URLs từ page"""
        course_urls = set()
        
        try:
            course_selectors = [
                'a[href*="/courses/"]',
                '.course-card a',
                '.course-link',
                '.course-item a'
            ]
            
            for selector in course_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        href = element.get_attribute('href')
                        if href and self._is_valid_course_url(href):
                            course_urls.add(href)
                except:
                    continue
            
            print(f"Extracted {len(course_urls)} URLs")
            
        except Exception as e:
            print(f"Lỗi extract URLs: {e}")
        
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
        """Cào thông tin course với format JSON yêu cầu"""
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
            
            return {
                'id': self.create_document_id(self.source, url),
                'title': title,
                'description': description or '',
                'authors': authors,
                'subject': subject,
                'source': 'MIT',
                'url': url,
                'url_pdf': url_pdf,
                'scraped_at': datetime.now().isoformat()
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
    
    def save_to_minio(self, documents: List[Dict[str, Any]], source: str = "mit_ocw", logical_date: str = None):
        """Lưu dữ liệu vào MinIO"""
        if not self.minio_enable or not self.minio_client or not documents:
            print("MinIO không được bật hoặc không có dữ liệu để lưu")
            return ""
        
        if logical_date is None:
            logical_date = datetime.now().strftime("%Y-%m-%d")
        
        # Tạo temporary file
        os.makedirs('/tmp', exist_ok=True) if os.name != 'nt' else os.makedirs('temp', exist_ok=True)
        tmp_dir = '/tmp' if os.name != 'nt' else 'temp'
        tmp_path = os.path.join(tmp_dir, f"{source}_{logical_date}_{int(time.time())}.jsonl")
        
        try:
            # Ghi dữ liệu vào temp file
            with open(tmp_path, 'w', encoding='utf-8') as f:
                for doc in documents:
                    f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            
            # Upload lên MinIO
            object_name = f"{source}/{logical_date}/data_{int(time.time())}.jsonl"
            self.minio_client.fput_object(self.minio_bucket, object_name, tmp_path)
            os.remove(tmp_path)
            print(f"[MinIO] Đã lưu data: s3://{self.minio_bucket}/{object_name}")
            return object_name
        except Exception as e:
            print(f"[MinIO] Lỗi khi lưu vào MinIO: {e}")
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
    
    def scrape_fallback(self) -> List[Dict[str, Any]]:
        """Fallback method"""
        print("Sử dụng requests fallback...")
        return self.scrape_with_selenium()

# Ví dụ sử dụng:
# scraper = MITOCWScraper(max_documents=50)  # Giới hạn 50 tài liệu
# documents = scraper.scrape_with_selenium()

# Hoặc không giới hạn:
# scraper = MITOCWScraper()  # Không giới hạn, cào tất cả