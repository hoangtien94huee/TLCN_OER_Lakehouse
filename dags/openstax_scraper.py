import requests
from bs4 import BeautifulSoup
import json
import time
import hashlib
from urllib.parse import urljoin, urlparse
import re
from datetime import datetime
import os
from typing import List, Dict, Any

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

# MinIO imports - available in Docker environment via requirements.txt
try:
    from minio import Minio  # type: ignore
except ImportError:
    Minio = None
    print("Warning: MinIO library not found. MinIO features will be disabled.")

class OpenStaxScraper:
    """Scraper nâng cao cho các tài liệu OER sử dụng Selenium"""
    
    def __init__(self, delay: float = 2.0, use_selenium: bool = True):
        # Session cho requests thông thường
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.127 Safari/537.36'
        })
        
        # Selenium setup
        self.use_selenium = use_selenium
        self.driver = None
        if use_selenium:
            self.setup_selenium()
        
        self.documents = []
        self.scraped_urls = set()
        self.delay = delay
        
        # Tạo thư mục output
        self.output_dir = "scraped_data"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # MinIO setup
        self.minio_client = None
        self.minio_bucket = os.getenv('MINIO_BUCKET', 'oer-raw')
        self.minio_enable = str(os.getenv('MINIO_ENABLE', '0')).lower() in {'1', 'true', 'yes'}
        if self.minio_enable and Minio is not None:
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
        elif self.minio_enable and Minio is None:
            print("[MinIO] MinIO library not available, disabling MinIO features")
            self.minio_enable = False
    
    def setup_selenium(self):
        """Thiết lập Selenium WebDriver"""
        try:
            print("Đang thiết lập Selenium...")
            
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.82 Safari/537.36")
            
            # Sử dụng ChromeDriver đã cài sẵn trong Docker
            chromedriver_paths = [
                '/usr/local/bin/chromedriver',
                '/usr/bin/chromedriver',
                'chromedriver'
            ]
            
            for path in chromedriver_paths:
                try:
                    if os.path.exists(path) or path == 'chromedriver':
                        service = Service(path)
                        self.driver = webdriver.Chrome(service=service, options=chrome_options)
                        print(f"Sử dụng ChromeDriver tại: {path}")
                        break
                except Exception as e:
                    print(f"Không thể sử dụng ChromeDriver tại {path}: {e}")
                    continue
                
            print("Selenium đã sẵn sàng!")
            
        except Exception as e:
            print(f"Lỗi thiết lập Selenium: {e}")
            print("Sẽ sử dụng requests thay thế")
            self.use_selenium = False
    
    def get_page_selenium(self, url: str) -> BeautifulSoup:
        """Lấy nội dung trang web bằng Selenium"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                print(f"[Selenium] Đang cào: {url} (attempt {attempt + 1})")
                
                # Check if driver is still alive
                if not self._is_driver_alive():
                    print("Driver đã disconnect, đang restart...")
                    self.setup_selenium()
                    if not self.driver:
                        print("Không thể restart driver")
                        return None
                
                self.driver.get(url)
                
                # Đợi trang load
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Đợi thêm cho JavaScript load content
                time.sleep(3)
                
                # Scroll để trigger lazy loading và load thêm content
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Scroll lên trên và xuống dưới để đảm bảo tất cả content được load
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(1)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Thử click vào các nút "Load more" hoặc "Show more" nếu có
                try:
                    load_more_buttons = self.driver.find_elements(By.XPATH, 
                        "//button[contains(text(), 'Load more') or contains(text(), 'Show more') or contains(text(), 'View all')]")
                    for button in load_more_buttons:
                        if button.is_displayed():
                            self.driver.execute_script("arguments[0].click();", button)
                            time.sleep(2)
                except Exception as e:
                    print(f"Không thể click Load more: {e}")
                
                html = self.driver.page_source
                return BeautifulSoup(html, 'html.parser')
                
            except Exception as e:
                print(f"Lỗi Selenium attempt {attempt + 1}: {e}")
                if "disconnected" in str(e).lower() or "devtools" in str(e).lower():
                    print("DevTools disconnect detected, will retry...")
                    if self.driver:
                        try:
                            self.driver.quit()
                        except:
                            pass
                        self.driver = None
                    
                    if attempt < max_retries - 1:  # Don't sleep on last attempt
                        time.sleep(2)
                        continue
                else:
                    break
        
        print(f"Failed to scrape {url} after {max_retries} attempts")
        return None
    
    def _is_driver_alive(self):
        """Kiểm tra xem driver còn hoạt động không"""
        try:
            self.driver.current_url
            return True
        except:
            return False
    
    def get_page_requests(self, url: str) -> BeautifulSoup:
        """Lấy nội dung trang web bằng requests (fallback)"""
        try:
            print(f"[Requests] Đang cào: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"Lỗi requests khi cào {url}: {e}")
            return None
    
    def get_page(self, url: str) -> BeautifulSoup:
        """Lấy nội dung trang web - ưu tiên Selenium"""
        if self.use_selenium and self.driver:
            return self.get_page_selenium(url)
        else:
            return self.get_page_requests(url)
    
    def create_doc_hash(self, url: str, title: str) -> str:
        """Tạo hash unique cho document"""
        unique_string = f"{url}_{title}"
        return hashlib.md5(unique_string.encode('utf-8')).hexdigest()
    
    def parse_author_info(self, author_text: str, selector_type: str = '') -> List[str]:
        """Parse thông tin tác giả từ text, xử lý đặc biệt cho loc-senior-author"""
        authors = []
        
        if not author_text:
            return authors
            
        # Xử lý đặc biệt cho loc-senior-author  
        if 'loc-senior-author' in selector_type:
            
            # Xóa dấu ngoặc kép ở đầu và cuối nếu có
            clean_text = author_text.strip()
            if clean_text.startswith('"') and clean_text.endswith('"'):
                clean_text = clean_text[1:-1]
            
            # Tách theo dấu phẩy
            parts = [part.strip() for part in clean_text.split(',')]
            
            for i, part in enumerate(parts):
                if part:
                    # Bỏ dấu ngoặc kép còn sót lại
                    clean_part = part.strip('"').strip("'").strip()
                    
                    if i == 0:
                        # Part đầu tiên là tên tác giả
                        if clean_part and clean_part not in authors:
                            authors.append(clean_part)
                    else:
                        # Các part sau: kiểm tra xem có phải university/institution không
                        university_keywords = [
                            'university', 'college', 'institute', 'school', 
                            'academy', 'polytechnic', 'tech', 'state'
                        ]
                        is_institution = any(keyword.lower() in clean_part.lower() 
                                           for keyword in university_keywords)
                        
                        # Nếu không phải institution, có thể là tác giả khác
                        if not is_institution and len(clean_part) > 2:
                            # Kiểm tra format name (có space và ít nhất 2 từ)
                            words = clean_part.split()
                            if len(words) >= 2 and clean_part not in authors:
                                authors.append(clean_part)
                                
        else:
            # Xử lý bình thường cho các selector khác
            clean_text = author_text.strip('"').strip("'").strip()
            
            # Tách theo dấu phẩy nếu có nhiều tác giả
            if ',' in clean_text and not any(keyword in clean_text.lower() 
                                           for keyword in ['university', 'college', 'institute']):
                parts = [part.strip() for part in clean_text.split(',')]
                for part in parts:
                    if part and part not in authors:
                        authors.append(part)
            else:
                # Single author hoặc author + institution
                if clean_text and clean_text not in authors:
                    authors.append(clean_text)
                
        return authors
    
    def scrape_openstax_with_selenium(self) -> List[Dict[str, Any]]:
        """Cào OpenStax sử dụng Selenium để load JavaScript và tự động tìm tất cả sách"""
        print("Bắt đầu cào OpenStax với Selenium...")
        documents = []
        
        if not self.use_selenium:
            print("Selenium không khả dụng, sử dụng method thay thế")
            return self.scrape_openstax_fallback()
        
        base_url = "https://openstax.org"
        book_urls = set()  # Sử dụng set để tránh duplicate
        
        # Phase 1: Cào trang chủ subjects để tìm tất cả categories
        print("Phase 1: Tìm tất cả subject categories...")
        subjects_page = f"{base_url}/subjects"
        soup = self.get_page(subjects_page)
        
        subject_urls = [subjects_page]  # Bắt đầu với trang subjects chính
        
        if soup:
            # Tìm tất cả link subjects
            subject_links = soup.find_all('a', href=True)
            for link in subject_links:
                href = link.get('href')
                if href and '/subjects/' in href and href != '/subjects':
                    # Bỏ qua URLs có hash fragments (anchor links)
                    if '#' in href:
                        continue
                    
                    full_url = urljoin(base_url, href)
                    # Đảm bảo không có hash fragments trong full URL
                    clean_url = full_url.split('#')[0]
                    
                    if clean_url not in subject_urls:
                        subject_urls.append(clean_url)
                        print(f"Tìm thấy subject: {href}")
        
        # Phase 2: Cào từng subject page để tìm sách (bỏ qua hash URLs)
        print(f"Phase 2: Cào {len(subject_urls)} subject pages...")
        for i, subject_url in enumerate(subject_urls, 1):
            # Bỏ qua URLs có hash fragments
            if '#' in subject_url:
                print(f"Bỏ qua hash URL {i}/{len(subject_urls)}: {subject_url}")
                continue
                
            print(f"Đang cào subject {i}/{len(subject_urls)}: {subject_url}")
            soup = self.get_page(subject_url)
            if not soup:
                continue
            
            # Tìm tất cả links có thể là sách
            all_links = soup.find_all('a', href=True)
            
            for link in all_links:
                href = link.get('href')
                link_text = link.get_text(strip=True)
                link_text_lower = link_text.lower()
                
                if href and link_text:
                    # Bỏ qua các link không phải tên sách
                    skip_keywords = [
                        'view online', 'read online', 'access book', 'get this book',
                        'instructor resources', 'student resources', 'download',
                        'free book', 'errata', 'ancillary', 'webinar', 'adoption',
                        'view more', 'show more', 'read more', 'learn more',
                        'click here', 'get', 'access', 'resources'
                    ]
                    
                    # Skip nếu text chứa keyword không mong muốn
                    if any(keyword in link_text_lower for keyword in skip_keywords):
                        continue
                    
                    # Chỉ lấy links đến books và có text là tên sách thực sự
                    if '/books/' in href or '/details/books/' in href:
                        # Kiểm tra xem có phải là tên sách thực sự không
                        # Tên sách thường có độ dài hợp lý và không chứa các từ khóa action
                        if (len(link_text) > 10 and len(link_text) < 100 and 
                            not any(action in link_text_lower for action in ['click', 'here', 'more', 'view', 'get', 'access'])):
                            
                            full_url = urljoin(base_url, href)
                            # Lưu cả URL và tên sách để kiểm tra
                            old_size = len(book_urls)
                            book_urls.add(full_url)
                            
                            # Chỉ in ra nếu thực sự thêm URL mới
                            if len(book_urls) > old_size:
                                print(f"  Tìm thấy sách: {link_text}")
            
            time.sleep(1)  # Delay giữa các subject pages
        
        # Phase 3: Thử cào thêm từ sitemap hoặc API nếu có
        print("Phase 3: Tìm kiếm bổ sung...")
        try:
            # Thử trang browse hoặc library
            additional_pages = [
                f"{base_url}/browse",
                f"{base_url}/library", 
                f"{base_url}/catalog"
            ]
            
            for page_url in additional_pages:
                print(f"Kiểm tra trang bổ sung: {page_url}")
                soup = self.get_page(page_url)
                if soup:
                    links = soup.find_all('a', href=True)
                    for link in links:
                        href = link.get('href')
                        link_text = link.get_text(strip=True)
                        link_text_lower = link_text.lower()
                        
                        if href and link_text and ('/books/' in href or '/details/books/' in href):
                            # Áp dụng cùng logic lọc như trên
                            skip_keywords = [
                                'view online', 'read online', 'access book', 'get this book',
                                'instructor resources', 'student resources', 'download',
                                'free book', 'errata', 'ancillary', 'webinar', 'adoption',
                                'view more', 'show more', 'read more', 'learn more',
                                'click here', 'get', 'access', 'resources'
                            ]
                            
                            if not any(keyword in link_text_lower for keyword in skip_keywords):
                                if (len(link_text) > 10 and len(link_text) < 100 and 
                                    not any(action in link_text_lower for action in ['click', 'here', 'more', 'view', 'get', 'access'])):
                                    
                                    full_url = urljoin(base_url, href)
                                    if full_url not in book_urls:
                                        book_urls.add(full_url)
                                        print(f"  Sách bổ sung: {link_text}")
        except Exception as e:
            print(f"Lỗi khi tìm sách bổ sung: {e}")
        
        # Phase 4: Lọc và làm sạch URLs
        print("Phase 4: Làm sạch danh sách URLs...")
        clean_book_urls = []
        for url in book_urls:
            # Loại bỏ parameters không cần thiết
            clean_url = url.split('?')[0].split('#')[0]
            
            # Chỉ giữ URLs hợp lệ
            if '/books/' in clean_url and clean_url not in clean_book_urls:
                clean_book_urls.append(clean_url)
        
        print(f"Tìm thấy {len(clean_book_urls)} sách unique từ Selenium")
        
        # Phase 5: Cào chi tiết từng sách
        print("Phase 5: Cào chi tiết từng sách...")
        for i, book_url in enumerate(clean_book_urls, 1):
            try:
                print(f"Đang cào sách {i}/{len(clean_book_urls)}: {book_url}")
                doc = self.scrape_openstax_book(book_url)
                if doc:
                    documents.append(doc)
                time.sleep(self.delay)
                
            except KeyboardInterrupt:
                print(f"Người dùng đã dừng quá trình cào sau {i-1}/{len(clean_book_urls)} sách...")
                # Lưu dữ liệu hiện tại vào MinIO
                if documents:
                    print(f"Lưu emergency backup do ngắt quá trình: {len(documents)} tài liệu")
                    self.save_to_minio(documents, "openstax", datetime.now().strftime("%Y-%m-%d"), "books_emergency")
                raise
                
            except Exception as e:
                print(f"Lỗi khi cào sách {book_url}: {e}")
                # Lưu dữ liệu hiện tại vào MinIO khi có lỗi
                if documents:
                    print(f"Lưu emergency backup: {len(documents)} tài liệu đã cào được")
                    self.save_to_minio(documents, "openstax", datetime.now().strftime("%Y-%m-%d"), "books_emergency")
                continue
        
        return documents
    
    def scrape_openstax_fallback(self) -> List[Dict[str, Any]]:
        """Fallback method cho OpenStax khi Selenium không hoạt động"""
        print("Fallback: Sử dụng requests để tìm sách OpenStax...")
        documents = []
        base_url = "https://openstax.org"
        
        # Thử cào trang chủ và subjects
        book_urls = []
        pages_to_check = [
            f"{base_url}/subjects",
            f"{base_url}/subjects/math", 
            f"{base_url}/subjects/science",
            f"{base_url}/subjects/social-sciences",
            f"{base_url}/subjects/humanities",
            f"{base_url}/subjects/business"
        ]
        
        for page_url in pages_to_check:
            print(f"Đang kiểm tra: {page_url}")
            soup = self.get_page_requests(page_url)
            if soup:
                # Tìm tất cả link có chứa "/books/"
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href')
                    if href and '/books/' in href:
                        full_url = urljoin(base_url, href)
                        if full_url not in book_urls:
                            book_urls.append(full_url)
                            print(f"Tìm thấy: {link.get_text(strip=True)} -> {href}")
        
        print(f"Tổng cộng tìm thấy {len(book_urls)} sách từ fallback method")
        
        for i, book_url in enumerate(book_urls, 1):
            try:
                print(f"Đang cào sách {i}/{len(book_urls)}: {book_url}")
                doc = self.scrape_openstax_book(book_url)
                if doc:
                    documents.append(doc)
                time.sleep(self.delay)
                
            except KeyboardInterrupt:
                print(f"Người dùng đã dừng fallback method sau {i-1}/{len(book_urls)} sách...")
                # Lưu dữ liệu hiện tại vào MinIO
                if documents:
                    print(f"Lưu emergency backup (fallback): {len(documents)} tài liệu")
                    self.save_to_minio(documents, "openstax_fallback", datetime.now().strftime("%Y-%m-%d"), "books_fallback")
                raise
                
            except Exception as e:
                print(f"Lỗi khi cào sách (fallback) {book_url}: {e}")
                # Lưu dữ liệu hiện tại vào MinIO khi có lỗi
                if documents:
                    print(f"Lưu emergency backup (fallback): {len(documents)} tài liệu đã cào được")
                    self.save_to_minio(documents, "openstax_fallback", datetime.now().strftime("%Y-%m-%d"), "books_fallback")
                continue
        
        return documents
    
    def scrape_openstax_book(self, url: str) -> Dict[str, Any]:
        """Cào thông tin một cuốn sách OpenStax"""
        soup = self.get_page(url)
        if not soup:
            return None
        
        try:
            # Title - thử nhiều cách
            title = "Unknown Book"
            
            # Method 1: Hero title
            title_elem = soup.select_one('h1.hero-title, h1[data-testid="hero-title"]')
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            # Method 2: Meta tags
            if title == "Unknown Book" or 'OpenStax' in title:
                meta_title = soup.find('meta', {'property': 'og:title'})
                if meta_title:
                    title = meta_title.get('content', '')
            
            # Method 3: Page title
            if title == "Unknown Book" or 'OpenStax' in title:
                page_title = soup.find('title')
                if page_title:
                    title = page_title.get_text(strip=True).replace(' | OpenStax', '')
            
            # Method 4: Từ URL
            if title == "Unknown Book" or 'OpenStax' in title:
                if '/books/' in url:
                    title = url.split('/books/')[-1].replace('-', ' ').title()
            
            # Description
            description = ""
            desc_selectors = [
                'meta[name="description"]',
                'meta[property="og:description"]', 
                '.book-description',
                '.description',
                '.hero-subtitle'
            ]
            
            for selector in desc_selectors:
                desc_elem = soup.select_one(selector)
                if desc_elem:
                    if desc_elem.name == 'meta':
                        description = desc_elem.get('content', '')
                    else:
                        description = desc_elem.get_text(strip=True)
                    if description and len(description) > 50:
                        break
            
            # Authors
            authors = []
            author_selectors = [
                '.loc-senior-author',  # Class chính cho senior author
                '.book-authors .author',
                '.authors .author', 
                '.contributor',
                '[data-testid="author"]'
            ]
            
            for selector in author_selectors:
                author_elems = soup.select(selector)
                if author_elems:
                    print(f"Tìm thấy {len(author_elems)} elements với selector: {selector}")
                for elem in author_elems:
                    author_text = elem.get_text(strip=True)
                    if author_text:
                        print(f"Raw author text từ {selector}: '{author_text}'")
                        # Sử dụng method parse_author_info để xử lý chính xác
                        parsed_authors = self.parse_author_info(author_text, selector)
                        print(f"Parsed authors: {parsed_authors}")
                        for author in parsed_authors:
                            if author and author not in authors:
                                authors.append(author)
            
            print(f"Final authors list: {authors}")
            
            # Tìm URL PDF nếu có
            pdf_url = ""
            try:
                # Tìm các link PDF phổ biến
                pdf_links = soup.find_all('a', href=True)
                for link in pdf_links:
                    href = link.get('href', '')
                    if '.pdf' in href.lower() or 'download' in href.lower():
                        if href.startswith('/'):
                            pdf_url = urljoin(url, href)
                        elif href.startswith('http'):
                            pdf_url = href
                        break
                        
                # Nếu không tìm thấy PDF link trực tiếp, thử tìm trong các button download
                if not pdf_url:
                    download_buttons = soup.find_all(['button', 'a'], text=lambda t: t and ('download' in t.lower() or 'pdf' in t.lower()))
                    for btn in download_buttons:
                        parent = btn.find_parent('a') or btn
                        href = parent.get('href', '')
                        if href and '.pdf' in href.lower():
                            if href.startswith('/'):
                                pdf_url = urljoin(url, href)
                            elif href.startswith('http'):
                                pdf_url = href
                            break
            except Exception as e:
                print(f"Lỗi khi tìm PDF URL: {e}")
            
            # Extract subject/category từ OpenStax
            subject = ""
            try:
                # Tìm subject từ breadcrumb
                breadcrumb = soup.find('nav', {'aria-label': 'breadcrumb'}) or soup.find('ol', class_='breadcrumb')
                if breadcrumb:
                    links = breadcrumb.find_all('a')
                    # Lấy item thứ 2 trong breadcrumb (thường là subject)
                    if len(links) >= 2:
                        subject = links[1].get_text(strip=True)
                
                # Nếu không có breadcrumb, thử tìm từ category tags
                if not subject:
                    category_selectors = [
                        '.book-category',
                        '.subject',
                        '[data-subject]',
                        '.book-subject',
                        '.category'
                    ]
                    for selector in category_selectors:
                        category_elem = soup.select_one(selector)
                        if category_elem:
                            subject = category_elem.get_text(strip=True)
                            break
                
                # Nếu vẫn không có, thử extract từ URL path
                if not subject:
                    url_path = urlparse(url).path
                    path_parts = [part for part in url_path.split('/') if part and part != 'books']
                    if path_parts:
                        # Lấy phần đầu tiên của path làm subject
                        potential_subject = path_parts[0].replace('-', ' ').title()
                        if len(potential_subject) > 2:  # Chỉ lấy nếu có ý nghĩa
                            subject = potential_subject
                
                # Nếu vẫn không có, thử tìm từ meta tags
                if not subject:
                    meta_subject = soup.find('meta', {'name': 'subject'}) or soup.find('meta', {'property': 'book:subject'})
                    if meta_subject:
                        subject = meta_subject.get('content', '').strip()
                
                # Fallback: tìm từ title nếu có pattern chung
                if not subject and title:
                    # Các subject phổ biến trong OpenStax
                    common_subjects = [
                        'Biology', 'Chemistry', 'Physics', 'Mathematics', 'Math',
                        'History', 'Psychology', 'Economics', 'Sociology', 
                        'Statistics', 'Algebra', 'Calculus', 'Anatomy', 'Physiology',
                        'Astronomy', 'Business', 'Literature', 'Writing', 'Philosophy'
                    ]
                    for subj in common_subjects:
                        if subj.lower() in title.lower():
                            subject = subj
                            break
                
                # Clean up subject
                if subject:
                    subject = subject.strip().title()
                    # Loại bỏ các từ không cần thiết
                    unwanted_words = ['Openstax', 'Book', 'Textbook', 'Free', 'Online']
                    for word in unwanted_words:
                        subject = subject.replace(word, '').strip()
                    subject = ' '.join(subject.split())  # Normalize spaces
                    
            except Exception as e:
                print(f"Lỗi khi extract subject: {e}")
            
            doc = {
                'id': self.create_doc_hash(url, title),
                'title': title,
                'description': description[:500] + "..." if len(description) > 500 else description,
                'authors': authors,
                'subject': subject,
                'source': 'OpenStax',
                'url': url,
                'url_pdf': pdf_url,
                'scraped_at': datetime.now().isoformat()
            }
            
            print(f"Đã cào: {title}")
            return doc
            
        except Exception as e:
            print(f"Lỗi khi cào {url}: {e}")
            return None
    
    def save_to_json(self, documents: List[Dict[str, Any]], filename: str):
        """Lưu documents ra file JSON"""
        if not documents:
            print(f"Không có dữ liệu để lưu vào {filename}")
            return
        
        filepath = os.path.join(self.output_dir, filename)
        
        output = {
            'total_documents': len(documents),
            'scraped_at': datetime.now().isoformat(),
            'source': documents[0]['source'] if documents else 'Unknown',
            'documents': documents
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"Đã lưu {len(documents)} tài liệu vào {filepath}")
    
    def save_to_minio(self, documents: List[Dict[str, Any]], source: str = "openstax", logical_date: str = None, file_type: str = "books"):
        """Lưu dữ liệu vào MinIO với đường dẫn có tổ chức"""
        if not self.minio_enable or not self.minio_client or not documents:
            print("MinIO không được bật hoặc không có dữ liệu để lưu")
            return ""
        
        if logical_date is None:
            logical_date = datetime.now().strftime("%Y-%m-%d")
        
        # Tạo đường dẫn có tổ chức cho OpenStax
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
            
            print(f"[MinIO] OpenStax saved: s3://{self.minio_bucket}/{object_name}")
            print(f"[MinIO] Total {len(documents)} books saved")
            
            return object_name
            
        except Exception as e:
            print(f"[MinIO] Lỗi khi lưu emergency backup: {e}")
            # Vẫn cố gắng xóa temp file
            try:
                os.remove(tmp_path)
            except:
                pass
            return ""
    
    def list_minio_backups(self, source: str = "openstax"):
        """Liệt kê các backup có sẵn trong MinIO"""
        if not self.minio_enable or not self.minio_client:
            print("MinIO không được bật")
            return []
        
        try:
            objects = self.minio_client.list_objects(self.minio_bucket, prefix=f"{source}/", recursive=True)
            backups = []
            for obj in objects:
                if obj.object_name.endswith('.jsonl'):
                    backups.append({
                        'path': obj.object_name,
                        'size': obj.size,
                        'last_modified': obj.last_modified,
                        'is_emergency': 'emergency_backup' in obj.object_name
                    })
            
            backups.sort(key=lambda x: x['last_modified'], reverse=True)
            
            print(f"Tìm thấy {len(backups)} backup files trong MinIO:")
            for backup in backups:
                backup_type = "EMERGENCY" if backup['is_emergency'] else "NORMAL"
                print(f"  [{backup_type}] {backup['path']} - {backup['size']} bytes - {backup['last_modified']}")
            
            return backups
            
        except Exception as e:
            print(f"[MinIO] Lỗi khi liệt kê backups: {e}")
            return []
    
    def restore_from_minio(self, object_name: str):
        """Khôi phục dữ liệu từ MinIO backup"""
        if not self.minio_enable or not self.minio_client:
            print("MinIO không được bật")
            return []
        
        try:
            # Tải file từ MinIO
            os.makedirs('/tmp', exist_ok=True) if os.name != 'nt' else os.makedirs('temp', exist_ok=True)
            tmp_dir = '/tmp' if os.name != 'nt' else 'temp'
            local_path = os.path.join(tmp_dir, f"restore_{int(time.time())}.jsonl")
            
            self.minio_client.fget_object(self.minio_bucket, object_name, local_path)
            
            # Đọc và parse dữ liệu
            documents = []
            with open(local_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        documents.append(json.loads(line))
            
            print(f"[MinIO] Đã khôi phục {len(documents)} tài liệu từ {object_name}")
            
            # Xóa temp file
            os.remove(local_path)
            
            return documents
            
        except Exception as e:
            print(f"[MinIO] Lỗi khi khôi phục từ backup: {e}")
            return []
    
    def run_openstax_scraper(self):
        """Chạy scraper chỉ cho OpenStax với error handling và MinIO backup"""
        print("BẮT ĐẦU CÀO OPENSTAX VỚI SELENIUM")
        
        start_time = time.time()
        openstax_docs = []
        
        try:
            # Cào OpenStax
            if self.use_selenium:
                openstax_docs = self.scrape_openstax_with_selenium()
            else:
                openstax_docs = self.scrape_openstax_fallback()
            
            # Lưu thành công vào file JSON và MinIO
            self.save_to_json(openstax_docs, "openstax_selenium_documents.json")
            
            # Lưu vào MinIO như final backup
            if openstax_docs:
                print("Lưu dữ liệu hoàn chỉnh vào MinIO...")
                self.save_to_minio(openstax_docs, "openstax", datetime.now().strftime("%Y-%m-%d"))
            
        except KeyboardInterrupt:
            print("\n=== NGẮT QUÁ TRÌNH BỞI NGƯỜI DÙNG ===")
            # Lưu dữ liệu hiện tại vào MinIO
            if openstax_docs:
                print(f"Lưu emergency backup: {len(openstax_docs)} tài liệu đã cào được")
                self.save_to_minio(openstax_docs, "openstax", datetime.now().strftime("%Y-%m-%d"))
                self.save_to_json(openstax_docs, "openstax_partial_documents.json")
            print("Dữ liệu đã được lưu trước khi thoát.")
            raise
            
        except Exception as e:
            print(f"\n=== LỖI NGHIÊM TRỌNG: {e} ===")
            # Lưu dữ liệu hiện tại vào MinIO
            if openstax_docs:
                print(f"Lưu emergency backup: {len(openstax_docs)} tài liệu đã cào được")
                self.save_to_minio(openstax_docs, "openstax", datetime.now().strftime("%Y-%m-%d"))
                self.save_to_json(openstax_docs, "openstax_error_backup.json")
            print("Dữ liệu đã được lưu trước khi báo lỗi.")
            raise
        
        finally:
            # Dọn dẹp
            self.cleanup()
        
        # Thống kê
        end_time = time.time()
        print(f"\nTHỐNG KÊ")
        print("=" * 30)
        print(f"Thời gian: {end_time - start_time:.1f} giây")
        print(f"OpenStax: {len(openstax_docs)} tài liệu")
        
        
        print(f"\nFile JSON đã được lưu: {self.output_dir}/openstax_selenium_documents.json")
        
        # Hiển thị thông tin MinIO backups
        if self.minio_enable:
            print("\n=== MINIO BACKUP STATUS ===")
            self.list_minio_backups("openstax")
    
    def cleanup(self):
        """Dọn dẹp resources"""
        if self.driver:
            print("Đang đóng Selenium...")
            self.driver.quit()


# # Usage example with error handling and MinIO backup
# if __name__ == "__main__":
#     # Thiết lập environment variables để bật MinIO (tùy chọn)
#     # os.environ['MINIO_ENABLE'] = '1'
#     # os.environ['MINIO_ENDPOINT'] = 'localhost:9000'
#     # os.environ['MINIO_ACCESS_KEY'] = 'minioadmin'
#     # os.environ['MINIO_SECRET_KEY'] = 'minioadmin'
#     # os.environ['MINIO_BUCKET'] = 'oer-raw'
    
#     scraper = AdvancedOERScraper(delay=1.0, use_selenium=True)
    
#     try:
#         # Chạy scraper với automatic backup
#         scraper.run_openstax_scraper()
        
#     except KeyboardInterrupt:
#         print("\nScraper đã bị ngắt bởi người dùng")
        
#         # Liệt kê các backup có sẵn
#         print("\nCác backup có sẵn trong MinIO:")
#         backups = scraper.list_minio_backups("openstax")
        
#         # Ví dụ khôi phục từ backup gần nhất
#         if backups:
#             latest_backup = backups[0]['path']
#             print(f"\nKhôi phục từ backup gần nhất: {latest_backup}")
#             recovered_docs = scraper.restore_from_minio(latest_backup)
#             if recovered_docs:
#                 scraper.save_to_json(recovered_docs, "openstax_recovered_documents.json")
#                 print(f"Đã khôi phục và lưu {len(recovered_docs)} tài liệu")
        
#     except Exception as e:
#         print(f"Lỗi không mong muốn: {e}")
        
#     finally:
#         scraper.cleanup()
