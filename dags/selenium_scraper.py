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
import concurrent.futures as futures
from requests.adapters import HTTPAdapter
from minio import Minio
from minio.error import S3Error
try:
    # urllib3 v2
    from urllib3.util.retry import Retry
    _RETRY_KW = {'allowed_methods': frozenset(['GET'])}
except Exception:  # pragma: no cover
    # urllib3 v1 fallback
    from urllib3.util import Retry
    _RETRY_KW = {'method_whitelist': frozenset(['GET'])}

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import StaleElementReferenceException, InvalidSessionIdException, WebDriverException

class AdvancedOERScraper:
    """Scraper nâng cao cho các tài liệu OER sử dụng Selenium"""
    
    def __init__(self, delay: float = 2.0, use_selenium: bool = True):
        # Session cho requests thông thường
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        # Connection pooling + retries for speed and robustness
        retry_cfg = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504], **_RETRY_KW)
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retry_cfg)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
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

        # PDF download output dir (optional)
        self.pdf_output_dir = os.getenv('PDF_DOWNLOAD_PATH', '/opt/airflow/scraped_pdfs/otl')
        try:
            os.makedirs(self.pdf_output_dir, exist_ok=True)
        except Exception:
            pass

        # MinIO client (optional)
        self.minio_client = None
        self.minio_bucket = os.getenv('MINIO_BUCKET', 'oer-raw')
        self.minio_enable = str(os.getenv('MINIO_ENABLE', '0')).lower() in {'1', 'true', 'yes'}
        if self.minio_enable:
            self.minio_client = Minio(
                endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
                access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
                secure=str(os.getenv('MINIO_SECURE', '0')).lower() in {'1', 'true', 'yes'}
            )
            try:
                if not self.minio_client.bucket_exists(self.minio_bucket):
                    self.minio_client.make_bucket(self.minio_bucket)
            except Exception as e:
                print(f"[MinIO] Bucket init error: {e}")

    def upload_raw_records_to_minio(self, records: List[Dict[str, Any]], source: str, logical_date: str) -> str:
        if not self.minio_enable or not self.minio_client or not records:
            return ''
        # Save to a local temp JSONL then upload
        prefix = f"{source}/{logical_date}"
        os.makedirs('/opt/airflow/tmp', exist_ok=True)
        tmp_path = f"/opt/airflow/tmp/{source}_{logical_date}.jsonl"
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                for r in records:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            object_name = f"{prefix}/data.jsonl"
            self.minio_client.fput_object(self.minio_bucket, object_name, tmp_path)
            print(f"[MinIO] Uploaded: s3://{self.minio_bucket}/{object_name}")
            return object_name
        except Exception as e:
            print(f"[MinIO] Upload JSONL error: {e}")
            return ''

    def upload_pdf_to_minio(self, local_path: str, source: str, logical_date: str) -> str:
        if not self.minio_enable or not self.minio_client or not local_path or not os.path.exists(local_path):
            return ''
        try:
            file_name = os.path.basename(local_path)
            object_name = f"{source}/{logical_date}/pdfs/{file_name}"
            self.minio_client.fput_object(self.minio_bucket, object_name, local_path)
            print(f"[MinIO] Uploaded PDF: s3://{self.minio_bucket}/{object_name}")
            return object_name
        except Exception as e:
            print(f"[MinIO] Upload PDF error: {e}")
            return ''
    
    def setup_selenium(self):
        """Thiết lập Selenium WebDriver"""
        try:
            print("Đang thiết lập Selenium...")
            
            chrome_options = Options()
            # Ưu tiên tải nhanh nội dung DOM
            try:
                chrome_options.page_load_strategy = 'eager'
            except Exception:
                pass
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
            chrome_options.add_argument("--blink-settings=imagesEnabled=false")
            # Tắt tải ảnh để nhanh hơn
            try:
                prefs = {"profile.managed_default_content_settings.images": 2}
                chrome_options.add_experimental_option("prefs", prefs)
            except Exception:
                pass
            
            # Ưu tiên Selenium Manager tự tìm chromedriver phù hợp với Chrome
            # Tránh dùng webdriver-manager vì có thể trả sai đường dẫn file (gây Exec format error)
            self.driver = webdriver.Chrome(options=chrome_options)
            print("Selenium đã sẵn sàng!")
            
        except Exception as e:
            print(f"Lỗi thiết lập Selenium: {e}")
            print("Sẽ sử dụng requests thay thế")
            self.use_selenium = False

    def restart_driver(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        self.driver = None
        self.setup_selenium()
    
    def get_page_selenium(self, url: str) -> BeautifulSoup:
        """Lấy nội dung trang web bằng Selenium"""
        try:
            print(f"[Selenium] Đang cào: {url}")
            self.driver.get(url)

            domain = urlparse(url).netloc
            if 'open.umn.edu' in domain:
                # OTL là trang tĩnh -> chỉ cần chờ body
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            else:
                # Với các trang JS nặng hơn (OpenStax) giữ logic cuộn nhanh
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(2)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(0.8)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.0)
                try:
                    load_more_buttons = self.driver.find_elements(By.XPATH, 
                        "//button[contains(text(), 'Load more') or contains(text(), 'Show more') or contains(text(), 'View all')]")
                    for button in load_more_buttons:
                        if button.is_displayed():
                            self.driver.execute_script("arguments[0].click();", button)
                            time.sleep(1.0)
                except Exception as e:
                    print(f"Không thể click Load more: {e}")
            
            html = self.driver.page_source
            return BeautifulSoup(html, 'html.parser')
            
        except Exception as e:
            print(f"Lỗi Selenium khi cào {url}: {e}")
            return None
    
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
        content = f"{url}_{title}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def extract_subjects(self, title: str, description: str = "") -> List[str]:
        """Trích xuất môn học từ title và description"""
        text = f"{title} {description}".lower()
        
        subjects = []
        subject_keywords = {
            'mathematics': ['math', 'algebra', 'calculus', 'geometry', 'statistics', 'mathematical', 'trigonometry'],
            'physics': ['physics', 'mechanics', 'thermodynamics', 'quantum', 'physical'],
            'chemistry': ['chemistry', 'organic', 'inorganic', 'biochemistry', 'chemical'],
            'biology': ['biology', 'anatomy', 'physiology', 'genetics', 'biological', 'microbiology'],
            'computer science': ['programming', 'computer', 'software', 'algorithm', 'coding'],
            'engineering': ['engineering', 'mechanical', 'electrical', 'civil'],
            'business': ['business', 'management', 'marketing', 'finance', 'economics', 'accounting', 'entrepreneurship'],
            'psychology': ['psychology', 'cognitive', 'behavioral', 'psychological'],
            'history': ['history', 'historical'],
            'literature': ['literature', 'english', 'writing', 'literary'],
            'medicine': ['medicine', 'medical', 'health', 'nursing'],
            'education': ['education', 'teaching', 'learning', 'educational'],
            'arts': ['art', 'music', 'painting', 'drawing', 'artistic'],
            'philosophy': ['philosophy', 'philosophical', 'ethics'],
            'government': ['government', 'political', 'politics', 'policy']
        }
        
        for subject, keywords in subject_keywords.items():
            if any(keyword in text for keyword in keywords):
                subjects.append(subject)
        
        return subjects if subjects else ['general']
    
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
            print(f"Đang cào sách {i}/{len(clean_book_urls)}: {book_url}")
            doc = self.scrape_openstax_book(book_url)
            if doc:
                documents.append(doc)
            time.sleep(self.delay)
        
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
            print(f"Đang cào sách {i}/{len(book_urls)}: {book_url}")
            doc = self.scrape_openstax_book(book_url)
            if doc:
                documents.append(doc)
            time.sleep(self.delay)
        
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
            
            # Subjects
            subjects = self.extract_subjects(title, description)
            
            doc = {
                'id': self.create_doc_hash(url, title),
                'title': title,
                'description': description[:500] + "..." if len(description) > 500 else description,
                'authors': authors,
                'subjects': subjects,
                'source': 'OpenStax',
                'url': url,
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
    
    def run_openstax_scraper(self):
        """Chạy scraper chỉ cho OpenStax"""
        print("BẮT ĐẦU CÀO OPENSTAX VỚI SELENIUM")
        print("=" * 50)
        
        start_time = time.time()
        
        # Cào OpenStax
        if self.use_selenium:
            openstax_docs = self.scrape_openstax_with_selenium()
        else:
            openstax_docs = self.scrape_openstax_fallback()
        
        self.save_to_json(openstax_docs, "openstax_selenium_documents.json")
        
        # Thống kê
        end_time = time.time()
        print(f"\nTHỐNG KÊ")
        print("=" * 30)
        print(f"Thời gian: {end_time - start_time:.1f} giây")
        print(f"OpenStax: {len(openstax_docs)} tài liệu")
        
        # Thống kê theo subjects
        subjects_count = {}
        for doc in openstax_docs:
            for subject in doc.get('subjects', []):
                subjects_count[subject] = subjects_count.get(subject, 0) + 1
        
        print(f"\nTOP SUBJECTS:")
        for subject, count in sorted(subjects_count.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  • {subject}: {count} tài liệu")
        
        print(f"\nFile JSON đã được lưu: {self.output_dir}/openstax_selenium_documents.json")
    
    def cleanup(self):
        """Dọn dẹp resources"""
        if self.driver:
            print("Đang đóng Selenium...")
            self.driver.quit()

    # ========================
    # Open Textbook Library (open.umn.edu)
    # ========================
    def _is_otl_book_url(self, url: str) -> bool:
        """Kiểm tra URL có phải trang sách hợp lệ của Open Textbook Library hay không."""
        if not url:
            return False
        if '/opentextbooks/textbooks/' not in url:
            return False
        blocked_slugs = {'newest', 'submit', 'in_development'}
        slug = url.rstrip('/').split('/')[-1]
        return slug not in blocked_slugs

    def _collect_otl_book_links_from_page(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Từ một trang bất kỳ của Open Textbook Library, thu thập các link sách hợp lệ"""
        book_urls: List[str] = []
        if not soup:
            return book_urls
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            full_url = urljoin(base_url, href) if href else ''
            clean_url = full_url.split('#')[0].split('?')[0]
            if self._is_otl_book_url(clean_url) and clean_url not in book_urls:
                book_urls.append(clean_url)
        return book_urls

    def _collect_otl_books_from_subject(self, subject_url: str, max_pages: int = 200) -> List[str]:
        """Thu thập tất cả link sách từ một subject.
        - Nếu Selenium khả dụng: mở trực tiếp trang subject, cuộn để lazy-load toàn bộ, sau đó lần theo nút Next nếu có.
        - Nếu không: fallback sang phương pháp listing có filter và phân trang.
        """
        base_url = "https://open.umn.edu"
        seen_urls: set[str] = set()

        if self.use_selenium and self.driver:
            # 1) Mở subject và lazy-load bằng cuộn trang
            try:
                self.driver.get(subject_url)
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except Exception:
                pass

            def collect_current_book_links() -> List[str]:
                # Tránh lỗi stale/invalid session bằng cách parse từ page_source và tự khôi phục phiên nếu mất
                urls: List[str] = []
                try:
                    html = self.driver.page_source
                except (InvalidSessionIdException, WebDriverException):
                    # Khôi phục driver và tải lại subject
                    self.restart_driver()
                    if self.use_selenium and self.driver:
                        try:
                            self.driver.get(subject_url)
                            WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                            html = self.driver.page_source
                        except Exception:
                            return urls
                    else:
                        return urls
                soup_local = BeautifulSoup(html, 'html.parser')
                for a in soup_local.find_all('a', href=True):
                    href = (a.get('href') or '').strip()
                    if not href:
                        continue
                    full = urljoin(base_url, href.split('#')[0])
                    clean = full.split('?')[0]
                    if self._is_otl_book_url(clean) and clean not in urls:
                        urls.append(clean)
                return urls

            def lazy_load_all_visible(max_scrolls: int = 80):
                last_count = 0
                stable_rounds = 0
                for _ in range(max_scrolls):
                    urls_now = collect_current_book_links()
                    if len(urls_now) <= last_count:
                        stable_rounds += 1
                    else:
                        stable_rounds = 0
                        last_count = len(urls_now)
                    # Scroll xuống đáy trang để kích hoạt lazy-load
                    try:
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    except (InvalidSessionIdException, WebDriverException):
                        # Khôi phục và thử lại một lần
                        self.restart_driver()
                        try:
                            self.driver.get(subject_url)
                            WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                        except Exception:
                            return
                    except Exception:
                        pass
                    time.sleep(0.8)
                    if stable_rounds >= 3:
                        break

            # Lazy-load và thu thập ở trang đầu
            lazy_load_all_visible()
            for u in collect_current_book_links():
                seen_urls.add(u)

            # 2) Theo nút Next (nếu có) để lấy tất cả trang trong subject
            for _ in range(max_pages):
                next_link_el = None
                try:
                    # rel="next" hoặc text-based 'Next'/'»'
                    next_link_el = self.driver.find_element(By.CSS_SELECTOR, "a[rel*='next']")
                except Exception:
                    pass
                if not next_link_el:
                    try:
                        candidates = self.driver.find_elements(By.XPATH, "//a[normalize-space()='Next' or normalize-space()='›' or normalize-space()='»']")
                        next_link_el = candidates[0] if candidates else None
                    except Exception:
                        next_link_el = None
                if not next_link_el:
                    break

                clicked = False
                try:
                    self.driver.execute_script("arguments[0].click();", next_link_el)
                    clicked = True
                except Exception:
                    try:
                        next_link_el.click()
                        clicked = True
                    except Exception:
                        clicked = False
                if not clicked:
                    break
                # Đợi trang kế tiếp render
                time.sleep(1.2)
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                except Exception:
                    pass
                # Lazy-load trang kế tiếp
                lazy_load_all_visible()
                for u in collect_current_book_links():
                    seen_urls.add(u)

            return sorted(seen_urls)

        # Fallback: phương pháp listing có filter
        soup = self.get_page_requests(subject_url)
        listing_urls: list[str] = []
        if soup:
            for a in soup.find_all('a', href=True):
                href = a.get('href') or ''
                full = urljoin(base_url, href)
                if '/opentextbooks/textbooks' in full and '?' in full:
                    if full not in listing_urls:
                        listing_urls.append(full)
        if not listing_urls:
            slug = subject_url.rstrip('/').split('/')[-1]
            candidate_params = ['subject', 'subjects', 'topic', 'discipline', 'term', 'term-subject']
            for p in candidate_params:
                listing_urls.append(f"{base_url}/opentextbooks/textbooks?{p}={slug}")
        for listing in list(dict.fromkeys(listing_urls)):
            links = self._collect_otl_books_from_listing(listing, max_pages=max_pages * 2)
            for u in links:
                if u not in seen_urls:
                    seen_urls.add(u)
            time.sleep(0.2)
        return sorted(seen_urls)

    def _collect_otl_books_from_listing(self, listing_url: str, max_pages: int = 500) -> List[str]:
        """Thu thập link sách từ trang listing như '/opentextbooks/textbooks/newest' với phân trang."""
        base_url = "https://open.umn.edu"
        seen_urls: set[str] = set()

        # Duyệt qua tham số page tuần tự
        for page in range(0, max_pages):
            url = listing_url
            if page > 0:
                joiner = '&' if ('?' in listing_url) else '?'
                url = f"{listing_url}{joiner}page={page}"
            soup = self.get_page_requests(url)
            links = self._collect_otl_book_links_from_page(soup, base_url)
            new_links = [u for u in links if u not in seen_urls]
            if not new_links:
                break
            for u in new_links:
                seen_urls.add(u)
            time.sleep(0.3)

        # Theo liên kết Next (nếu site không dùng page=? kiểu tuần tự)
        next_url = listing_url
        for _ in range(max_pages):
            soup = self.get_page_requests(next_url)
            if not soup:
                break
            for u in self._collect_otl_book_links_from_page(soup, base_url):
                if u not in seen_urls:
                    seen_urls.add(u)
            next_link = None
            link_tag = soup.find('a', rel=lambda v: v and 'next' in v)
            if link_tag and link_tag.get('href'):
                next_link = urljoin(base_url, link_tag['href'])
            else:
                for a in soup.find_all('a', href=True):
                    txt = (a.get_text() or '').strip().lower()
                    if txt in {'next', 'next ›', '›', '»'}:
                        next_link = urljoin(base_url, a['href'])
                        break
            if not next_link or next_link == next_url:
                break
            next_url = next_link
            time.sleep(0.3)

        return sorted(seen_urls)

    def _collect_otl_nav_links_from_page(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Thu thập các link điều hướng (subjects/listings) để tiếp tục crawl (không phải trang sách)."""
        nav_urls: List[str] = []
        if not soup:
            return nav_urls
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            full_url = urljoin(base_url, href)
            clean_url = full_url.split('#')[0]
            # Chỉ ở trong không gian /opentextbooks/
            if '/opentextbooks/' not in clean_url:
                continue
            # Bỏ qua trang sách
            if self._is_otl_book_url(clean_url):
                continue
            # Nhận diện nav pages: subjects, textbooks listings, filters
            if any(segment in clean_url for segment in ['/opentextbooks/subjects', '/opentextbooks/textbooks']):
                if clean_url not in nav_urls:
                    nav_urls.append(clean_url)
        return nav_urls

    def crawl_otl_all_book_urls(self, max_frontier: int = 5000) -> List[str]:
        """Crawl toàn bộ không gian /opentextbooks/ để gom link sách, dùng BFS frontier an toàn."""
        base_url = "https://open.umn.edu"
        seeds = [
            f"{base_url}/opentextbooks/",
            f"{base_url}/opentextbooks/subjects",
            f"{base_url}/opentextbooks/textbooks",
            f"{base_url}/opentextbooks/textbooks/newest",
        ]

        visited: set[str] = set()
        frontier: List[str] = []
        for s in seeds:
            frontier.append(s)
            visited.add(s)

        book_urls: set[str] = set()

        steps = 0
        while frontier and steps < max_frontier:
            current = frontier.pop(0)
            steps += 1
            print(f"[OTL] Crawl {steps}: {current}")
            soup = self.get_page_requests(current)

            # Thu thập sách ở trang hiện tại
            for u in self._collect_otl_book_links_from_page(soup, base_url):
                book_urls.add(u)

            # Mở rộng frontier bằng các nav links
            for nav in self._collect_otl_nav_links_from_page(soup, base_url):
                # Chuẩn hóa đơn giản để tránh lặp tham số
                nav_clean = nav.split('&page=')[0]
                if nav_clean not in visited:
                    visited.add(nav_clean)
                    frontier.append(nav_clean)

            time.sleep(0.2)

        print(f"[OTL] Crawl hoàn tất. Nav visited: {len(visited)}, tổng URL sách: {len(book_urls)}")
        return sorted(book_urls)

    def scrape_open_textbook_library_book(self, url: str) -> Dict[str, Any]:
        """Cào chi tiết một sách từ Open Textbook Library"""
        # Ưu tiên Selenium nếu đang bật, fallback requests
        soup = self.get_page(url)
        if not soup:
            return None

        try:
            # Title
            title = "Unknown Book"
            h1 = soup.find('h1')
            if h1:
                title = h1.get_text(strip=True)
            if title == "Unknown Book":
                og_title = soup.find('meta', {'property': 'og:title'})
                if og_title:
                    title = og_title.get('content', '').strip() or title
            if title == "Unknown Book":
                page_title = soup.find('title')
                if page_title:
                    title = page_title.get_text(strip=True)

            # Description
            description = ""
            desc_candidates = [
                ('meta', {'name': 'description'}),
                ('meta', {'property': 'og:description'}),
            ]
            for name, attrs in desc_candidates:
                elem = soup.find(name, attrs)
                if elem:
                    if name == 'meta':
                        description = elem.get('content', '').strip()
                    else:
                        description = elem.get_text(strip=True)
                    if len(description) > 30:
                        break

            if len(description) < 30:
                # Thử các khối nội dung thường gặp
                for selector in ['.book-description', '.field--name-body', '.description', '.content']:
                    elem = soup.select_one(selector)
                    if elem:
                        description = elem.get_text(strip=True)
                        if len(description) > 30:
                            break

            # Authors (nâng cao: ưu tiên JSON-LD/meta, sau đó đến selectors; loại bỏ tổ chức)
            def _collect_authors_from_ldjson(soup_obj) -> List[str]:
                names: List[str] = []
                for script in soup_obj.find_all('script', attrs={'type': 'application/ld+json'}):
                    try:
                        data = json.loads(script.string or '')
                    except Exception:
                        continue

                    def extract_from_obj(obj):
                        if isinstance(obj, dict):
                            if 'author' in obj:
                                return extract_from_obj(obj['author'])
                            if 'name' in obj and isinstance(obj['name'], str):
                                return [obj['name']]
                            # nested graph
                            if '@graph' in obj:
                                return extract_from_obj(obj['@graph'])
                        elif isinstance(obj, list):
                            acc = []
                            for it in obj:
                                acc.extend(extract_from_obj(it) or [])
                            return acc
                        elif isinstance(obj, str):
                            return [obj]
                        return []

                    for n in extract_from_obj(data) or []:
                        if n and n not in names:
                            names.append(n)
                return names

            def _cleanup_author_tokens(raw: str) -> List[str]:
                # Tách chuỗi theo "," hoặc " and " nhưng loại bỏ tổ chức sau dấu phẩy
                tokens = []
                if not raw:
                    return tokens
                text = raw.replace('By ', '').replace('by ', '').strip()
                # Cắt theo dòng nếu có xuống hàng
                lines = [ln.strip() for ln in re.split(r'\n|\r', text) if ln.strip()]
                pieces = []
                for ln in lines or [text]:
                    parts = [p.strip() for p in re.split(r' and |,', ln) if p.strip()]
                    # Giữ phần đầu nếu phần sau có vẻ là tổ chức
                    if len(parts) >= 2:
                        first = parts[0]
                        rest = ' '.join(parts[1:])
                        org_keys = ['university', 'college', 'institute', 'school', 'academy', 'polytechnic', 'tech', 'state']
                        if any(k in rest.lower() for k in org_keys):
                            pieces.append(first)
                            continue
                    pieces.extend(parts)
                # Lọc những token giống tên người
                cleaned = []
                for p in pieces:
                    if not p:
                        continue
                    # Loại bỏ token có 3+ từ tổ chức phổ biến
                    org_keys = ['university', 'college', 'institute', 'school', 'academy', 'press']
                    if any(k in p.lower() for k in org_keys):
                        continue
                    # Tên hợp lệ: có ít nhất 2 từ chữ cái
                    words = [w for w in re.split(r'\s+', p) if w]
                    if len(words) >= 2 and 2 <= len(p) <= 120:
                        cleaned.append(p)
                # Dedupe giữ thứ tự
                seen = set()
                out = []
                for c in cleaned:
                    if c not in seen:
                        seen.add(c)
                        out.append(c)
                return out

            authors: List[str] = []
            # 1) JSON-LD
            for n in _collect_authors_from_ldjson(soup):
                for a in _cleanup_author_tokens(n):
                    if a not in authors:
                        authors.append(a)
            # 2) Meta tags
            if not authors:
                for meta_name in ['author', 'book:author']:
                    for m in soup.find_all('meta', attrs={'name': meta_name}):
                        val = (m.get('content') or '').strip()
                        for a in _cleanup_author_tokens(val):
                            if a not in authors:
                                authors.append(a)
                    for m in soup.find_all('meta', attrs={'property': meta_name}):
                        val = (m.get('content') or '').strip()
                        for a in _cleanup_author_tokens(val):
                            if a not in authors:
                                authors.append(a)
            # 3) Structured selectors
            if not authors:
                selector_candidates = [
                    '.field--name-field-authors .field__item',
                    '.field--name-field-contributors .field__item',
                    '.contributors .field__item',
                    '.contributors', '.contributor', '.authors', '[itemprop="author"] [itemprop="name"]',
                    '[itemprop="author"]'
                ]
                for selector in selector_candidates:
                    for elem in soup.select(selector):
                        txt = elem.get_text(' ', strip=True)
                        for a in _cleanup_author_tokens(txt):
                            if a not in authors:
                                authors.append(a)
            # Cuối cùng: nếu vẫn rỗng, thử lấy rõ ràng mỗi dòng trong contributors block
            if not authors:
                contrib = soup.select_one('.contributors, .field--name-field-contributors')
                if contrib:
                    lines = [ln.strip() for ln in contrib.get_text('\n', strip=True).split('\n') if ln.strip()]
                    for ln in lines:
                        for a in _cleanup_author_tokens(ln):
                            if a not in authors:
                                authors.append(a)

            subjects = self.extract_subjects(title, description)

            # PDF link
            def _extract_pdf_link_from_soup(soup_obj) -> str:
                base_url = "https://open.umn.edu"
                # Tìm thẻ a có href chứa .pdf hoặc đường dẫn bitstreams thường dùng cho file
                for a in soup_obj.find_all('a', href=True):
                    href = (a.get('href') or '').strip()
                    text = (a.get_text() or '').strip().lower()
                    if not href:
                        continue
                    href_lower = href.lower()
                    is_pdf = href_lower.endswith('.pdf') or '/bitstreams/' in href_lower or 'format=pdf' in href_lower
                    is_pdf_text = ('pdf' in text) or ('download' in text)
                    if is_pdf or is_pdf_text:
                        return urljoin(base_url, href)
                return ''

            pdf_url = _extract_pdf_link_from_soup(soup) or ''

            # Optional: download PDF
            pdf_path = ''
            if pdf_url and str(os.getenv('OTL_DOWNLOAD_PDFS', '0')).lower() in {'1', 'true', 'yes'}:
                try:
                    safe_title = re.sub(r"[^\w\-\.]+", "_", title)[:120] or 'book'
                    file_name = f"{safe_title}_{self.create_doc_hash(pdf_url, title)[:8]}.pdf"
                    dest_path = os.path.join(self.pdf_output_dir, file_name)
                    # Skip if already exists
                    if not os.path.exists(dest_path):
                        with self.session.get(pdf_url, stream=True, timeout=60) as r:
                            r.raise_for_status()
                            with open(dest_path, 'wb') as f:
                                for chunk in r.iter_content(chunk_size=1024 * 256):
                                    if chunk:
                                        f.write(chunk)
                    pdf_path = dest_path
                except Exception as e:
                    print(f"[OTL] Lỗi tải PDF {pdf_url}: {e}")

            doc = {
                'id': self.create_doc_hash(url, title),
                'title': title,
                'description': description[:500] + "..." if len(description) > 500 else description,
                'authors': authors,
                'subjects': subjects,
                'source': 'Open Textbook Library',
                'url': url,
                'pdf_url': pdf_url or None,
                'pdf_path': pdf_path or None,
                'scraped_at': datetime.now().isoformat()
            }
            print(f"Đã cào OTL: {title}")
            return doc
        except Exception as e:
            print(f"Lỗi khi cào OTL {url}: {e}")
            return None

    def scrape_open_textbook_library(self) -> List[Dict[str, Any]]:
        """Cào toàn bộ sách từ Open Textbook Library (ưu tiên requests, Selenium nếu cần)"""
        base_url = "https://open.umn.edu"
        start_url = f"{base_url}/opentextbooks/"
        documents: List[Dict[str, Any]] = []

        # 1) Lấy danh sách subject (ưu tiên Selenium nếu bật)
        def _get_otl(url: str) -> BeautifulSoup:
            return self.get_page(url) if self.use_selenium else self.get_page_requests(url)

        soup = _get_otl(f"{base_url}/opentextbooks/subjects")
        subject_links: List[str] = []
        if soup:
            for a in soup.find_all('a', href=True):
                href = a.get('href')
                if not href:
                    continue
                full = urljoin(base_url, href.split('#')[0])
                # Chỉ lấy các link subject dạng /opentextbooks/subjects/<slug>
                if '/opentextbooks/subjects/' in full and not self._is_otl_book_url(full):
                    subject_links.append(full)
        # Dedupe và giữ thứ tự tương đối
        subject_links = list(dict.fromkeys(subject_links))

        # 2) Thu thập sách theo từng subject với phân trang
        book_urls: set[str] = set()
        for i, sub_url in enumerate(subject_links, 1):
            print(f"[OTL] Cào subject {i}/{len(subject_links)}: {sub_url}")
            links = self._collect_otl_books_from_subject(sub_url)
            for u in links:
                book_urls.add(u)
            time.sleep(0.5)

        # 3) Bổ sung từ listing tổng và 'newest' (phân trang đầy đủ)
        listing_url = f"{base_url}/opentextbooks/textbooks"
        print(f"[OTL] Cào listing tổng: {listing_url}")
        for u in self._collect_otl_books_from_listing(listing_url):
            book_urls.add(u)

        newest_url = f"{base_url}/opentextbooks/textbooks/newest"
        print(f"[OTL] Cào listing newest: {newest_url}")
        for u in self._collect_otl_books_from_listing(newest_url):
            book_urls.add(u)

        # 4) Crawl BFS toàn không gian /opentextbooks/
        for u in self.crawl_otl_all_book_urls():
            book_urls.add(u)

        # 5) Bổ sung từ trang chủ (nếu có sách nổi bật)
        home_soup = _get_otl(start_url)
        for u in self._collect_otl_book_links_from_page(home_soup, base_url):
            book_urls.add(u)

        print(f"[OTL] Tìm thấy {len(book_urls)} URL sách (sau phân trang)")

        urls = sorted(book_urls)
        if self.use_selenium and self.driver:
            # Dùng Selenium: duyệt tuần tự, không delay thừa
            for idx, url in enumerate(urls, 1):
                print(f"[OTL] ({idx}/{len(urls)}) {url}")
                doc = self.scrape_open_textbook_library_book(url)
                if doc:
                    documents.append(doc)
        else:
            # Requests mode: dùng concurrency
            max_workers = int(os.getenv('OTL_CONCURRENCY', '16'))
            print(f"[OTL] Bắt đầu cào chi tiết với {max_workers} luồng (requests)...")

            def _fetch(url: str):
                try:
                    return self.scrape_open_textbook_library_book(url)
                except Exception as e:
                    print(f"[OTL] Lỗi khi cào {url}: {e}")
                    return None

            with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                for idx, doc in enumerate(executor.map(_fetch, urls), 1):
                    if doc:
                        documents.append(doc)
                    if idx % 100 == 0:
                        print(f"[OTL] Đã xử lý {idx}/{len(urls)} sách")

        return documents

def main():
    print()
    
    scraper = AdvancedOERScraper(delay=2.0, use_selenium=True)
    
    try:
        scraper.run_openstax_scraper()
    finally:
        scraper.cleanup()

if __name__ == "__main__":
    main()
