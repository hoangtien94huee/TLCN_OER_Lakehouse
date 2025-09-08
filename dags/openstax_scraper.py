from typing import List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime
import time
import os
import json
import hashlib
from requests import Session
from requests.adapters import HTTPAdapter
from minio import Minio
try:
    # urllib3 v2
    from urllib3.util.retry import Retry
    _RETRY_KW = {'allowed_methods': frozenset(['GET'])}
except Exception:  # pragma: no cover
    # urllib3 v1 fallback
    from urllib3.util import Retry
    _RETRY_KW = {'method_whitelist': frozenset(['GET'])}

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class OpenStaxScraper:
    """Self-contained OpenStax scraper (no external core dependency)."""

    def __init__(self, delay: float = 2.0, use_selenium: bool = True):
        # HTTP session with retries
        self.session: Session = Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        })
        retry_cfg = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504], **_RETRY_KW)
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retry_cfg)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        # Selenium setup
        self.use_selenium = use_selenium
        self.driver = None
        if use_selenium:
            self.setup_selenium()

        self.delay = delay
        self.output_dir = "scraped_data"
        os.makedirs(self.output_dir, exist_ok=True)

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

    # ---- Core utilities ----
    def setup_selenium(self):
        try:
            print("Đang thiết lập Selenium...")
            chrome_options = Options()
            try:
                chrome_options.page_load_strategy = 'eager'
            except Exception:
                pass
            chrome_options.add_argument("--headless")
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
            try:
                prefs = {"profile.managed_default_content_settings.images": 2}
                chrome_options.add_experimental_option("prefs", prefs)
            except Exception:
                pass
            self.driver = webdriver.Chrome(options=chrome_options)
            print("Selenium đã sẵn sàng!")
        except Exception as e:
            print(f"Lỗi thiết lập Selenium: {e}")
            print("Sẽ sử dụng requests thay thế")
            self.use_selenium = False

    def cleanup(self):
        if self.driver:
            try:
                print("Đang đóng Selenium...")
                self.driver.quit()
            except Exception:
                pass

    def get_page_selenium(self, url: str) -> BeautifulSoup:
        try:
            print(f"[Selenium] Đang cào: {url}")
            self.driver.get(url)
            domain = urlparse(url).netloc
            if 'open.umn.edu' in domain:
                WebDriverWait(self.driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            else:
                WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(2)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(0.8)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.0)
                try:
                    load_more_buttons = self.driver.find_elements(By.XPATH, "//button[contains(text(), 'Load more') or contains(text(), 'Show more') or contains(text(), 'View all')]")
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
        try:
            print(f"[Requests] Đang cào: {url}")
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            return BeautifulSoup(r.text, 'html.parser')
        except Exception as e:
            print(f"Lỗi requests khi cào {url}: {e}")
            return None

    def get_page(self, url: str) -> BeautifulSoup:
        if self.use_selenium and self.driver:
            return self.get_page_selenium(url)
        return self.get_page_requests(url)

    def create_doc_hash(self, url: str, title: str) -> str:
        return hashlib.md5(f"{url}_{title}".encode()).hexdigest()

    def extract_subjects(self, title: str, description: str = "") -> List[str]:
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
            if any(k in text for k in keywords):
                subjects.append(subject)
        return subjects if subjects else ['general']

    def parse_author_info(self, author_text: str, selector_type: str = '') -> List[str]:
        authors: List[str] = []
        if not author_text:
            return authors
        if 'loc-senior-author' in selector_type:
            clean_text = author_text.strip()
            if clean_text.startswith('"') and clean_text.endswith('"'):
                clean_text = clean_text[1:-1]
            parts = [part.strip() for part in clean_text.split(',')]
            for i, part in enumerate(parts):
                if part:
                    clean_part = part.strip('"').strip("'").strip()
                    if i == 0:
                        if clean_part and clean_part not in authors:
                            authors.append(clean_part)
                    else:
                        university_keywords = ['university', 'college', 'institute', 'school', 'academy', 'polytechnic', 'tech', 'state']
                        is_institution = any(k.lower() in clean_part.lower() for k in university_keywords)
                        if not is_institution and len(clean_part) > 2:
                            words = clean_part.split()
                            if len(words) >= 2 and clean_part not in authors:
                                authors.append(clean_part)
        else:
            clean_text = author_text.strip('"').strip("'").strip()
            if ',' in clean_text and not any(keyword in clean_text.lower() for keyword in ['university', 'college', 'institute']):
                parts = [part.strip() for part in clean_text.split(',')]
                for part in parts:
                    if part and part not in authors:
                        authors.append(part)
            else:
                if clean_text and clean_text not in authors:
                    authors.append(clean_text)
        return authors

    # ---- MinIO helpers ----
    def upload_raw_records_to_minio(self, records: List[Dict[str, Any]], source: str, logical_date: str) -> str:
        if not self.minio_enable or not self.minio_client or not records:
            return ''
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

    def scrape_with_selenium(self) -> List[Dict[str, Any]]:
        """Cào OpenStax sử dụng Selenium để load JavaScript và tự động tìm tất cả sách"""
        print("Bắt đầu cào OpenStax với Selenium...")
        documents: List[Dict[str, Any]] = []

        if not self.use_selenium:
            print("Selenium không khả dụng, sử dụng method thay thế")
            return self.scrape_fallback()

        base_url = "https://openstax.org"
        book_urls = set()

        # Phase 1: Cào trang chủ subjects để tìm tất cả categories
        print("Phase 1: Tìm tất cả subject categories...")
        subjects_page = f"{base_url}/subjects"
        soup = self.get_page(subjects_page)

        subject_urls = [subjects_page]

        if soup:
            subject_links = soup.find_all('a', href=True)
            for link in subject_links:
                href = (link.get('href') or '').strip()
                if not href:
                    continue
                # Chuẩn hóa về absolute và lọc chặt chẽ theo domain + path '/subjects/'
                abs_url = urljoin(base_url, href).split('#')[0]
                parsed = urlparse(abs_url)
                if parsed.netloc != urlparse(base_url).netloc:
                    continue
                path = parsed.path or ''
                if not path.startswith('/subjects/'):
                    continue
                if '/opentextbooks/' in path:
                    continue
                if path in {'/subjects', '/subjects/'}:
                    continue
                if abs_url not in subject_urls:
                    subject_urls.append(abs_url)
                    print(f"Tìm thấy subject: {path}")

        # Phase 2: Cào từng subject page để tìm sách (bỏ qua hash URLs)
        print(f"Phase 2: Cào {len(subject_urls)} subject pages...")
        for i, subject_url in enumerate(subject_urls, 1):
            if '#' in subject_url:
                print(f"Bỏ qua hash URL {i}/{len(subject_urls)}: {subject_url}")
                continue

            print(f"Đang cào subject {i}/{len(subject_urls)}: {subject_url}")
            soup = self.get_page(subject_url)
            if not soup:
                continue

            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = (link.get('href') or '').strip()
                link_text = (link.get_text(strip=True) or '').strip()
                if not href:
                    continue
                full_url = urljoin(base_url, href).split('#')[0].split('?')[0]
                # Chỉ domain openstax.org và path chứa '/books/' hoặc '/details/books/'
                parsed = urlparse(full_url)
                if parsed.netloc != urlparse(base_url).netloc:
                    continue
                path = parsed.path or ''
                if '/opentextbooks/' in path:
                    continue
                if ('/books/' not in path and '/details/books/' not in path):
                    continue
                # Lọc text để tránh các CTA không phải tên sách
                lt = (link_text or '').lower()
                if any(k in lt for k in ['view online','read online','access','download','instructor','student','more','click','get','resources']):
                    continue
                if 10 < len(link_text) < 120:
                    before = len(book_urls)
                    book_urls.add(full_url)
                    if len(book_urls) > before:
                        print(f"  Tìm thấy sách: {link_text} -> {full_url}")

            time.sleep(1)

        # Phase 3: Thử cào thêm từ sitemap hoặc API nếu có
        print("Phase 3: Tìm kiếm bổ sung...")
        try:
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
                        href = (link.get('href') or '').strip()
                        link_text = (link.get_text(strip=True) or '').strip()
                        if not href:
                            continue
                        full_url = urljoin(base_url, href).split('#')[0].split('?')[0]
                        parsed = urlparse(full_url)
                        if parsed.netloc != urlparse(base_url).netloc:
                            continue
                        path = parsed.path or ''
                        if '/opentextbooks/' in path:
                            continue
                        if ('/books/' not in path and '/details/books/' not in path):
                            continue
                        lt = (link_text or '').lower()
                        if any(k in lt for k in ['view online','read online','access','download','instructor','student','more','click','get','resources']):
                            continue
                        if 10 < len(link_text) < 100:
                            if full_url not in book_urls:
                                book_urls.add(full_url)
                                print(f"  Sách bổ sung: {link_text} -> {full_url}")
        except Exception as e:
            print(f"Lỗi khi tìm sách bổ sung: {e}")

        # Phase 4: Lọc và làm sạch URLs
        print("Phase 4: Làm sạch danh sách URLs...")
        clean_book_urls: List[str] = []
        for url in book_urls:
            clean_url = url.split('?')[0].split('#')[0]
            if '/books/' in clean_url and clean_url not in clean_book_urls:
                clean_book_urls.append(clean_url)

        print(f"Tìm thấy {len(clean_book_urls)} sách unique từ Selenium")

        # Phase 5: Cào chi tiết từng sách
        print("Phase 5: Cào chi tiết từng sách...")
        # Optional max books for quick tests
        max_books = int(os.getenv('OPENSTAX_MAX_BOOKS', '0')) or None
        urls_to_fetch = clean_book_urls[:max_books] if max_books else clean_book_urls
        for i, book_url in enumerate(urls_to_fetch, 1):
            print(f"Đang cào sách {i}/{len(clean_book_urls)}: {book_url}")
            doc = self.scrape_book(book_url)
            if doc:
                documents.append(doc)
            time.sleep(self.delay)

        return documents

    def scrape_fallback(self) -> List[Dict[str, Any]]:
        """Fallback method cho OpenStax khi Selenium không hoạt động"""
        print("Fallback: Sử dụng requests để tìm sách OpenStax...")
        documents: List[Dict[str, Any]] = []
        base_url = "https://openstax.org"

        book_urls: List[str] = []
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
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href')
                    if href and '/books/' in href:
                        full_url = urljoin(base_url, href)
                        if full_url not in book_urls:
                            book_urls.append(full_url)
                            print(f"Tìm thấy: {link.get_text(strip=True)} -> {href}")

        print(f"Tổng cộng tìm thấy {len(book_urls)} sách từ fallback method")

        # Optional max books for quick tests
        max_books = int(os.getenv('OPENSTAX_MAX_BOOKS', '0')) or None
        urls_to_fetch = book_urls[:max_books] if max_books else book_urls
        for i, book_url in enumerate(urls_to_fetch, 1):
            print(f"Đang cào sách {i}/{len(book_urls)}: {book_url}")
            doc = self.scrape_book(book_url)
            if doc:
                documents.append(doc)
            time.sleep(self.delay)

        return documents

    def scrape_book(self, url: str) -> Dict[str, Any]:
        """Cào thông tin một cuốn sách OpenStax"""
        soup = self.get_page(url)
        if not soup:
            return None

        try:
            title = "Unknown Book"

            title_elem = soup.select_one('h1.hero-title, h1[data-testid="hero-title"]')
            if title_elem:
                title = title_elem.get_text(strip=True)

            if title == "Unknown Book" or 'OpenStax' in title:
                meta_title = soup.find('meta', {'property': 'og:title'})
                if meta_title:
                    title = meta_title.get('content', '')

            if title == "Unknown Book" or 'OpenStax' in title:
                page_title = soup.find('title')
                if page_title:
                    title = page_title.get_text(strip=True).replace(' | OpenStax', '')

            if title == "Unknown Book" or 'OpenStax' in title:
                if '/books/' in url:
                    title = url.split('/books/')[-1].replace('-', ' ').title()

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

            authors: List[str] = []
            author_selectors = [
                '.loc-senior-author',
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
                        parsed_authors = self.parse_author_info(author_text, selector)
                        print(f"Parsed authors: {parsed_authors}")
                        for author in parsed_authors:
                            if author and author not in authors:
                                authors.append(author)

            print(f"Final authors list: {authors}")

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


