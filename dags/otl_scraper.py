from typing import List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import os
import re
import json
import hashlib
from datetime import datetime
from requests import Session
from requests.adapters import HTTPAdapter
from minio import Minio
try:
    from urllib3.util.retry import Retry
    _RETRY_KW = {'allowed_methods': frozenset(['GET'])}
except Exception:  # pragma: no cover
    from urllib3.util import Retry
    _RETRY_KW = {'method_whitelist': frozenset(['GET'])}

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException


class OTLScraper:
    """Self-contained OTL scraper (no external core dependency)."""

    def __init__(self, delay: float = 0.0, use_selenium: bool = True):
        self.session: Session = Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        })
        retry_cfg = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504], **_RETRY_KW)
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retry_cfg)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        self.use_selenium = use_selenium
        self.driver = None
        if use_selenium:
            self.setup_selenium()

        self.delay = delay
        self.output_dir = "scraped_data"
        self.pdf_output_dir = os.getenv('PDF_DOWNLOAD_PATH', '/opt/airflow/scraped_pdfs/otl')
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            os.makedirs(self.pdf_output_dir, exist_ok=True)
        except Exception:
            pass

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

    # Core utilities
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

    def get_page(self, url: str) -> BeautifulSoup:
        if self.use_selenium and self.driver:
            try:
                self.driver.get(url)
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                html = self.driver.page_source
                return BeautifulSoup(html, 'html.parser')
            except Exception:
                return self.get_page_requests(url)
        return self.get_page_requests(url)

    def get_page_requests(self, url: str) -> BeautifulSoup:
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            return BeautifulSoup(r.text, 'html.parser')
        except Exception:
            return None

    def create_doc_hash(self, url: str, title: str) -> str:
        return hashlib.md5(f"{url}_{title}".encode()).hexdigest()

    def extract_subjects(self, title: str, description: str = "") -> List[str]:
        text = f"{title} {description}".lower()
        subjects: List[str] = []
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

    def scrape_open_textbook_library(self) -> List[Dict[str, Any]]:
        base_url = "https://open.umn.edu"
        start_url = f"{base_url}/opentextbooks/"
        documents: List[Dict[str, Any]] = []

        def _get_otl(url: str) -> BeautifulSoup:
            return self.get_page(url)

        soup = _get_otl(f"{base_url}/opentextbooks/subjects")
        subject_links: List[str] = []
        if soup:
            for a in soup.find_all('a', href=True):
                href = a.get('href')
                if not href:
                    continue
                full = urljoin(base_url, href.split('#')[0])
                if '/opentextbooks/subjects/' in full and not self._is_otl_book_url(full):
                    subject_links.append(full)
        subject_links = list(dict.fromkeys(subject_links))

        book_urls: set[str] = set()
        # Optional limit number of subjects to crawl for quick tests
        max_subjects = int(os.getenv('OTL_MAX_SUBJECTS', '0')) or None
        limited_subject_links = subject_links[:max_subjects] if max_subjects else subject_links
        for i, sub_url in enumerate(limited_subject_links, 1):
            print(f"[OTL] Cào subject {i}/{len(subject_links)}: {sub_url}")
            # Limit pages per subject if configured
            max_pages_per_subject = int(os.getenv('OTL_MAX_PAGES_PER_SUBJECT', '0')) or 200
            links = self._collect_books_from_subject(sub_url, max_pages=max_pages_per_subject)
            for u in links:
                book_urls.add(u)
            time.sleep(0.5)

        display_names = self._collect_subject_display_names()
        for name in display_names:
            url = f"{base_url}/opentextbooks/textbooks?subjects={name}"
            for u in self._collect_books_from_listing(url):
                book_urls.add(u)

        listing_url = f"{base_url}/opentextbooks/textbooks"
        print(f"[OTL] Cào listing tổng: {listing_url}")
        for u in self._collect_books_from_listing(listing_url):
            book_urls.add(u)

        newest_url = f"{base_url}/opentextbooks/textbooks/newest"
        print(f"[OTL] Cào listing newest: {newest_url}")
        for u in self._collect_books_from_listing(newest_url):
            book_urls.add(u)

        # Optional BFS frontier limit
        max_frontier = int(os.getenv('OTL_MAX_FRONTIER', '0')) or 5000
        for u in self.crawl_all_book_urls(max_frontier=max_frontier):
            book_urls.add(u)

        home_soup = _get_otl(start_url)
        for u in self._collect_book_links_from_page(home_soup, base_url):
            book_urls.add(u)

        print(f"[OTL] Tìm thấy {len(book_urls)} URL sách (sau phân trang)")

        urls = sorted(book_urls)
        # Optional limit total books to fetch details
        max_books = int(os.getenv('OTL_MAX_BOOKS', '0')) or None
        if max_books:
            urls = urls[:max_books]
        if self.use_selenium and self.driver:
            for idx, url in enumerate(urls, 1):
                print(f"[OTL] ({idx}/{len(urls)}) {url}")
                doc = self.scrape_open_textbook_library_book(url)
                if doc:
                    documents.append(doc)
        else:
            from concurrent import futures
            max_workers = int(os.getenv('OTL_CONCURRENCY', '16'))
            print(f"[OTL] Bắt đầu cào chi tiết với {max_workers} luồng (requests)...")

            def _fetch(u: str):
                try:
                    return self.scrape_open_textbook_library_book(u)
                except Exception as e:
                    print(f"[OTL] Lỗi khi cào {u}: {e}")
                    return None

            with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                for idx, doc in enumerate(executor.map(_fetch, urls), 1):
                    if doc:
                        documents.append(doc)
                    if idx % 100 == 0:
                        print(f"[OTL] Đã xử lý {idx}/{len(urls)} sách")

        return documents

    # ============ Core OTL logic moved here ============
    def _is_otl_book_url(self, url: str) -> bool:
        if not url:
            return False
        if '/opentextbooks/textbooks/' not in url:
            return False
        blocked_slugs = {'newest', 'submit', 'in_development'}
        slug = url.rstrip('/').split('/')[-1]
        return slug not in blocked_slugs

    def _collect_book_links_from_page(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        book_urls: List[str] = []
        if not soup:
            return book_urls
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            full_url = urljoin(base_url, href) if href else ''
            clean_url = full_url.split('#')[0].split('?')[0]
            if self._is_otl_book_url(clean_url) and clean_url not in book_urls:
                book_urls.append(clean_url)
        for link in soup.find_all('link', href=True):
            rel = (link.get('rel') or '')
            rel_str = ' '.join(rel) if isinstance(rel, list) else str(rel)
            typ = (link.get('type') or '')
            href = link.get('href')
            if href and 'alternate' in rel_str and ('text/html' in typ or typ == '' or typ is None):
                full_url = urljoin(base_url, href)
                clean_url = full_url.split('#')[0].split('?')[0]
                if self._is_otl_book_url(clean_url) and clean_url not in book_urls:
                    book_urls.append(clean_url)
        return book_urls

    def _collect_books_from_subject(self, subject_url: str, max_pages: int = 200) -> List[str]:
        base_url = "https://open.umn.edu"
        seen_urls: set[str] = set()
        if self.use_selenium and self.driver:
            try:
                self.driver.get(subject_url)
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            except Exception:
                pass

            def collect_current_book_links() -> List[str]:
                urls: List[str] = []
                try:
                    html = self.driver.page_source
                except (InvalidSessionIdException, WebDriverException):
                    try:
                        self.setup_selenium()
                        self.driver.get(subject_url)
                        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                        html = self.driver.page_source
                    except Exception:
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

            def lazy_load_all_visible(max_scrolls: int = 200):
                last_count = 0
                stable_rounds = 0
                for _ in range(max_scrolls):
                    urls_now = collect_current_book_links()
                    if len(urls_now) <= last_count:
                        stable_rounds += 1
                    else:
                        stable_rounds = 0
                        last_count = len(urls_now)
                    try:
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        btns = []
                        try:
                            btns = self.driver.find_elements(By.XPATH, "//button[contains(., 'Load more') or contains(., 'Show more') or contains(., 'More')]")
                        except Exception:
                            btns = []
                        for b in btns:
                            try:
                                if b.is_displayed():
                                    self.driver.execute_script("arguments[0].click();", b)
                                    time.sleep(0.6)
                            except Exception:
                                pass
                    except (InvalidSessionIdException, WebDriverException):
                        try:
                            self.setup_selenium()
                            self.driver.get(subject_url)
                            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                        except Exception:
                            return
                    except Exception:
                        pass
                    time.sleep(0.8)
                    if stable_rounds >= 3:
                        break

            lazy_load_all_visible()
            for u in collect_current_book_links():
                seen_urls.add(u)

            for _ in range(max_pages):
                next_link_el = None
                try:
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
                time.sleep(1.2)
                try:
                    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                except Exception:
                    pass
                lazy_load_all_visible()
                for u in collect_current_book_links():
                    seen_urls.add(u)
            return sorted(seen_urls)

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
            candidate_params = ['subject', 'subjects', 'topic', 'discipline', 'term', 'term-subject', 'taxonomy', 'category', 'categories']
            for p in candidate_params:
                listing_urls.append(f"{base_url}/opentextbooks/textbooks?{p}={slug}")
            pretty = slug.replace('-', ' ').title()
            listing_urls.append(f"{base_url}/opentextbooks/textbooks?subjects={pretty}")
        for listing in list(dict.fromkeys(listing_urls)):
            links = self._collect_books_from_listing(listing, max_pages=max_pages * 2)
            for u in links:
                if u not in seen_urls:
                    seen_urls.add(u)
            time.sleep(0.2)
        return sorted(seen_urls)

    def _collect_books_from_listing(self, listing_url: str, max_pages: int = 1000) -> List[str]:
        base_url = "https://open.umn.edu"
        seen_urls: set[str] = set()
        for page in range(0, max_pages):
            url = listing_url
            if page > 0:
                joiner = '&' if ('?' in listing_url) else '?'
                url = f"{listing_url}{joiner}page={page}"
            soup = self.get_page_requests(url)
            links = self._collect_book_links_from_page(soup, base_url)
            new_links = [u for u in links if u not in seen_urls]
            if not new_links:
                break
            for u in new_links:
                seen_urls.add(u)
            time.sleep(0.3)
        next_url = listing_url
        for _ in range(max_pages):
            soup = self.get_page_requests(next_url)
            if not soup:
                break
            for u in self._collect_book_links_from_page(soup, base_url):
                if u not in seen_urls:
                    seen_urls.add(u)
            next_link = None
            link_tag = soup.find('a', rel=lambda v: v and 'next' in v)
            if link_tag and link_tag.get('href'):
                next_link = urljoin(base_url, link_tag['href'])
            else:
                for a in soup.find_all('a', href=True):
                    txt = (a.get_text() or '').strip().lower()
                    if txt in {'next', 'next ›', '›', '»', 'more', 'older'}:
                        next_link = urljoin(base_url, a['href'])
                        break
            if not next_link or next_link == next_url:
                break
            next_url = next_link
            time.sleep(0.3)
        return sorted(seen_urls)

    def _collect_nav_links_from_page(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        nav_urls: List[str] = []
        if not soup:
            return nav_urls
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            full_url = urljoin(base_url, href)
            clean_url = full_url.split('#')[0]
            if '/opentextbooks/' not in clean_url:
                continue
            if self._is_otl_book_url(clean_url):
                continue
            if any(segment in clean_url for segment in ['/opentextbooks/subjects', '/opentextbooks/textbooks']):
                if clean_url not in nav_urls:
                    nav_urls.append(clean_url)
        return nav_urls

    def crawl_all_book_urls(self, max_frontier: int = 5000) -> List[str]:
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
            for u in self._collect_book_links_from_page(soup, base_url):
                book_urls.add(u)
            for nav in self._collect_nav_links_from_page(soup, base_url):
                nav_clean = nav.split('&page=')[0]
                if nav_clean not in visited:
                    visited.add(nav_clean)
                    frontier.append(nav_clean)
            time.sleep(0.2)
        print(f"[OTL] Crawl hoàn tất. Nav visited: {len(visited)}, tổng URL sách: {len(book_urls)}")
        return sorted(book_urls)

    def _collect_subject_display_names(self) -> List[str]:
        base_url = "https://open.umn.edu"
        names: list[str] = []
        soup = self.get_page(f"{base_url}/opentextbooks/subjects")
        if not soup:
            return names
        for a in soup.find_all('a', href=True):
            href = (a.get('href') or '').strip()
            text = (a.get_text() or '').strip()
            if not text:
                continue
            if '/opentextbooks/subjects/' in href:
                if text not in names:
                    names.append(text)
        return names

    def scrape_open_textbook_library_book(self, url: str) -> Dict[str, Any]:
        soup = self.get_page(url)
        if not soup:
            return None
        try:
            def _clean_description_noise(text: str) -> str:
                if not text:
                    return ''
                noise_patterns = [
                    r"Stay Updated[\s\S]*$",
                    r"CENTER FOR OPEN EDUCATION[\s\S]*$",
                    r"University of Minnesota[\s\S]*$",
                    r"Creative Commons Attribution[\s\S]*$",
                    r"Join Our Newsletter[\s\S]*$",
                    r"Bluesky|Mastodon|LinkedIn|YouTube"
                ]
                cleaned = text
                for pat in noise_patterns:
                    cleaned = re.sub(pat, "", cleaned, flags=re.IGNORECASE)
                return re.sub(r"\s+", " ", cleaned).strip()

            def _collect_official_subjects(soup_obj: BeautifulSoup) -> List[str]:
                subjects: List[str] = []
                for a in soup_obj.find_all('a', href=True):
                    href = (a.get('href') or '').strip()
                    if '/opentextbooks/subjects/' in href:
                        name = (a.get_text() or '').strip().lower()
                        if name and name not in subjects:
                            subjects.append(name)
                for sel in [
                    '.field--name-field-subjects a',
                    '.taxonomy-term a[href*="/opentextbooks/subjects/"]',
                    'nav.breadcrumb a[href*="/opentextbooks/subjects/"]']:
                    for a in soup_obj.select(sel):
                        name = (a.get_text() or '').strip().lower()
                        if name and name not in subjects:
                            subjects.append(name)
                return subjects

            def _extract_metadata_from_ldjson(soup_obj: BeautifulSoup) -> Dict[str, Any]:
                meta: Dict[str, Any] = {}
                for script in soup_obj.find_all('script', attrs={'type': 'application/ld+json'}):
                    try:
                        data = json.loads(script.string or '')
                    except Exception:
                        continue
                    def walk(obj):
                        if isinstance(obj, dict):
                            if obj.get('@type') in {'Book', 'CreativeWork'}:
                                if isinstance(obj.get('isbn'), str):
                                    meta.setdefault('isbn', obj['isbn'])
                                if isinstance(obj.get('datePublished'), str):
                                    meta.setdefault('date_published', obj['datePublished'])
                                if isinstance(obj.get('inLanguage'), str):
                                    meta.setdefault('language', obj['inLanguage'])
                                pub = obj.get('publisher')
                                if pub:
                                    if isinstance(pub, dict) and 'name' in pub:
                                        meta.setdefault('publisher', pub.get('name'))
                                    elif isinstance(pub, str):
                                        meta.setdefault('publisher', pub)
                                lic = obj.get('license')
                                if isinstance(lic, str):
                                    meta.setdefault('license_url', lic)
                            for v in obj.values():
                                walk(v)
                        elif isinstance(obj, list):
                            for it in obj:
                                walk(it)
                    walk(data)
                return meta

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
            if title == "Unknown Book":
                alt = soup.select_one('h1.page-title, .node__title, [itemprop="name"]')
                if alt:
                    t = alt.get_text(strip=True)
                    if t:
                        title = t

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
                for selector in ['.book-description', '.field--name-body', '.description', '.content']:
                    elem = soup.select_one(selector)
                    if elem:
                        description = elem.get_text('\n', strip=True)
                        if len(description) > 30:
                            break
            description = _clean_description_noise(description)

            # Authors
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
                tokens = []
                if not raw:
                    return tokens
                text = raw.replace('By ', '').replace('by ', '').strip()
                lines = [ln.strip() for ln in re.split(r'\n|\r', text) if ln.strip()]
                pieces = []
                for ln in lines or [text]:
                    parts = [p.strip() for p in re.split(r' and |,', ln) if p.strip()]
                    if len(parts) >= 2:
                        first = parts[0]
                        rest = ' '.join(parts[1:])
                        org_keys = ['university', 'college', 'institute', 'school', 'academy', 'polytechnic', 'tech', 'state']
                        if any(k in rest.lower() for k in org_keys):
                            pieces.append(first)
                            continue
                    pieces.extend(parts)
                cleaned = []
                for p in pieces:
                    if not p:
                        continue
                    org_keys = ['university', 'college', 'institute', 'school', 'academy', 'press']
                    if any(k in p.lower() for k in org_keys):
                        continue
                    words = [w for w in re.split(r'\s+', p) if w]
                    if len(words) >= 2 and 2 <= len(p) <= 120:
                        cleaned.append(p)
                seen = set()
                out = []
                for c in cleaned:
                    if c not in seen:
                        seen.add(c)
                        out.append(c)
                return out

            authors: List[str] = []
            for n in _collect_authors_from_ldjson(soup):
                for a in _cleanup_author_tokens(n):
                    if a not in authors:
                        authors.append(a)
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
            if not authors:
                contrib = soup.select_one('.contributors, .field--name-field-contributors')
                if contrib:
                    lines = [ln.strip() for ln in contrib.get_text('\n', strip=True).split('\n') if ln.strip()]
                    for ln in lines:
                        for a in _cleanup_author_tokens(ln):
                            if a not in authors:
                                authors.append(a)

            def _collect_official_subjects_or_guess() -> List[str]:
                subjects = _collect_official_subjects(soup)
                return subjects if subjects else self.extract_subjects(title, description)

            # Subject extraction from title/description as in OpenStax file
            def _extract_subjects_from_text(text_title: str, text_desc: str) -> List[str]:
                tx = f"{text_title} {text_desc}".lower()
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
                    if any(k in tx for k in keywords):
                        subjects.append(subject)
                return subjects if subjects else ['general']

            # Use official taxonomy if available; otherwise, heuristic
            subjects = _collect_official_subjects_or_guess() if callable(_collect_official_subjects) else _extract_subjects_from_text(title, description)

            def _extract_pdf_link_from_soup(soup_obj) -> str:
                base_url = "https://open.umn.edu"
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
            pdf_path = ''
            if pdf_url and str(os.getenv('OTL_DOWNLOAD_PDFS', '0')).lower() in {'1', 'true', 'yes'}:
                try:
                    safe_title = re.sub(r"[^\w\-\.]+", "_", title)[:120] or 'book'
                    file_name = f"{safe_title}_{self.create_doc_hash(pdf_url, title)[:8]}.pdf"
                    dest_path = os.path.join(self.pdf_output_dir, file_name)
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
            # Metadata extras
            meta = _extract_metadata_from_ldjson(soup)
            for k in ['license', 'license_url', 'language', 'publisher', 'isbn', 'date_published']:
                if meta.get(k):
                    doc[k] = meta[k]
            return doc
        except Exception as e:
            print(f"Lỗi khi cào OTL {url}: {e}")
            return None


