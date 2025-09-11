from typing import List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
from datetime import datetime
import os
import time
import random

# Selenium imports for live mode
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
except Exception:
    webdriver = None


BASE_URL = "https://open.umn.edu"

# Throttling/backoff cấu hình qua ENV (mặc định an toàn)
DEFAULT_DELAY_BASE_SEC = float(os.getenv('OTL_DELAY_BASE_SEC', '0.8'))
DEFAULT_DELAY_JITTER_SEC = float(os.getenv('OTL_DELAY_JITTER_SEC', '0.4'))
RETRY_BACKOFF_BASE_SEC = float(os.getenv('OTL_RETRY_BACKOFF_BASE_SEC', '2.0'))
RETRY_BACKOFF_MAX_SEC = float(os.getenv('OTL_RETRY_BACKOFF_MAX_SEC', '10.0'))
WAIT_UNTIL_CLEAR = (os.getenv('OTL_WAIT_UNTIL_CLEAR', 'false').lower() in ['1','true','yes'])
WAIT_MAX_MINUTES = int(os.getenv('OTL_WAIT_MAX_MINUTES', '15'))
PER_BOOK_DELAY_SEC = float(os.getenv('OTL_PER_BOOK_DELAY_SEC', '0.6'))
SUBJECT_COOLDOWN_SEC = float(os.getenv('OTL_SUBJECT_COOLDOWN_SEC', '3.0'))
FORCE_RANDOM_UA = (os.getenv('OTL_FORCE_RANDOM_UA', 'true').lower() in ['1','true','yes'])
CUSTOM_UA = os.getenv('OTL_USER_AGENT', '').strip()
PROXY_SERVER = os.getenv('OTL_PROXY', '').strip()
BOOK_MAX_ATTEMPTS = int(os.getenv('OTL_BOOK_MAX_ATTEMPTS', '4'))
RETRY_PDF_ON_MISS = (os.getenv('OTL_RETRY_PDF_ON_MISS', 'true').lower() in ['1','true','yes'])


def _sleep(base_sec: float = DEFAULT_DELAY_BASE_SEC, jitter_sec: float = DEFAULT_DELAY_JITTER_SEC) -> None:
    delay = max(0.0, base_sec) + max(0.0, jitter_sec) * random.random()
    time.sleep(delay)


def _backoff_sleep(attempt_index: int) -> None:
    # attempt_index: 1,2,3,...
    delay = RETRY_BACKOFF_BASE_SEC * (2 ** max(0, attempt_index - 1))
    time.sleep(min(delay, RETRY_BACKOFF_MAX_SEC))


def _with_cache_bust(url: str) -> str:
    if not url:
        return url
    ts = str(int(time.time() * 1000))
    return f"{url}&ts={ts}" if ('?' in url) else f"{url}?ts={ts}"


def _pick_user_agent() -> str:
    if CUSTOM_UA:
        return CUSTOM_UA
    ua_pool = [
        # Unified Chrome version across all scrapers
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.127 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.127 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    ]
    return random.choice(ua_pool)


def _ensure_not_retry_later(driver) -> BeautifulSoup:
    """Ensure current page is not a 'Retry later' placeholder.
    If WAIT_UNTIL_CLEAR is enabled, wait until cleared (up to WAIT_MAX_MINUTES).
    Else, try up to 3 backoff refreshes.
    Returns BeautifulSoup of the final page.
    """
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    text = soup.get_text()
    if 'Retry later' not in text:
        return soup
    if WAIT_UNTIL_CLEAR:
        print("[OTL] [Wait] 'Retry later' detected — waiting until it clears...", flush=True)
        start_ts = time.time()
        attempt = 1
        while True: 
            if time.time() - start_ts > WAIT_MAX_MINUTES * 60:
                print("[OTL] [Wait] Max wait time reached; proceeding.", flush=True)
                return BeautifulSoup(driver.page_source, 'html.parser')
            _backoff_sleep(attempt)
            attempt += 1
            try:
                driver.refresh()
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            except Exception:
                pass
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            if 'Retry later' not in soup.get_text():
                print("[OTL] [Wait] Cleared; continue.", flush=True)
                return soup
    else:
        print("[OTL] [Leaf] Hit 'Retry later', applying backoff...", flush=True)
        for attempt in range(1, 4):
            _backoff_sleep(attempt)
            try:
                driver.refresh()
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            except Exception:
                pass
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            if 'Retry later' not in soup.get_text():
                break
        return soup


def _md5_id(url: str, title: str) -> str:
    import hashlib
    return hashlib.md5(f"{url}_{title}".encode("utf-8")).hexdigest()


def parse_book_detail_html(html: str, book_url: str, subjects: List[str]) -> Dict[str, Any]:
    """Parse a book detail HTML into the required output schema."""
    soup = BeautifulSoup(html, 'html.parser')

    # Title
    title = ''
    h1 = soup.select_one('#info h1') or soup.find('h1')
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        title_tag = soup.find('title')
        title = (title_tag.get_text(strip=True) if title_tag else '').strip()

    # Description
    description = ''
    about = soup.select_one('#AboutBook')
    if about:
        p = about.find('p')
        if p:
            description = p.get_text('\n', strip=True)

    # Authors
    authors: List[str] = []
    for p in soup.find_all('p'):
        txt = (p.get_text(' ', strip=True) or '')
        if txt.lower().startswith('contributors:'):
            raw = txt.split(':', 1)[1].strip()
            parts = [re.sub(r'\s+', ' ', x).strip() for x in re.split(r',| and ', raw) if x.strip()]
            for a in parts:
                if a and a not in authors:
                    authors.append(a)
            break
    if not authors:
        for meta_name in ['author', 'book:author']:
            for m in soup.find_all('meta', attrs={'name': meta_name}):
                val = (m.get('content') or '').strip()
                if val and val not in authors:
                    authors.append(val)
    # Heuristic: authors appear as plain <p> lines inside #info above metadata fields
    if not authors:
        info_div = soup.select_one('#info')
        if info_div:
            started_block = False
            for p in info_div.find_all('p'):
                txt = (p.get_text(' ', strip=True) or '')
                if not txt:
                    continue
                ltxt = txt.lower()
                # Skip reviews and star rows
                if 'review' in ltxt:
                    continue
                # If this looks like a metadata field, stop after we started authors block
                meta_prefixes = (
                    'copyright year', 'publisher', 'language', 'isbn', 'license',
                    'format', 'pages', 'doi', 'edition'
                )
                if any(ltxt.startswith(pref) for pref in meta_prefixes) or ':' in txt:
                    if started_block:
                        break
                    else:
                        continue
                # Treat as author line(s)
                started_block = True
                parts = [re.sub(r'\s+', ' ', x).strip() for x in re.split(r',| and ', txt) if x.strip()]
                for a in parts:
                    if a and a not in authors:
                        authors.append(a)

    # Extract PDF URL if present; keep 'url' as the book detail page
    url_field = book_url
    pdf_url = ''
    for a in soup.select('#book-types a[href]'):
        label = (a.get_text(strip=True) or '').lower()
        if 'pdf' in label:
            pdf_url = urljoin(BASE_URL, a['href'])
            break

    doc = {
        'id': _md5_id(book_url, title or book_url),
        'title': title,
        'description': description,
        'authors': authors,
        'subjects': subjects or [],
        'source': 'Open Textbook Library',
        'url': url_field,
        'url_pdf': pdf_url or None,
        'scraped_at': datetime.now().isoformat()
    }
    return doc


# =========== Live (Selenium) helpers ===========
def _init_driver(headless: bool = True):
    if webdriver is None:
        raise RuntimeError("Selenium not available in this environment")
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--blink-settings=imagesEnabled=false")
    # Reduce automation fingerprints
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)

    ua = _pick_user_agent() if FORCE_RANDOM_UA or CUSTOM_UA else None
    if ua:
        options.add_argument(f"--user-agent={ua}")

    if PROXY_SERVER:
        options.add_argument(f"--proxy-server={PROXY_SERVER}")
    try:
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
    except Exception:
        pass
    driver = webdriver.Chrome(options=options)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception:
        pass
    return driver


def live_scrape_leaf_subject_selenium(subject_name: str, subject_url: str) -> List[Dict[str, Any]]:
    """Live scrape using a single Selenium driver for both listing and book details."""
    results: List[Dict[str, Any]] = []
    driver = _init_driver(headless=True)
    try:
        print(f"[OTL] ===== Start subject: {subject_name} | {subject_url} =====", flush=True)
        # Collect listing URLs with pagination first, fallback to scroll
        def _with_scroll(u: str) -> str:
            if 'scroll=true' in (u or ''):
                return u
            return f"{u}&scroll=true" if ('?' in u) else f"{u}?scroll=true"

        current_url = _with_scroll(subject_url)
        urls: List[str] = []
        visited_pages: set[str] = set()
        page_no = 1
        while True:
            driver.get(current_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            soup = _ensure_not_retry_later(driver)
            for h2 in soup.select('div.row.short-description h2 > a[href]'):
                href = (h2.get('href') or '').strip()
                if not href:
                    continue
                full = urljoin(BASE_URL, href)
                if '/opentextbooks/textbooks/' in full and full not in urls:
                    urls.append(full)
            print(f"[OTL] [Leaf] Page {page_no} listing count so far: {len(urls)}", flush=True)
            next_a = soup.select_one('#pagination a[rel="next"][href]')
            if not next_a:
                break
            next_url = _with_scroll(urljoin(BASE_URL, next_a.get('href') or ''))
            next_url = _with_cache_bust(next_url)
            if not next_url or next_url in visited_pages:
                break
            visited_pages.add(next_url)
            page_no += 1
            current_url = next_url
            _sleep()

        # Fallback to scroll if pagination failed to produce any URLs
        if not urls:
            last_count = 0
            stable_rounds = 0
            while True:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                for h2 in soup.select('div.row.short-description h2 > a[href]'):
                    href = (h2.get('href') or '').strip()
                    if not href:
                        continue
                    full = urljoin(BASE_URL, href)
                    if '/opentextbooks/textbooks/' in full and full not in urls:
                        urls.append(full)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                _sleep(0.4, 0.4)
                if len(urls) == last_count:
                    stable_rounds += 1
                else:
                    stable_rounds = 0
                    last_count = len(urls)
                print(f"[OTL] [Leaf] Listing progress: {last_count} books, stable_rounds={stable_rounds}", flush=True)
                if stable_rounds >= 3:
                    break
        print(f"[OTL] [Leaf] Found {len(urls)} book URLs for subject '{subject_name}'", flush=True)

        # Visit each book and parse
        for idx, url in enumerate(urls, start=1):
            print(f"[OTL] [Leaf] Open book {idx}/{len(urls)}: {url}", flush=True)
            # Per-book delay and cache-busting to reduce server-side throttling
            if PER_BOOK_DELAY_SEC > 0:
                time.sleep(PER_BOOK_DELAY_SEC + DEFAULT_DELAY_JITTER_SEC * random.random())
            attempts = 0
            doc = None
            while attempts < max(1, BOOK_MAX_ATTEMPTS):
                attempts += 1
                driver.get(_with_cache_bust(url))
                try:
                    WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                except Exception:
                    pass
                # Ensure not stuck on retry page
                soup = _ensure_not_retry_later(driver)
                # Give the page a moment for dynamic blocks
                _sleep(0.4, 0.4)
                # Try to scroll to Formats section to trigger lazy loads
                try:
                    driver.execute_script("var el=document.getElementById('Formats'); if(el){el.scrollIntoView({behavior:'instant',block:'center'});} else {window.scrollTo(0, document.body.scrollHeight);} ")
                    WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                except Exception:
                    pass
                html = driver.page_source
                doc_try = parse_book_detail_html(html, url, [subject_name])
                missing_title = not (doc_try.get('title') or '').strip()
                missing_pdf = not bool(doc_try.get('url_pdf'))
                if not missing_title and (not RETRY_PDF_ON_MISS or not missing_pdf):
                    doc = doc_try
                    break
                # If missing title or pdf, backoff and retry
                _backoff_sleep(attempts)
            if not doc:
                doc = parse_book_detail_html(driver.page_source, url, [subject_name])
            if doc:
                results.append(doc)
                has_pdf = 'url_pdf' in doc and bool(doc['url_pdf'])
                print(f"[OTL] [Leaf] Parsed book {idx}/{len(urls)} | title='{doc.get('title','')}' | pdf={has_pdf}", flush=True)
        print(f"[OTL] ===== End subject: {subject_name} | total_books={len(results)} =====", flush=True)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return results


def live_parse_subjects_index(index_url: str, root_subject: str) -> List[Dict[str, str]]:
    """Open the subjects index and extract child subjects of a given parent.
    Returns list of {name, url, slug}. If no children, returns the parent as a single leaf.
    """
    driver = _init_driver(headless=True)
    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or '').strip()).lower()
    try:
        print(f"[OTL] [Index] Open subjects index: {index_url}", flush=True)
        print(f"[OTL] [Index] Target parent: {root_subject}", flush=True)
        driver.get(_with_cache_bust(index_url))
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        soup = _ensure_not_retry_later(driver)
        result: List[Dict[str, str]] = []
        for li in soup.select('#subjects ul.subject-directory > li'):
            a = li.find('a', href=True)
            if not a:
                continue
            if _norm(a.get_text()) != _norm(root_subject):
                continue
            ul = li.find('ul')
            if ul:
                for ca in ul.select('a[href]'):
                    name = ca.get_text(strip=True)
                    href = ca.get('href') or ''
                    if '/opentextbooks/subjects/' in href:
                        full = urljoin(BASE_URL, href)
                        result.append({
                            'name': name,
                            'url': full,
                            'slug': full.rstrip('/').split('/')[-1]
                        })
            else:
                href = a.get('href') or ''
                full = urljoin(BASE_URL, href)
                result.append({
                    'name': a.get_text(strip=True),
                    'url': full,
                    'slug': full.rstrip('/').split('/')[-1]
                })
            break
        print(f"[OTL] [Index] Found children for '{root_subject}': {len(result)}", flush=True)
        return result
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def live_scrape_root_subject_selenium(root_subject_name: str, index_url: str, max_children: int | None = None) -> List[Dict[str, Any]]:
    """Scrape all child subjects under a root subject from the index using Selenium end-to-end."""
    docs: List[Dict[str, Any]] = []
    children = live_parse_subjects_index(index_url, root_subject_name)
    if max_children:
        children = children[:max_children]
    print(f"[OTL] [Root] Start root='{root_subject_name}', children={len(children)}", flush=True)
    for child in children:
        name = child.get('name') or ''
        url = child.get('url') or ''
        if not name or not url:
            continue
        print(f"[OTL] [Root] Process child subject: {name} | {url}", flush=True)
        # Cooldown between subjects to reduce rate limiting
        if SUBJECT_COOLDOWN_SEC > 0:
            time.sleep(SUBJECT_COOLDOWN_SEC)
        for doc in live_scrape_leaf_subject_selenium(name, url):
            docs.append(doc)
    print(f"[OTL] [Root] Done root='{root_subject_name}', total_docs={len(docs)}", flush=True)
    return docs


def live_parse_all_subjects_index(index_url: str) -> List[Dict[str, str]]:
    """Parse the entire subjects index to get every leaf subject (parent without children or each child).
    Returns list of {parent, name, url, slug}.
    """
    driver = _init_driver(headless=True)
    leaves: List[Dict[str, str]] = []
    try:
        print(f"[OTL] [Index-All] Open subjects index: {index_url}", flush=True)
        driver.get(_with_cache_bust(index_url))
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        soup = _ensure_not_retry_later(driver)
        for li in soup.select('#subjects ul.subject-directory > li'):
            a = li.find('a', href=True)
            if not a:
                continue
            parent_name = a.get_text(strip=True)
            ul = li.find('ul')
            if ul:
                for ca in ul.select('a[href]'):
                    name = ca.get_text(strip=True)
                    href = ca.get('href') or ''
                    if '/opentextbooks/subjects/' in href:
                        full = urljoin(BASE_URL, href)
                        leaves.append({
                            'parent': parent_name,
                            'name': name,
                            'url': full,
                            'slug': full.rstrip('/').split('/')[-1]
                        })
            else:
                href = a.get('href') or ''
                full = urljoin(BASE_URL, href)
                leaves.append({
                    'parent': parent_name,
                    'name': parent_name,
                    'url': full,
                    'slug': full.rstrip('/').split('/')[-1]
                })
        print(f"[OTL] [Index-All] Found leaves: {len(leaves)}", flush=True)
        return leaves
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def live_scrape_all_subjects_selenium(index_url: str, max_parents: int | None = None, max_children: int | None = None) -> List[Dict[str, Any]]:
    """Scrape all leaf subjects discovered from the index. Optional limits for testing.
    max_parents limits number of distinct parents processed; max_children limits children per parent.
    """
    leaves = live_parse_all_subjects_index(index_url)
    if max_parents is not None or max_children is not None:
        filtered: List[Dict[str, str]] = []
        seen_parent: List[str] = []
        per_parent: Dict[str, int] = {}
        for leaf in leaves:
            parent = leaf.get('parent') or ''
            if max_parents is not None:
                if parent not in seen_parent:
                    if len(seen_parent) >= max_parents:
                        continue
                    seen_parent.append(parent)
            if max_children is not None:
                cnt = per_parent.get(parent, 0)
                if cnt >= max_children:
                    continue
                per_parent[parent] = cnt + 1
            filtered.append(leaf)
        leaves = filtered
    print(f"[OTL] [All] Scrape leaves count: {len(leaves)}", flush=True)
    docs: List[Dict[str, Any]] = []
    for idx, leaf in enumerate(leaves, start=1):
        name = leaf.get('name') or ''
        url = leaf.get('url') or ''
        if not name or not url:
            continue
        print(f"[OTL] [All] ({idx}/{len(leaves)}) Subject: {name} | {url}", flush=True)
        for doc in live_scrape_leaf_subject_selenium(name, url):
            docs.append(doc)
    print(f"[OTL] [All] Done all leaves, total_docs={len(docs)}", flush=True)
    return docs


