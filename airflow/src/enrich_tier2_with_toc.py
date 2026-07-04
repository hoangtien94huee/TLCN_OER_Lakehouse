"""
enrich_tier2_with_toc.py
========================
Enrich existing oer_pages_tier2 Elasticsearch index with chapter_title and
section_title derived from Silver Layer TOC data (via Iceberg/Spark).

For each book that has a TOC, each indexed page gets the chapter/section
title of the TOC entry whose page range contains that page. This enables
the BM25 boost (section_title^2, chapter_title^2) to work correctly.

Usage (inside oer-airflow-scraper container):
  python3 /opt/airflow/dags/../evaluation/prototype_tier2_invertedindex/enrich_tier2_with_toc.py

Or from host:
  docker exec oer-airflow-scraper python3 /path/to/enrich_tier2_with_toc.py
"""
import os, sys, json, time, logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ES = os.getenv("PAGEINDEX_TIER1_ES_HOST", "http://elasticsearch:9200")
INDEX = os.getenv("PAGEINDEX_TIER2_ES_INDEX", "oer_pages_tier2")
SCROLL_BATCH = int(os.getenv("ENRICH_SCROLL_BATCH", "500"))
UPDATE_BATCH = int(os.getenv("ENRICH_UPDATE_BATCH", "200"))


# ---------------------------------------------------------------------------
# TOC helpers
# ---------------------------------------------------------------------------

def _extract_sections_from_toc(toc_json):
    """Parse TOC list → flat list of {chapter_title, section_title, page_start, page_end}."""
    try:
        toc = json.loads(toc_json) if isinstance(toc_json, str) else (toc_json or [])
    except Exception:
        return []
    if not isinstance(toc, list):
        return []
    sections = []
    for ci, chapter in enumerate(toc, 1):
        if not isinstance(chapter, dict):
            continue
        ch_title = str(chapter.get("chapter_title") or f"Chapter {ci}")
        ch_start = int(chapter.get("page_start") or 1)
        ch_end = int(chapter.get("page_end") or ch_start)
        raw_secs = chapter.get("sections") or []
        if not raw_secs:
            sections.append({
                "chapter_title": ch_title,
                "section_title": ch_title,
                "page_start": ch_start,
                "page_end": ch_end,
            })
            continue
        for sec in raw_secs:
            if not isinstance(sec, dict):
                continue
            sections.append({
                "chapter_title": ch_title,
                "section_title": str(sec.get("section_title") or ch_title),
                "page_start": int(sec.get("page_start") or ch_start),
                "page_end": int(sec.get("page_end") or ch_end),
            })
    return sections


def _find_section_for_page(sections, page_no):
    """Return (chapter_title, section_title) for the section containing page_no."""
    best = None
    for s in sections:
        if s["page_start"] <= page_no <= s["page_end"]:
            # Prefer narrower range (more specific section)
            if best is None or (s["page_end"] - s["page_start"]) < (best["page_end"] - best["page_start"]):
                best = s
    if best:
        return best["chapter_title"], best["section_title"]
    # fallback: nearest section by distance
    if sections:
        nearest = min(sections, key=lambda s: min(abs(page_no - s["page_start"]), abs(page_no - s["page_end"])))
        return nearest["chapter_title"], nearest["section_title"]
    return None, None


# ---------------------------------------------------------------------------
# TOC data source: Iceberg via Spark
# ---------------------------------------------------------------------------

def fetch_toc_map_from_iceberg():
    """Fetch {asset_uid: toc_json_str} from oer_document_structure Iceberg table via Spark."""
    sys.path.insert(0, "/opt/airflow/src")
    from pageindex import PageIndexEngine
    eng = PageIndexEngine()
    log.info("Creating Spark session to read Iceberg Silver layer...")
    spark = eng._create_spark_session()
    table = eng.structure_table  # "silver.default.oer_document_structure"
    try:
        df = spark.sql(f"""
            SELECT asset_uid, table_of_contents_json
            FROM {table}
            WHERE table_of_contents_json IS NOT NULL
              AND table_of_contents_json != '[]'
              AND table_of_contents_json != 'null'
              AND table_of_contents_json != ''
        """)
        rows = df.collect()
        toc_map = {}
        for row in rows:
            uid = row["asset_uid"]
            toc = row["table_of_contents_json"]
            if uid and toc:
                toc_map[uid] = toc
        log.info(f"Loaded TOC for {len(toc_map)} books from {table}")
        spark.stop()
        return toc_map
    except Exception as e:
        log.warning(f"Table {table} failed: {e}")
    spark.stop()
    return {}


# ---------------------------------------------------------------------------
# ES helpers
# ---------------------------------------------------------------------------

def es_count_missing():
    """Count pages still missing chapter_title in tier2 index."""
    body = {"query": {"bool": {"must_not": {"exists": {"field": "chapter_title"}}}}}
    r = requests.post(f"{ES}/{INDEX}/_count", json=body, timeout=30)
    return r.json().get("count", -1) if r.status_code == 200 else -1


def es_agg_asset_uids():
    """Return set of all unique asset_uids in the tier2 index."""
    body = {
        "size": 0,
        "aggs": {"uids": {"terms": {"field": "asset_uid", "size": 10000}}}
    }
    r = requests.post(f"{ES}/{INDEX}/_search", json=body, timeout=60)
    if r.status_code != 200:
        log.error(f"Failed to aggregate asset_uids: {r.text[:200]}")
        return set()
    buckets = r.json().get("aggregations", {}).get("uids", {}).get("buckets", [])
    uids = {b["key"] for b in buckets}
    log.info(f"Found {len(uids)} unique asset_uids in {INDEX}")
    return uids


def bulk_update(updates):
    """Bulk update ES docs. updates = list of (doc_id, chapter_title, section_title)."""
    if not updates:
        return 0
    lines = []
    for doc_id, ch, sec in updates:
        lines.append(json.dumps({"update": {"_index": INDEX, "_id": doc_id}}))
        lines.append(json.dumps({"doc": {"chapter_title": ch, "section_title": sec}}))
    body_bytes = ("\n".join(lines) + "\n").encode("utf-8", "ignore")
    r = requests.post(f"{ES}/_bulk",
                      data=body_bytes,
                      headers={"Content-Type": "application/x-ndjson"},
                      timeout=120)
    if r.status_code != 200:
        log.warning(f"Bulk update HTTP {r.status_code}")
        return 0
    resp = r.json()
    errors = [i for i in resp.get("items", []) if i.get("update", {}).get("error")]
    if errors:
        log.warning(f"{len(errors)} update errors in bulk response")
    return len(updates) - len(errors)


def enrich_book(asset_uid, toc_json):
    """Scroll all pages of this book and update chapter_title/section_title."""
    sections = _extract_sections_from_toc(toc_json)
    if not sections:
        return 0  # no usable TOC

    # Initial scroll request
    body = {
        "size": SCROLL_BATCH,
        "query": {"term": {"asset_uid": asset_uid}},
        "_source": ["page_no"],
    }
    r = requests.post(f"{ES}/{INDEX}/_search?scroll=2m", json=body, timeout=60)
    if r.status_code != 200:
        log.warning(f"Scroll init failed for {asset_uid}: {r.status_code}")
        return 0

    scroll_data = r.json()
    scroll_id = scroll_data.get("_scroll_id")
    hits = scroll_data.get("hits", {}).get("hits", [])

    updates = []
    total_updated = 0

    def flush():
        nonlocal total_updated
        if updates:
            total_updated += bulk_update(list(updates))
            updates.clear()

    while hits:
        for h in hits:
            page_no = (h.get("_source") or {}).get("page_no", 0)
            ch_title, sec_title = _find_section_for_page(sections, page_no)
            if ch_title:
                updates.append((h["_id"], ch_title, sec_title or ch_title))
        if len(updates) >= UPDATE_BATCH:
            flush()

        # Next scroll page
        r2 = requests.post(
            f"{ES}/_search/scroll",
            json={"scroll": "2m", "scroll_id": scroll_id},
            timeout=60,
        )
        if r2.status_code != 200:
            break
        scroll_data = r2.json()
        scroll_id = scroll_data.get("_scroll_id", scroll_id)
        hits = scroll_data.get("hits", {}).get("hits", [])

    flush()

    # Clean up scroll context
    try:
        requests.delete(f"{ES}/_search/scroll",
                        json={"scroll_id": scroll_id}, timeout=10)
    except Exception:
        pass

    return total_updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info(f"=== Enrich {INDEX} with TOC chapter/section titles ===")
    log.info(f"ES host: {ES}")

    # 1. Check how many pages need enriching
    missing_before = es_count_missing()
    log.info(f"Pages currently missing chapter_title: {missing_before}")
    if missing_before == 0:
        log.info("All pages already enriched — nothing to do.")
        return

    # 2. Load TOC map from Iceberg
    log.info("Loading TOC data from Silver layer (Iceberg via Spark)...")
    toc_map = fetch_toc_map_from_iceberg()
    if not toc_map:
        log.error("No TOC data loaded. Exiting.")
        sys.exit(1)

    # 3. Get all asset_uids present in the ES index
    indexed_uids = es_agg_asset_uids()
    to_enrich = indexed_uids & set(toc_map.keys())
    no_toc = indexed_uids - set(toc_map.keys())
    log.info(f"Books to enrich: {len(to_enrich)} / {len(indexed_uids)} (have TOC)")
    log.info(f"Books WITHOUT TOC (will stay null): {len(no_toc)}")

    # 4. Enrich book by book
    t0 = time.time()
    total_pages = 0
    book_list = sorted(to_enrich)

    for i, uid in enumerate(book_list, 1):
        pages_done = enrich_book(uid, toc_map[uid])
        total_pages += pages_done
        if i % 25 == 0 or i == len(book_list):
            elapsed = time.time() - t0
            rate = total_pages / elapsed if elapsed > 0 else 0
            log.info(f"  [{i}/{len(book_list)}] {total_pages} pages updated | {rate:.0f} p/s | {elapsed:.0f}s elapsed")

    # 5. Refresh & report
    requests.post(f"{ES}/{INDEX}/_refresh", timeout=60)
    missing_after = es_count_missing()
    log.info("=" * 60)
    log.info(f"DONE. Updated {total_pages} pages across {len(book_list)} books.")
    log.info(f"Pages missing chapter_title: {missing_before} → {missing_after}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
