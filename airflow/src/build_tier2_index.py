"""
Indexer for Tier-2 page-level inverted index (oer_pages_tier2).

Builds an Elasticsearch inverted index of per-page book text so that Tier-2
retrieval can use BM25 over the whole book instead of loading the PDF and
scanning a TOC-selected range with linear keyword overlap.

CPU only. No LLM, no GPU, no embeddings — pure text inverted index.

Page-text source is pluggable:
  - PROTOTYPE/eval env: reads pre-extracted pages from a cached JSON
    (produced from local PDFs), so we can verify on the 12 eval books.
  - PRODUCTION: swap load_pages() to use the engine's MinIO + pypdf
    extraction (_get_pdf_bytes + _get_page_texts_cached) — same text logic.

Usage:
  python3 build_tier2_index.py --pages /tmp/vi_book_pages.json --recreate
  python3 build_tier2_index.py --pages /tmp/vi_book_pages.json --resume
"""
import argparse, json, sys, time
import requests

ES_HOST = "http://localhost:9200"
INDEX   = "oer_pages_tier2"

MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                # standard English-ish analyzer; a synonym filter (bilingual
                # glossary) can be plugged here later as the optional Phase 4.
                "page_text": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"],
                }
            }
        },
    },
    "mappings": {
        "properties": {
            "asset_uid":     {"type": "keyword"},
            "title":         {"type": "text", "analyzer": "page_text"},
            "page_no":       {"type": "integer"},
            "global_idx":    {"type": "integer"},
            "pdf_name":      {"type": "keyword"},
            "chapter_title": {"type": "text", "analyzer": "page_text"},
            "section_title": {"type": "text", "analyzer": "page_text"},
            "text":          {"type": "text", "analyzer": "page_text"},
        }
    },
}


def es(method, path, **kw):
    r = requests.request(method, f"{ES_HOST}{path}", timeout=60, **kw)
    return r


def ensure_index(recreate: bool):
    exists = es("HEAD", f"/{INDEX}").status_code == 200
    if exists and recreate:
        es("DELETE", f"/{INDEX}")
        exists = False
    if not exists:
        r = es("PUT", f"/{INDEX}", json=MAPPING)
        r.raise_for_status()
        print(f"Created index {INDEX}")
    else:
        print(f"Index {INDEX} already exists")


def already_indexed(asset_uid: str) -> bool:
    r = es("POST", f"/{INDEX}/_count", json={"query": {"term": {"asset_uid": asset_uid}}})
    if r.status_code != 200:
        return False
    return (r.json().get("count") or 0) > 0


def load_pages(path: str) -> dict:
    """Prototype source: {asset_uid: {title, pages:[{g,pdf,p,text}]}}.
    PRODUCTION: replace with engine extraction over MinIO PDFs."""
    return json.load(open(path))


def bulk_index(asset_uid: str, title: str, pages: list, batch: int = 500):
    total = 0
    for i in range(0, len(pages), batch):
        chunk = pages[i:i + batch]
        lines = []
        for pg in chunk:
            doc_id = f"{asset_uid}:{pg['g']}"
            lines.append(json.dumps({"index": {"_index": INDEX, "_id": doc_id}}))
            lines.append(json.dumps({
                "asset_uid": asset_uid,
                "title": title,
                "page_no": pg.get("p"),
                "global_idx": pg.get("g"),
                "pdf_name": pg.get("pdf"),
                "chapter_title": pg.get("chapter_title"),
                "section_title": pg.get("section_title"),
                "text": pg.get("text") or "",
            }, ensure_ascii=False))
        body = "\n".join(lines) + "\n"
        r = es("POST", "/_bulk", data=body.encode("utf-8"),
               headers={"Content-Type": "application/x-ndjson"})
        r.raise_for_status()
        if r.json().get("errors"):
            print(f"  ! bulk errors in {asset_uid}", file=sys.stderr)
        total += len(chunk)
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", required=True, help="cached pages JSON")
    ap.add_argument("--recreate", action="store_true", help="drop & recreate index")
    ap.add_argument("--resume", action="store_true", help="skip books already indexed")
    args = ap.parse_args()

    ensure_index(recreate=args.recreate)
    books = load_pages(args.pages)
    t0 = time.time()
    grand = 0
    for i, (uid, v) in enumerate(books.items(), 1):
        if args.resume and not args.recreate and already_indexed(uid):
            print(f"[{i}/{len(books)}] skip (already indexed) {str(v.get('title'))[:40]}")
            continue
        n = bulk_index(uid, v.get("title") or "", v.get("pages") or [])
        grand += n
        print(f"[{i}/{len(books)}] {str(v.get('title'))[:40]:40} -> {n} pages ({time.time()-t0:.0f}s)")
    es("POST", f"/{INDEX}/_refresh")
    cnt = es("GET", f"/{INDEX}/_count").json().get("count")
    print(f"DONE. indexed {grand} pages this run; index now holds {cnt} pages.")


if __name__ == "__main__":
    main()