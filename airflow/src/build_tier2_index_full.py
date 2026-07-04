"""
Full-corpus streaming indexer for oer_pages_tier2.

For each book in /tmp/full_corpus_pdf_map.json:
  - skip if already indexed (resume),
  - extract page text via pypdf (CPU),
  - bulk-index pages into ES.

CPU only, no GPU, no Groq. Safe to interrupt & re-run (resume).
"""
import os, json, sys, time
import requests
from pypdf import PdfReader

ES_HOST = "http://localhost:9200"
INDEX   = "oer_pages_tier2"
MAP     = "/tmp/full_corpus_pdf_map.json"
MIN_CHARS = 40


def es(method, path, **kw):
    return requests.request(method, f"{ES_HOST}{path}", timeout=120, **kw)


def already_indexed(uid):
    r = es("POST", f"/{INDEX}/_count", json={"query": {"term": {"asset_uid": uid}}})
    return r.status_code == 200 and (r.json().get("count") or 0) > 0


def extract_pages(pdfs):
    pages = []
    g = 0
    for pdf in pdfs:
        try:
            reader = PdfReader(pdf)
        except Exception as e:
            print(f"    ! open fail {os.path.basename(pdf)}: {e}", flush=True)
            continue
        for pno, page in enumerate(reader.pages):
            try:
                txt = (page.extract_text() or "").strip()
            except Exception:
                txt = ""
            if len(txt) >= MIN_CHARS:
                pages.append({"g": g, "pdf": os.path.basename(pdf), "p": pno, "text": txt})
                g += 1
    return pages


def bulk_index(uid, title, pages, batch=500):
    total = 0
    for i in range(0, len(pages), batch):
        chunk = pages[i:i + batch]
        lines = []
        for pg in chunk:
            lines.append(json.dumps({"index": {"_index": INDEX, "_id": f"{uid}:{pg['g']}"}}))
            lines.append(json.dumps({
                "asset_uid": uid, "title": title,
                "page_no": pg["p"], "global_idx": pg["g"], "pdf_name": pg["pdf"],
                "chapter_title": None, "section_title": None,
                "text": pg["text"],
            }, ensure_ascii=False))
        r = es("POST", "/_bulk", data=("\n".join(lines) + "\n").encode("utf-8"),
               headers={"Content-Type": "application/x-ndjson"})
        r.raise_for_status()
        if r.json().get("errors"):
            print(f"    ! bulk errors {uid}", file=sys.stderr, flush=True)
        total += len(chunk)
    return total


def main():
    books = json.load(open(MAP))
    n = len(books)
    t0 = time.time()
    done = skipped = grand = 0
    for i, (uid, v) in enumerate(books.items(), 1):
        title = v.get("title") or ""
        if already_indexed(uid):
            skipped += 1
            continue
        pages = extract_pages(v.get("pdfs") or [])
        if not pages:
            print(f"[{i}/{n}] (0 trang) {title[:40]}", flush=True)
            continue
        bulk_index(uid, title, pages)
        done += 1
        grand += len(pages)
        el = time.time() - t0
        print(f"[{i}/{n}] {title[:42]:42} {len(pages):>5}tr | tổng {grand} trang | {el:.0f}s", flush=True)
    es("POST", f"/{INDEX}/_refresh")
    cnt = es("GET", f"/{INDEX}/_count").json().get("count")
    print(f"DONE. mới index {done} sách / {grand} trang, bỏ qua {skipped} (đã có). "
          f"Index hiện giữ {cnt} trang. elapsed={time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()