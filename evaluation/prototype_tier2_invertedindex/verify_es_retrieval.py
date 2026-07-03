"""Verify oer_pages_tier2 ES BM25 retrieval on the VI eval questions.
Cross-book search (no asset_uid filter) -> book-level Hit@K + MRR,
to confirm ES reproduces the in-memory BM25 prototype numbers."""
import json, requests

ES="http://localhost:9200"; INDEX="oer_pages_tier2"
PAGES = json.load(open('/tmp/vi_book_pages.json'))
OUTS  = json.load(open('../pipeline_outputs_vi_translated.json'))
TESTVI= json.load(open('../test_set_vi.json'))
trans = {o['id']: (o.get('translated_question') or '') for o in OUTS}
books = set(PAGES.keys())
CONTENT={'definition','comparison','multi_step'}

def search(query, k=10):
    body={"size":k,"_source":["asset_uid"],
          "query":{"multi_match":{"query":query,"type":"best_fields",
                   "fields":["text","section_title^2","chapter_title^2","title^3"]}}}
    r=requests.post(f"{ES}/{INDEX}/_search",json=body,timeout=30)
    r.raise_for_status()
    return [h["_source"]["asset_uid"] for h in r.json()["hits"]["hits"]]

def run(qs,label):
    n=len(qs); h5=h10=0; rr=0.0
    for q in qs:
        gold=q['source_asset_uid']
        query = trans.get(q['id']) or q['question']
        uids = search(query,10)
        if gold in uids[:5]: h5+=1
        if gold in uids[:10]: h10+=1
        for rank,u in enumerate(uids,1):
            if u==gold: rr+=1.0/rank; break
    print(f"{label:48} n={n:3}  Hit@5={h5/n:.4f}  Hit@10={h10/n:.4f}  MRR={rr/n:.4f}")

qs=[q for q in TESTVI if q['question_type']!='out_of_scope' and q.get('source_asset_uid') in books]
print("=== ES oer_pages_tier2 BM25 (query = Groq translated_question) ===")
run(qs, "ALL in-scope")
run([q for q in qs if q['question_type'] in CONTENT], "CONTENT only (def+comp+multi)")