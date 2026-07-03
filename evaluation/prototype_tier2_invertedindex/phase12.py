"""Phase 1 (glossary) + Phase 2 (BM25 page-level retrieval) prototype.
No LLM, no Groq tokens. CPU only. Translation via Wikipedia langlinks (free)."""
import os, re, json, math, time, unicodedata
from collections import Counter
import requests

_STOP = set("""the a an and or of to in for on with as by at from is are was were be been
being this that these those it its he she they we you i which who whom whose what when where
how why all any both each few more most other some such no nor not only own same so than too
very can will just should now also into about over under between out up down off above below
their there here then once during before after while because if else through against within
without along across behind beyond plus etc per via vs use used using one two three first second
new may must shall would could might them his her our your my me him us also many much based upon
following given different various several including especially e.g i.e figure table chapter section
page see et al pp vol no fig""".split())

def is_term_token(t):
    return t.isalpha() and len(t) >= 4 and t not in _STOP

PAGES   = json.load(open('/tmp/vi_book_pages.json'))
OUTS    = json.load(open('/home/lib/oer_chatbot_project/TLCN_OER_Lakehouse/evaluation/pipeline_outputs_vi_translated.json'))
TESTVI  = json.load(open('/home/lib/oer_chatbot_project/TLCN_OER_Lakehouse/evaluation/test_set_vi.json'))
GLO_CACHE = '/tmp/glossary_cache.json'
wiki_cache = json.load(open(GLO_CACHE)) if os.path.exists(GLO_CACHE) else {}

def fold(s):
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()

def tok(s):
    return re.findall(r'[a-z0-9]+', fold(s))

# ---------------- PHASE 1: glossary ----------------
def wiki_vi(term):
    if term in wiki_cache:
        return wiki_cache[term]
    val = None
    try:
        r = requests.get('https://en.wikipedia.org/w/api.php', params={
            'action':'query','titles':term,'prop':'langlinks','lllang':'vi',
            'format':'json','redirects':1}, timeout=8,
            headers={'User-Agent':'oer-glossary/1.0'})
        pages = r.json().get('query',{}).get('pages',{})
        for _,p in pages.items():
            ll = p.get('langlinks')
            if ll: val = ll[0].get('*')
    except Exception:
        val = None
    wiki_cache[term] = val
    return val

def build_glossary(max_terms=700):
    # Pure-python n-gram candidate extraction (no sklearn/numpy).
    uni = Counter(); bi = Counter(); tri = Counter()
    for v in PAGES.values():
        toks = tok(' '.join(p['text'] for p in v['pages']))
        uni.update(t for t in toks if is_term_token(t))
        for i in range(len(toks)-1):
            a,b = toks[i], toks[i+1]
            if is_term_token(a) and is_term_token(b):
                bi[f"{a} {b}"] += 1
        for i in range(len(toks)-2):
            a,b,c = toks[i], toks[i+1], toks[i+2]
            if is_term_token(a) and is_term_token(b) and is_term_token(c):
                tri[f"{a} {b} {c}"] += 1
    cands = ([w for w,c in bi.most_common(900) if c>=4]
             + [w for w,c in tri.most_common(400) if c>=3]
             + [w for w,c in uni.most_common(1200) if c>=8])
    seen=set(); ordered=[]
    for c in cands:
        if c not in seen:
            seen.add(c); ordered.append(c)
    glossary = {}   # vi_folded_term -> en_term
    tried = 0
    for en in ordered:
        if tried >= max_terms: break
        tried += 1
        vi = wiki_vi(en)
        if vi and fold(vi) != fold(en):
            glossary[fold(vi)] = en
        if tried % 100 == 0:
            json.dump(wiki_cache, open(GLO_CACHE,'w'), ensure_ascii=False)
            print(f"    translated {tried}/{max_terms}, glossary={len(glossary)}", flush=True)
    json.dump(wiki_cache, open(GLO_CACHE,'w'), ensure_ascii=False)
    return glossary

# ---------------- BM25 ----------------
class BM25:
    def __init__(self, docs, k1=1.5, b=0.75):
        self.k1, self.b = k1, b
        self.docs = docs
        self.N = len(docs)
        self.tf = [Counter(d) for d in docs]
        self.dl = [len(d) for d in docs]
        self.avgdl = sum(self.dl)/max(self.N,1)
        df = Counter()
        for d in docs:
            for t in set(d): df[t]+=1
        self.idf = {t: math.log(1+(self.N-c+0.5)/(c+0.5)) for t,c in df.items()}
    def topk(self, q, k=10):
        sc = []
        for i in range(self.N):
            tf = self.tf[i]; dl = self.dl[i]; s = 0.0
            for t in q:
                f = tf.get(t)
                if not f: continue
                idf = self.idf.get(t,0.0)
                s += idf*(f*(self.k1+1))/(f+self.k1*(1-self.b+self.b*dl/self.avgdl))
            if s>0: sc.append((s,i))
        sc.sort(reverse=True)
        return sc[:k]

# ---------------- Build page corpus ----------------
page_uid = []      # uid per page index
page_tokens = []
for uid, v in PAGES.items():
    for p in v['pages']:
        page_uid.append(uid)
        page_tokens.append(tok(p['text']))

print(f"Corpus: {len(page_tokens)} pages, {len(PAGES)} books", flush=True)

print("Building glossary (Wikipedia translate)...", flush=True)
t0=time.time()
GLO = build_glossary()
print(f"  glossary terms: {len(GLO)}  ({time.time()-t0:.0f}s)", flush=True)

bm = BM25(page_tokens)
print("BM25 index ready", flush=True)

# translated_question lookup
trans = {o['id']: (o.get('translated_question') or '') for o in OUTS}

def expand_glossary(vi_text):
    f = fold(vi_text)
    add = []
    for vi_term, en in GLO.items():
        if vi_term and vi_term in f:
            add.append(en)
    return add

# ---------------- Phase 2 measurement ----------------
book_uids = set(PAGES.keys())
qs = [q for q in TESTVI
      if q['question_type'] != 'out_of_scope'
      and q.get('source_asset_uid') in book_uids]

variants = ['vi_raw','groq_trans','vi_glossary','groq_glossary','en_ceiling']
CONTENT = {'definition','comparison','multi_step'}   # exclude find_material (metadata)

def run(question_set, label):
    agg = {v:{'hit5':0,'hit10':0,'rr':0.0} for v in variants}
    n = len(question_set)
    if n==0: return
    for q in question_set:
        gold = q['source_asset_uid']
        vi_q = q['question']
        tq   = trans.get(q['id'], '')
        gl   = expand_glossary(vi_q)
        gt   = q.get('ground_truth') or ''
        queries = {
            'vi_raw':        tok(vi_q),
            'groq_trans':    tok(tq) if tq else tok(vi_q),
            'vi_glossary':   tok(vi_q) + tok(' '.join(gl)),
            'groq_glossary': (tok(tq) if tq else tok(vi_q)) + tok(' '.join(gl)),
            'en_ceiling':    tok(gt) if gt else tok(vi_q),
        }
        for v, qt in queries.items():
            res = bm.topk(qt, k=10)
            uids = [page_uid[i] for _,i in res]
            if gold in uids[:5]:  agg[v]['hit5']+=1
            if gold in uids[:10]: agg[v]['hit10']+=1
            rr=0.0
            for rank,u in enumerate(uids,1):
                if u==gold: rr=1.0/rank; break
            agg[v]['rr']+=rr
    print(f"\n===== {label} (n={n}, 12 books, page-level BM25) =====")
    print(f"{'variant':16} {'Hit@5':>8} {'Hit@10':>8} {'MRR':>8}")
    for v in variants:
        a=agg[v]
        print(f"{v:16} {a['hit5']/n:>8.4f} {a['hit10']/n:>8.4f} {a['rr']/n:>8.4f}")

run(qs, "ALL in-scope (incl. find_material/metadata)")
run([q for q in qs if q['question_type'] in CONTENT], "CONTENT only (definition+comparison+multi_step)")

covered = sum(1 for q in qs if expand_glossary(q['question']))
print(f"\nGlossary hit some VI term in {covered}/{len(qs)} questions")
json.dump(GLO, open('/tmp/glossary_vi_en.json','w'), ensure_ascii=False, indent=2)
print("Saved glossary -> /tmp/glossary_vi_en.json")