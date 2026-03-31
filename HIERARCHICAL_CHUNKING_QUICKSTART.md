# 📋 HIERARCHICAL CHUNKING - QUICK START GUIDE

> **Mục đích**: File hướng dẫn tóm tắt để implement Hierarchical Chunking  
> **Chi tiết đầy đủ**: Xem `HIERARCHICAL_CHUNKING_DESIGN.md`

---

## 🎯 Tổng Quan

### Mục tiêu
Transform flat chunking (1800 chars, 200 pages limit) → Hierarchical 3-tier (2400 chars, 1000+ pages)

### Lợi ích
- ✅ Xử lý tài liệu lớn (1000+ trang)
- ✅ Chapter navigation
- ✅ Giảm 70-80% chunks cần index
- ✅ Search quality +15-20%
- ✅ Search latency <500ms

---

## 📐 Kiến Trúc 3-Tier

```
TIER 1: Document Summary (1 chunk/document)
   ↓
TIER 2: Chapter Summaries (1 chunk/chapter)
   ↓
TIER 3: Section Details (N chunks/chapter)
```

### Schema Changes

```sql
-- Thêm vào oer_chunks table
ALTER TABLE oer_chunks ADD COLUMN:
  chunk_type STRING,           -- 'doc_summary' | 'chapter_summary' | 'section_detail'
  chunk_tier INT,              -- 1 | 2 | 3
  chapter_id STRING,
  chapter_title STRING,
  chapter_number INT,
  chapter_page_start INT,
  chapter_page_end INT,
  section_id STRING,
  section_title STRING,
  section_number STRING,
  parent_chunk_id STRING,
  is_summary BOOLEAN

-- Tạo table mới
CREATE TABLE oer_document_structure (
  structure_id STRING PRIMARY KEY,
  asset_uid STRING,
  has_toc BOOLEAN,
  toc_method STRING,           -- 'pdf_outline' | 'pdfplumber' | 'regex' | 'flat'
  toc_confidence FLOAT,
  table_of_contents JSON,      -- Chapter/section hierarchy
  total_pages INT,
  total_chapters INT,
  total_sections INT
)
```

---

## 🔧 Implementation Steps

### Phase 1: TOC Extraction (Week 1-2)

**Files to create:**
- `airflow/src/hierarchical/toc_extractor.py` - Multi-method TOC extraction
- `airflow/src/hierarchical/summarizer.py` - Extractive summarization

**Dependencies:**
```python
# Already in requirements.txt:
PyPDF2==3.0.1
pdfplumber>=0.10.0

# Add:
scikit-learn==1.3.2  # For TF-IDF summarization
```

**Key methods:**
1. PyPDF2 outline (confidence: 0.95)
2. pdfplumber TOC page (confidence: 0.85)
3. Regex patterns (confidence: 0.70)
4. Flat fallback (confidence: 0.50)

### Phase 2: Chunking Logic (Week 3-4)

**Files to modify:**
- `airflow/src/silver_transform.py`:
  - Update `_build_chunks_df()` method
  - Add TOC extraction step
  - Generate 3 tiers of chunks

**Config changes:**
```bash
# docker-compose.yml
ENABLE_HIERARCHICAL_CHUNKING=1      # Feature flag
SILVER_MAX_PDF_PAGES=1000           # Remove 200 limit
SILVER_CHUNK_MAX_CHARS=2400         # Larger chunks (tier 3)
SILVER_CHUNK_OVERLAP=300            # More overlap
HIERARCHICAL_INDEX_ALL_TIERS=1      # Index tier 3? (0=on-demand)
TOC_MIN_CONFIDENCE=0.60             # Minimum confidence to use TOC
```

### Phase 3: Search Integration (Week 5-6)

**Files to modify:**
- `airflow/src/elasticsearch_sync.py`:
  - Update ES mapping (add new fields)
  - Index all 3 tiers

- `airflow/src/chatbot_api.py`:
  - 2-stage search (tier 1-2 → expand to tier 3)
  - Add chapter context to results

**ES Mapping additions:**
```json
{
  "chunk_type": {"type": "keyword"},
  "chunk_tier": {"type": "integer"},
  "chapter_title": {"type": "text"},
  "chapter_number": {"type": "integer"},
  "chapter_page_range": {"type": "integer_range"},
  "section_title": {"type": "text"},
  "is_summary": {"type": "boolean"}
}
```

---

## 📊 Algorithm Details

### TOC Extraction Logic

```python
def extract_toc(pdf_path):
    # Method 1: Try PyPDF2 outline
    if has_pdf_outline():
        return parse_outline()  # confidence: 0.95
    
    # Method 2: Try pdfplumber
    if has_toc_page():
        return parse_toc_text()  # confidence: 0.85
    
    # Method 3: Regex chapter detection
    if detect_chapter_patterns():
        return extract_via_regex()  # confidence: 0.70
    
    # Method 4: Fallback
    return generate_flat_toc()  # confidence: 0.50
```

### Chapter Detection Patterns

```python
CHAPTER_PATTERNS = [
    r'^Chapter\s+\d+',
    r'^CHAPTER\s+\d+',
    r'^Chương\s+\d+',
    r'^\d+\.\s+[A-Z]',    # "1. Introduction"
    r'^[IVX]+\.\s+',      # Roman: "I. ..."
]

SECTION_PATTERNS = [
    r'^\d+\.\d+',         # "1.1", "2.3"
    r'^Section\s+\d+',
]
```

### Summarization (Extractive)

```python
def generate_chapter_summary(chapter_text, max_chars=800):
    # 1. Sentence tokenization
    sentences = split_sentences(chapter_text)
    
    # 2. TF-IDF vectorization
    tfidf_matrix = TfidfVectorizer().fit_transform(sentences)
    
    # 3. Rank by importance
    scores = tfidf_matrix.sum(axis=1)
    top_sentences = rank_sentences(scores, top_n=4)
    
    # 4. Return summary
    return join_sentences(top_sentences)[:max_chars]
```

### 3-Tier Chunk Generation

```python
def generate_hierarchical_chunks(resource, pdf_path):
    # Step 1: Extract TOC
    toc_result = extract_toc(pdf_path)
    
    if toc_result['confidence'] < 0.60:
        # Fallback to flat chunking
        return generate_flat_chunks(pdf_path)
    
    chunks = []
    
    # Tier 1: Document summary
    doc_summary = generate_document_summary(resource, toc_result['toc'])
    chunks.append({
        'chunk_tier': 1,
        'chunk_type': 'doc_summary',
        'chunk_text': doc_summary,
        'is_summary': True
    })
    
    # Tier 2 + 3: Iterate chapters
    for chapter in toc_result['toc']:
        # Extract chapter text
        chapter_text = extract_pages(
            pdf_path,
            start=chapter['page_start'],
            end=chapter['page_end']
        )
        
        # Tier 2: Chapter summary
        chapter_summary = generate_chapter_summary(chapter_text)
        chapter_chunk_id = generate_chunk_id()
        chunks.append({
            'chunk_id': chapter_chunk_id,
            'chunk_tier': 2,
            'chunk_type': 'chapter_summary',
            'chunk_text': chapter_summary,
            'chapter_id': chapter['chapter_id'],
            'chapter_title': chapter['chapter_title'],
            'chapter_number': chapter['chapter_number'],
            'is_summary': True
        })
        
        # Tier 3: Section details
        section_chunks = chunk_text_smart(
            chapter_text,
            max_chars=2400,
            overlap=300
        )
        
        for idx, section_text in enumerate(section_chunks):
            chunks.append({
                'chunk_tier': 3,
                'chunk_type': 'section_detail',
                'chunk_text': section_text,
                'chapter_id': chapter['chapter_id'],
                'parent_chunk_id': chapter_chunk_id,
                'is_summary': False
            })
    
    return chunks
```

---

## 🔍 Search Flow

### Current (Flat)
```
User Query → ES (all chunks) → Top 5 → Return
```

### New (Hierarchical)
```
User Query
  ↓
Stage 1: Search Tier 1+2 (summaries only)
  ↓
Identify relevant chapters
  ↓
Stage 2: Expand to Tier 3 (details) for those chapters
  ↓
Re-rank with cross-encoder
  ↓
Return top 5 with chapter context
```

### API Response Format

```json
{
  "answer": "Calculus is the mathematical study of continuous change...",
  "sources": [
    {
      "chunk_text": "...",
      "chunk_tier": 3,
      "chapter_title": "Chapter 1: Introduction to Calculus",
      "chapter_number": 1,
      "section_title": "1.1 What is Calculus?",
      "page_range": [3, 15],
      "expand_url": "/api/expand/ch01"
    }
  ]
}
```

---

## ⚙️ Configuration

### Environment Variables

```bash
# Feature flags
ENABLE_HIERARCHICAL_CHUNKING=1
HIERARCHICAL_INDEX_ALL_TIERS=1        # 0=tier 3 on-demand, 1=index all
HIERARCHICAL_SUMMARY_METHOD=extractive

# Chunking
SILVER_MAX_PDF_PAGES=1000
SILVER_CHUNK_MAX_CHARS=2400
SILVER_CHUNK_OVERLAP=300
SILVER_CHUNK_MIN_CHARS=400

# TOC
TOC_EXTRACTION_ENABLED=1
TOC_MIN_CONFIDENCE=0.60
TOC_FALLBACK_CHAPTER_SIZE=50          # Pages per chapter (fallback)

# Summary sizes
DOC_SUMMARY_MAX_CHARS=1000
CHAPTER_SUMMARY_MAX_CHARS=800
SECTION_CHUNK_MAX_CHARS=2400
```

### Feature Flag Logic

```python
# In silver_transform.py
if os.getenv('ENABLE_HIERARCHICAL_CHUNKING', '0') == '1':
    chunks = generate_hierarchical_chunks(resource, pdf_path)
else:
    chunks = generate_flat_chunks(pdf_path)  # Old behavior
```

---

## 🧪 Testing

### Unit Tests
```python
def test_extract_pdf_outline():
    toc = extract_toc('test.pdf')
    assert toc['method'] == 'pdf_outline'
    assert toc['confidence'] >= 0.90

def test_chapter_detection():
    assert is_chapter_title('Chapter 1: Introduction') == True
    assert is_chapter_title('1.1 Definition') == False

def test_summary_generation():
    text = "Long text about calculus..."
    summary = generate_chapter_summary(text, max_chars=800)
    assert len(summary) <= 800
    assert 'calculus' in summary.lower()
```

### Integration Tests
```python
def test_hierarchical_chunking_e2e():
    chunks = generate_hierarchical_chunks(resource, 'textbook.pdf')
    
    tier1 = [c for c in chunks if c['chunk_tier'] == 1]
    tier2 = [c for c in chunks if c['chunk_tier'] == 2]
    tier3 = [c for c in chunks if c['chunk_tier'] == 3]
    
    assert len(tier1) == 1           # 1 doc summary
    assert len(tier2) >= 5           # Multiple chapters
    assert len(tier3) > len(tier2)   # More sections
```

### Search Quality Tests
```python
def test_search_returns_chapter_context():
    response = search("What is calculus?", top_k=5)
    
    for result in response['results']:
        assert 'chapter_title' in result
        assert result['chunk_tier'] in [1, 2]  # Summaries first
```

---

## 📈 Success Metrics

### Before (Flat)
- Max pages: 200
- Chunk size: 1800 chars
- Search latency: 800ms
- Precision@5: 0.70
- Chapter context: ❌

### After (Hierarchical)
- Max pages: 1000+
- Chunk size: 2400 chars (tier 3)
- Search latency: <500ms
- Precision@5: 0.82+ (+17%)
- Chapter context: ✅

### Storage Impact
- Tier 1: +0.1% (negligible)
- Tier 2: +5-10% (chapter summaries)
- Tier 3: -70% (if on-demand) or +20% (if indexed)
- **Net**: -60% (on-demand) or +30% (all indexed)

---

## 🚀 Deployment Strategy

### Phase 1: Development (Week 1-2)
- ✅ Create TOC extractor
- ✅ Create summarizer
- ✅ Update schema
- ✅ Unit tests

### Phase 2: Implementation (Week 3-4)
- ✅ Update silver_transform.py
- ✅ Update config
- ✅ Integration tests

### Phase 3: Search Integration (Week 5-6)
- ✅ Update ES mapping
- ✅ Update chatbot_api.py
- ✅ Add re-ranking

### Phase 4: Production Rollout (Week 7-8)
- ✅ Feature flag: OFF (deploy code)
- ✅ Feature flag: ON 10% (canary)
- ✅ Monitor 24h
- ✅ Feature flag: ON 50%
- ✅ Monitor 48h
- ✅ Feature flag: ON 100%

### Rollback Plan
```bash
# Instant rollback: Turn off feature flag
export ENABLE_HIERARCHICAL_CHUNKING=0

# Old chunks still in ES (backward compatible)
# Search falls back to flat mode
```

---

## 📚 File Checklist

### To Create
- [ ] `airflow/src/hierarchical/__init__.py`
- [ ] `airflow/src/hierarchical/toc_extractor.py` (multi-method TOC extraction)
- [ ] `airflow/src/hierarchical/summarizer.py` (extractive summarization)
- [ ] `airflow/tests/unit/test_toc_extractor.py`
- [ ] `airflow/tests/unit/test_summarizer.py`
- [ ] `airflow/tests/integration/test_hierarchical_chunking.py`

### To Modify
- [ ] `airflow/requirements.txt` (add scikit-learn)
- [ ] `airflow/src/silver_transform.py` (_build_chunks_df method)
- [ ] `airflow/src/elasticsearch_sync.py` (mapping + indexing)
- [ ] `airflow/src/chatbot_api.py` (2-stage search + chapter context)
- [ ] `docker-compose.yml` (add env vars)

### SQL Migrations
- [ ] `ALTER TABLE oer_chunks` (add hierarchical fields)
- [ ] `CREATE TABLE oer_document_structure`

---

## ⚠️ Common Pitfalls

1. **TOC extraction fails**: Always have flat fallback
2. **Performance**: Cache TOC in oer_document_structure
3. **Storage**: Use tier 3 on-demand if storage limited
4. **Search quality**: Always keep tier 1-2 indexed
5. **Backward compat**: Feature flag + parallel run

---

## 📖 References

- Full design: `HIERARCHICAL_CHUNKING_DESIGN.md`
- E5 migration: `MIGRATION_E5.md`
- Project README: `README.md`

---

**Created**: 2026-03-31  
**Status**: Ready for Implementation  
**Next**: Start with Phase 1 (TOC Extraction)
