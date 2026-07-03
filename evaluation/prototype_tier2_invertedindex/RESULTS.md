# Prototype: Tier-2 page-level inverted index (BM25) + bilingual glossary

Scope: 12/21 sách VI có PDF local (8 sách MIT OCW không có PDF → loại).
Corpus: 8395 trang. Glossary: dịch qua Wikipedia langlinks (free, không token Groq).
Metric: book-level Hit@K + MRR (gold = source_asset_uid trong top-K trang).

## ALL in-scope (n=46, gồm cả find_material/metadata)
| variant        | Hit@5  | Hit@10 | MRR    |
|----------------|--------|--------|--------|
| vi_raw         | 0.5217 | 0.6739 | 0.4012 |
| groq_trans     | 0.5652 | 0.7174 | 0.4447 |
| vi_glossary    | 0.5000 | 0.6957 | 0.4012 |
| groq_glossary  | 0.5435 | 0.7391 | 0.4447 |
| en_ceiling*    | 0.6739 | 0.7174 | 0.6057 |

## CONTENT only (n=34, definition+comparison+multi_step)
| variant        | Hit@5  | Hit@10 | MRR    |
|----------------|--------|--------|--------|
| vi_raw         | 0.6176 | 0.7353 | 0.4768 |
| groq_trans     | 0.6471 | 0.7647 | 0.5062 |
| vi_glossary    | 0.5882 | 0.7353 | 0.4731 |
| groq_glossary  | 0.6176 | 0.7647 | 0.5025 |
| en_ceiling*    | 0.5588 | 0.6176 | 0.4886 |

(*en_ceiling = query bằng ground_truth EN; không phải trần sạch vì ground_truth dài/nhiễu.)

## Kết luận
1. BM25 page-level (toàn sách) cho retrieval VI khá tốt (content Hit@10=0.76) —
   cao hơn production 0.48 → đưa Tier-2 lên chỉ mục ngược là hướng đáng giá.
2. Glossary kiểu trích corpus-term→VI gần như vô tác dụng (khớp 2/46) vì
   không trùng từ vựng câu hỏi; câu hỏi VI lại đã lẫn nhiều tiếng Anh.
3. Dịch query (Groq) cho mức tăng nhỏ ổn định (+3-4% Hit, +3% MRR).
4. Lưu ý: test set VI bị code-mixed (tên sách/thuật ngữ tiếng Anh) → làm
   nhiễu phép đo cross-lingual.

## END-TO-END (full VI eval qua chatbot API, container nạp code mới + cờ ES)
Đã restart chatbot với PAGEINDEX_TIER2_BACKEND=elasticsearch, chạy 100 câu VI,
sau đó KHÔI PHỤC chatbot về backend pdf gốc.

| Metric (VI, 100 câu) | Baseline (PDF tier2) | ES tier2 |
|----------------------|----------------------|----------|
| evidence_hit_rate (metric cũ) | 0.4826 | 0.9202 |
| source-match đúng sách (content, 12 sách) | 0.088 | 0.206 |

### CẢNH BÁO trung thực
- evidence_hit_rate 0.92 PHẦN LỚN LÀ ARTIFACT: đường ES chỉ trả về trang đã
  match BM25 -> tỷ lệ (trang khớp / trang tải) ≈ 1 một cách hiển nhiên. KHÔNG
  chứng minh retrieve đúng sách/đúng đáp án.
- source-match (không thiên vị) chỉ tăng 0.088 -> 0.206.

### Chẩn đoán gốc rễ (CONTENT, 12 sách index, n=34)
- 76% retrieve SAI sách  -> TIER-1 chọn nhầm sách
-  3% không retrieve được
- 21% đúng sách gold

=> Nút thắt thật của VI là TIER-1 (chọn sách), KHÔNG phải Tier-2.
   ES Tier-2 retrieval tốt (offline Hit@5=0.68 khi tìm tự do) nhưng bị Tier-1
   gate vào sai sách nên không phát huy được end-to-end.
   (Lưu ý: source-match hơi thiếu tin cậy vì câu hỏi chung có thể đúng nhiều
    sách, nên 76% "sai" là cận trên của vấn đề.)

## PHƯƠNG ÁN 2 — Cross-book retrieval (đã triển khai, sau cờ PAGEINDEX_TIER2_CROSSBOOK)
Cho câu hỏi nội dung: 1 truy vấn BM25 xuyên TOÀN BỘ sách trên oer_pages_tier2 →
lấy top pages → 1 lần gọi Groq trả lời. Bỏ qua Tier-1 chọn-sai-sách. Có fallback.

### Source-match (CONTENT, 12 sách, n=34) — metric không thiên vị
| Cách | source-match |
|------|--------------|
| Baseline PDF tier2          | 0.088 |
| Tier-2 ES (gate 1 sách)     | 0.206 |
| Cross-book (Phương án 2)    | 0.794 |

### Latency (smoke test)
- Truy vấn ES xuyên 8395 trang: ~31 ms (không đáng kể).
- Câu trả lời bình thường: ~4s (bằng baseline).
- Outlier 41s quan sát được = Groq 429 rate-limit do bắn request liên tiếp khi
  test, KHÔNG phải do cross-book. Pipeline thật có batch-pause để giảm thiểu.

### Kết luận
- Cross-book giải đúng nút thắt Tier-1: gold book có trong context 0.088 -> 0.79.
- Latency không tăng (retrieval 31ms, 1 lần Groq). Throttle là vấn đề quota Groq.
- Code sau cờ (mặc định pdf) + fallback; chatbot đã khôi phục về backend pdf gốc.

## FULL END-TO-END EVAL (100 câu VI qua API, cross-book bật, Groq đã hồi)
100/100 success, 0 errors. Sau đó đã KHÔI PHỤC chatbot về backend pdf gốc.

| Metric (VI) | Baseline (PDF) | Cross-book |
|-------------|----------------|------------|
| source-match 12 sách index (n=46)   | 0.065 | 0.935 |
| source-match 12 sách + CONTENT (n=34)| 0.088 | 0.912 |
| source-match ALL in-scope (n=80)    | 0.063 | 0.538 |
| evidence_hit_rate (cũ, thiên vị)    | 0.483 | 0.988 |
| Latency trung bình (ms)             | 2634  | 2986  |
| Latency P95 (ms)                    | 5978  | 4843  |

Kết luận:
- Sách CÓ trong index: retrieve đúng sách 0.07 -> 0.93 (end-to-end, metric công bằng).
- source-match ALL 0.54 bị kéo xuống bởi 34 câu thuộc 8 sách MIT chưa index
  (cross-book chỉ tìm trong 12 sách đã index). Index đủ 21 sách -> số này sẽ cao hơn.
- Latency ~3s, KHÔNG chậm hơn baseline; P95 còn tốt hơn. 0 lỗi 429 (ít call hơn).

## INDEX TOÀN KHO (hướng A — từ MinIO) — HOÀN TẤT
Nguồn: Iceberg oer_documents (6224 doc, tất cả language=en) → tải PDF từ MinIO →
trích text (pypdf) → bỏ PDF scan ảnh → index vào oer_pages_tier2.

| | |
|---|---|
| Sách index được | 5955 / 6224 |
| Tổng trang | 283.018 |
| Bỏ (scan ảnh, no-text) | 211 |
| Lỗi (PDF hỏng/missing) | 23 |

Lưu ý: sách tiếng Việt là PDF scan ảnh → không có text → đã loại ở tầng curation
(oer_documents toàn EN). Muốn dùng sách Việt phải chạy OCR (dự án riêng).
Script + map lưu tại: index_from_minio.py, docs_map.json (mount bền).

## SCOPE GATE cho cross-book (sửa OOS) — HOÀN TẤT
Vấn đề: BM25 luôn tìm thấy trang khớp → không tách được OOS (điểm in-scope≈OOS).
Giải: thêm 1 lần LLM phân loại chủ đề câu hỏi (academic vs đời sống) trước khi trả lời.
- Prompt 1-shot ban đầu: OOS detect 0.65 (thiếu nhất quán).
- Prompt few-shot (9 ví dụ): **OOS detect 0.90**, content giữ **1.00** (không false-positive).
Cờ: PAGEINDEX_TIER2_CROSSBOOK_SCOPE_CHECK (mặc định bật). +1 Groq call/câu nội dung;
OOS thì chỉ tốn 1 call (từ chối sớm, không sinh answer).
