# Đánh giá hệ thống Hỏi–Đáp OER

Module đánh giá cho hệ thống hỏi–đáp học liệu mở (OER): **truy hồi BM25 ở cấp trang (vectorless)** + **sinh câu trả lời bằng qwen2.5:7b**. Đánh giá theo 2 tầng: **Retrieval** (Recall@k, MRR) và **Generation** (RAGAS + human-eval).

## Cấu trúc thư mục
```
evaluation/
├── scripts/    # mã nguồn pipeline đánh giá (CHẠY TỪ thư mục evaluation/)
├── data/       # bộ câu hỏi thử nghiệm + đầu ra pipeline (answer + context)
├── results/    # số liệu metric + human-eval + dashboard
├── docs/       # phần viết luận văn, kế hoạch, slide
└── prototype_tier2_invertedindex/   # prototype Tier-2 (tham khảo)
```

### `scripts/`
| File | Vai trò |
|---|---|
| `gen_course_testset_llm.py` | Sinh bộ test theo môn (LLM) → `data/test_set_course_v2.json` |
| `run_pipeline.py` | Gửi câu hỏi qua API chatbot → `data/pipeline_outputs_*.json` |
| `retrieval_metrics.py` | Tính **Recall@k, MRR** |
| `judge_relevance.py` | Chấm đa-liên-quan (multi-relevant, LLM judge) |
| `ragas_eval.py` | **RAGAS**: Faithfulness, Answer Relevancy |
| `human_eval_make.py` / `human_eval_score.py` | Tạo template + chấm **human-eval** |
| `make_simple_dashboard.py` | Vẽ dashboard 4 chỉ số → `results/dashboard_simple.png` |
| `config.py` | Cấu hình chung (Groq key/model) — **các script khác `from config import`** |
| `course_metrics.py`, `compare_course.py`, `eval_retrieval_direct.py` | tiện ích phụ |

### `data/`
- `test_set_course_v2.json` — **130 câu** (course-scoped, EN+VI, 4 loại)
- `test_set_subset.json` — 18 câu (dùng cho A/B faithfulness)
- `pipeline_outputs_course_v2_new.json` — answer (prompt grounding mới)
- `pipeline_outputs_course_v2_fixed.json` — answer (sau khi fix intent find_material)

### `results/`
- `course_multirelevant_result.json` — Recall@5 = **0.85**, MRR = **0.79**
- `ragas_new70b_FINAL.json` — Answer Relevancy = **0.87** (judge llama-3.3-70b, full 130)
- `ragas_sub_new.json` / `ragas_sub_old.json` — A/B faithfulness 8b (**0.63 → 0.84**)
- `ragas_70b_A.json`, `ragas_70b_B.json`, `ragas_new70b_000.json`, `ragas_new70b_018.json` — faithfulness 70b (~0.77, mẫu)
- `human_eval_template_v2.csv` — 32 câu chấm tay
- `dashboard_simple.png` — bảng 4 chỉ số

### `docs/`
- `3.4_DANH_GIA_revised.md` — mục 3.4 luận văn (đánh giá)
- `CHUONG2_LLM_kientruc.md` — LLM + kiến trúc + mô hình áp dụng
- `RAGAS_PLAN.md`, `chatbot_qa_testset_vi.md`, `metrics_frame.txt`

## Cách chạy (đứng tại thư mục `evaluation/`)
```bash
# 1) Sinh bộ test theo môn
python scripts/gen_course_testset_llm.py --out data/test_set_course_v2.json

# 2) Chạy pipeline qua API chatbot
python scripts/run_pipeline.py --test-set data/test_set_course_v2.json \
       --output data/pipeline_outputs_course_v2_new.json

# 3) Retrieval metrics (Recall@k, MRR)
python scripts/retrieval_metrics.py data/pipeline_outputs_course_v2_new.json
python scripts/judge_relevance.py  data/pipeline_outputs_course_v2_new.json data/test_set_course_v2.json

# 4) RAGAS (truyền GROQ_API_KEY inline)
GROQ_API_KEY=<eval_key> python scripts/ragas_eval.py \
       data/pipeline_outputs_course_v2_fixed.json --out results/ragas.json

# 5) Dashboard
python scripts/make_simple_dashboard.py
```
> ⚠️ Chạy **từ thư mục `evaluation/`** (không phải từ trong `scripts/`) để đường dẫn `data/…`, `results/…` và `from config import` hoạt động đúng.

## Kết quả chính
| Tầng | Chỉ số | Giá trị | Nguồn |
|---|---|---|---|
| Retrieval | Recall@5 / MRR | **0.85 / 0.79** | Manning 2008 · Voorhees 1999 |
| Generation | Answer Relevancy | **0.87** | RAGAS — Es et al. 2024 |
| Generation | Faithfulness | **0.84** (8b) / 0.77 (70b) | RAGAS |
| Kiểm chứng | Human-eval | **0.84** | chấm tay |

*(`ragas_env/`, `logs/`, `__pycache__/` không đẩy lên git — xem `.gitignore`.)*
