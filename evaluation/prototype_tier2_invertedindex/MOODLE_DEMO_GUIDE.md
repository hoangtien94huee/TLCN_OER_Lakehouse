# Hướng dẫn DEMO chatbot trên Moodle (course-scoped)

## Trạng thái hệ thống (đã set up sẵn)
- Chatbot API: http://172.19.24.202:18088/api  (container oer-airflow-scraper, port 18088)
- Cross-book Tier-2: BẬT (chỉ mục oer_pages_tier2, 5955 sách / 283k trang)
- Scope-check (chặn câu đời sống): BẬT
- Course-scoping (khóa theo môn): BẬT
- 4 cờ đã ghi vào docker-compose.yml → sống qua recreate.
  (Lưu ý: `docker restart` thường KHÔNG nạp cờ mới — cần `docker compose --profile etl up -d oer-scraper` để chốt.)

## 10 môn — TÊN PHẢI ĐẶT ĐÚNG khi tạo khóa học trên Moodle
(so khớp bỏ dấu/thường hóa, nên hoa/thường/thiếu dấu vẫn nhận; có thể kèm hậu tố như "- HK1")

1. Kinh tế học
2. Đại số tuyến tính
3. Quản trị kinh doanh
4. Xác suất thống kê
5. Lập trình và Khoa học máy tính
6. Giải tích
7. Vật lý đại cương
8. Marketing
9. Kinh tế vi mô và vĩ mô
10. Nguyên lý kế toán

(map sách: evaluation/prototype_tier2_invertedindex/course_book_map.json — sửa được)

## ⚠️ ĐIỀU KIỆN BẮT BUỘC: Moodle phải gửi course_name cho API
Course-scoping chỉ chạy khi widget chatbot trên Moodle **gửi `course_name`** (tên môn) trong request tới /api/ask.
- Nếu widget đã gửi context Moodle (course_name) → scoping tự chạy.
- Nếu CHƯA gửi → chatbot chạy global (không khóa môn). Cần kiểm tra/cấu hình widget.

Kiểm tra nhanh (giả lập Moodle gửi course_name):
```
curl -s http://172.19.24.202:18088/api/ask -H 'Content-Type: application/json' \
  -d '{"question":"Đạo hàm của hàm số là gì?","course_name":"Giải tích","language":"vi"}' | python3 -m json.tool
```

## Kịch bản demo (mỗi môn, hỏi THONG THẢ — tránh Groq 429)
1. Vào khóa học (vd "Giải tích") → mở chatbot.
2. **Gợi ý sách:** "Gợi ý sách cho tôi" → ra sách của môn.
3. **Hỏi nội dung trong môn:** "Đạo hàm của hàm số là gì?" → trả lời + trích nguồn (Calculus Volume 1).
4. **Hỏi lệch môn (trong Kinh tế hỏi đạo hàm):** kỳ vọng từ chối "ngoài phạm vi môn".
5. **Hỏi đời sống:** "Công thức nấu phở?" → từ chối.

## Lưu ý quan trọng khi demo
- **Hỏi từ tốn, mỗi câu chờ trả lời xong.** Hỏi dồn dập → Groq free-tier 429 → chậm/sai. (Model hiện tại llama-3.3-70b giới hạn thấp; cân nhắc đổi sang model free-tier nhanh hơn nếu cần.)
- **In-course (hỏi đúng môn): chạy tốt, ổn định.**
- **Out-of-course refusal:** hoạt động khi Groq không bị nghẽn. Với từ liên ngành (vd "đạo hàm" có cả trong kinh tế) có thể vẫn trả lời theo góc độ môn đó — không hẳn sai.
- **Quay video demo dự phòng** phòng khi mạng/Groq trục trặc.

## Lệnh kiểm tra nhanh trạng thái
```
# health + cờ
docker exec oer-airflow-scraper bash -lc 'curl -s localhost:8088/api/health; \
  p=$(pgrep -f "uvicorn chatbot_api"|head -1); tr "\0" "\n" < /proc/$p/environ | grep PAGEINDEX_TIER2'
# số sách đã index
curl -s localhost:9200/oer_pages_tier2/_count
```