# Bộ câu hỏi kiểm thử chatbot (Vietnamese)

## Thông tin chung

- Mục tiêu: kiểm thử chất lượng trả lời theo ngữ nghĩa và bám nguồn PDF.
- Số lượng: 10 cặp hỏi-đáp.
- Ngôn ngữ: tiếng Việt.
- Nguồn: nội dung trích theo trang từ các PDF đã lập chỉ mục.

## Danh sách tài liệu đã dùng

1. `Linear Algebra (2016)`  
   - source: `open textbook library`  
   - asset_uid: `08c79d5c3d5df1d072e084ad931523d282f87be5057c31b2840ddc6f19ab3507`  
   - chủ đề: `Đại số tuyến tính`  
   - tổng trang: `436`
2. `Introduction to Python Programming – OpenStax`  
   - source: `openstax`  
   - asset_uid: `b9307858c8b74a9552de02ff0be7e28c8642104d9ba6f0d5f96b838ccb23ae32`  
   - chủ đề: `Lập trình Python`  
   - tổng trang: `415`
3. `Optimal, Integral, Likely (OIL) – Calculus & Probability for Commerce`  
   - source: `open textbook library`  
   - asset_uid: `f0fcae8162a7c1c656d74b253aedc6b1d516f503cbc93c5f656a9717698e3179`  
   - chủ đề: `Giải tích tích phân`  
   - tổng trang: `850`
4. `Direct Energy`  
   - source: `open textbook library`  
   - asset_uid: `ff4f7cf5c3c369bdfe9f63ef40f5fe5f1f61cabdd9b26ad087b3f71b8842d49d`  
   - chủ đề: `Vật lý năng lượng / Kỹ thuật điện`  
   - tổng trang: `384`
5. `Linear Algebra (Hefferon)`  
   - source: `open textbook library`  
   - asset_uid: `57f45f296e648404ee0af0fec22fdfb31ce6a844555e1dbad24b6f5caf8a9e7a`  
   - chủ đề: `Đại số tuyến tính`  
   - tổng trang: `404`
6. `Multivariable Calculus with Theory`  
   - source: `mit_ocw`  
   - asset_uid: `18738220d76e1f45d1a9e9989726b526ecdef3ef71d7d3ddc1bffd401fd7c2d4`  
   - chủ đề: `Giải tích nhiều biến`  
   - tổng trang: `17`

## 10 cặp hỏi-đáp kiểm thử

### 1) Định nghĩa đại số tuyến tính

- Câu hỏi: `Đại số tuyến tính là gì? Hãy nêu định nghĩa theo giáo trình.`
- Đáp án kỳ vọng: `Đại số tuyến tính là nghiên cứu về vector và hàm tuyến tính. Cụ thể, vector là những đối tượng có thể cộng lại với nhau; hàm tuyến tính là hàm của vector tôn trọng phép cộng vector. Mục tiêu của đại số tuyến tính là tổ chức thông tin về không gian vector để giải quyết các bài toán liên quan đến hàm tuyến tính nhiều biến.`
- Tài liệu nguồn: `Linear Algebra (2016) – Open Textbook Library`
- Trang nguồn: `9`
- Loại câu hỏi: `định nghĩa`

### 2) Định nghĩa kernel

- Câu hỏi: `Kernel (nhân) của một ánh xạ tuyến tính L là gì?`
- Đáp án kỳ vọng: `Kernel của ánh xạ tuyến tính L: U → V là tập hợp tất cả các vector trong U được ánh xạ tới vector không: kerL = {u ∈ U | L(u) = 0} ⊂ U. Tìm kernel nghĩa là giải hệ phương trình tuyến tính thuần nhất. Vì điều kiện đóng được thỏa mãn, kernel là một không gian con của U.`
- Tài liệu nguồn: `Linear Algebra (2016) – Open Textbook Library`
- Trang nguồn: `201`
- Loại câu hỏi: `định nghĩa`

### 3) Phụ thuộc tuyến tính

- Câu hỏi: `Các vector v₁, v₂, ..., vₙ được gọi là phụ thuộc tuyến tính khi nào? Nêu định nghĩa chính thức.`
- Đáp án kỳ vọng: `Các vector v₁, v₂, ..., vₙ phụ thuộc tuyến tính nếu tồn tại các hằng số c₁, c₂, ..., cₙ không đồng thời bằng không sao cho: c₁v₁ + c₂v₂ + ··· + cₙvₙ = 0. Ngược lại, chúng độc lập tuyến tính. Lưu ý: vector không luôn luôn tạo nên tập phụ thuộc tuyến tính vì α·0 = 0 với mọi vô hướng α.`
- Tài liệu nguồn: `Linear Algebra (2016) – Open Textbook Library`
- Trang nguồn: `204`
- Loại câu hỏi: `định nghĩa`

### 4) Python `def`

- Câu hỏi: `Trong Python, từ khóa def dùng để làm gì? Hãy giải thích cách định nghĩa và gọi một hàm.`
- Đáp án kỳ vọng: `Từ khóa def dùng để định nghĩa một hàm (function) trong Python. Cú pháp: def tên_hàm(): tiếp theo là khối lệnh thụt vào. Sau khi định nghĩa, hàm được gọi bằng tên kèm dấu ngoặc đơn: tên_hàm(). Ví dụ từ sách: def print_phone_num(): → print_phone_num() in ra số điện thoại. Hàm giúp tái sử dụng code và tránh lặp lại.`
- Tài liệu nguồn: `Introduction to Python Programming – OpenStax`
- Trang nguồn: `156–158`
- Loại câu hỏi: `giải thích`

### 5) Boolean trong Python

- Câu hỏi: `Kiểu dữ liệu Boolean trong Python là gì? Nó có những giá trị nào và liên hệ với kiểu số nguyên ra sao?`
- Đáp án kỳ vọng: `Kiểu bool (Boolean) trong Python chỉ có hai giá trị: True (đúng) và False (sai). Bool là lớp con của int: True tương đương 1, False tương đương 0. Vì vậy float(True) = 1.0, int(False) = 0. Các biểu thức so sánh (==, !=, <, >, <=, >=) đều trả về giá trị Boolean.`
- Tài liệu nguồn: `Introduction to Python Programming – OpenStax`
- Trang nguồn: `102–105`
- Loại câu hỏi: `định nghĩa`

### 6) Python `input()`

- Câu hỏi: `Hàm input() trong Python làm gì, và tại sao cần dùng int() hoặc float() sau khi gọi input()?`
- Đáp án kỳ vọng: `Hàm input() đọc một dòng văn bản từ bàn phím do người dùng nhập. Nó nhận một tham số tùy chọn là chuỗi prompt để hiển thị thông báo trước khi nhập. Hàm luôn trả về kiểu str dù người dùng nhập số. Vì vậy, nếu muốn tính toán số học, phải chuyển đổi kiểu: int(input()) để lấy số nguyên, hoặc float(input()) để lấy số thực.`
- Tài liệu nguồn: `Introduction to Python Programming – OpenStax`
- Trang nguồn: `20–22, 53`
- Loại câu hỏi: `giải thích`

### 7) Direct energy conversion

- Câu hỏi: `Chuyển đổi năng lượng trực tiếp (direct energy conversion) là gì? Hãy nêu ví dụ.`
- Đáp án kỳ vọng: `Chuyển đổi năng lượng trực tiếp là quá trình chuyển đổi giữa các dạng năng lượng như điện, từ, động năng, thế năng, quang, hóa học và hạt nhân. Ví dụ tự nhiên: ánh sáng mặt trời làm ấm nhà (quang → nhiệt); lá rơi (thế năng → động năng). Ví dụ thiết bị: thermocouple (cảm biến nhiệt độ), pin nhiên liệu (fuel cell) và pin mặt trời cấp điện cho vệ tinh và ô tô.`
- Tài liệu nguồn: `Direct Energy – Open Textbook Library`
- Trang nguồn: `11–12`
- Loại câu hỏi: `định nghĩa`

### 8) Trạng thái vật chất

- Câu hỏi: `Theo giáo trình Direct Energy, vật liệu có thể được phân loại theo trạng thái vật chất như thế nào? Liệt kê đầy đủ.`
- Đáp án kỳ vọng: `Theo giáo trình Direct Energy, vật liệu có thể được phân loại thành bốn trạng thái vật chất chính: (1) Chất rắn (solids), (2) Chất lỏng (liquids), (3) Chất khí (gases), và (4) Plasma – khí bị ion hóa. Ngoài ra còn tồn tại các trạng thái vật chất ít phổ biến hơn như ngưng tụ Bose–Einstein, nhưng sách không đi sâu vào các trạng thái này.`
- Tài liệu nguồn: `Direct Energy – Open Textbook Library`
- Trang nguồn: `22`
- Loại câu hỏi: `liệt kê`

### 9) Phân cực sóng điện từ

- Câu hỏi: `Sóng điện từ phân cực tuyến tính (linearly polarized), phân cực tròn (circularly polarized) và phân cực elip (elliptically polarized) khác nhau như thế nào?`
- Đáp án kỳ vọng: `Theo giáo trình Direct Energy, sóng điện từ có điện trường E, từ trường H và hướng truyền luôn vuông góc nhau. (1) Phân cực tuyến tính: hướng của E cố định trên một đường thẳng. (2) Phân cực tròn: hướng E quay đều quanh trục truyền – hình chiếu lên mặt phẳng là đường tròn. (3) Phân cực elip: hướng E quay không đều – hình chiếu là hình elip. Chiều quay (trái/phải) xác định bằng quy tắc bàn tay phải theo hướng truyền sóng.`
- Tài liệu nguồn: `Direct Energy – Open Textbook Library`
- Trang nguồn: `92–94`
- Loại câu hỏi: `giải thích`

### 10) Tổng Riemann

- Câu hỏi: `Tổng Riemann (Riemann sum) là gì và nó liên hệ với tích phân xác định như thế nào? Nêu công thức của tổng Riemann phải (right Riemann sum).`
- Đáp án kỳ vọng: `Tổng Riemann là phương pháp xấp xỉ diện tích dưới đường cong bằng cách chia khoảng [a, b] thành n hình chữ nhật có cùng chiều rộng Δx = (b−a)/n. Công thức tổng Riemann phải: Σᵢ₌₁ⁿ f(a + i·Δx)·Δx. Khi n → ∞, tổng này hội tụ về tích phân xác định: ∫ₐᵇ f(x)dx. Ví dụ từ sách: với f(x) = x³, a = −1, b = 5, n = 3 thì Δx = 2 và các điểm trái là x₀ = −1, x₁ = 1, x₂ = 3.`
- Tài liệu nguồn: `OIL – Calculus & Probability (Open Textbook Library)`
- Trang nguồn: `440–442`
- Loại câu hỏi: `công thức`

## Tóm tắt phân bổ

- `Linear Algebra (2016)`: câu `1, 2, 3` (định nghĩa ×3)
- `Introduction to Python Programming`: câu `4, 5, 6` (giải thích ×2, định nghĩa ×1)
- `Direct Energy`: câu `7, 8, 9` (định nghĩa ×1, liệt kê ×1, giải thích ×1)
- `OIL Calculus`: câu `10` (công thức ×1)

Tổng: 4 tài liệu, 4 chủ đề, 4 loại câu hỏi.
