<?php
/**
 * HCMUTE Demo Data Seeder v3
 * Truong Dai hoc Su pham Ky thuat TP.HCM (HCMUTE)
 * Khoa Công nghệ Thông tin - Chuong trinh chinh quy
 *
 * Usage: docker exec oer-moodle php /tmp/moodle_seed_v3.php
 */
define('CLI_SCRIPT', true);
require('/opt/bitnami/moodle/config.php');
require_once($CFG->libdir  . '/clilib.php');
require_once($CFG->libdir  . '/enrollib.php');
require_once($CFG->libdir  . '/moodlelib.php');
require_once($CFG->dirroot . '/course/lib.php');
require_once($CFG->dirroot . '/user/lib.php');

echo "=== HCMUTE DEMO SEEDER v3 ===\n";
echo "Truong: Dai hoc Su pham Ky thuat TP.HCM (HCMUTE)\n\n";

// ── MODULE TYPE IDs ────────────────────────────────────────────────────────────
$MOD = [];
foreach (['label', 'page', 'forum', 'assign'] as $m) {
    $r = $DB->get_record('modules', ['name' => $m]);
    if ($r) $MOD[$m] = $r->id;
}

// ── HELPER: ensure section exists and set name/summary ────────────────────────
function h_section($DB, $cid, $no, $name = '', $summary = '') {
    course_create_sections_if_missing($cid, $no);
    $s = $DB->get_record('course_sections', ['course' => $cid, 'section' => $no]);
    if ($s) {
        if ($name    !== '') $s->name = $name;
        if ($summary !== '') { $s->summary = $summary; $s->summaryformat = FORMAT_HTML; }
        $DB->update_record('course_sections', $s);
    }
    return $s ? $s->id : null;
}

// ── HELPER: add a course module (generic) ─────────────────────────────────────
function h_add_cm($DB, $MOD, $cid, $modname, $iid, $secno) {
    $cm = new stdClass();
    $cm->course      = $cid;  $cm->module    = $MOD[$modname];
    $cm->instance    = $iid;  $cm->section   = 0;
    $cm->visible     = 1;     $cm->visibleold = 1;
    $cm->added       = time(); $cm->score     = 0;
    $cm->indent      = 0;     $cm->showdescription = 1;
    $cmid = $DB->insert_record('course_modules', $cm);
    course_add_cm_to_section($cid, $cmid, $secno);
    context_module::instance($cmid); // ensure context exists
    return $cmid;
}

// ── HELPER: add label ─────────────────────────────────────────────────────────
function h_label($DB, $MOD, $cid, $secno, $html) {
    if (!isset($MOD['label'])) return null;
    $r = new stdClass();
    $r->course = $cid; $r->name = "lbl_{$cid}_{$secno}";
    $r->intro  = $html; $r->introformat = FORMAT_HTML;
    $r->timemodified = time();
    $lid = $DB->insert_record('label', $r);
    return h_add_cm($DB, $MOD, $cid, 'label', $lid, $secno);
}

// ── HELPER: add page resource ─────────────────────────────────────────────────
function h_page($DB, $MOD, $cid, $secno, $name, $html) {
    if (!isset($MOD['page'])) return null;
    $r = new stdClass();
    $r->course = $cid; $r->name = $name;
    $r->intro  = ''; $r->introformat = FORMAT_HTML;
    $r->content = $html; $r->contentformat = FORMAT_HTML;
    $r->legacyfiles = 0; $r->legacyfileslast = null;
    $r->display = 5; $r->displayoptions = ''; $r->revision = 1;
    $r->timemodified = time();
    $pid = $DB->insert_record('page', $r);
    return h_add_cm($DB, $MOD, $cid, 'page', $pid, $secno);
}

// ── HELPER: add forum ─────────────────────────────────────────────────────────
function h_forum($DB, $MOD, $cid, $secno, $name, $intro) {
    if (!isset($MOD['forum'])) return null;
    $r = new stdClass();
    $r->course   = $cid; $r->type = 'general'; $r->name = $name;
    $r->intro    = $intro; $r->introformat = FORMAT_HTML;
    $r->assessed = 0; $r->scale = 100; $r->maxbytes = 0;
    $r->maxattachments = 9; $r->forcesubscribe = 0;
    $r->trackingtype = 1; $r->rsstype = 0; $r->rssarticles = 0;
    $r->timemodified = time(); $r->warnafter = 0;
    $r->blockafter = 0; $r->blockperiod = 0;
    $r->completiondiscussions = 0; $r->completionreplies = 0;
    $r->completionposts = 0; $r->displaywordcount = 0;
    $fid = $DB->insert_record('forum', $r);
    return h_add_cm($DB, $MOD, $cid, 'forum', $fid, $secno);
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 1 — DELETE OLD DATA
// ═══════════════════════════════════════════════════════════════════════════════
echo "[1/5] Xoa du lieu cu...\n";
foreach ($DB->get_records_select('course', 'id > 1') as $c) {
    delete_course($c->id, false);
    echo "  - Xoa course: {$c->fullname}\n";
}
$DB->delete_records_select('course_categories', 'id > 1');
foreach (['22133024','22133061','le.thanh.tung','nguyen.thi.mai.anh'] as $u) {
    if ($rec = $DB->get_record('user', ['username' => $u])) {
        $DB->delete_records('user', ['id' => $rec->id]);
        echo "  - Xoa user: {$u}\n";
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 2 — CATEGORIES
// ═══════════════════════════════════════════════════════════════════════════════
echo "\n[2/5] Tao danh muc HCMUTE-CNTT...\n";

$c0 = new stdClass();
$c0->name = 'Khoa Công nghệ Thông tin'; $c0->idnumber = 'CNTTSPKT';
$c0->description = 'Trường Đại học Công Nghệ Kỹ thuật TP.HCM (HCMUTE) – 01 Võ Văn Ngân, P.Linh Chiểu, TP.Thủ Đức';
$c0->parent = 0; $c0->sortorder = 10000; $c0->visible = 1;
$c0->depth = 1; $c0->path = ''; $c0->timemodified = time();
$ROOT = $DB->insert_record('course_categories', $c0);
$DB->set_field('course_categories', 'path', "/{$ROOT}", ['id' => $ROOT]);

$CAT = [];
foreach ([
    'Y1' => 'Năm nhất – Cơ sở Đại cương',
    'Y2' => 'Năm hai – Cơ sở Ngành',
    'Y3' => 'Năm ba – Chuyên ngành',
] as $k => $nm) {
    $c = new stdClass();
    $c->name = $nm; $c->idnumber = "HCMUTE-{$k}";
    $c->parent = $ROOT; $c->sortorder = 1000; $c->visible = 1;
    $c->depth = 2; $c->path = ''; $c->timemodified = time();
    $id = $DB->insert_record('course_categories', $c);
    $DB->set_field('course_categories', 'path', "/{$ROOT}/{$id}", ['id' => $id]);
    $CAT[$k] = $id;
    echo "  + $nm (id=$id)\n";
}
fix_course_sortorder();

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 3 — 10 COURSES WITH RICH CONTENT
// ═══════════════════════════════════════════════════════════════════════════════
echo "\n[3/5] Tao 10 khoa hoc CNTT voi noi dung chi tiet...\n";

// Each entry: [fullname, shortname, idnumber, cat_key, startdate, summary, sections[1..15]]
// sections[0] = General (no name override), sections[1..15] = week topics
$COURSES = [

// ──────────────────────────────────────────────────────────────────────────────
[
'fullname'  => 'Đại số tuyến tính',
'shortname' => 'MATH101',
'idnumber'  => 'HCMUTE-MATH101',
'cat'       => 'Y1',
'start'     => mktime(0,0,0,9,2,2024),
'end'       => mktime(0,0,0,12,27,2024),
'summary'   => '<p><strong>Mã môn học:</strong> MATH101 &nbsp;|&nbsp; <strong>Số tín chỉ:</strong> 3 TC</p><p>Môn học cung cấp kiến thức nền tảng về hệ phương trình tuyến tính, ma trận, định thức, không gian vector và ánh xạ tuyến tính. Rất quan trọng cho Đồ họa máy tính và Machine Learning.</p>',
'weeks' => [
    1 => ['Tuần 1: Hệ phương trình tuyến tính', '<p>Giới thiệu về hệ phương trình tuyến tính, phương pháp khử Gauss.</p>'],
    2 => ['Tuần 2: Ma trận và các phép toán', '<p>Cộng, nhân ma trận, ma trận chuyển vị, ma trận khả nghịch.</p>'],
    3 => ['Tuần 3: Định thức', '<p>Tính chất của định thức, công thức Cramer.</p>'],
    4 => ['Tuần 4: Không gian vector', '<p>Định nghĩa, tính chất, không gian con.</p>'],
    5 => ['Tuần 5: Độc lập tuyến tính', '<p>Cơ sở và số chiều của không gian vector.</p>'],
    6 => ['Tuần 6: Ánh xạ tuyến tính', '<p>Định nghĩa ánh xạ, nhân và ảnh của ánh xạ tuyến tính.</p>'],
    7 => ['Tuần 7: Ma trận của ánh xạ tuyến tính', '<p>Biểu diễn ánh xạ tuyến tính qua ma trận.</p>'],
    8 => ['Tuần 8: KIỂM TRA GIỮA KỲ', '<p>Thi tự luận trên giấy.</p>'],
    9 => ['Tuần 9: Giá trị riêng và vector riêng', '<p>Tính toán giá trị riêng, đa thức đặc trưng.</p>'],
    10=> ['Tuần 10: Chéo hóa ma trận', '<p>Điều kiện chéo hóa, ứng dụng tính lũy thừa ma trận.</p>'],
    11=> ['Tuần 11: Không gian Euclid', '<p>Tích vô hướng, trực giao và trực chuẩn.</p>'],
    12=> ['Tuần 12: Quá trình Gram-Schmidt', '<p>Trực giao hóa cơ sở.</p>'],
    13=> ['Tuần 13: Dạng toàn phương', '<p>Đưa dạng toàn phương về dạng chính tắc.</p>'],
    14=> ['Tuần 14: Ứng dụng trong CNTT', '<p>Ma trận trong xử lý ảnh và Machine Learning.</p>'],
    15=> ['Tuần 15: THI CUỐI KỲ', '<p>Thi tự luận 90 phút.</p>'],
]
],

// ──────────────────────────────────────────────────────────────────────────────
[
'fullname'  => 'Giải tích',
'shortname' => 'MATH102',
'idnumber'  => 'HCMUTE-MATH102',
'cat'       => 'Y1',
'start'     => mktime(0,0,0,1,6,2025),
'end'       => mktime(0,0,0,5,16,2025),
'summary'   => '<p><strong>Mã môn học:</strong> MATH102 &nbsp;|&nbsp; <strong>Số tín chỉ:</strong> 4 TC</p><p>Học về hàm số, giới hạn, đạo hàm, tích phân và ứng dụng. Đây là nền tảng tối ưu hóa trong Trí tuệ nhân tạo.</p>',
'weeks' => [
    1 => ['Tuần 1: Hàm số và Đồ thị', '<p>Ôn tập hàm số, các hàm cơ bản.</p>'],
    2 => ['Tuần 2: Giới hạn và Liên tục', '<p>Định nghĩa và các định lý về giới hạn.</p>'],
    3 => ['Tuần 3: Đạo hàm', '<p>Ý nghĩa hình học, vật lý của đạo hàm.</p>'],
    4 => ['Tuần 4: Quy tắc tính đạo hàm', '<p>Đạo hàm hàm hợp, hàm ẩn, hàm ngược.</p>'],
    5 => ['Tuần 5: Ứng dụng của đạo hàm', '<p>Cực trị, khảo sát sự biến thiên hàm số.</p>'],
    6 => ['Tuần 6: Tích phân bất định', '<p>Khái niệm nguyên hàm, các phương pháp tính tích phân.</p>'],
    7 => ['Tuần 7: Tích phân xác định', '<p>Định nghĩa tích phân Riemann, định lý cơ bản của giải tích.</p>'],
    8 => ['Tuần 8: KIỂM TRA GIỮA KỲ', '<p>Thi tự luận trên giấy.</p>'],
    9 => ['Tuần 9: Ứng dụng tích phân', '<p>Tính diện tích hình phẳng, thể tích khối tròn xoay.</p>'],
    10=> ['Tuần 10: Hàm nhiều biến', '<p>Đạo hàm riêng, cực trị hàm nhiều biến.</p>'],
    11=> ['Tuần 11: Tích phân bội hai', '<p>Định nghĩa, đổi biến trong hệ tọa độ cực.</p>'],
    12=> ['Tuần 12: Chuỗi số', '<p>Chuỗi số vô hạn, các tiêu chuẩn hội tụ.</p>'],
    13=> ['Tuần 13: Chuỗi lũy thừa', '<p>Chuỗi Taylor và Maclaurin.</p>'],
    14=> ['Tuần 14: Phương trình vi phân', '<p>Phương trình vi phân bậc một cơ bản.</p>'],
    15=> ['Tuần 15: THI CUỐI KỲ', '<p>Thi tự luận 90 phút.</p>'],
]
],

// ──────────────────────────────────────────────────────────────────────────────
[
'fullname'  => 'Xác suất thống kê',
'shortname' => 'MATH103',
'idnumber'  => 'HCMUTE-MATH103',
'cat'       => 'Y2',
'start'     => mktime(0,0,0,9,2,2025),
'end'       => mktime(0,0,0,12,27,2025),
'summary'   => '<p><strong>Mã môn học:</strong> MATH103 &nbsp;|&nbsp; <strong>Số tín chỉ:</strong> 3 TC</p><p>Cung cấp kiến thức về xác suất, biến ngẫu nhiên, phân phối xác suất và thống kê suy diễn. Cực kỳ quan trọng cho Khoa học dữ liệu.</p>',
'weeks' => [
    1 => ['Tuần 1: Mở đầu về Xác suất', '<p>Không gian mẫu, biến cố, xác suất cổ điển.</p>'],
    2 => ['Tuần 2: Xác suất có điều kiện', '<p>Quy tắc nhân, Định lý Bayes.</p>'],
    3 => ['Tuần 3: Biến ngẫu nhiên', '<p>Biến ngẫu nhiên rời rạc và liên tục.</p>'],
    4 => ['Tuần 4: Hàm phân phối xác suất', '<p>Kỳ vọng, phương sai, độ lệch chuẩn.</p>'],
    5 => ['Tuần 5: Các phân phối rời rạc', '<p>Phân phối Nhị thức, Poisson, Siêu hình học.</p>'],
    6 => ['Tuần 6: Các phân phối liên tục', '<p>Phân phối Chuẩn (Normal), Phân phối Mũ.</p>'],
    7 => ['Tuần 7: Biến ngẫu nhiên hai chiều', '<p>Phân phối đồng thời, hiệp phương sai, hệ số tương quan.</p>'],
    8 => ['Tuần 8: KIỂM TRA GIỮA KỲ', '<p>Thi tự luận 90 phút.</p>'],
    9 => ['Tuần 9: Luật số lớn và Định lý Giới hạn trung tâm', '<p>Ý nghĩa và ứng dụng.</p>'],
    10=> ['Tuần 10: Thống kê mô tả', '<p>Thu thập, trình bày dữ liệu, các đại lượng thống kê mẫu.</p>'],
    11=> ['Tuần 11: Ước lượng tham số', '<p>Ước lượng điểm, khoảng tin cậy cho trung bình và tỷ lệ.</p>'],
    12=> ['Tuần 12: Kiểm định giả thuyết', '<p>Các bước kiểm định, sai lầm loại I và loại II.</p>'],
    13=> ['Tuần 13: Kiểm định trung bình mẫu', '<p>Kiểm định Z, kiểm định T (T-test).</p>'],
    14=> ['Tuần 14: Phân tích hồi quy và tương quan', '<p>Hồi quy tuyến tính đơn biến, phương pháp bình phương tối thiểu.</p>'],
    15=> ['Tuần 15: THI CUỐI KỲ', '<p>Thi tự luận 90 phút.</p>'],
]
],

// ──────────────────────────────────────────────────────────────────────────────
[
'fullname'  => 'Nhập môn Lập trình Python',
'shortname' => 'IT201',
'idnumber'  => 'HCMUTE-IT201',
'cat'       => 'Y1',
'start'     => mktime(0,0,0,9,2,2024),
'end'       => mktime(0,0,0,12,27,2024),
'summary'   => '<p><strong>Mã môn học:</strong> IT201 &nbsp;|&nbsp; <strong>Số tín chỉ:</strong> 3 TC</p><p>Cung cấp tư duy lập trình căn bản qua ngôn ngữ Python, từ cú pháp, kiểu dữ liệu, vòng lặp đến khái niệm lập trình hướng đối tượng cơ bản.</p>',
'weeks' => [
    1 => ['Tuần 1: Giới thiệu Python', '<p>Cài đặt môi trường, viết chương trình Hello World.</p>'],
    2 => ['Tuần 2: Kiểu dữ liệu và biến', '<p>Số, chuỗi, boolean, định dạng chuỗi.</p>'],
    3 => ['Tuần 3: Cấu trúc điều khiển', '<p>Câu lệnh if-elif-else.</p>'],
    4 => ['Tuần 4: Vòng lặp', '<p>Vòng lặp for, while, break, continue.</p>'],
    5 => ['Tuần 5: Cấu trúc dữ liệu List và Tuple', '<p>Các phương thức xử lý mảng, tuple.</p>'],
    6 => ['Tuần 6: Cấu trúc dữ liệu Set và Dictionary', '<p>Lưu trữ và tra cứu cặp khóa-giá trị.</p>'],
    7 => ['Tuần 7: Hàm (Functions)', '<p>Định nghĩa hàm, tham số, giá trị trả về, lambda.</p>'],
    8 => ['Tuần 8: KIỂM TRA GIỮA KỲ', '<p>Thi lập trình trên máy.</p>'],
    9 => ['Tuần 9: Đọc ghi file', '<p>Mở, đọc, ghi file txt và csv.</p>'],
    10=> ['Tuần 10: Xử lý ngoại lệ', '<p>Khối try-except, bắt lỗi an toàn.</p>'],
    11=> ['Tuần 11: Lập trình Hướng đối tượng 1', '<p>Class, Object, constructor (__init__).</p>'],
    12=> ['Tuần 12: Lập trình Hướng đối tượng 2', '<p>Kế thừa, đa hình, phương thức dunder.</p>'],
    13=> ['Tuần 13: Thư viện chuẩn (Standard Library)', '<p>os, sys, math, random, datetime.</p>'],
    14=> ['Tuần 14: Ôn tập và Project nhỏ', '<p>Xây dựng game đoán số hoặc ứng dụng quản lý Todo bằng CLI.</p>'],
    15=> ['Tuần 15: THI CUỐI KỲ', '<p>Thi lập trình 120 phút.</p>'],
]
],

// ──────────────────────────────────────────────────────────────────────────────
[
'fullname'  => 'Phát triển Ứng dụng Web',
'shortname' => 'IT301',
'idnumber'  => 'HCMUTE-IT301',
'cat'       => 'Y2',
'start'     => mktime(0,0,0,1,6,2025),
'end'       => mktime(0,0,0,5,16,2025),
'summary'   => '<p><strong>Mã môn học:</strong> IT301 &nbsp;|&nbsp; <strong>Số tín chỉ:</strong> 4 TC</p><p>Học cách xây dựng giao diện và ứng dụng web căn bản bằng HTML5, CSS3, JavaScript, và có kiến thức về backend.</p>',
'weeks' => [
    1 => ['Tuần 1: HTML5 Cơ bản', '<p>Cấu trúc trang web, Semantic Tags, Forms.</p>'],
    2 => ['Tuần 2: CSS3 Căn bản', '<p>Box Model, Selectors, Typography.</p>'],
    3 => ['Tuần 3: CSS Layout', '<p>Flexbox và CSS Grid.</p>'],
    4 => ['Tuần 4: Responsive Design', '<p>Media queries, thiết kế Mobile-first.</p>'],
    5 => ['Tuần 5: JavaScript Căn bản', '<p>Cú pháp, biến, vòng lặp, hàm.</p>'],
    6 => ['Tuần 6: Thao tác DOM', '<p>Lấy phần tử, xử lý sự kiện (Events).</p>'],
    7 => ['Tuần 7: Bất đồng bộ trong JS', '<p>Promises, Async/Await, Fetch API.</p>'],
    8 => ['Tuần 8: KIỂM TRA GIỮA KỲ', '<p>Làm web tĩnh HTML/CSS/JS.</p>'],
    9 => ['Tuần 9: Giới thiệu Framework/Library', '<p>Khái niệm SPA, giới thiệu React.js cơ bản.</p>'],
    10=> ['Tuần 10: State và Props (React)', '<p>Quản lý trạng thái giao diện.</p>'],
    11=> ['Tuần 11: Backend Cơ bản (Node.js)', '<p>Khởi tạo server, Express.js cơ bản.</p>'],
    12=> ['Tuần 12: RESTful API', '<p>Tạo các endpoint GET, POST, PUT, DELETE.</p>'],
    13=> ['Tuần 13: Kết nối CSDL', '<p>Kết nối MongoDB/MySQL đơn giản.</p>'],
    14=> ['Tuần 14: Tích hợp Frontend-Backend', '<p>Call API từ trang web hiển thị dữ liệu.</p>'],
    15=> ['Tuần 15: THI CUỐI KỲ', '<p>Bảo vệ đồ án web cá nhân/nhóm.</p>'],
]
],

// ──────────────────────────────────────────────────────────────────────────────
[
'fullname'  => 'Kiến trúc Máy tính',
'shortname' => 'IT302',
'idnumber'  => 'HCMUTE-IT302',
'cat'       => 'Y2',
'start'     => mktime(0,0,0,1,6,2025),
'end'       => mktime(0,0,0,5,16,2025),
'summary'   => '<p><strong>Mã môn học:</strong> IT302 &nbsp;|&nbsp; <strong>Số tín chỉ:</strong> 3 TC</p><p>Khám phá cách hoạt động của phần cứng máy tính từ cổng logic, tập lệnh CPU, cho đến kiến trúc bộ nhớ và pipeline.</p>',
'weeks' => [
    1 => ['Tuần 1: Lịch sử máy tính và Hệ đếm', '<p>Hệ nhị phân, bát phân, thập lục phân.</p>'],
    2 => ['Tuần 2: Đại số Boole và Cổng Logic', '<p>Các cổng AND, OR, NOT, XOR. Rút gọn hàm Boole.</p>'],
    3 => ['Tuần 3: Mạch tổ hợp', '<p>Mạch cộng, mạch giải mã, đa kênh.</p>'],
    4 => ['Tuần 4: Mạch tuần tự', '<p>Flip-flops, thanh ghi, bộ đếm.</p>'],
    5 => ['Tuần 5: Tổ chức CPU', '<p>Khối ALU, Control Unit, Registers.</p>'],
    6 => ['Tuần 6: Tập lệnh (ISA)', '<p>Kiến trúc RISC vs CISC, các lệnh hợp ngữ cơ bản.</p>'],
    7 => ['Tuần 7: Pipeline', '<p>Tăng tốc CPU, các lỗi hazard (data, control).</p>'],
    8 => ['Tuần 8: KIỂM TRA GIỮA KỲ', '<p>Thi tự luận trên giấy.</p>'],
    9 => ['Tuần 9: Phân cấp bộ nhớ', '<p>Tháp bộ nhớ, nguyên lý locality.</p>'],
    10=> ['Tuần 10: Bộ nhớ Cache', '<p>Cache Mapping (Direct, Set Associative), thay thế trang.</p>'],
    11=> ['Tuần 11: Bộ nhớ ảo', '<p>Paging, Translation Lookaside Buffer (TLB).</p>'],
    12=> ['Tuần 12: Giao tiếp I/O', '<p>Polling, Ngắt (Interrupt), DMA.</p>'],
    13=> ['Tuần 13: Xử lý song song', '<p>Đa nhân (Multicore), siêu phân luồng (Hyper-threading).</p>'],
    14=> ['Tuần 14: Ôn tập Kiến trúc hệ thống', '<p>Tổng quan toàn hệ thống CPU - Bus - Memory.</p>'],
    15=> ['Tuần 15: THI CUỐI KỲ', '<p>Thi tự luận 90 phút.</p>'],
]
],

// ──────────────────────────────────────────────────────────────────────────────
[
'fullname'  => 'Cấu trúc Dữ liệu và Giải thuật',
'shortname' => 'IT303',
'idnumber'  => 'HCMUTE-IT303',
'cat'       => 'Y2',
'start'     => mktime(0,0,0,9,2,2025),
'end'       => mktime(0,0,0,12,27,2025),
'summary'   => '<p><strong>Mã môn học:</strong> IT303 &nbsp;|&nbsp; <strong>Số tín chỉ:</strong> 4 TC</p><p>Học về phân tích độ phức tạp thuật toán và cách cài đặt các CTDL cốt lõi (Linked List, Tree, Graph) cũng như các giải thuật sắp xếp, tìm kiếm.</p>',
'weeks' => [
    1 => ['Tuần 1: Phân tích thuật toán', '<p>Ký pháp Big-O, đo lường độ phức tạp thời gian/không gian.</p>'],
    2 => ['Tuần 2: Mảng và Tìm kiếm', '<p>Linear search, Binary search.</p>'],
    3 => ['Tuần 3: Danh sách liên kết', '<p>Singly linked list, Doubly linked list.</p>'],
    4 => ['Tuần 4: Ngăn xếp (Stack) và Hàng đợi (Queue)', '<p>LIFO, FIFO và ứng dụng.</p>'],
    5 => ['Tuần 5: Các thuật toán sắp xếp (O(N^2))', '<p>Bubble, Insertion, Selection sort.</p>'],
    6 => ['Tuần 6: Các thuật toán sắp xếp (O(N log N))', '<p>Merge sort, Quick sort.</p>'],
    7 => ['Tuần 7: Bảng băm (Hash Table)', '<p>Hash function, xử lý đụng độ (Collision).</p>'],
    8 => ['Tuần 8: KIỂM TRA GIỮA KỲ', '<p>Thi viết code và phân tích thuật toán.</p>'],
    9 => ['Tuần 9: Cây nhi phân (Binary Tree)', '<p>Duyệt cây (Pre, In, Post, Level-order).</p>'],
    10=> ['Tuần 10: Cây tìm kiếm nhị phân (BST)', '<p>Thêm, xóa, tìm kiếm trong BST.</p>'],
    11=> ['Tuần 11: Cây cân bằng (AVL / Red-Black Tree)', '<p>Khái niệm cân bằng cây.</p>'],
    12=> ['Tuần 12: Đồ thị (Graph)', '<p>Biểu diễn đồ thị, Duyệt BFS và DFS.</p>'],
    13=> ['Tuần 13: Thuật toán đường đi ngắn nhất', '<p>Thuật toán Dijkstra, Bellman-Ford.</p>'],
    14=> ['Tuần 14: Cây khung nhỏ nhất', '<p>Thuật toán Prim và Kruskal.</p>'],
    15=> ['Tuần 15: THI CUỐI KỲ', '<p>Thi lập trình thuật toán 120 phút.</p>'],
]
],

// ──────────────────────────────────────────────────────────────────────────────
[
'fullname'  => 'Mạng máy tính',
'shortname' => 'IT304',
'idnumber'  => 'HCMUTE-IT304',
'cat'       => 'Y2',
'start'     => mktime(0,0,0,1,5,2026),
'end'       => mktime(0,0,0,5,15,2026),
'summary'   => '<p><strong>Mã môn học:</strong> IT304 &nbsp;|&nbsp; <strong>Số tín chỉ:</strong> 3 TC</p><p>Hiểu về kiến trúc mạng, mô hình OSI và TCP/IP, cùng chi tiết hoạt động của các giao thức ở từng tầng.</p>',
'weeks' => [
    1 => ['Tuần 1: Tổng quan Mạng máy tính', '<p>Internet, thiết bị mạng, cấu trúc biên và lõi.</p>'],
    2 => ['Tuần 2: Mô hình OSI và TCP/IP', '<p>7 tầng OSI, sự đóng gói dữ liệu.</p>'],
    3 => ['Tuần 3: Tầng Ứng dụng (Application)', '<p>HTTP, FTP, SMTP, DNS.</p>'],
    4 => ['Tuần 4: Lập trình Socket', '<p>TCP Socket, UDP Socket cơ bản.</p>'],
    5 => ['Tuần 5: Tầng Giao vận (Transport) 1', '<p>Nguyên lý giao thức UDP, checksum.</p>'],
    6 => ['Tuần 6: Tầng Giao vận (Transport) 2', '<p>Giao thức TCP, 3-way handshake, điều khiển luồng.</p>'],
    7 => ['Tuần 7: Kiểm soát tắc nghẽn TCP', '<p>Cơ chế chống tắc nghẽn của TCP.</p>'],
    8 => ['Tuần 8: KIỂM TRA GIỮA KỲ', '<p>Thi trắc nghiệm và tự luận.</p>'],
    9 => ['Tuần 9: Tầng Mạng (Network) - Data Plane', '<p>Kiến trúc Router, chuyển mạch IP, IPv4, NAT.</p>'],
    10=> ['Tuần 10: Tầng Mạng (Network) - Control Plane', '<p>Thuật toán định tuyến: Link State (OSPF), Distance Vector (RIP, BGP).</p>'],
    11=> ['Tuần 11: Địa chỉ IP và Subnetting', '<p>Cách chia mạng con (Subnet mask, CIDR).</p>'],
    12=> ['Tuần 12: Tầng Liên kết dữ liệu (Data Link)', '<p>Lỗi bit, CSMA/CD, địa chỉ MAC, ARP.</p>'],
    13=> ['Tuần 13: Mạng LAN và Switch', '<p>Switch Ethernet, VLAN.</p>'],
    14=> ['Tuần 14: Mạng Không dây và Bảo mật mạng cơ bản', '<p>Wi-Fi (802.11), mã hóa cơ bản.</p>'],
    15=> ['Tuần 15: THI CUỐI KỲ', '<p>Thi tự luận 90 phút.</p>'],
]
],

// ──────────────────────────────────────────────────────────────────────────────
[
'fullname'  => 'Cơ sở dữ liệu',
'shortname' => 'IT305',
'idnumber'  => 'HCMUTE-IT305',
'cat'       => 'Y3',
'start'     => mktime(0,0,0,9,1,2026),
'end'       => mktime(0,0,0,12,26,2026),
'summary'   => '<p><strong>Mã môn học:</strong> IT305 &nbsp;|&nbsp; <strong>Số tín chỉ:</strong> 4 TC</p><p>Học về thiết kế cơ sở dữ liệu quan hệ, đại số quan hệ, SQL, và chuẩn hóa dữ liệu cũng như cách DBMS quản lý giao dịch.</p>',
'weeks' => [
    1 => ['Tuần 1: Giới thiệu chung về CSDL', '<p>Hệ quản trị CSDL (DBMS) và các mô hình dữ liệu.</p>'],
    2 => ['Tuần 2: Mô hình Thực thể - Mối quan hệ (ER)', '<p>Thực thể, thuộc tính, mối quan hệ 1-1, 1-N, N-N.</p>'],
    3 => ['Tuần 3: Chuyển đổi mô hình ER sang mô hình Quan hệ', '<p>Các quy tắc chuyển đổi tạo bảng.</p>'],
    4 => ['Tuần 4: Đại số quan hệ', '<p>Các phép toán Chọn, Chiếu, Kết nối (Join).</p>'],
    5 => ['Tuần 5: Ngôn ngữ SQL (DDL)', '<p>Tạo bảng, ràng buộc khóa chính, khóa ngoại.</p>'],
    6 => ['Tuần 6: Ngôn ngữ SQL (DML) 1', '<p>Select, Insert, Update, Delete cơ bản.</p>'],
    7 => ['Tuần 7: Ngôn ngữ SQL (DML) 2', '<p>Group By, Having, Subqueries.</p>'],
    8 => ['Tuần 8: KIỂM TRA GIỮA KỲ', '<p>Viết truy vấn SQL trên giấy.</p>'],
    9 => ['Tuần 9: Phụ thuộc hàm', '<p>Định nghĩa, các tính chất phụ thuộc hàm.</p>'],
    10=> ['Tuần 10: Chuẩn hóa dữ liệu (Normalization)', '<p>Dạng chuẩn 1NF, 2NF, 3NF, BCNF.</p>'],
    11=> ['Tuần 11: Transaction và Concurrency', '<p>Tính chất ACID, xử lý đồng thời, khóa (Locking).</p>'],
    12=> ['Tuần 12: Đánh chỉ mục (Indexing)', '<p>B-Tree Index, Hash Index, tối ưu câu truy vấn.</p>'],
    13=> ['Tuần 13: View, Trigger và Stored Procedure', '<p>Lập trình trên CSDL.</p>'],
    14=> ['Tuần 14: CSDL NoSQL (Giới thiệu)', '<p>Mô hình Document (MongoDB).</p>'],
    15=> ['Tuần 15: THI CUỐI KỲ', '<p>Thi tự luận 90 phút và Bảo vệ đồ án môn học.</p>'],
]
],

// ──────────────────────────────────────────────────────────────────────────────
[
'fullname'  => 'Công nghệ phần mềm',
'shortname' => 'IT401',
'idnumber'  => 'HCMUTE-IT401',
'cat'       => 'Y3',
'start'     => mktime(0,0,0,9,1,2026),
'end'       => mktime(0,0,0,12,26,2026),
'summary'   => '<p><strong>Mã môn học:</strong> IT401 &nbsp;|&nbsp; <strong>Số tín chỉ:</strong> 3 TC</p><p>Tổng hợp quy trình sản xuất phần mềm: thu thập yêu cầu, thiết kế kiến trúc, thiết kế mẫu (Design Pattern), và kiểm thử phần mềm.</p>',
'weeks' => [
    1 => ['Tuần 1: Tổng quan CNPM', '<p>Vòng đời phát triển phần mềm (SDLC).</p>'],
    2 => ['Tuần 2: Các mô hình phát triển phần mềm', '<p>Waterfall, Spiral, Agile (Scrum).</p>'],
    3 => ['Tuần 3: Thu thập và Quản lý yêu cầu', '<p>User stories, Use cases, Requirement Elicitation.</p>'],
    4 => ['Tuần 4: Mô hình hóa với UML', '<p>Class diagram, Sequence diagram, Activity diagram.</p>'],
    5 => ['Tuần 5: Kiến trúc phần mềm (Software Architecture)', '<p>Client-Server, MVC, Microservices.</p>'],
    6 => ['Tuần 6: Các nguyên lý thiết kế SOLID', '<p>SRP, OCP, LSP, ISP, DIP.</p>'],
    7 => ['Tuần 7: Design Patterns (Phần 1)', '<p>Creational Patterns: Singleton, Factory.</p>'],
    8 => ['Tuần 8: KIỂM TRA GIỮA KỲ', '<p>Thi lý thuyết UML và Kiến trúc.</p>'],
    9 => ['Tuần 9: Design Patterns (Phần 2)', '<p>Structural Patterns: Adapter, Facade, Decorator.</p>'],
    10=> ['Tuần 10: Design Patterns (Phần 3)', '<p>Behavioral Patterns: Observer, Strategy.</p>'],
    11=> ['Tuần 11: Kiểm thử phần mềm (Testing)', '<p>White-box, Black-box testing, Unit Test, Integration Test.</p>'],
    12=> ['Tuần 12: Quản lý mã nguồn và CI/CD', '<p>Sử dụng Git chuyên sâu, khái niệm Continuous Integration.</p>'],
    13=> ['Tuần 13: Đảm bảo chất lượng (QA & QC)', '<p>Review code, metrics, refactoring.</p>'],
    14=> ['Tuần 14: Quản lý dự án phần mềm', '<p>Lập lịch, quản lý rủi ro, ước lượng effort (Story points).</p>'],
    15=> ['Tuần 15: THI CUỐI KỲ', '<p>Bảo vệ Project nhóm (Một phần mềm hoàn chỉnh áp dụng quy trình Scrum).</p>'],
]
]
];
// end $COURSES

// ─── CREATE COURSES ───────────────────────────────────────────────────────────
$PDF_MAP = [
    'MATH101' => [
        ['name' => 'Giáo trình Linear Algebra - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/279']
    ],
    'MATH102' => [
        ['name' => 'Giáo trình Calculus Volume 1 - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/1972'],
        ['name' => 'Giáo trình Calculus Volume 2 - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/1964'],
        ['name' => 'Giáo trình Calculus Volume 3 - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/2012']
    ],
    'MATH103' => [
        ['name' => 'Giáo trình Introductory Statistics - 2e - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/1947']
    ],
    'IT201' => [
        ['name' => 'Giáo trình Introduction to Python Programming - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/1963'],
        ['name' => 'Giáo trình Introduction to Computer Science - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/1950']
    ],
    'IT301' => [
        ['name' => 'Giáo trình Web Development and Programming - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/744'],
        ['name' => 'Giáo trình React Programming (Modern Web Applications) - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/256']
    ],
    'IT302' => [
        ['name' => 'Giáo trình Electrical and Computer Engineering - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/326']
    ],
    'IT303' => [
        ['name' => 'Giáo trình An Open Guide to Data Structures and Algorithms - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/1730']
    ],
    'IT304' => [
        ['name' => 'Giáo trình An Introduction to Computer Networks - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/1380']
    ],
    'IT305' => [
        ['name' => 'Giáo trình Database Design - 2nd Edition - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/1731']
    ],
    'IT401' => [
        ['name' => 'Giáo trình Foundations of Software Engineering - Tài liệu OER (DSpace)', 'url' => 'http://localhost:4000/handle/123456789/33']
    ]
];

$CREATED = [];
foreach ($COURSES as $cdef) {
    // Create course object
    $co = new stdClass();
    $co->fullname      = $cdef['fullname'];
    $co->shortname     = $cdef['shortname'];
    $co->idnumber      = $cdef['idnumber'];
    $co->category      = $CAT[$cdef['cat']];
    $co->summary       = $cdef['summary'];
    $co->summaryformat = FORMAT_HTML;
    $co->numsections   = 15;
    $co->visible       = 1;
    $co->startdate     = $cdef['start'];
    $co->enddate       = $cdef['end'];
    $co->lang          = 'en';
    $co->format        = 'weeks';
    $co->showgrades    = 1;
    $co->newsitems     = 5;
    $co->enablecompletion = 1;

    $new_course = create_course($co);
    $cid = $new_course->id;
    $CREATED[] = $cid;
    echo "  + [{$cdef['shortname']}] {$cdef['fullname']} (id={$cid})\n";

    // ─── SECTION 0: Forum + Page syllabus ────────────────────────────────────
    h_section($DB, $cid, 0, '', '');

    h_forum($DB, $MOD, $cid, 0,
        'Hoi dap & Thao luan – ' . $cdef['shortname'],
        '<p>Dien dan hoi dap va thao luan bai giang. Sinh vien dat cau hoi, trao doi voi ban hoc va giang vien. Moi cau hoi deu duoc tra loi trong vong 24h gio trong ngay hoc.</p>'
    );

    h_page($DB, $MOD, $cid, 0,
        'De cuong mon hoc (Syllabus) – ' . $cdef['shortname'],
        '<h3>De cuong Mon hoc: ' . $cdef['fullname'] . '</h3>'
        . $cdef['summary']
        . '<hr><h4>Lich trinh giang day</h4><table border="1" cellpadding="6" style="border-collapse:collapse;width:100%">'
        . '<tr style="background:#1a73e8;color:#fff"><th>Tuan</th><th>Noi dung</th></tr>'
        . implode('', array_map(
            fn($wk, $data) => "<tr><td>Tuan {$wk}</td><td>{$data[0]}</td></tr>",
            array_keys($cdef['weeks']),
            array_values($cdef['weeks'])
          ))
        . '</table>'
        . '<hr><h4>Danh gia mon hoc</h4>'
        . '<ul><li><strong>Qua trinh (40%):</strong> Chuyen can (10%), Bai tap ve nha (15%), Kiem tra giua ky (15%)</li>'
        . '<li><strong>Cuoi ky (60%):</strong> Thi cuoi ky tu luan / thuc hanh</li></ul>'
    );

    // Add PDF textbook list as label in section 0
    if (isset($PDF_MAP[$cdef['shortname']])) {
        $html = '<div class="alert alert-info" style="border: 1px solid #bce8f1; border-radius: 4px; padding: 15px; margin-bottom: 20px; background-color: #d9edf7; color: #31708f;">';
        $html .= '<h4 style="margin-top: 0; color: #245269; font-size: 1.15rem;"><b>📚 Giáo trình & Tài liệu học tập chính (PDF OER)</b></h4>';
        $html .= '<p style="margin-bottom: 10px;">Sinh viên click vào các liên kết dưới đây để tải về hoặc đọc trực tiếp tài liệu học tập chính thức của học phần:</p>';
        $html .= '<ul style="margin-bottom: 0; padding-left: 20px;">';
        foreach ($PDF_MAP[$cdef['shortname']] as $pdf) {
            $html .= '<li style="margin-bottom: 8px;"><a href="' . htmlspecialchars($pdf['url']) . '" target="_blank" style="font-weight: bold; color: #1b809e; text-decoration: underline;">🔗 ' . htmlspecialchars($pdf['name']) . '</a></li>';
        }
        $html .= '</ul>';
        $html .= '</div>';
        h_label($DB, $MOD, $cid, 0, $html);
    }

    // ─── SECTIONS 1-15: Name + Summary ───────────────────────────────────────
    foreach ($cdef['weeks'] as $wno => [$wname, $wsummary]) {
        h_section($DB, $cid, $wno, $wname, $wsummary);
    }

    rebuild_course_cache($cid);
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 4 — STUDENT ACCOUNTS
// ═══════════════════════════════════════════════════════════════════════════════
echo "\n[4/5] Tao tai khoan sinh vien...\n";

$STUDENTS = [
    [
        'username'   => '22133024',
        'password'   => '22133024',
        'firstname'  => 'Van Huy',
        'lastname'   => 'Dang',
        'email'      => '22133024@student.hcmute.edu.vn',
        'idnumber'   => '22133024',
        'department' => 'Khoa Công nghệ Thông tin',
        'institution'=> 'Truong Dai hoc Su pham Ky thuat TP.HCM (HCMUTE)',
        'city'       => 'TP. Ho Chi Minh',
        'country'    => 'VN',
        'description'=> 'Sinh viên CNTT K22 (7480201). MSSV: 22133024. Định hướng: Kỹ thuật phần mềm & DevOps.',
        'phone1'     => '0901234567',
    ],
    [
        'username'   => '22133061',
        'password'   => '22133061',
        'firstname'  => 'Nhu Hoang Tien',
        'lastname'   => 'Nguyen',
        'email'      => '22133061@student.hcmute.edu.vn',
        'idnumber'   => '22133061',
        'department' => 'Khoa Công nghệ Thông tin',
        'institution'=> 'Truong Dai hoc Su pham Ky thuat TP.HCM (HCMUTE)',
        'city'       => 'Binh Duong',
        'country'    => 'VN',
        'description'=> 'Sinh viên CNTT K22 (7480201). MSSV: 22133061. Định hướng: AI/ML và Khoa học dữ liệu.',
        'phone1'     => '0987654321',
    ],
];

$STUDENT_IDS = [];
foreach ($STUDENTS as $sd) {
    if ($ex = $DB->get_record('user', ['username' => $sd['username']])) {
        $DB->delete_records('user', ['id' => $ex->id]);
    }
    $u = new stdClass();
    $u->auth            = 'manual';
    $u->confirmed       = 1;
    $u->policyagreed    = 1;
    $u->deleted         = 0;
    $u->suspended       = 0;
    $u->mnethostid      = $CFG->mnet_localhost_id;
    $u->username        = $sd['username'];
    $u->password        = hash_internal_user_password($sd['password']);
    $u->firstname       = $sd['firstname'];
    $u->lastname        = $sd['lastname'];
    $u->email           = $sd['email'];
    $u->idnumber        = $sd['idnumber'];
    $u->department      = $sd['department'];
    $u->institution     = $sd['institution'];
    $u->city            = $sd['city'];
    $u->country         = $sd['country'];
    $u->description     = $sd['description'];
    $u->descriptionformat = FORMAT_HTML;
    $u->phone1          = $sd['phone1'];
    $u->lang            = 'en';
    $u->calendartype    = 'gregorian';
    $u->timezone        = 'Asia/Ho_Chi_Minh';
    $u->firstaccess     = 0;
    $u->lastaccess      = 0;
    $u->lastlogin       = 0;
    $u->currentlogin    = 0;
    $u->timecreated     = time();
    $u->timemodified    = time();
    $u->trackforums     = 0;
    $u->mailformat      = 1;
    $uid = $DB->insert_record('user', $u);
    $STUDENT_IDS[] = $uid;
    echo "  + {$sd['lastname']} {$sd['firstname']} | u={$sd['username']} / p={$sd['password']} (id={$uid})\n";
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 5 — ENROLL STUDENTS
// ═══════════════════════════════════════════════════════════════════════════════
echo "\n[5/5] Ghi danh sinh vien...\n";
$student_role = $DB->get_record('role', ['shortname' => 'student']);
$enrol_plugin = enrol_get_plugin('manual');

foreach ($CREATED as $cid) {
    $cobj = $DB->get_record('course', ['id' => $cid]);
    $ei   = $DB->get_record('enrol', ['courseid' => $cid, 'enrol' => 'manual']);
    if (!$ei) {
        $eid = $enrol_plugin->add_instance($cobj);
        $ei  = $DB->get_record('enrol', ['id' => $eid]);
    }
    foreach ($STUDENT_IDS as $uid) {
        $enrol_plugin->enrol_user($ei, $uid, $student_role->id,
            mktime(0,0,0,9,2,2024), mktime(0,0,0,5,31,2027));
    }
    echo "  + Ghi danh: {$cobj->fullname}\n";
}

// ═══════════════════════════════════════════════════════════════════════════════
// DONE
// ═══════════════════════════════════════════════════════════════════════════════
echo "\n====================================================\n";
echo "HOAN THANH! Du lieu demo HCMUTE-CNTT\n";
echo "====================================================\n";
echo "Truong: Dai hoc Su pham Ky thuat TP.HCM (HCMUTE)\n";
echo "Khoa:   Cong nghe Thong tin – 01 Vo Van Ngan, Thu Duc\n";
echo "----------------------------------------------------\n";
echo "Khoa hoc: " . count($CREATED) . "\n";
echo "Sinh vien: " . count($STUDENT_IDS) . "\n";
echo "----------------------------------------------------\n";
echo "Tai khoan:\n";
echo "  Admin          : admin / admin123\n";
foreach ($STUDENTS as $sd) {
    echo "  {$sd['lastname']} {$sd['firstname']}: {$sd['username']} / {$sd['password']}\n";
}
echo "====================================================\n";
