<?php
namespace local_oerchatbot\hook;

use core\hook\output\before_footer_html_generation;

class before_footer {
    public static function callback(before_footer_html_generation $hook): void {
        global $PAGE, $COURSE, $USER, $DB;

        if (!get_config('local_oerchatbot', 'enabled')) {
            return;
        }
        if (CLI_SCRIPT) {
            return;
        }

        $imageurl = (new \moodle_url('/local/oerchatbot/pix/hcmute.png'))->out(false);
        $js = "(function() {
            // Guest Hero Image
            if (!document.getElementById('oerchatbot-guest-hero-style')) {
                var style = document.createElement('style');
                style.id = 'oerchatbot-guest-hero-style';
                style.textContent =
                    '#frontpage-available-course-list { display: none !important; }' +
                    '#region-main h1.h2.mb-0 { display: none !important; }' +
                    '.oerchatbot-guest-hero-wrap { width: 100%; display: flex; justify-content: center; align-items: center; padding: 12px 0 20px; }' +
                    '.oerchatbot-guest-hero { max-width: min(100%, 980px); width: 100%; border-radius: 10px; }';
                document.head.appendChild(style);
            }

            // HCMUTE Branding (Logo & Footer)
            var hcmuteLogo = 'https://upload.wikimedia.org/wikipedia/commons/e/e9/Logo_Tr%C6%B0%E1%BB%9Dng_%C4%90%E1%BA%A1i_H%E1%BB%8Dc_C%C3%B4ng_ngh%E1%BB%87_K%E1%BB%B9_Thu%E1%BA%ADt_TP_H%E1%BB%93_Ch%C3%AD_Minh.png';
            var hcmuteFooter = '<div style=\"text-align:center; padding: 20px; background-color: #003366; color: #fff;\"><h5 style=\"color: #fff; font-weight: bold; margin-bottom: 10px;\">TRƯỜNG ĐẠI HỌC CÔNG NGHỆ KỸ THUẬT TP. HỒ CHÍ MINH</h5><p style=\"margin-bottom: 5px;\">Số 1 Võ Văn Ngân, Phường Linh Chiểu, Thành phố Thủ Đức, Thành phố Hồ Chí Minh.</p><p style=\"margin-bottom: 0;\">Điện thoại: (+84 - 028) 38968641 - (+84 -028) 38961333 | Email: ptchc@hcmute.edu.vn</p></div>';

            document.addEventListener('DOMContentLoaded', function() {
                // Change Logo
                var logos = document.querySelectorAll('.navbar-brand.logo img, .navbar-brand img');
                logos.forEach(function(img) {
                    img.src = hcmuteLogo;
                    img.style.maxHeight = '50px';
                });

                // Replace Footer
                var footer = document.querySelector('#page-footer');
                if (footer) {
                    footer.innerHTML = hcmuteFooter;
                }
            });
        })();";
        $PAGE->requires->js_init_code($js);

        $isguest = isguestuser() || !isloggedin();
        $sitepath = (string)$PAGE->url->get_path();
        $issiteindex = in_array($sitepath, ['/', '/index.php'], true);

        if ($isguest && $issiteindex) {
            $js_guest = "(function() {
                var target = document.getElementById('region-main');
                if (!target || document.getElementById('oerchatbot-guest-hero')) { return; }
                var wrap = document.createElement('div');
                wrap.id = 'oerchatbot-guest-hero';
                wrap.className = 'oerchatbot-guest-hero-wrap';
                var img = document.createElement('img');
                img.className = 'oerchatbot-guest-hero';
                img.src = " . json_encode($imageurl) . ";
                img.alt = 'HCMUTE';
                wrap.appendChild(img);
                target.insertBefore(wrap, target.firstChild);
            })();";
            $PAGE->requires->js_init_code($js_guest);
        }

        if ($isguest) {
            return;
        }

        $apiurl = (string)(get_config('local_oerchatbot', 'apiurl') ?: 'http://127.0.0.1:18088/api/ask');
        $apikey = (string)(get_config('local_oerchatbot', 'apikey') ?: '');
        $position = (string)(get_config('local_oerchatbot', 'position') ?: 'right');
        $title = (string)(get_config('local_oerchatbot', 'title') ?: 'OER Chatbot');
        $scriptpath = (string)$PAGE->url->get_path();
        $cmid = 0;
        if (strpos($scriptpath, '/mod/') === 0 && substr($scriptpath, -9) === '/view.php') {
            $cmid = optional_param('id', 0, PARAM_INT);
        }
        $sectionnum = optional_param('section', 0, PARAM_INT);
        $activityname = format_string($PAGE->activityname ?? '');
        $sectionname = '';
        $activityid = null;
        $iscoursecontext = (
            isset($COURSE->id)
            && (int)$COURSE->id > 1
            && (
                strpos($scriptpath, '/course/') === 0
                || strpos($scriptpath, '/mod/') === 0
                || strpos($scriptpath, '/grade/') === 0
            )
        );

        if ($cmid > 0 && $COURSE->id > 0) {
            $cm = get_coursemodule_from_id('', $cmid, $COURSE->id, false, IGNORE_MISSING);
            if ($cm) {
                $activityid = (int)$cm->instance;
                if (isset($cm->section) && (int)$cm->section > 0) {
                    $sectionrecord = $DB->get_record('course_sections', ['id' => (int)$cm->section], 'id,section,name', IGNORE_MISSING);
                    if ($sectionrecord) {
                        $sectionnum = (int)$sectionrecord->section;
                        $sectionname = trim(format_string($sectionrecord->name ?? ''));
                    }
                }
            }
        }

        if ($sectionname === '' && $sectionnum > 0) {
            $sectionname = 'Topic ' . $sectionnum;
        }

        $context = [
            'apiUrl' => $apiurl,
            'apiKey' => $apikey,
            'position' => ($position === 'left') ? 'left' : 'right',
            'title' => $title,
            'hasCourseContext' => $iscoursecontext,
            'courseId' => ($iscoursecontext && isset($COURSE->id) && (int)$COURSE->id > 1) ? (int)$COURSE->id : null,
            'courseName' => ($iscoursecontext && isset($COURSE->fullname)) ? (string)$COURSE->fullname : null,
            'userRole' => self::get_user_role(),
            'pageUrl' => (string)$PAGE->url,
            'cmId' => $cmid > 0 ? $cmid : null,
            'sectionNum' => $sectionnum > 0 ? $sectionnum : null,
            'sectionName' => $sectionname !== '' ? $sectionname : null,
            'activityId' => $activityid,
            'activityName' => $activityname !== '' ? $activityname : null,
            'userId' => isset($USER->id) ? (int)$USER->id : null,
        ];

        $PAGE->requires->js_call_amd('local_oerchatbot/widget', 'init', [$context]);
    }

    private static function get_user_role(): string {
        global $PAGE;
        $context = $PAGE->context ?? \context_system::instance();

        if (has_capability('moodle/course:update', $context)) {
            return 'teacher';
        }
        if (has_capability('moodle/course:view', $context)) {
            return 'student';
        }
        return 'guest';
    }
}
