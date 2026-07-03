<?php
defined('MOODLE_INTERNAL') || die();


function local_oerchatbot_get_user_role(): string {
    global $PAGE;
    $context = $PAGE->context ?? context_system::instance();

    if (has_capability('moodle/course:update', $context)) {
        return 'teacher';
    }
    if (has_capability('moodle/course:view', $context)) {
        return 'student';
    }
    return 'guest';
}
