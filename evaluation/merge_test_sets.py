"""
Gộp test_set.json (EN) + test_set_vi.json (VI) → test_set_combined.json (200 câu).

Usage:
  python3 merge_test_sets.py
  python3 merge_test_sets.py --en test_set.json --vi test_set_vi.json --out test_set_combined.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent


def main():
    parser = argparse.ArgumentParser(description="Gộp 2 test set EN + VI thành 1 file")
    parser.add_argument("--en",  default=str(ROOT / "test_set.json"),
                        help="Đường dẫn test set tiếng Anh (mặc định: test_set.json)")
    parser.add_argument("--vi",  default=str(ROOT / "test_set_vi.json"),
                        help="Đường dẫn test set tiếng Việt (mặc định: test_set_vi.json)")
    parser.add_argument("--out", default=str(ROOT / "test_set_combined.json"),
                        help="File output (mặc định: test_set_combined.json)")
    args = parser.parse_args()

    en_path  = Path(args.en)
    vi_path  = Path(args.vi)
    out_path = Path(args.out)

    if not en_path.exists():
        print(f"[ERROR] Không tìm thấy: {en_path}", file=sys.stderr)
        sys.exit(1)
    if not vi_path.exists():
        print(f"[ERROR] Không tìm thấy: {vi_path}", file=sys.stderr)
        sys.exit(1)

    en_questions = json.loads(en_path.read_text(encoding="utf-8"))
    vi_questions = json.loads(vi_path.read_text(encoding="utf-8"))

    # Đánh số lại ID tuần tự: Q001–Q100 (EN), Q101–Q200 (VI)
    for idx, q in enumerate(en_questions, 1):
        q["id"] = f"Q{idx:03d}"

    for idx, q in enumerate(vi_questions, 101):
        q["id"] = f"Q{idx:03d}"

    combined = en_questions + vi_questions

    out_path.write_text(
        json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Thống kê
    by_lang  = {"en": 0, "vi": 0}
    by_type  = {}
    for q in combined:
        by_lang[q.get("language", "?")] = by_lang.get(q.get("language", "?"), 0) + 1
        t = q.get("question_type", "?")
        by_type[t] = by_type.get(t, 0) + 1

    print(f"✓ Đã gộp {len(combined)} câu hỏi → {out_path}")
    print(f"  Tiếng Anh : {by_lang.get('en', 0)} câu")
    print(f"  Tiếng Việt: {by_lang.get('vi', 0)} câu")
    print("  Phân loại :")
    for t, n in sorted(by_type.items()):
        print(f"    {t:<14}: {n}")


if __name__ == "__main__":
    main()