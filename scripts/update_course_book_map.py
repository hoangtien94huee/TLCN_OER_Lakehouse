#!/usr/bin/env python3
import json, os

MAP_PATH = os.path.join(os.path.dirname(__file__), '..', 'airflow', 'src', 'course_book_map.json')

with open(MAP_PATH, 'r', encoding='utf-8') as f:
    book_map = json.load(f)

HCMUTE_ENTRIES = {
    "Đại số tuyến tính": {
        "asset_uids": ["08c79d5c3d5df1d072e084ad931523d282f87be5057c31b2840ddc6f19ab3507"],
        "books": [{"asset_uid": "08c79d5c3d5df1d072e084ad931523d282f87be5057c31b2840ddc6f19ab3507", "title": "Linear Algebra", "source_url": "https://open.umn.edu/opentextbooks/textbooks/linear-algebra-2016"}]
    },
    "Giải tích": {
        "asset_uids": [
            "d1c81cf26edba1933ba50c2680ab78f24b0af40b49f9976378eeb063b7df08b4",
            "a73d32efee8018e69eeef1fcc4f03939634e94b2abeb8e03e3cc43c49a37a7cd",
            "821360183184d5f4705bd0c1ccbfbce7171f11a8436ba33b6ee979a6d89551a3"
        ],
        "books": [
            {"asset_uid": "d1c81cf26edba1933ba50c2680ab78f24b0af40b49f9976378eeb063b7df08b4", "title": "Free Calculus Volume 1 Textbook Available for Download - OpenStax", "source_url": "https://openstax.org/details/books/calculus-volume-1"},
            {"asset_uid": "a73d32efee8018e69eeef1fcc4f03939634e94b2abeb8e03e3cc43c49a37a7cd", "title": "Free Calculus Volume 2 Textbook Available for Download - OpenStax", "source_url": "https://openstax.org/details/books/calculus-volume-2"},
            {"asset_uid": "821360183184d5f4705bd0c1ccbfbce7171f11a8436ba33b6ee979a6d89551a3", "title": "Free Calculus Volume 3 Textbook Available for Download - OpenStax", "source_url": "https://openstax.org/details/books/calculus-volume-3"}
        ]
    },
    "Xác suất thống kê": {
        "asset_uids": ["d30749fca7fe32a88b7ec5f26b3456cfd709d201c3c48be2a83729249f598e99"],
        "books": [{"asset_uid": "d30749fca7fe32a88b7ec5f26b3456cfd709d201c3c48be2a83729249f598e99", "title": "Free Introductory Statistics 2e Textbook for Download - OpenStax", "source_url": "https://openstax.org/details/books/introductory-statistics-2e"}]
    },
    "Nhập môn Lập trình Python": {
        "asset_uids": ["b9307858c8b74a9552de02ff0be7e28c8642104d9ba6f0d5f96b838ccb23ae32", "325b71626edcd2dbb4023b36994f37360c1beb23954db3a945cfef31dc7c4173"],
        "books": [
            {"asset_uid": "b9307858c8b74a9552de02ff0be7e28c8642104d9ba6f0d5f96b838ccb23ae32", "title": "Introduction to Python Programming - OpenStax", "source_url": "https://openstax.org/details/books/introduction-python-programming"},
            {"asset_uid": "325b71626edcd2dbb4023b36994f37360c1beb23954db3a945cfef31dc7c4173", "title": "Introduction to Computer Science - OpenStax", "source_url": "https://openstax.org/details/books/introduction-computer-science"}
        ]
    },
    "Phát triển Ứng dụng Web": {
        "asset_uids": ["9f0237976e191cd1cd6963e63969a53164a66a7b219decc5510667da6728cce7", "47a3dc1e28586828555eab2bd723db5a88c751296cf15df3650cdad163014a04"],
        "books": [
            {"asset_uid": "9f0237976e191cd1cd6963e63969a53164a66a7b219decc5510667da6728cce7", "title": "Information Technology Essentials", "source_url": "https://open.umn.edu/opentextbooks/textbooks/information-technology-essentials"},
            {"asset_uid": "47a3dc1e28586828555eab2bd723db5a88c751296cf15df3650cdad163014a04", "title": "Web Programming", "source_url": "https://open.umn.edu/opentextbooks/textbooks/web-programming"}
        ]
    },
    "Cấu trúc Dữ liệu và Giải thuật": {
        "asset_uids": ["5bdeea246969f8f55317123444a085aff9a1ff4bd7801fa386d38a4cba85b499", "895be2e253175e5ce167a11c7286e9d06ef6ed5e2ec51b5278ff6e83f8f267cf"],
        "books": [
            {"asset_uid": "5bdeea246969f8f55317123444a085aff9a1ff4bd7801fa386d38a4cba85b499", "title": "Introduction to Algorithms", "source_url": "https://ocw.mit.edu/courses/6-006-introduction-to-algorithms-fall-2011/"},
            {"asset_uid": "895be2e253175e5ce167a11c7286e9d06ef6ed5e2ec51b5278ff6e83f8f267cf", "title": "Introduction to Algorithms", "source_url": "https://ocw.mit.edu/courses/6-006-introduction-to-algorithms-spring-2020/"}
        ]
    },
    "Mạng Máy tính": {
        "asset_uids": ["d3661450c5503d85cc14ad6d025525822626d261399b0fc43a8265ca19ff2fa0", "16185cb54844f634eccad8c81afd156ac3529aeb38f8554463989380023d291d"],
        "books": [
            {"asset_uid": "d3661450c5503d85cc14ad6d025525822626d261399b0fc43a8265ca19ff2fa0", "title": "Data Communication Networks", "source_url": "https://ocw.mit.edu/courses/6-263j-data-communication-networks-fall-2002/"},
            {"asset_uid": "16185cb54844f634eccad8c81afd156ac3529aeb38f8554463989380023d291d", "title": "Networks", "source_url": "https://ocw.mit.edu/courses/14-15j-networks-spring-2018/"}
        ]
    },
    "Cơ sở dữ liệu": {
        "asset_uids": ["d07fac8a9a189849d2a5557dfad88056ce0445bcfa9f12bb55e80f9db291028f"],
        "books": [{"asset_uid": "d07fac8a9a189849d2a5557dfad88056ce0445bcfa9f12bb55e80f9db291028f", "title": "Database Systems", "source_url": "https://ocw.mit.edu/courses/6-830-database-systems-fall-2010/"}]
    },
    "Kiến trúc Máy tính": {
        "asset_uids": ["812d3c5a17b140770f03a7b04637d3644d55b8d1f34eb01e90dde56a293675e7"],
        "books": [{"asset_uid": "812d3c5a17b140770f03a7b04637d3644d55b8d1f34eb01e90dde56a293675e7", "title": "Advanced System Architecture", "source_url": "https://ocw.mit.edu/courses/6-823-computer-system-architecture-fall-2005/"}]
    },
    "Công nghệ phần mềm": {
        "asset_uids": ["0a95e0c52a55adcc48dd804566c303f0b0933c0bd912da05b76da9156cc01007"],
        "books": [{"asset_uid": "0a95e0c52a55adcc48dd804566c303f0b0933c0bd912da05b76da9156cc01007", "title": "Software Engineering Concepts", "source_url": "https://ocw.mit.edu/courses/16-355j-software-engineering-concepts-fall-2005/"}]
    }
}

for cname, cinfo in HCMUTE_ENTRIES.items():
    book_map[cname] = cinfo

# Find stale courses that were removed
to_remove = []
for k in book_map.keys():
    if k not in HCMUTE_ENTRIES and k in ["Trí tuệ Nhân tạo", "Toán rời rạc ứng dụng CNTT", "Mạng Máy tính", "Hệ quản trị Cơ sở Dữ liệu", "Hệ điều hành", "Lập trình Hướng đối tượng (Java)"]:
        to_remove.append(k)
for k in to_remove:
    del book_map[k]

with open(MAP_PATH, 'w', encoding='utf-8') as f:
    json.dump(book_map, f, ensure_ascii=False, indent=2)

print("Updated course_book_map.json successfully with 10 courses!")
