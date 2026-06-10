"""Shared pipeline configuration.

Keep low-level parsing deterministic. Business keywords and runtime output
choices live here so future agents can find them without spelunking through
entrypoint code.
"""

from __future__ import annotations

OUTPUT_SHAPES = ("both", "db", "wide")
DEFAULT_OUTPUT_SHAPE = "both"
DEFAULT_OUTPUT_ROOT = "outputs"
DEFAULT_REVIEW_SUMMARIES_DIR = "review_summaries"
THREADS_URL_TEMPLATE = "https://www.threads.com/@{username}/post/{code}"

# Source-of-truth location for private raw inputs. The CLI intentionally reads
# local files; download working copies from this Drive folder into ignored
# local paths such as raw/ before running extraction.
GOOGLE_DRIVE_THREADS_FOLDER_NAME = ""
GOOGLE_DRIVE_THREADS_FOLDER_URL = ""
GOOGLE_DRIVE_RAW_FOLDER_NAME = ""
GOOGLE_DRIVE_RAW_FOLDER_URL = ""
GOOGLE_DRIVE_RAW_RCLONE_REMOTE = ""
GOOGLE_DRIVE_RUNS_RCLONE_REMOTE = ""
GOOGLE_DRIVE_MEDIA_CACHE_RCLONE_REMOTE = ""

AD_DISCLOSURE_KEYWORDS = [
    "광고",
    "파트너스",
    "쿠팡 파트너스",
    "수수료",
    "일정액",
    "제공받",
    "제공받습니다",
    "협찬",
    "AD",
    "#ad",
]

CONVERSION_KEYWORDS = [
    "추천",
    "쟁여",
    "구매",
    "구입",
    "사면",
    "샀",
    "쓰는",
    "먹는",
    "입는",
    "제품",
    "정보",
    "링크",
    "할인",
    "가격",
    "특가",
    "핫딜",
    "재구매",
    "필수",
    "템",
    "내돈내산",
    "후기",
    "쿠팡",
    "파트너스",
]

CATEGORY_KEYWORDS = {
    "food": [
        "먹",
        "맛",
        "안주",
        "맥주",
        "닭",
        "꼬치",
        "커피",
        "과자",
        "빵",
        "키위",
        "옥수수",
        "과일",
        "음식",
        "식품",
        "간식",
        "냉동",
        "밀키트",
        "고기",
        "라면",
        "음료",
        "도시락",
    ],
    "fashion": [
        "옷",
        "나시",
        "티",
        "후드",
        "후드티",
        "바지",
        "원피스",
        "셔츠",
        "코디",
        "패션",
        "신발",
        "운동화",
        "가방",
        "키링",
        "시계",
        "착용",
        "입었",
        "스타일",
    ],
    "beauty": [
        "화장",
        "피부",
        "선크림",
        "쿠션",
        "립",
        "틴트",
        "크림",
        "앰플",
        "샴푸",
        "트리트먼트",
        "향수",
        "뷰티",
        "메이크업",
    ],
    "celebrity": [
        "카리나",
        "고윤정",
        "김선호",
        "손연재",
        "연예인",
        "아이돌",
        "배우",
        "셀럽",
        "공항",
        "착장",
    ],
    "home_living": [
        "집",
        "주방",
        "청소",
        "수납",
        "침대",
        "이불",
        "가구",
        "거실",
        "생활",
        "살림",
        "욕실",
        "인테리어",
        "용품",
    ],
    "baby_kids": [
        "아기",
        "육아",
        "유아",
        "아이",
        "기저귀",
        "장난감",
        "분유",
        "키즈",
        "어린이",
    ],
    "pet": [
        "강아지",
        "고양이",
        "반려",
        "댕댕",
        "냥이",
        "펫",
        "사료",
        "간식",
    ],
    "electronics": [
        "폰",
        "휴대폰",
        "충전",
        "케이블",
        "노트북",
        "이어폰",
        "전자",
        "기기",
        "가전",
        "마우스",
        "키보드",
    ],
    "health": [
        "건강",
        "운동",
        "헬스",
        "다이어트",
        "영양",
        "비타민",
        "유산균",
        "마사지",
        "스트레칭",
    ],
    "sports": [
        "러닝",
        "요가",
        "필라테스",
        "골프",
        "테니스",
        "등산",
        "운동복",
        "운동화",
    ],
    "travel": [
        "여행",
        "호텔",
        "공항",
        "캐리어",
        "숙소",
        "캠핑",
        "차박",
        "나들이",
    ],
}
