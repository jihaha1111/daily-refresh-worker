#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Threads Coupang body -> first link-comment extractor.

Input:
    Threads/Instagram-like export JSON list.

Output:
    - matched body -> first Coupang link comment pairs
    - excluded exceptions with reasons
    - summary
    - field dictionary

Core matching rule:
    body.user.id == link.user.id == link.reply_to_author.id

The matcher uses a global 1:1 greedy selection so a single Coupang link
comment cannot be attached to multiple body posts.
"""

import argparse
import csv
import datetime as dt
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from threads_coupang_pipeline.media import (  # noqa: E402
    get_best_image_candidate,
    get_best_video_url,
)
from threads_coupang_pipeline.config import (  # noqa: E402
    AD_DISCLOSURE_KEYWORDS,
    CATEGORY_KEYWORDS,
    CONVERSION_KEYWORDS,
    OUTPUT_SHAPES,
)
from threads_coupang_pipeline.text import (  # noqa: E402
    extract_coupang_urls,
    extract_urls,
    normalize_url_tail,
    split_text_lines,
)
from threads_coupang_pipeline.view_counts import (  # noqa: E402
    apply_view_counts_to_items,
)

LINKAGE_FIELD_KEYS = [
    "idx",
    "pk",
    "id",
    "code",
    "threads_url",
    "taken_at",
    "user_id",
    "username",
    "text",
    "text_preview",
    "text_length",
    "is_reply",
    "reply_to_author_id",
    "reply_to_author_username",
    "self_thread_id",
    "self_thread_position",
    "self_thread_length",
    "coupang_urls",
    "first_coupang_url",
    "has_coupang_link",
]

ANALYSIS_FIELD_KEYS = [
    "user_full_name",
    "user_is_verified",
    "user_is_private",
    "user_profile_pic_url",
    "media_type",
    "media_type_label",
    "media_count",
    "is_carousel",
    "has_video",
    "has_audio",
    "image_url",
    "original_width",
    "original_height",
    "like_count",
    "direct_reply_count",
    "quote_count",
    "repost_count",
    "reply_control",
    "like_and_view_counts_disabled",
    "tag_display_name",
    "tag_id",
    "tag_cluster_name",
    "accessibility_caption",
    "gen_ai_detection_method",
    "is_paid_partnership",
    "has_ad_disclosure",
    "pitch_type",
    "content_category",
    "media_richness_score",
    "conversion_intent_score",
    "content_quality_score",
]

ITEM_CORE_FIELD_KEYS = [
    "idx",
    "pk",
    "code",
    "threads_url",
    "taken_at",
    "user_id",
    "username",
    "text",
    "is_reply",
    "reply_to_author_id",
    "reply_to_author_username",
    "self_thread_position",
    "self_thread_length",
    "has_coupang_link",
    "coupang_urls",
    "first_coupang_url",
]

ITEM_ANALYSIS_FIELD_KEYS = [
    "caption_is_edited",
    "media_type",
    "media_type_label",
    "media_count",
    "is_carousel",
    "has_video",
    "has_audio",
    "image_url",
    "original_width",
    "original_height",
    "like_count",
    "direct_reply_count",
    "quote_count",
    "repost_count",
    "reshare_count",
    "view_count",
    "tag_display_name",
    "tag_id",
    "tag_cluster_name",
    "accessibility_caption",
    "gen_ai_detection_method",
    "can_reply",
    "reply_control",
    "can_quote_post",
    "has_ad_disclosure",
    "pitch_type",
    "content_category",
    "media_richness_score",
    "conversion_intent_score",
    "content_quality_score",
]

def safe_get(obj: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    cur: Any = obj
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def compact_text(text: str, max_len: int = 500) -> str:
    text = (text or "").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def has_any_keyword(text: str, keywords: Iterable[str]) -> bool:
    text_lower = (text or "").lower()
    return any(keyword.lower() in text_lower for keyword in keywords)


def count_keywords(text: str, keywords: Iterable[str]) -> int:
    text_lower = (text or "").lower()
    return sum(1 for keyword in keywords if keyword.lower() in text_lower)


def get_caption_text(item: Dict[str, Any]) -> str:
    return safe_get(item, ["caption", "text"], "") or ""


def get_pk(item: Dict[str, Any]) -> str:
    return str(item.get("pk", "") or "")


def get_id(item: Dict[str, Any]) -> str:
    return str(item.get("id", "") or "")


def get_code(item: Dict[str, Any]) -> str:
    return str(item.get("code", "") or "")


def get_taken_at(item: Dict[str, Any]) -> Optional[int]:
    value = item.get("taken_at")
    return value if isinstance(value, int) else None


def get_user_id(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["user", "id"], "") or "")


def get_username(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["user", "username"], "") or "")


def get_user_full_name(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["user", "full_name"], "") or "")


def get_user_is_verified(item: Dict[str, Any]) -> Optional[bool]:
    return safe_get(item, ["user", "is_verified"], None)


def get_user_is_private(item: Dict[str, Any]) -> Optional[bool]:
    return safe_get(item, ["user", "text_post_app_is_private"], None)


def get_user_profile_pic_url(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["user", "profile_pic_url"], "") or "")


def get_reply_to_author_id(item: Dict[str, Any]) -> str:
    value = safe_get(item, ["text_post_app_info", "reply_to_author", "id"], None)
    return str(value) if value is not None else ""


def get_reply_to_author_username(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["text_post_app_info", "reply_to_author", "username"], "") or "")


def get_is_reply(item: Dict[str, Any]) -> Optional[bool]:
    return safe_get(item, ["text_post_app_info", "is_reply"], None)


def get_self_thread_position(item: Dict[str, Any]) -> Optional[int]:
    value = safe_get(
        item,
        ["text_post_app_info", "self_thread_info", "post_position_in_self_thread"],
        None,
    )
    return value if isinstance(value, int) else None


def get_self_thread_length(item: Dict[str, Any]) -> Optional[int]:
    value = safe_get(
        item,
        ["text_post_app_info", "self_thread_info", "self_thread_length"],
        None,
    )
    return value if isinstance(value, int) else None


def get_self_thread_id(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["text_post_app_info", "self_thread_info", "id"], "") or "")


def get_like_count(item: Dict[str, Any]) -> Optional[int]:
    value = item.get("like_count")
    return value if isinstance(value, int) else None


def get_media_type(item: Dict[str, Any]) -> Any:
    return item.get("media_type")


def get_original_width(item: Dict[str, Any]) -> Optional[int]:
    value = item.get("original_width")
    return value if isinstance(value, int) else None


def get_original_height(item: Dict[str, Any]) -> Optional[int]:
    value = item.get("original_height")
    return value if isinstance(value, int) else None


def get_direct_reply_count(item: Dict[str, Any]) -> Optional[int]:
    value = safe_get(item, ["text_post_app_info", "direct_reply_count"], None)
    return value if isinstance(value, int) else None


def get_quote_count(item: Dict[str, Any]) -> Optional[int]:
    value = safe_get(item, ["text_post_app_info", "quote_count"], None)
    return value if isinstance(value, int) else None


def get_repost_count(item: Dict[str, Any]) -> Optional[int]:
    value = safe_get(item, ["text_post_app_info", "repost_count"], None)
    return value if isinstance(value, int) else None


def get_reshare_count(item: Dict[str, Any]) -> Optional[int]:
    value = safe_get(item, ["text_post_app_info", "reshare_count"], None)
    return value if isinstance(value, int) else None


def get_view_count(item: Dict[str, Any]) -> Optional[int]:
    candidates = [
        safe_get(item, ["view_count"], None),
        safe_get(item, ["play_count"], None),
        safe_get(item, ["text_post_app_info", "view_count"], None),
        safe_get(item, ["text_post_app_info", "public_view_count"], None),
    ]
    for value in candidates:
        if isinstance(value, int):
            return value
    return None


def get_caption_is_edited(item: Dict[str, Any]) -> Optional[bool]:
    value = item.get("caption_is_edited")
    return value if isinstance(value, bool) else None


def get_can_reply(item: Dict[str, Any]) -> Optional[bool]:
    value = safe_get(item, ["text_post_app_info", "can_reply"], None)
    return value if isinstance(value, bool) else None


def get_reply_control(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["text_post_app_info", "reply_control"], "") or "")


def get_can_quote_post(item: Dict[str, Any]) -> Optional[bool]:
    value = safe_get(item, ["text_post_app_info", "share_info", "can_quote_post"], None)
    return value if isinstance(value, bool) else None


def get_tag_display_name(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["text_post_app_info", "tag_header", "display_name"], "") or "")


def get_tag_id(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["text_post_app_info", "tag_header", "id"], "") or "")


def get_tag_cluster_name(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["text_post_app_info", "tag_header", "tag_cluster_name"], "") or "")


def get_accessibility_caption(item: Dict[str, Any]) -> str:
    return str(item.get("accessibility_caption") or "")


def get_gen_ai_detection_method(item: Dict[str, Any]) -> str:
    return str(safe_get(item, ["gen_ai_detection_method", "detection_method"], "") or "")


def get_has_audio(item: Dict[str, Any]) -> Optional[bool]:
    value = item.get("has_audio")
    return value if isinstance(value, bool) else None


def get_is_paid_partnership(item: Dict[str, Any]) -> Optional[bool]:
    value = item.get("is_paid_partnership")
    return value if isinstance(value, bool) else None


def get_like_and_view_counts_disabled(item: Dict[str, Any]) -> Optional[bool]:
    value = item.get("like_and_view_counts_disabled")
    return value if isinstance(value, bool) else None


def make_threads_url(username: str, code: str) -> str:
    if not username or not code:
        return ""
    return f"https://www.threads.com/@{username}/post/{code}"


def get_first_candidate_url(candidates: Any) -> str:
    best = get_best_image_candidate(candidates)
    return str(best.get("url", "") or "")


def get_best_image_url(item: Dict[str, Any]) -> str:
    """
    Rule:
    - Use image_versions2.candidates[0].url.
    - If carousel, use carousel_media[0].image_versions2.candidates[0].url.
    """
    candidates = safe_get(item, ["image_versions2", "candidates"], []) or []
    url = get_first_candidate_url(candidates)
    if url:
        return url

    carousel = item.get("carousel_media") or []
    if isinstance(carousel, list) and carousel:
        first_media = carousel[0]
        if isinstance(first_media, dict):
            candidates = safe_get(first_media, ["image_versions2", "candidates"], []) or []
            url = get_first_candidate_url(candidates)
            if url:
                return url

    return ""


def get_media_count(item: Dict[str, Any]) -> int:
    carousel = item.get("carousel_media")
    if isinstance(carousel, list) and carousel:
        return len(carousel)

    if item.get("video_versions"):
        return 1

    candidates = safe_get(item, ["image_versions2", "candidates"], []) or []
    if candidates:
        return 1

    return 0


def get_has_video(item: Dict[str, Any]) -> bool:
    if item.get("video_versions"):
        return True

    carousel = item.get("carousel_media") or []
    if isinstance(carousel, list):
        for media in carousel:
            if isinstance(media, dict) and media.get("video_versions"):
                return True

    return False


def get_is_carousel(item: Dict[str, Any]) -> bool:
    carousel = item.get("carousel_media")
    return isinstance(carousel, list) and len(carousel) > 0


def get_media_type_label(item: Dict[str, Any]) -> str:
    if get_is_carousel(item):
        return "carousel"

    if get_has_video(item):
        return "video"

    media_type = item.get("media_type")
    if media_type == 1:
        return "image"

    if media_type == 19:
        return "text_or_reply"

    candidates = safe_get(item, ["image_versions2", "candidates"], []) or []
    if candidates:
        return "image"

    return "unknown"


def classify_content_category(text: str, accessibility_caption: str = "", tag: str = "") -> str:
    blob = f"{text}\n{accessibility_caption}\n{tag}".lower()
    scores = {
        category: count_keywords(blob, keywords)
        for category, keywords in CATEGORY_KEYWORDS.items()
    }
    best_category = max(scores, key=scores.get)

    if scores[best_category] == 0:
        return "unknown"

    return best_category


def media_richness_score(x: Dict[str, Any]) -> int:
    score = 0

    if x["image_url"]:
        score += 1

    if x["is_carousel"]:
        score += 2

    if x["has_video"]:
        score += 2

    if x["accessibility_caption"]:
        score += 1

    width = x["original_width"] or 0
    height = x["original_height"] or 0
    if width >= 1000 or height >= 1000:
        score += 1

    return min(score, 5)


def conversion_intent_score(text: str) -> int:
    count = count_keywords(text, CONVERSION_KEYWORDS)
    if "link.coupang.com" in (text or "").lower():
        count += 1
    return min(count, 5)


def content_quality_score(x: Dict[str, Any]) -> int:
    score = 0

    text_len = len(x["text"] or "")
    if text_len >= 30:
        score += 1
    if text_len >= 80:
        score += 1

    if x["media_richness_score"] >= 2:
        score += 1

    if x["conversion_intent_score"] >= 1:
        score += 1

    like_count = x["like_count"] or 0
    reply_count = x["direct_reply_count"] or 0
    if like_count >= 1 or reply_count >= 1:
        score += 1

    return min(score, 5)


def has_ad_disclosure(text: str) -> bool:
    return has_any_keyword(text, AD_DISCLOSURE_KEYWORDS)


def detect_pitch_type(text: str) -> str:
    rules = [
        ("discount_or_price", ["할인", "가격", "특가", "핫딜", "저렴", "만원"]),
        ("stock_up", ["쟁여", "쟁임", "계속 사", "계속 먹", "재구매"]),
        ("recommendation", ["추천", "강추", "필수", "템"]),
        ("product_info", ["정보", "제품", "링크", "구경"]),
        ("review", ["후기", "써보", "먹어보", "입어보", "사용"]),
        ("celebrity_item", ["착용", "연예인", "아이돌", "배우", "셀럽"]),
        ("ad_disclosure", AD_DISCLOSURE_KEYWORDS),
    ]

    matched = []
    for label, keywords in rules:
        if has_any_keyword(text, keywords):
            matched.append(label)

    return "|".join(matched) if matched else "unknown"


def build_flat_item(item: Dict[str, Any], idx: int) -> Dict[str, Any]:
    text = get_caption_text(item)
    urls = extract_coupang_urls(text)
    username = get_username(item)
    code = get_code(item)
    image_url = get_best_image_url(item)
    tag_display_name = get_tag_display_name(item)
    accessibility_caption = get_accessibility_caption(item)

    flat = {
        "idx": idx,
        "pk": get_pk(item),
        "id": get_id(item),
        "code": code,
        "threads_url": make_threads_url(username, code),
        "taken_at": get_taken_at(item),
        "text": text,
        "text_preview": compact_text(text, 300),
        "text_length": len(text or ""),
        "user_id": get_user_id(item),
        "username": username,
        "user_full_name": get_user_full_name(item),
        "user_is_verified": get_user_is_verified(item),
        "user_is_private": get_user_is_private(item),
        "user_profile_pic_url": get_user_profile_pic_url(item),
        "is_reply": get_is_reply(item),
        "reply_to_author_id": get_reply_to_author_id(item),
        "reply_to_author_username": get_reply_to_author_username(item),
        "self_thread_id": get_self_thread_id(item),
        "self_thread_position": get_self_thread_position(item),
        "self_thread_length": get_self_thread_length(item),
        "coupang_urls": urls,
        "first_coupang_url": urls[0] if urls else "",
        "has_coupang_link": bool(urls),
        "media_type": get_media_type(item),
        "media_type_label": get_media_type_label(item),
        "media_count": get_media_count(item),
        "is_carousel": get_is_carousel(item),
        "has_video": get_has_video(item),
        "has_audio": get_has_audio(item),
        "image_url": image_url,
        "original_width": get_original_width(item),
        "original_height": get_original_height(item),
        "caption_is_edited": get_caption_is_edited(item),
        "like_count": get_like_count(item),
        "direct_reply_count": get_direct_reply_count(item),
        "quote_count": get_quote_count(item),
        "repost_count": get_repost_count(item),
        "reshare_count": get_reshare_count(item),
        "view_count": get_view_count(item),
        "can_reply": get_can_reply(item),
        "reply_control": get_reply_control(item),
        "can_quote_post": get_can_quote_post(item),
        "like_and_view_counts_disabled": get_like_and_view_counts_disabled(item),
        "tag_display_name": tag_display_name,
        "tag_id": get_tag_id(item),
        "tag_cluster_name": get_tag_cluster_name(item),
        "accessibility_caption": accessibility_caption,
        "gen_ai_detection_method": get_gen_ai_detection_method(item),
        "is_paid_partnership": get_is_paid_partnership(item),
        "has_ad_disclosure": has_ad_disclosure(text),
        "pitch_type": detect_pitch_type(text),
        "raw": item,
    }

    flat["content_category"] = classify_content_category(
        flat["text"],
        flat["accessibility_caption"],
        flat["tag_display_name"],
    )
    flat["media_richness_score"] = media_richness_score(flat)
    flat["conversion_intent_score"] = conversion_intent_score(flat["text"])
    flat["content_quality_score"] = content_quality_score(flat)

    return flat


def is_body_candidate(x: Dict[str, Any]) -> bool:
    return x["has_coupang_link"] is False and x["is_reply"] is False


def is_direct_link_post(x: Dict[str, Any]) -> bool:
    return x["has_coupang_link"] is True and x["is_reply"] is False


def is_link_reply_candidate(x: Dict[str, Any]) -> bool:
    if not x["has_coupang_link"]:
        return False

    if x["is_reply"] is True:
        return True

    if x["reply_to_author_id"]:
        return True

    pos = x["self_thread_position"]
    if isinstance(pos, int) and pos > 1:
        return True

    return False


def is_same_author_self_reply(body: Dict[str, Any], link: Dict[str, Any]) -> bool:
    return bool(
        body["user_id"]
        and link["user_id"]
        and link["reply_to_author_id"]
        and body["user_id"] == link["user_id"] == link["reply_to_author_id"]
    )


def score_pair(body: Dict[str, Any], link: Dict[str, Any]) -> Tuple[int, int, int, int, int, int, int]:
    """
    Lower is better.

    Priority:
    1. matching non-empty self_thread_id
    2. export adjacency, especially body immediately followed by link
    3. self_thread position shape
    4. closest timestamp
    5. closest export index
    """
    body_thread_id = body["self_thread_id"]
    link_thread_id = link["self_thread_id"]
    self_thread_id_rank = 0 if body_thread_id and body_thread_id == link_thread_id else 1

    index_delta = link["idx"] - body["idx"]
    index_distance = abs(index_delta)
    if index_delta == 1:
        adjacency_rank = 0
    elif 1 < index_delta <= 3:
        adjacency_rank = 1
    elif index_distance <= 3:
        adjacency_rank = 2
    else:
        adjacency_rank = 3

    body_pos = body["self_thread_position"]
    link_pos = link["self_thread_position"]
    if body_pos == 1 and link_pos == 2:
        position_rank = 0
    elif link_pos == 2:
        position_rank = 1
    elif isinstance(link_pos, int) and link_pos > 2:
        position_rank = 2
    else:
        position_rank = 3

    body_time = body["taken_at"]
    link_time = link["taken_at"]
    if isinstance(body_time, int) and isinstance(link_time, int):
        time_distance = abs(link_time - body_time)
    else:
        time_distance = 10**12

    return (
        self_thread_id_rank,
        adjacency_rank,
        position_rank,
        time_distance,
        index_distance,
        link["idx"],
        body["idx"],
    )


def determine_confidence(body: Dict[str, Any], link: Dict[str, Any]) -> Tuple[str, str]:
    body_pos = body["self_thread_position"]
    link_pos = link["self_thread_position"]
    body_len = body["self_thread_length"]
    link_len = link["self_thread_length"]

    if body_pos == 1 and link_pos == 2:
        if body_len is None or link_len is None or body_len == link_len:
            return "high", "same_author_reply_to_author + self_thread_position_1_to_2"

    return "medium", "same_author_reply_to_author + global_1_to_1_nearest_valid_link_reply"


def prefix_fields(
    prefix: str,
    x: Dict[str, Any],
    include_analysis_fields: bool = True,
) -> Dict[str, Any]:
    keys = list(LINKAGE_FIELD_KEYS)
    if include_analysis_fields:
        keys.extend(ANALYSIS_FIELD_KEYS)
    return {f"{prefix}_{key}": x.get(key) for key in keys}


def estimate_body_link_relevance_score(body: Dict[str, Any], link: Dict[str, Any]) -> int:
    score = 0

    if body["user_id"] == link["user_id"] == link["reply_to_author_id"]:
        score += 2

    if body["content_category"] != "unknown" and body["content_category"] == link["content_category"]:
        score += 1

    if link["conversion_intent_score"] >= 1:
        score += 1

    if body["self_thread_position"] == 1 and link["self_thread_position"] == 2:
        score += 1

    return min(score, 5)


def estimate_body_link_relevance_reason(body: Dict[str, Any], link: Dict[str, Any]) -> str:
    reasons = []

    if body["user_id"] == link["user_id"] == link["reply_to_author_id"]:
        reasons.append("same_author_self_reply")

    if body["self_thread_position"] == 1 and link["self_thread_position"] == 2:
        reasons.append("self_thread_position_1_to_2")

    if body["content_category"] != "unknown" and body["content_category"] == link["content_category"]:
        reasons.append("same_content_category")

    if link["conversion_intent_score"] >= 1:
        reasons.append("link_has_conversion_intent")

    return "|".join(reasons) if reasons else "weak_or_unknown"


def make_match_record(
    body: Dict[str, Any],
    link: Dict[str, Any],
    pair_score: Tuple[int, int, int, int, int, int, int],
    include_analysis_fields: bool = True,
) -> Dict[str, Any]:
    confidence, method = determine_confidence(body, link)

    body_time = body["taken_at"]
    link_time = link["taken_at"]
    if isinstance(body_time, int) and isinstance(link_time, int):
        time_diff_seconds = link_time - body_time
    else:
        time_diff_seconds = None

    record: Dict[str, Any] = {}
    record.update(prefix_fields("body", body, include_analysis_fields))
    record.update(prefix_fields("link", link, include_analysis_fields))

    body_link_same_category = None
    if include_analysis_fields:
        body_link_same_category = body["content_category"] == link["content_category"]

    record.update(
        {
            "coupang_urls": link["coupang_urls"],
            "first_coupang_url": link["first_coupang_url"],
            "time_diff_seconds": time_diff_seconds,
            "match_type": "body_to_first_link_comment",
            "match_confidence": confidence,
            "match_method": method,
            "match_score_tuple": pair_score,
            "body_link_same_author": body["user_id"] == link["user_id"],
            "body_link_reply_to_author_match": body["user_id"] == link["reply_to_author_id"],
        }
    )

    if include_analysis_fields:
        record.update(
            {
                "body_link_same_category": body_link_same_category,
                "body_link_relevance_score": estimate_body_link_relevance_score(body, link),
                "body_link_relevance_reason": estimate_body_link_relevance_reason(body, link),
            }
        )

    return record


def make_exception_record(
    x: Dict[str, Any],
    exception_type: str,
    reason: str,
    matched_body: Optional[Dict[str, Any]] = None,
    include_analysis_fields: bool = True,
) -> Dict[str, Any]:
    record = dict(prefix_fields("item", x, include_analysis_fields))
    record.update(
        {
            "exception_type": exception_type,
            "reason": reason,
        }
    )

    if matched_body:
        record.update(
            {
                "matched_body_idx": matched_body["idx"],
                "matched_body_pk": matched_body["pk"],
                "matched_body_code": matched_body["code"],
                "matched_body_threads_url": matched_body["threads_url"],
                "matched_body_user_id": matched_body["user_id"],
                "matched_body_username": matched_body["username"],
                "matched_body_text_preview": matched_body["text_preview"],
            }
        )
    else:
        record.update(
            {
                "matched_body_idx": "",
                "matched_body_pk": "",
                "matched_body_code": "",
                "matched_body_threads_url": "",
                "matched_body_user_id": "",
                "matched_body_username": "",
                "matched_body_text_preview": "",
            }
        )

    return record


def make_match_id(body: Dict[str, Any], link: Dict[str, Any]) -> str:
    return f"{body['pk']}__{link['pk']}"


def get_time_diff_seconds(body: Dict[str, Any], link: Dict[str, Any]) -> Optional[int]:
    body_time = body["taken_at"]
    link_time = link["taken_at"]
    if isinstance(body_time, int) and isinstance(link_time, int):
        return link_time - body_time
    return None


def make_item_core_record(x: Dict[str, Any]) -> Dict[str, Any]:
    return {key: x.get(key) for key in ITEM_CORE_FIELD_KEYS}


def make_item_analysis_record(x: Dict[str, Any]) -> Dict[str, Any]:
    record = {"pk": x["pk"]}
    record.update({key: x.get(key) for key in ITEM_ANALYSIS_FIELD_KEYS})
    return record


def get_asset_type(best_image_url: str, best_video_url: str) -> str:
    if best_video_url:
        return "video"
    if best_image_url:
        return "image"
    return "unknown"


def make_media_asset_record(
    item: Dict[str, Any],
    media: Dict[str, Any],
    media_index: int,
    media_source: str,
) -> Dict[str, Any]:
    image_candidate = get_best_image_candidate(
        safe_get(media, ["image_versions2", "candidates"], []) or []
    )
    best_image_url = str(image_candidate.get("url", "") or "")
    best_image_width = image_candidate.get("width")
    best_image_height = image_candidate.get("height")
    video_versions = media.get("video_versions") or []
    best_video_url = get_best_video_url(video_versions)

    return {
        "item_pk": item["pk"],
        "media_index": media_index,
        "media_source": media_source,
        "asset_type": get_asset_type(best_image_url, best_video_url),
        "media_pk": str(media.get("pk", "") or item["pk"]),
        "media_id": str(media.get("id", "") or item["id"]),
        "best_image_url": best_image_url,
        "best_image_width": best_image_width if isinstance(best_image_width, int) else None,
        "best_image_height": best_image_height if isinstance(best_image_height, int) else None,
        "best_video_url": best_video_url,
        "video_version_count": len(video_versions) if isinstance(video_versions, list) else 0,
        "original_width": media.get("original_width") if isinstance(media.get("original_width"), int) else item.get("original_width"),
        "original_height": media.get("original_height") if isinstance(media.get("original_height"), int) else item.get("original_height"),
        "accessibility_caption": str(media.get("accessibility_caption") or item.get("accessibility_caption") or ""),
        "has_audio": media.get("has_audio") if isinstance(media.get("has_audio"), bool) else item.get("has_audio"),
    }


def make_media_asset_records(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = item["raw"]
    carousel = raw.get("carousel_media") or []
    records = []

    if isinstance(carousel, list) and carousel:
        for media_index, media in enumerate(carousel):
            if isinstance(media, dict):
                records.append(
                    make_media_asset_record(
                        item=item,
                        media=media,
                        media_index=media_index,
                        media_source="carousel",
                    )
                )
        return records

    has_top_level_media = bool(
        safe_get(raw, ["image_versions2", "candidates"], [])
        or raw.get("video_versions")
    )
    if has_top_level_media:
        records.append(
            make_media_asset_record(
                item=item,
                media=raw,
                media_index=0,
                media_source="top_level",
            )
        )

    return records


def unwrap_threads_redirect_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc != "l.threads.com":
        return url
    query = parse_qs(parsed.query)
    targets = query.get("u") or []
    if targets:
        return targets[0]
    return url


def is_coupang_link(url: str) -> bool:
    return "link.coupang.com" in (url or "").lower()


def add_item_link_candidate(
    candidates: List[Dict[str, Any]],
    source: str,
    url: str,
    display_text: str = "",
    preview_title: str = "",
    preview_display_url: str = "",
    preview_image_url: str = "",
    preview_favicon_url: str = "",
) -> None:
    if not url:
        return

    normalized_url = normalize_url_tail(unwrap_threads_redirect_url(url))
    candidates.append(
        {
            "source": source,
            "url": normalized_url,
            "display_text": display_text,
            "is_coupang_link": is_coupang_link(normalized_url),
            "preview_title": preview_title,
            "preview_display_url": preview_display_url,
            "preview_image_url": preview_image_url,
            "preview_favicon_url": preview_favicon_url,
        }
    )


def make_item_link_records(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = item["raw"]
    candidates: List[Dict[str, Any]] = []

    for url in extract_urls(item["text"]):
        add_item_link_candidate(candidates, "caption", url)

    fragments = safe_get(raw, ["text_post_app_info", "text_fragments", "fragments"], []) or []
    if isinstance(fragments, list):
        for fragment in fragments:
            if not isinstance(fragment, dict):
                continue
            link_fragment = fragment.get("link_fragment")
            if isinstance(link_fragment, dict):
                add_item_link_candidate(
                    candidates,
                    "text_fragment",
                    str(link_fragment.get("uri", "") or ""),
                    display_text=str(link_fragment.get("display_text", "") or fragment.get("plaintext", "") or ""),
                )

    preview = safe_get(raw, ["text_post_app_info", "link_preview_attachment"], None)
    if isinstance(preview, dict):
        add_item_link_candidate(
            candidates,
            "link_preview",
            str(preview.get("url", "") or ""),
            display_text=str(preview.get("display_url", "") or ""),
            preview_title=str(preview.get("title", "") or ""),
            preview_display_url=str(preview.get("display_url", "") or ""),
            preview_image_url=str(preview.get("image_url", "") or ""),
            preview_favicon_url=str(preview.get("favicon_url", "") or ""),
        )

    grouped: Dict[Tuple[str, str, str, str, str, str], Dict[str, Any]] = {}
    for candidate in candidates:
        key = (
            candidate["source"],
            candidate["url"],
            candidate["display_text"],
            candidate["preview_title"],
            candidate["preview_image_url"],
            candidate["preview_favicon_url"],
        )
        if key not in grouped:
            grouped[key] = dict(candidate)
            grouped[key]["occurrence_count"] = 0
        grouped[key]["occurrence_count"] += 1

    records = []
    for link_index, candidate in enumerate(grouped.values()):
        record = {
            "item_pk": item["pk"],
            "link_index": link_index,
            "source": candidate["source"],
            "url": candidate["url"],
            "display_text": candidate["display_text"],
            "is_coupang_link": candidate["is_coupang_link"],
            "occurrence_count": candidate["occurrence_count"],
            "preview_title": candidate["preview_title"],
            "preview_display_url": candidate["preview_display_url"],
            "preview_image_url": candidate["preview_image_url"],
            "preview_favicon_url": candidate["preview_favicon_url"],
        }
        records.append(record)

    return records


def make_match_core_record(
    body: Dict[str, Any],
    link: Dict[str, Any],
    pair_score: Tuple[int, int, int, int, int, int, int],
) -> Dict[str, Any]:
    confidence, method = determine_confidence(body, link)
    return {
        "match_id": make_match_id(body, link),
        "body_pk": body["pk"],
        "link_pk": link["pk"],
        "first_coupang_url": link["first_coupang_url"],
        "time_diff_seconds": get_time_diff_seconds(body, link),
        "match_confidence": confidence,
        "match_method": method,
        "match_score_tuple": pair_score,
    }


def make_match_analysis_record(body: Dict[str, Any], link: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "match_id": make_match_id(body, link),
        "body_link_same_category": body["content_category"] == link["content_category"],
        "body_link_relevance_score": estimate_body_link_relevance_score(body, link),
        "body_link_relevance_reason": estimate_body_link_relevance_reason(body, link),
    }


def make_exception_core_record(
    x: Dict[str, Any],
    exception_type: str,
    reason: str,
    matched_body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    matched_body_pk = matched_body["pk"] if matched_body else ""
    record = {
        "exception_id": f"{exception_type}__{x['pk']}__{matched_body_pk}",
        "exception_type": exception_type,
        "reason": reason,
        "item_idx": x["idx"],
        "item_pk": x["pk"],
        "item_code": x["code"],
        "item_threads_url": x["threads_url"],
        "item_taken_at": x["taken_at"],
        "item_user_id": x["user_id"],
        "item_username": x["username"],
        "item_text": x["text"],
        "item_is_reply": x["is_reply"],
        "item_reply_to_author_id": x["reply_to_author_id"],
        "item_reply_to_author_username": x["reply_to_author_username"],
        "item_self_thread_position": x["self_thread_position"],
        "item_self_thread_length": x["self_thread_length"],
        "item_has_coupang_link": x["has_coupang_link"],
        "item_first_coupang_url": x["first_coupang_url"],
        "matched_body_pk": matched_body_pk,
        "matched_body_code": matched_body["code"] if matched_body else "",
        "matched_body_threads_url": matched_body["threads_url"] if matched_body else "",
    }
    return record


def make_wide_review_record(
    body: Dict[str, Any],
    link: Dict[str, Any],
    pair_score: Tuple[int, int, int, int, int, int, int],
    body_media_urls: Optional[List[str]] = None,
    link_media_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    match_core = make_match_core_record(body, link, pair_score)
    match_analysis = make_match_analysis_record(body, link)
    body_media_urls = body_media_urls or []
    link_media_urls = link_media_urls or []

    record = {
        "match_id": match_core["match_id"],
        "body_pk": body["pk"],
        "body_code": body["code"],
        "body_threads_url": body["threads_url"],
        "body_taken_at": body["taken_at"],
        "body_user_id": body["user_id"],
        "body_username": body["username"],
        "body_text": body["text"],
        "body_self_thread_position": body["self_thread_position"],
        "body_self_thread_length": body["self_thread_length"],
        "body_media_type_label": body["media_type_label"],
        "body_media_count": body["media_count"],
        "body_is_carousel": body["is_carousel"],
        "body_has_video": body["has_video"],
        "body_image_url": body["image_url"],
        "body_media_urls": body_media_urls,
        "body_media_asset_count": len(body_media_urls),
        "body_original_width": body["original_width"],
        "body_original_height": body["original_height"],
        "body_like_count": body["like_count"],
        "body_direct_reply_count": body["direct_reply_count"],
        "body_quote_count": body["quote_count"],
        "body_repost_count": body["repost_count"],
        "body_reshare_count": body["reshare_count"],
        "body_tag_display_name": body["tag_display_name"],
        "body_accessibility_caption": body["accessibility_caption"],
        "body_has_ad_disclosure": body["has_ad_disclosure"],
        "body_pitch_type": body["pitch_type"],
        "body_content_category": body["content_category"],
        "body_media_richness_score": body["media_richness_score"],
        "body_conversion_intent_score": body["conversion_intent_score"],
        "body_content_quality_score": body["content_quality_score"],
        "link_pk": link["pk"],
        "link_code": link["code"],
        "link_threads_url": link["threads_url"],
        "link_taken_at": link["taken_at"],
        "link_user_id": link["user_id"],
        "link_username": link["username"],
        "link_text": link["text"],
        "link_reply_to_author_id": link["reply_to_author_id"],
        "link_self_thread_position": link["self_thread_position"],
        "link_self_thread_length": link["self_thread_length"],
        "link_first_coupang_url": link["first_coupang_url"],
        "link_media_urls": link_media_urls,
        "link_media_asset_count": len(link_media_urls),
        "link_like_count": link["like_count"],
        "link_direct_reply_count": link["direct_reply_count"],
        "link_quote_count": link["quote_count"],
        "link_repost_count": link["repost_count"],
        "link_reshare_count": link["reshare_count"],
        "link_has_ad_disclosure": link["has_ad_disclosure"],
        "link_pitch_type": link["pitch_type"],
        "link_content_category": link["content_category"],
        "link_conversion_intent_score": link["conversion_intent_score"],
        "time_diff_seconds": match_core["time_diff_seconds"],
        "match_confidence": match_core["match_confidence"],
        "match_method": match_core["match_method"],
        "match_score_tuple": match_core["match_score_tuple"],
        "body_link_same_category": match_analysis["body_link_same_category"],
        "body_link_relevance_score": match_analysis["body_link_relevance_score"],
        "body_link_relevance_reason": match_analysis["body_link_relevance_reason"],
    }
    return record


def make_match_content_summary_record(
    body: Dict[str, Any],
    link: Dict[str, Any],
    pair_score: Tuple[int, int, int, int, int, int, int],
    body_media_urls: Optional[List[str]] = None,
    link_media_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    match_core = make_match_core_record(body, link, pair_score)
    body_media_urls = body_media_urls or []
    link_media_urls = link_media_urls or []

    return {
        "match_id": match_core["match_id"],
        "match_confidence": match_core["match_confidence"],
        "first_coupang_url": match_core["first_coupang_url"],
        "body_username": body["username"],
        "body_tag_display_name": body["tag_display_name"],
        "body_taken_at": body["taken_at"],
        "body_taken_at_iso": timestamp_to_kst_iso(body["taken_at"]),
        "body_text": body["text"],
        "body_text_lines": split_text_lines(body["text"]),
        "body_threads_url": body["threads_url"],
        "body_media_urls": body_media_urls,
        "body_view_count": body["view_count"],
        "body_like_count": body["like_count"],
        "body_direct_reply_count": body["direct_reply_count"],
        "body_repost_count": body["repost_count"],
        "body_reshare_count": body["reshare_count"],
        "body_quote_count": body["quote_count"],
        "link_username": link["username"],
        "link_tag_display_name": link["tag_display_name"],
        "link_taken_at": link["taken_at"],
        "link_taken_at_iso": timestamp_to_kst_iso(link["taken_at"]),
        "link_text": link["text"],
        "link_text_lines": split_text_lines(link["text"]),
        "link_threads_url": link["threads_url"],
        "link_media_urls": link_media_urls,
        "link_coupang_urls": link["coupang_urls"],
        "link_first_coupang_url": link["first_coupang_url"],
        "link_view_count": link["view_count"],
        "link_like_count": link["like_count"],
        "link_direct_reply_count": link["direct_reply_count"],
        "link_repost_count": link["repost_count"],
        "link_reshare_count": link["reshare_count"],
        "link_quote_count": link["quote_count"],
    }


MATCH_ENGAGEMENT_SUMMARY_FIELDS = (
    "match_id",
    "body_text",
    "body_view_count",
    "body_like_count",
    "body_direct_reply_count",
    "body_repost_count",
    "body_reshare_count",
    "link_text",
    "link_view_count",
    "link_like_count",
    "link_direct_reply_count",
    "link_repost_count",
    "link_reshare_count",
)


def make_match_engagement_summary_record(
    match_content_summary_record: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        field: match_content_summary_record.get(field)
        for field in MATCH_ENGAGEMENT_SUMMARY_FIELDS
    }


def normalize_for_csv_value(value: Any) -> Any:
    if isinstance(value, list):
        return " | ".join(map(str, value))
    if isinstance(value, tuple):
        return " | ".join(map(str, value))
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return value


def write_json(path: Path, data: Any) -> None:
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return

    normalized = []
    all_keys = []

    for row in rows:
        out = {}
        for key, value in row.items():
            out[key] = normalize_for_csv_value(value)
            if key not in all_keys:
                all_keys.append(key)
        normalized.append(out)

    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(normalized)


def make_field_dictionary() -> List[Dict[str, str]]:
    fields = [
        ("items_core", "key", "pk", "게시물/댓글 고유 ID. item_analysis와 join하는 기본 키."),
        ("items_core", "trace", "idx", "원본 JSON 배열에서의 위치."),
        ("items_core", "trace", "code", "Threads post shortcode."),
        ("items_core", "trace", "threads_url", "Threads 웹 URL."),
        ("items_core", "trace", "taken_at", "Unix timestamp 형태의 작성 시각."),
        ("items_core", "author", "user_id", "작성자 고유 ID."),
        ("items_core", "author", "username", "작성자 계정명."),
        ("items_core", "content", "text", "caption.text 원문."),
        ("items_core", "reply", "is_reply", "답글/댓글 여부."),
        ("items_core", "reply", "reply_to_author_id", "답글 대상 작성자 ID."),
        ("items_core", "reply", "reply_to_author_username", "답글 대상 작성자 계정명."),
        ("items_core", "thread", "self_thread_position", "self-thread 안에서의 순서."),
        ("items_core", "thread", "self_thread_length", "self-thread 전체 길이."),
        ("items_core", "link", "has_coupang_link", "쿠팡 링크 포함 여부."),
        ("items_core", "link", "coupang_urls", "추출된 쿠팡 링크 목록."),
        ("items_core", "link", "first_coupang_url", "첫 번째 쿠팡 링크."),
        ("item_analysis", "key", "pk", "items_core.pk와 join하는 키."),
        ("item_analysis", "content", "caption_is_edited", "caption 편집 여부."),
        ("item_analysis", "media", "media_type", "원본 media_type 숫자."),
        ("item_analysis", "media", "media_type_label", "image/video/carousel/text_or_reply/unknown 라벨."),
        ("item_analysis", "media", "media_count", "미디어 개수."),
        ("item_analysis", "media", "is_carousel", "캐러셀 여부."),
        ("item_analysis", "media", "has_video", "동영상 포함 여부."),
        ("item_analysis", "media", "has_audio", "오디오 포함 여부."),
        ("item_analysis", "media", "image_url", "본문/미디어 분석용 대표 이미지 URL."),
        ("item_analysis", "media", "original_width", "원본 미디어 너비."),
        ("item_analysis", "media", "original_height", "원본 미디어 높이."),
        ("item_analysis", "metrics", "like_count", "좋아요 수."),
        ("item_analysis", "metrics", "direct_reply_count", "직접 댓글/답글 수."),
        ("item_analysis", "metrics", "quote_count", "인용 수."),
        ("item_analysis", "metrics", "repost_count", "리포스트 수."),
        ("item_analysis", "metrics", "reshare_count", "리쉐어 수. 원본에 없으면 빈 값."),
        ("item_analysis", "metrics", "view_count", "조회수. 원본 또는 --view-counts-file에서 제공되면 저장."),
        ("item_analysis", "tag", "tag_display_name", "Threads 태그 표시명."),
        ("item_analysis", "tag", "tag_id", "Threads 태그 ID."),
        ("item_analysis", "tag", "tag_cluster_name", "Threads 태그 클러스터명."),
        ("item_analysis", "content", "accessibility_caption", "이미지 자동 설명."),
        ("item_analysis", "aux", "gen_ai_detection_method", "C2PA/AI 감지 관련 참고 메타데이터."),
        ("item_analysis", "aux", "can_reply", "답글 허용 여부."),
        ("item_analysis", "aux", "reply_control", "답글 허용 범위."),
        ("item_analysis", "aux", "can_quote_post", "인용 가능 여부."),
        ("item_analysis", "derived", "has_ad_disclosure", "광고 고지 문구 포함 여부."),
        ("item_analysis", "derived", "pitch_type", "링크 유도 문구 유형."),
        ("item_analysis", "derived", "content_category", "키워드 기반 콘텐츠 카테고리."),
        ("item_analysis", "derived", "media_richness_score", "미디어 풍부도 0~5."),
        ("item_analysis", "derived", "conversion_intent_score", "구매/추천/링크 유도성 0~5."),
        ("item_analysis", "derived", "content_quality_score", "콘텐츠 품질 보조 점수 0~5."),
        ("media_assets", "key", "item_pk", "items_core.pk와 join하는 원본 item pk."),
        ("media_assets", "key", "media_index", "item 안에서의 미디어 순서. top-level은 0."),
        ("media_assets", "media", "media_source", "top_level 또는 carousel."),
        ("media_assets", "media", "asset_type", "image/video/unknown."),
        ("media_assets", "media", "media_pk", "carousel media pk 또는 item pk."),
        ("media_assets", "media", "media_id", "carousel media id 또는 item id."),
        ("media_assets", "media", "best_image_url", "가장 고해상도 이미지 후보 URL."),
        ("media_assets", "media", "best_image_width", "대표 이미지 너비."),
        ("media_assets", "media", "best_image_height", "대표 이미지 높이."),
        ("media_assets", "media", "best_video_url", "대표 video_versions URL."),
        ("media_assets", "media", "video_version_count", "video_versions 후보 개수."),
        ("media_assets", "media", "original_width", "원본 미디어 너비."),
        ("media_assets", "media", "original_height", "원본 미디어 높이."),
        ("media_assets", "content", "accessibility_caption", "미디어별 이미지 자동 설명."),
        ("media_assets", "media", "has_audio", "미디어 오디오 포함 여부."),
        ("item_links", "key", "item_pk", "items_core.pk와 join하는 원본 item pk."),
        ("item_links", "key", "link_index", "item 안에서의 링크 순서."),
        ("item_links", "link", "source", "caption, text_fragment, link_preview 중 출처."),
        ("item_links", "link", "url", "추출 또는 redirect 해제된 URL."),
        ("item_links", "link", "display_text", "텍스트에 표시된 링크 문구."),
        ("item_links", "link", "is_coupang_link", "쿠팡 링크 여부."),
        ("item_links", "link", "occurrence_count", "같은 source/url 반복 횟수."),
        ("item_links", "preview", "preview_title", "link_preview 상품명/제목."),
        ("item_links", "preview", "preview_display_url", "link_preview 표시 도메인."),
        ("item_links", "preview", "preview_image_url", "link_preview 썸네일 URL."),
        ("item_links", "preview", "preview_favicon_url", "link_preview favicon URL."),
        ("matches_core", "key", "match_id", "body_pk와 link_pk를 조합한 안정적 매칭 ID."),
        ("matches_core", "relation", "body_pk", "쿠팡 링크가 없는 원본문 pk."),
        ("matches_core", "relation", "link_pk", "본문에 연결된 첫 번째 쿠팡 링크댓글 pk."),
        ("matches_core", "link", "first_coupang_url", "매칭된 링크댓글의 첫 번째 쿠팡 URL."),
        ("matches_core", "match", "time_diff_seconds", "링크댓글 작성 시각 - 본문 작성 시각."),
        ("matches_core", "match", "match_confidence", "high 또는 medium."),
        ("matches_core", "match", "match_method", "매칭에 사용된 방법 설명."),
        ("matches_core", "match", "match_score_tuple", "전역 1:1 greedy 정렬 점수."),
        ("match_analysis", "key", "match_id", "matches_core.match_id와 1:1 join하는 키."),
        ("match_analysis", "derived", "body_link_same_category", "본문과 링크댓글의 카테고리 일치 여부."),
        ("match_analysis", "derived", "body_link_relevance_score", "본문과 링크댓글 연결성 점수 0~5."),
        ("match_analysis", "derived", "body_link_relevance_reason", "연결성 점수의 이유."),
        ("exceptions_core", "key", "exception_id", "예외 레코드 ID."),
        ("exceptions_core", "exception", "exception_type", "예외 분류."),
        ("exceptions_core", "exception", "reason", "예외로 분류된 이유."),
        ("exceptions_core", "item", "item_pk", "예외 대상 글/댓글 pk."),
        ("exceptions_core", "item", "item_text", "예외 대상 caption 원문."),
        ("exceptions_core", "relation", "matched_body_pk", "extra_link_after_first 등에서 연결된 body pk."),
        ("matches_wide_review", "review", "*", "검수 편의를 위해 core/analysis 일부를 병합한 축약 wide 파일."),
        ("matches_wide_review", "media", "body_media_urls", "body에 연결된 이미지/영상 대표 URL 배열."),
        ("matches_wide_review", "media", "link_media_urls", "링크댓글에 연결된 이미지/영상 대표 URL 배열."),
        ("matches_wide_review", "media", "body_media_asset_count", "body_media_urls에 포함된 URL 개수."),
        ("matches_wide_review", "media", "link_media_asset_count", "link_media_urls에 포함된 URL 개수."),
        ("match_content_summary", "key", "match_id", "matches_core.match_id와 같은 매칭 ID."),
        ("match_content_summary", "match", "match_confidence", "매칭 신뢰도."),
        ("match_content_summary", "link", "first_coupang_url", "매칭된 첫 번째 쿠팡 URL."),
        ("match_content_summary", "body", "body_username", "본문 작성자 계정명."),
        ("match_content_summary", "body", "body_tag_display_name", "본문 태그 표시명."),
        ("match_content_summary", "body", "body_taken_at_iso", "Asia/Seoul 기준 본문 작성 시각 ISO 문자열."),
        ("match_content_summary", "body", "body_text", "본문 caption 원문."),
        ("match_content_summary", "body", "body_text_lines", "본문 텍스트를 줄 단위로 나눈 배열."),
        ("match_content_summary", "body", "body_threads_url", "본문 Threads URL."),
        ("match_content_summary", "body", "body_media_urls", "본문 이미지/영상 대표 URL 배열."),
        ("match_content_summary", "body_metrics", "body_view_count", "본문 조회수. 원본 또는 --view-counts-file에 없으면 빈 값."),
        ("match_content_summary", "body_metrics", "body_like_count", "본문 좋아요 수."),
        ("match_content_summary", "body_metrics", "body_direct_reply_count", "본문 직접 댓글/답글 수."),
        ("match_content_summary", "body_metrics", "body_repost_count", "본문 리포스트 수."),
        ("match_content_summary", "body_metrics", "body_reshare_count", "본문 리쉐어 수."),
        ("match_content_summary", "body_metrics", "body_quote_count", "본문 인용 수."),
        ("match_content_summary", "link", "link_username", "링크댓글 작성자 계정명."),
        ("match_content_summary", "link", "link_tag_display_name", "링크댓글 태그 표시명."),
        ("match_content_summary", "link", "link_taken_at_iso", "Asia/Seoul 기준 링크댓글 작성 시각 ISO 문자열."),
        ("match_content_summary", "link", "link_text", "링크댓글 caption 원문."),
        ("match_content_summary", "link", "link_text_lines", "링크댓글 텍스트를 줄 단위로 나눈 배열."),
        ("match_content_summary", "link", "link_threads_url", "링크댓글 Threads URL."),
        ("match_content_summary", "link", "link_media_urls", "링크댓글 이미지/영상 대표 URL 배열."),
        ("match_content_summary", "link", "link_coupang_urls", "링크댓글에서 추출된 쿠팡 URL 배열."),
        ("match_content_summary", "link", "link_first_coupang_url", "링크댓글의 첫 번째 쿠팡 URL."),
        ("match_content_summary", "link_metrics", "link_view_count", "링크댓글 조회수. 원본 또는 --view-counts-file에 없으면 빈 값."),
        ("match_content_summary", "link_metrics", "link_like_count", "링크댓글 좋아요 수."),
        ("match_content_summary", "link_metrics", "link_direct_reply_count", "링크댓글 직접 댓글/답글 수."),
        ("match_content_summary", "link_metrics", "link_repost_count", "링크댓글 리포스트 수."),
        ("match_content_summary", "link_metrics", "link_reshare_count", "링크댓글 리쉐어 수."),
        ("match_content_summary", "link_metrics", "link_quote_count", "링크댓글 인용 수."),
        ("match_engagement_summary", "key", "match_id", "matches_core.match_id와 같은 매칭 ID."),
        ("match_engagement_summary", "body", "body_text", "본문 caption 원문."),
        ("match_engagement_summary", "body_metrics", "body_view_count", "본문 조회수. 원본 또는 --view-counts-file에 없으면 빈 값."),
        ("match_engagement_summary", "body_metrics", "body_like_count", "본문 좋아요 수."),
        ("match_engagement_summary", "body_metrics", "body_direct_reply_count", "본문 직접 댓글/답글 수."),
        ("match_engagement_summary", "body_metrics", "body_repost_count", "본문 리포스트 수."),
        ("match_engagement_summary", "body_metrics", "body_reshare_count", "본문 리쉐어 수."),
        ("match_engagement_summary", "link", "link_text", "링크댓글 caption 원문."),
        ("match_engagement_summary", "link_metrics", "link_view_count", "링크댓글 조회수. 원본 또는 --view-counts-file에 없으면 빈 값."),
        ("match_engagement_summary", "link_metrics", "link_like_count", "링크댓글 좋아요 수."),
        ("match_engagement_summary", "link_metrics", "link_direct_reply_count", "링크댓글 직접 댓글/답글 수."),
        ("match_engagement_summary", "link_metrics", "link_repost_count", "링크댓글 리포스트 수."),
        ("match_engagement_summary", "link_metrics", "link_reshare_count", "링크댓글 리쉐어 수."),
    ]

    return [
        {
            "table_name": table_name,
            "field_group": field_group,
            "field": field,
            "description": description,
        }
        for table_name, field_group, field, description in fields
    ]


def build_pair_candidates(
    body_candidates: List[Dict[str, Any]],
    link_reply_candidates: List[Dict[str, Any]],
) -> Tuple[
    List[Tuple[Tuple[int, int, int, int, int, int, int], Dict[str, Any], Dict[str, Any]]],
    Dict[str, List[Tuple[Tuple[int, int, int, int, int, int, int], Dict[str, Any]]]],
]:
    pairs = []
    by_body: Dict[str, List[Tuple[Tuple[int, int, int, int, int, int, int], Dict[str, Any]]]] = defaultdict(list)

    for body in body_candidates:
        for link in link_reply_candidates:
            if link["pk"] == body["pk"]:
                continue

            if not is_same_author_self_reply(body, link):
                continue

            pair_score = score_pair(body, link)
            pairs.append((pair_score, body, link))
            by_body[body["pk"]].append((pair_score, link))

    for candidates in by_body.values():
        candidates.sort(key=lambda item: item[0])

    pairs.sort(key=lambda item: item[0])
    return pairs, by_body


def select_global_one_to_one_matches(
    pair_candidates: List[Tuple[Tuple[int, int, int, int, int, int, int], Dict[str, Any], Dict[str, Any]]]
) -> List[Tuple[Tuple[int, int, int, int, int, int, int], Dict[str, Any], Dict[str, Any]]]:
    used_body_pks = set()
    used_link_pks = set()
    selected = []

    for pair_score, body, link in pair_candidates:
        if body["pk"] in used_body_pks:
            continue
        if link["pk"] in used_link_pks:
            continue

        selected.append((pair_score, body, link))
        used_body_pks.add(body["pk"])
        used_link_pks.add(link["pk"])

    return selected


def count_duplicate_values(values: Iterable[str]) -> int:
    counts = Counter(value for value in values if value)
    return sum(count - 1 for count in counts.values() if count > 1)


def build_media_urls_by_item_pk(media_assets: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    grouped: Dict[str, List[Tuple[int, str]]] = defaultdict(list)

    for asset in media_assets:
        item_pk = str(asset.get("item_pk", "") or "")
        if not item_pk:
            continue

        url = str(asset.get("best_video_url") or asset.get("best_image_url") or "")
        if not url:
            continue

        media_index = asset.get("media_index")
        if not isinstance(media_index, int):
            try:
                media_index = int(media_index)
            except (TypeError, ValueError):
                media_index = 10**12

        grouped[item_pk].append((media_index, url))

    result = {}
    for item_pk, values in grouped.items():
        values.sort(key=lambda value: value[0])
        result[item_pk] = [url for _, url in values]

    return result


def timestamp_to_kst_iso(timestamp: Any) -> str:
    if not isinstance(timestamp, int):
        return ""

    kst = dt.timezone(dt.timedelta(hours=9), name="Asia/Seoul")
    return dt.datetime.fromtimestamp(timestamp, tz=kst).isoformat()


def run_extraction(
    input_path: Path,
    output_prefix: str,
    output_dir: Path,
    output_shape: str = "both",
    view_counts_file: Optional[Path] = None,
) -> Dict[str, Any]:
    if output_shape not in OUTPUT_SHAPES:
        raise ValueError(f"output_shape must be one of: {', '.join(OUTPUT_SHAPES)}")

    raw = json.loads(input_path.read_text(encoding="utf-8"))

    if not isinstance(raw, list):
        raise ValueError("Input JSON must be a list of items.")

    items = [build_flat_item(item, idx) for idx, item in enumerate(raw)]
    view_count_summary: Dict[str, Any] = {}
    if view_counts_file is not None:
        view_count_summary = apply_view_counts_to_items(items, view_counts_file)

    items_core = [make_item_core_record(item) for item in items]
    item_analysis = [make_item_analysis_record(item) for item in items]
    media_assets = [
        record
        for item in items
        for record in make_media_asset_records(item)
    ]
    media_urls_by_item_pk = build_media_urls_by_item_pk(media_assets)
    item_links = [
        record
        for item in items
        for record in make_item_link_records(item)
    ]

    body_candidates = [x for x in items if is_body_candidate(x)]
    link_reply_candidates = [x for x in items if is_link_reply_candidate(x)]
    direct_link_posts = [x for x in items if is_direct_link_post(x)]

    pair_candidates, pair_candidates_by_body = build_pair_candidates(
        body_candidates,
        link_reply_candidates,
    )
    selected_pairs = select_global_one_to_one_matches(pair_candidates)
    selected_pairs = sorted(selected_pairs, key=lambda item: (item[1]["idx"], item[2]["idx"]))

    matches_core = []
    match_analysis = []
    matches_wide_review = []
    match_content_summary = []
    match_engagement_summary = []
    exceptions_core = []

    selected_by_body_pk = {}
    selected_by_link_pk = {}

    for pair_score, body, link in selected_pairs:
        body_media_urls = media_urls_by_item_pk.get(body["pk"], [])
        link_media_urls = media_urls_by_item_pk.get(link["pk"], [])

        selected_by_body_pk[body["pk"]] = link
        selected_by_link_pk[link["pk"]] = body
        matches_core.append(make_match_core_record(body, link, pair_score))
        match_analysis.append(make_match_analysis_record(body, link))
        matches_wide_review.append(
            make_wide_review_record(
                body,
                link,
                pair_score,
                body_media_urls=body_media_urls,
                link_media_urls=link_media_urls,
            )
        )
        content_summary_record = make_match_content_summary_record(
            body,
            link,
            pair_score,
            body_media_urls=body_media_urls,
            link_media_urls=link_media_urls,
        )
        match_content_summary.append(content_summary_record)
        match_engagement_summary.append(
            make_match_engagement_summary_record(content_summary_record)
        )

    exception_pks = set()

    for body in body_candidates:
        selected_link = selected_by_body_pk.get(body["pk"])

        if not selected_link:
            exceptions_core.append(
                make_exception_core_record(
                    body,
                    "body_without_link_comment",
                    "Body candidate has no valid unused self Coupang link reply after global 1:1 matching.",
                )
            )
            exception_pks.add(body["pk"])
            continue

        for pair_score, extra in pair_candidates_by_body.get(body["pk"], []):
            if extra["pk"] == selected_link["pk"]:
                continue
            if extra["pk"] in selected_by_link_pk:
                continue
            if extra["pk"] in exception_pks:
                continue

            exceptions_core.append(
                make_exception_core_record(
                    extra,
                    "extra_link_after_first",
                    "Additional Coupang link reply after the first matched link reply for this body.",
                    matched_body=body,
                )
            )
            exception_pks.add(extra["pk"])

    for x in direct_link_posts:
        if x["pk"] in exception_pks:
            continue

        exceptions_core.append(
            make_exception_core_record(
                x,
                "direct_link_post",
                "The post itself contains a Coupang link, not a body -> link comment structure.",
            )
        )
        exception_pks.add(x["pk"])

    for link in link_reply_candidates:
        if link["pk"] in selected_by_link_pk:
            continue
        if link["pk"] in exception_pks:
            continue

        possible_bodies = [
            body
            for body in body_candidates
            if body["user_id"] == link["reply_to_author_id"]
        ]

        if link["user_id"] != link["reply_to_author_id"]:
            exception_type = "external_or_nested_reply_link"
            reason = (
                "Coupang link reply author differs from reply_to_author; "
                "likely external reply or nested reply."
            )
        elif possible_bodies:
            exception_type = "self_reply_link_without_matched_body"
            reason = (
                "Self Coupang link reply exists, but no stable unused body match was selected. "
                "Likely duplicate, ambiguous, or outside the intended body -> first link comment structure."
            )
        else:
            exception_type = "self_reply_link_without_matched_body"
            reason = (
                "Self Coupang link reply exists, but corresponding body is not available "
                "or not a valid body candidate."
            )

        exceptions_core.append(
            make_exception_core_record(
                link,
                exception_type,
                reason,
            )
        )
        exception_pks.add(link["pk"])

    exceptions_core.sort(key=lambda x: (x.get("item_idx", 10**12), x.get("exception_type", "")))

    output_dir.mkdir(parents=True, exist_ok=True)

    items_core_json = output_dir / f"{output_prefix}_items_core.json"
    items_core_csv = output_dir / f"{output_prefix}_items_core.csv"
    matches_core_json = output_dir / f"{output_prefix}_matches_core.json"
    matches_core_csv = output_dir / f"{output_prefix}_matches_core.csv"
    item_analysis_json = output_dir / f"{output_prefix}_item_analysis.json"
    item_analysis_csv = output_dir / f"{output_prefix}_item_analysis.csv"
    media_assets_json = output_dir / f"{output_prefix}_media_assets.json"
    media_assets_csv = output_dir / f"{output_prefix}_media_assets.csv"
    item_links_json = output_dir / f"{output_prefix}_item_links.json"
    item_links_csv = output_dir / f"{output_prefix}_item_links.csv"
    match_analysis_json = output_dir / f"{output_prefix}_match_analysis.json"
    match_analysis_csv = output_dir / f"{output_prefix}_match_analysis.csv"
    exceptions_core_json = output_dir / f"{output_prefix}_exceptions_core.json"
    exceptions_core_csv = output_dir / f"{output_prefix}_exceptions_core.csv"
    matches_wide_review_json = output_dir / f"{output_prefix}_matches_wide_review.json"
    matches_wide_review_csv = output_dir / f"{output_prefix}_matches_wide_review.csv"
    match_content_summary_json = output_dir / f"{output_prefix}_match_content_summary.json"
    match_content_summary_csv = output_dir / f"{output_prefix}_match_content_summary.csv"
    match_engagement_summary_json = output_dir / f"{output_prefix}_match_engagement_summary.json"
    match_engagement_summary_csv = output_dir / f"{output_prefix}_match_engagement_summary.csv"
    summary_json = output_dir / f"{output_prefix}_coupang_enhanced_summary.json"
    field_dict_json = output_dir / f"{output_prefix}_coupang_enhanced_field_dictionary.json"
    field_dict_csv = output_dir / f"{output_prefix}_coupang_enhanced_field_dictionary.csv"

    match_confidence_counts = Counter(match["match_confidence"] for match in matches_core)
    exception_type_counts = Counter(exception["exception_type"] for exception in exceptions_core)
    selected_bodies = [body for _, body, _ in selected_pairs]
    selected_links = [link for _, _, link in selected_pairs]
    body_media_type_counts = Counter(body.get("media_type_label", "unknown") for body in selected_bodies)
    body_media_type_raw_counts = Counter(str(body.get("media_type", "unknown")) for body in selected_bodies)
    body_category_counts = Counter(body.get("content_category", "unknown") for body in selected_bodies)
    link_pitch_type_counts = Counter(link.get("pitch_type", "unknown") for link in selected_links)
    body_tag_counts = Counter(body.get("tag_display_name", "") or "none" for body in selected_bodies)
    username_counts = Counter(body.get("username", "") or "unknown" for body in selected_bodies)

    matched_body_pks = [match.get("body_pk", "") for match in matches_core]
    matched_link_pks = [match.get("link_pk", "") for match in matches_core]

    summary = {
        "input_file": str(input_path),
        "output_prefix": output_prefix,
        "output_shape": output_shape,
        "matching_policy": "global_1_to_1_greedy",
        "total_items": len(items),
        "items_with_coupang_link": sum(1 for x in items if x["has_coupang_link"]),
        "items_without_coupang_link": sum(1 for x in items if not x["has_coupang_link"]),
        "body_candidates": len(body_candidates),
        "link_reply_candidates": len(link_reply_candidates),
        "direct_link_posts": len(direct_link_posts),
        "pair_candidates": len(pair_candidates),
        "matches": len(matches_core),
        "exceptions": len(exceptions_core),
        "items_core_rows": len(items_core),
        "matches_core_rows": len(matches_core),
        "item_analysis_rows": len(item_analysis),
        "media_assets_rows": len(media_assets),
        "item_links_rows": len(item_links),
        "match_analysis_rows": len(match_analysis),
        "exceptions_core_rows": len(exceptions_core),
        "matches_wide_review_rows": len(matches_wide_review),
        "match_content_summary_rows": len(match_content_summary),
        "match_engagement_summary_rows": len(match_engagement_summary),
        "unique_matched_body_pks": len(set(matched_body_pks)),
        "unique_matched_link_pks": len(set(matched_link_pks)),
        "duplicate_matched_body_pks": count_duplicate_values(matched_body_pks),
        "duplicate_matched_link_pks": count_duplicate_values(matched_link_pks),
        "match_confidence_counts": dict(match_confidence_counts),
        "exception_type_counts": dict(exception_type_counts),
        "body_media_type_counts": dict(body_media_type_counts),
        "body_media_type_raw_counts": dict(body_media_type_raw_counts),
        "body_category_counts": dict(body_category_counts),
        "link_pitch_type_counts": dict(link_pitch_type_counts),
        "body_tag_counts": dict(body_tag_counts),
        "top_usernames_by_matches": dict(username_counts.most_common(30)),
        "output_files": {},
    }
    if view_count_summary:
        summary.update(view_count_summary)

    output_files: Dict[str, str] = {}

    if output_shape in ("both", "db"):
        write_json(items_core_json, items_core)
        write_csv(items_core_csv, items_core)
        write_json(matches_core_json, matches_core)
        write_csv(matches_core_csv, matches_core)
        write_json(item_analysis_json, item_analysis)
        write_csv(item_analysis_csv, item_analysis)
        write_json(media_assets_json, media_assets)
        write_csv(media_assets_csv, media_assets)
        write_json(item_links_json, item_links)
        write_csv(item_links_csv, item_links)
        write_json(match_analysis_json, match_analysis)
        write_csv(match_analysis_csv, match_analysis)
        write_json(exceptions_core_json, exceptions_core)
        write_csv(exceptions_core_csv, exceptions_core)
        output_files.update(
            {
                "items_core_json": str(items_core_json),
                "items_core_csv": str(items_core_csv),
                "matches_core_json": str(matches_core_json),
                "matches_core_csv": str(matches_core_csv),
                "item_analysis_json": str(item_analysis_json),
                "item_analysis_csv": str(item_analysis_csv),
                "media_assets_json": str(media_assets_json),
                "media_assets_csv": str(media_assets_csv),
                "item_links_json": str(item_links_json),
                "item_links_csv": str(item_links_csv),
                "match_analysis_json": str(match_analysis_json),
                "match_analysis_csv": str(match_analysis_csv),
                "exceptions_core_json": str(exceptions_core_json),
                "exceptions_core_csv": str(exceptions_core_csv),
            }
        )

    if output_shape in ("both", "wide"):
        write_json(matches_wide_review_json, matches_wide_review)
        write_csv(matches_wide_review_csv, matches_wide_review)
        write_json(match_content_summary_json, match_content_summary)
        write_csv(match_content_summary_csv, match_content_summary)
        write_json(match_engagement_summary_json, match_engagement_summary)
        write_csv(match_engagement_summary_csv, match_engagement_summary)
        output_files.update(
            {
                "matches_wide_review_json": str(matches_wide_review_json),
                "matches_wide_review_csv": str(matches_wide_review_csv),
                "match_content_summary_json": str(match_content_summary_json),
                "match_content_summary_csv": str(match_content_summary_csv),
                "match_engagement_summary_json": str(match_engagement_summary_json),
                "match_engagement_summary_csv": str(match_engagement_summary_csv),
            }
        )

    field_dictionary = make_field_dictionary()

    output_files.update(
        {
            "summary_json": str(summary_json),
            "field_dictionary_json": str(field_dict_json),
            "field_dictionary_csv": str(field_dict_csv),
        }
    )
    summary["output_files"] = output_files

    write_json(summary_json, summary)
    write_json(field_dict_json, field_dictionary)
    write_csv(field_dict_csv, field_dictionary)

    return summary


def infer_prefix(input_path: Path) -> str:
    stem = input_path.stem
    return re.sub(r"[^0-9A-Za-z가-힣_-]+", "_", stem)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract body -> first Coupang link-comment pairs from Threads export JSON."
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Input JSON file path. Must be a list of exported items.",
    )
    parser.add_argument(
        "--output-prefix",
        "-p",
        default=None,
        help="Output file prefix. Defaults to input file stem.",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        default=".",
        help="Output directory. Defaults to current directory.",
    )
    parser.add_argument(
        "--output-shape",
        choices=OUTPUT_SHAPES,
        default="both",
        help=(
            "Output shape: both = normalized DB files + wide review, "
            "db = normalized DB files only, wide = review file only."
        ),
    )
    parser.add_argument(
        "--view-counts-file",
        default=None,
        help=(
            "Optional external view-count CSV with url/threads_url and "
            "view_counts_value/view_count columns. Rows match raw items by username + code."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_prefix = args.output_prefix or infer_prefix(input_path)

    summary = run_extraction(
        input_path=input_path,
        output_prefix=output_prefix,
        output_dir=output_dir,
        output_shape=args.output_shape,
        view_counts_file=Path(args.view_counts_file) if args.view_counts_file else None,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
