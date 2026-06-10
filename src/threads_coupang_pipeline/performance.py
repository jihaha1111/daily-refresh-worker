"""Performance labeling helpers for matched Coupang body/link records."""

from __future__ import annotations

from collections import Counter
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .content_classification import classify_match_content


STABLE_VIEW_THRESHOLD = 5000
POSITIVE_TRAINING_BODY_THRESHOLD = 12500
VIRAL_BODY_VIEW_PERCENTILE = 0.8

METRIC_VERSION_V2_REVENUE_PROXY = "performance_v2_revenue_proxy"

EXPECTED_CLICK_RATE = 0.10
EXPECTED_REVENUE_PER_CLICK_KRW = 100

GRADE_GOLD = "Gold"
GRADE_V2_S = "S"
GRADE_V2_A = "A"
GRADE_V2_B = "B"
GRADE_V2_C = "C"
GRADE_V2_REFERENCE = "excluded/reference"

TYPE_HIGH_CONVERSION = "전환형고성과"
TYPE_CONVERSION_CANDIDATE = "전환형후보"
TYPE_LOW_CONVERSION = "저전환형"
TYPE_VIRAL = "바이럴형"
TYPE_REFERENCE = "제외/참고"

BASE_LABEL_FIELDS = (
    "match_id",
    "match_confidence",
    "first_coupang_url",
    "body_username",
    "body_taken_at_iso",
    "body_text",
    "body_threads_url",
    "body_view_count",
    "body_like_count",
    "body_direct_reply_count",
    "body_repost_count",
    "body_reshare_count",
    "body_quote_count",
    "link_username",
    "link_taken_at_iso",
    "link_text",
    "link_threads_url",
    "link_coupang_urls",
    "link_first_coupang_url",
    "link_view_count",
    "link_like_count",
    "link_direct_reply_count",
    "link_repost_count",
    "link_reshare_count",
    "link_quote_count",
)

COMPUTED_LABEL_FIELDS = (
    "metric_version",
    "content_category",
    "is_recipe",
    "recipe_confidence",
    "link_view_rate",
    "body_engagement",
    "body_engagement_rate",
    "expected_clicks",
    "expected_revenue_krw",
    "exposure_tier",
    "performance_grade",
    "performance_type",
    "learning_segment",
    "coupang_score",
)

PERFORMANCE_LABEL_FIELDS = BASE_LABEL_FIELDS + COMPUTED_LABEL_FIELDS

MANUAL_TAGGING_FIELDS = (
    "category_tag",
    "primary_hook_type",
    "secondary_hook_types",
    "product_name_exposed",
    "price_exposed",
    "curiosity_score",
    "link_need_score",
    "body_link_fit_score",
    "ad_smell_score",
    "risk_score",
    "review_note",
)

TAGGING_SAMPLE_FIELDS = (
    "sample_bucket",
    "sample_reason",
) + PERFORMANCE_LABEL_FIELDS + MANUAL_TAGGING_FIELDS


def parse_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text == "" or text.lower() in {"none", "null"}:
        return None
    return int(text.replace(",", ""))


def round_metric(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(value, 6)


def link_view_rate(
    body_view_count: Optional[int],
    link_view_count: Optional[int],
) -> Optional[float]:
    if body_view_count is None or link_view_count is None:
        return None
    if body_view_count <= 0:
        return None
    return link_view_count / body_view_count


def body_engagement(
    like_count: Optional[int],
    direct_reply_count: Optional[int],
    repost_count: Optional[int],
    reshare_count: Optional[int],
    quote_count: Optional[int],
) -> int:
    return (
        (like_count or 0)
        + (direct_reply_count or 0) * 3
        + (repost_count or 0) * 5
        + (reshare_count or 0) * 6
        + (quote_count or 0) * 4
    )


def body_engagement_rate(
    engagement: int,
    body_view_count: Optional[int],
) -> Optional[float]:
    if body_view_count is None or body_view_count <= 0:
        return None
    return engagement / body_view_count


def expected_clicks(link_view_count: Optional[int]) -> Optional[float]:
    if link_view_count is None:
        return None
    return link_view_count * EXPECTED_CLICK_RATE


def expected_revenue_krw(link_view_count: Optional[int]) -> Optional[int]:
    if link_view_count is None:
        return None
    return int(round(link_view_count * EXPECTED_CLICK_RATE * EXPECTED_REVENUE_PER_CLICK_KRW))


def exposure_tier(body_view_count: Optional[int]) -> str:
    if body_view_count is None:
        return "unknown"
    if body_view_count < 2000:
        return "reference"
    if body_view_count < STABLE_VIEW_THRESHOLD:
        return "discovery"
    if body_view_count < POSITIVE_TRAINING_BODY_THRESHOLD:
        return "stable"
    if body_view_count < 30000:
        return "high"
    if body_view_count < 80000:
        return "scale"
    return "mega"


def performance_grade(
    body_view_count: Optional[int],
    link_view_count: Optional[int],
    rate: Optional[float],
    stable_view_threshold: int = STABLE_VIEW_THRESHOLD,
) -> str:
    revenue = expected_revenue_krw(link_view_count)
    if body_view_count is None or link_view_count is None or rate is None or revenue is None:
        return GRADE_V2_REFERENCE
    if body_view_count < stable_view_threshold:
        return GRADE_V2_REFERENCE
    if revenue >= 100000 and rate >= 0.10:
        return GRADE_GOLD
    if revenue >= 60000 and rate >= 0.05:
        return GRADE_V2_S
    if revenue >= 20000 and rate >= 0.08:
        return GRADE_V2_A
    if revenue >= 5000 and rate >= 0.05:
        return GRADE_V2_B
    return GRADE_V2_C


def learning_segment(
    body_view_count: Optional[int],
    link_view_count: Optional[int],
    rate: Optional[float],
    grade: str,
    positive_training_body_threshold: int = POSITIVE_TRAINING_BODY_THRESHOLD,
) -> str:
    revenue = expected_revenue_krw(link_view_count)
    if body_view_count is None or link_view_count is None or rate is None or revenue is None:
        return "reference"
    if grade == GRADE_GOLD:
        return "gold_high_performance"
    if grade in {GRADE_V2_S, GRADE_V2_A} and body_view_count >= positive_training_body_threshold:
        return "target_high_performance"
    if grade == GRADE_V2_A:
        return "strong_candidate"
    if grade == GRADE_V2_B:
        return "conversion_candidate"
    if 2000 <= body_view_count < STABLE_VIEW_THRESHOLD and rate >= 0.10 and revenue >= 3000:
        return "discovery_high_conversion"
    if revenue >= 50000 and rate < 0.05:
        return "viral_low_conversion"
    if grade == GRADE_V2_C:
        return "stable_low"
    return "reference"


def performance_type(learning_segment_value: str, grade: str) -> str:
    if learning_segment_value in {"gold_high_performance", "target_high_performance", "strong_candidate"}:
        return TYPE_HIGH_CONVERSION
    if learning_segment_value == "conversion_candidate":
        return TYPE_CONVERSION_CANDIDATE
    if learning_segment_value == "viral_low_conversion":
        return TYPE_VIRAL
    if grade == GRADE_V2_C:
        return TYPE_LOW_CONVERSION
    return TYPE_REFERENCE


def percentile_ranks(values: Sequence[Tuple[int, float]]) -> Dict[int, float]:
    if not values:
        return {}
    if len(values) == 1:
        return {values[0][0]: 1.0}

    sorted_values = sorted(values, key=lambda x: x[1])
    denominator = len(sorted_values) - 1
    ranks: Dict[int, float] = {}
    start = 0

    while start < len(sorted_values):
        end = start
        while end + 1 < len(sorted_values) and sorted_values[end + 1][1] == sorted_values[start][1]:
            end += 1

        percentile = ((start + end) / 2) / denominator
        for position in range(start, end + 1):
            index, _ = sorted_values[position]
            ranks[index] = percentile
        start = end + 1

    return ranks


def build_performance_labels(
    rows: Iterable[Mapping[str, Any]],
    stable_view_threshold: int = STABLE_VIEW_THRESHOLD,
    positive_training_body_threshold: int = POSITIVE_TRAINING_BODY_THRESHOLD,
) -> List[Dict[str, Any]]:
    labels: List[Dict[str, Any]] = []

    for row in rows:
        body_view_count = parse_optional_int(row.get("body_view_count"))
        link_view_count = parse_optional_int(row.get("link_view_count"))
        body_like_count = parse_optional_int(row.get("body_like_count"))
        body_direct_reply_count = parse_optional_int(row.get("body_direct_reply_count"))
        body_repost_count = parse_optional_int(row.get("body_repost_count"))
        body_reshare_count = parse_optional_int(row.get("body_reshare_count"))
        body_quote_count = parse_optional_int(row.get("body_quote_count"))

        rate = link_view_rate(body_view_count, link_view_count)
        engagement = body_engagement(
            body_like_count,
            body_direct_reply_count,
            body_repost_count,
            body_reshare_count,
            body_quote_count,
        )
        engagement_rate = body_engagement_rate(engagement, body_view_count)

        classification = classify_match_content(row)
        expected_clicks_value = expected_clicks(link_view_count)
        expected_revenue_value = expected_revenue_krw(link_view_count)
        grade = performance_grade(
            body_view_count,
            link_view_count,
            rate,
            stable_view_threshold=stable_view_threshold,
        )
        segment = learning_segment(
            body_view_count,
            link_view_count,
            rate,
            grade,
            positive_training_body_threshold=positive_training_body_threshold,
        )
        initial_performance_type = performance_type(segment, grade)

        label: Dict[str, Any] = {
            field: row.get(field, "")
            for field in BASE_LABEL_FIELDS
        }
        label.update(
            {
                "metric_version": METRIC_VERSION_V2_REVENUE_PROXY,
                "content_category": classification.content_category,
                "is_recipe": classification.is_recipe,
                "recipe_confidence": classification.recipe_confidence,
                "body_view_count": body_view_count,
                "body_like_count": body_like_count,
                "body_direct_reply_count": body_direct_reply_count,
                "body_repost_count": body_repost_count,
                "body_reshare_count": body_reshare_count,
                "body_quote_count": body_quote_count,
                "link_view_count": link_view_count,
                "link_like_count": parse_optional_int(row.get("link_like_count")),
                "link_direct_reply_count": parse_optional_int(row.get("link_direct_reply_count")),
                "link_repost_count": parse_optional_int(row.get("link_repost_count")),
                "link_reshare_count": parse_optional_int(row.get("link_reshare_count")),
                "link_quote_count": parse_optional_int(row.get("link_quote_count")),
                "link_view_rate": round_metric(rate),
                "body_engagement": engagement,
                "body_engagement_rate": round_metric(engagement_rate),
                "expected_clicks": round_metric(expected_clicks_value),
                "expected_revenue_krw": expected_revenue_value,
                "exposure_tier": exposure_tier(body_view_count),
                "performance_grade": grade,
                "performance_type": initial_performance_type,
                "learning_segment": segment,
                "coupang_score": None,
                "_raw_link_view_rate": rate,
                "_raw_body_engagement_rate": engagement_rate,
            }
        )
        labels.append(label)

    stable_indexes = [
        index
        for index, label in enumerate(labels)
        if (label.get("body_view_count") or 0) >= stable_view_threshold
    ]
    score_eligible_indexes = [
        index
        for index in stable_indexes
        if label_has_score_inputs(labels[index])
    ]

    body_view_percentiles = percentile_ranks(
        [
            (index, float(labels[index]["body_view_count"]))
            for index in stable_indexes
            if labels[index].get("body_view_count") is not None
        ]
    )
    link_view_percentiles = percentile_ranks(
        [
            (index, float(labels[index]["link_view_count"]))
            for index in score_eligible_indexes
        ]
    )
    link_rate_percentiles = percentile_ranks(
        [
            (index, float(labels[index]["_raw_link_view_rate"]))
            for index in score_eligible_indexes
        ]
    )
    engagement_rate_percentiles = percentile_ranks(
        [
            (index, float(labels[index]["_raw_body_engagement_rate"]))
            for index in score_eligible_indexes
        ]
    )

    for index, label in enumerate(labels):
        score_parts = (
            link_view_percentiles.get(index),
            link_rate_percentiles.get(index),
            body_view_percentiles.get(index),
            engagement_rate_percentiles.get(index),
        )
        if all(part is not None for part in score_parts):
            link_view_pct, link_rate_pct, body_view_pct, engagement_pct = score_parts
            score = (
                0.40 * link_view_pct
                + 0.35 * link_rate_pct
                + 0.15 * body_view_pct
                + 0.10 * engagement_pct
            )
            label["coupang_score"] = round_metric(score)

        label.pop("_raw_link_view_rate", None)
        label.pop("_raw_body_engagement_rate", None)

    return labels


def label_has_score_inputs(label: Mapping[str, Any]) -> bool:
    return (
        label.get("body_view_count") is not None
        and label.get("link_view_count") is not None
        and label.get("_raw_link_view_rate") is not None
        and label.get("_raw_body_engagement_rate") is not None
    )


def grade_counts(labels: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    return dict(Counter(str(label.get("performance_grade", "")) for label in labels))


def type_counts(labels: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    return dict(Counter(str(label.get("performance_type", "")) for label in labels))


def sort_value(value: Any, default: float = -1.0) -> float:
    if value is None or value == "":
        return default
    return float(value)


def select_tagging_sample(
    labels: Sequence[Mapping[str, Any]],
    sample_size: int = 150,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if sample_size <= 0:
        return [], {"requested_sample_size": sample_size, "selected_sample_size": 0}

    high_performance_grades = {GRADE_GOLD, GRADE_V2_S, GRADE_V2_A}
    b_grades = {GRADE_V2_B}
    c_grades = {GRADE_V2_C}
    reference_grades = {GRADE_V2_REFERENCE}

    buckets: List[Tuple[str, str, Callable[[Mapping[str, Any]], bool], Callable[[Mapping[str, Any]], Any]]] = [
        (
            "s_a_candidate",
            "S/A grade rows with strong link movement.",
            lambda row: row.get("performance_grade") in high_performance_grades,
            lambda row: (
                -sort_value(row.get("coupang_score")),
                -sort_value(row.get("link_view_rate")),
                -sort_value(row.get("link_view_count")),
            ),
        ),
        (
            "viral",
            "High body views with low link_view_rate.",
            lambda row: row.get("performance_type") == TYPE_VIRAL,
            lambda row: (-sort_value(row.get("body_view_count")), sort_value(row.get("link_view_rate"), 1.0)),
        ),
        (
            "c_grade",
            "Stable body views but low conversion.",
            lambda row: row.get("performance_grade") in c_grades and row.get("performance_type") != TYPE_VIRAL,
            lambda row: (sort_value(row.get("link_view_rate"), 1.0), -sort_value(row.get("body_view_count"))),
        ),
        (
            "ambiguous_mid",
            "Middle or boundary rows for refining tag criteria.",
            lambda row: row.get("performance_grade") in b_grades,
            lambda row: (abs(sort_value(row.get("coupang_score"), 0.5) - 0.5), -sort_value(row.get("body_view_count"))),
        ),
        (
            "reference",
            "Below stable threshold or missing core view counts.",
            lambda row: row.get("performance_grade") in reference_grades,
            lambda row: (-sort_value(row.get("body_view_count")), -sort_value(row.get("link_view_count"))),
        ),
    ]

    quota_base = sample_size // len(buckets)
    quota_remainder = sample_size % len(buckets)
    quotas = {
        bucket[0]: quota_base + (1 if index < quota_remainder else 0)
        for index, bucket in enumerate(buckets)
    }

    selected: List[Dict[str, Any]] = []
    selected_ids = set()
    bucket_counts: Dict[str, int] = {}
    bucket_shortages: Dict[str, int] = {}

    for bucket_name, reason, predicate, sort_key in buckets:
        quota = quotas[bucket_name]
        candidates = [
            row
            for row in labels
            if str(row.get("match_id", "")) not in selected_ids and predicate(row)
        ]
        candidates = sorted(candidates, key=sort_key)
        picked = candidates[:quota]
        bucket_counts[bucket_name] = len(picked)
        if len(picked) < quota:
            bucket_shortages[bucket_name] = quota - len(picked)
        for row in picked:
            selected.append(make_tagging_sample_row(row, bucket_name, reason))
            selected_ids.add(str(row.get("match_id", "")))

    if len(selected) < sample_size:
        remaining = [
            row
            for row in labels
            if str(row.get("match_id", "")) not in selected_ids
        ]
        remaining = sorted(
            remaining,
            key=lambda row: (
                -sort_value(row.get("coupang_score")),
                -sort_value(row.get("body_view_count")),
                -sort_value(row.get("link_view_count")),
            ),
        )
        fill_count = min(sample_size - len(selected), len(remaining))
        for row in remaining[:fill_count]:
            selected.append(
                make_tagging_sample_row(
                    row,
                    "fill_remaining",
                    "Filled because one or more priority buckets had too few rows.",
                )
            )
            selected_ids.add(str(row.get("match_id", "")))
        bucket_counts["fill_remaining"] = fill_count

    summary = {
        "requested_sample_size": sample_size,
        "selected_sample_size": len(selected),
        "bucket_quotas": quotas,
        "bucket_counts": bucket_counts,
        "bucket_shortages": bucket_shortages,
        "unfilled_slots": max(0, sample_size - len(selected)),
    }
    return selected, summary


def make_tagging_sample_row(
    label: Mapping[str, Any],
    bucket_name: str,
    reason: str,
) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "sample_bucket": bucket_name,
        "sample_reason": reason,
    }
    for field in PERFORMANCE_LABEL_FIELDS:
        row[field] = label.get(field)
    for field in MANUAL_TAGGING_FIELDS:
        row[field] = ""
    return row
