"""External Threads view-count CSV parsing and matching helpers."""

import csv
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse


URL_COLUMNS = ("url", "threads_url")
VIEW_COUNT_COLUMNS = ("view_counts_value", "view_count")

PostKey = Tuple[str, str]


class ViewCountError(ValueError):
    """Raised when view-count input cannot be safely matched."""


def make_post_key(username: str, code: str) -> Optional[PostKey]:
    username = (username or "").strip().lower()
    code = (code or "").strip()
    if not username or not code:
        return None
    return (username, code)


def extract_post_key_from_url(url: str) -> Optional[PostKey]:
    raw_url = (url or "").strip()
    if not raw_url:
        return None
    parsed = urlparse(raw_url)
    path = unquote(parsed.path or raw_url)
    match = re.search(r"/@([^/?#]+)/post/([^/?#]+)", path)
    if not match:
        return None
    return make_post_key(match.group(1), match.group(2).rstrip("/"))


def pick_column(header: Sequence[str], candidates: Sequence[str], label: str) -> str:
    for candidate in candidates:
        if candidate in header:
            return candidate
    raise ViewCountError(
        f"View-count CSV is missing {label} column. Expected one of: {', '.join(candidates)}"
    )


def parse_view_count(value: str, row_label: str) -> Optional[int]:
    value = (value or "").strip()
    if value == "":
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ViewCountError(f"{row_label} has non-integer view count: {value!r}") from exc
    if parsed < 0:
        raise ViewCountError(f"{row_label} has negative view count: {value!r}")
    return parsed


def read_view_count_csv(path: Path) -> Tuple[Dict[PostKey, Optional[int]], Dict[str, Any]]:
    if not path.exists():
        raise ViewCountError(f"View-count CSV not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = list(reader.fieldnames or [])
        url_column = pick_column(header, URL_COLUMNS, "URL")
        count_column = pick_column(header, VIEW_COUNT_COLUMNS, "view count")
        values: Dict[PostKey, Optional[int]] = {}
        rows = 0
        available = 0
        unavailable = 0

        for row_index, row in enumerate(reader, start=2):
            rows += 1
            row_label = f"view-count row {row_index}"
            url = (row.get(url_column) or "").strip()
            key = extract_post_key_from_url(url)
            if key is None:
                raise ViewCountError(
                    f"{row_label} does not contain a /@username/post/code URL: {url!r}"
                )
            if key in values:
                username, code = key
                raise ViewCountError(
                    f"{row_label} duplicates view-count key: @{username}/post/{code}"
                )

            view_count = parse_view_count(row.get(count_column, ""), row_label)
            if view_count is None:
                unavailable += 1
            else:
                available += 1
            values[key] = view_count

    summary = {
        "external_view_count_file": str(path),
        "external_view_count_rows": rows,
        "external_view_count_available_items": available,
        "external_view_count_unavailable_rows": unavailable,
    }
    return values, summary


def apply_view_counts_to_items(
    items: List[Dict[str, Any]],
    view_counts_file: Path,
) -> Dict[str, Any]:
    view_counts_by_key, summary = read_view_count_csv(view_counts_file)
    item_by_key: Dict[PostKey, Dict[str, Any]] = {}

    for item in items:
        key = make_post_key(str(item.get("username", "") or ""), str(item.get("code", "") or ""))
        if key is None:
            continue
        if key in item_by_key:
            username, code = key
            raise ViewCountError(f"Raw export has duplicate post key: @{username}/post/{code}")
        item_by_key[key] = item

    unmatched_keys = sorted(set(view_counts_by_key) - set(item_by_key))
    if unmatched_keys:
        samples = [f"@{username}/post/{code}" for username, code in unmatched_keys[:10]]
        raise ViewCountError(
            "View-count CSV contains URLs not found in raw export: " + ", ".join(samples)
        )

    for key, view_count in view_counts_by_key.items():
        item_by_key[key]["view_count"] = view_count

    missing_items = len(set(item_by_key) - set(view_counts_by_key))
    summary.update(
        {
            "external_view_count_matched_items": len(view_counts_by_key),
            "external_view_count_unmatched_urls": len(unmatched_keys),
            "external_view_count_missing_items": missing_items,
        }
    )
    return summary
