"""Build media refresh/download queue rows for selected performance grades."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .media_cache import (
    item_pks_from_match_id,
    normalize_grade_token,
    read_csv_rows,
    source_url_hash,
    split_grade_tokens,
)


MEDIA_REFRESH_QUEUE_COLUMNS = [
    "export_key",
    "queue_id",
    "match_id",
    "performance_grade",
    "match_side",
    "item_pk",
    "media_index",
    "cache_role",
    "input_kind",
    "asset_type",
    "threads_url",
    "source_url_hash",
    "has_existing_source_url",
    "best_image_url_hash",
    "best_video_url_hash",
    "media_source",
    "media_pk",
    "media_id",
    "best_image_width",
    "best_image_height",
    "video_version_count",
    "original_width",
    "original_height",
    "has_audio",
    "priority",
    "queue_status",
    "created_at",
]

GRADE_PRIORITY = {
    "gold": 1,
    "s": 2,
    "a": 3,
    "b": 4,
}

MATCH_SIDE_ORDER = {
    "body": 0,
    "link": 1,
}

CACHE_ROLE_ORDER = {
    "preview": 0,
    "media": 1,
}


@dataclass(frozen=True)
class MediaRefreshQueueSummary:
    output_csv: Path
    performance_rows: int
    selected_matches: int
    source_media_assets: int
    rows: int
    duplicate_rows: int
    skipped_invalid_match_ids: int


def write_media_refresh_queue_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MEDIA_REFRESH_QUEUE_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {column: row.get(column, "") for column in MEDIA_REFRESH_QUEUE_COLUMNS}
            )


def selected_grade_set(values: Iterable[str]) -> set[str]:
    return {
        normalize_grade_token(grade)
        for grade in split_grade_tokens(values)
        if normalize_grade_token(grade)
    }


def priority_for_grade(grade: str) -> int:
    return GRADE_PRIORITY.get(normalize_grade_token(grade), 99)


def build_items_by_pk(items_rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    return {row.get("pk", ""): row for row in items_rows if row.get("pk", "")}


def build_media_by_item_pk(media_rows: Iterable[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    media_by_item: Dict[str, List[Dict[str, str]]] = {}
    for row in media_rows:
        item_pk = row.get("item_pk", "")
        if not item_pk:
            continue
        media_by_item.setdefault(item_pk, []).append(row)
    for rows in media_by_item.values():
        rows.sort(key=lambda row: safe_int(row.get("media_index") or "0"))
    return media_by_item


def safe_int(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return 0


def hash_or_blank(value: str) -> str:
    text = (value or "").strip()
    return source_url_hash(text) if text else ""


def planned_refresh_roles(media_row: Dict[str, str]) -> List[Tuple[str, str, str]]:
    asset_type = (media_row.get("asset_type") or "unknown").strip() or "unknown"
    best_image_url = (media_row.get("best_image_url") or "").strip()
    best_video_url = (media_row.get("best_video_url") or "").strip()
    planned: List[Tuple[str, str, str]] = []
    if asset_type == "video" or best_video_url:
        if best_image_url:
            planned.append(("preview", "image", best_image_url))
        planned.append(("media", "video", best_video_url))
        return planned
    planned.append(("media", "image", best_image_url))
    return planned


def fallback_threads_url(
    item_pk: str,
    match_side: str,
    item_rows_by_pk: Dict[str, Dict[str, str]],
    performance_row: Dict[str, str],
) -> str:
    item_threads_url = (item_rows_by_pk.get(item_pk, {}).get("threads_url") or "").strip()
    if item_threads_url:
        return item_threads_url
    fallback_key = "body_threads_url" if match_side == "body" else "link_threads_url"
    return (performance_row.get(fallback_key) or "").strip()


def build_queue_id(
    export_key: str,
    match_id: str,
    match_side: str,
    item_pk: str,
    media_index: str,
    cache_role: str,
) -> str:
    return f"{export_key}:{match_id}:{match_side}:{item_pk}:{media_index}:{cache_role}"


def build_queue_row(
    *,
    export_key: str,
    created_at: str,
    performance_row: Dict[str, str],
    match_side: str,
    item_pk: str,
    media_row: Dict[str, str],
    cache_role: str,
    input_kind: str,
    source_url: str,
    threads_url: str,
) -> Dict[str, Any]:
    match_id = performance_row.get("match_id", "")
    performance_grade = performance_row.get("performance_grade", "")
    media_index = media_row.get("media_index", "")
    best_image_url = media_row.get("best_image_url", "")
    best_video_url = media_row.get("best_video_url", "")
    source = (source_url or "").strip()
    return {
        "export_key": export_key,
        "queue_id": build_queue_id(
            export_key,
            match_id,
            match_side,
            item_pk,
            media_index,
            cache_role,
        ),
        "match_id": match_id,
        "performance_grade": performance_grade,
        "match_side": match_side,
        "item_pk": item_pk,
        "media_index": media_index,
        "cache_role": cache_role,
        "input_kind": input_kind,
        "asset_type": media_row.get("asset_type", ""),
        "threads_url": threads_url,
        "source_url_hash": hash_or_blank(source),
        "has_existing_source_url": "true" if source else "false",
        "best_image_url_hash": hash_or_blank(best_image_url),
        "best_video_url_hash": hash_or_blank(best_video_url),
        "media_source": media_row.get("media_source", ""),
        "media_pk": media_row.get("media_pk", ""),
        "media_id": media_row.get("media_id", ""),
        "best_image_width": media_row.get("best_image_width", ""),
        "best_image_height": media_row.get("best_image_height", ""),
        "video_version_count": media_row.get("video_version_count", ""),
        "original_width": media_row.get("original_width", ""),
        "original_height": media_row.get("original_height", ""),
        "has_audio": media_row.get("has_audio", ""),
        "priority": priority_for_grade(performance_grade),
        "queue_status": "queued",
        "created_at": created_at,
    }


def queue_sort_key(row: Dict[str, Any]) -> Tuple[int, str, int, str, int, str]:
    media_index = safe_int(str(row.get("media_index") or "0"))
    return (
        int(row.get("priority") or 99),
        str(row.get("match_id") or ""),
        MATCH_SIDE_ORDER.get(str(row.get("match_side") or ""), 99),
        str(row.get("item_pk") or ""),
        media_index,
        str(CACHE_ROLE_ORDER.get(str(row.get("cache_role") or ""), 99)),
    )


def build_media_refresh_queue_rows(
    *,
    export_key: str,
    performance_rows: Sequence[Dict[str, str]],
    media_rows: Sequence[Dict[str, str]],
    items_rows: Sequence[Dict[str, str]],
    performance_grades: Iterable[str],
    created_at: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int, int, int]:
    selected_grades = selected_grade_set(performance_grades)
    item_rows_by_pk = build_items_by_pk(items_rows)
    media_by_item_pk = build_media_by_item_pk(media_rows)
    timestamp = created_at or datetime.now(timezone.utc).isoformat()
    output_rows: List[Dict[str, Any]] = []
    selected_matches = 0
    skipped_invalid_match_ids = 0

    for performance_row in performance_rows:
        grade = performance_row.get("performance_grade", "")
        if normalize_grade_token(grade) not in selected_grades:
            continue
        item_pks = item_pks_from_match_id(performance_row.get("match_id", ""))
        if len(item_pks) != 2:
            skipped_invalid_match_ids += 1
            continue
        selected_matches += 1
        for match_side, item_pk in (("body", item_pks[0]), ("link", item_pks[1])):
            threads_url = fallback_threads_url(
                item_pk,
                match_side,
                item_rows_by_pk,
                performance_row,
            )
            for media_row in media_by_item_pk.get(item_pk, []):
                for cache_role, input_kind, source_url in planned_refresh_roles(media_row):
                    output_rows.append(
                        build_queue_row(
                            export_key=export_key,
                            created_at=timestamp,
                            performance_row=performance_row,
                            match_side=match_side,
                            item_pk=item_pk,
                            media_row=media_row,
                            cache_role=cache_role,
                            input_kind=input_kind,
                            source_url=source_url,
                            threads_url=threads_url,
                        )
                    )

    deduped: Dict[str, Dict[str, Any]] = {}
    duplicate_rows = 0
    for row in output_rows:
        queue_id = str(row.get("queue_id") or "")
        if queue_id in deduped:
            duplicate_rows += 1
            continue
        deduped[queue_id] = row
    rows = sorted(deduped.values(), key=queue_sort_key)
    return rows, selected_matches, duplicate_rows, skipped_invalid_match_ids


def prepare_media_refresh_queue(
    *,
    performance_labels_csv: Path,
    media_assets_csv: Path,
    items_core_csv: Path,
    output_csv: Path,
    export_key: str,
    performance_grades: Iterable[str],
    created_at: Optional[str] = None,
) -> MediaRefreshQueueSummary:
    performance_rows = read_csv_rows(performance_labels_csv)
    media_rows = read_csv_rows(media_assets_csv)
    items_rows = read_csv_rows(items_core_csv)
    rows, selected_matches, duplicate_rows, skipped_invalid_match_ids = (
        build_media_refresh_queue_rows(
            export_key=export_key,
            performance_rows=performance_rows,
            media_rows=media_rows,
            items_rows=items_rows,
            performance_grades=performance_grades,
            created_at=created_at,
        )
    )
    write_media_refresh_queue_csv(output_csv, rows)
    return MediaRefreshQueueSummary(
        output_csv=output_csv,
        performance_rows=len(performance_rows),
        selected_matches=selected_matches,
        source_media_assets=len(media_rows),
        rows=len(rows),
        duplicate_rows=duplicate_rows,
        skipped_invalid_match_ids=skipped_invalid_match_ids,
    )
