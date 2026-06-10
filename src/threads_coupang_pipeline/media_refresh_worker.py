"""Media refresh/download worker for queued private media cache targets."""

from __future__ import annotations

import base64
import html
import json
import re
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .media_cache import (
    CACHE_STATUS_CACHED,
    CACHE_STATUS_DOWNLOAD_FAILED,
    CACHE_STATUS_MISSING_SOURCE_URL,
    build_missing_row,
    cache_one_media,
    read_csv_rows,
    write_media_cache_csv,
)
from .media_cache_drive import push_media_cache_to_drive
from .media_refresh_queue import queue_sort_key


THREADS_URL_RE = re.compile(r"https?(?::|\\u003a)(?://|\\/\\/)[^\"'<>\s]+")
STP_DIMENSION_RE = re.compile(r"(?:^|[_-])[ps](\d+)x(\d+)(?:_|$)")
WIDTH_FIELD_RE = re.compile(r"(?:\\?[\"'])?width(?:\\?[\"'])?\s*:\s*\\?[\"']?(\d+)")
HEIGHT_FIELD_RE = re.compile(r"(?:\\?[\"'])?height(?:\\?[\"'])?\s*:\s*\\?[\"']?(\d+)")
CANDIDATE_DIMENSION_CONTEXT_CHARS = 800
MIN_REFRESHED_IMAGE_BYTES = 10 * 1024
MIN_REFRESHED_IMAGE_DIMENSION = 128

CacheFunc = Callable[..., Dict[str, Any]]
FetchFunc = Callable[..., str]


@dataclass(frozen=True)
class MediaRefreshWorkerSummary:
    export_key: str
    queue_rows: int
    processed_rows: int
    max_rows: int
    max_parallel: int
    fetch_attempts: int
    upload_to_drive: bool
    output_csv: Path
    status_counts: Dict[str, int]
    cached_rows: int
    refreshed_cached_rows: int
    direct_cached_rows: int
    missing_source_rows: int
    download_failed_rows: int
    limit_applied: bool
    drive_sync: Optional[Dict[str, Any]]


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_candidate_url(value: str) -> str:
    text = html.unescape(value or "")
    text = text.replace("\\/", "/")
    text = text.replace("\\u0025", "%")
    text = text.replace("\\u0026", "&")
    text = text.replace("\\u003d", "=")
    text = text.replace("\\u003f", "?")
    text = text.replace("\\u003a", ":")
    text = text.rstrip(".,);]")
    return text


def repeated_unquote(value: str, *, limit: int = 3) -> str:
    text = value or ""
    for _ in range(limit):
        decoded = urllib.parse.unquote(text)
        if decoded == text:
            break
        text = decoded
    return text


def media_pk_from_ig_cache_key(url: str) -> str:
    parsed = urllib.parse.urlparse(normalize_candidate_url(url))
    values = urllib.parse.parse_qs(parsed.query).get("ig_cache_key") or []
    if not values:
        return ""
    cache_key = repeated_unquote(values[0]).split(".", 1)[0]
    if not cache_key:
        return ""
    try:
        padded = cache_key + "=" * ((4 - len(cache_key) % 4) % 4)
        return base64.b64decode(padded).decode("utf-8", errors="replace")
    except Exception:
        return ""


def refreshed_sources_by_media_pk(urls: Sequence[str]) -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = {}
    for url in urls:
        media_pk = media_pk_from_ig_cache_key(url)
        if not media_pk:
            continue
        grouped.setdefault(media_pk, []).append(url)
    return grouped


def candidate_stp_dimensions(url: str) -> Tuple[int, int]:
    parsed = urllib.parse.urlparse(normalize_candidate_url(url))
    stp_values = urllib.parse.parse_qs(parsed.query).get("stp") or []
    best_width = 0
    best_height = 0
    for stp in stp_values:
        for match in STP_DIMENSION_RE.finditer(repeated_unquote(stp)):
            width = int(match.group(1))
            height = int(match.group(2))
            if width * height > best_width * best_height:
                best_width = width
                best_height = height
    return best_width, best_height


def candidate_context(body: str, start: int, end: int) -> Tuple[str, int]:
    object_start = body.rfind("{", 0, start)
    object_end = body.find("}", end)
    if object_start != -1 and object_end != -1:
        object_end += 1
        if object_start < start and object_end > end and object_end - object_start <= 3000:
            return body[object_start:object_end], object_start

    context_start = max(0, start - CANDIDATE_DIMENSION_CONTEXT_CHARS)
    context_end = min(len(body), end + CANDIDATE_DIMENSION_CONTEXT_CHARS)
    return body[context_start:context_end], context_start


def nearest_int_field(pattern: re.Pattern[str], context: str, target_offset: int) -> int:
    matches = list(pattern.finditer(context))
    if not matches:
        return 0
    nearest = min(matches, key=lambda match: abs(match.start() - target_offset))
    try:
        return int(nearest.group(1))
    except (TypeError, ValueError):
        return 0


def candidate_context_dimensions(body: str, start: int, end: int) -> Tuple[int, int]:
    context, context_start = candidate_context(body, start, end)
    context = html.unescape(context)
    target_offset = max(0, start - context_start)
    width = nearest_int_field(WIDTH_FIELD_RE, context, target_offset)
    height = nearest_int_field(HEIGHT_FIELD_RE, context, target_offset)
    if width > 0 and height > 0:
        return width, height
    return 0, 0


def iter_media_candidate_matches(body: str, input_kind: str) -> Iterable[Tuple[str, int, int]]:
    seen = set()
    for match in THREADS_URL_RE.finditer(body or ""):
        url = normalize_candidate_url(match.group(0))
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            continue
        lower = url.lower()
        if input_kind == "video":
            is_match = ".mp4" in lower or "video" in lower
        else:
            is_match = any(ext in lower for ext in (".jpg", ".jpeg", ".png", ".webp"))
        if (
            not is_match
            or url in seen
            or (input_kind != "video" and is_ignored_image_candidate_url(url))
        ):
            continue
        seen.add(url)
        yield url, match.start(), match.end()


def extract_media_candidate_dimensions(body: str, input_kind: str) -> Dict[str, Tuple[int, int]]:
    dimensions: Dict[str, Tuple[int, int]] = {}
    for url, start, end in iter_media_candidate_matches(body, input_kind):
        width, height = candidate_context_dimensions(body, start, end)
        if width > 0 and height > 0:
            dimensions.setdefault(url, (width, height))
    return dimensions


def refreshed_source_quality_key(
    index_and_url: Tuple[int, str],
    candidate_dimensions_by_url: Optional[Dict[str, Tuple[int, int]]] = None,
) -> Tuple[int, int, int, int, int]:
    index, url = index_and_url
    parsed = urllib.parse.urlparse(normalize_candidate_url(url))
    query = urllib.parse.parse_qs(parsed.query)
    has_resize = bool(query.get("stp"))
    metadata_width, metadata_height = (0, 0)
    if candidate_dimensions_by_url:
        metadata_width, metadata_height = candidate_dimensions_by_url.get(url, (0, 0))
    stp_width, stp_height = candidate_stp_dimensions(url)
    metadata_area = metadata_width * metadata_height
    stp_area = stp_width * stp_height
    original_rank = 1 if not has_resize else 0
    metadata_rank = 1 if metadata_area > 0 else 0
    return metadata_rank, metadata_area, original_rank, stp_area, -index


def sort_refreshed_sources_by_quality(
    urls: Sequence[str],
    candidate_dimensions_by_url: Optional[Dict[str, Tuple[int, int]]] = None,
) -> List[str]:
    indexed = list(enumerate(urls))
    return [
        url
        for _index, url in sorted(
            indexed,
            key=lambda item: refreshed_source_quality_key(item, candidate_dimensions_by_url),
            reverse=True,
        )
    ]


def refreshed_source_groups_for_fallback(
    urls: Sequence[str],
    candidate_dimensions_by_url: Optional[Dict[str, Tuple[int, int]]] = None,
) -> List[Tuple[str, List[str]]]:
    by_media_pk = refreshed_sources_by_media_pk(urls)
    if by_media_pk:
        ordered_media_pks: List[str] = []
        for url in urls:
            media_pk = media_pk_from_ig_cache_key(url)
            if media_pk and media_pk not in ordered_media_pks:
                ordered_media_pks.append(media_pk)
        return [
            (
                media_pk,
                sort_refreshed_sources_by_quality(
                    by_media_pk[media_pk],
                    candidate_dimensions_by_url,
                ),
            )
            for media_pk in ordered_media_pks
        ]
    return [
        ("", [url])
        for url in urls
    ]


def is_ignored_image_candidate_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if "static.xx.fbcdn.net" in host or "static.cdninstagram.com" in host:
        return True
    if "/rsrc.php" in path:
        return True
    return False


def extract_media_candidates(body: str, input_kind: str) -> List[str]:
    return [
        url
        for url, _start, _end in iter_media_candidate_matches(body, input_kind)
    ]


def fetch_threads_body(
    threads_url: str,
    *,
    timeout: float,
    user_agent: str = "Mozilla/5.0",
) -> str:
    if not threads_url:
        return ""
    request = urllib.request.Request(
        threads_url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read(5 * 1024 * 1024)
            encoding = response.headers.get_content_charset() or "utf-8"
            return raw.decode(encoding, errors="replace")
    except Exception:
        return ""


def source_url_for_queue_row(
    queue_row: Dict[str, str],
    media_assets_by_key: Dict[Tuple[str, str], Dict[str, str]],
) -> str:
    key = (queue_row.get("item_pk", ""), queue_row.get("media_index", ""))
    media_row = media_assets_by_key.get(key, {})
    input_kind = queue_row.get("input_kind", "")
    if input_kind == "video":
        return (media_row.get("best_video_url") or "").strip()
    return (media_row.get("best_image_url") or "").strip()


def media_asset_for_queue_row(
    queue_row: Dict[str, str],
    media_assets_by_key: Dict[Tuple[str, str], Dict[str, str]],
) -> Dict[str, str]:
    key = (queue_row.get("item_pk", ""), queue_row.get("media_index", ""))
    return media_assets_by_key.get(key, {})


def build_planned_from_queue_row(
    queue_row: Dict[str, str],
    source_url: str,
    media_asset: Dict[str, str],
) -> Dict[str, str]:
    return {
        "item_pk": queue_row.get("item_pk", ""),
        "media_index": queue_row.get("media_index", ""),
        "cache_role": queue_row.get("cache_role", ""),
        "asset_type": queue_row.get("asset_type", "") or media_asset.get("asset_type", ""),
        "input_kind": queue_row.get("input_kind", ""),
        "source_url": source_url,
    }


def cache_url_with_attempts(
    *,
    export_key: str,
    queue_row: Dict[str, str],
    source_url: str,
    media_asset: Dict[str, str],
    cache_root: Path,
    timeout: float,
    max_bytes: Optional[int],
    fetch_attempts: int,
    cache_func: CacheFunc = cache_one_media,
) -> Dict[str, Any]:
    attempts = max(fetch_attempts, 1)
    planned = build_planned_from_queue_row(queue_row, source_url, media_asset)
    result: Dict[str, Any] = {}
    for attempt in range(attempts):
        result = cache_func(
            export_key,
            planned,
            cache_root,
            timeout=timeout,
            max_bytes=max_bytes,
            force=False,
        )
        if result.get("cache_status") != CACHE_STATUS_DOWNLOAD_FAILED:
            return result
        if attempt + 1 < attempts:
            time.sleep(0.1)
    return result


def cached_refreshed_image_quality_error(row: Dict[str, Any]) -> str:
    if row.get("cache_status") != CACHE_STATUS_CACHED or row.get("input_kind") != "image":
        return ""
    try:
        byte_size = int(row.get("byte_size") or 0)
    except (TypeError, ValueError):
        return "refreshed image is missing byte_size"
    if byte_size < MIN_REFRESHED_IMAGE_BYTES:
        return (
            "refreshed image below minimum byte_size "
            f"({byte_size} < {MIN_REFRESHED_IMAGE_BYTES})"
        )
    width_raw = row.get("width", "")
    height_raw = row.get("height", "")
    if width_raw and height_raw:
        try:
            width = int(width_raw)
            height = int(height_raw)
        except (TypeError, ValueError):
            return "refreshed image has invalid dimensions"
        if min(width, height) < MIN_REFRESHED_IMAGE_DIMENSION:
            return (
                "refreshed image below minimum dimension "
                f"({width}x{height}, min {MIN_REFRESHED_IMAGE_DIMENSION})"
            )
    return ""


def remove_cached_file(cache_root: Path, row: Dict[str, Any]) -> None:
    rel_path = str(row.get("cache_rel_path") or "")
    if not rel_path:
        return
    path = Path(rel_path)
    if path.is_absolute() or ".." in path.parts:
        return
    (cache_root / path).unlink(missing_ok=True)


def refreshed_source_candidate(
    queue_row: Dict[str, str],
    *,
    timeout: float,
    fetch_func: FetchFunc = fetch_threads_body,
) -> List[str]:
    candidates, _dimensions = refreshed_source_candidates_with_dimensions(
        queue_row,
        timeout=timeout,
        fetch_func=fetch_func,
    )
    return candidates


def refreshed_source_candidates_with_dimensions(
    queue_row: Dict[str, str],
    *,
    timeout: float,
    fetch_func: FetchFunc = fetch_threads_body,
) -> Tuple[List[str], Dict[str, Tuple[int, int]]]:
    threads_url = queue_row.get("threads_url", "")
    body = fetch_func(threads_url, timeout=timeout)
    if not body:
        return [], {}
    input_kind = queue_row.get("input_kind", "")
    return (
        extract_media_candidates(body, input_kind),
        extract_media_candidate_dimensions(body, input_kind),
    )


def queue_row_target_identity(row: Dict[str, str]) -> Tuple[str, str, str]:
    return (
        row.get("media_index", ""),
        row.get("cache_role", ""),
        row.get("input_kind", ""),
    )


def queue_row_group_key(row: Dict[str, str]) -> Tuple[str, str, str]:
    return (
        row.get("threads_url", ""),
        row.get("item_pk", ""),
        row.get("input_kind", ""),
    )


def media_index_sort_key(value: str) -> Tuple[int, Any]:
    text = str(value or "")
    try:
        return (0, int(text))
    except ValueError:
        return (1, text)


def content_key(row: Dict[str, Any]) -> str:
    if row.get("cache_status") != CACHE_STATUS_CACHED:
        return ""
    return str(row.get("content_sha256") or "")


def reuse_duplicate_cached_content(
    *,
    rows: Sequence[Dict[str, Any]],
    cache_root: Path,
) -> List[Dict[str, Any]]:
    canonical_rel_path_by_hash: Dict[str, str] = {}
    output: List[Dict[str, Any]] = []
    for row in rows:
        result = dict(row)
        key = content_key(result)
        rel_path = str(result.get("cache_rel_path") or "")
        if key and rel_path:
            canonical_rel_path = canonical_rel_path_by_hash.get(key)
            if canonical_rel_path:
                if rel_path != canonical_rel_path:
                    remove_cached_file(cache_root, result)
                result["cache_rel_path"] = canonical_rel_path
            else:
                canonical_rel_path_by_hash[key] = rel_path
        output.append(result)
    return output


def should_dedupe_refreshed_content(row: Dict[str, Any]) -> bool:
    return row.get("cache_status") == CACHE_STATUS_CACHED and row.get("input_kind") == "image"


def media_pk_for_target(row: Dict[str, str], media_assets_by_key: Dict[Tuple[str, str], Dict[str, str]]) -> str:
    media_asset = media_asset_for_queue_row(row, media_assets_by_key)
    return str(media_asset.get("media_pk") or "")


def refreshed_target_requires_keyed_media_pk(
    row: Dict[str, str],
    media_asset: Dict[str, str],
) -> bool:
    asset_type = row.get("asset_type", "") or media_asset.get("asset_type", "")
    return (
        asset_type == "video"
        and row.get("cache_role") == "preview"
        and row.get("input_kind") == "image"
    )


def try_refreshed_sources_for_target(
    *,
    export_key: str,
    row: Dict[str, str],
    media_asset: Dict[str, str],
    target_sources: Sequence[str],
    used_image_content: set,
    cache_root: Path,
    timeout: float,
    max_bytes: Optional[int],
    fetch_attempts: int,
    cache_func: CacheFunc,
) -> Tuple[Optional[Dict[str, Any]], bool, int]:
    last_result: Optional[Dict[str, Any]] = None
    for source_index, refreshed_source in enumerate(target_sources):
        refreshed_result = cache_url_with_attempts(
            export_key=export_key,
            queue_row=row,
            source_url=refreshed_source,
            media_asset=media_asset,
            cache_root=cache_root,
            timeout=timeout,
            max_bytes=max_bytes,
            fetch_attempts=fetch_attempts,
            cache_func=cache_func,
        )
        if refreshed_result.get("cache_status") != CACHE_STATUS_CACHED:
            last_result = refreshed_result
            continue

        quality_error = cached_refreshed_image_quality_error(refreshed_result)
        if quality_error:
            remove_cached_file(cache_root, refreshed_result)
            last_result = build_missing_row(
                export_key,
                build_planned_from_queue_row(row, refreshed_source, media_asset),
                CACHE_STATUS_DOWNLOAD_FAILED,
                quality_error,
            )
            continue

        if should_dedupe_refreshed_content(refreshed_result):
            key = content_key(refreshed_result)
            if key and key in used_image_content:
                remove_cached_file(cache_root, refreshed_result)
                continue
            if key:
                used_image_content.add(key)

        return refreshed_result, True, source_index + 1
    return last_result, False, 0


def process_queue_group(
    *,
    export_key: str,
    indexed_rows: Sequence[Tuple[int, Dict[str, str]]],
    media_assets_by_key: Dict[Tuple[str, str], Dict[str, str]],
    cache_root: Path,
    timeout: float,
    max_bytes: Optional[int],
    fetch_attempts: int,
    cache_func: CacheFunc = cache_one_media,
    fetch_func: FetchFunc = fetch_threads_body,
) -> List[Tuple[int, Dict[str, Any], bool]]:
    if not indexed_rows:
        return []

    target_representatives: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    target_rows: Dict[Tuple[str, str, str], List[Tuple[int, Dict[str, str]]]] = {}
    for index, row in indexed_rows:
        target = queue_row_target_identity(row)
        target_representatives.setdefault(target, row)
        target_rows.setdefault(target, []).append((index, row))

    ordered_targets = sorted(
        target_representatives,
        key=lambda target: (
            media_index_sort_key(target[0]),
            target[1],
            target[2],
        ),
    )

    target_results: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    target_used_refresh: Dict[Tuple[str, str, str], bool] = {}
    pending_targets: List[Tuple[str, str, str]] = []
    used_image_content = set()

    for target in ordered_targets:
        row = target_representatives[target]
        media_asset = media_asset_for_queue_row(row, media_assets_by_key)
        direct_source = source_url_for_queue_row(row, media_assets_by_key)
        if direct_source:
            direct_result = cache_url_with_attempts(
                export_key=export_key,
                queue_row=row,
                source_url=direct_source,
                media_asset=media_asset,
                cache_root=cache_root,
                timeout=timeout,
                max_bytes=max_bytes,
                fetch_attempts=fetch_attempts,
                cache_func=cache_func,
            )
            if direct_result.get("cache_status") == CACHE_STATUS_CACHED:
                target_results[target] = direct_result
                target_used_refresh[target] = False
                if should_dedupe_refreshed_content(direct_result):
                    key = content_key(direct_result)
                    if key:
                        used_image_content.add(key)
                continue
        else:
            direct_result = build_missing_row(
                export_key,
                build_planned_from_queue_row(row, "", media_asset),
                CACHE_STATUS_MISSING_SOURCE_URL,
            )
        target_results[target] = direct_result
        target_used_refresh[target] = False
        pending_targets.append(target)

    if pending_targets:
        representative = target_representatives[pending_targets[0]]
        refreshed_sources, candidate_dimensions_by_url = refreshed_source_candidates_with_dimensions(
            representative,
            timeout=timeout,
            fetch_func=fetch_func,
        )
        refreshed_by_media_pk = refreshed_sources_by_media_pk(refreshed_sources)
        fallback_groups = refreshed_source_groups_for_fallback(
            refreshed_sources,
            candidate_dimensions_by_url,
        )
        fallback_group_index = 0
        fallback_source_index = 0
        for target in pending_targets:
            row = target_representatives[target]
            media_asset = media_asset_for_queue_row(row, media_assets_by_key)
            target_media_pk = media_pk_for_target(row, media_assets_by_key)
            requires_keyed_media_pk = refreshed_target_requires_keyed_media_pk(row, media_asset)
            if target_media_pk and target_media_pk in refreshed_by_media_pk:
                target_sources = sort_refreshed_sources_by_quality(
                    refreshed_by_media_pk[target_media_pk],
                    candidate_dimensions_by_url,
                )
            elif requires_keyed_media_pk:
                continue
            elif target_media_pk and refreshed_by_media_pk:
                continue
            elif refreshed_by_media_pk and fallback_group_index < len(fallback_groups):
                _group_key, target_sources = fallback_groups[fallback_group_index]
                fallback_group_index += 1
            elif not refreshed_by_media_pk and fallback_source_index < len(refreshed_sources):
                target_sources = refreshed_sources[fallback_source_index:]
            else:
                continue

            refreshed_result, used_refresh, consumed_count = try_refreshed_sources_for_target(
                export_key=export_key,
                row=row,
                media_asset=media_asset,
                target_sources=target_sources,
                used_image_content=used_image_content,
                cache_root=cache_root,
                timeout=timeout,
                max_bytes=max_bytes,
                fetch_attempts=fetch_attempts,
                cache_func=cache_func,
            )
            if refreshed_result:
                target_results[target] = refreshed_result
                target_used_refresh[target] = used_refresh
            if not refreshed_by_media_pk:
                if used_refresh:
                    fallback_source_index += consumed_count
                else:
                    break

    output: List[Tuple[int, Dict[str, Any], bool]] = []
    for target, rows in target_rows.items():
        result = target_results[target]
        used_refresh = target_used_refresh.get(target, False)
        for index, _row in rows:
            output.append((index, result, used_refresh))
    return output


def build_media_assets_by_key(media_rows: Iterable[Dict[str, str]]) -> Dict[Tuple[str, str], Dict[str, str]]:
    return {
        (row.get("item_pk", ""), row.get("media_index", "")): row
        for row in media_rows
    }


def group_indexed_queue_rows(
    indexed_rows: Sequence[Tuple[int, Dict[str, str]]],
) -> List[List[Tuple[int, Dict[str, str]]]]:
    groups: Dict[Tuple[str, str, str], List[Tuple[int, Dict[str, str]]]] = {}
    for index, row in indexed_rows:
        groups.setdefault(queue_row_group_key(row), []).append((index, row))
    return list(groups.values())


def limit_queue_rows(rows: Sequence[Dict[str, str]], max_rows: int) -> List[Dict[str, str]]:
    rows = sorted(rows, key=queue_sort_key)
    if max_rows <= 0:
        return list(rows)
    return list(rows[:max_rows])


def count_statuses(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        status = str(row.get("cache_status") or "")
        counts[status] = counts.get(status, 0) + 1
    return counts


def sanitized_summary(summary: MediaRefreshWorkerSummary) -> Dict[str, Any]:
    return {
        "export_key": summary.export_key,
        "queue_rows": summary.queue_rows,
        "processed_rows": summary.processed_rows,
        "max_rows": summary.max_rows,
        "max_parallel": summary.max_parallel,
        "fetch_attempts": summary.fetch_attempts,
        "upload_to_drive": summary.upload_to_drive,
        "output_csv": str(summary.output_csv),
        "status_counts": summary.status_counts,
        "cached_rows": summary.cached_rows,
        "refreshed_cached_rows": summary.refreshed_cached_rows,
        "direct_cached_rows": summary.direct_cached_rows,
        "missing_source_rows": summary.missing_source_rows,
        "download_failed_rows": summary.download_failed_rows,
        "limit_applied": summary.limit_applied,
        "drive_sync": summary.drive_sync,
    }


def write_worker_summary_json(path: Path, summary: MediaRefreshWorkerSummary) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(sanitized_summary(summary), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def run_media_refresh_worker(
    *,
    export_key: str,
    queue_csv: Path,
    media_assets_csv: Path,
    output_csv: Path,
    cache_root: Path,
    max_rows: int = 20,
    max_parallel: int = 6,
    sleep_seconds: float = 0.0,
    timeout_seconds: float = 20.0,
    fetch_attempts: int = 2,
    max_bytes: Optional[int] = 200 * 1024 * 1024,
    upload_to_drive: bool = False,
    prefix: Optional[str] = None,
    output_dir: Optional[Path] = None,
    drive_media_cache_dir: str = "",
    rclone_bin: str = "rclone",
    drive_retry_attempts: int = 5,
    drive_retry_initial_delay_seconds: float = 30.0,
    drive_retry_backoff_multiplier: float = 2.0,
    drive_retry_max_delay_seconds: float = 240.0,
    sync_func: Optional[Callable[..., Any]] = None,
    cache_func: Optional[CacheFunc] = None,
    fetch_func: Optional[FetchFunc] = None,
) -> MediaRefreshWorkerSummary:
    queue_rows = read_csv_rows(queue_csv)
    media_rows = read_csv_rows(media_assets_csv)
    process_rows = limit_queue_rows(queue_rows, max_rows)
    media_assets_by_key = build_media_assets_by_key(media_rows)
    selected_cache_func = cache_func or cache_one_media
    selected_fetch_func = fetch_func or fetch_threads_body
    max_workers = max(int(max_parallel or 1), 1)
    output_rows: List[Dict[str, Any]] = []
    refreshed_cached_rows = 0

    indexed_groups = group_indexed_queue_rows(list(enumerate(process_rows)))

    def run_group(group_index_and_rows: Tuple[int, List[Tuple[int, Dict[str, str]]]]) -> List[Tuple[int, Dict[str, Any], bool]]:
        group_index, rows = group_index_and_rows
        if sleep_seconds > 0 and group_index:
            time.sleep(sleep_seconds)
        return process_queue_group(
            export_key=export_key,
            indexed_rows=rows,
            media_assets_by_key=media_assets_by_key,
            cache_root=cache_root,
            timeout=timeout_seconds,
            max_bytes=max_bytes,
            fetch_attempts=fetch_attempts,
            cache_func=selected_cache_func,
            fetch_func=selected_fetch_func,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(run_group, (index, rows)): index
            for index, rows in enumerate(indexed_groups)
        }
        indexed_results: List[Tuple[int, Dict[str, Any], bool]] = []
        for future in as_completed(future_map):
            indexed_results.extend(future.result())
    sorted_results = sorted(indexed_results, key=lambda value: value[0])
    output_rows = reuse_duplicate_cached_content(
        rows=[row for _index, row, _used_refresh in sorted_results],
        cache_root=cache_root,
    )
    for _index, _row, used_refresh in sorted_results:
        if used_refresh:
            refreshed_cached_rows += 1

    write_media_cache_csv(output_csv, output_rows)
    status_counts = count_statuses(output_rows)
    cached_rows = status_counts.get(CACHE_STATUS_CACHED, 0)
    missing_source_rows = status_counts.get(CACHE_STATUS_MISSING_SOURCE_URL, 0)
    download_failed_rows = status_counts.get(CACHE_STATUS_DOWNLOAD_FAILED, 0)
    drive_sync = None
    if upload_to_drive:
        sync = sync_func or push_media_cache_to_drive
        sync_summary = sync(
            export_key=export_key,
            prefix=prefix or export_key,
            output_dir=output_dir or output_csv.parent,
            cache_root=cache_root,
            drive_media_cache_dir=drive_media_cache_dir,
            rclone_bin=rclone_bin,
            dry_run=False,
            rclone_retry_attempts=drive_retry_attempts,
            rclone_retry_initial_delay_seconds=drive_retry_initial_delay_seconds,
            rclone_retry_backoff_multiplier=drive_retry_backoff_multiplier,
            rclone_retry_max_delay_seconds=drive_retry_max_delay_seconds,
        )
        drive_sync = {
            "drive_remote_dir": getattr(sync_summary, "drive_remote_dir", ""),
            "manifest_path": getattr(sync_summary, "manifest_path", ""),
        }

    return MediaRefreshWorkerSummary(
        export_key=export_key,
        queue_rows=len(queue_rows),
        processed_rows=len(process_rows),
        max_rows=max_rows,
        max_parallel=max_workers,
        fetch_attempts=fetch_attempts,
        upload_to_drive=upload_to_drive,
        output_csv=output_csv,
        status_counts=status_counts,
        cached_rows=cached_rows,
        refreshed_cached_rows=refreshed_cached_rows,
        direct_cached_rows=cached_rows - refreshed_cached_rows,
        missing_source_rows=missing_source_rows,
        download_failed_rows=download_failed_rows,
        limit_applied=max_rows > 0 and len(queue_rows) > len(process_rows),
        drive_sync=drive_sync,
    )
