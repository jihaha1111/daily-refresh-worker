"""Private media cache helpers for expiring Threads/CDN URLs."""

from __future__ import annotations

import csv
import hashlib
import mimetypes
import os
import struct
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


MEDIA_CACHE_COLUMNS = [
    "export_key",
    "item_pk",
    "media_index",
    "cache_role",
    "asset_type",
    "input_kind",
    "source_url_hash",
    "cache_status",
    "cache_rel_path",
    "mime_type",
    "byte_size",
    "content_sha256",
    "width",
    "height",
    "duration_seconds",
    "cached_at",
    "error",
]

CACHE_STATUS_CACHED = "cached"
CACHE_STATUS_EXPIRED = "expired"
CACHE_STATUS_FORBIDDEN = "forbidden"
CACHE_STATUS_MISSING_SOURCE_URL = "missing_source_url"
CACHE_STATUS_DOWNLOAD_FAILED = "download_failed"


@dataclass(frozen=True)
class MediaCacheSummary:
    output_csv: Path
    source_media_assets: int
    eligible_media_assets: int
    rows: int
    cached: int
    expired: int
    forbidden: int
    missing_source_url: int
    download_failed: int


@dataclass(frozen=True)
class MediaCacheExtensionRepairSummary:
    metadata_csv: Path
    export_key: str
    dry_run: bool
    rows: int
    cached_rows: int
    updated_rows: int
    renamed_files: int
    missing_files: int
    errors: List[str]


def source_url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def content_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extension_from_mime_type(mime_type: str) -> Optional[str]:
    clean_mime = (mime_type or "").split(";", 1)[0].strip().lower()
    known_extensions = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "video/mp4": ".mp4",
        "video/quicktime": ".mov",
        "video/webm": ".webm",
    }
    if clean_mime in known_extensions:
        return known_extensions[clean_mime]
    if clean_mime.startswith(("image/", "video/")):
        guessed = mimetypes.guess_extension(clean_mime)
        if guessed and guessed != ".bin":
            return guessed
    return None


def safe_extension(
    url: str,
    mime_type: str,
    input_kind: str,
    *,
    prefer_mime: bool = False,
) -> str:
    if prefer_mime:
        mime_extension = extension_from_mime_type(mime_type)
        if mime_extension:
            return mime_extension
    parsed = urllib.parse.urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix and len(suffix) <= 10 and all(ch.isalnum() or ch == "." for ch in suffix):
        return suffix
    mime_extension = extension_from_mime_type(mime_type)
    if mime_extension:
        return mime_extension
    return ".mp4" if input_kind == "video" else ".bin"


def guess_mime_type(path: Path, url: str, header_value: str = "") -> str:
    if header_value:
        return header_value.split(";", 1)[0].strip()
    guessed, _encoding = mimetypes.guess_type(path.name or urllib.parse.urlparse(url).path)
    return guessed or ""


def image_dimensions(path: Path) -> Tuple[Optional[int], Optional[int]]:
    data = path.read_bytes()[:64 * 1024]
    if data.startswith(b"\x89PNG\r\n\x1a\n") and len(data) >= 24:
        width, height = struct.unpack(">II", data[16:24])
        return int(width), int(height)
    if data.startswith((b"GIF87a", b"GIF89a")) and len(data) >= 10:
        width, height = struct.unpack("<HH", data[6:10])
        return int(width), int(height)
    if data.startswith(b"\xff\xd8"):
        idx = 2
        while idx + 9 < len(data):
            if data[idx] != 0xFF:
                idx += 1
                continue
            marker = data[idx + 1]
            idx += 2
            if marker in (0xD8, 0xD9):
                continue
            if idx + 2 > len(data):
                break
            segment_length = struct.unpack(">H", data[idx:idx + 2])[0]
            if segment_length < 2 or idx + segment_length > len(data):
                break
            if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
                height, width = struct.unpack(">HH", data[idx + 3:idx + 7])
                return int(width), int(height)
            idx += segment_length
    if data.startswith(b"RIFF") and len(data) >= 30 and data[8:12] == b"WEBP":
        chunk = data[12:16]
        if chunk == b"VP8X":
            width_minus_one = int.from_bytes(data[24:27], "little")
            height_minus_one = int.from_bytes(data[27:30], "little")
            return width_minus_one + 1, height_minus_one + 1
        if chunk == b"VP8L" and len(data) >= 25 and data[20] == 0x2F:
            b1, b2, b3, b4 = data[21:25]
            width = 1 + b1 + ((b2 & 0x3F) << 8)
            height = 1 + ((b2 & 0xC0) >> 6) + (b3 << 2) + ((b4 & 0x0F) << 10)
            return int(width), int(height)
        if chunk == b"VP8 ":
            start = data.find(b"\x9d\x01\x2a", 20)
            if start != -1 and start + 7 <= len(data):
                width_raw, height_raw = struct.unpack("<HH", data[start + 3:start + 7])
                return int(width_raw & 0x3FFF), int(height_raw & 0x3FFF)
    return None, None


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_media_cache_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MEDIA_CACHE_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in MEDIA_CACHE_COLUMNS})


def _safe_existing_cache_rel_path(export_key: str, rel_path: str) -> Optional[Path]:
    if not rel_path:
        return None
    path = Path(rel_path)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        return None
    if not path.parts or path.parts[0] != export_key:
        return None
    return path


def normalize_media_cache_extensions(
    *,
    metadata_csv: Path,
    cache_root: Path,
    export_key: str,
    dry_run: bool = False,
) -> MediaCacheExtensionRepairSummary:
    rows = read_csv_rows(metadata_csv)
    updates: Dict[str, str] = {}
    errors: List[str] = []
    cached_rows = 0
    updated_rows = 0

    for index, row in enumerate(rows, start=2):
        if row.get("cache_status") != CACHE_STATUS_CACHED:
            continue
        cached_rows += 1
        rel_path_value = row.get("cache_rel_path", "")
        rel_path = _safe_existing_cache_rel_path(export_key, rel_path_value)
        if rel_path is None:
            errors.append(f"row {index}: invalid cache_rel_path: {rel_path_value!r}")
            continue
        target_extension = extension_from_mime_type(row.get("mime_type", ""))
        if not target_extension:
            continue
        if rel_path.suffix.lower() == target_extension:
            continue
        if rel_path.suffix.lower() != ".bin":
            continue
        updates[rel_path.as_posix()] = rel_path.with_suffix(target_extension).as_posix()
        updated_rows += 1

    renamed_files = 0
    missing_files = 0
    for old_rel, new_rel in sorted(updates.items()):
        old_path = cache_root / old_rel
        new_path = cache_root / new_rel
        if old_path.exists():
            if new_path.exists():
                if content_sha256(old_path) != content_sha256(new_path):
                    errors.append(f"refusing to overwrite different cache file: {new_rel}")
                    continue
                if not dry_run:
                    old_path.unlink()
            elif not dry_run:
                new_path.parent.mkdir(parents=True, exist_ok=True)
                old_path.rename(new_path)
            renamed_files += 1
        elif new_path.exists():
            renamed_files += 1
        else:
            errors.append(f"cached file not found: {old_path}")
            missing_files += 1

    if updates and not errors and not dry_run:
        for row in rows:
            rel_path_value = row.get("cache_rel_path", "")
            if rel_path_value in updates:
                row["cache_rel_path"] = updates[rel_path_value]
        write_media_cache_csv(metadata_csv, rows)

    return MediaCacheExtensionRepairSummary(
        metadata_csv=metadata_csv,
        export_key=export_key,
        dry_run=dry_run,
        rows=len(rows),
        cached_rows=cached_rows,
        updated_rows=updated_rows,
        renamed_files=renamed_files,
        missing_files=missing_files,
        errors=errors,
    )


def split_grade_tokens(values: Iterable[str]) -> List[str]:
    grades: List[str] = []
    for value in values:
        for token in str(value).split(","):
            token = token.strip()
            if token:
                grades.append(token)
    return grades


def normalize_grade_token(value: str) -> str:
    return str(value or "").strip().casefold()


def item_pks_from_match_id(match_id: str) -> List[str]:
    parts = [part for part in str(match_id or "").split("__") if part]
    if len(parts) != 2:
        return []
    return parts


def selected_item_pks_from_performance_labels(
    path: Path,
    selected_grades: Iterable[str],
) -> List[str]:
    selected = {
        normalize_grade_token(grade)
        for grade in split_grade_tokens(selected_grades)
        if normalize_grade_token(grade)
    }
    if not selected:
        return []
    item_pks = set()
    for row in read_csv_rows(path):
        if normalize_grade_token(row.get("performance_grade", "")) not in selected:
            continue
        for item_pk in item_pks_from_match_id(row.get("match_id", "")):
            item_pks.add(item_pk)
    return sorted(item_pks)


def cache_plan_for_media_asset(row: Dict[str, str]) -> List[Dict[str, str]]:
    asset_type = (row.get("asset_type") or "unknown").strip() or "unknown"
    best_image_url = (row.get("best_image_url") or "").strip()
    best_video_url = (row.get("best_video_url") or "").strip()
    base = {
        "item_pk": row.get("item_pk", ""),
        "media_index": row.get("media_index", ""),
        "asset_type": asset_type,
    }
    if best_video_url:
        planned = []
        if best_image_url:
            planned.append(
                {
                    **base,
                    "cache_role": "preview",
                    "input_kind": "image",
                    "source_url": best_image_url,
                }
            )
        planned.append(
            {
                **base,
                "cache_role": "media",
                "input_kind": "video",
                "source_url": best_video_url,
            }
        )
        return planned
    return [
        {
            **base,
            "cache_role": "media",
            "input_kind": "image" if best_image_url else asset_type,
            "source_url": best_image_url,
        }
    ]


def cache_rel_path(
    export_key: str,
    item_pk: str,
    media_index: str,
    cache_role: str,
    url: str,
    extension: str,
) -> Path:
    digest = source_url_hash(url)[:16]
    safe_item_pk = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in item_pk)
    safe_index = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in media_index)
    filename = f"{safe_index}_{cache_role}_{digest}{extension}"
    return Path(export_key) / safe_item_pk / filename


def status_for_http_error(exc: urllib.error.HTTPError) -> str:
    if exc.code == 403:
        return CACHE_STATUS_FORBIDDEN
    if exc.code in (404, 410):
        return CACHE_STATUS_EXPIRED
    return CACHE_STATUS_DOWNLOAD_FAILED


def download_to_cache(
    url: str,
    destination: Path,
    *,
    timeout: float,
    max_bytes: Optional[int],
) -> Tuple[str, str]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".download-", dir=str(destination.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    mime_type = ""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            mime_type = guess_mime_type(destination, url, response.headers.get("content-type", ""))
            total = 0
            with tmp_path.open("wb") as out:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if max_bytes is not None and total > max_bytes:
                        raise ValueError(f"download exceeds max_bytes={max_bytes}")
                    out.write(chunk)
        tmp_path.replace(destination)
        return mime_type, ""
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def build_missing_row(
    export_key: str,
    planned: Dict[str, str],
    status: str,
    error: str = "",
) -> Dict[str, Any]:
    return {
        "export_key": export_key,
        "item_pk": planned.get("item_pk", ""),
        "media_index": planned.get("media_index", ""),
        "cache_role": planned.get("cache_role", ""),
        "asset_type": planned.get("asset_type", ""),
        "input_kind": planned.get("input_kind", "unknown") or "unknown",
        "source_url_hash": source_url_hash(planned.get("source_url", "")) if planned.get("source_url") else "",
        "cache_status": status,
        "cache_rel_path": "",
        "mime_type": "",
        "byte_size": "",
        "content_sha256": "",
        "width": "",
        "height": "",
        "duration_seconds": "",
        "cached_at": "",
        "error": error,
    }


def cache_one_media(
    export_key: str,
    planned: Dict[str, str],
    cache_root: Path,
    *,
    timeout: float,
    max_bytes: Optional[int],
    force: bool,
) -> Dict[str, Any]:
    url = (planned.get("source_url") or "").strip()
    if not url:
        return build_missing_row(export_key, planned, CACHE_STATUS_MISSING_SOURCE_URL)

    guessed_mime, _encoding = mimetypes.guess_type(urllib.parse.urlparse(url).path)
    extension = safe_extension(url, guessed_mime or "", planned.get("input_kind", "unknown"))
    rel_path = cache_rel_path(
        export_key,
        planned.get("item_pk", ""),
        planned.get("media_index", ""),
        planned.get("cache_role", ""),
        url,
        extension,
    )
    destination = cache_root / rel_path
    try:
        if not destination.exists() or force:
            mime_type, _error = download_to_cache(
                url,
                destination,
                timeout=timeout,
                max_bytes=max_bytes,
            )
            final_extension = safe_extension(
                url,
                mime_type,
                planned.get("input_kind", "unknown"),
                prefer_mime=True,
            )
            if final_extension != extension:
                final_rel_path = cache_rel_path(
                    export_key,
                    planned.get("item_pk", ""),
                    planned.get("media_index", ""),
                    planned.get("cache_role", ""),
                    url,
                    final_extension,
                )
                final_destination = cache_root / final_rel_path
                if final_destination != destination:
                    final_destination.parent.mkdir(parents=True, exist_ok=True)
                    if final_destination.exists() and not force:
                        destination.unlink(missing_ok=True)
                    else:
                        destination.replace(final_destination)
                    rel_path = final_rel_path
                    destination = final_destination
        else:
            mime_type = guess_mime_type(destination, url)
        byte_size = destination.stat().st_size
        width: Optional[int] = None
        height: Optional[int] = None
        if planned.get("input_kind") == "image":
            width, height = image_dimensions(destination)
        return {
            "export_key": export_key,
            "item_pk": planned.get("item_pk", ""),
            "media_index": planned.get("media_index", ""),
            "cache_role": planned.get("cache_role", ""),
            "asset_type": planned.get("asset_type", ""),
            "input_kind": planned.get("input_kind", "unknown") or "unknown",
            "source_url_hash": source_url_hash(url),
            "cache_status": CACHE_STATUS_CACHED,
            "cache_rel_path": rel_path.as_posix(),
            "mime_type": mime_type,
            "byte_size": byte_size,
            "content_sha256": content_sha256(destination),
            "width": width or "",
            "height": height or "",
            "duration_seconds": "",
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "error": "",
        }
    except urllib.error.HTTPError as exc:
        return build_missing_row(export_key, planned, status_for_http_error(exc), str(exc))
    except Exception as exc:
        return build_missing_row(export_key, planned, CACHE_STATUS_DOWNLOAD_FAILED, str(exc))


def cache_media_assets(
    *,
    media_assets_csv: Path,
    output_csv: Path,
    cache_root: Path,
    export_key: str,
    timeout: float = 20.0,
    max_bytes: Optional[int] = 200 * 1024 * 1024,
    force: bool = False,
    item_pk_allowlist: Optional[Iterable[str]] = None,
) -> MediaCacheSummary:
    media_rows = read_csv_rows(media_assets_csv)
    allowlist = set(item_pk_allowlist or [])
    if allowlist:
        eligible_media_rows = [
            row for row in media_rows if (row.get("item_pk") or "") in allowlist
        ]
    else:
        eligible_media_rows = media_rows
    output_rows: List[Dict[str, Any]] = []
    for media_row in eligible_media_rows:
        for planned in cache_plan_for_media_asset(media_row):
            output_rows.append(
                cache_one_media(
                    export_key,
                    planned,
                    cache_root,
                    timeout=timeout,
                    max_bytes=max_bytes,
                    force=force,
                )
            )
    write_media_cache_csv(output_csv, output_rows)
    counts = {status: 0 for status in (
        CACHE_STATUS_CACHED,
        CACHE_STATUS_EXPIRED,
        CACHE_STATUS_FORBIDDEN,
        CACHE_STATUS_MISSING_SOURCE_URL,
        CACHE_STATUS_DOWNLOAD_FAILED,
    )}
    for row in output_rows:
        status = str(row.get("cache_status") or "")
        if status in counts:
            counts[status] += 1
    return MediaCacheSummary(
        output_csv=output_csv,
        source_media_assets=len(media_rows),
        eligible_media_assets=len(eligible_media_rows),
        rows=len(output_rows),
        cached=counts[CACHE_STATUS_CACHED],
        expired=counts[CACHE_STATUS_EXPIRED],
        forbidden=counts[CACHE_STATUS_FORBIDDEN],
        missing_source_url=counts[CACHE_STATUS_MISSING_SOURCE_URL],
        download_failed=counts[CACHE_STATUS_DOWNLOAD_FAILED],
    )
