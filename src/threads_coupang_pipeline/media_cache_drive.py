"""Google Drive sync helpers for private media-cache artifacts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from .drive_runner import DriveRunnerError, join_remote_path, run_rclone_with_retry
from .media_cache import CACHE_STATUS_CACHED, content_sha256, read_csv_rows


MANIFEST_VERSION = 1
SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


@dataclass(frozen=True)
class MediaCacheValidation:
    metadata_rows: int
    cache_status_counts: Dict[str, int]
    cached_file_count: int
    cached_byte_size: int
    missing_file_count: int
    errors: List[str]


@dataclass(frozen=True)
class MediaCacheDriveSummary:
    action: str
    dry_run: bool
    export_key: str
    prefix: str
    drive_remote_dir: str
    metadata_csv: str
    manifest_path: str
    operations: List[List[str]]
    validation: Optional[MediaCacheValidation]


def validate_safe_token(name: str, value: str) -> str:
    token = str(value or "").strip()
    if not SAFE_TOKEN_RE.fullmatch(token):
        raise DriveRunnerError(
            f"{name} must contain only letters, digits, underscores, and hyphens: {value!r}"
        )
    return token


def validate_cache_rel_path(export_key: str, rel_path: str) -> Path:
    if not rel_path:
        raise DriveRunnerError("cached media row is missing cache_rel_path")
    path = Path(rel_path)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise DriveRunnerError(f"cache_rel_path must be a safe relative path: {rel_path!r}")
    if not path.parts or path.parts[0] != export_key:
        raise DriveRunnerError(
            f"cache_rel_path must start with export_key {export_key!r}: {rel_path!r}"
        )
    return path


def metadata_csv_path(output_dir: Path, prefix: str) -> Path:
    return output_dir / f"{prefix}_media_cache_assets.csv"


def manifest_path(output_dir: Path, prefix: str) -> Path:
    return output_dir / f"{prefix}_media_cache_manifest.json"


def validate_media_cache_files(
    *,
    metadata_csv: Path,
    cache_root: Path,
    export_key: str,
) -> MediaCacheValidation:
    if not metadata_csv.exists():
        raise DriveRunnerError(f"media cache metadata CSV not found: {metadata_csv}")
    rows = read_csv_rows(metadata_csv)
    status_counts: Dict[str, int] = {}
    cached_file_count = 0
    cached_byte_size = 0
    missing_file_count = 0
    errors: List[str] = []
    counted_rel_paths = set()

    for index, row in enumerate(rows, start=2):
        status = row.get("cache_status", "") or ""
        status_counts[status] = status_counts.get(status, 0) + 1
        if status != CACHE_STATUS_CACHED:
            continue
        try:
            rel_path = validate_cache_rel_path(export_key, row.get("cache_rel_path", ""))
        except DriveRunnerError as exc:
            errors.append(f"row {index}: {exc}")
            missing_file_count += 1
            continue
        local_path = cache_root / rel_path
        if not local_path.exists():
            errors.append(f"row {index}: cached file not found: {local_path}")
            missing_file_count += 1
            continue
        if not local_path.is_file():
            errors.append(f"row {index}: cache path is not a file: {local_path}")
            missing_file_count += 1
            continue
        actual_size = local_path.stat().st_size
        expected_size = row.get("byte_size", "")
        if not expected_size:
            errors.append(f"row {index}: cached row is missing byte_size")
        else:
            try:
                if int(expected_size) != actual_size:
                    errors.append(
                        f"row {index}: byte_size mismatch for {row.get('cache_rel_path')}: "
                        f"metadata={expected_size}, actual={actual_size}"
                    )
            except ValueError:
                errors.append(f"row {index}: invalid byte_size: {expected_size!r}")
        expected_hash = row.get("content_sha256", "")
        if not expected_hash:
            errors.append(f"row {index}: cached row is missing content_sha256")
        else:
            actual_hash = content_sha256(local_path)
            if expected_hash != actual_hash:
                errors.append(
                    f"row {index}: content_sha256 mismatch for {row.get('cache_rel_path')}"
                )
        rel_path_key = rel_path.as_posix()
        if rel_path_key not in counted_rel_paths:
            counted_rel_paths.add(rel_path_key)
            cached_file_count += 1
            cached_byte_size += actual_size

    return MediaCacheValidation(
        metadata_rows=len(rows),
        cache_status_counts=status_counts,
        cached_file_count=cached_file_count,
        cached_byte_size=cached_byte_size,
        missing_file_count=missing_file_count,
        errors=errors,
    )


def build_manifest(
    *,
    export_key: str,
    prefix: str,
    drive_remote_dir: str,
    metadata_csv_name: str,
    validation: MediaCacheValidation,
) -> Dict[str, Any]:
    return {
        "manifest_version": MANIFEST_VERSION,
        "export_key": export_key,
        "prefix": prefix,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "drive_remote_dir": drive_remote_dir,
        "metadata_csv_name": metadata_csv_name,
        "cache_status_counts": validation.cache_status_counts,
        "cached_file_count": validation.cached_file_count,
        "cached_byte_size": validation.cached_byte_size,
        "missing_file_count": validation.missing_file_count,
    }


def write_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def validation_to_dict(validation: Optional[MediaCacheValidation]) -> Optional[Dict[str, Any]]:
    if validation is None:
        return None
    return {
        "metadata_rows": validation.metadata_rows,
        "cache_status_counts": validation.cache_status_counts,
        "cached_file_count": validation.cached_file_count,
        "cached_byte_size": validation.cached_byte_size,
        "missing_file_count": validation.missing_file_count,
        "errors": validation.errors,
    }


def summary_to_dict(summary: MediaCacheDriveSummary) -> Dict[str, Any]:
    return {
        "action": summary.action,
        "dry_run": summary.dry_run,
        "export_key": summary.export_key,
        "prefix": summary.prefix,
        "drive_remote_dir": summary.drive_remote_dir,
        "metadata_csv": summary.metadata_csv,
        "manifest_path": summary.manifest_path,
        "operations": summary.operations,
        "validation": validation_to_dict(summary.validation),
    }


def ensure_no_validation_errors(validation: MediaCacheValidation) -> None:
    if validation.errors:
        raise DriveRunnerError("media cache validation failed: " + "; ".join(validation.errors))


def push_media_cache_to_drive(
    *,
    export_key: str,
    prefix: str,
    output_dir: Path,
    cache_root: Path,
    drive_media_cache_dir: str,
    rclone_bin: str = "rclone",
    dry_run: bool = False,
    rclone_retry_attempts: int = 1,
    rclone_retry_initial_delay_seconds: float = 30.0,
    rclone_retry_backoff_multiplier: float = 2.0,
    rclone_retry_max_delay_seconds: float = 240.0,
) -> MediaCacheDriveSummary:
    export_key = validate_safe_token("export_key", export_key)
    prefix = validate_safe_token("prefix", prefix)
    metadata_csv = metadata_csv_path(output_dir, prefix)
    local_cache_dir = cache_root / export_key
    drive_remote_dir = join_remote_path(drive_media_cache_dir, export_key)
    manifest = manifest_path(output_dir, prefix)
    validation = validate_media_cache_files(
        metadata_csv=metadata_csv,
        cache_root=cache_root,
        export_key=export_key,
    )
    ensure_no_validation_errors(validation)

    if validation.cached_file_count and not local_cache_dir.exists():
        raise DriveRunnerError(f"media cache directory not found: {local_cache_dir}")

    operations = [
        ["mkdir", drive_remote_dir],
        ["copyto", str(metadata_csv), join_remote_path(drive_remote_dir, metadata_csv.name)],
        ["copyto", str(manifest), join_remote_path(drive_remote_dir, manifest.name)],
    ]
    if local_cache_dir.exists():
        operations.insert(1, ["copy", str(local_cache_dir), drive_remote_dir])
    if not dry_run:
        write_manifest(
            manifest,
            build_manifest(
                export_key=export_key,
                prefix=prefix,
                drive_remote_dir=drive_remote_dir,
                metadata_csv_name=metadata_csv.name,
                validation=validation,
            ),
        )
        for args in operations:
            run_rclone_with_retry(
                rclone_bin,
                args,
                capture_output=True,
                attempts=rclone_retry_attempts,
                initial_delay_seconds=rclone_retry_initial_delay_seconds,
                backoff_multiplier=rclone_retry_backoff_multiplier,
                max_delay_seconds=rclone_retry_max_delay_seconds,
            )

    return MediaCacheDriveSummary(
        action="push",
        dry_run=dry_run,
        export_key=export_key,
        prefix=prefix,
        drive_remote_dir=drive_remote_dir,
        metadata_csv=str(metadata_csv),
        manifest_path=str(manifest),
        operations=operations,
        validation=validation,
    )


def pull_media_cache_from_drive(
    *,
    export_key: str,
    prefix: str,
    output_dir: Path,
    cache_root: Path,
    drive_media_cache_dir: str,
    rclone_bin: str = "rclone",
    dry_run: bool = False,
    rclone_retry_attempts: int = 1,
    rclone_retry_initial_delay_seconds: float = 30.0,
    rclone_retry_backoff_multiplier: float = 2.0,
    rclone_retry_max_delay_seconds: float = 240.0,
) -> MediaCacheDriveSummary:
    export_key = validate_safe_token("export_key", export_key)
    prefix = validate_safe_token("prefix", prefix)
    metadata_csv = metadata_csv_path(output_dir, prefix)
    local_cache_dir = cache_root / export_key
    drive_remote_dir = join_remote_path(drive_media_cache_dir, export_key)
    manifest = manifest_path(output_dir, prefix)
    operations: List[List[str]] = [
        [
            "copy",
            drive_remote_dir,
            str(local_cache_dir),
            "--exclude",
            "*_media_cache_assets.csv",
            "--exclude",
            "*_media_cache_manifest.json",
        ],
        ["copyto", join_remote_path(drive_remote_dir, metadata_csv.name), str(metadata_csv)],
        ["copyto", join_remote_path(drive_remote_dir, manifest.name), str(manifest)],
    ]
    validation: Optional[MediaCacheValidation] = None
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
        local_cache_dir.mkdir(parents=True, exist_ok=True)
        for args in operations:
            run_rclone_with_retry(
                rclone_bin,
                args,
                capture_output=True,
                attempts=rclone_retry_attempts,
                initial_delay_seconds=rclone_retry_initial_delay_seconds,
                backoff_multiplier=rclone_retry_backoff_multiplier,
                max_delay_seconds=rclone_retry_max_delay_seconds,
            )
        validation = validate_media_cache_files(
            metadata_csv=metadata_csv,
            cache_root=cache_root,
            export_key=export_key,
        )
        ensure_no_validation_errors(validation)

    return MediaCacheDriveSummary(
        action="pull",
        dry_run=dry_run,
        export_key=export_key,
        prefix=prefix,
        drive_remote_dir=drive_remote_dir,
        metadata_csv=str(metadata_csv),
        manifest_path=str(manifest),
        operations=operations,
        validation=validation,
    )
