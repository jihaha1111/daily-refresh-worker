"""Google Drive date-pair runner helpers.

The operational runner keeps private raw files out of the repo by downloading
the required Drive pair into a temporary directory for the duration of one
extraction run.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence


DATE_TOKEN_RE = re.compile(r"^\d{6}$")
SOURCE_NAME_RE = re.compile(r"^threads-source-\d{6}\.json$")
VIEW_COUNT_NAME_RE = re.compile(r"^threads-viewcount-\d{6}\.csv$")
TRANSIENT_RCLONE_ERROR_MARKERS = (
    "RATE_LIMIT_EXCEEDED",
    "rateLimitExceeded",
    "userRateLimitExceeded",
    "Quota exceeded",
    "quota exceeded",
    "backendError",
    "internalError",
    "Error 500",
    "Error 502",
    "Error 503",
    "Error 504",
    "connection reset",
    "connection refused",
    "i/o timeout",
    "TLS handshake timeout",
    "temporary failure",
    "timeout awaiting response headers",
)


class DriveRunnerError(ValueError):
    """Raised when the Drive date-pair workflow cannot run safely."""


@dataclass(frozen=True)
class DriveFilePair:
    date: str
    source_name: str
    view_count_name: str
    source_uri: str
    view_count_uri: str


def validate_date_token(date: str) -> str:
    if not DATE_TOKEN_RE.fullmatch(date or ""):
        raise DriveRunnerError(f"--date must use YYMMDD format, got: {date!r}")
    return date


def expected_filenames(date: str) -> tuple[str, str]:
    validate_date_token(date)
    return f"threads-source-{date}.json", f"threads-viewcount-{date}.csv"


def join_remote_path(remote_dir: str, filename: str) -> str:
    return f"{remote_dir.rstrip('/')}/{filename}"


def normalize_lsf_entries(entries: Iterable[str]) -> List[str]:
    names: List[str] = []
    for entry in entries:
        name = entry.strip()
        if not name:
            continue
        names.append(name.rstrip("/"))
    return names


def is_raw_family_name(name: str) -> bool:
    return name.startswith("threads-source-") or name.startswith("threads-viewcount")


def is_valid_raw_pair_name(name: str) -> bool:
    return bool(SOURCE_NAME_RE.fullmatch(name) or VIEW_COUNT_NAME_RE.fullmatch(name))


def resolve_drive_pair(date: str, entries: Sequence[str], drive_raw_dir: str) -> DriveFilePair:
    source_name, view_count_name = expected_filenames(date)
    names = normalize_lsf_entries(entries)

    malformed = sorted(
        name for name in names if is_raw_family_name(name) and not is_valid_raw_pair_name(name)
    )
    if malformed:
        raise DriveRunnerError(
            "Drive raw folder contains malformed raw filenames: "
            + ", ".join(malformed)
        )

    duplicates = [
        name for name in (source_name, view_count_name) if names.count(name) > 1
    ]
    if duplicates:
        raise DriveRunnerError(
            f"Drive raw folder contains duplicate required files: {', '.join(duplicates)}"
        )

    missing = [name for name in (source_name, view_count_name) if name not in names]
    if missing:
        raise DriveRunnerError(
            f"Missing required Drive raw pair for date {date}: {', '.join(missing)}"
        )

    return DriveFilePair(
        date=date,
        source_name=source_name,
        view_count_name=view_count_name,
        source_uri=join_remote_path(drive_raw_dir, source_name),
        view_count_uri=join_remote_path(drive_raw_dir, view_count_name),
    )


def ensure_rclone_available(rclone_bin: str) -> None:
    if Path(rclone_bin).exists() or shutil.which(rclone_bin):
        return
    raise DriveRunnerError(f"rclone was not found: {rclone_bin}")


def run_rclone(
    rclone_bin: str,
    args: Sequence[str],
    *,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    ensure_rclone_available(rclone_bin)
    completed = subprocess.run(
        [rclone_bin, *args],
        text=True,
        encoding="utf-8",
        capture_output=capture_output,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        message = stderr or f"rclone exited with code {completed.returncode}"
        raise DriveRunnerError(message)
    return completed


def is_transient_rclone_error(message: str) -> bool:
    return any(marker in str(message or "") for marker in TRANSIENT_RCLONE_ERROR_MARKERS)


def bounded_retry_delays(
    *,
    attempts: int,
    initial_delay_seconds: float,
    backoff_multiplier: float,
    max_delay_seconds: float,
) -> List[float]:
    if attempts <= 1:
        return []
    delay = max(float(initial_delay_seconds), 0.0)
    multiplier = max(float(backoff_multiplier), 1.0)
    max_delay = max(float(max_delay_seconds), 0.0)
    delays: List[float] = []
    for _attempt in range(attempts - 1):
        delays.append(min(delay, max_delay) if max_delay else delay)
        delay *= multiplier
    return delays


def run_rclone_with_retry(
    rclone_bin: str,
    args: Sequence[str],
    *,
    capture_output: bool = True,
    attempts: int = 5,
    initial_delay_seconds: float = 30.0,
    backoff_multiplier: float = 2.0,
    max_delay_seconds: float = 240.0,
    sleep_func: Callable[[float], None] = time.sleep,
) -> subprocess.CompletedProcess[str]:
    max_attempts = max(int(attempts or 1), 1)
    delays = bounded_retry_delays(
        attempts=max_attempts,
        initial_delay_seconds=initial_delay_seconds,
        backoff_multiplier=backoff_multiplier,
        max_delay_seconds=max_delay_seconds,
    )
    last_error: Optional[DriveRunnerError] = None
    for attempt_index in range(max_attempts):
        try:
            return run_rclone(rclone_bin, args, capture_output=capture_output)
        except DriveRunnerError as exc:
            last_error = exc
            if attempt_index >= max_attempts - 1 or not is_transient_rclone_error(str(exc)):
                raise
            sleep_func(delays[attempt_index])
    assert last_error is not None
    raise last_error


def list_drive_raw_files(rclone_bin: str, drive_raw_dir: str) -> List[str]:
    completed = run_rclone(rclone_bin, ["lsf", drive_raw_dir])
    return normalize_lsf_entries((completed.stdout or "").splitlines())


def copy_drive_file(
    rclone_bin: str,
    source_uri: str,
    destination: Path,
    *,
    retry_attempts: int = 1,
    retry_initial_delay_seconds: float = 30.0,
    retry_backoff_multiplier: float = 2.0,
    retry_max_delay_seconds: float = 240.0,
    sleep_func: Callable[[float], None] = time.sleep,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    run_rclone_with_retry(
        rclone_bin,
        ["copyto", source_uri, str(destination)],
        capture_output=True,
        attempts=retry_attempts,
        initial_delay_seconds=retry_initial_delay_seconds,
        backoff_multiplier=retry_backoff_multiplier,
        max_delay_seconds=retry_max_delay_seconds,
        sleep_func=sleep_func,
    )


def build_dry_run_report(
    pair: DriveFilePair,
    output_dir: Path,
    review_dir: Path,
    output_shape: str,
) -> Dict[str, Any]:
    return {
        "dry_run": True,
        "date": pair.date,
        "source_file": pair.source_uri,
        "view_counts_file": pair.view_count_uri,
        "output_prefix": pair.date,
        "output_dir": str(output_dir),
        "output_shape": output_shape,
        "review_summary_file": str(review_dir / f"{pair.date}_match_content_summary.csv"),
        "review_summary_all_file": str(review_dir / "all_match_content_summary.csv"),
    }


def replace_summary_source_paths(
    summary: Dict[str, Any],
    pair: DriveFilePair,
) -> Dict[str, Any]:
    updated = dict(summary)
    updated["input_file"] = pair.source_uri
    updated["external_view_count_file"] = pair.view_count_uri
    return updated


def write_summary_json(output_dir: Path, prefix: str, summary: Dict[str, Any]) -> None:
    summary_path = output_dir / f"{prefix}_coupang_enhanced_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def copy_review_summary(
    output_dir: Path,
    prefix: str,
    review_dir: Path,
) -> Dict[str, Any]:
    source = output_dir / f"{prefix}_match_content_summary.csv"
    if not source.exists():
        return {
            "review_summary_file": None,
            "review_summary_all_file": None,
            "review_summary_rows": 0,
            "review_summary_all_rows": 0,
        }

    review_dir.mkdir(parents=True, exist_ok=True)
    per_date_path = review_dir / f"{prefix}_match_content_summary.csv"
    shutil.copyfile(source, per_date_path)

    all_path = review_dir / "all_match_content_summary.csv"
    total_rows = rebuild_all_review_summary(review_dir, all_path)
    current_rows = count_csv_rows(per_date_path)
    return {
        "review_summary_file": str(per_date_path),
        "review_summary_all_file": str(all_path),
        "review_summary_rows": current_rows,
        "review_summary_all_rows": total_rows,
    }


def count_csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return sum(1 for _ in reader)


def rebuild_all_review_summary(review_dir: Path, output_path: Path) -> int:
    files = sorted(review_dir.glob("[0-9][0-9][0-9][0-9][0-9][0-9]_match_content_summary.csv"))
    base_fields: Optional[List[str]] = None
    rows: List[Dict[str, str]] = []

    for path in files:
        date = path.name.split("_", 1)[0]
        with path.open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fields = list(reader.fieldnames or [])
            if base_fields is None:
                base_fields = fields
            elif fields != base_fields:
                raise DriveRunnerError(
                    f"Cannot aggregate review summaries with different headers: {path}"
                )
            for row in reader:
                rows.append({"export_date": date, **row})

    fieldnames = ["export_date", *(base_fields or [])]
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)
