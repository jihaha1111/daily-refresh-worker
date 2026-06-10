#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Regenerate a media refresh queue and download queued media into private cache."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from analyze_coupang_performance import run_analysis  # noqa: E402
from extract_coupang_body_link_enhanced import run_extraction  # noqa: E402
from threads_coupang_pipeline.config import (  # noqa: E402
    GOOGLE_DRIVE_MEDIA_CACHE_RCLONE_REMOTE,
    GOOGLE_DRIVE_RAW_RCLONE_REMOTE,
    GOOGLE_DRIVE_RUNS_RCLONE_REMOTE,
)
from threads_coupang_pipeline.content_classification import (  # noqa: E402
    CONTENT_SCOPE_CHOICES,
    CONTENT_SCOPE_NON_RECIPE,
)
from threads_coupang_pipeline.drive_runner import (  # noqa: E402
    DriveRunnerError,
    copy_drive_file,
    join_remote_path,
    validate_date_token,
)
from threads_coupang_pipeline.media_cache import split_grade_tokens  # noqa: E402
from threads_coupang_pipeline.media_refresh_queue import prepare_media_refresh_queue  # noqa: E402
from threads_coupang_pipeline.media_refresh_worker import (  # noqa: E402
    run_media_refresh_worker,
    sanitized_summary,
    write_worker_summary_json,
)


DEFAULT_PERFORMANCE_GRADES = ["Gold", "S", "A", "B"]


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download queued Gold/S/A/B media into private media cache."
    )
    parser.add_argument("--date", required=True, help="YYMMDD target date.")
    parser.add_argument("--export-key", default=None, help="Logical export key. Defaults to --date.")
    parser.add_argument("--prefix", default=None, help="Output prefix. Defaults to --date.")
    parser.add_argument("--drive-raw-dir", default=GOOGLE_DRIVE_RAW_RCLONE_REMOTE)
    parser.add_argument("--drive-runs-dir", default=GOOGLE_DRIVE_RUNS_RCLONE_REMOTE)
    parser.add_argument("--drive-media-cache-dir", default=GOOGLE_DRIVE_MEDIA_CACHE_RCLONE_REMOTE)
    parser.add_argument("--rclone-bin", default="rclone")
    parser.add_argument("--work-dir", default=None, help="Working directory. Defaults to a temp dir.")
    parser.add_argument("--output-dir", default=None, help="Extraction output dir. Defaults to {work-dir}/outputs/{date}.")
    parser.add_argument("--cache-root", default=None, help="Private cache root. Defaults to {work-dir}/private/media-cache.")
    parser.add_argument("--max-rows", type=non_negative_int, default=20)
    parser.add_argument("--max-parallel", type=positive_int, default=6)
    parser.add_argument("--sleep-seconds", type=non_negative_float, default=0.0)
    parser.add_argument("--timeout-seconds", type=non_negative_float, default=20.0)
    parser.add_argument("--fetch-attempts", type=positive_int, default=2)
    parser.add_argument("--drive-retry-attempts", type=positive_int, default=5)
    parser.add_argument("--drive-retry-initial-delay-seconds", type=non_negative_float, default=30.0)
    parser.add_argument("--drive-retry-backoff-multiplier", type=non_negative_float, default=2.0)
    parser.add_argument("--drive-retry-max-delay-seconds", type=non_negative_float, default=240.0)
    parser.add_argument("--performance-grades", nargs="*", default=DEFAULT_PERFORMANCE_GRADES)
    parser.add_argument(
        "--content-scope",
        choices=CONTENT_SCOPE_CHOICES,
        default=CONTENT_SCOPE_NON_RECIPE,
        help="Performance label content scope. Defaults to non_recipe.",
    )
    parser.add_argument("--upload-to-drive", action="store_true")
    parser.add_argument("--summary-json", default=None)
    return parser.parse_args(argv)


def run_with_work_dir(args: argparse.Namespace, work_dir: Path) -> dict:
    date = validate_date_token(args.date)
    export_key = args.export_key or date
    prefix = args.prefix or date
    output_dir = Path(args.output_dir) if args.output_dir else work_dir / "outputs" / date
    cache_root = Path(args.cache_root) if args.cache_root else work_dir / "private" / "media-cache"
    raw_path = work_dir / f"threads-source-{date}.json"
    view_path = work_dir / f"threads-viewcount-{date}.csv"
    source_uri = join_remote_path(args.drive_raw_dir, raw_path.name)
    view_uri = join_remote_path(join_remote_path(args.drive_runs_dir, date), view_path.name)

    copy_drive_file(
        args.rclone_bin,
        source_uri,
        raw_path,
        retry_attempts=args.drive_retry_attempts,
        retry_initial_delay_seconds=args.drive_retry_initial_delay_seconds,
        retry_backoff_multiplier=args.drive_retry_backoff_multiplier,
        retry_max_delay_seconds=args.drive_retry_max_delay_seconds,
    )
    copy_drive_file(
        args.rclone_bin,
        view_uri,
        view_path,
        retry_attempts=args.drive_retry_attempts,
        retry_initial_delay_seconds=args.drive_retry_initial_delay_seconds,
        retry_backoff_multiplier=args.drive_retry_backoff_multiplier,
        retry_max_delay_seconds=args.drive_retry_max_delay_seconds,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    run_extraction(
        input_path=raw_path,
        output_prefix=prefix,
        output_dir=output_dir,
        output_shape="both",
        view_counts_file=view_path,
    )
    run_analysis(
        input_path=output_dir / f"{prefix}_match_content_summary.csv",
        output_dir=output_dir,
        output_prefix=prefix,
        content_scope=args.content_scope,
    )
    performance_grades = split_grade_tokens(args.performance_grades)
    if not performance_grades:
        raise SystemExit("--performance-grades must include at least one grade.")
    queue_csv = output_dir / f"{prefix}_media_refresh_queue.csv"
    prepare_media_refresh_queue(
        performance_labels_csv=output_dir / f"{prefix}_performance_labels.csv",
        media_assets_csv=output_dir / f"{prefix}_media_assets.csv",
        items_core_csv=output_dir / f"{prefix}_items_core.csv",
        output_csv=queue_csv,
        export_key=export_key,
        performance_grades=performance_grades,
    )
    output_csv = output_dir / f"{prefix}_media_cache_assets.csv"
    worker_summary = run_media_refresh_worker(
        export_key=export_key,
        queue_csv=queue_csv,
        media_assets_csv=output_dir / f"{prefix}_media_assets.csv",
        output_csv=output_csv,
        cache_root=cache_root,
        max_rows=args.max_rows,
        max_parallel=args.max_parallel,
        sleep_seconds=args.sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        fetch_attempts=args.fetch_attempts,
        upload_to_drive=args.upload_to_drive,
        prefix=prefix,
        output_dir=output_dir,
        drive_media_cache_dir=args.drive_media_cache_dir,
        rclone_bin=args.rclone_bin,
        drive_retry_attempts=args.drive_retry_attempts,
        drive_retry_initial_delay_seconds=args.drive_retry_initial_delay_seconds,
        drive_retry_backoff_multiplier=args.drive_retry_backoff_multiplier,
        drive_retry_max_delay_seconds=args.drive_retry_max_delay_seconds,
    )
    summary_json = (
        Path(args.summary_json)
        if args.summary_json
        else output_dir / f"{prefix}_media_refresh_worker_summary.json"
    )
    write_worker_summary_json(summary_json, worker_summary)
    summary = sanitized_summary(worker_summary)
    summary.update(
        {
            "date": date,
            "prefix": prefix,
            "performance_grades": performance_grades,
            "content_scope": args.content_scope,
            "source_uri": source_uri,
            "view_count_uri": view_uri,
            "queue_csv": str(queue_csv),
            "summary_json": str(summary_json),
        }
    )
    summary_json.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        if args.work_dir:
            work_dir = Path(args.work_dir)
            work_dir.mkdir(parents=True, exist_ok=True)
            summary = run_with_work_dir(args, work_dir)
        else:
            with tempfile.TemporaryDirectory(prefix=f"media_refresh_{args.date}_") as tmp:
                summary = run_with_work_dir(args, Path(tmp))
    except DriveRunnerError as exc:
        raise SystemExit(str(exc)) from exc

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
