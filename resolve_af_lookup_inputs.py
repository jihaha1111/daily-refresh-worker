#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Resolve prepared Coupang AF lookup inputs into Drive-storable evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from threads_coupang_pipeline.af_lookup_resolver import (  # noqa: E402
    read_af_lookup_input,
    resolve_af_lookup_rows,
    write_af_lookup_outputs,
)
from threads_coupang_pipeline.drive_runner import validate_date_token  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Follow Coupang short-link redirects from af-link-lookup-input-YYMMDD.csv "
            "and write af_id result/account mapping files."
        )
    )
    parser.add_argument("--date", required=True, help="YYMMDD export key, for example 260430.")
    parser.add_argument("--input", required=True, help="Local af-link-lookup-input-YYMMDD.csv path.")
    parser.add_argument("--output-dir", required=True, help="Directory for resolved AF files.")
    parser.add_argument("--max-redirects", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=25,
        help="Write sanitized progress logs every N rows. Use 0 to disable.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Maximum concurrent link requests. Default: 8.",
    )
    parser.add_argument(
        "--user-agent",
        default="Mozilla/5.0",
        help="User-Agent used by the GitHub Actions resolver.",
    )
    parser.add_argument("--source-uri", default=None)
    parser.add_argument("--github-run-id", default=None)
    parser.add_argument("--github-sha", default=None)
    return parser.parse_args()


def optional_manifest_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def main() -> None:
    args = parse_args()
    export_key = validate_date_token(args.date)
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        raise SystemExit(f"input file not found: {input_path}")
    if args.max_redirects < 0:
        raise SystemExit("--max-redirects must be non-negative")
    if args.timeout_seconds <= 0:
        raise SystemExit("--timeout-seconds must be positive")
    if args.sleep_seconds < 0:
        raise SystemExit("--sleep-seconds must be non-negative")
    if args.progress_interval < 0:
        raise SystemExit("--progress-interval must be non-negative")
    if args.max_workers <= 0:
        raise SystemExit("--max-workers must be positive")

    rows = read_af_lookup_input(input_path)
    print(
        (
            f"Starting AF lookup: rows={len(rows)} date={export_key} "
            f"max_redirects={args.max_redirects} "
            f"max_workers={args.max_workers}"
        ),
        file=sys.stderr,
    )
    result = resolve_af_lookup_rows(
        rows,
        export_key=export_key,
        max_redirects=args.max_redirects,
        timeout_seconds=args.timeout_seconds,
        sleep_seconds=args.sleep_seconds,
        user_agent=args.user_agent,
        progress_interval=args.progress_interval,
        progress_stream=sys.stderr,
        max_workers=args.max_workers,
    )
    print(
        (
            f"Finished AF lookup: resolved={result.resolved_rows} "
            f"failed={result.failed_rows} unique_af_ids={result.unique_af_ids} "
            f"account_pair_rows={result.account_pair_rows}"
        ),
        file=sys.stderr,
    )
    extra_manifest: Dict[str, Any] = {
        "source_uri": optional_manifest_value(args.source_uri) or str(input_path),
        "github_run_id": optional_manifest_value(args.github_run_id),
        "github_sha": optional_manifest_value(args.github_sha),
        "max_redirects": args.max_redirects,
        "timeout_seconds": args.timeout_seconds,
        "sleep_seconds": args.sleep_seconds,
        "progress_interval": args.progress_interval,
        "max_workers": args.max_workers,
    }
    manifest = write_af_lookup_outputs(
        result,
        output_dir,
        source_input=input_path,
        extra_manifest=extra_manifest,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
