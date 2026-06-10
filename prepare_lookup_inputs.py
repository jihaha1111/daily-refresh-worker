#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Prepare GitHub Actions lookup inputs from a Threads raw export."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from extract_coupang_body_link_enhanced import run_extraction
from threads_coupang_pipeline.drive_runner import validate_date_token
from threads_coupang_pipeline.lookup_prepare import (
    DEFAULT_BODY_LIKE_THRESHOLD,
    build_lookup_prepare_result,
    read_csv_rows,
    write_lookup_prepare_outputs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run existing body/link matching on a raw Threads export and "
            "write view-count and AF lookup input files."
        )
    )
    parser.add_argument(
        "--date",
        required=True,
        help="YYMMDD export key, for example 260430.",
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Local threads-source-YYMMDD.json path.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory for prepared lookup files.",
    )
    parser.add_argument(
        "--body-like-threshold",
        type=int,
        default=DEFAULT_BODY_LIKE_THRESHOLD,
        help=f"Minimum body_like_count to queue view/AF lookup. Default: {DEFAULT_BODY_LIKE_THRESHOLD}.",
    )
    parser.add_argument(
        "--source-uri",
        default=None,
        help="Optional source URI to record in run-manifest, e.g. configured RAW_REMOTE_DIR",
    )
    parser.add_argument(
        "--github-run-id",
        default=None,
        help="Optional GitHub Actions run id to record in run-manifest.",
    )
    parser.add_argument(
        "--github-sha",
        default=None,
        help="Optional GitHub commit SHA to record in run-manifest.",
    )
    return parser.parse_args()


def optional_manifest_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def run_prepare_lookup_inputs(
    *,
    date: str,
    input_path: Path,
    output_dir: Path,
    body_like_threshold: int = DEFAULT_BODY_LIKE_THRESHOLD,
    source_uri: Optional[str] = None,
    github_run_id: Optional[str] = None,
    github_sha: Optional[str] = None,
) -> Dict[str, Any]:
    export_key = validate_date_token(date)
    if body_like_threshold < 0:
        raise SystemExit("--body-like-threshold must be non-negative")
    if not input_path.exists():
        raise SystemExit(f"input file not found: {input_path}")

    with tempfile.TemporaryDirectory(prefix=f"prepare_lookup_{export_key}_") as tmp:
        extraction_dir = Path(tmp) / "extraction"
        extraction_summary = run_extraction(
            input_path=input_path,
            output_prefix=export_key,
            output_dir=extraction_dir,
            output_shape="both",
        )
        match_summary_path = extraction_dir / f"{export_key}_match_content_summary.csv"
        item_links_path = extraction_dir / f"{export_key}_item_links.csv"
        items_core_path = extraction_dir / f"{export_key}_items_core.csv"
        matches_core_path = extraction_dir / f"{export_key}_matches_core.csv"
        exceptions_core_path = extraction_dir / f"{export_key}_exceptions_core.csv"
        rows = read_csv_rows(match_summary_path)

        result = build_lookup_prepare_result(
            rows,
            export_key=export_key,
            body_like_threshold=body_like_threshold,
            item_links_rows=read_csv_rows(item_links_path),
            items_core_rows=read_csv_rows(items_core_path),
            matches_core_rows=read_csv_rows(matches_core_path),
            exceptions_core_rows=read_csv_rows(exceptions_core_path),
        )

        extra_manifest: Dict[str, Any] = {
            "source_uri": optional_manifest_value(source_uri) or str(input_path),
            "github_run_id": optional_manifest_value(github_run_id),
            "github_sha": optional_manifest_value(github_sha),
            "matching_policy": extraction_summary.get("matching_policy"),
            "raw_total_items": extraction_summary.get("total_items"),
            "raw_body_candidates": extraction_summary.get("body_candidates"),
            "raw_link_reply_candidates": extraction_summary.get("link_reply_candidates"),
            "raw_matches": extraction_summary.get("matches"),
            "raw_exceptions": extraction_summary.get("exceptions"),
            "match_confidence_counts": extraction_summary.get("match_confidence_counts", {}),
        }
        manifest = write_lookup_prepare_outputs(
            result,
            output_dir,
            source_match_summary=match_summary_path,
            extra_manifest=extra_manifest,
        )

    return manifest


def main() -> None:
    args = parse_args()
    manifest = run_prepare_lookup_inputs(
        date=args.date,
        input_path=Path(args.input),
        output_dir=Path(args.output_dir),
        body_like_threshold=args.body_like_threshold,
        source_uri=args.source_uri,
        github_run_id=args.github_run_id,
        github_sha=args.github_sha,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
