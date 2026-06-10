#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Prepare a Gold/S/A/B media refresh queue for future download workers."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from threads_coupang_pipeline.media_cache import split_grade_tokens  # noqa: E402
from threads_coupang_pipeline.media_refresh_queue import (  # noqa: E402
    prepare_media_refresh_queue,
)


DEFAULT_PERFORMANCE_GRADES = ["Gold", "S", "A", "B"]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create an ignored media refresh queue from selected performance grades."
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        default=".",
        help="Directory containing {prefix}_performance_labels.csv, media_assets, and items_core.",
    )
    parser.add_argument("--prefix", "-p", required=True, help="Extraction output prefix.")
    parser.add_argument(
        "--export-key",
        default=None,
        help="Logical export key. Defaults to --prefix.",
    )
    parser.add_argument(
        "--performance-labels-csv",
        default=None,
        help="Optional explicit performance labels CSV path.",
    )
    parser.add_argument(
        "--media-assets-csv",
        default=None,
        help="Optional explicit media_assets CSV path.",
    )
    parser.add_argument(
        "--items-core-csv",
        default=None,
        help="Optional explicit items_core CSV path.",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Optional output CSV path. Defaults to {output-dir}/{prefix}_media_refresh_queue.csv.",
    )
    parser.add_argument(
        "--performance-grades",
        nargs="*",
        default=DEFAULT_PERFORMANCE_GRADES,
        help=(
            "Performance grades to queue. Accepts space-separated or comma-separated "
            "values. Defaults to Gold S A B."
        ),
    )
    return parser.parse_args(argv)


def required_path(path: Path, label: str) -> Path:
    if not path.exists():
        raise SystemExit(f"{label} not found: {path}")
    return path


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    prefix = args.prefix
    export_key = args.export_key or prefix
    performance_labels_csv = required_path(
        Path(args.performance_labels_csv)
        if args.performance_labels_csv
        else output_dir / f"{prefix}_performance_labels.csv",
        "performance labels CSV",
    )
    media_assets_csv = required_path(
        Path(args.media_assets_csv)
        if args.media_assets_csv
        else output_dir / f"{prefix}_media_assets.csv",
        "media assets CSV",
    )
    items_core_csv = required_path(
        Path(args.items_core_csv)
        if args.items_core_csv
        else output_dir / f"{prefix}_items_core.csv",
        "items core CSV",
    )
    output_csv = (
        Path(args.output_csv)
        if args.output_csv
        else output_dir / f"{prefix}_media_refresh_queue.csv"
    )
    performance_grades = split_grade_tokens(args.performance_grades)
    if not performance_grades:
        raise SystemExit("--performance-grades must include at least one grade.")
    summary = prepare_media_refresh_queue(
        performance_labels_csv=performance_labels_csv,
        media_assets_csv=media_assets_csv,
        items_core_csv=items_core_csv,
        output_csv=output_csv,
        export_key=export_key,
        performance_grades=performance_grades,
    )
    payload = summary.__dict__.copy()
    payload["output_csv"] = str(summary.output_csv)
    payload["performance_grades"] = performance_grades
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
