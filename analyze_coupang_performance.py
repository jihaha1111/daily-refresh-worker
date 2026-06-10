#!/usr/bin/env python3
"""Build performance labels and manual tagging samples from match summaries."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.content_classification import (  # noqa: E402
    CONTENT_SCOPE_CHOICES,
    CONTENT_SCOPE_NON_RECIPE,
    add_content_classification,
    row_matches_content_scope,
)
from threads_coupang_pipeline.performance import (  # noqa: E402
    METRIC_VERSION_V2_REVENUE_PROXY,
    PERFORMANCE_LABEL_FIELDS,
    STABLE_VIEW_THRESHOLD,
    TAGGING_SAMPLE_FIELDS,
    build_performance_labels,
    grade_counts,
    select_tagging_sample,
    type_counts,
)


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def normalize_for_csv_value(value: Any) -> Any:
    if isinstance(value, list):
        return " | ".join(map(str, value))
    if isinstance(value, tuple):
        return " | ".join(map(str, value))
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return value


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: Optional[List[str]] = None) -> None:
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: normalize_for_csv_value(row.get(key, "")) for key in fieldnames})


def infer_output_prefix(input_path: Path) -> str:
    name = input_path.name
    suffix = "_match_content_summary.csv"
    if name.endswith(suffix):
        return name[: -len(suffix)]
    return input_path.stem


def build_summary(
    input_path: Path,
    output_prefix: str,
    input_row_count: int,
    labels: List[Dict[str, Any]],
    sample: List[Dict[str, Any]],
    sample_summary: Dict[str, Any],
    stable_view_threshold: int,
    metric_version: str,
    content_scope: str,
    content_scope_row_count: int,
    input_content_category_counts: Dict[str, int],
    output_files: Dict[str, str],
) -> Dict[str, Any]:
    stable_rows = [
        row for row in labels if (row.get("body_view_count") or 0) >= stable_view_threshold
    ]
    score_rows = [row for row in labels if row.get("coupang_score") is not None]
    return {
        "input_file": str(input_path),
        "output_prefix": output_prefix,
        "metric_version": metric_version,
        "content_scope": content_scope,
        "stable_view_threshold": stable_view_threshold,
        "input_rows": input_row_count,
        "content_scope_rows": content_scope_row_count,
        "performance_label_rows": len(labels),
        "stable_body_view_rows": len(stable_rows),
        "coupang_score_rows": len(score_rows),
        "tagging_sample_rows": len(sample),
        "input_content_category_counts": input_content_category_counts,
        "performance_grade_counts": grade_counts(labels),
        "performance_type_counts": type_counts(labels),
        "tagging_sample_summary": sample_summary,
        "output_files": output_files,
    }


def run_analysis(
    input_path: Path,
    output_dir: Optional[Path] = None,
    output_prefix: Optional[str] = None,
    sample_size: int = 150,
    stable_view_threshold: int = STABLE_VIEW_THRESHOLD,
    content_scope: str = CONTENT_SCOPE_NON_RECIPE,
) -> Dict[str, Any]:
    rows = read_csv_rows(input_path)
    classified_rows = [add_content_classification(row) for row in rows]
    scoped_rows = [
        row for row in classified_rows if row_matches_content_scope(row, content_scope)
    ]
    input_content_category_counts: Dict[str, int] = {}
    for row in classified_rows:
        category = str(row.get("content_category", "") or "")
        input_content_category_counts[category] = input_content_category_counts.get(category, 0) + 1

    output_dir = output_dir or input_path.parent
    output_prefix = output_prefix or infer_output_prefix(input_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    labels = build_performance_labels(
        scoped_rows,
        stable_view_threshold=stable_view_threshold,
    )
    sample, sample_summary = select_tagging_sample(labels, sample_size=sample_size)

    labels_csv = output_dir / f"{output_prefix}_performance_labels.csv"
    labels_json = output_dir / f"{output_prefix}_performance_labels.json"
    sample_csv = output_dir / f"{output_prefix}_tagging_sample.csv"
    sample_json = output_dir / f"{output_prefix}_tagging_sample.json"
    summary_json = output_dir / f"{output_prefix}_performance_summary.json"

    write_csv(labels_csv, labels, fieldnames=list(PERFORMANCE_LABEL_FIELDS))
    write_json(labels_json, labels)
    write_csv(sample_csv, sample, fieldnames=list(TAGGING_SAMPLE_FIELDS))
    write_json(sample_json, sample)

    output_files = {
        "performance_labels_csv": str(labels_csv),
        "performance_labels_json": str(labels_json),
        "tagging_sample_csv": str(sample_csv),
        "tagging_sample_json": str(sample_json),
        "performance_summary_json": str(summary_json),
    }
    summary = build_summary(
        input_path=input_path,
        output_prefix=output_prefix,
        input_row_count=len(rows),
        labels=labels,
        sample=sample,
        sample_summary=sample_summary,
        stable_view_threshold=stable_view_threshold,
        metric_version=METRIC_VERSION_V2_REVENUE_PROXY,
        content_scope=content_scope,
        content_scope_row_count=len(scoped_rows),
        input_content_category_counts=input_content_category_counts,
        output_files=output_files,
    )
    write_json(summary_json, summary)
    return summary


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Coupang performance labels and manual tagging samples."
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to a *_match_content_summary.csv file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory. Defaults to the input file directory.",
    )
    parser.add_argument(
        "--output-prefix",
        default=None,
        help="Output prefix. Defaults to the input prefix before _match_content_summary.csv.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=150,
        help="Number of rows to include in the manual tagging sample.",
    )
    parser.add_argument(
        "--stable-view-threshold",
        type=int,
        default=STABLE_VIEW_THRESHOLD,
        help="Minimum body_view_count for stable performance grading.",
    )
    parser.add_argument(
        "--content-scope",
        choices=CONTENT_SCOPE_CHOICES,
        default=CONTENT_SCOPE_NON_RECIPE,
        help="Rows to include in this analysis output.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    summary = run_analysis(
        input_path=args.input,
        output_dir=args.output_dir,
        output_prefix=args.output_prefix,
        sample_size=args.sample_size,
        stable_view_threshold=args.stable_view_threshold,
        content_scope=args.content_scope,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
