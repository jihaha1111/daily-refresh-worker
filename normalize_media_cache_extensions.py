#!/usr/bin/env python3
"""Normalize cached media file extensions from stored MIME metadata."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from threads_coupang_pipeline.media_cache import (  # noqa: E402
    normalize_media_cache_extensions,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rename cached media files with generic extensions, such as .bin, "
            "to MIME-derived extensions and update media_cache_assets metadata."
        )
    )
    parser.add_argument("--export-key", required=True, help="Export key, e.g. 260430.")
    parser.add_argument("--prefix", required=True, help="Output prefix, e.g. 260430.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory containing {prefix}_media_cache_assets.csv.",
    )
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=Path("private/media-cache"),
        help="Root directory for local cached media files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report planned updates without renaming files or rewriting metadata.",
    )
    return parser


def summary_to_dict(summary) -> Dict[str, Any]:
    return {
        "metadata_csv": str(summary.metadata_csv),
        "export_key": summary.export_key,
        "dry_run": summary.dry_run,
        "rows": summary.rows,
        "cached_rows": summary.cached_rows,
        "updated_rows": summary.updated_rows,
        "renamed_files": summary.renamed_files,
        "missing_files": summary.missing_files,
        "errors": summary.errors,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    metadata_csv = args.output_dir / f"{args.prefix}_media_cache_assets.csv"
    summary = normalize_media_cache_extensions(
        metadata_csv=metadata_csv,
        cache_root=args.cache_root,
        export_key=args.export_key,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary_to_dict(summary), ensure_ascii=False, indent=2))
    return 1 if summary.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
