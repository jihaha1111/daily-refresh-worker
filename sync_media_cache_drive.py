#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Push and pull private media-cache artifacts to Google Drive."""

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

from threads_coupang_pipeline.config import GOOGLE_DRIVE_MEDIA_CACHE_RCLONE_REMOTE  # noqa: E402
from threads_coupang_pipeline.drive_runner import DriveRunnerError  # noqa: E402
from threads_coupang_pipeline.media_cache_drive import (  # noqa: E402
    pull_media_cache_from_drive,
    push_media_cache_to_drive,
    summary_to_dict,
)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--export-key", required=True, help="Logical export key, e.g. 260531.")
    parser.add_argument("--prefix", required=True, help="Extraction output prefix, e.g. 260531.")
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory containing/restoring {prefix}_media_cache_assets.csv.",
    )
    parser.add_argument(
        "--cache-root",
        default=str(PROJECT_ROOT / "private" / "media-cache"),
        help="Ignored private media cache root.",
    )
    parser.add_argument(
        "--drive-media-cache-dir",
        default=GOOGLE_DRIVE_MEDIA_CACHE_RCLONE_REMOTE,
        help="Drive media-cache rclone root.",
    )
    parser.add_argument(
        "--rclone-bin",
        default="rclone",
        help="rclone executable. Defaults to rclone on PATH.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned rclone operations without uploading or downloading.",
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync ignored private media-cache artifacts with Google Drive."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    push_parser = subparsers.add_parser("push", help="Upload local media cache to Drive.")
    add_common_args(push_parser)
    pull_parser = subparsers.add_parser("pull", help="Restore media cache from Drive.")
    add_common_args(pull_parser)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    kwargs = {
        "export_key": args.export_key,
        "prefix": args.prefix,
        "output_dir": Path(args.output_dir),
        "cache_root": Path(args.cache_root),
        "drive_media_cache_dir": args.drive_media_cache_dir,
        "rclone_bin": args.rclone_bin,
        "dry_run": args.dry_run,
    }
    if args.command == "push":
        try:
            summary = push_media_cache_to_drive(**kwargs)
        except DriveRunnerError as exc:
            raise SystemExit(str(exc)) from exc
    elif args.command == "pull":
        try:
            summary = pull_media_cache_from_drive(**kwargs)
        except DriveRunnerError as exc:
            raise SystemExit(str(exc)) from exc
    else:
        raise SystemExit(f"unsupported command: {args.command}")
    print(json.dumps(summary_to_dict(summary), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
