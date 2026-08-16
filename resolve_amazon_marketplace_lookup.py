#!/usr/bin/env python3
"""Resolve a private Amazon short-link queue on an approved network runner."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional, Sequence


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.amazon_marketplace_resolver import (  # noqa: E402
    AmazonMarketplaceResolverError,
    read_amazon_marketplace_lookup_input,
    resolve_amazon_marketplace_rows,
    validate_approved_runner_environment,
    write_amazon_marketplace_outputs,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Follow approved Amazon short-link redirects and write private "
            "marketplace and Associates evidence. Run only on the approved runner."
        )
    )
    parser.add_argument("--run-key", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-redirects", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--fetch-attempts", type=int, default=2)
    parser.add_argument("--retry-sleep-seconds", type=float, default=1.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--progress-interval", type=int, default=25)
    parser.add_argument("--user-agent", default="Mozilla/5.0")
    parser.add_argument("--github-run-id")
    parser.add_argument("--github-sha")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = Path(args.input)
    try:
        validate_approved_runner_environment(os.environ)
        rows = read_amazon_marketplace_lookup_input(input_path)
        result = resolve_amazon_marketplace_rows(
            rows,
            run_key=args.run_key,
            max_redirects=args.max_redirects,
            timeout_seconds=args.timeout_seconds,
            fetch_attempts=args.fetch_attempts,
            retry_sleep_seconds=args.retry_sleep_seconds,
            sleep_seconds=args.sleep_seconds,
            max_workers=args.max_workers,
            progress_interval=args.progress_interval,
            progress_stream=sys.stderr,
            user_agent=args.user_agent,
        )
        write_amazon_marketplace_outputs(
            result,
            Path(args.output_dir),
            repo_root=ROOT,
            source_input=input_path,
            max_redirects=args.max_redirects,
            timeout_seconds=args.timeout_seconds,
            fetch_attempts=args.fetch_attempts,
            retry_sleep_seconds=args.retry_sleep_seconds,
            sleep_seconds=args.sleep_seconds,
            max_workers=args.max_workers,
            github_run_id=args.github_run_id,
            github_sha=args.github_sha,
        )
        sys.stdout.write(
            json.dumps(result.public_report(), ensure_ascii=False, indent=2, sort_keys=True)
            + "\n"
        )
        return 0
    except (AmazonMarketplaceResolverError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
