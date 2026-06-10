#!/usr/bin/env python3
"""Resolve prepared Threads view-count lookup inputs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.lookup_prepare import write_csv_rows, write_json  # noqa: E402
from threads_coupang_pipeline.view_lookup_resolver import (  # noqa: E402
    PROBE_MODES,
    VIEW_LOOKUP_STATE_FIELDS,
    merge_view_lookup_outputs,
    prepare_initial_state,
    prepare_retry_state,
    probe_lookup_rows,
    read_numbered_rows,
    split_numbered_rows,
)


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


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def print_summary(summary: Dict[str, Any]) -> None:
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


def cmd_prepare_initial(args: argparse.Namespace) -> int:
    summary = prepare_initial_state(
        Path(args.input),
        Path(args.output_dir),
        max_rows=args.max_rows,
    )
    print_summary(summary)
    return 0


def cmd_prepare_retry(args: argparse.Namespace) -> int:
    summary = prepare_retry_state(
        Path(args.previous_state_dir),
        Path(args.output_dir),
        max_rows=args.max_rows,
    )
    print_summary(summary)
    return 0


def cmd_split_shards(args: argparse.Namespace) -> int:
    summary = split_numbered_rows(
        Path(args.numbered),
        Path(args.output_dir),
        shard_size=args.shard_size,
        target_shard_count=args.target_shard_count,
    )
    if args.matrix_file:
        matrix_path = Path(args.matrix_file)
        matrix_path.parent.mkdir(parents=True, exist_ok=True)
        matrix_path.write_text(json.dumps(summary["matrix"], separators=(",", ":")) + "\n", encoding="utf-8")
    if args.summary_file:
        write_json(Path(args.summary_file), summary)
    print_summary(summary)
    return 0


def cmd_probe_shard(args: argparse.Namespace) -> int:
    rows = read_numbered_rows(Path(args.input))
    results = probe_lookup_rows(
        rows,
        timeout_seconds=args.timeout_seconds,
        sleep_seconds=args.sleep_seconds,
        user_agent=args.user_agent,
        probe_mode=args.probe_mode,
        fetch_attempts=args.fetch_attempts,
        retry_sleep_seconds=args.retry_sleep_seconds,
        missing_count_attempts=args.missing_count_attempts,
        missing_count_sleep_seconds=args.missing_count_sleep_seconds,
        stop_on_rate_limit=not args.no_stop_on_rate_limit,
        progress_interval=args.progress_interval,
        diagnostic_log=args.log_diagnostics,
        progress_stream=sys.stdout,
    )
    write_csv_rows(Path(args.output), results, VIEW_LOOKUP_STATE_FIELDS)
    status_counts: Dict[str, int] = {}
    for row in results:
        status = str(row.get("view_lookup_status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    print_summary(
        {
            "input_rows": len(rows),
            "output_rows": len(results),
            "probe_mode": args.probe_mode,
            "status_counts": status_counts,
        }
    )
    return 0


def find_shard_result_paths(shard_results_dir: Path) -> List[Path]:
    if not shard_results_dir.exists():
        return []
    return sorted(shard_results_dir.glob("*.csv"))


def cmd_merge(args: argparse.Namespace) -> int:
    previous_final_path = Path(args.previous_final) if args.previous_final else None
    shard_paths = find_shard_result_paths(Path(args.shard_results_dir)) if args.shard_results_dir else []
    extra_manifest = {
        "source_input_uri": args.source_input_uri or None,
        "source_map_uri": args.source_map_uri or None,
        "github_run_id": args.github_run_id or None,
        "github_sha": args.github_sha or None,
    }
    manifest = merge_view_lookup_outputs(
        export_key=args.date,
        all_input_path=Path(args.all_input),
        map_path=Path(args.map),
        output_dir=Path(args.output_dir),
        previous_final_path=previous_final_path,
        shard_result_paths=shard_paths,
        extra_manifest=extra_manifest,
    )
    print_summary(manifest)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare, probe, and merge Drive-backed Threads view-count lookup rows."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_initial = subparsers.add_parser("prepare-initial")
    prepare_initial.add_argument("--input", required=True, help="Local view-lookup-input-YYMMDD.csv path.")
    prepare_initial.add_argument("--output-dir", required=True, help="Output directory for all_input.csv and numbered.txt.")
    prepare_initial.add_argument("--max-rows", type=non_negative_int, default=0, help="Limit rows for runner smoke. 0 means all rows.")
    prepare_initial.set_defaults(func=cmd_prepare_initial)

    prepare_retry = subparsers.add_parser("prepare-retry")
    prepare_retry.add_argument("--previous-state-dir", required=True, help="Directory from previous retry-state artifact.")
    prepare_retry.add_argument("--output-dir", required=True, help="Output directory for restored state.")
    prepare_retry.add_argument("--max-rows", type=non_negative_int, default=0, help="Limit retry rows for runner smoke. 0 means all rows.")
    prepare_retry.set_defaults(func=cmd_prepare_retry)

    split_shards = subparsers.add_parser("split-shards")
    split_shards.add_argument("--numbered", required=True, help="numbered.txt path.")
    split_shards.add_argument("--output-dir", required=True, help="Directory for shard-N.tsv files.")
    split_shards.add_argument("--shard-size", type=positive_int, default=50)
    split_shards.add_argument(
        "--target-shard-count",
        type=non_negative_int,
        default=0,
        help="Optional exact balanced shard count. 0 keeps shard-size chunking.",
    )
    split_shards.add_argument("--matrix-file", help="Optional path for GitHub matrix JSON.")
    split_shards.add_argument("--summary-file", help="Optional split summary JSON path.")
    split_shards.set_defaults(func=cmd_split_shards)

    probe_shard = subparsers.add_parser("probe-shard")
    probe_shard.add_argument("--input", required=True, help="Shard TSV path with idx<TAB>url rows.")
    probe_shard.add_argument("--output", required=True, help="Shard result CSV path.")
    probe_shard.add_argument("--timeout-seconds", type=non_negative_float, default=10.0)
    probe_shard.add_argument("--sleep-seconds", type=non_negative_float, default=0.0)
    probe_shard.add_argument("--progress-interval", type=int, default=0)
    probe_shard.add_argument("--user-agent", default="Mozilla/5.0")
    probe_shard.add_argument("--probe-mode", choices=PROBE_MODES, default="static")
    probe_shard.add_argument("--fetch-attempts", type=positive_int, default=1)
    probe_shard.add_argument("--retry-sleep-seconds", type=non_negative_float, default=0.0)
    probe_shard.add_argument("--missing-count-attempts", type=positive_int, default=1)
    probe_shard.add_argument("--missing-count-sleep-seconds", type=non_negative_float, default=0.0)
    probe_shard.add_argument("--log-diagnostics", action="store_true")
    probe_shard.add_argument("--no-stop-on-rate-limit", action="store_true")
    probe_shard.set_defaults(func=cmd_probe_shard)

    merge = subparsers.add_parser("merge")
    merge.add_argument("--date", required=True, help="YYMMDD export key.")
    merge.add_argument("--all-input", required=True, help="all_input.csv path.")
    merge.add_argument("--map", required=True, help="view-lookup-map-YYMMDD.csv path.")
    merge.add_argument("--output-dir", required=True, help="Output directory for merged files.")
    merge.add_argument("--previous-final", help="Optional previous_final.csv path.")
    merge.add_argument("--shard-results-dir", help="Directory containing shard result CSV files.")
    merge.add_argument("--source-input-uri", default="")
    merge.add_argument("--source-map-uri", default="")
    merge.add_argument("--github-run-id", default="")
    merge.add_argument("--github-sha", default="")
    merge.set_defaults(func=cmd_merge)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
