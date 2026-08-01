# Daily Refresh Worker

Public Drive-only ingestion worker for running expensive refresh jobs on GitHub Actions.

This repository intentionally contains only the worker surface needed to create Drive artifacts for date-based ingestion. It does not contain database loading, dashboard code, private raw data, generated outputs, media cache artifacts, Drive folder URLs, Drive folder IDs, credentials, or project-specific analysis documents.

## Required Repository Configuration

Secrets:

- `RCLONE_CONFIG_GDRIVE`

Variables:

- `RAW_REMOTE_DIR`: rclone path to the raw input directory
- `RUNS_REMOTE_DIR`: rclone path to the per-date run artifact directory
- `MEDIA_CACHE_REMOTE_DIR`: rclone path to the per-date media cache directory

Do not commit real Drive URLs, folder IDs, tokens, raw files, generated CSVs, or media files.

## Operating Flow

Run these Actions in order for a target `YYMMDD` date:

1. `Prepare Lookup Inputs`
2. `Resolve Link Lookup`
3. `Resolve View Metrics`
4. `Refresh Media Cache`

All operational input, shard, result, retry, and media artifacts are read from and written to Drive. Operational workflows must not use GitHub artifact storage.

After the public Actions finish, run the private repository local DB load with `GH_REPO` pointing at this repository:

```bash
GH_REPO=jihaha1111/daily-refresh-worker \
python3 run_threads_to_postgres.py \
  --date YYMMDD \
  --run-id threads-YYMMDD-public-actions \
  --mode local-temp \
  --database-url postgresql:///threads_coupang \
  --write-status \
  --cache-media \
  --media-cache-mode actions \
  --media-cache-max-parallel 6 \
  --media-cache-sleep-seconds 0 \
  --media-cache-performance-grades Gold S A B \
  --media-cache-content-scope non_recipe
```

## View Metrics Defaults

For full operational runs use:

- `max_rows=0`
- `max_parallel=20`
- `target_shard_count=20`
- `shard_size=9999`
- `probe_mode=scrapling-dynamic`
- `timeout_seconds=20`
- `fetch_attempts=2`
- `missing_count_attempts=2`
- `missing_count_sleep_seconds=1`
- `sleep_seconds=0`
- `progress_interval=25`
- `log_diagnostics=false`
- `upload_to_drive=true`

The workflow pins `apify-fingerprint-datapoints==0.13.0` and smoke-imports
the selected Scrapling fetcher before installing its browser runtime. This
prevents a known header-generation import failure in the newer transitive
dataset release. After merged results and retry state are uploaded to Drive,
the workflow fails if every input row ended as `request_error` so an unusable
data result cannot appear operationally successful.

## Media Expiry Policy

Media URLs can expire. The media refresh worker preserves non-cached rows as warning metadata instead of failing the run. Expected warning statuses include `expired`, `forbidden`, `download_failed`, and `missing_source_url`.

If non-cached rows are high, rerun only `Refresh Media Cache` later and then rerun the private local DB load for that date.

## Local Checks

```bash
python3 scripts/check_sensitive_strings.py
python3 -m unittest discover -s tests
python3 -m py_compile \
  extract_coupang_body_link_enhanced.py \
  analyze_coupang_performance.py \
  prepare_lookup_inputs.py \
  resolve_af_lookup_inputs.py \
  resolve_view_lookup_inputs.py \
  run_media_refresh_download.py \
  prepare_media_refresh_queue.py \
  sync_media_cache_drive.py \
  normalize_media_cache_extensions.py \
  src/threads_coupang_pipeline/*.py
```
