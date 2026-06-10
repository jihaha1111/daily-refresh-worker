# Operations

This repository is a public Drive-only worker. The canonical database load and dashboard remain outside this repository.

## One-Date Runbook

1. Confirm the date token is six digits.
2. Confirm repository variables are configured:
   - `RAW_REMOTE_DIR`
   - `RUNS_REMOTE_DIR`
   - `MEDIA_CACHE_REMOTE_DIR`
3. Confirm `RCLONE_CONFIG_GDRIVE` is configured as a repository secret.
4. Run `Prepare Lookup Inputs`.
5. Run `Resolve Link Lookup`.
6. Run `Resolve View Metrics`.
7. Run `Refresh Media Cache`.
8. In the private repository, run the local DB load using `GH_REPO=jihaha1111/daily-refresh-worker`.
9. Verify private DB status, row counts, media cache coverage, and review summary regeneration.

## Operational Data Policy

Operational workflows use Drive for all input, shard, result, retry-state, and media cache files.

Do not add operational GitHub artifact upload/download steps. Synthetic CI outputs are acceptable only when they cannot contain real operational data.

## Repair Policy

If the view metrics workflow leaves retry rows, rerun `Resolve View Metrics` with `source_run_id` set to the previous run id.

If media cache coverage is low because URLs expired or were forbidden, rerun `Refresh Media Cache` for the same date and then rerun the private local DB load.
