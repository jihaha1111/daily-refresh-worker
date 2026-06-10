import csv
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.drive_runner import DriveRunnerError  # noqa: E402
from threads_coupang_pipeline.media_cache import MEDIA_CACHE_COLUMNS, content_sha256  # noqa: E402
from threads_coupang_pipeline.media_cache_drive import (  # noqa: E402
    pull_media_cache_from_drive,
    push_media_cache_to_drive,
    validate_media_cache_files,
    validate_safe_token,
)


def write_csv(path: Path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MEDIA_CACHE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_fake_rclone(path: Path) -> None:
    path.write_text(
        """#!/usr/bin/env python3
import os
import shutil
import sys
from pathlib import Path

remote_root = Path(os.environ["FAKE_RCLONE_REMOTE_ROOT"])

def map_path(value):
    if ":" not in value:
        return Path(value)
    _remote, rest = value.split(":", 1)
    return remote_root / rest.strip("/")

def copy_dir(source, destination, excludes):
    destination.mkdir(parents=True, exist_ok=True)
    for child in source.rglob("*"):
        if child.is_dir():
            continue
        if any(child.name.endswith(pattern.lstrip("*")) for pattern in excludes):
            continue
        rel = child.relative_to(source)
        target = destination / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(child, target)

args = sys.argv[1:]
command = args[0]
fail_state = os.environ.get("FAKE_RCLONE_FAIL_STATE")
failures = int(os.environ.get("FAKE_RCLONE_TRANSIENT_FAILURES", "0"))
if fail_state and failures:
    state = Path(fail_state)
    count = int(state.read_text() if state.exists() else "0")
    if count < failures:
        state.write_text(str(count + 1))
        print("googleapi: Error 403: Quota exceeded, reason: RATE_LIMIT_EXCEEDED", file=sys.stderr)
        sys.exit(1)

if command == "mkdir":
    map_path(args[1]).mkdir(parents=True, exist_ok=True)
elif command == "copyto":
    source = map_path(args[1])
    destination = map_path(args[2])
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
elif command == "copy":
    source = map_path(args[1])
    destination = map_path(args[2])
    excludes = []
    idx = 3
    while idx < len(args):
        if args[idx] == "--exclude":
            excludes.append(args[idx + 1])
            idx += 2
        else:
            idx += 1
    copy_dir(source, destination, excludes)
else:
    print(f"unsupported fake rclone command: {command}", file=sys.stderr)
    sys.exit(2)
""",
        encoding="utf-8",
    )
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR)


class MediaCacheDriveTests(unittest.TestCase):
    def build_cache_fixture(self, root: Path) -> tuple[Path, Path, Path]:
        export_key = "260531"
        prefix = "260531"
        cache_root = root / "private" / "media-cache"
        output_dir = root / "outputs" / export_key
        media_path = cache_root / export_key / "body" / "0_media_abc.jpg"
        media_path.parent.mkdir(parents=True, exist_ok=True)
        media_path.write_bytes(b"cached image bytes")
        metadata_csv = output_dir / f"{prefix}_media_cache_assets.csv"
        write_csv(
            metadata_csv,
            [
                {
                    "export_key": export_key,
                    "item_pk": "body",
                    "media_index": "0",
                    "cache_role": "media",
                    "asset_type": "image",
                    "input_kind": "image",
                    "source_url_hash": "not-a-url",
                    "cache_status": "cached",
                    "cache_rel_path": f"{export_key}/body/0_media_abc.jpg",
                    "mime_type": "image/jpeg",
                    "byte_size": str(media_path.stat().st_size),
                    "content_sha256": content_sha256(media_path),
                    "width": "300",
                    "height": "400",
                    "duration_seconds": "",
                    "cached_at": "2026-06-02T00:00:00+00:00",
                    "error": "",
                },
                {
                    "export_key": export_key,
                    "item_pk": "link",
                    "media_index": "0",
                    "cache_role": "media",
                    "asset_type": "image",
                    "input_kind": "image",
                    "source_url_hash": "",
                    "cache_status": "missing_source_url",
                    "cache_rel_path": "",
                    "mime_type": "",
                    "byte_size": "",
                    "content_sha256": "",
                    "width": "",
                    "height": "",
                    "duration_seconds": "",
                    "cached_at": "",
                    "error": "",
                },
            ],
        )
        return cache_root, output_dir, metadata_csv

    def test_push_dry_run_reports_operations_without_remote_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root, output_dir, _metadata_csv = self.build_cache_fixture(root)
            remote = root / "remote"
            fake_rclone = root / "fake_rclone.py"
            write_fake_rclone(fake_rclone)
            env = dict(os.environ)
            env["FAKE_RCLONE_REMOTE_ROOT"] = str(remote)

            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "sync_media_cache_drive.py"),
                    "push",
                    "--export-key",
                    "260531",
                    "--prefix",
                    "260531",
                    "--output-dir",
                    str(output_dir),
                    "--cache-root",
                    str(cache_root),
                    "--drive-media-cache-dir",
                    "fake:workspace/media-cache",
                    "--rclone-bin",
                    str(fake_rclone),
                    "--dry-run",
                ],
                cwd=ROOT,
                env=env,
                text=True,
                encoding="utf-8",
                capture_output=True,
                check=True,
            )

            report = json.loads(completed.stdout)
            self.assertTrue(report["dry_run"])
            self.assertEqual(
                report["drive_remote_dir"],
                "fake:workspace/media-cache/260531",
            )
            self.assertEqual(report["validation"]["cached_file_count"], 1)
            self.assertFalse(remote.exists())
            self.assertFalse((output_dir / "260531_media_cache_manifest.json").exists())

    def test_validation_counts_shared_cache_rel_path_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root, _output_dir, metadata_csv = self.build_cache_fixture(root)
            with metadata_csv.open("r", encoding="utf-8-sig", newline="") as f:
                rows = list(csv.DictReader(f))
            duplicate_cached_row = dict(rows[0])
            duplicate_cached_row["item_pk"] = "duplicate"
            duplicate_cached_row["media_index"] = "1"
            rows.insert(1, duplicate_cached_row)
            write_csv(metadata_csv, rows)

            validation = validate_media_cache_files(
                metadata_csv=metadata_csv,
                cache_root=cache_root,
                export_key="260531",
            )

            self.assertEqual(validation.metadata_rows, 3)
            self.assertEqual(validation.cache_status_counts["cached"], 2)
            self.assertEqual(validation.cached_file_count, 1)
            self.assertEqual(validation.cached_byte_size, int(rows[0]["byte_size"]))
            self.assertEqual(validation.missing_file_count, 0)
            self.assertEqual(validation.errors, [])

    def test_push_rejects_missing_cached_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root, output_dir, _metadata_csv = self.build_cache_fixture(root)
            (cache_root / "260531" / "body" / "0_media_abc.jpg").unlink()

            with self.assertRaises(DriveRunnerError):
                push_media_cache_to_drive(
                    export_key="260531",
                    prefix="260531",
                    output_dir=output_dir,
                    cache_root=cache_root,
                    drive_media_cache_dir="fake:workspace/media-cache",
                    rclone_bin="unused",
                    dry_run=True,
                )

    def test_push_rejects_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root, output_dir, _metadata_csv = self.build_cache_fixture(root)
            (cache_root / "260531" / "body" / "0_media_abc.jpg").write_bytes(b"changed")

            with self.assertRaises(DriveRunnerError):
                push_media_cache_to_drive(
                    export_key="260531",
                    prefix="260531",
                    output_dir=output_dir,
                    cache_root=cache_root,
                    drive_media_cache_dir="fake:workspace/media-cache",
                    rclone_bin="unused",
                    dry_run=True,
                )

    def test_push_writes_manifest_without_source_urls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root, output_dir, _metadata_csv = self.build_cache_fixture(root)
            remote = root / "remote"
            fake_rclone = root / "fake_rclone.py"
            write_fake_rclone(fake_rclone)
            env = dict(os.environ)
            env["FAKE_RCLONE_REMOTE_ROOT"] = str(remote)
            old_env = os.environ.copy()
            try:
                os.environ.update(env)
                summary = push_media_cache_to_drive(
                    export_key="260531",
                    prefix="260531",
                    output_dir=output_dir,
                    cache_root=cache_root,
                    drive_media_cache_dir="fake:workspace/media-cache",
                    rclone_bin=str(fake_rclone),
                )
            finally:
                os.environ.clear()
                os.environ.update(old_env)

            manifest = json.loads(Path(summary.manifest_path).read_text(encoding="utf-8"))
            self.assertEqual(manifest["manifest_version"], 1)
            self.assertEqual(manifest["cached_file_count"], 1)
            self.assertEqual(manifest["missing_file_count"], 0)
            manifest_text = json.dumps(manifest)
            self.assertNotIn("https://", manifest_text)

    def test_push_retries_transient_rclone_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root, output_dir, _metadata_csv = self.build_cache_fixture(root)
            remote = root / "remote"
            fake_rclone = root / "fake_rclone.py"
            state = root / "state.txt"
            write_fake_rclone(fake_rclone)
            old_env = os.environ.copy()
            try:
                os.environ.update(
                    {
                        "FAKE_RCLONE_REMOTE_ROOT": str(remote),
                        "FAKE_RCLONE_FAIL_STATE": str(state),
                        "FAKE_RCLONE_TRANSIENT_FAILURES": "1",
                    }
                )
                summary = push_media_cache_to_drive(
                    export_key="260531",
                    prefix="260531",
                    output_dir=output_dir,
                    cache_root=cache_root,
                    drive_media_cache_dir="fake:workspace/media-cache",
                    rclone_bin=str(fake_rclone),
                    rclone_retry_attempts=2,
                    rclone_retry_initial_delay_seconds=0,
                    rclone_retry_max_delay_seconds=0,
                )
            finally:
                os.environ.clear()
                os.environ.update(old_env)

            self.assertEqual(state.read_text(encoding="utf-8"), "1")
            self.assertTrue(Path(summary.manifest_path).exists())
            self.assertTrue(
                (
                    remote
                    / "workspace"
                    / "media-cache"
                    / "260531"
                    / "body"
                    / "0_media_abc.jpg"
                ).exists()
            )
            self.assertTrue(
                (
                    remote
                    / "workspace"
                    / "media-cache"
                    / "260531"
                    / "260531_media_cache_manifest.json"
                ).exists()
            )

    def test_pull_restores_metadata_and_mirrored_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_cache_root, source_output_dir, _metadata_csv = self.build_cache_fixture(
                root / "source"
            )
            remote = root / "remote"
            fake_rclone = root / "fake_rclone.py"
            write_fake_rclone(fake_rclone)
            old_env = os.environ.copy()
            try:
                os.environ["FAKE_RCLONE_REMOTE_ROOT"] = str(remote)
                push_media_cache_to_drive(
                    export_key="260531",
                    prefix="260531",
                    output_dir=source_output_dir,
                    cache_root=source_cache_root,
                    drive_media_cache_dir="fake:workspace/media-cache",
                    rclone_bin=str(fake_rclone),
                )
                restored_cache_root = root / "restored" / "private" / "media-cache"
                restored_output_dir = root / "restored" / "outputs" / "260531"
                summary = pull_media_cache_from_drive(
                    export_key="260531",
                    prefix="260531",
                    output_dir=restored_output_dir,
                    cache_root=restored_cache_root,
                    drive_media_cache_dir="fake:workspace/media-cache",
                    rclone_bin=str(fake_rclone),
                )
            finally:
                os.environ.clear()
                os.environ.update(old_env)

            restored_media = restored_cache_root / "260531" / "body" / "0_media_abc.jpg"
            self.assertTrue(restored_media.exists())
            self.assertTrue((restored_output_dir / "260531_media_cache_assets.csv").exists())
            self.assertTrue((restored_output_dir / "260531_media_cache_manifest.json").exists())
            self.assertEqual(summary.validation.cached_file_count, 1)
            self.assertFalse(
                (restored_cache_root / "260531" / "260531_media_cache_assets.csv").exists()
            )

    def test_export_key_rejects_slashes_and_path_traversal(self) -> None:
        for value in ("../260531", "260531/extra", "260531.extra", ""):
            with self.subTest(value=value):
                with self.assertRaises(DriveRunnerError):
                    validate_safe_token("export_key", value)


if __name__ == "__main__":
    unittest.main()
