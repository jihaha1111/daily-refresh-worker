import base64
import csv
import json
import sys
import tempfile
import urllib.parse
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.media_cache import (  # noqa: E402
    CACHE_STATUS_CACHED,
    CACHE_STATUS_DOWNLOAD_FAILED,
    CACHE_STATUS_EXPIRED,
    CACHE_STATUS_FORBIDDEN,
    CACHE_STATUS_MISSING_SOURCE_URL,
    MEDIA_CACHE_COLUMNS,
    build_missing_row,
    read_csv_rows,
)
from threads_coupang_pipeline.media_refresh_queue import MEDIA_REFRESH_QUEUE_COLUMNS  # noqa: E402
from threads_coupang_pipeline.media_refresh_worker import (  # noqa: E402
    extract_media_candidates,
    run_media_refresh_worker,
    sanitized_summary,
)


PNG_1X1 = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x02\x00\x00\x00"
    b"\x90wS\xde"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)

MEDIA_COLUMNS = [
    "item_pk",
    "media_index",
    "media_source",
    "asset_type",
    "media_pk",
    "media_id",
    "best_image_url",
    "best_image_width",
    "best_image_height",
    "best_video_url",
    "video_version_count",
    "original_width",
    "original_height",
    "accessibility_caption",
    "has_audio",
]


def write_csv(path: Path, columns, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def queue_row(
    item_pk: str,
    *,
    media_index: str = "0",
    cache_role: str = "media",
    input_kind: str = "image",
    asset_type: str = "image",
    priority: str = "1",
    threads_url: str = "",
) -> dict:
    row = {column: "" for column in MEDIA_REFRESH_QUEUE_COLUMNS}
    row.update(
        {
            "export_key": "fixture",
            "queue_id": f"fixture:match:{item_pk}:{media_index}:{cache_role}",
            "match_id": "body__link",
            "performance_grade": "Gold",
            "match_side": "body",
            "item_pk": item_pk,
            "media_index": media_index,
            "cache_role": cache_role,
            "input_kind": input_kind,
            "asset_type": asset_type,
            "threads_url": threads_url,
            "has_existing_source_url": "true",
            "priority": priority,
            "queue_status": "queued",
            "created_at": "2026-06-03T00:00:00+00:00",
        }
    )
    return row


def media_row(
    item_pk: str,
    *,
    media_index: str = "0",
    asset_type: str = "image",
    image_url: str = "",
    video_url: str = "",
) -> dict:
    row = {column: "" for column in MEDIA_COLUMNS}
    row.update(
        {
            "item_pk": item_pk,
            "media_index": media_index,
            "media_source": "top_level",
            "asset_type": asset_type,
            "media_pk": f"{item_pk}_{media_index}",
            "media_id": f"{item_pk}_{media_index}",
            "best_image_url": image_url,
            "best_image_width": "1",
            "best_image_height": "1",
            "best_video_url": video_url,
            "video_version_count": "1" if video_url else "0",
            "original_width": "1",
            "original_height": "1",
            "has_audio": "true" if video_url else "false",
        }
    )
    return row


def fake_cached_row(
    export_key: str,
    planned: dict,
    *,
    byte_size: str = "12000",
    width: str = "300",
    height: str = "400",
    cache_rel_path: str = "",
    content_sha256: str = "synthetic-content-hash",
) -> dict:
    return {
        "export_key": export_key,
        "item_pk": planned.get("item_pk", ""),
        "media_index": planned.get("media_index", ""),
        "cache_role": planned.get("cache_role", ""),
        "asset_type": planned.get("asset_type", ""),
        "input_kind": planned.get("input_kind", ""),
        "source_url_hash": "synthetic-hash",
        "cache_status": CACHE_STATUS_CACHED,
        "cache_rel_path": cache_rel_path or f"{export_key}/{planned.get('item_pk', '')}/fixture.bin",
        "mime_type": "application/octet-stream",
        "byte_size": byte_size,
        "content_sha256": content_sha256,
        "width": width,
        "height": height,
        "duration_seconds": "",
        "cached_at": "2026-06-03T00:00:00+00:00",
        "error": "",
    }


def media_pk_url(media_pk: str, label: str, *, ext: str = "jpg", stp: str = "") -> str:
    cache_key = base64.b64encode(media_pk.encode("utf-8")).decode("ascii") + ".3"
    query = {"ig_cache_key": cache_key}
    if stp:
        query["stp"] = stp
    return (
        f"https://cdn.example.test/{label}.{ext}?"
        f"{urllib.parse.urlencode(query)}"
    )


class MediaRefreshWorkerTests(unittest.TestCase):
    def run_fixture(
        self,
        *,
        queue_rows,
        media_rows,
        max_rows: int = 20,
        max_parallel: int = 1,
        cache_func=None,
        fetch_func=None,
        upload_to_drive: bool = False,
        sync_func=None,
    ):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        queue_csv = root / "fixture_media_refresh_queue.csv"
        media_csv = root / "fixture_media_assets.csv"
        output_csv = root / "fixture_media_cache_assets.csv"
        write_csv(queue_csv, MEDIA_REFRESH_QUEUE_COLUMNS, queue_rows)
        write_csv(media_csv, MEDIA_COLUMNS, media_rows)
        summary = run_media_refresh_worker(
            export_key="fixture",
            queue_csv=queue_csv,
            media_assets_csv=media_csv,
            output_csv=output_csv,
            cache_root=root / "private" / "media-cache",
            max_rows=max_rows,
            max_parallel=max_parallel,
            sleep_seconds=0,
            timeout_seconds=1,
            fetch_attempts=1,
            upload_to_drive=upload_to_drive,
            prefix="fixture",
            output_dir=root,
            drive_media_cache_dir="fake:workspace/media-cache",
            rclone_bin="fake-rclone",
            cache_func=cache_func if cache_func else None,
            fetch_func=fetch_func if fetch_func else None,
            sync_func=sync_func,
        )
        return root, summary, output_csv, read_csv_rows(output_csv)

    def test_rows_are_processed_in_priority_order_and_respect_max_rows(self) -> None:
        calls = []

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            calls.append(planned["item_pk"])
            return fake_cached_row(export_key, planned)

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row("b_item", priority="4"),
                queue_row("gold_item", priority="1"),
                queue_row("a_item", priority="3"),
            ],
            media_rows=[
                media_row("b_item", image_url="fake://b"),
                media_row("gold_item", image_url="fake://gold"),
                media_row("a_item", image_url="fake://a"),
            ],
            max_rows=2,
            cache_func=fake_cache,
        )

        self.assertEqual(calls, ["gold_item", "a_item"])
        self.assertEqual([row["item_pk"] for row in rows], ["gold_item", "a_item"])
        self.assertEqual(summary.processed_rows, 2)
        self.assertTrue(summary.limit_applied)

    def test_direct_image_and_video_downloads_write_cache_metadata_without_source_urls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_image = root / "source.png"
            source_video = root / "source.mp4"
            source_image.write_bytes(PNG_1X1)
            source_video.write_bytes(b"synthetic video bytes")
            queue_csv = root / "fixture_media_refresh_queue.csv"
            media_csv = root / "fixture_media_assets.csv"
            output_csv = root / "fixture_media_cache_assets.csv"
            cache_root = root / "private" / "media-cache"
            write_csv(
                queue_csv,
                MEDIA_REFRESH_QUEUE_COLUMNS,
                [
                    queue_row("image_item", asset_type="image", input_kind="image"),
                    queue_row(
                        "video_item",
                        cache_role="preview",
                        input_kind="image",
                        asset_type="video",
                    ),
                    queue_row(
                        "video_item",
                        cache_role="media",
                        input_kind="video",
                        asset_type="video",
                    ),
                ],
            )
            write_csv(
                media_csv,
                MEDIA_COLUMNS,
                [
                    media_row("image_item", image_url=source_image.as_uri()),
                    media_row(
                        "video_item",
                        asset_type="video",
                        image_url=source_image.as_uri(),
                        video_url=source_video.as_uri(),
                    ),
                ],
            )

            summary = run_media_refresh_worker(
                export_key="fixture",
                queue_csv=queue_csv,
                media_assets_csv=media_csv,
                output_csv=output_csv,
                cache_root=cache_root,
                max_rows=0,
                max_parallel=1,
                sleep_seconds=0,
                timeout_seconds=1,
                fetch_attempts=1,
            )
            rows = read_csv_rows(output_csv)
            output_text = output_csv.read_text(encoding="utf-8-sig")
            summary_text = json.dumps(sanitized_summary(summary), ensure_ascii=False)

            self.assertEqual(list(rows[0].keys()), MEDIA_CACHE_COLUMNS)
            self.assertEqual(len(rows), 3)
            self.assertEqual(summary.cached_rows, 3)
            self.assertEqual(
                [(row["item_pk"], row["cache_role"], row["input_kind"]) for row in rows],
                [
                    ("image_item", "media", "image"),
                    ("video_item", "preview", "image"),
                    ("video_item", "media", "video"),
                ],
            )
            self.assertTrue(all((cache_root / row["cache_rel_path"]).exists() for row in rows))
            self.assertNotIn(source_image.as_uri(), output_text)
            self.assertNotIn(source_video.as_uri(), output_text)
            self.assertNotIn(source_image.as_uri(), summary_text)
            self.assertNotIn(source_video.as_uri(), summary_text)

    def test_missing_source_and_unusable_threads_url_becomes_missing_source_url(self) -> None:
        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[queue_row("missing_item", threads_url="")],
            media_rows=[media_row("missing_item")],
            fetch_func=lambda threads_url, *, timeout: "",
        )

        self.assertEqual(rows[0]["cache_status"], CACHE_STATUS_MISSING_SOURCE_URL)
        self.assertEqual(summary.missing_source_rows, 1)

    def test_direct_statuses_are_preserved_for_worker_outputs(self) -> None:
        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            source = planned["source_url"]
            if source.startswith("forbidden://"):
                return build_missing_row(export_key, planned, CACHE_STATUS_FORBIDDEN, "403")
            if source.startswith("expired404://") or source.startswith("expired410://"):
                return build_missing_row(export_key, planned, CACHE_STATUS_EXPIRED, "expired")
            return build_missing_row(
                export_key,
                planned,
                CACHE_STATUS_DOWNLOAD_FAILED,
                "generic",
            )

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row("forbidden_item"),
                queue_row("expired404_item"),
                queue_row("expired410_item"),
                queue_row("failed_item"),
            ],
            media_rows=[
                media_row("forbidden_item", image_url="forbidden://asset"),
                media_row("expired404_item", image_url="expired404://asset"),
                media_row("expired410_item", image_url="expired410://asset"),
                media_row("failed_item", image_url="failed://asset"),
            ],
            max_rows=0,
            cache_func=fake_cache,
        )

        statuses = {row["item_pk"]: row["cache_status"] for row in rows}
        self.assertEqual(statuses["forbidden_item"], CACHE_STATUS_FORBIDDEN)
        self.assertEqual(statuses["expired404_item"], CACHE_STATUS_EXPIRED)
        self.assertEqual(statuses["expired410_item"], CACHE_STATUS_EXPIRED)
        self.assertEqual(statuses["failed_item"], CACHE_STATUS_DOWNLOAD_FAILED)
        self.assertEqual(summary.download_failed_rows, 1)

    def test_refresh_candidate_can_cache_when_existing_source_is_missing(self) -> None:
        refreshed_url = "https://cdn.example.test/refreshed.jpg?secret=raw"
        escaped_url = refreshed_url.replace("/", "\\/").replace("?", "\\u003f")

        def fake_fetch(threads_url, *, timeout):
            self.assertEqual(threads_url, "https://www.threads.com/@fixture/post/ABC")
            return f'<script>window.__data="{escaped_url}"</script>'

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            self.assertEqual(planned["source_url"], refreshed_url)
            return fake_cached_row(export_key, planned)

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row(
                    "refresh_item",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                )
            ],
            media_rows=[media_row("refresh_item")],
            cache_func=fake_cache,
            fetch_func=fake_fetch,
        )
        summary_text = json.dumps(sanitized_summary(summary), ensure_ascii=False)

        self.assertEqual(rows[0]["cache_status"], CACHE_STATUS_CACHED)
        self.assertEqual(summary.refreshed_cached_rows, 1)
        self.assertEqual(summary.direct_cached_rows, 0)
        self.assertNotIn(refreshed_url, summary_text)

    def test_refresh_maps_image_candidates_by_item_media_order(self) -> None:
        candidate_urls = [
            media_pk_url("carousel_item_0", "image-0-small", stp="dst-jpg_p320x320"),
            media_pk_url("carousel_item_0", "image-0-original"),
            media_pk_url("carousel_item_1", "image-1-small", stp="dst-jpg_p320x320"),
            media_pk_url("carousel_item_1", "image-1-large", stp="dst-jpg_p1080x1080"),
            media_pk_url("carousel_item_2", "image-2-large", stp="dst-jpg_p1080x1080"),
            media_pk_url("carousel_item_2", "image-2-small", stp="dst-jpg_p320x320"),
        ]
        calls = []

        def fake_fetch(threads_url, *, timeout):
            self.assertEqual(threads_url, "https://www.threads.com/@fixture/post/ABC")
            return " ".join(f'"{url}"' for url in candidate_urls)

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            calls.append((planned["media_index"], planned["source_url"]))
            return fake_cached_row(
                export_key,
                planned,
                content_sha256=planned["source_url"],
                cache_rel_path=(
                    f"{export_key}/{planned['item_pk']}/"
                    f"{planned['media_index']}_"
                    f"{urllib.parse.urlparse(planned['source_url']).path.rsplit('/', 1)[-1]}"
                ),
            )

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row(
                    "carousel_item",
                    media_index="0",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
                queue_row(
                    "carousel_item",
                    media_index="1",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
                queue_row(
                    "carousel_item",
                    media_index="2",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
            ],
            media_rows=[
                media_row("carousel_item", media_index="0"),
                media_row("carousel_item", media_index="1"),
                media_row("carousel_item", media_index="2"),
            ],
            max_rows=0,
            cache_func=fake_cache,
            fetch_func=fake_fetch,
        )

        self.assertEqual(
            calls,
            [
                ("0", media_pk_url("carousel_item_0", "image-0-original")),
                ("1", media_pk_url("carousel_item_1", "image-1-large", stp="dst-jpg_p1080x1080")),
                ("2", media_pk_url("carousel_item_2", "image-2-large", stp="dst-jpg_p1080x1080")),
            ],
        )
        self.assertEqual(summary.cached_rows, 3)
        self.assertEqual(len({row["cache_rel_path"] for row in rows}), 3)
        self.assertTrue(rows[0]["cache_rel_path"].endswith("image-0-original.jpg"))
        self.assertTrue(rows[1]["cache_rel_path"].endswith("image-1-large.jpg"))
        self.assertTrue(rows[2]["cache_rel_path"].endswith("image-2-large.jpg"))

    def test_refresh_maps_video_candidates_by_media_pk_and_uses_highest_quality(self) -> None:
        candidate_urls = [
            media_pk_url("video_item_0", "video-0-small", ext="mp4", stp="dash-vp9_s360x640"),
            media_pk_url("video_item_0", "video-0-original", ext="mp4"),
            media_pk_url("video_item_1", "video-1-small", ext="mp4", stp="dash-vp9_s360x640"),
            media_pk_url("video_item_1", "video-1-large", ext="mp4", stp="dash-vp9_s1080x1920"),
        ]
        calls = []

        def fake_fetch(threads_url, *, timeout):
            self.assertEqual(threads_url, "https://www.threads.com/@fixture/post/ABC")
            return " ".join(f'"{url}"' for url in candidate_urls)

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            calls.append((planned["media_index"], planned["source_url"]))
            return fake_cached_row(
                export_key,
                planned,
                content_sha256=planned["source_url"],
            )

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row(
                    "video_item",
                    media_index="0",
                    input_kind="video",
                    asset_type="video",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
                queue_row(
                    "video_item",
                    media_index="1",
                    input_kind="video",
                    asset_type="video",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
            ],
            media_rows=[
                media_row("video_item", media_index="0", asset_type="video"),
                media_row("video_item", media_index="1", asset_type="video"),
            ],
            max_rows=0,
            cache_func=fake_cache,
            fetch_func=fake_fetch,
        )

        self.assertEqual(
            calls,
            [
                ("0", media_pk_url("video_item_0", "video-0-original", ext="mp4")),
                ("1", media_pk_url("video_item_1", "video-1-large", ext="mp4", stp="dash-vp9_s1080x1920")),
            ],
        )
        self.assertEqual(summary.cached_rows, 2)
        self.assertEqual([row["cache_status"] for row in rows], [
            CACHE_STATUS_CACHED,
            CACHE_STATUS_CACHED,
        ])

    def test_refresh_uses_candidate_metadata_dimensions_for_video_quality(self) -> None:
        small_url = media_pk_url("video_dim_item_0", "video-small", ext="mp4")
        large_url = media_pk_url("video_dim_item_0", "video-large", ext="mp4")
        calls = []

        def fake_fetch(threads_url, *, timeout):
            self.assertEqual(threads_url, "https://www.threads.com/@fixture/post/ABC")
            return (
                f'{{"width":360,"height":640,"url":"{small_url}"}} '
                f'{{"width":1080,"height":1920,"url":"{large_url}"}}'
            )

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            calls.append(planned["source_url"])
            return fake_cached_row(
                export_key,
                planned,
                content_sha256=planned["source_url"],
            )

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row(
                    "video_dim_item",
                    media_index="0",
                    input_kind="video",
                    asset_type="video",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
            ],
            media_rows=[
                media_row("video_dim_item", media_index="0", asset_type="video"),
            ],
            max_rows=0,
            cache_func=fake_cache,
            fetch_func=fake_fetch,
        )

        self.assertEqual(calls, [large_url])
        self.assertEqual(summary.cached_rows, 1)
        self.assertEqual(rows[0]["cache_status"], CACHE_STATUS_CACHED)

    def test_video_preview_does_not_use_unkeyed_image_fallback(self) -> None:
        def fake_fetch(threads_url, *, timeout):
            self.assertEqual(threads_url, "https://www.threads.com/@fixture/post/ABC")
            return '"https://cdn.example.test/unrelated-story-image.jpg"'

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            raise AssertionError("unkeyed image fallback should not be cached as a video preview")

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row(
                    "video_preview_item",
                    media_index="0",
                    cache_role="preview",
                    input_kind="image",
                    asset_type="video",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
            ],
            media_rows=[
                media_row("video_preview_item", media_index="0", asset_type="video"),
            ],
            max_rows=0,
            cache_func=fake_cache,
            fetch_func=fake_fetch,
        )

        self.assertEqual(rows[0]["cache_status"], CACHE_STATUS_MISSING_SOURCE_URL)
        self.assertEqual(summary.cached_rows, 0)
        self.assertEqual(summary.missing_source_rows, 1)

    def test_refresh_does_not_reuse_first_candidate_when_index_is_missing(self) -> None:
        candidate_urls = [
            media_pk_url("carousel_item_0", "image-0"),
            media_pk_url("carousel_item_1", "image-1"),
        ]
        calls = []

        def fake_fetch(threads_url, *, timeout):
            return " ".join(f'"{url}"' for url in candidate_urls)

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            calls.append(planned["source_url"])
            return fake_cached_row(
                export_key,
                planned,
                content_sha256=planned["source_url"],
            )

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row(
                    "carousel_item",
                    media_index="0",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
                queue_row(
                    "carousel_item",
                    media_index="1",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
                queue_row(
                    "carousel_item",
                    media_index="2",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
            ],
            media_rows=[
                media_row("carousel_item", media_index="0"),
                media_row("carousel_item", media_index="1"),
                media_row("carousel_item", media_index="2"),
            ],
            max_rows=0,
            cache_func=fake_cache,
            fetch_func=fake_fetch,
        )

        self.assertEqual(
            calls,
            [
                media_pk_url("carousel_item_0", "image-0"),
                media_pk_url("carousel_item_1", "image-1"),
            ],
        )
        self.assertEqual([row["cache_status"] for row in rows], [
            CACHE_STATUS_CACHED,
            CACHE_STATUS_CACHED,
            CACHE_STATUS_MISSING_SOURCE_URL,
        ])
        self.assertEqual(summary.cached_rows, 2)
        self.assertEqual(summary.missing_source_rows, 1)

    def test_duplicate_queue_target_uses_same_candidate_offset(self) -> None:
        candidate_urls = [
            media_pk_url("carousel_item_0", "image-0"),
            media_pk_url("carousel_item_1", "image-1"),
        ]
        calls = []
        first = queue_row(
            "carousel_item",
            media_index="0",
            threads_url="https://www.threads.com/@fixture/post/ABC",
        )
        duplicate = {
            **first,
            "queue_id": "fixture:another-match:carousel_item:0:media",
            "match_id": "another_body__another_link",
        }
        second = queue_row(
            "carousel_item",
            media_index="1",
            threads_url="https://www.threads.com/@fixture/post/ABC",
        )

        def fake_fetch(threads_url, *, timeout):
            return " ".join(f'"{url}"' for url in candidate_urls)

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            calls.append((planned["media_index"], planned["source_url"]))
            return fake_cached_row(
                export_key,
                planned,
                content_sha256=planned["source_url"],
            )

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[first, duplicate, second],
            media_rows=[
                media_row("carousel_item", media_index="0"),
                media_row("carousel_item", media_index="1"),
            ],
            max_rows=0,
            cache_func=fake_cache,
            fetch_func=fake_fetch,
        )

        self.assertEqual(
            calls,
            [
                ("0", media_pk_url("carousel_item_0", "image-0")),
                ("1", media_pk_url("carousel_item_1", "image-1")),
            ],
        )
        self.assertEqual([row["cache_status"] for row in rows], [
            CACHE_STATUS_CACHED,
            CACHE_STATUS_CACHED,
            CACHE_STATUS_CACHED,
        ])
        self.assertEqual(summary.cached_rows, 3)

    def test_refresh_skips_duplicate_image_content_within_group(self) -> None:
        candidate_urls = [
            "https://cdn.example.test/image-0-large.jpg",
            "https://cdn.example.test/image-0-small.jpg",
            "https://cdn.example.test/image-1-large.jpg",
        ]
        calls = []

        def fake_fetch(threads_url, *, timeout):
            return " ".join(f'"{url}"' for url in candidate_urls)

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            calls.append((planned["media_index"], planned["source_url"]))
            if planned["source_url"].endswith("image-1-large.jpg"):
                digest = "second-image-content"
            else:
                digest = "first-image-content"
            return fake_cached_row(
                export_key,
                planned,
                content_sha256=digest,
                cache_rel_path=(
                    f"{export_key}/{planned['item_pk']}/"
                    f"{planned['media_index']}_{planned['source_url'].rsplit('/', 1)[-1]}"
                ),
            )

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row(
                    "carousel_item",
                    media_index="0",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
                queue_row(
                    "carousel_item",
                    media_index="1",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                ),
            ],
            media_rows=[
                media_row("carousel_item", media_index="0"),
                media_row("carousel_item", media_index="1"),
            ],
            max_rows=0,
            cache_func=fake_cache,
            fetch_func=fake_fetch,
        )

        self.assertEqual(
            calls,
            [
                ("0", "https://cdn.example.test/image-0-large.jpg"),
                ("1", "https://cdn.example.test/image-0-small.jpg"),
                ("1", "https://cdn.example.test/image-1-large.jpg"),
            ],
        )
        self.assertEqual(summary.cached_rows, 2)
        self.assertEqual(
            [row["content_sha256"] for row in rows],
            ["first-image-content", "second-image-content"],
        )

    def test_duplicate_cached_content_reuses_first_file_path(self) -> None:
        calls = []

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            calls.append((planned["item_pk"], planned["media_index"]))
            rel_path = (
                f"{export_key}/{planned['item_pk']}/"
                f"{planned['media_index']}_media_{planned['item_pk']}.jpg"
            )
            path = cache_root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"duplicate-image-bytes")
            return fake_cached_row(
                export_key,
                planned,
                byte_size=str(path.stat().st_size),
                width="836",
                height="836",
                cache_rel_path=rel_path,
                content_sha256="same-content-hash",
            )

        root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row("body_item", media_index="0", threads_url="https://www.threads.com/@fixture/post/BODY"),
                queue_row("link_item", media_index="0", threads_url="https://www.threads.com/@fixture/post/LINK"),
            ],
            media_rows=[
                media_row("body_item", media_index="0", image_url="https://cdn.example.test/body.jpg"),
                media_row("link_item", media_index="0", image_url="https://cdn.example.test/link.jpg"),
            ],
            max_rows=0,
            cache_func=fake_cache,
        )

        self.assertEqual(calls, [("body_item", "0"), ("link_item", "0")])
        self.assertEqual(summary.cached_rows, 2)
        self.assertEqual(rows[0]["content_sha256"], rows[1]["content_sha256"])
        self.assertEqual(rows[0]["cache_rel_path"], rows[1]["cache_rel_path"])
        self.assertTrue((root / "private" / "media-cache" / rows[0]["cache_rel_path"]).exists())
        self.assertFalse(
            (
                root
                / "private"
                / "media-cache"
                / "fixture/link_item/0_media_link_item.jpg"
            ).exists()
        )

    def test_refresh_skips_low_quality_image_candidate_and_tries_next(self) -> None:
        low_quality_url = "https://cdn.example.test/static-placeholder.webp"
        good_url = "https://cdn.example.test/refreshed.jpg"
        calls = []

        def fake_fetch(threads_url, *, timeout):
            return f'"{low_quality_url}" "{good_url}"'

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            calls.append(planned["source_url"])
            if planned["source_url"] == low_quality_url:
                rel_path = f"{export_key}/{planned['item_pk']}/placeholder.webp"
                path = cache_root / rel_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"placeholder")
                return fake_cached_row(
                    export_key,
                    planned,
                    byte_size="1128",
                    width="57",
                    height="57",
                    cache_rel_path=rel_path,
                )
            return fake_cached_row(export_key, planned)

        root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row(
                    "refresh_item",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                )
            ],
            media_rows=[media_row("refresh_item")],
            cache_func=fake_cache,
            fetch_func=fake_fetch,
        )

        self.assertEqual(calls, [low_quality_url, good_url])
        self.assertEqual(rows[0]["cache_status"], CACHE_STATUS_CACHED)
        self.assertEqual(rows[0]["byte_size"], "12000")
        self.assertEqual(summary.refreshed_cached_rows, 1)
        self.assertFalse(
            (root / "private" / "media-cache" / "fixture" / "refresh_item" / "placeholder.webp").exists()
        )

    def test_refresh_low_quality_image_only_becomes_download_failed(self) -> None:
        low_quality_url = "https://cdn.example.test/static-placeholder.webp"

        def fake_fetch(threads_url, *, timeout):
            return f'"{low_quality_url}"'

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            return fake_cached_row(
                export_key,
                planned,
                byte_size="1128",
                width="57",
                height="57",
            )

        _root, summary, _output_csv, rows = self.run_fixture(
            queue_rows=[
                queue_row(
                    "refresh_item",
                    threads_url="https://www.threads.com/@fixture/post/ABC",
                )
            ],
            media_rows=[media_row("refresh_item")],
            cache_func=fake_cache,
            fetch_func=fake_fetch,
        )

        self.assertEqual(rows[0]["cache_status"], CACHE_STATUS_DOWNLOAD_FAILED)
        self.assertIn("minimum byte_size", rows[0]["error"])
        self.assertEqual(summary.cached_rows, 0)
        self.assertEqual(summary.download_failed_rows, 1)

    def test_static_placeholder_image_candidates_are_ignored(self) -> None:
        body = (
            '"https://static.xx.fbcdn.net/rsrc.php/v4/placeholder.webp" '
            '"https://cdn.example.test/real-image.jpg"'
        )

        self.assertEqual(
            extract_media_candidates(body, "image"),
            ["https://cdn.example.test/real-image.jpg"],
        )

    def test_upload_to_drive_false_does_not_call_sync_and_true_calls_sync(self) -> None:
        sync_calls = []

        def fake_cache(export_key, planned, cache_root, *, timeout, max_bytes, force):
            return build_missing_row(export_key, planned, CACHE_STATUS_MISSING_SOURCE_URL)

        def fake_sync(**kwargs):
            sync_calls.append(kwargs)
            return SimpleNamespace(
                drive_remote_dir=kwargs["drive_media_cache_dir"] + "/" + kwargs["export_key"],
                manifest_path=str(kwargs["output_dir"] / "fixture_media_cache_manifest.json"),
            )

        self.run_fixture(
            queue_rows=[queue_row("missing_item")],
            media_rows=[media_row("missing_item")],
            cache_func=fake_cache,
            upload_to_drive=False,
            sync_func=fake_sync,
        )
        self.assertEqual(sync_calls, [])

        _root, summary, _output_csv, _rows = self.run_fixture(
            queue_rows=[queue_row("missing_item")],
            media_rows=[media_row("missing_item")],
            cache_func=fake_cache,
            upload_to_drive=True,
            sync_func=fake_sync,
        )
        self.assertEqual(len(sync_calls), 1)
        self.assertEqual(sync_calls[0]["export_key"], "fixture")
        self.assertEqual(sync_calls[0]["prefix"], "fixture")
        self.assertEqual(
            sync_calls[0]["drive_media_cache_dir"],
            "fake:workspace/media-cache",
        )
        self.assertEqual(
            summary.drive_sync["drive_remote_dir"],
            "fake:workspace/media-cache/fixture",
        )


if __name__ == "__main__":
    unittest.main()
