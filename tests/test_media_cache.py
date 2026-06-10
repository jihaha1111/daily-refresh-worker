import csv
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.media_cache import (  # noqa: E402
    CACHE_STATUS_CACHED,
    CACHE_STATUS_MISSING_SOURCE_URL,
    MEDIA_CACHE_COLUMNS,
    cache_one_media,
    cache_media_assets,
    image_dimensions,
    normalize_media_cache_extensions,
    selected_item_pks_from_performance_labels,
)


PNG_1X1 = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x02\x00\x00\x00"
    b"\x90wS\xde"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)

JPEG_BYTES = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\xff\xd9"


class FakeDownloadResponse:
    def __init__(self, payload: bytes, content_type: str) -> None:
        self.payload = payload
        self.headers = {"content-type": content_type}
        self._offset = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if self._offset >= len(self.payload):
            return b""
        if size is None or size < 0:
            size = len(self.payload) - self._offset
        chunk = self.payload[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


def write_csv(path: Path, columns, rows) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


class MediaCacheTests(unittest.TestCase):
    def test_cache_one_media_prefers_response_mime_for_extensionless_image_url(self) -> None:
        import urllib.request

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            original_urlopen = urllib.request.urlopen
            try:
                urllib.request.urlopen = lambda url, timeout: FakeDownloadResponse(
                    JPEG_BYTES,
                    "image/jpeg; charset=binary",
                )
                row = cache_one_media(
                    "fixture",
                    {
                        "item_pk": "body",
                        "media_index": "0",
                        "cache_role": "media",
                        "asset_type": "image",
                        "input_kind": "image",
                        "source_url": "https://cdn.example/media?id=1",
                    },
                    root / "cache",
                    timeout=1,
                    max_bytes=1024,
                    force=False,
                )
            finally:
                urllib.request.urlopen = original_urlopen

            self.assertEqual(row["cache_status"], CACHE_STATUS_CACHED)
            self.assertEqual(row["mime_type"], "image/jpeg")
            self.assertTrue(row["cache_rel_path"].endswith(".jpg"))
            self.assertTrue((root / "cache" / row["cache_rel_path"]).exists())

    def test_cache_one_media_prefers_response_mime_for_extensionless_video_url(self) -> None:
        import urllib.request

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            original_urlopen = urllib.request.urlopen
            try:
                urllib.request.urlopen = lambda url, timeout: FakeDownloadResponse(
                    b"synthetic mp4 bytes",
                    "video/mp4",
                )
                row = cache_one_media(
                    "fixture",
                    {
                        "item_pk": "body",
                        "media_index": "0",
                        "cache_role": "media",
                        "asset_type": "video",
                        "input_kind": "video",
                        "source_url": "https://cdn.example/video?id=1",
                    },
                    root / "cache",
                    timeout=1,
                    max_bytes=1024,
                    force=False,
                )
            finally:
                urllib.request.urlopen = original_urlopen

            self.assertEqual(row["cache_status"], CACHE_STATUS_CACHED)
            self.assertEqual(row["mime_type"], "video/mp4")
            self.assertTrue(row["cache_rel_path"].endswith(".mp4"))
            self.assertTrue((root / "cache" / row["cache_rel_path"]).exists())

    def test_normalize_media_cache_extensions_renames_shared_bin_jpeg_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            export_key = "260424"
            cache_root = root / "private" / "media-cache"
            bin_rel_path = (
                f"{export_key}/3881882852250196668/1_media_cfdc141ebd6bd0a2.bin"
            )
            jpg_rel_path = (
                f"{export_key}/3881882852250196668/1_media_cfdc141ebd6bd0a2.jpg"
            )
            bin_path = cache_root / bin_rel_path
            bin_path.parent.mkdir(parents=True, exist_ok=True)
            bin_path.write_bytes(JPEG_BYTES)
            metadata_csv = root / "outputs" / export_key / f"{export_key}_media_cache_assets.csv"
            rows = [
                {
                    "export_key": export_key,
                    "item_pk": "3881882852250196668",
                    "media_index": "1",
                    "cache_role": "media",
                    "asset_type": "image",
                    "input_kind": "image",
                    "source_url_hash": "hash",
                    "cache_status": "cached",
                    "cache_rel_path": bin_rel_path,
                    "mime_type": "image/jpeg",
                    "byte_size": str(len(JPEG_BYTES)),
                    "content_sha256": "sha",
                    "width": "",
                    "height": "",
                    "duration_seconds": "",
                    "cached_at": "2026-06-03T00:00:00+00:00",
                    "error": "",
                },
                {
                    "export_key": export_key,
                    "item_pk": "duplicate",
                    "media_index": "0",
                    "cache_role": "media",
                    "asset_type": "image",
                    "input_kind": "image",
                    "source_url_hash": "hash2",
                    "cache_status": "cached",
                    "cache_rel_path": bin_rel_path,
                    "mime_type": "image/jpeg",
                    "byte_size": str(len(JPEG_BYTES)),
                    "content_sha256": "sha",
                    "width": "",
                    "height": "",
                    "duration_seconds": "",
                    "cached_at": "2026-06-03T00:00:00+00:00",
                    "error": "",
                },
            ]
            metadata_csv.parent.mkdir(parents=True, exist_ok=True)
            write_csv(metadata_csv, MEDIA_CACHE_COLUMNS, rows)

            summary = normalize_media_cache_extensions(
                metadata_csv=metadata_csv,
                cache_root=cache_root,
                export_key=export_key,
            )
            updated_rows = read_csv(metadata_csv)

            self.assertEqual(summary.updated_rows, 2)
            self.assertEqual(summary.renamed_files, 1)
            self.assertEqual(summary.errors, [])
            self.assertFalse((cache_root / bin_rel_path).exists())
            self.assertTrue((cache_root / jpg_rel_path).exists())
            self.assertEqual([row["cache_rel_path"] for row in updated_rows], [jpg_rel_path] * 2)

    def test_image_dimensions_reads_webp_vp8x_header(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fixture.webp"
            width_minus_one = (57 - 1).to_bytes(3, "little")
            height_minus_one = (57 - 1).to_bytes(3, "little")
            path.write_bytes(
                b"RIFF"
                + (22).to_bytes(4, "little")
                + b"WEBP"
                + b"VP8X"
                + (10).to_bytes(4, "little")
                + b"\x00\x00\x00\x00"
                + width_minus_one
                + height_minus_one
            )

            self.assertEqual(image_dimensions(path), (57, 57))

    def test_cache_media_assets_writes_image_video_and_missing_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_image = root / "source.png"
            source_video = root / "source.mp4"
            source_image.write_bytes(PNG_1X1)
            source_video.write_bytes(b"synthetic video bytes")
            media_csv = root / "fixture_media_assets.csv"
            output_csv = root / "fixture_media_cache_assets.csv"
            cache_root = root / "cache"
            columns = [
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
            write_csv(
                media_csv,
                columns,
                [
                    {
                        "item_pk": "body",
                        "media_index": "0",
                        "media_source": "top_level",
                        "asset_type": "image",
                        "best_image_url": source_image.as_uri(),
                    },
                    {
                        "item_pk": "body",
                        "media_index": "1",
                        "media_source": "top_level",
                        "asset_type": "video",
                        "best_image_url": source_image.as_uri(),
                        "best_video_url": source_video.as_uri(),
                    },
                    {
                        "item_pk": "body",
                        "media_index": "2",
                        "media_source": "top_level",
                        "asset_type": "unknown",
                    },
                ],
            )

            summary = cache_media_assets(
                media_assets_csv=media_csv,
                output_csv=output_csv,
                cache_root=cache_root,
                export_key="fixture",
                timeout=5,
                max_bytes=1024 * 1024,
            )
            rows = read_csv(output_csv)

            self.assertEqual(summary.rows, 4)
            self.assertEqual(summary.source_media_assets, 3)
            self.assertEqual(summary.eligible_media_assets, 3)
            self.assertEqual(summary.cached, 3)
            self.assertEqual(summary.missing_source_url, 1)
            self.assertEqual(list(rows[0].keys()), MEDIA_CACHE_COLUMNS)
            self.assertEqual(rows[0]["cache_status"], CACHE_STATUS_CACHED)
            self.assertEqual(rows[0]["input_kind"], "image")
            self.assertEqual(rows[0]["width"], "1")
            self.assertEqual(rows[0]["height"], "1")
            self.assertNotIn(source_image.as_uri(), output_csv.read_text(encoding="utf-8-sig"))
            video_rows = [row for row in rows if row["input_kind"] == "video"]
            self.assertEqual(len(video_rows), 1)
            self.assertEqual(video_rows[0]["cache_role"], "media")
            self.assertTrue((cache_root / video_rows[0]["cache_rel_path"]).exists())
            missing_rows = [
                row for row in rows if row["cache_status"] == CACHE_STATUS_MISSING_SOURCE_URL
            ]
            self.assertEqual(len(missing_rows), 1)

    def test_cache_media_assets_can_filter_to_selected_performance_grades(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_image = root / "source.png"
            source_image.write_bytes(PNG_1X1)
            media_csv = root / "fixture_media_assets.csv"
            performance_csv = root / "fixture_performance_labels.csv"
            output_csv = root / "fixture_media_cache_assets.csv"
            cache_root = root / "cache"
            media_columns = [
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
            write_csv(
                media_csv,
                media_columns,
                [
                    {
                        "item_pk": "gold_body",
                        "media_index": "0",
                        "media_source": "top_level",
                        "asset_type": "image",
                        "best_image_url": source_image.as_uri(),
                    },
                    {
                        "item_pk": "gold_link",
                        "media_index": "0",
                        "media_source": "top_level",
                        "asset_type": "image",
                        "best_image_url": source_image.as_uri(),
                    },
                    {
                        "item_pk": "c_body",
                        "media_index": "0",
                        "media_source": "top_level",
                        "asset_type": "image",
                        "best_image_url": source_image.as_uri(),
                    },
                ],
            )
            write_csv(
                performance_csv,
                ["match_id", "performance_grade"],
                [
                    {"match_id": "gold_body__gold_link", "performance_grade": "Gold"},
                    {"match_id": "c_body__c_link", "performance_grade": "C"},
                ],
            )

            allowlist = selected_item_pks_from_performance_labels(
                performance_csv,
                ["gold", "s", "a", "b"],
            )
            summary = cache_media_assets(
                media_assets_csv=media_csv,
                output_csv=output_csv,
                cache_root=cache_root,
                export_key="fixture",
                item_pk_allowlist=allowlist,
            )
            rows = read_csv(output_csv)

            self.assertEqual(allowlist, ["gold_body", "gold_link"])
            self.assertEqual(summary.source_media_assets, 3)
            self.assertEqual(summary.eligible_media_assets, 2)
            self.assertEqual(summary.rows, 2)
            self.assertEqual({row["item_pk"] for row in rows}, {"gold_body", "gold_link"})


if __name__ == "__main__":
    unittest.main()
