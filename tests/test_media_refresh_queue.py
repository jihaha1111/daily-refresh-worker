import csv
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.media_refresh_queue import (  # noqa: E402
    MEDIA_REFRESH_QUEUE_COLUMNS,
    prepare_media_refresh_queue,
)


def write_csv(path: Path, columns, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


ITEMS_COLUMNS = [
    "idx",
    "pk",
    "code",
    "threads_url",
    "taken_at",
    "user_id",
    "username",
    "text",
    "is_reply",
    "reply_to_author_id",
    "reply_to_author_username",
    "self_thread_position",
    "self_thread_length",
    "has_coupang_link",
    "coupang_urls",
    "first_coupang_url",
]

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

PERFORMANCE_COLUMNS = [
    "match_id",
    "match_confidence",
    "first_coupang_url",
    "body_username",
    "body_taken_at_iso",
    "body_text",
    "body_threads_url",
    "body_view_count",
    "body_like_count",
    "body_direct_reply_count",
    "body_repost_count",
    "body_reshare_count",
    "body_quote_count",
    "link_username",
    "link_taken_at_iso",
    "link_text",
    "link_threads_url",
    "link_coupang_urls",
    "link_first_coupang_url",
    "link_view_count",
    "link_like_count",
    "link_direct_reply_count",
    "link_repost_count",
    "link_reshare_count",
    "link_quote_count",
    "metric_version",
    "content_category",
    "is_recipe",
    "recipe_confidence",
    "link_view_rate",
    "body_engagement",
    "body_engagement_rate",
    "expected_clicks",
    "expected_revenue_krw",
    "exposure_tier",
    "performance_grade",
    "performance_type",
    "learning_segment",
    "coupang_score",
]


def performance_row(match_id: str, grade: str, body_url: str = "", link_url: str = ""):
    row = {column: "" for column in PERFORMANCE_COLUMNS}
    row.update(
        {
            "match_id": match_id,
            "match_confidence": "high",
            "performance_grade": grade,
            "body_threads_url": body_url,
            "link_threads_url": link_url,
        }
    )
    return row


def item_row(pk: str, url: str):
    row = {column: "" for column in ITEMS_COLUMNS}
    row.update({"pk": pk, "threads_url": url, "username": "fixture"})
    return row


def media_row(
    item_pk: str,
    media_index: str,
    asset_type: str,
    *,
    image_url: str = "",
    video_url: str = "",
):
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
            "best_image_width": "300",
            "best_image_height": "400",
            "best_video_url": video_url,
            "video_version_count": "1" if video_url else "0",
            "original_width": "300",
            "original_height": "400",
            "has_audio": "true" if video_url else "false",
        }
    )
    return row


class MediaRefreshQueueTests(unittest.TestCase):
    def run_fixture(self, performance_rows, media_rows, items_rows):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        performance_csv = root / "fixture_performance_labels.csv"
        media_csv = root / "fixture_media_assets.csv"
        items_csv = root / "fixture_items_core.csv"
        output_csv = root / "fixture_media_refresh_queue.csv"
        write_csv(performance_csv, PERFORMANCE_COLUMNS, performance_rows)
        write_csv(media_csv, MEDIA_COLUMNS, media_rows)
        write_csv(items_csv, ITEMS_COLUMNS, items_rows)
        summary = prepare_media_refresh_queue(
            performance_labels_csv=performance_csv,
            media_assets_csv=media_csv,
            items_core_csv=items_csv,
            output_csv=output_csv,
            export_key="fixture",
            performance_grades=["Gold", "S", "A", "B"],
            created_at="2026-06-02T00:00:00+00:00",
        )
        return summary, output_csv, read_csv(output_csv)

    def test_gold_s_a_b_only_body_and_link_media_are_queued(self) -> None:
        source_image = "https://cdn.example.test/body.jpg?secret=raw"
        source_video = "https://cdn.example.test/link.mp4?secret=raw"
        summary, output_csv, rows = self.run_fixture(
            [
                performance_row("gold_body__gold_link", "Gold"),
                performance_row("s_body__s_link", "S"),
                performance_row("a_body__a_link", "A"),
                performance_row("b_body__b_link", "B"),
                performance_row("c_body__c_link", "C"),
                performance_row("bad-match-id", "Gold"),
            ],
            [
                media_row("gold_body", "0", "image", image_url=source_image),
                media_row("gold_link", "0", "video", image_url=source_image, video_url=source_video),
                media_row("s_body", "0", "image", image_url=source_image),
                media_row("a_body", "0", "image", image_url=source_image),
                media_row("b_body", "0", "image", image_url=source_image),
                media_row("c_body", "0", "image", image_url=source_image),
            ],
            [
                item_row("gold_body", "https://www.threads.com/@fixture/post/GOLD_BODY"),
                item_row("gold_link", "https://www.threads.com/@fixture/post/GOLD_LINK"),
                item_row("s_body", "https://www.threads.com/@fixture/post/S_BODY"),
                item_row("a_body", "https://www.threads.com/@fixture/post/A_BODY"),
                item_row("b_body", "https://www.threads.com/@fixture/post/B_BODY"),
                item_row("c_body", "https://www.threads.com/@fixture/post/C_BODY"),
            ],
        )

        self.assertEqual(list(rows[0].keys()), MEDIA_REFRESH_QUEUE_COLUMNS)
        self.assertEqual(summary.selected_matches, 4)
        self.assertEqual(summary.skipped_invalid_match_ids, 1)
        self.assertNotIn("c_body", {row["item_pk"] for row in rows})
        self.assertEqual({row["performance_grade"] for row in rows}, {"Gold", "S", "A", "B"})
        self.assertIn("body", {row["match_side"] for row in rows})
        self.assertIn("link", {row["match_side"] for row in rows})
        self.assertEqual(
            [
                (row["cache_role"], row["input_kind"])
                for row in rows
                if row["item_pk"] == "gold_link"
            ],
            [("preview", "image"), ("media", "video")],
        )
        output_text = output_csv.read_text(encoding="utf-8-sig")
        self.assertNotIn(source_image, output_text)
        self.assertNotIn(source_video, output_text)

    def test_missing_source_url_still_emits_threads_refreshable_row(self) -> None:
        _summary, _output_csv, rows = self.run_fixture(
            [
                performance_row(
                    "missing_body__missing_link",
                    "Gold",
                    body_url="https://www.threads.com/@fixture/post/FALLBACK_BODY",
                    link_url="https://www.threads.com/@fixture/post/FALLBACK_LINK",
                )
            ],
            [media_row("missing_body", "0", "image"), media_row("missing_link", "0", "video")],
            [item_row("missing_body", ""), item_row("missing_link", "")],
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual({row["has_existing_source_url"] for row in rows}, {"false"})
        self.assertEqual(
            {
                row["threads_url"]
                for row in rows
            },
            {
                "https://www.threads.com/@fixture/post/FALLBACK_BODY",
                "https://www.threads.com/@fixture/post/FALLBACK_LINK",
            },
        )

    def test_duplicate_queue_ids_are_deduped_deterministically(self) -> None:
        summary, _output_csv, rows = self.run_fixture(
            [
                performance_row("dup_body__dup_link", "Gold"),
                performance_row("dup_body__dup_link", "Gold"),
            ],
            [media_row("dup_body", "0", "image", image_url="https://cdn.example.test/dup.jpg")],
            [item_row("dup_body", "https://www.threads.com/@fixture/post/DUP_BODY")],
        )

        self.assertEqual(summary.duplicate_rows, 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["queue_id"],
            "fixture:dup_body__dup_link:body:dup_body:0:media",
        )
        self.assertEqual(rows[0]["priority"], "1")


if __name__ == "__main__":
    unittest.main()
