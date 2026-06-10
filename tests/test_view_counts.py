import csv
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.view_counts import (  # noqa: E402
    ViewCountError,
    apply_view_counts_to_items,
    extract_post_key_from_url,
    read_view_count_csv,
)


def write_csv(path: Path, columns, rows) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


class ViewCountTests(unittest.TestCase):
    def test_extract_post_key_from_url_uses_username_and_code(self) -> None:
        self.assertEqual(
            extract_post_key_from_url("https://example.test/@Fixture_User/post/BODY001?x=1"),
            ("fixture_user", "BODY001"),
        )
        self.assertIsNone(extract_post_key_from_url("https://l.threads.com/?u=https%3A%2F%2Fexample"))

    def test_read_view_count_csv_supports_empty_unavailable_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "view_counts.csv"
            write_csv(
                path,
                ["idx", "url", "view_counts_value"],
                [
                    {
                        "idx": "1",
                        "url": "https://example.test/@fixture_user/post/BODY001",
                        "view_counts_value": "42",
                    },
                    {
                        "idx": "2",
                        "url": "https://example.test/@fixture_user/post/LINK001",
                        "view_counts_value": "",
                    },
                ],
            )

            values, summary = read_view_count_csv(path)

            self.assertEqual(values[("fixture_user", "BODY001")], 42)
            self.assertIsNone(values[("fixture_user", "LINK001")])
            self.assertEqual(summary["external_view_count_rows"], 2)
            self.assertEqual(summary["external_view_count_available_items"], 1)
            self.assertEqual(summary["external_view_count_unavailable_rows"], 1)

    def test_apply_view_counts_to_items_matches_by_raw_username_and_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "view_counts.csv"
            write_csv(
                path,
                ["url", "view_count"],
                [
                    {
                        "url": "https://example.test/@fixture_user/post/BODY001",
                        "view_count": "100",
                    },
                    {
                        "url": "https://example.test/@fixture_user/post/LINK001",
                        "view_count": "",
                    },
                ],
            )
            items = [
                {"username": "Fixture_User", "code": "BODY001", "view_count": None},
                {"username": "fixture_user", "code": "LINK001", "view_count": 9},
                {"username": "other_user", "code": "MISSING001", "view_count": None},
            ]

            summary = apply_view_counts_to_items(items, path)

            self.assertEqual(items[0]["view_count"], 100)
            self.assertIsNone(items[1]["view_count"])
            self.assertIsNone(items[2]["view_count"])
            self.assertEqual(summary["external_view_count_matched_items"], 2)
            self.assertEqual(summary["external_view_count_missing_items"], 1)

    def test_invalid_view_count_inputs_fail(self) -> None:
        cases = [
            (
                "duplicate",
                [
                    {
                        "url": "https://example.test/@fixture_user/post/BODY001",
                        "view_counts_value": "1",
                    },
                    {
                        "url": "https://example.test/@fixture_user/post/BODY001",
                        "view_counts_value": "2",
                    },
                ],
            ),
            (
                "negative",
                [
                    {
                        "url": "https://example.test/@fixture_user/post/BODY001",
                        "view_counts_value": "-1",
                    }
                ],
            ),
            (
                "non-integer",
                [
                    {
                        "url": "https://example.test/@fixture_user/post/BODY001",
                        "view_counts_value": "many",
                    }
                ],
            ),
            (
                "non-post-url",
                [{"url": "https://example.test/not-a-post", "view_counts_value": "1"}],
            ),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            for name, rows in cases:
                with self.subTest(name=name):
                    path = Path(tmp) / f"{name}.csv"
                    write_csv(path, ["url", "view_counts_value"], rows)
                    with self.assertRaises(ViewCountError):
                        read_view_count_csv(path)

    def test_unknown_view_count_url_fails_against_raw_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "view_counts.csv"
            write_csv(
                path,
                ["url", "view_counts_value"],
                [
                    {
                        "url": "https://example.test/@missing_user/post/UNKNOWN001",
                        "view_counts_value": "1",
                    }
                ],
            )

            with self.assertRaises(ViewCountError):
                apply_view_counts_to_items(
                    [{"username": "fixture_user", "code": "BODY001", "view_count": None}],
                    path,
                )


if __name__ == "__main__":
    unittest.main()
