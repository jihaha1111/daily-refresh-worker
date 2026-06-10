import csv
import json
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

from analyze_coupang_performance import run_analysis  # noqa: E402
from threads_coupang_pipeline.performance import (  # noqa: E402
    GRADE_GOLD,
    GRADE_V2_A,
    GRADE_V2_B,
    GRADE_V2_C,
    GRADE_V2_REFERENCE,
    GRADE_V2_S,
    METRIC_VERSION_V2_REVENUE_PROXY,
    TYPE_VIRAL,
    body_engagement,
    build_performance_labels,
    expected_revenue_krw,
    link_view_rate,
)


def make_row(
    match_id: str,
    body_view_count: str,
    link_view_count: str,
    body_like_count: str = "0",
    body_direct_reply_count: str = "0",
    body_repost_count: str = "0",
    body_reshare_count: str = "0",
    body_quote_count: str = "0",
) -> dict:
    return {
        "match_id": match_id,
        "match_confidence": "high",
        "first_coupang_url": "https://link.coupang.com/a/test",
        "body_username": "fixture_user",
        "body_taken_at_iso": "2026-04-22T00:00:00+09:00",
        "body_text": f"body {match_id}",
        "body_threads_url": f"https://www.threads.com/@fixture/post/{match_id}b",
        "body_view_count": body_view_count,
        "body_like_count": body_like_count,
        "body_direct_reply_count": body_direct_reply_count,
        "body_repost_count": body_repost_count,
        "body_reshare_count": body_reshare_count,
        "body_quote_count": body_quote_count,
        "link_username": "fixture_user",
        "link_taken_at_iso": "2026-04-22T00:01:00+09:00",
        "link_text": f"link {match_id}",
        "link_threads_url": f"https://www.threads.com/@fixture/post/{match_id}l",
        "link_coupang_urls": "https://link.coupang.com/a/test",
        "link_first_coupang_url": "https://link.coupang.com/a/test",
        "link_view_count": link_view_count,
        "link_like_count": "0",
        "link_direct_reply_count": "0",
        "link_repost_count": "0",
        "link_reshare_count": "0",
        "link_quote_count": "0",
    }


class PerformanceTests(unittest.TestCase):
    def test_rate_and_engagement_helpers(self) -> None:
        self.assertEqual(link_view_rate(1000, 125), 0.125)
        self.assertIsNone(link_view_rate(0, 125))
        self.assertIsNone(link_view_rate(None, 125))
        self.assertEqual(body_engagement(10, 2, 3, 4, 5), 75)

    def test_build_performance_labels_grades_and_viral_type(self) -> None:
        labels = build_performance_labels(
            [
                make_row("gold", "100000", "10000"),
                make_row("s", "120000", "6000"),
                make_row("a", "25000", "2000"),
                make_row("b", "10000", "500"),
                make_row("c", "10000", "100"),
                make_row("viral", "2000000", "6000"),
                make_row("reference", "4000", "1000"),
                make_row("missing_link", "6000", ""),
            ]
        )
        by_id = {row["match_id"]: row for row in labels}

        self.assertEqual(by_id["gold"]["performance_grade"], GRADE_GOLD)
        self.assertEqual(by_id["s"]["performance_grade"], GRADE_V2_S)
        self.assertEqual(by_id["a"]["performance_grade"], GRADE_V2_A)
        self.assertEqual(by_id["b"]["performance_grade"], GRADE_V2_B)
        self.assertEqual(by_id["c"]["performance_grade"], GRADE_V2_C)
        self.assertEqual(by_id["reference"]["performance_grade"], GRADE_V2_REFERENCE)
        self.assertEqual(by_id["missing_link"]["performance_grade"], GRADE_V2_REFERENCE)
        self.assertEqual(by_id["viral"]["performance_type"], TYPE_VIRAL)

    def test_coupang_score_only_for_stable_rows_with_link_view(self) -> None:
        labels = build_performance_labels(
            [
                make_row("low", "5000", "100", body_like_count="1"),
                make_row("high", "20000", "3000", body_like_count="100"),
                make_row("reference", "4000", "1000"),
                make_row("missing_link", "6000", ""),
            ]
        )
        by_id = {row["match_id"]: row for row in labels}

        self.assertIsNotNone(by_id["low"]["coupang_score"])
        self.assertIsNotNone(by_id["high"]["coupang_score"])
        self.assertGreater(by_id["high"]["coupang_score"], by_id["low"]["coupang_score"])
        self.assertIsNone(by_id["reference"]["coupang_score"])
        self.assertIsNone(by_id["missing_link"]["coupang_score"])

    def test_build_performance_labels_v2_revenue_proxy_grades(self) -> None:
        labels = build_performance_labels(
            [
                make_row("gold", "100000", "10000"),
                make_row("s", "120000", "6000"),
                make_row("a", "25000", "2000"),
                make_row("a_small", "10000", "2000"),
                make_row("b", "10000", "500"),
                make_row("c", "10000", "100"),
                make_row("viral_low", "2000000", "6000"),
                make_row("reference", "4999", "1000"),
            ]
        )
        by_id = {row["match_id"]: row for row in labels}

        self.assertEqual(by_id["gold"]["performance_grade"], GRADE_GOLD)
        self.assertEqual(by_id["s"]["performance_grade"], GRADE_V2_S)
        self.assertEqual(by_id["a"]["performance_grade"], GRADE_V2_A)
        self.assertEqual(by_id["a_small"]["performance_grade"], GRADE_V2_A)
        self.assertEqual(by_id["b"]["performance_grade"], GRADE_V2_B)
        self.assertEqual(by_id["c"]["performance_grade"], GRADE_V2_C)
        self.assertEqual(by_id["reference"]["performance_grade"], GRADE_V2_REFERENCE)
        self.assertEqual(by_id["gold"]["expected_clicks"], 1000.0)
        self.assertEqual(by_id["gold"]["expected_revenue_krw"], 100000)
        self.assertEqual(expected_revenue_krw(12000), 120000)
        self.assertEqual(by_id["a"]["learning_segment"], "target_high_performance")
        self.assertEqual(by_id["a_small"]["learning_segment"], "strong_candidate")
        self.assertEqual(by_id["viral_low"]["learning_segment"], "viral_low_conversion")
        self.assertEqual(by_id["reference"]["exposure_tier"], "discovery")

    def test_cli_writes_labels_sample_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_csv = tmp_path / "fixture_match_content_summary.csv"
            rows = [
                make_row("s", "10000", "1600"),
                make_row("b", "5000", "300"),
                make_row("reference", "4000", "100"),
            ]
            with input_csv.open("w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)

            summary = run_analysis(input_csv, sample_size=3)

            labels_csv = tmp_path / "fixture_performance_labels.csv"
            labels_json = tmp_path / "fixture_performance_labels.json"
            sample_csv = tmp_path / "fixture_tagging_sample.csv"
            sample_json = tmp_path / "fixture_tagging_sample.json"
            summary_json = tmp_path / "fixture_performance_summary.json"

            for path in [labels_csv, labels_json, sample_csv, sample_json, summary_json]:
                self.assertTrue(path.exists(), path)

            labels = json.loads(labels_json.read_text(encoding="utf-8"))
            sample = json.loads(sample_json.read_text(encoding="utf-8"))
            self.assertEqual(len(labels), 3)
            self.assertEqual(len(sample), 3)
            self.assertEqual(summary["metric_version"], METRIC_VERSION_V2_REVENUE_PROXY)
            self.assertEqual(summary["content_scope"], "non_recipe")
            self.assertIn("category_tag", sample[0])
            self.assertIn("content_category", labels[0])
            self.assertIn("expected_revenue_krw", labels[0])
            self.assertEqual(summary["output_prefix"], "fixture")

    def test_cli_content_scope_filters_recipe_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_csv = tmp_path / "fixture_match_content_summary.csv"
            rows = [
                make_row("general", "10000", "1000"),
                {
                    **make_row("recipe", "10000", "1000"),
                    "link_text": "재료: 계란 2개. 조리 순서: 섞어 굽기",
                },
            ]
            with input_csv.open("w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)

            non_recipe_summary = run_analysis(input_csv, sample_size=2)
            non_recipe_labels = json.loads(
                (tmp_path / "fixture_performance_labels.json").read_text(encoding="utf-8")
            )
            recipe_summary = run_analysis(
                input_csv,
                output_prefix="fixture_recipe",
                sample_size=2,
                content_scope="recipe_only",
            )
            recipe_labels = json.loads(
                (tmp_path / "fixture_recipe_performance_labels.json").read_text(encoding="utf-8")
            )

            self.assertEqual(non_recipe_summary["content_scope_rows"], 1)
            self.assertEqual(non_recipe_labels[0]["match_id"], "general")
            self.assertEqual(recipe_summary["content_scope_rows"], 1)
            self.assertEqual(recipe_labels[0]["match_id"], "recipe")
            self.assertTrue(recipe_labels[0]["is_recipe"])


if __name__ == "__main__":
    unittest.main()
