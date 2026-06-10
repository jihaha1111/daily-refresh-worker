import csv
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.lookup_prepare import (  # noqa: E402
    AF_LINK_LOOKUP_INPUT_FIELDS,
    AF_LINK_LOOKUP_UNIQUE_URL_FIELDS,
    MATCH_VIEW_CANDIDATE_FIELDS,
    VIEW_LOOKUP_INPUT_FIELDS,
    VIEW_LOOKUP_MAP_FIELDS,
    VIEW_LOOKUP_SKIPPED_FIELDS,
    build_lookup_prepare_result,
    write_lookup_prepare_outputs,
)


def make_row(match_id: str, body_like_count: str, first_coupang_url: str = "https://link.coupang.com/a/test") -> dict:
    return {
        "match_id": match_id,
        "match_confidence": "high",
        "first_coupang_url": first_coupang_url,
        "link_coupang_urls": first_coupang_url,
        "body_username": "fixture_user",
        "body_taken_at_iso": "2026-04-30T12:00:00+09:00",
        "body_threads_url": f"https://www.threads.com/@fixture_user/post/{match_id}BODY",
        "body_like_count": body_like_count,
        "body_direct_reply_count": "1",
        "body_repost_count": "0",
        "body_reshare_count": "0",
        "body_quote_count": "0",
        "body_view_count": "",
        "link_username": "fixture_user",
        "link_taken_at_iso": "2026-04-30T12:01:00+09:00",
        "link_threads_url": f"https://www.threads.com/@fixture_user/post/{match_id}LINK",
        "link_like_count": "0",
        "link_direct_reply_count": "0",
        "link_repost_count": "0",
        "link_reshare_count": "0",
        "link_quote_count": "0",
        "link_view_count": "",
    }


def read_rows(path: Path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def item_row(pk: str, username: str = "fixture_user") -> dict:
    return {
        "pk": pk,
        "threads_url": f"https://www.threads.com/@{username}/post/{pk}",
        "taken_at": "1800000000",
        "user_id": f"{username}_id",
        "username": username,
        "is_reply": "true",
        "has_coupang_link": "true",
    }


def link_row(item_pk: str, link_index: str, url: str, source: str = "text_fragment") -> dict:
    return {
        "item_pk": item_pk,
        "link_index": link_index,
        "source": source,
        "url": url,
        "is_coupang_link": "true",
    }


class LookupPrepareTests(unittest.TestCase):
    def test_build_lookup_outputs_after_body_like_filter(self) -> None:
        result = build_lookup_prepare_result(
            [
                make_row("eligible", "4"),
                make_row("skipped_low", "3"),
                make_row("skipped_missing", ""),
            ],
            export_key="260430",
            body_like_threshold=4,
            item_links_rows=[
                link_row("link_1", "0", "https://link.coupang.com/a/test💜"),
                link_row("link_2", "0", "https://link.coupang.com/a/test"),
            ],
            items_core_rows=[item_row("link_1"), item_row("link_2", "other_user")],
            matches_core_rows=[{"match_id": "eligible", "link_pk": "link_1", "body_pk": "body_1"}],
            exceptions_core_rows=[{"item_pk": "link_2", "exception_type": "self_reply_link_without_matched_body"}],
        )

        self.assertEqual(result.total_matches, 3)
        self.assertEqual(result.eligible_matches, 1)
        self.assertEqual(result.skipped_matches, 2)
        self.assertEqual(result.lookup_url_rows, 2)
        self.assertEqual(result.af_link_lookup_rows, 2)
        self.assertEqual(result.af_link_unique_url_rows, 1)
        self.assertEqual(result.policy_version, "v1_body_like_ge_4_after_matching")

        self.assertEqual(result.view_lookup_input[0]["idx"], 1)
        self.assertEqual(result.view_lookup_input[0]["url"], "https://www.threads.com/@fixture_user/post/eligibleBODY")
        self.assertEqual(result.view_lookup_input[1]["idx"], 2)
        self.assertEqual(result.view_lookup_map[0]["lookup_id"], "eligible:body")
        self.assertEqual(result.view_lookup_map[0]["role"], "body")
        self.assertEqual(result.view_lookup_map[0]["target_view_field"], "body_view_count")
        self.assertEqual(result.view_lookup_map[0]["threads_post_key"], "fixture_user/eligibleBODY")
        self.assertEqual(result.view_lookup_map[1]["lookup_id"], "eligible:link")
        self.assertEqual(result.view_lookup_map[1]["target_view_field"], "link_view_count")
        self.assertEqual(
            result.view_lookup_map[0]["first_coupang_url"],
            result.view_lookup_map[1]["first_coupang_url"],
        )

        self.assertEqual(result.view_lookup_skipped[0]["view_lookup_skip_reason"], "body_like_count_lt_4")
        self.assertEqual(result.view_lookup_skipped[1]["view_lookup_skip_reason"], "body_like_count_missing")
        self.assertEqual(result.af_link_lookup_input[0]["match_id"], "eligible")
        self.assertEqual(result.af_link_lookup_input[0]["match_role"], "link")
        self.assertEqual(result.af_link_lookup_input[1]["exception_type"], "self_reply_link_without_matched_body")
        self.assertEqual(result.af_link_lookup_unique_urls[0]["evidence_count"], 2)
        self.assertEqual(result.match_view_candidates[0]["view_lookup_url_count"], 2)

    def test_write_lookup_outputs_with_stable_headers(self) -> None:
        result = build_lookup_prepare_result(
            [make_row("eligible", "5"), make_row("skipped", "0")],
            export_key="260430",
            body_like_threshold=4,
            item_links_rows=[link_row("link_1", "0", "https://link.coupang.com/a/test")],
            items_core_rows=[item_row("link_1")],
        )

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            manifest = write_lookup_prepare_outputs(result, output_dir)

            expected_files = {
                "match_view_candidates_csv": MATCH_VIEW_CANDIDATE_FIELDS,
                "view_lookup_input_csv": VIEW_LOOKUP_INPUT_FIELDS,
                "view_lookup_map_csv": VIEW_LOOKUP_MAP_FIELDS,
                "view_lookup_skipped_csv": VIEW_LOOKUP_SKIPPED_FIELDS,
                "af_link_lookup_input_csv": AF_LINK_LOOKUP_INPUT_FIELDS,
                "af_link_lookup_unique_urls_csv": AF_LINK_LOOKUP_UNIQUE_URL_FIELDS,
            }
            for key, fields in expected_files.items():
                path = Path(manifest["output_files"][key])
                self.assertTrue(path.exists(), key)
                with path.open(encoding="utf-8", newline="") as f:
                    reader = csv.reader(f)
                    self.assertEqual(next(reader), list(fields))

            self.assertEqual(len(read_rows(output_dir / "match-view-candidates-260430.csv")), 1)
            self.assertEqual(len(read_rows(output_dir / "view-lookup-input-260430.csv")), 2)
            self.assertEqual(len(read_rows(output_dir / "view-lookup-map-260430.csv")), 2)
            self.assertEqual(len(read_rows(output_dir / "view-lookup-skipped-260430.csv")), 1)
            self.assertEqual(len(read_rows(output_dir / "af-link-lookup-input-260430.csv")), 1)
            self.assertEqual(len(read_rows(output_dir / "af-link-lookup-unique-urls-260430.csv")), 1)
            self.assertEqual(manifest["eligible_matches"], 1)


if __name__ == "__main__":
    unittest.main()
