import csv
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.af_lookup_resolver import (  # noqa: E402
    AF_ACCOUNT_MAP_FIELDS,
    AF_LINK_LOOKUP_RESULT_FIELDS,
    AF_LINK_LOOKUP_UNIQUE_RESULT_FIELDS,
    HttpHop,
    find_affiliate_evidence,
    normalize_coupang_short_link,
    resolve_af_lookup_rows,
    write_af_lookup_outputs,
)


def make_row(
    match_id: str,
    coupang_url: str = "https://link.coupang.com/a/short",
    username: str = "fixture_user",
    item_pk: str = "link_item",
) -> dict:
    return {
        "idx": "1",
        "export_key": "260430",
        "item_pk": item_pk,
        "link_index": "0",
        "coupang_url": coupang_url,
        "normalized_coupang_url": coupang_url,
        "source": "text_fragment",
        "user_id": f"{username}_id",
        "username": username,
        "threads_url": f"https://www.threads.com/@{username}/post/{item_pk}",
        "taken_at": "1800000000",
        "is_reply": "true",
        "item_has_coupang_link": "true",
        "match_id": match_id,
        "match_role": "link",
        "exception_type": "",
    }


class AfLookupResolverTests(unittest.TestCase):
    def test_normalize_coupang_short_link_strips_trailing_comment_text(self) -> None:
        self.assertEqual(
            normalize_coupang_short_link("https://link.coupang.com/a/ezCai2💜"),
            "https://link.coupang.com/a/ezCai2",
        )
        self.assertEqual(
            normalize_coupang_short_link("link.coupang.com/a/abc123\u2060"),
            "https://link.coupang.com/a/abc123",
        )

    def test_find_affiliate_evidence_returns_raw_af_id(self) -> None:
        raw_af_id = "synthetic-af-001"
        url = f"https://www.coupang.com/vp/products/123?itemId=123&lptag={raw_af_id}&subid=private"

        evidence = find_affiliate_evidence(url)
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual(evidence.af_id, raw_af_id)
        self.assertIn("itemId=123", evidence.normalized_destination_url)
        self.assertNotIn(raw_af_id, evidence.normalized_destination_url)
        self.assertNotIn("private", evidence.normalized_destination_url)

    def test_resolve_rows_from_redirect_location_and_group_account_map(self) -> None:
        raw_af_id = "synthetic-af-shared"
        final_url = f"https://www.coupang.com/vp/products/123?itemId=123&lptag={raw_af_id}"
        calls = []

        def fake_request(url: str) -> HttpHop:
            calls.append(url)
            return HttpHop(status=302, url=url, location=final_url)

        result = resolve_af_lookup_rows(
            [
                make_row("match_1", username="account_a", item_pk="link_1"),
                make_row("match_2", username="account_b", item_pk="link_2"),
                make_row("match_3", username="account_a", item_pk="link_3"),
            ],
            export_key="260430",
            request_func=fake_request,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(result.total_rows, 3)
        self.assertEqual(result.resolved_rows, 3)
        self.assertEqual(result.unique_af_ids, 1)
        self.assertEqual(result.unique_url_rows, 1)
        self.assertEqual(result.account_pair_rows, 2)
        self.assertEqual(result.results[0]["af_lookup_status"], "resolved_redirect")
        self.assertEqual(result.results[0]["af_id"], raw_af_id)
        self.assertEqual(result.results[0]["username"], "account_a")
        account_rows = {
            row["thread_username"]: row
            for row in result.account_map
        }
        self.assertEqual(account_rows["account_a"]["evidence_count"], 2)
        self.assertEqual(account_rows["account_b"]["evidence_count"], 1)

    def test_parallel_resolution_preserves_input_order(self) -> None:
        raw_af_id = "synthetic-af-parallel"
        final_url = f"https://www.coupang.com/vp/products/123?lptag={raw_af_id}"

        def fake_request(url: str) -> HttpHop:
            return HttpHop(status=302, url=url, location=final_url)

        result = resolve_af_lookup_rows(
            [
                make_row("first", item_pk="first"),
                make_row("second", item_pk="second", coupang_url="https://link.coupang.com/a/second"),
                make_row("third", item_pk="third", coupang_url="https://link.coupang.com/a/third"),
            ],
            export_key="260430",
            request_func=fake_request,
            max_workers=3,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        self.assertEqual(
            [row["match_id"] for row in result.results],
            ["first", "second", "third"],
        )
        self.assertEqual(result.resolved_rows, 3)

    def test_row_level_request_error_does_not_abort_run(self) -> None:
        def fake_request(url: str) -> HttpHop:
            raise UnicodeEncodeError("ascii", "\ufffc", 0, 1, "synthetic")

        result = resolve_af_lookup_rows(
            [make_row("bad_url"), make_row("missing", coupang_url="")],
            export_key="260430",
            request_func=fake_request,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        self.assertEqual(result.total_rows, 2)
        self.assertEqual(result.resolved_rows, 0)
        self.assertEqual(result.failed_rows, 2)
        self.assertEqual(result.results[0]["af_lookup_status"], "not_resolved")
        self.assertEqual(result.results[0]["error"], "UnicodeEncodeError")
        self.assertEqual(result.results[1]["af_lookup_status"], "missing_coupang_url")

    def test_write_outputs_with_stable_headers(self) -> None:
        raw_af_id = "synthetic-af-direct"
        result = resolve_af_lookup_rows(
            [
                make_row(
                    "match_1",
                    coupang_url=f"https://www.coupang.com/vp/products/1?lptag={raw_af_id}",
                )
            ],
            export_key="260430",
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            manifest = write_af_lookup_outputs(result, output_dir)

            expected_files = {
                "af_link_lookup_results_csv": AF_LINK_LOOKUP_RESULT_FIELDS,
                "af_link_lookup_unique_results_csv": AF_LINK_LOOKUP_UNIQUE_RESULT_FIELDS,
                "af_account_map_csv": AF_ACCOUNT_MAP_FIELDS,
            }
            for key, fields in expected_files.items():
                path = Path(manifest["output_files"][key])
                self.assertTrue(path.exists(), key)
                with path.open(encoding="utf-8", newline="") as f:
                    reader = csv.reader(f)
                    self.assertEqual(next(reader), list(fields))

            self.assertTrue((output_dir / "af-link-lookup-manifest-260430.json").exists())
            self.assertEqual(manifest["resolved_rows"], 1)


if __name__ == "__main__":
    unittest.main()
