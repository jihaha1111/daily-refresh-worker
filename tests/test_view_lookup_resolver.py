import csv
import io
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.view_lookup_resolver import (  # noqa: E402
    HttpResponse,
    VIEW_LOOKUP_RESULT_FIELDS,
    VIEW_LOOKUP_STATE_FIELDS,
    extract_view_count_from_html,
    fetch_for_mode,
    merge_view_lookup_outputs,
    parse_fetch_diagnostics,
    parse_compact_count,
    prepare_initial_state,
    probe_lookup_rows,
    split_numbered_rows,
    summarize_internal_fetch_statuses,
)


def write_csv(path: Path, columns, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_rows(path: Path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


class ViewLookupResolverTests(unittest.TestCase):
    def test_prepare_initial_filters_to_threads_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "view-lookup-input-260430.csv"
            write_csv(
                input_path,
                ["idx", "url"],
                [
                    {"idx": "1", "url": "https://www.threads.com/@fixture/post/AAA"},
                    {"idx": "2", "url": "https://threads.net/@fixture/post/BBB"},
                    {"idx": "3", "url": "https://example.test/@fixture/post/CCC"},
                    {"idx": "4", "url": "not-a-url"},
                ],
            )

            summary = prepare_initial_state(input_path, root / "prepared")

            self.assertEqual(summary["all_input_rows"], 2)
            self.assertEqual(
                read_rows(root / "prepared" / "all_input.csv"),
                [
                    {"idx": "1", "url": "https://www.threads.com/@fixture/post/AAA"},
                    {"idx": "2", "url": "https://threads.net/@fixture/post/BBB"},
                ],
            )
            self.assertEqual(
                (root / "prepared" / "numbered.txt").read_text(encoding="utf-8"),
                "1\thttps://www.threads.com/@fixture/post/AAA\n"
                "2\thttps://threads.net/@fixture/post/BBB\n",
            )

    def test_prepare_initial_can_limit_rows_for_runner_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "view-lookup-input-260430.csv"
            write_csv(
                input_path,
                ["idx", "url"],
                [
                    {"idx": "1", "url": "https://www.threads.com/@fixture/post/AAA"},
                    {"idx": "2", "url": "https://www.threads.com/@fixture/post/BBB"},
                    {"idx": "3", "url": "https://www.threads.com/@fixture/post/CCC"},
                ],
            )

            summary = prepare_initial_state(input_path, root / "prepared", max_rows=2)

            self.assertEqual(summary["source_rows"], 3)
            self.assertEqual(summary["all_input_rows"], 2)
            self.assertEqual(len(read_rows(root / "prepared" / "all_input.csv")), 2)

    def test_split_numbered_rows_uses_fixed_shard_size_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            numbered = root / "numbered.txt"
            numbered.write_text(
                "".join(
                    f"{idx}\thttps://www.threads.com/@fixture/post/{idx}\n"
                    for idx in range(1, 8)
                ),
                encoding="utf-8",
            )
            shard_dir = root / "shards"
            shard_dir.mkdir()
            (shard_dir / "shard-99.tsv").write_text("stale\n", encoding="utf-8")

            summary = split_numbered_rows(numbered, shard_dir, shard_size=3)

            self.assertEqual(summary["split_mode"], "fixed_size")
            self.assertEqual(summary["total_rows"], 7)
            self.assertEqual(summary["shard_count"], 3)
            self.assertEqual(
                summary["shard_rows"],
                [
                    {"shard": 1, "rows": 3},
                    {"shard": 2, "rows": 3},
                    {"shard": 3, "rows": 1},
                ],
            )
            self.assertEqual(
                summary["matrix"],
                {"include": [{"shard": 1}, {"shard": 2}, {"shard": 3}]},
            )
            self.assertFalse((shard_dir / "shard-99.tsv").exists())

    def test_split_numbered_rows_balances_exact_target_shard_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            numbered = root / "numbered.txt"
            numbered.write_text(
                "".join(
                    f"{idx}\thttps://www.threads.com/@fixture/post/{idx}\n"
                    for idx in range(1, 11)
                ),
                encoding="utf-8",
            )

            summary = split_numbered_rows(
                numbered,
                root / "shards",
                shard_size=50,
                target_shard_count=3,
            )

            self.assertEqual(summary["split_mode"], "balanced")
            self.assertEqual(summary["total_rows"], 10)
            self.assertEqual(summary["shard_count"], 3)
            self.assertEqual(
                summary["shard_rows"],
                [
                    {"shard": 1, "rows": 4},
                    {"shard": 2, "rows": 3},
                    {"shard": 3, "rows": 3},
                ],
            )
            self.assertEqual(
                (root / "shards" / "shard-1.tsv").read_text(encoding="utf-8").count("\n"),
                4,
            )
            self.assertEqual(
                (root / "shards" / "shard-2.tsv").read_text(encoding="utf-8").count("\n"),
                3,
            )
            self.assertEqual(
                (root / "shards" / "shard-3.tsv").read_text(encoding="utf-8").count("\n"),
                3,
            )

    def test_split_numbered_rows_target_count_can_create_empty_shards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            numbered = root / "numbered.txt"
            numbered.write_text(
                "1\thttps://www.threads.com/@fixture/post/1\n"
                "2\thttps://www.threads.com/@fixture/post/2\n",
                encoding="utf-8",
            )

            summary = split_numbered_rows(
                numbered,
                root / "shards",
                shard_size=50,
                target_shard_count=4,
            )

            self.assertEqual(summary["shard_count"], 4)
            self.assertEqual(
                summary["shard_rows"],
                [
                    {"shard": 1, "rows": 1},
                    {"shard": 2, "rows": 1},
                    {"shard": 3, "rows": 0},
                    {"shard": 4, "rows": 0},
                ],
            )
            self.assertEqual((root / "shards" / "shard-4.tsv").read_text(encoding="utf-8"), "")

    def test_split_numbered_rows_balanced_empty_queue_has_no_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            numbered = root / "numbered.txt"
            numbered.write_text("", encoding="utf-8")

            summary = split_numbered_rows(
                numbered,
                root / "shards",
                shard_size=50,
                target_shard_count=4,
            )

            self.assertEqual(summary["split_mode"], "balanced")
            self.assertFalse(summary["has_work"])
            self.assertEqual(summary["shard_count"], 0)
            self.assertEqual(summary["matrix"], {"include": []})

    def test_extract_view_count_from_json_script(self) -> None:
        markup = """
        <html><script type="application/json">
        {"props":{"post":{"code":"ABC123","text_post_app_info":{"view_count":12345}}}}
        </script></html>
        """

        self.assertEqual(
            extract_view_count_from_html(
                markup,
                "https://www.threads.com/@fixture/post/ABC123",
            ),
            12345,
        )

    def test_extract_view_count_from_visible_text(self) -> None:
        self.assertEqual(parse_compact_count("1.2K"), 1200)
        self.assertEqual(parse_compact_count("3.4만"), 34000)
        self.assertEqual(
            extract_view_count_from_html(
                "<html><body><span>조회수 1.2만</span></body></html>",
                "https://www.threads.com/@fixture/post/ABC123",
            ),
            12000,
        )

    def test_probe_rows_stops_after_rate_limit(self) -> None:
        calls = []

        def fake_fetch(url: str) -> HttpResponse:
            calls.append(url)
            if url.endswith("ONE"):
                return HttpResponse(
                    status=200,
                    body='{"code":"ONE","view_count":77}',
                    final_url=url,
                )
            return HttpResponse(status=429, body="", final_url=url, error="too_many_requests")

        rows = [
            {"idx": "1", "url": "https://www.threads.com/@fixture/post/ONE"},
            {"idx": "2", "url": "https://www.threads.com/@fixture/post/TWO"},
            {"idx": "3", "url": "https://www.threads.com/@fixture/post/THREE"},
        ]

        results = probe_lookup_rows(
            rows,
            fetch_func=fake_fetch,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        self.assertEqual(len(calls), 2)
        self.assertEqual([row["view_lookup_status"] for row in results], [
            "resolved",
            "rate_limited",
            "unexecuted_after_rate_limit",
        ])
        self.assertEqual(results[0]["view_counts_value"], 77)
        self.assertEqual(results[2]["http_status"], "")

    def test_probe_rows_retries_request_error(self) -> None:
        calls = []

        def fake_fetch(url: str) -> HttpResponse:
            calls.append(url)
            if len(calls) == 1:
                return HttpResponse(status=0, body="", final_url=url, error="transient")
            return HttpResponse(status=200, body='{"code":"ONE","view_count":77}', final_url=url)

        results = probe_lookup_rows(
            [{"idx": "1", "url": "https://www.threads.com/@fixture/post/ONE"}],
            fetch_func=fake_fetch,
            fetch_attempts=2,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        self.assertEqual(len(calls), 2)
        self.assertEqual(results[0]["view_lookup_status"], "resolved")
        self.assertEqual(results[0]["view_counts_value"], 77)

    def test_probe_rows_retries_missing_count(self) -> None:
        calls = []

        def fake_fetch(url: str) -> HttpResponse:
            calls.append(url)
            if len(calls) == 1:
                return HttpResponse(
                    status=200,
                    body="<html>No count yet</html>",
                    final_url=url,
                    internal_fetch_statuses=(302,),
                )
            return HttpResponse(status=200, body='{"code":"ONE","view_count":77}', final_url=url)

        results = probe_lookup_rows(
            [{"idx": "1", "url": "https://www.threads.com/@fixture/post/ONE"}],
            fetch_func=fake_fetch,
            missing_count_attempts=2,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        self.assertEqual(len(calls), 2)
        self.assertEqual(results[0]["view_lookup_status"], "resolved")
        self.assertEqual(results[0]["view_counts_value"], 77)
        self.assertEqual(results[0]["internal_fetch_statuses"], "302")
        self.assertEqual(results[0]["redirect_observed"], "true")

    def test_probe_rows_classifies_account_unavailable_without_retry(self) -> None:
        calls = []

        def fake_fetch(url: str) -> HttpResponse:
            calls.append(url)
            return HttpResponse(
                status=200,
                body="<html><body>This profile isn't available</body></html>",
                final_url=url,
            )

        results = probe_lookup_rows(
            [{"idx": "1", "url": "https://www.threads.com/@fixture/post/ONE"}],
            fetch_func=fake_fetch,
            missing_count_attempts=3,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(results[0]["view_lookup_status"], "unavailable")
        self.assertEqual(results[0]["error"], "account_unavailable")

    def test_probe_rows_classifies_post_unavailable_without_retry(self) -> None:
        calls = []

        def fake_fetch(url: str) -> HttpResponse:
            calls.append(url)
            return HttpResponse(
                status=200,
                body="<html><body>This content isn't available</body></html>",
                final_url=url,
            )

        results = probe_lookup_rows(
            [{"idx": "1", "url": "https://www.threads.com/@fixture/post/ONE"}],
            fetch_func=fake_fetch,
            missing_count_attempts=3,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(results[0]["view_lookup_status"], "unavailable")
        self.assertEqual(results[0]["error"], "post_unavailable")

    def test_probe_rows_marks_missing_count_after_retries(self) -> None:
        calls = []

        def fake_fetch(url: str) -> HttpResponse:
            calls.append(url)
            return HttpResponse(status=200, body="<html>No count</html>", final_url=url)

        results = probe_lookup_rows(
            [{"idx": "1", "url": "https://www.threads.com/@fixture/post/ONE"}],
            fetch_func=fake_fetch,
            missing_count_attempts=2,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        self.assertEqual(len(calls), 2)
        self.assertEqual(results[0]["view_lookup_status"], "unavailable")
        self.assertEqual(results[0]["error"], "view_count_not_found_after_retries")

    def test_probe_rows_logs_sanitized_diagnostics(self) -> None:
        calls = []
        stream = io.StringIO()

        def fake_fetch(url: str) -> HttpResponse:
            calls.append(url)
            if len(calls) == 1:
                return HttpResponse(status=200, body="<html>No count yet</html>", final_url=url)
            return HttpResponse(status=200, body='{"code":"ONE","view_count":77}', final_url=url)

        results = probe_lookup_rows(
            [{"idx": "1", "url": "https://www.threads.com/@fixture/post/ONE"}],
            fetch_func=fake_fetch,
            missing_count_attempts=2,
            diagnostic_log=True,
            progress_stream=stream,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        log_value = stream.getvalue()
        self.assertEqual(results[0]["view_lookup_status"], "resolved")
        self.assertIn("idx=1", log_value)
        self.assertIn("status=resolved", log_value)
        self.assertIn("view_counts_value=77", log_value)
        self.assertIn("event=resolved_after_missing_count_retry", log_value)
        self.assertNotIn("https://", log_value)

    def test_probe_rows_preserves_network_diagnostics(self) -> None:
        stream = io.StringIO()

        def fake_fetch(url: str) -> HttpResponse:
            return HttpResponse(
                status=200,
                body='{"code":"ONE","view_count":77}',
                final_url=url,
                internal_fetch_statuses=(302, 302),
                fetch_warning_count=1,
                fetch_warning_codes=("ERR_HTTP_RESPONSE_CODE_FAILURE",),
            )

        results = probe_lookup_rows(
            [{"idx": "1", "url": "https://www.threads.com/@fixture/post/ONE"}],
            fetch_func=fake_fetch,
            diagnostic_log=True,
            progress_stream=stream,
            resolved_at="2026-05-08T00:00:00+00:00",
        )

        self.assertEqual(results[0]["view_lookup_status"], "resolved")
        self.assertEqual(results[0]["internal_fetch_statuses"], "302x2")
        self.assertEqual(results[0]["redirect_observed"], "true")
        self.assertEqual(results[0]["fetch_warning_count"], 1)
        self.assertEqual(results[0]["fetch_warning_codes"], "ERR_HTTP_RESPONSE_CODE_FAILURE")
        log_value = stream.getvalue()
        self.assertIn("internal_fetch_statuses=302x2", log_value)
        self.assertIn("redirect_observed=true", log_value)
        self.assertIn("fetch_warning_codes=ERR_HTTP_RESPONSE_CODE_FAILURE", log_value)
        self.assertNotIn("https://", log_value)

    def test_parse_fetch_diagnostics_from_scrapling_logs(self) -> None:
        records = [
            (
                20,
                "[2026-05-11 05:48:06] INFO: Fetched (302) "
                "<GET https://www.threads.com/@fixture/post/ONE> (referer: https://www.google.com/)",
            ),
            (
                30,
                "page.goto failed with ERR_HTTP_RESPONSE_CODE_FAILURE for "
                "https://www.threads.com/@fixture/post/ONE",
            ),
            (
                20,
                "[2026-05-11 05:48:07] INFO: Fetched (302) "
                "<GET https://www.threads.com/@fixture/post/ONE> (referer: https://www.google.com/)",
            ),
        ]

        statuses, warning_count, warning_codes = parse_fetch_diagnostics(records)

        self.assertEqual(statuses, (302, 302))
        self.assertEqual(summarize_internal_fetch_statuses(statuses), "302x2")
        self.assertEqual(warning_count, 1)
        self.assertEqual(warning_codes, ("ERR_HTTP_RESPONSE_CODE_FAILURE",))

    def test_unknown_probe_mode_fails_before_fetching(self) -> None:
        with self.assertRaises(ValueError):
            fetch_for_mode("unknown", 1.0, "ua")

    def test_merge_outputs_writes_loader_csv_and_retry_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            all_input = root / "all_input.csv"
            view_map = root / "view-lookup-map-260430.csv"
            previous_final = root / "previous_final.csv"
            shard_dir = root / "shards"
            output_dir = root / "merged"

            write_csv(
                all_input,
                ["idx", "url"],
                [
                    {"idx": "1", "url": "https://www.threads.com/@fixture/post/ONE"},
                    {"idx": "2", "url": "https://www.threads.com/@fixture/post/TWO"},
                    {"idx": "3", "url": "https://www.threads.com/@fixture/post/THREE"},
                    {"idx": "4", "url": "https://www.threads.com/@fixture/post/FOUR"},
                ],
            )
            write_csv(
                view_map,
                [
                    "idx",
                    "lookup_id",
                    "export_key",
                    "match_id",
                    "role",
                    "target_view_field",
                    "threads_post_key",
                ],
                [
                    {
                        "idx": "1",
                        "lookup_id": "m1:body",
                        "export_key": "260430",
                        "match_id": "m1",
                        "role": "body",
                        "target_view_field": "body_view_count",
                        "threads_post_key": "fixture/ONE",
                    },
                    {
                        "idx": "2",
                        "lookup_id": "m1:link",
                        "export_key": "260430",
                        "match_id": "m1",
                        "role": "link",
                        "target_view_field": "link_view_count",
                        "threads_post_key": "fixture/TWO",
                    },
                    {
                        "idx": "3",
                        "lookup_id": "m2:body",
                        "export_key": "260430",
                        "match_id": "m2",
                        "role": "body",
                        "target_view_field": "body_view_count",
                        "threads_post_key": "fixture/THREE",
                    },
                    {
                        "idx": "4",
                        "lookup_id": "m2:link",
                        "export_key": "260430",
                        "match_id": "m2",
                        "role": "link",
                        "target_view_field": "link_view_count",
                        "threads_post_key": "fixture/FOUR",
                    },
                ],
            )
            write_csv(
                previous_final,
                VIEW_LOOKUP_STATE_FIELDS,
                [
                    {
                        "idx": "1",
                        "url": "https://www.threads.com/@fixture/post/ONE",
                        "view_counts_value": "100",
                        "view_lookup_status": "resolved",
                        "http_status": "200",
                        "error": "",
                        "resolved_at": "2026-05-08T00:00:00+00:00",
                    }
                ],
            )
            write_csv(
                shard_dir / "shard-1.csv",
                VIEW_LOOKUP_STATE_FIELDS,
                [
                    {
                        "idx": "2",
                        "url": "https://www.threads.com/@fixture/post/TWO",
                        "view_counts_value": "",
                        "view_lookup_status": "unavailable",
                        "http_status": "200",
                        "error": "view_count_not_found",
                        "internal_fetch_statuses": "302x2",
                        "redirect_observed": "true",
                        "fetch_warning_count": "0",
                        "fetch_warning_codes": "",
                        "resolved_at": "2026-05-08T00:00:01+00:00",
                    },
                    {
                        "idx": "3",
                        "url": "https://www.threads.com/@fixture/post/THREE",
                        "view_counts_value": "",
                        "view_lookup_status": "rate_limited",
                        "http_status": "429",
                        "error": "rate_limited",
                        "internal_fetch_statuses": "429",
                        "redirect_observed": "false",
                        "fetch_warning_count": "1",
                        "fetch_warning_codes": "ERR_HTTP_RESPONSE_CODE_FAILURE",
                        "resolved_at": "2026-05-08T00:00:02+00:00",
                    },
                    {
                        "idx": "4",
                        "url": "https://www.threads.com/@fixture/post/FOUR",
                        "view_counts_value": "",
                        "view_lookup_status": "request_error",
                        "http_status": "0",
                        "error": "scrapling_fetch_error:Error",
                        "resolved_at": "2026-05-08T00:00:03+00:00",
                    },
                ],
            )

            manifest = merge_view_lookup_outputs(
                export_key="260430",
                all_input_path=all_input,
                map_path=view_map,
                output_dir=output_dir,
                previous_final_path=previous_final,
                shard_result_paths=[shard_dir / "shard-1.csv"],
            )

            self.assertEqual(manifest["total_rows"], 4)
            self.assertEqual(manifest["resolved_rows"], 1)
            self.assertEqual(manifest["retry_rows"], 2)
            self.assertEqual(
                manifest["network_diagnostics"],
                {
                    "rows_with_internal_fetch_statuses": 2,
                    "redirect_observed_rows": 1,
                    "fetch_warning_rows": 1,
                    "internal_fetch_status_counts": {"302": 2, "429": 1},
                },
            )

            result_rows = read_rows(output_dir / "view-lookup-results-260430.csv")
            self.assertEqual(list(result_rows[0].keys()), list(VIEW_LOOKUP_RESULT_FIELDS))
            self.assertEqual(result_rows[0]["lookup_id"], "m1:body")
            self.assertEqual(result_rows[1]["view_lookup_status"], "unavailable")
            self.assertEqual(result_rows[1]["internal_fetch_statuses"], "302x2")
            self.assertEqual(result_rows[1]["redirect_observed"], "true")

            view_count_rows = read_rows(output_dir / "threads-viewcount-260430.csv")
            self.assertEqual(
                [row["view_counts_value"] for row in view_count_rows],
                ["100", "", "", ""],
            )

            final_rows = read_rows(output_dir / "retry-state" / "final.csv")
            retry_rows = read_rows(output_dir / "retry-state" / "rate_limited_and_unexecuted.csv")
            self.assertEqual([row["idx"] for row in final_rows], ["1", "2"])
            self.assertEqual([row["idx"] for row in retry_rows], ["3", "4"])


if __name__ == "__main__":
    unittest.main()
