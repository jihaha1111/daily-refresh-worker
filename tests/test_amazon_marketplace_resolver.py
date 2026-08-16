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
CLI = ROOT / "resolve_amazon_marketplace_lookup.py"
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.amazon_marketplace_resolver import (  # noqa: E402
    AMAZON_MARKETPLACE_MANIFEST_VERSION,
    AMAZON_MARKETPLACE_QUEUE_FIELDS,
    AMAZON_MARKETPLACE_QUEUE_VERSION,
    AMAZON_MARKETPLACE_RESULT_FIELDS,
    AMAZON_MARKETPLACE_RESULT_VERSION,
    AmazonMarketplaceResolverError,
    HttpHop,
    amazon_lookup_url_id,
    classify_amazon_destination,
    normalize_amazon_short_url,
    read_amazon_marketplace_lookup_input,
    resolve_amazon_marketplace_rows,
    resolve_amazon_short_url,
    validate_approved_runner_environment,
    validate_lookup_rows,
    validate_public_report,
    write_amazon_marketplace_outputs,
)


def queue_row(url: str) -> dict:
    normalized = normalize_amazon_short_url(url)
    assert normalized is not None
    return {
        "contract_version": AMAZON_MARKETPLACE_QUEUE_VERSION,
        "lookup_url_id": amazon_lookup_url_id(normalized[0]),
        "short_url": normalized[0],
        "short_host": normalized[1],
        "evidence_count": 1,
    }


class AmazonMarketplaceResolverTests(unittest.TestCase):
    def test_network_cli_environment_is_public_runner_only(self) -> None:
        approved = {
            "GITHUB_ACTIONS": "true",
            "RUNNER_ENVIRONMENT": "github-hosted",
            "GITHUB_REPOSITORY": "jihaha1111/daily-refresh-worker",
        }
        validate_approved_runner_environment(approved)
        for changed in (
            {**approved, "GITHUB_ACTIONS": "false"},
            {**approved, "RUNNER_ENVIRONMENT": "self-hosted"},
            {**approved, "GITHUB_REPOSITORY": "private/repository"},
        ):
            with self.subTest(changed=changed):
                with self.assertRaisesRegex(
                    AmazonMarketplaceResolverError, "restricted|requires"
                ):
                    validate_approved_runner_environment(changed)

        denied_input = "/tmp/private-amazon-marketplace-queue.csv"
        completed = subprocess.run(
            [
                sys.executable,
                str(CLI),
                "--run-key",
                "local-denied",
                "--input",
                denied_input,
                "--output-dir",
                "/tmp/private-amazon-marketplace-output",
            ],
            cwd=ROOT,
            env={**os.environ, "GITHUB_ACTIONS": "false"},
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("approved GitHub Actions runner", completed.stderr)
        self.assertNotIn(denied_input, completed.stderr)

    def test_short_url_rule_is_exact_https_and_spoof_safe(self) -> None:
        self.assertEqual(
            normalize_amazon_short_url("AMZN.TO/AbC123?x=1#fragment"),
            ("https://amzn.to/AbC123?x=1", "amzn.to"),
        )
        self.assertEqual(
            normalize_amazon_short_url("https://link.amazon/example"),
            ("https://link.amazon/example", "link.amazon"),
        )
        for value in (
            "http://amzn.to/path",
            "https://amzn.to/",
            "https://www.amzn.to/path",
            "https://amzn.to.evil.example/path",
            "https://amzn.to@evil.example/path",
            "https://amzn.to:8443/path",
            "https://amzn.to/path with-space",
            "ftp://link.amazon/path",
        ):
            with self.subTest(value=value):
                self.assertIsNone(normalize_amazon_short_url(value))

    def test_destination_allowlist_marketplace_and_affiliate_rules(self) -> None:
        jp = classify_amazon_destination(
            "https://www.amazon.co.jp/dp/B000?tag=synthetic-jp-22&ref_=private#frag"
        )
        self.assertIsNotNone(jp)
        assert jp is not None
        self.assertEqual(jp.marketplace, "amazon_jp")
        self.assertEqual(jp.locale, "ja-JP")
        self.assertEqual(jp.affiliate_id, "synthetic-jp-22")
        self.assertEqual(jp.affiliate_status, "resolved")
        self.assertEqual(jp.normalized_destination_url, "https://www.amazon.co.jp/dp/B000")

        no_tag = classify_amazon_destination("https://amazon.co.jp/dp/B001?ref_=private")
        self.assertIsNotNone(no_tag)
        assert no_tag is not None
        self.assertEqual(no_tag.affiliate_status, "missing")
        self.assertEqual(no_tag.affiliate_id, "")

        us = classify_amazon_destination("https://smile.amazon.com/dp/B002?tag=us-20")
        self.assertIsNotNone(us)
        assert us is not None
        self.assertEqual((us.marketplace, us.locale), ("amazon_us", "en-US"))

        other = classify_amazon_destination("https://www.amazon.de/dp/B003?tag=de-21")
        self.assertIsNotNone(other)
        assert other is not None
        self.assertEqual((other.marketplace, other.locale), ("amazon_de", ""))

        for value in (
            "http://www.amazon.co.jp/dp/B000?tag=leak-22",
            "https://amazon.co.jp.evil.example/dp/B000?tag=leak-22",
            "https://amazon.co.jp@evil.example/dp/B000?tag=leak-22",
            "https://www.amazon.co.jp:8443/dp/B000?tag=leak-22",
            "https://amazon.jp/dp/B000?tag=leak-22",
        ):
            with self.subTest(value=value):
                self.assertIsNone(classify_amazon_destination(value))

        ambiguous = classify_amazon_destination(
            "https://www.amazon.co.jp/dp/B?tag=one-22&tag=two-22"
        )
        self.assertIsNotNone(ambiguous)
        assert ambiguous is not None
        self.assertEqual(ambiguous.affiliate_status, "invalid")
        self.assertEqual(ambiguous.affiliate_id, "")
        self.assertEqual(ambiguous.error_code, "ambiguous_affiliate_tag")

    def test_resolution_matrix_separates_marketplace_and_affiliate_evidence(self) -> None:
        rows = [
            queue_row("https://amzn.to/jp-tag"),
            queue_row("https://link.amazon/jp-no-tag"),
            queue_row("https://amzn.to/us"),
            queue_row("https://amzn.to/other"),
            queue_row("https://amzn.to/disallowed"),
            queue_row("https://amzn.to/unresolved"),
        ]
        destinations = {
            "https://amzn.to/jp-tag": HttpHop(
                302,
                "https://www.amazon.co.jp/dp/B000?tag=synthetic-jp-22&ref_=private",
            ),
            "https://link.amazon/jp-no-tag": HttpHop(
                302, "https://amazon.co.jp/dp/B001?ref_=private"
            ),
            "https://amzn.to/us": HttpHop(
                301, "https://www.amazon.com/dp/B002?tag=synthetic-us-20"
            ),
            "https://amzn.to/other": HttpHop(
                302, "https://www.amazon.de/dp/B003?tag=synthetic-de-21"
            ),
            "https://amzn.to/disallowed": HttpHop(
                302, "https://amazon.co.jp.evil.example/?tag=must-not-leak"
            ),
            "https://amzn.to/unresolved": HttpHop(200),
        }
        calls = []

        def fake_request(url: str) -> HttpHop:
            calls.append(url)
            return destinations[url]

        result = resolve_amazon_marketplace_rows(
            rows,
            run_key="260813-amazon-jp",
            request_func=fake_request,
            resolved_at="2026-08-17T00:00:00+00:00",
            max_workers=3,
            progress_interval=0,
        )

        self.assertEqual(len(calls), 6)
        self.assertEqual(
            [row["resolution_status"] for row in result.rows],
            [
                "amazon_affiliate_id_resolved",
                "amazon_marketplace_confirmed",
                "amazon_affiliate_id_resolved",
                "amazon_affiliate_id_resolved",
                "disallowed_destination",
                "unresolved",
            ],
        )
        self.assertEqual(result.rows[0]["affiliate_id"], "synthetic-jp-22")
        self.assertEqual(result.rows[1]["affiliate_id"], "")
        self.assertEqual(result.rows[1]["marketplace"], "amazon_jp")
        self.assertEqual(result.rows[2]["marketplace"], "amazon_us")
        self.assertEqual(result.rows[3]["marketplace"], "amazon_de")
        self.assertNotIn("?", result.rows[0]["normalized_destination_url"])
        self.assertEqual(result.rows[4]["resolved_host"], "")

        report = result.public_report()
        self.assertEqual(report["counts"]["marketplace_confirmed_rows"], 4)
        self.assertEqual(report["counts"]["japan_marketplace_rows"], 2)
        self.assertEqual(report["counts"]["united_states_marketplace_rows"], 1)
        self.assertEqual(report["counts"]["other_approved_marketplace_rows"], 1)
        self.assertEqual(report["counts"]["affiliate_identity_resolved_rows"], 3)
        serialized = json.dumps(report, sort_keys=True)
        for private_value in (
            "260813-amazon-jp",
            "synthetic-jp-22",
            rows[0]["lookup_url_id"],
            rows[0]["short_url"],
            "www.amazon.co.jp",
            "/dp/B000",
        ):
            self.assertNotIn(private_value, serialized)

    def test_retry_redirect_limit_and_disallowed_hop_are_bounded(self) -> None:
        attempts = []

        def retry_request(url: str) -> HttpHop:
            attempts.append(url)
            if len(attempts) == 1:
                return HttpHop(0, error_code="network_error")
            return HttpHop(302, "https://www.amazon.co.jp/dp/B?tag=retry-22")

        resolved = resolve_amazon_short_url(
            "https://amzn.to/retry",
            request_func=retry_request,
            fetch_attempts=2,
        )
        self.assertEqual(resolved.resolution_status, "amazon_affiliate_id_resolved")
        self.assertEqual(resolved.request_attempt_count, 2)

        chained_calls = []

        def chained_request(url: str) -> HttpHop:
            chained_calls.append(url)
            return HttpHop(302, "https://link.amazon/second")

        limited = resolve_amazon_short_url(
            "https://amzn.to/first",
            request_func=chained_request,
            max_redirects=1,
        )
        self.assertEqual(limited.resolution_status, "redirect_limit_exceeded")
        self.assertEqual(len(chained_calls), 1)

        forbidden_calls = []

        def forbidden_request(url: str) -> HttpHop:
            forbidden_calls.append(url)
            return HttpHop(302, "https://private.example/path")

        disallowed = resolve_amazon_short_url(
            "https://amzn.to/no-ssrf",
            request_func=forbidden_request,
        )
        self.assertEqual(disallowed.resolution_status, "disallowed_destination")
        self.assertEqual(forbidden_calls, ["https://amzn.to/no-ssrf"])

    def test_http_network_and_invalid_input_are_row_isolated(self) -> None:
        calls = 0

        def network_request(url: str) -> HttpHop:
            nonlocal calls
            calls += 1
            return HttpHop(0, error_code="network_error")

        network = resolve_amazon_short_url(
            "https://amzn.to/network",
            request_func=network_request,
            fetch_attempts=3,
        )
        self.assertEqual(network.resolution_status, "network_error")
        self.assertEqual(network.request_attempt_count, 3)
        self.assertEqual(calls, 3)

        http = resolve_amazon_short_url(
            "https://amzn.to/http",
            request_func=lambda url: HttpHop(404, error_code="http_404"),
        )
        self.assertEqual(http.resolution_status, "http_error")
        self.assertEqual(http.http_status, 404)

        invalid_calls = []
        invalid = resolve_amazon_short_url(
            "https://evil.example/amzn.to/path",
            request_func=lambda url: invalid_calls.append(url),
        )
        self.assertEqual(invalid.resolution_status, "invalid_input")
        self.assertEqual(invalid_calls, [])

    def test_queue_contract_and_output_files_are_strict_and_private(self) -> None:
        row = queue_row("https://amzn.to/output")
        self.assertEqual(validate_lookup_rows([row]), [row])
        duplicate = dict(row)
        with self.assertRaisesRegex(AmazonMarketplaceResolverError, "duplicate"):
            validate_lookup_rows([row, duplicate])
        inconsistent = dict(row)
        inconsistent["lookup_url_id"] = "private-wrong-id"
        with self.assertRaisesRegex(AmazonMarketplaceResolverError, "inconsistent"):
            validate_lookup_rows([inconsistent])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            queue_path = root / "queue.csv"
            with queue_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=AMAZON_MARKETPLACE_QUEUE_FIELDS)
                writer.writeheader()
                writer.writerow(row)
            self.assertEqual(read_amazon_marketplace_lookup_input(queue_path), [row])

            result = resolve_amazon_marketplace_rows(
                [row],
                run_key="fixture-run",
                request_func=lambda url: HttpHop(
                    302, "https://www.amazon.co.jp/dp/B?tag=output-22"
                ),
                resolved_at="2026-08-17T00:00:00+00:00",
                progress_interval=0,
            )
            written = write_amazon_marketplace_outputs(
                result,
                root / "resolved",
                repo_root=ROOT,
                source_input=queue_path,
                max_redirects=5,
                timeout_seconds=10,
                fetch_attempts=2,
                retry_sleep_seconds=0,
                sleep_seconds=0,
                max_workers=1,
                github_run_id="synthetic-run",
                github_sha="synthetic-sha",
            )
            result_path = written["results_csv"]
            manifest_path = written["manifest_json"]
            self.assertEqual(stat.S_IMODE(result_path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(manifest_path.stat().st_mode), 0o600)
            with result_path.open(encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                self.assertEqual(tuple(reader.fieldnames or ()), AMAZON_MARKETPLACE_RESULT_FIELDS)
                resolved_row = next(reader)
            self.assertEqual(resolved_row["contract_version"], AMAZON_MARKETPLACE_RESULT_VERSION)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["contract_version"], AMAZON_MARKETPLACE_MANIFEST_VERSION)
            self.assertEqual(manifest["source_input"], "queue.csv")

    def test_public_report_validator_rejects_private_structure(self) -> None:
        row = queue_row("https://amzn.to/private")
        short_key_result = resolve_amazon_marketplace_rows(
            [row],
            run_key="a",
            request_func=lambda url: HttpHop(200),
            resolved_at="2026-08-17T00:00:00+00:00",
            progress_interval=0,
        )
        self.assertEqual(short_key_result.public_report()["counts"]["input_rows"], 1)
        report = {
            "contract_version": "safe",
            "lookup_url_id": row["lookup_url_id"],
        }
        with self.assertRaisesRegex(AmazonMarketplaceResolverError, "private field"):
            validate_public_report(report, [row], [], "private-run")


if __name__ == "__main__":
    unittest.main()
