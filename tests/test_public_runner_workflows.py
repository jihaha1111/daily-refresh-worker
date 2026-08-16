import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


class PublicRunnerWorkflowTests(unittest.TestCase):
    def test_operational_workflows_do_not_use_artifacts(self) -> None:
        for path in sorted(WORKFLOWS.glob("*.yml")):
            if path.name == "ci.yml":
                continue
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("actions/upload-artifact", text, path.name)
            self.assertNotIn("actions/download-artifact", text, path.name)
            self.assertNotIn("gh run download", text, path.name)

    def test_operational_workflows_use_repository_configuration(self) -> None:
        workflow_names = {path.name for path in WORKFLOWS.glob("*.yml")}
        self.assertEqual(
            workflow_names,
            {
                "ci.yml",
                "prepare-lookup.yml",
                "resolve-amazon-marketplace.yml",
                "resolve-link-lookup.yml",
                "resolve-view-metrics.yml",
                "refresh-media-cache.yml",
            },
        )
        for name in [
            "prepare-lookup.yml",
            "resolve-amazon-marketplace.yml",
            "resolve-link-lookup.yml",
            "resolve-view-metrics.yml",
            "refresh-media-cache.yml",
        ]:
            text = (WORKFLOWS / name).read_text(encoding="utf-8")
            self.assertIn("RCLONE_CONFIG_GDRIVE", text)
            self.assertIn("vars.", text)

    def test_amazon_marketplace_resolution_is_drive_only_and_sanitized(self) -> None:
        text = (WORKFLOWS / "resolve-amazon-marketplace.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("amazon-marketplace-lookup-input-${RUN_KEY}.csv", text)
        self.assertIn('$RUNNER_TEMP/amazon-marketplace-results', text)
        self.assertIn("RUNS_REMOTE_DIR", text)
        self.assertIn("row evidence: private Drive output only", text)
        self.assertIn("resolve_amazon_marketplace_lookup.py", text)
        self.assertIn('>"$log_file" 2>&1', text)
        self.assertNotIn("curl ", text)
        self.assertNotIn("RUNS_FOLDER_ID", text)

    def test_view_metrics_is_drive_only(self) -> None:
        text = (WORKFLOWS / "resolve-view-metrics.yml").read_text(encoding="utf-8")
        self.assertIn("view-shards/$GITHUB_RUN_ID", text)
        self.assertIn("view-shard-results/$GITHUB_RUN_ID", text)
        self.assertIn("view-retry-state/$GITHUB_RUN_ID", text)
        self.assertIn("rclone_retry rclone", text)
        self.assertIn(" copy ", text)
        self.assertIn(" copyto ", text)

    def test_view_metrics_preserves_date_scope_and_isolates_amazon_jp_scope(self) -> None:
        text = (WORKFLOWS / "resolve-view-metrics.yml").read_text(encoding="utf-8")
        self.assertIn('lookup_scope:', text)
        self.assertIn('default: "date"', text)
        self.assertIn('date)', text)
        self.assertIn('if ! [[ "$TARGET_DATE" =~ ^[0-9]{6}$ ]]', text)
        self.assertIn('${RUNS_REMOTE_DIR%/}/$TARGET_DATE', text)
        self.assertIn(
            '${RUNS_REMOTE_DIR%/}/amazon-jp/view-lookups/$TARGET_DATE', text
        )
        self.assertIn('amazon-jp-view-lookup-input-${TARGET_DATE}.csv', text)
        self.assertIn(
            'amazon-jp-view-lookup-runner-map-${TARGET_DATE}.csv', text
        )
        self.assertIn('--lookup-scope "$LOOKUP_SCOPE"', text)
        self.assertIn('group: resolve-view-metrics-${{ inputs.lookup_scope }}-${{ inputs.target_date }}', text)

    def test_view_metrics_guards_scrapling_import_and_total_request_failure(self) -> None:
        text = (WORKFLOWS / "resolve-view-metrics.yml").read_text(encoding="utf-8")
        self.assertIn('"apify-fingerprint-datapoints==0.13.0"', text)
        self.assertIn("from scrapling.fetchers import DynamicFetcher", text)
        self.assertIn("from scrapling.fetchers import StealthyFetcher", text)
        self.assertLess(
            text.index("copy view-results/retry-state"),
            text.index("all {total_rows} view metric rows ended as request_error"),
        )


if __name__ == "__main__":
    unittest.main()
