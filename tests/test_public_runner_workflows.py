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
                "resolve-link-lookup.yml",
                "resolve-view-metrics.yml",
                "refresh-media-cache.yml",
            },
        )
        for name in [
            "prepare-lookup.yml",
            "resolve-link-lookup.yml",
            "resolve-view-metrics.yml",
            "refresh-media-cache.yml",
        ]:
            text = (WORKFLOWS / name).read_text(encoding="utf-8")
            self.assertIn("RCLONE_CONFIG_GDRIVE", text)
            self.assertIn("vars.", text)

    def test_view_metrics_is_drive_only(self) -> None:
        text = (WORKFLOWS / "resolve-view-metrics.yml").read_text(encoding="utf-8")
        self.assertIn("view-shards/$GITHUB_RUN_ID", text)
        self.assertIn("view-shard-results/$GITHUB_RUN_ID", text)
        self.assertIn("view-retry-state/$GITHUB_RUN_ID", text)
        self.assertIn("rclone copy", text)


if __name__ == "__main__":
    unittest.main()
