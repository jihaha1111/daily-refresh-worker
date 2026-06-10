import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read_rows(path: Path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


class PrepareLookupInputsCliTests(unittest.TestCase):
    def test_cli_writes_full_af_link_lookup_from_item_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "prepare_lookup_inputs.py"),
                    "--date",
                    "260430",
                    "--input",
                    str(ROOT / "tests/fixtures/minimal_threads_export.json"),
                    "--output-dir",
                    str(output_dir),
                    "--body-like-threshold",
                    "4",
                ],
                cwd=ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            self.assertIn('"af_link_lookup_rows": 3', completed.stdout)
            af_rows = read_rows(output_dir / "af-link-lookup-input-260430.csv")
            unique_rows = read_rows(output_dir / "af-link-lookup-unique-urls-260430.csv")
            self.assertEqual(len(af_rows), 3)
            self.assertEqual(len(unique_rows), 2)
            self.assertEqual({row["item_pk"] for row in af_rows}, {"1002", "1003"})


if __name__ == "__main__":
    unittest.main()
