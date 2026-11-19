"""Check benchmark preparation flags without launching or changing Spark."""

import subprocess
import tempfile
import unittest
from pathlib import Path

MAKEFILE = Path(__file__).resolve().parents[1] / "Makefile"


class BenchmarkPreparationTest(unittest.TestCase):
    def command(self, completed):
        with tempfile.TemporaryDirectory() as directory:
            for name in completed:
                path = Path(directory) / "data" / name
                path.mkdir(parents=True)
                (path / "_SUCCESS").touch()
            return subprocess.run(
                ["make", "-n", "-f", str(MAKEFILE), "run-deep-part"],
                cwd=directory,
                capture_output=True,
                text=True,
                check=True,
            ).stdout

    def test_fresh_benchmark_prepares_both_layouts(self):
        command = self.command([])
        self.assertIn("--prepare-csv", command)
        self.assertIn("--prepare-partitioned", command)

    def test_pipeline_layout_is_reused(self):
        command = self.command(["cdnnba_by_season.parquet"])
        self.assertIn("--prepare-csv", command)
        self.assertNotIn("--prepare-partitioned", command)

    def test_completed_benchmark_is_reusable(self):
        command = self.command(["cdnnba_by_season.parquet", "cdnnba_csv"])
        self.assertNotIn("--prepare-csv", command)
        self.assertNotIn("--prepare-partitioned", command)


if __name__ == "__main__":
    unittest.main()
