"""Run with python3 -m unittest discover -s benches -p test_scan_scheduling.py."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


RUNNER = Path(__file__).with_name("scan_scheduling.py")


class FixtureGeneratorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.output_index = 0
        # Both versions can read the same fixtures, but only the newer version
        # knows how to generate them. Use arbitrary labels, not baseline/candidate.
        for label in ("old", "new"):
            binary = self.root / label
            binary.write_text(
                f"#!{sys.executable}\n"
                "import json, sys\n"
                "from pathlib import Path\n"
                "command, root = sys.argv[1:3]\n"
                "root = Path(root)\n"
                "if command == 'prepare':\n"
                f"    if {label == 'old'}: sys.exit('unknown fixture shape')\n"
                "    root.mkdir()\n"
                "    fixture = {'shape': sys.argv[3], 'writer': 'new'}\n"
                "    (root / 'fixture.json').write_text(json.dumps(fixture))\n"
                "    print(json.dumps(fixture))\n"
                "elif command == 'run':\n"
                "    fixture = json.loads((root / 'fixture.json').read_text())\n"
                "    print(json.dumps(dict(fixture, status='ok', elapsed_us=100)))\n"
                "else:\n"
                "    sys.exit('unknown command')\n"
            )
            binary.chmod(0o755)

    def run_cli(self, labels, *extra):
        self.output_index += 1
        output = self.root / f"results-{self.output_index}"
        command = [sys.executable, str(RUNNER), "--output-dir", str(output),
                   "--repetitions", "1", "--probe-repetitions", "1", "--suite", "performance",
                   "--case-filter", r"^(tiny|unequal-tiny)-local-.*-direct$"]
        for label in labels:
            command.extend(["--binary", f"{label}={self.root / label}"])
        result = subprocess.run([*command, *extra], capture_output=True, text=True, timeout=30)
        return result, output

    def assert_measurements(self, result, output, labels):
        self.assertEqual(result.returncode, 0, result.stderr)
        metadata = json.loads((output / "metadata.json").read_text())
        self.assertEqual(metadata.get("fixture_binary"), "new")
        self.assertEqual(set(metadata["fixtures"]), {"tiny", "unequal-tiny"})
        runs = [json.loads(line) for line in (output / "runs.jsonl").read_text().splitlines()]
        self.assertEqual(len(runs), 4 * len(labels))  # Two shapes, warmup and measured.
        self.assertEqual({run["binary"] for run in runs}, set(labels))
        self.assertTrue(all(run["status"] == "ok" and run["writer"] == "new" for run in runs))
        return {shape: fixture["sha256"] for shape, fixture in metadata["fixtures"].items()}

    def test_explicit_generator_does_not_depend_on_comparison_order(self):
        fixture_hashes = []
        for labels in (("old", "new"), ("new", "old")):
            result, output = self.run_cli(labels, "--fixture-binary", "new")
            fixture_hashes.append(self.assert_measurements(result, output, labels))
        self.assertEqual(fixture_hashes[0], fixture_hashes[1])

    def test_single_binary_is_the_default_generator(self):
        result, output = self.run_cli(("new",))
        self.assert_measurements(result, output, ("new",))

    def test_missing_or_unknown_generator_is_rejected_before_output_creation(self):
        for labels, extra in [
            (("old", "new"), ()),
            (("old", "new"), ("--fixture-binary", "missing")),
            (("new",), ("--fixture-binary", "missing")),
        ]:
            with self.subTest(labels=labels, extra=extra):
                result, output = self.run_cli(labels, *extra)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("--fixture-binary", result.stderr)
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
