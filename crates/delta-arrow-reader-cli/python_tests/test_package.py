import importlib.metadata
import importlib.util
import os
import subprocess
import sys
import sysconfig
import tempfile
import unittest
from pathlib import Path


class CliPackageTests(unittest.TestCase):
    def test_installed_package_has_no_python_api_dependencies(self):
        distribution = importlib.metadata.distribution("delta-arrow-reader-cli")
        self.assertFalse(distribution.requires)
        self.assertIsNone(importlib.util.find_spec("pyarrow"))
        self.assertIsNone(importlib.util.find_spec("delta_arrow_reader"))
        self.assertEqual(distribution.metadata["License-Expression"], "Apache-2.0")
        notices = (
            Path(sysconfig.get_path("data"))
            / "share/delta-arrow-reader/cli-THIRD_PARTY_NOTICES.txt"
        )
        text = notices.read_text(encoding="utf-8")
        for dependency in ("clap", "Rust standard library", "datafusion"):
            self.assertIn(dependency, text)

    def test_installed_cli(self):
        root = Path(__file__).resolve().parents[3]
        fixture = (
            root
            / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned"
        )
        name = "delta-arrow-reader.exe" if os.name == "nt" else "delta-arrow-reader"
        executable = Path(sysconfig.get_path("scripts")) / name
        target = Path(os.environ.get("CARGO_TARGET_DIR", root / "target"))
        name = "xtask.exe" if os.name == "nt" else "xtask"
        native = os.environ.get("DELTA_ARROW_READER_XTASK", target / "debug" / name)
        temporary = tempfile.TemporaryDirectory(prefix="secret installed CLI ")
        self.addCleanup(temporary.cleanup)
        output = Path(temporary.name) / "output"
        subprocess.run(
            [
                str(native),
                "check-installed-python-cli",
                str(executable),
                "--expected-version",
                importlib.metadata.version("delta-arrow-reader-cli"),
                "--fixture",
                str(fixture),
                "--python-base-prefix",
                sys.base_prefix,
                "--output-directory",
                str(output),
            ],
            check=True,
            timeout=180,
        )


if __name__ == "__main__":
    unittest.main()
