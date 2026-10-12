import importlib
import importlib.machinery
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sysconfig
import tempfile
import unittest

import delta_arrow_reader
import pyarrow as pa


class PackageTests(unittest.TestCase):
    def test_installed_package(self):
        distribution = importlib.metadata.distribution("delta-arrow-reader")
        self.assertEqual(distribution.metadata["Name"], "delta-arrow-reader")
        self.assertEqual(delta_arrow_reader.__version__, distribution.version)
        self.assertEqual(distribution.metadata["Requires-Python"], ">=3.10")
        self.assertIn("pyarrow>=18.0.0", distribution.requires)
        self.assertEqual(distribution.metadata["License-Expression"], "Apache-2.0")

        files = {str(path): path for path in distribution.files}
        for name in (
            "delta_arrow_reader/__init__.pyi",
            "delta_arrow_reader/py.typed",
        ):
            self.assertIn(name, files)
            self.assertTrue(distribution.locate_file(files[name]).is_file())
        for name in ("LICENSE", "NOTICE"):
            self.assertTrue(any(path.name == name for path in files.values()))

        native = importlib.import_module("delta_arrow_reader.delta_arrow_reader")
        self.assertTrue(
            any(
                native.__file__.endswith(suffix)
                for suffix in importlib.machinery.EXTENSION_SUFFIXES
            )
        )

    def test_installed_cli_reads_external_writer_fixture(self):
        name = "delta-arrow-reader.exe" if os.name == "nt" else "delta-arrow-reader"
        executable = Path(sysconfig.get_path("scripts")) / name
        fixture = (Path(__file__).resolve().parents[3]
                   / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned")
        with pa.ipc.open_file(fixture / "expected.arrow") as reader:
            expected = reader.read_all()
        expected_rows = {row["id"]: row["label"] for row in expected.to_pylist()}

        with tempfile.TemporaryDirectory(prefix="installed CLI ") as directory:
            def run(*arguments):
                result = subprocess.run(
                    [str(executable), *map(str, arguments)],
                    cwd=directory, capture_output=True, timeout=30,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stderr, b"")
                return result.stdout

            self.assertEqual(
                run("--version").decode(),
                f"delta-arrow-reader {delta_arrow_reader.__version__}\n",
            )
            inspection = json.loads(run("inspect", fixture / "table"))
            self.assertEqual(inspection["table_version"], "0")
            self.assertEqual(
                [field["name"] for field in inspection["schema"]["fields"]],
                expected.schema.names,
            )
            stream = run("scan", fixture / "table", "--column", "label",
                         "--column", "id", "--limit", "5")
            with pa.ipc.open_stream(stream) as reader:
                table = reader.read_all()
            self.assertEqual(table.schema, pa.schema([
                expected.schema.field("label"), expected.schema.field("id"),
            ]))
            self.assertEqual(table.num_rows, 5)
            self.assertEqual(len(set(table.column("id").to_pylist())), 5)
            for row in table.to_pylist():
                self.assertEqual(row["label"], expected_rows[row["id"]])

        notices = Path(sysconfig.get_path("data")) / "share/delta-arrow-reader"
        for package, dependency in (("python", "pyo3"), ("cli", "clap")):
            text = (notices / f"{package}-THIRD_PARTY_NOTICES.txt").read_text(encoding="utf-8")
            self.assertIn(dependency, text)
            self.assertIn("Rust standard library", text)


if __name__ == "__main__":
    unittest.main()
