import importlib
import importlib.machinery
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
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

        notices = Path(sysconfig.get_path("data")) / "share/delta-arrow-reader"
        for package, dependency in (("python", "pyo3"), ("cli", "clap")):
            text = (notices / f"{package}-THIRD_PARTY_NOTICES.txt").read_text(encoding="utf-8")
            self.assertIn(dependency, text)
            self.assertIn("Rust standard library", text)
            if package == "python":
                self.assertIn("datafusion", text)


class InstalledCliTests(unittest.TestCase):
    def setUp(self):
        name = "delta-arrow-reader.exe" if os.name == "nt" else "delta-arrow-reader"
        self.executable = Path(sysconfig.get_path("scripts")) / name
        self.assertTrue(self.executable.is_file(), self.executable)
        self.fixture = (Path(__file__).resolve().parents[3]
                        / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned")
        temporary = tempfile.TemporaryDirectory(prefix="secret installed CLI ")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.environment = {
            key: value for key, value in os.environ.items()
            if not key.startswith(("LD_", "DYLD_")) and key != "RUST_LOG"
        }
        if os.name == "nt":
            windows = Path(os.environ["SystemRoot"])
            # CPython supplies the supported MSVC runtime, without build-tool DLLs.
            paths = (windows / "System32", windows, Path(sys.base_prefix))
            self.environment["PATH"] = os.pathsep.join(map(str, paths))
        else:
            self.environment["PATH"] = "/usr/bin:/bin"

    def run_cli(self, *arguments):
        return subprocess.run(
            [str(self.executable), *map(str, arguments)],
            cwd=self.directory, env=self.environment, stdin=subprocess.DEVNULL,
            capture_output=True, timeout=30,
        )

    def successful_output(self, *arguments):
        result = self.run_cli(*arguments)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, b"")
        return result.stdout

    def test_installed_cli_process_contract(self):
        root = Path(__file__).resolve().parents[3]
        target = Path(os.environ.get("CARGO_TARGET_DIR", root / "target"))
        name = "xtask.exe" if os.name == "nt" else "xtask"
        native = os.environ.get("DELTA_ARROW_READER_XTASK", target / "debug" / name)
        subprocess.run([
            str(native), "check-installed-python-cli", str(self.executable),
            "--expected-version", delta_arrow_reader.__version__,
            "--fixture", str(self.fixture), "--python-base-prefix", sys.base_prefix,
        ], check=True, timeout=180)

    def test_installed_cli_reads_external_writer_fixture(self):
        with pa.ipc.open_file(self.fixture / "expected.arrow") as reader:
            expected = reader.read_all()
        expected_rows = {row["id"]: row["label"] for row in expected.to_pylist()}
        inspection = json.loads(self.successful_output("inspect", self.fixture / "table"))
        self.assertEqual(inspection["format_version"], 1)
        self.assertEqual(inspection["table_version"], "0")
        self.assertEqual(
            [field["name"] for field in inspection["schema"]["fields"]],
            expected.schema.names,
        )
        for limit in (0, 5, expected.num_rows + 1):
            with self.subTest(limit=limit):
                stream = self.successful_output(
                    "scan", self.fixture / "table", "--column", "label",
                    "--column", "id", "--limit", limit,
                )
                # Arrow readers also accept EOF without an end marker.
                self.assertTrue(stream.endswith(b"\xff\xff\xff\xff\x00\x00\x00\x00"))
                source = pa.BufferReader(stream)
                with pa.ipc.open_stream(source) as reader:
                    table = reader.read_all()
                self.assertEqual(source.tell(), len(stream))
                self.assertEqual(table.schema, pa.schema([
                    expected.schema.field("label"), expected.schema.field("id"),
                ]))
                self.assertEqual(table.num_rows, min(limit, expected.num_rows))
                self.assertEqual(len(set(table.column("id").to_pylist())), table.num_rows)
                for row in table.to_pylist():
                    self.assertEqual(row["label"], expected_rows[row["id"]])



if __name__ == "__main__":
    unittest.main()
