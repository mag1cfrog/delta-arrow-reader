from concurrent.futures import ThreadPoolExecutor
import importlib
import importlib.machinery
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import sysconfig
import tempfile
import unittest

import delta_arrow_reader
import pyarrow as pa
import pyarrow.parquet as pq


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

    def assert_diagnostic(self, stderr, phase, code):
        text = stderr.decode("utf-8")
        self.assertTrue(text.endswith("\n"), text)
        self.assertEqual(len(text.splitlines()), 1, text)
        self.assertTrue(text.startswith(("ERROR delta_arrow_reader: ",
                                         "ERROR delta_arrow_reader_cli: ")), text)
        fields = text.split()
        self.assertIn(f'error_phase="{phase}"', fields)
        self.assertIn(f'error_code="{code}"', fields)
        self.assertIn('outcome="failed"', fields)
        self.assertTrue(any(field.startswith('error_reason="') for field in fields), text)
        self.assertNotIn("\x1b", text)
        self.assertNotIn(b"secret", stderr)
        self.assertNotIn(b"Traceback", stderr)

    def test_installed_cli_help_and_version(self):
        self.assertEqual(
            self.successful_output("--version").decode(),
            f"delta-arrow-reader {delta_arrow_reader.__version__}\n",
        )
        for command in ((), ("inspect",), ("scan",)):
            with self.subTest(command=command):
                help_text = self.successful_output(*command, "--help").decode()
                usage = " ".join(("Usage: delta-arrow-reader", *command)) + " "
                self.assertIn(usage, help_text)
                self.assertNotIn("secret", help_text)

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

    def test_installed_cli_errors_are_redacted(self):
        malformed = self.directory / "secret-options.json"
        malformed.write_text('{"token": "secret-value",', encoding="utf-8")
        for command in ("inspect", "scan"):
            for arguments, status, phase, code in (
                ((self.fixture / "table", "--storage-options-file", malformed),
                 2, "configuration", "invalid_input_json"),
                ((self.directory / "secret-missing-table",),
                 1, "table_location", "invalid_table_location"),
                ((self.fixture / "table", "--table-version", "secret-version"),
                 2, "configuration", "invalid_cli_argument"),
            ):
                with self.subTest(command=command, code=code):
                    result = self.run_cli(command, *arguments)
                    self.assertEqual(result.returncode, status, result.stderr)
                    self.assertEqual(result.stdout, b"")
                    self.assert_diagnostic(result.stderr, phase, code)

    def test_installed_cli_closed_stdout_returns_status_three(self):
        for arguments in (("--help",), ("inspect", self.fixture / "table"),
                          ("scan", self.fixture / "table")):
            with self.subTest(arguments=arguments):
                reader, writer = os.pipe()
                os.close(reader)
                with os.fdopen(writer, "wb") as stdout:
                    result = subprocess.run(
                        [str(self.executable), *map(str, arguments)],
                        cwd=self.directory, env=self.environment, stdin=subprocess.DEVNULL,
                        stdout=stdout, stderr=subprocess.PIPE, timeout=30,
                    )
                self.assertEqual(result.returncode, 3, result.stderr)
                self.assert_diagnostic(result.stderr, "execution", "output_write")

    def test_installed_cli_external_termination(self):
        table = self.directory / "table"
        log = table / "_delta_log"
        log.mkdir(parents=True)
        log_name = "00000000000000000000.json"
        actions = [json.loads(line) for line in
                   (self.fixture / "table/_delta_log" / log_name).read_text(encoding="utf-8").splitlines()]
        add = next(action["add"] for action in actions if "add" in action)
        data = pq.ParquetFile(self.fixture / "table" / add["path"]).read()
        label = data.schema.get_field_index("label")
        # Exceed the platforms' pipe capacity so the scan cannot exit before termination.
        data = data.set_column(label, data.schema.field(label),
                               pa.array(["x" * 32768] * data.num_rows))
        self.assertGreater(data.nbytes, 1024 * 1024)
        pq.write_table(data, table / "part.parquet")
        add.update(path="part.parquet", size=(table / "part.parquet").stat().st_size)
        add.pop("stats", None)
        actions = [action for action in actions if "protocol" in action or "metaData" in action]
        actions.append({"add": add})
        (log / log_name).write_text(
            "".join(json.dumps(action) + "\n" for action in actions), encoding="utf-8",
        )

        signals = (signal.SIGINT, signal.SIGTERM) if os.name != "nt" else (None,)
        for termination_signal in signals:
            with self.subTest(signal=termination_signal), ThreadPoolExecutor(max_workers=1) as pool:
                child = subprocess.Popen(
                    [str(self.executable), "scan", str(table)],
                    cwd=self.directory, env=self.environment, stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                try:
                    output = pool.submit(child.stdout.read, 1)
                    self.assertEqual(output.result(timeout=30), b"\xff")
                    self.assertIsNone(child.poll(), "scan exited before termination")
                    if termination_signal is None:
                        child.terminate()
                    else:
                        child.send_signal(termination_signal)
                    child.wait(timeout=30)
                    self.assertEqual(child.stderr.read(), b"")
                    if termination_signal is None:
                        self.assertNotEqual(child.returncode, 0)
                    else:
                        self.assertEqual(child.returncode, -termination_signal)
                finally:
                    if child.poll() is None:
                        child.kill()
                    child.wait(timeout=30)
                    child.stdout.close()
                    child.stderr.close()


if __name__ == "__main__":
    unittest.main()
