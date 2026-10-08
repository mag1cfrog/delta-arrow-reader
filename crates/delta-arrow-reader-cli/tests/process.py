"""CLI process checks, run by cargo test on Linux using only Python's stdlib."""

import functools
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import unittest

DAR = Path(sys.argv.pop(1)).resolve()
VERSION = sys.argv.pop(1)
ROOT = Path(__file__).resolve().parents[3]
CORPUS = ROOT / "tests/reader/fixtures/external_writer/corpus"
LIMIT = 1024 * 1024

spec = importlib.util.spec_from_file_location("reader_https", ROOT / "tests/reader/https.py")
http = importlib.util.module_from_spec(spec)
spec.loader.exec_module(http)


class RecordingStorageHandler(http.Storage):
    def do_PROPFIND(self):
        self.server.requests.append((self.path, None, self.client_address[1]))
        super().do_PROPFIND()


class ProcessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temporary = tempfile.TemporaryDirectory(prefix="dar-process-faults-")
        cls.addClassCleanup(temporary.cleanup)
        cls.fault_library = Path(temporary.name) / "process_faults.so"
        subprocess.run(
            ["cc", "-shared", "-fPIC", str(Path(__file__).with_name("process_faults.c")),
             "-o", str(cls.fault_library), "-ldl"], check=True, capture_output=True, timeout=30,
        )

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="secret-dar-")
        self.addCleanup(temporary.cleanup)
        self.cwd = Path(temporary.name)
        self.env = dict(os.environ, NO_PROXY="*", no_proxy="*", RUST_LOG="trace")

    def invoke(self, *args, **kwargs):
        return subprocess.run(
            [DAR, *args], cwd=self.cwd, env=self.env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, **kwargs,
        )

    def assert_error(self, result, status, phase, code):
        self.assertEqual(result.returncode, status, result)
        self.assertEqual(result.stdout, b"")
        self.assertTrue(result.stderr.endswith(b"\n"))
        self.assertEqual(result.stderr.count(b"\n"), 1)
        diagnostic = json.loads(result.stderr)
        self.assertEqual(set(diagnostic), {"phase", "code", "message"})
        self.assertTrue(all(isinstance(value, str) for value in diagnostic.values()))
        self.assertEqual((diagnostic["phase"], diagnostic["code"]), (phase, code))
        self.assertNotIn(b"secret", result.stderr)
        return diagnostic

    def assert_inspection(self, result):
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, b"")
        self.assertTrue(result.stdout.endswith(b"\n"))
        self.assertEqual(result.stdout.count(b"\n"), 1)
        value = json.loads(result.stdout)
        self.assertEqual(set(value), {"format_version", "table_version", "schema"})
        self.assertEqual(value["format_version"], 1)
        self.assertIsInstance(value["table_version"], str)
        return value

    def metadata_table(self, fields=(), protocol=1):
        table = self.cwd / "-secret table, with spaces"
        log = table / "_delta_log"
        log.mkdir(parents=True, exist_ok=True)
        actions = [
            {"protocol": {"minReaderVersion": protocol, "minWriterVersion": 2}},
            {"metaData": {
                "id": "test", "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": fields}),
                "partitionColumns": [], "configuration": {},
            }},
        ]
        (log / "00000000000000000000.json").write_text(
            "".join(json.dumps(action) + "\n" for action in actions), encoding="utf-8"
        )
        return table

    def options(self, content=b"{}"):
        path = self.cwd / "secret options, file.json"
        path.write_bytes(content)
        return path

    def test_static_help_and_version_outside_checkout(self):
        # Any attempted runtime worker creation fails, even with an explicit count.
        self.env.update(LD_PRELOAD=str(self.fault_library), DAR_TEST_REJECT_THREADS="1")
        for args in [("--help",), ("-h",), ("inspect", "--help"), ("inspect", "-h"),
                     ("help", "inspect"),
                     ("inspect", "--storage-options-file", "secret-missing", "--help")]:
            with self.subTest(args=args):
                result = self.invoke(*args)
                self.assertEqual(result.returncode, 0)
                self.assertEqual(result.stderr, b"")
                self.assertIn(b"Usage:", result.stdout)
                self.assertIn(b"--help", result.stdout)
                if "inspect" in args:
                    self.assertIn(b"--table-version", result.stdout)
                    self.assertIn(b"--storage-options-file", result.stdout)
        for flag in ["--version", "-V"]:
            result = self.invoke(flag)
            self.assertEqual((result.returncode, result.stdout, result.stderr),
                             (0, f"dar {VERSION}\n".encode(), b""))

    def test_inspection_ignores_inherited_tokio_worker_count(self):
        table = self.metadata_table()
        for count in ["0", "secret-invalid-count", "9999999999999999999999999", "secret-\udcff"]:
            with self.subTest(count=count):
                self.env["TOKIO_WORKER_THREADS"] = count
                self.assert_inspection(self.invoke("inspect", table))

    def test_thread_start_failures_are_redacted_and_do_not_hang(self):
        table = self.metadata_table()
        self.env.update(LD_PRELOAD=str(self.fault_library), RUST_BACKTRACE="full")
        # Reject the core blocking thread, Kernel's background thread, and its
        # blocking worker in turn. Later failures must not strand a waiting caller.
        for limit in range(3):
            with self.subTest(limit=limit):
                self.env["DAR_TEST_THREAD_LIMIT"] = str(limit)
                self.assert_error(self.invoke("inspect", table), 1, "execution", "runtime_initialization")
        self.env["DAR_TEST_THREAD_LIMIT"] = "3"
        self.assert_inspection(self.invoke("inspect", table))

    def test_local_validation_before_any_table_request(self):
        self.env.update(LD_PRELOAD=str(self.fault_library), DAR_TEST_REJECT_THREADS="1")
        with http.serve(functools.partial(RecordingStorageHandler, directory=self.cwd)) as server:
            table = f"http://127.0.0.1:{server.server_port}/secret-table"
            cases = [(), ("secret-command",), ("inspect",), ("inspect", table, "secret-extra"),
                     ("inspect", "--secret-option", table), ("inspect", "--table-version"),
                     ("inspect", "--storage-options-file"),
                     ("inspect", "--table-version", "0", "--table-version", "1", table),
                     ("inspect", "--storage-options-file", "secret-missing", "--storage-options-file", "secret-missing", table),
                     ("inspect", b"secret-\xff"), ("inspect", "--", table, "extra")]
            for version in ["", "-1", "+1", " 1", "1 ", "1\n", "1.0", "1e2", "0x1", "18446744073709551616", "secret", "\u0661"]:
                cases.append(("inspect", "--table-version", version, table))
            for args in cases:
                with self.subTest(args=args):
                    self.assert_error(self.invoke(*args), 2, "configuration", "invalid_cli_argument")

            for content in [b"", b"secret", b"{", b"{}secret", b"{}{}", b"\xff",
                            b'"secret"', b"[]", b"null", b'{"secret":1}', b'{"secret":true}',
                            b'{"secret":null}', b'{"secret":[]}', b'{"secret":{}}',
                            b'{"secret":"a","secret":"b"}', b'{"a":"a","\\u0061":"b"}',
                            b'{"secret":"\xff"}', b"{}" + b" " * (LIMIT - 1)]:
                with self.subTest(content=content[:50], size=len(content)):
                    self.assert_error(self.invoke("inspect", "--storage-options-file", self.options(content), table),
                               2, "configuration", "invalid_input_json")
            for path in [self.cwd / "secret-missing", self.cwd, "-"]:
                self.assert_error(self.invoke("inspect", "--storage-options-file", path, table),
                           2, "configuration", "input_file_io")
            self.assertEqual(server.requests, [])

    def test_json_size_limit_stops_reading_and_counts_bytes(self):
        table = self.metadata_table()
        for content in [b"{}", b"{}" + b" " * (LIMIT - 2)]:
            self.assert_inspection(self.invoke("inspect", "--storage-options-file", self.options(content), table))
        self.assert_error(self.invoke("inspect", "--storage-options-file", "/dev/zero", table),
                   2, "configuration", "invalid_input_json")
        content = json.dumps({"secret": "\u00e9" * (LIMIT // 2)}, ensure_ascii=False).encode()
        self.assertLess(len(content.decode()), LIMIT)
        self.assert_error(self.invoke("inspect", "--storage-options-file", self.options(content), table),
                   2, "configuration", "invalid_input_json")

    def test_empty_schema_relative_path_file_url_and_literal_dash_file(self):
        table = self.metadata_table()
        expected = {"format_version": 1, "table_version": "0", "schema": {"fields": [], "metadata": {}}}
        self.assertEqual(self.assert_inspection(self.invoke("inspect", "--", table.name)), expected)
        self.assertEqual(self.assert_inspection(self.invoke("inspect", table.as_uri())), expected)
        (self.cwd / "-").write_text("{}", encoding="utf-8")
        self.assertEqual(self.assert_inspection(self.invoke("inspect", "--storage-options-file", "-", table)), expected)

    def test_explicit_version_preserves_literal_percent_paths(self):
        table = self.metadata_table().rename(self.cwd / "secret%name")
        for location in [table, table.as_uri()]:
            with self.subTest(location=location):
                latest = self.assert_inspection(self.invoke("inspect", location))
                historical = self.assert_inspection(self.invoke("inspect", "--table-version", "0", location))
                self.assertEqual(historical, latest)

    def test_latest_historical_and_missing_data_files(self):
        for name, latest in [("partitioned", "0"), ("nested_mapping", "1"), ("deletion_vectors", "1")]:
            with self.subTest(name=name):
                table = self.cwd / name
                shutil.copytree(CORPUS / name / "table/_delta_log", table / "_delta_log")
                current = self.assert_inspection(self.invoke("inspect", table))
                original = self.assert_inspection(self.invoke("inspect", "--table-version", "0", table))
                self.assertEqual(current["table_version"], latest)
                self.assertEqual(original["table_version"], "0")
                if name == "nested_mapping":
                    def city_field(schema):
                        return schema["schema"]["fields"][2]["data_type"]["Struct"][1]
                    self.assertEqual(city_field(current)["name"], "city")
                    self.assertEqual(city_field(original)["name"], "old_city")
                    self.assertEqual(city_field(current)["metadata"]["delta.columnMapping.id"], "5")

    def test_schema_metadata_and_unscannable_protocol_are_inspectable(self):
        table = self.metadata_table([{"name": "secret-name", "type": "string", "nullable": False,
                                      "metadata": {"comment": "secret requested metadata"}}], protocol=4)
        field = self.assert_inspection(self.invoke("inspect", table))["schema"]["fields"][0]
        self.assertEqual(field["name"], "secret-name")
        self.assertEqual(field["metadata"]["comment"], "secret requested metadata")
        self.assertFalse(field["nullable"])

    def test_historical_inspection_ignores_unrelated_and_newer_log_files(self):
        table = self.metadata_table()
        log = table / "_delta_log"
        (log / "00000000000000000001.json").write_text("secret invalid JSON\n", encoding="utf-8")
        (log / "00000000000000000000.00000000000000000001.compacted.json").write_text(
            "secret invalid compaction\n", encoding="utf-8"
        )
        value = self.assert_inspection(self.invoke("inspect", "--table-version", "0", table))
        self.assertEqual(value["table_version"], "0")
        version = 1_000_000_000_000
        (log / f"{version:020}.crc").write_text("{}", encoding="utf-8")
        nested = log / f"{version:020}.nested"
        nested.mkdir()
        (nested / "00000000000000000000.json").write_text("{}", encoding="utf-8")
        self.assert_error(self.invoke("inspect", "--table-version", str(version), table),
                          1, "snapshot", "snapshot_load")

    def test_sparse_logs_fail_without_searching_numeric_version_gaps(self):
        table = self.metadata_table()
        log = table / "_delta_log"
        commit = (log / "00000000000000000000.json").read_bytes()
        version = 1_000_000_000_000
        for suffix, content in [
            (".checkpoint.parquet", b""),
            (".checkpoint.0000000001.0000000002.parquet", b"secret incomplete checkpoint"),
            (".json", commit),
        ]:
            with self.subTest(suffix=suffix):
                path = log / f"{version:020}{suffix}"
                path.write_bytes(content)
                try:
                    self.assert_error(self.invoke("inspect", "--table-version", str(version), table),
                                      1, "snapshot", "snapshot_load")
                finally:
                    path.unlink()

    def test_reader_errors_preserve_core_diagnostic_and_redact_secrets(self):
        table = self.metadata_table()
        for location, phase, code in [(self.cwd / "secret-missing", "table_location", "invalid_table_location"),
                                      (self.cwd, "snapshot", "snapshot_load"),
                                      ("unknown://secret-user:secret-password@host/table?token=secret-token", "storage", "storage_initialization")]:
            diagnostic = self.assert_error(self.invoke("inspect", location), 1, phase, code)
            self.assertTrue(diagnostic["message"].startswith(f"delta reader error: phase={phase} code={code} reason="))
        for version in ["999", "1000000000000", "18446744073709551614", "18446744073709551615"]:
            with self.subTest(version=version):
                self.assert_error(self.invoke("inspect", "--table-version", version, table),
                                  1, "snapshot", "snapshot_load")
        invalid = self.options(b'{"allow_http":"secret-invalid-value"}')
        self.assert_error(self.invoke("inspect", "--storage-options-file", invalid, "http://127.0.0.1:9/secret-table"),
                   1, "storage", "storage_initialization")
        table = self.metadata_table([{
            "name": "secret-array", "type": {"type": "array", "elementType": "string", "containsNull": True},
            "nullable": True, "metadata": {"delta.columnMapping.nested.ids": "secret-invalid-object"},
        }])
        self.assert_error(self.invoke("inspect", table), 1, "schema", "schema_conversion")

    def test_http_inspection_reads_only_metadata(self):
        with http.serve(functools.partial(RecordingStorageHandler, directory=CORPUS / "deletion_vectors/table")) as server:
            value = self.assert_inspection(self.invoke("inspect", "--storage-options-file", self.options(b'{"allow_http":"true"}'),
                                             f"http://127.0.0.1:{server.server_port}/"))
            self.assertEqual(value["table_version"], "1")
            self.assertTrue(server.requests)
            self.assertTrue(all(path == "/_delta_log" or path.startswith("/_delta_log/")
                                for path, *_ in server.requests), server.requests)

    def test_stdout_failures_including_broken_pipe(self):
        table = self.metadata_table()
        for args in [("--help",), ("--version",), ("inspect", table)]:
            with open("/dev/full", "wb") as sink:
                result = subprocess.run([DAR, *args], cwd=self.cwd, env=self.env, stdout=sink,
                                        stderr=subprocess.PIPE, timeout=15)
            result.stdout = b""
            self.assert_error(result, 3, "execution", "output_write")
        read_fd, write_fd = os.pipe()
        os.close(read_fd)
        with os.fdopen(write_fd, "wb") as sink:
            result = subprocess.run([DAR, "inspect", table], cwd=self.cwd, env=self.env, stdout=sink,
                                    stderr=subprocess.PIPE, timeout=15)
        result.stdout = b""
        self.assert_error(result, 3, "execution", "output_write")
        with open("/dev/full", "wb") as sink:
            result = subprocess.run([DAR, "--version"], stdout=sink, stderr=sink, timeout=15)
        self.assertEqual(result.returncode, 3)

    def test_signals_stop_blocked_loading_without_child_processes(self):
        for signum in [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]:
            with self.subTest(signal=signum):
                started, disconnected = threading.Event(), threading.Event()

                class BlockingStorageHandler(RecordingStorageHandler):
                    def send_head(self):
                        started.set()
                        self.wait_for_disconnect(disconnected)

                    def do_PROPFIND(self):
                        self.send_head()

                with http.serve(functools.partial(BlockingStorageHandler, directory=self.cwd)) as server:
                    args = [DAR, "inspect", "--storage-options-file", self.options(b'{"allow_http":"true"}'),
                            f"http://127.0.0.1:{server.server_port}/"]
                    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env) as process:
                        try:
                            self.assertTrue(started.wait(10), "table request never started")
                            for children in Path(f"/proc/{process.pid}/task").glob("*/children"):
                                self.assertEqual(children.read_text().strip(), "")
                            process.send_signal(signum)
                            stdout, stderr = process.communicate(timeout=10)
                            self.assertEqual(process.returncode, -signum)
                            self.assertEqual((stdout, stderr), (b"", b""))
                            self.assertTrue(disconnected.wait(5), "terminated process retained a socket")
                        finally:
                            if process.poll() is None:
                                process.kill()
                                process.communicate(timeout=10)

    def test_completion_does_not_wait_for_unused_blocking_prefetch(self):
        for invalid_schema in [False, True]:
            with self.subTest(invalid_schema=invalid_schema):
                fields = [{
                    "name": "secret-array", "type": {"type": "array", "elementType": "string", "containsNull": True},
                    "nullable": True, "metadata": {"delta.columnMapping.nested.ids": "secret-invalid-object"},
                }] if invalid_schema else []
                table = self.metadata_table(fields)
                log = table / "_delta_log"
                old = log / "00000000000000000000.json"
                latest = log / "00000000000000000001.json"
                latest.write_bytes(old.read_bytes())
                old.write_text("unused invalid JSON\n", encoding="utf-8")
                started = self.cwd / "open-started"
                started.unlink(missing_ok=True)
                self.env.update(LD_PRELOAD=str(self.fault_library), DAR_TEST_BLOCKED_OPEN=str(old),
                                DAR_TEST_LATEST_LOG=str(latest), DAR_TEST_OPEN_STARTED=str(started))
                result = self.invoke("inspect", table)
                self.assertTrue(started.exists(), "unused blocking prefetch never started")
                if invalid_schema:
                    self.assert_error(result, 1, "schema", "schema_conversion")
                else:
                    self.assertEqual(self.assert_inspection(result)["table_version"], "1")


if __name__ == "__main__":
    unittest.main()
