"""Linux process checks invoked by cargo xtask cli test-process."""

import functools
import importlib.util
from itertools import product
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from urllib.parse import parse_qs, quote, urlsplit

DAR = Path(sys.argv.pop(1)).resolve()
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

    def predicate_file(self, content):
        path = self.cwd / "secret predicate, file.json"
        path.write_bytes(content)
        return path

    def test_local_validation_before_any_table_request(self):
        self.env.update(LD_PRELOAD=str(self.fault_library), DAR_TEST_REJECT_THREADS="1")
        with http.serve(functools.partial(RecordingStorageHandler, directory=self.cwd)) as server:
            table = f"http://127.0.0.1:{server.server_port}/secret-table"
            cases = [(), ("secret-command",), ("inspect",), ("inspect", table, "secret-extra"),
                     ("inspect", "--secret-option", table), ("inspect", "--table-version"),
                     ("inspect", "--storage-options-file"),
                     ("inspect", "--predicate-file", "secret-missing", table),
                     ("inspect", "--execution-options-file", "secret-missing", table),
                     ("inspect", "--target-partitions", "1", table),
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

    def test_scan_validation_before_any_table_request(self):
        self.env.update(LD_PRELOAD=str(self.fault_library), DAR_TEST_REJECT_THREADS="1")
        with http.serve(functools.partial(RecordingStorageHandler, directory=self.cwd)) as server:
            table = f"http://127.0.0.1:{server.server_port}/secret-table"
            cases = [(), (table, "secret-extra"), ("--column",), ("--limit",),
                     ("--target-partitions",), ("--target-partitions", b"secret-\xff", table),
                     ("--target-partitions", "1", "--target-partitions", "2", table),
                     ("--predicate-file",), ("--predicate-file", b"secret-\xff", table),
                     ("--predicate-file", "secret-missing", "--predicate-file", "secret-missing", table),
                     ("--no-columns", "--column", "secret", table),
                     ("--no-columns", "--no-columns", table),
                     ("--limit", "0", "--limit", "1", table),
                     ("--column", b"secret-\xff", table),
                     ("--column", "id", "value", table),
                     ("--table-version", "0", "--table-version", "1", table),
                     (b"secret-\xff",)]
            for limit in ["", "-1", "+1", " 1", "1 ", "1\n", "1.0", "1e2", "0x1",
                          "18446744073709551616", "secret", "\u0661"]:
                cases.append(("--limit", limit, table))
                cases.append(("--target-partitions", limit, table))
            for target in ["0", "000", "-0", "+0"]:
                cases.append(("--target-partitions", target, table))
            for args in cases:
                with self.subTest(args=args):
                    self.assert_error(self.invoke("scan", *args), 2, "configuration", "invalid_cli_argument")
            self.assert_error(self.invoke("scan", "--storage-options-file", self.options(b"secret"), table),
                              2, "configuration", "invalid_input_json")
            master, slave = os.openpty()
            try:
                result = subprocess.run([DAR, "scan", table], cwd=self.cwd, env=self.env,
                                        stdout=slave, stderr=subprocess.PIPE, timeout=15)
                self.assertEqual(select.select([master], [], [], 0)[0], [], "binary output reached a terminal")
                result.stdout = b""
                self.assert_error(result, 2, "configuration", "invalid_cli_argument")
            finally:
                os.close(master)
                os.close(slave)
            self.assertEqual(server.requests, [])

    def test_execution_options_validation_before_any_table_request(self):
        self.env.update(LD_PRELOAD=str(self.fault_library), DAR_TEST_REJECT_THREADS="1")
        with http.serve(functools.partial(RecordingStorageHandler, directory=self.cwd)) as server:
            table = f"http://127.0.0.1:{server.server_port}/secret-table"
            for args in [
                ("--execution-options-file",),
                ("--execution-options-file", b"secret-\xff", table),
                ("--execution-options-file", "secret-missing", "--execution-options-file", "secret-missing", table),
            ]:
                with self.subTest(args=args):
                    self.assert_error(self.invoke("scan", *args), 2, "configuration", "invalid_cli_argument")
            for content in [
                b"", b"\xff", b"null", b"[]", b"{}{}", b"{}" + b" " * (LIMIT - 1),
                b'{"secret-extra":1}', b'{"parquet_backend":"secret-backend"}',
                b'{"parquet_backend":{"direct":null}}',
                b'{"prefetch_files_per_partition":null}',
                b'{"prefetch_files_per_partition":1e0}',
                b'{"prefetch_files_per_partition":18446744073709551616}',
                b'{"parquet_metadata_size_hint_bytes":null,"parquet_metadata_size_hint_bytes":null}',
            ]:
                with self.subTest(content=content[:100], size=len(content)):
                    diagnostic = self.assert_error(
                        self.invoke("scan", "--execution-options-file", self.options(content), table),
                        2, "configuration", "invalid_input_json",
                    )
                    self.assertEqual(diagnostic["message"], "Invalid JSON input.")
            for field, values in [
                ("max_concurrent_file_reads_per_scan", [0, sys.maxsize]),
                ("max_concurrent_file_reads_per_partition", [0, sys.maxsize]),
                ("output_buffer_batches_per_partition", [0, sys.maxsize]),
                ("parquet_metadata_size_hint_bytes", [0]),
                ("parquet_full_file_read_threshold_bytes", [0]),
            ]:
                for value in values:
                    with self.subTest(field=field, value=value):
                        content = json.dumps({field: value}).encode()
                        self.assert_error(
                            self.invoke("scan", "--execution-options-file", self.options(content), table),
                            2, "configuration", "invalid_configuration",
                        )
            for path in [self.cwd / "secret-missing", self.cwd, "-", "/dev/zero"]:
                with self.subTest(path=path):
                    code = "invalid_input_json" if path == "/dev/zero" else "input_file_io"
                    self.assert_error(self.invoke("scan", "--execution-options-file", path, table),
                                      2, "configuration", code)
            self.assertEqual(server.requests, [])

    def test_execution_options_file_size_boundary_and_literal_dash(self):
        table = CORPUS / "partitioned/table"
        for content in [b"{}", b"{}" + b" " * (LIMIT - 2)]:
            result = self.invoke("scan", "--execution-options-file", self.options(content),
                                 "--limit", "0", table)
            self.assertEqual((result.returncode, result.stderr), (0, b""))
            self.assertTrue(result.stdout.endswith(b"\xff\xff\xff\xff\0\0\0\0"))
        (self.cwd / "-").write_text('{"parquet_metadata_size_hint_bytes":null}', encoding="utf-8")
        result = self.invoke("scan", "--execution-options-file", "-", "--limit", "0", table)
        self.assertEqual((result.returncode, result.stderr), (0, b""))

    def test_predicate_validation_before_any_table_request(self):
        self.env.update(LD_PRELOAD=str(self.fault_library), DAR_TEST_REJECT_THREADS="1")
        constant = b'{"op":"constant","value":true}'
        too_deep = b'{"op":"not","arg":' * 32 + constant + b'}' * 32
        too_wide = b'{"op":"and","args":[' + b','.join([constant] * 1024) + b']}'
        with http.serve(functools.partial(RecordingStorageHandler, directory=self.cwd)) as server:
            table = f"http://127.0.0.1:{server.server_port}/secret-table"
            for content in [b"", b"\xff", b"{}", b"null", b"[]", constant + b"{}",
                            b'{"op":"constant","value":true,"value":false}',
                            b'{"op":"is_null","column":"secret-column","secret-extra":0}',
                            b'{"op":"eq","column":"secret-column","value":{"type":"int64","value":"secret-value"}}',
                            b'{"op":"eq","column":"secret-column","value":["utf8","secret-value"]}',
                            too_deep, too_wide, constant + b" " * (LIMIT + 1 - len(constant))]:
                with self.subTest(content=content[:100], size=len(content)):
                    diagnostic = self.assert_error(
                        self.invoke("scan", "--predicate-file", self.predicate_file(content), table),
                        2, "configuration", "invalid_input_json",
                    )
                    self.assertEqual(diagnostic["message"], "Invalid JSON input.")
            for path in [self.cwd / "secret-missing", self.cwd, "-", "/dev/zero"]:
                with self.subTest(path=path):
                    code = "invalid_input_json" if path == "/dev/zero" else "input_file_io"
                    self.assert_error(self.invoke("scan", "--predicate-file", path, table),
                                      2, "configuration", code)
            self.assertEqual(server.requests, [])

    def test_predicate_file_size_boundary_and_literal_dash(self):
        table = CORPUS / "partitioned/table"
        constant = b'{"op":"constant","value":true}'
        for content in [constant, constant + b" " * (LIMIT - len(constant))]:
            result = self.invoke("scan", "--predicate-file", self.predicate_file(content),
                                 "--limit", "0", table)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stderr, b"")
            self.assertTrue(result.stdout.endswith(b"\xff\xff\xff\xff\0\0\0\0"))
        (self.cwd / "-").write_bytes(constant)
        result = self.invoke("scan", "--predicate-file", "-", "--limit", "0", table)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_native_predicate_validation_leaves_stdout_empty(self):
        table = self.metadata_table([
            {"name": name, "type": kind, "nullable": True, "metadata": {}}
            for name, kind in [("id", "integer"), ("amount", "decimal(12,2)"),
                               ("created_at", "timestamp"), ("label", "string"),
                               ("data", "binary"), ("day", "date")]
        ])
        for column, scalar in [
            ("id", {"type": "int32", "value": "1"}),
            ("amount", {"type": "decimal128", "value": "12345", "precision": 12, "scale": 2}),
            ("created_at", {"type": "timestamp_us", "value": "-1", "timezone": "UTC"}),
            ("label", {"type": "utf8", "value": "secret-literal"}),
            ("data", {"type": "binary", "value": [0, 255]}),
            ("day", {"type": "date32", "value": "-1"}),
        ]:
            with self.subTest(valid_column=column):
                predicate = {"op": "eq", "column": column, "value": scalar}
                path = self.predicate_file(json.dumps(predicate).encode())
                result = self.invoke("scan", "--predicate-file", path, table)
                self.assertEqual((result.returncode, result.stderr), (0, b""))

        cases = [
            ({"op": "is_null", "column": ""}, "invalid_column_reference"),
            ({"op": "is_null", "column": "profile.secret"}, "invalid_column_reference"),
            ({"op": "is_null", "column": "secret-missing"}, "column_not_found"),
        ]
        for column, scalar in [
            ("id", {"type": "utf8", "value": "secret-literal"}),
            ("id", {"type": "int64", "value": "1"}),
            ("amount", {"type": "decimal128", "value": "1", "precision": 11, "scale": 2}),
            ("amount", {"type": "decimal128", "value": "1", "precision": 12, "scale": 3}),
            ("created_at", {"type": "timestamp_us", "value": "-1", "timezone": None}),
            ("created_at", {"type": "timestamp_us", "value": "-1", "timezone": "Etc/UTC"}),
            ("label", {"type": "large_utf8", "value": "secret-literal"}),
            ("data", {"type": "large_binary", "value": [0, 255]}),
            ("data", {"type": "fixed_size_binary", "value": [0, 255], "size": 2}),
            ("day", {"type": "int32", "value": "-1"}),
        ]:
            cases.append(({"op": "eq", "column": column, "value": scalar}, "scalar_type_mismatch"))
        for value, precision, scale in [("1", 0, 0), ("1", 39, 2), ("1", 12, 13),
                                        ("1000000000000", 12, 2), ("1", 255, -128)]:
            cases.append(({
                "op": "eq", "column": "amount",
                "value": {"type": "decimal128", "value": value, "precision": precision, "scale": scale},
            }, "invalid_decimal"))
        for op, constant in [("and", False), ("or", True)]:
            cases.append(({"op": op, "args": [
                {"op": "constant", "value": constant}, {"op": "is_null", "column": "secret-missing"},
            ]}, "column_not_found"))
        for predicate, reason in cases:
            for flags in [(), ("--limit", "0")]:
                with self.subTest(predicate=predicate, flags=flags):
                    path = self.predicate_file(json.dumps(predicate).encode())
                    diagnostic = self.assert_error(
                        self.invoke("scan", "--predicate-file", path, *flags, table),
                        1, "scan_planning", "unsupported_predicate",
                    )
                    self.assertTrue(diagnostic["message"].endswith(f"reason={reason}"), diagnostic)

    def test_duplicate_table_columns_fail_during_snapshot_loading(self):
        # Kernel rejects duplicate schema names before predicate planning can run.
        field = {"name": "secret-id", "type": "integer", "nullable": True, "metadata": {}}
        table = self.metadata_table([field, field])
        path = self.predicate_file(b'{"op":"is_null","column":"secret-id"}')
        self.assert_error(self.invoke("scan", "--predicate-file", path, table),
                          1, "snapshot", "snapshot_load")

    def test_scan_planning_errors_leave_stdout_empty(self):
        table = CORPUS / "partitioned/table"
        for columns in [("secret-missing",), ("id", "id"), ("id,value",), ("profile.city",)]:
            with self.subTest(columns=columns):
                args = ["scan", table]
                for column in columns:
                    args.extend(["--column", column])
                self.assert_error(self.invoke(*args), 1, "scan_planning", "invalid_projection")

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

    def test_explicit_version_stops_before_auxiliary_listing_pages(self):
        (self.cwd / "bucket").mkdir()
        for table_name in ["table", "secret%name"]:
            table = self.metadata_table().rename(self.cwd / "bucket" / table_name)
            commit = "00000000000000000000.json"
            commit_size = (table / "_delta_log" / commit).stat().st_size
            for last_entry in ["_last_checkpoint", "_sidecars/part.parquet", "_staged_commits/commit.json"]:
                with self.subTest(table=table_name, last_entry=last_entry):
                    class PaginatedStorageHandler(RecordingStorageHandler):
                        def do_GET(self):
                            query = parse_qs(urlsplit(self.path).query)
                            if query.get("list-type") != ["2"]:
                                return super().do_GET()
                            self.server.requests.append((self.path, None, self.client_address[1]))
                            if "continuation-token" in query:
                                self.send_error(403, "unnecessary listing page")
                                return
                            # The nested entry sorts before the valid commit and must
                            # be skipped without ending the listing at its leaf name.
                            entries = [("00000000000000000000.aaa/zzz", 1),
                                       (commit, commit_size), (last_entry, 1)]
                            contents = "".join(
                                f"<Contents><Key>{table_name}/_delta_log/{name}</Key>"
                                "<LastModified>2026-01-01T00:00:00Z</LastModified>"
                                f"<Size>{size}</Size></Contents>" for name, size in entries
                            )
                            data = (
                                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                                "<IsTruncated>true</IsTruncated>"
                                "<NextContinuationToken>auxiliary-page</NextContinuationToken>"
                                + contents + "</ListBucketResult>"
                            ).encode()
                            self.send_response(200)
                            self.send_header("Content-Type", "application/xml")
                            self.send_header("Content-Length", str(len(data)))
                            self.end_headers()
                            self.wfile.write(data)

                    with http.serve(functools.partial(PaginatedStorageHandler, directory=self.cwd)) as server:
                        options = self.options(json.dumps({
                            "aws_endpoint_url_s3": f"http://127.0.0.1:{server.server_port}",
                            "aws_region": "us-east-1", "aws_skip_signature": "true", "allow_http": "true",
                        }).encode())
                        for version in ["0", "999"]:
                            server.requests.clear()
                            result = self.invoke("inspect", "--table-version", version,
                                                 "--storage-options-file", options,
                                                 f"s3://bucket/{quote(table_name)}")
                            if version == "0":
                                self.assertEqual(self.assert_inspection(result)["table_version"], "0")
                            else:
                                self.assert_error(result, 1, "snapshot", "snapshot_load")
                            listings = [path for path, *_ in server.requests
                                        if "list-type" in parse_qs(urlsplit(path).query)]
                            self.assertEqual(len(listings), 1, server.requests)

    def test_http_inspection_reads_only_metadata(self):
        with http.serve(functools.partial(RecordingStorageHandler, directory=CORPUS / "deletion_vectors/table")) as server:
            value = self.assert_inspection(self.invoke("inspect", "--storage-options-file", self.options(b'{"allow_http":"true"}'),
                                             f"http://127.0.0.1:{server.server_port}/"))
            self.assertEqual(value["table_version"], "1")
            self.assertTrue(server.requests)
            self.assertTrue(all(path == "/_delta_log" or path.startswith("/_delta_log/")
                                for path, *_ in server.requests), server.requests)

    def test_stdout_failures_including_broken_pipe(self):
        table = self.metadata_table([{
            "name": "id", "type": "integer", "nullable": True, "metadata": {},
        }])
        for args in [("--help",), ("--version",), ("inspect", table), ("scan", table)]:
            with open("/dev/full", "wb") as sink:
                result = subprocess.run([DAR, *args], cwd=self.cwd, env=self.env, stdout=sink,
                                        stderr=subprocess.PIPE, timeout=15)
            result.stdout = b""
            self.assert_error(result, 3, "execution", "output_write")
        for command in ["inspect", "scan"]:
            read_fd, write_fd = os.pipe()
            os.close(read_fd)
            with os.fdopen(write_fd, "wb") as sink:
                result = subprocess.run([DAR, command, table], cwd=self.cwd, env=self.env, stdout=sink,
                                        stderr=subprocess.PIPE, timeout=15)
            result.stdout = b""
            self.assert_error(result, 3, "execution", "output_write")
        with open("/dev/full", "wb") as sink:
            result = subprocess.run([DAR, "--version"], stdout=sink, stderr=sink, timeout=15)
        self.assertEqual(result.returncode, 3)

    def test_signals_stop_blocked_loading_without_child_processes(self):
        for command, signum in product(["inspect", "scan"], [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]):
            with self.subTest(command=command, signal=signum):
                started, disconnected = threading.Event(), threading.Event()

                class BlockingStorageHandler(RecordingStorageHandler):
                    def send_head(self):
                        started.set()
                        self.wait_for_disconnect(disconnected)

                    def do_PROPFIND(self):
                        self.send_head()

                with http.serve(functools.partial(BlockingStorageHandler, directory=self.cwd)) as server:
                    args = [DAR, command, "--storage-options-file", self.options(b'{"allow_http":"true"}'),
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


if __name__ == "__main__":
    unittest.main()
