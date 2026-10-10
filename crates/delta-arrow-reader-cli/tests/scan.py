"""Linux scan checks: python scan.py /path/to/dar (requires PyArrow)."""

import concurrent.futures
import contextlib
import functools
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

DAR = Path(sys.argv.pop(1)).resolve()
ROOT = Path(__file__).resolve().parents[3]
CORPUS = ROOT / "tests/reader/fixtures/external_writer/corpus"
IPC_END = b"\xff\xff\xff\xff\x00\x00\x00\x00"

spec = importlib.util.spec_from_file_location("reader_https", ROOT / "tests/reader/https.py")
http = importlib.util.module_from_spec(spec)
spec.loader.exec_module(http)


class ScanStorage(http.Storage):
    def send_head(self):
        if self.path.endswith(".parquet"):
            with self.server.lock:
                self.server.data_paths.add(self.path)
                fail = (self.server.fail_after is not None
                        and len(self.server.data_paths) > self.server.fail_after)
            if self.server.block_data:
                self.server.held.set()
                self.wait_for_disconnect(self.server.disconnected)
                return None
            if fail:
                self.send_error(404, "secret simulated data-file failure")
                return None
        return super().send_head()


class ScanTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temporary = tempfile.TemporaryDirectory(prefix="dar-scan-faults-")
        cls.addClassCleanup(temporary.cleanup)
        cls.fault_library = Path(temporary.name) / "process_faults.so"
        subprocess.run(
            ["cc", "-shared", "-fPIC", str(Path(__file__).with_name("process_faults.c")),
             "-o", str(cls.fault_library), "-ldl"], check=True, capture_output=True, timeout=30,
        )

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="secret-dar-scan-")
        self.addCleanup(temporary.cleanup)
        self.cwd = Path(temporary.name)
        self.env = dict(os.environ, NO_PROXY="*", no_proxy="*", RUST_LOG="trace")
        self.options = self.cwd / "storage.json"
        self.options.write_text('{"allow_http":"true"}', encoding="utf-8")

    def repeated_table(self, count=32):
        # Reuse one checked-in Spark file. Many tiny files exercise producer
        # progress without making the pipe tests depend on host memory or speed.
        source = CORPUS / "partitioned/table"
        actions = [json.loads(line) for line in
                   (source / "_delta_log/00000000000000000000.json").read_text().splitlines()]
        template = next(action["add"] for action in actions if "add" in action)
        table = self.cwd / "table"
        log = table / "_delta_log"
        log.mkdir(parents=True)
        actions = [action for action in actions if "protocol" in action or "metaData" in action]
        for index in range(count):
            name = f"part-{index:03}.parquet"
            shutil.copyfile(source / template["path"], table / name)
            actions.append({"add": dict(template, path=name)})
        (log / "00000000000000000000.json").write_text(
            "".join(json.dumps(action) + "\n" for action in actions), encoding="utf-8",
        )
        return table

    @contextlib.contextmanager
    def storage(self, table, *, fail_after=None, block_data=False, handler=ScanStorage):
        with http.serve(functools.partial(handler, directory=table)) as server:
            server.lock = threading.Lock()
            server.data_paths = set()
            server.fail_after = fail_after
            server.block_data = block_data
            server.held = threading.Event()
            server.disconnected = threading.Event()
            server.url = f"http://127.0.0.1:{server.server_port}/"
            yield server

    @contextlib.contextmanager
    def start_scan(self, table, *flags):
        # A single CPU keeps the core's default partition count independent of
        # runner size. Set affinity before spawning, without a threaded preexec_fn.
        affinity = os.sched_getaffinity(0)
        try:
            os.sched_setaffinity(0, {min(affinity)})
            process = subprocess.Popen(
                [DAR, "scan", *flags, str(table)], cwd=self.cwd, env=self.env,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                pipesize=4096,
            )
        finally:
            os.sched_setaffinity(0, affinity)
        try:
            yield process
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=15)
            process.stdout.close()
            process.stderr.close()

    def read_with_timeout(self, process, operation):
        # Kill before joining a blocked reader thread when an assertion or read
        # fails, so a regression cannot hang the test runner during cleanup.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(operation)
            try:
                return future.result(timeout=15)
            except BaseException:
                process.kill()
                process.wait(timeout=15)
                raise

    def wait_for_blocked_output(self, process):
        deadline = time.monotonic() + 15
        while process.poll() is None and time.monotonic() < deadline:
            for wait_channel in Path(f"/proc/{process.pid}/task").glob("*/wchan"):
                try:
                    state = wait_channel.read_text().strip()
                except FileNotFoundError:
                    continue
                if "pipe" in state and "writ" in state:
                    return
            time.sleep(0.01)
        self.fail(f"child never blocked writing stdout (status={process.poll()})")

    def assert_diagnostic(self, stderr, phase, code):
        self.assertEqual(stderr.count(b"\n"), 1)
        diagnostic = json.loads(stderr)
        self.assertEqual(set(diagnostic), {"phase", "code", "message"})
        self.assertEqual(diagnostic["phase"], phase)
        self.assertEqual(diagnostic["code"], code)
        self.assertNotIn(b"secret", stderr)

    def assert_no_children(self, process):
        for children in Path(f"/proc/{process.pid}/task").glob("*/children"):
            self.assertEqual(children.read_text().strip(), "")

    def test_ipc_matches_spark_fixtures(self):
        for name in ("partitioned", "nested_mapping", "deletion_vectors"):
            fixture = CORPUS / name
            with pa.ipc.open_file(fixture / "expected.arrow") as source:
                expected = source.read_all()
            for columns in (None, ["value", "id"], []):
                projected = expected if columns is None else expected.select(columns)
                for limit in (None, 0, 1, 1000):
                    with self.subTest(fixture=name, columns=columns, limit=limit):
                        flags = ["--no-columns"] if columns == [] else []
                        for column in columns or []:
                            flags.extend(["--column", column])
                        if limit is not None:
                            flags.extend(["--limit", str(limit)])
                        result = subprocess.run(
                            [DAR, "scan", *flags, str(fixture / "table")], env=self.env,
                            capture_output=True, check=True, timeout=15,
                        )
                        self.assertEqual(result.stderr, b"")
                        self.assertTrue(result.stdout.endswith(IPC_END))
                        actual = pa.ipc.open_stream(result.stdout).read_all()
                        # The Rust integration test compares exact metadata with
                        # the core; Spark's independent oracle omits mapping IDs.
                        self.assertTrue(actual.schema.equals(projected.schema, check_metadata=False))
                        self.assertEqual(actual.num_rows, min(projected.num_rows, limit)
                                         if limit is not None else projected.num_rows)
                        if actual.num_columns and actual.num_rows:
                            if limit == 1:
                                self.assertIn(actual.to_pylist()[0], projected.to_pylist())
                            else:
                                self.assertTrue(actual.sort_by("id").equals(
                                    projected.sort_by("id"), check_metadata=False))

    def test_predicate_filters_a_column_omitted_from_output(self):
        predicate = self.cwd / "predicate.json"
        predicate.write_text(json.dumps({
            "op": "eq", "column": "id", "value": {"type": "int32", "value": "10"},
        }), encoding="utf-8")
        with self.start_scan(CORPUS / "partitioned/table", "--predicate-file", str(predicate),
                             "--column", "value") as process:
            stdout, stderr = process.communicate(timeout=15)
            self.assertEqual((process.returncode, stderr), (0, b""))
            self.assertTrue(stdout.endswith(IPC_END))
            actual = pa.ipc.open_stream(stdout).read_all()
            self.assertEqual(actual.schema.names, ["value"])
            self.assertEqual(actual.to_pylist(), [{"value": 30}])

    def test_execution_options_compose_with_filtered_scans(self):
        predicate = self.cwd / "predicate.json"
        predicate.write_text(json.dumps({
            "op": "eq", "column": "id", "value": {"type": "int32", "value": "10"},
        }), encoding="utf-8")
        options = self.cwd / "execution-options.json"
        for backend in ["direct", "delta_kernel", None]:
            flags = []
            if backend is not None:
                options.write_text(json.dumps({
                    "parquet_backend": backend, "max_concurrent_file_reads_per_scan": 1,
                    "max_concurrent_file_reads_per_partition": 1, "output_buffer_batches_per_partition": 1,
                    "prefetch_files_per_partition": 0, "parquet_metadata_size_hint_bytes": None,
                    "parquet_full_file_read_threshold_bytes": 1048576,
                }), encoding="utf-8")
                flags = ["--execution-options-file", str(options), "--target-partitions", "2"]
            with self.subTest(backend=backend), self.start_scan(
                CORPUS / "partitioned/table", *flags, "--predicate-file", str(predicate),
                "--column", "value", "--limit", "1",
            ) as process:
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual((process.returncode, stderr), (0, b""))
                self.assertTrue(stdout.endswith(IPC_END))
                actual = pa.ipc.open_stream(stdout).read_all()
                self.assertEqual(actual.schema.names, ["value"])
                self.assertEqual(actual.schema.field("value").type, pa.int32())
                self.assertEqual(actual.to_pylist(), [{"value": 30}])

    def test_partition_targets_and_read_capacities_bound_file_requests(self):
        class HeldDataStorage(ScanStorage):
            def send_head(self):
                if self.path.endswith(".parquet"):
                    with self.server.data_arrivals:
                        self.server.data_paths.add(self.path)
                        self.server.data_arrivals.notify_all()
                    self.server.release_data.wait(15)
                return super().send_head()

        table = self.repeated_table(count=4)
        options = self.cwd / "execution-options.json"
        usize_max = (1 << (8 * struct.calcsize("P"))) - 1
        # With one read per partition and no prefetch, held files reveal the
        # built partition count. start_scan pins the default to one CPU.
        cases = [
            ("default_target", None, 8, 1, 0, 1),
            ("two_partitions", 2, 8, 1, 0, 2),
            ("more_partitions_than_files", usize_max, 8, 1, 0, 4),
            ("scan_capacity", 2, 1, 3, 2, 1),
            ("partition_capacity", 1, 8, 1, 2, 1),
        ]
        expected = None
        for backend in ["direct", "delta_kernel"]:
            for name, target, scan_capacity, partition_capacity, prefetch, held_files in cases:
                options.write_text(json.dumps({
                    "parquet_backend": backend,
                    "max_concurrent_file_reads_per_scan": scan_capacity,
                    "max_concurrent_file_reads_per_partition": partition_capacity,
                    "output_buffer_batches_per_partition": 1,
                    "prefetch_files_per_partition": prefetch,
                }), encoding="utf-8")
                flags = [] if target is None else ["--target-partitions", str(target)]
                with self.subTest(backend=backend, case=name), self.storage(
                    table, handler=HeldDataStorage,
                ) as server:
                    server.data_arrivals = threading.Condition(server.lock)
                    server.release_data = threading.Event()
                    with self.start_scan(
                        server.url, "--storage-options-file", str(self.options),
                        "--execution-options-file", str(options), *flags,
                    ) as process:
                        try:
                            with server.data_arrivals:
                                self.assertTrue(server.data_arrivals.wait_for(
                                    lambda: len(server.data_paths) >= held_files, timeout=5,
                                ), f"expected {held_files} held files, got {server.data_paths}")
                                self.assertFalse(server.data_arrivals.wait_for(
                                    lambda: len(server.data_paths) > held_files, timeout=0.25,
                                ), f"too many files: {server.data_paths}")
                        finally:
                            server.release_data.set()
                        stdout, stderr = process.communicate(timeout=15)
                        self.assertEqual((process.returncode, stderr), (0, b""))
                        self.assertTrue(stdout.endswith(IPC_END))
                        actual = pa.ipc.open_stream(stdout).read_all().sort_by("id")
                        self.assertEqual(actual.num_rows, 4 * 120)
                        if expected is None:
                            expected = actual
                        else:
                            self.assertTrue(actual.equals(expected, check_metadata=True))

    def test_narrow_execution_options_bound_reads_while_output_is_blocked(self):
        table = self.repeated_table()
        options = self.cwd / "execution-options.json"
        for backend in ["direct", "delta_kernel"]:
            options.write_text(json.dumps({
                "parquet_backend": backend, "max_concurrent_file_reads_per_scan": 1,
                "max_concurrent_file_reads_per_partition": 1, "output_buffer_batches_per_partition": 1,
                "prefetch_files_per_partition": 0,
            }), encoding="utf-8")
            with self.subTest(backend=backend), self.storage(table) as server, self.start_scan(
                server.url, "--storage-options-file", str(self.options),
                "--execution-options-file", str(options), "--target-partitions", "1",
            ) as process:
                self.wait_for_blocked_output(process)
                time.sleep(0.25)
                with server.lock:
                    self.assertGreater(len(server.data_paths), 0)
                    # One file writing to stdout, one queued batch, one producer
                    # waiting to send. Each fixture file contains one batch.
                    self.assertLessEqual(len(server.data_paths), 3)
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual((process.returncode, stderr), (0, b""))
                self.assertTrue(stdout.endswith(IPC_END))
                self.assertEqual(pa.ipc.open_stream(stdout).read_all().num_rows, 32 * 120)
                self.assertEqual(len(server.data_paths), 32)

    def test_float64_predicates_distinguish_adjacent_values(self):
        table = self.cwd / "float64-table"
        log = table / "_delta_log"
        log.mkdir(parents=True)
        # The JSON parser previously rounded the first value to the second.
        data = pa.table({
            "id": pa.array([1, 2], pa.int32()),
            "value": pa.array([1.9651349465042103, 1.9651349465042105], pa.float64()),
        })
        pq.write_table(data, table / "part.parquet")
        fields = [{"name": name, "type": kind, "nullable": True, "metadata": {}}
                  for name, kind in [("id", "integer"), ("value", "double")]]
        actions = [
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {"metaData": {
                "id": "float64-predicates", "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": fields}),
                "partitionColumns": [], "configuration": {},
            }},
            {"add": {
                "path": "part.parquet", "partitionValues": {},
                "size": (table / "part.parquet").stat().st_size,
                "modificationTime": 0, "dataChange": True,
            }},
        ]
        (log / "00000000000000000000.json").write_text(
            "".join(json.dumps(action) + "\n" for action in actions), encoding="utf-8",
        )
        predicate = self.cwd / "predicate.json"
        rows = data.to_pylist()
        for op, expected_ids in [
            ("eq", [1]), ("ne", [2]), ("lt", []),
            ("le", [1]), ("gt", [2]), ("ge", [1, 2]),
        ]:
            predicate.write_text(json.dumps({
                "op": op, "column": "value",
                "value": {"type": "float64", "value": 1.9651349465042103},
            }), encoding="utf-8")
            with self.subTest(op=op), self.start_scan(
                table, "--predicate-file", str(predicate),
            ) as process:
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual((process.returncode, stderr), (0, b""))
                self.assertTrue(stdout.endswith(IPC_END))
                actual = pa.ipc.open_stream(stdout).read_all()
                self.assertEqual(actual.sort_by("id").to_pylist(),
                                 [rows[row_id - 1] for row_id in expected_ids])

    def test_empty_table_retains_schema(self):
        table = self.repeated_table(count=0)
        for flags, names in [((), ["id", "value", "label", "region"]), (("--no-columns",), [])]:
            with self.subTest(flags=flags), self.start_scan(table, *flags) as process:
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual((process.returncode, stderr), (0, b""))
                actual = pa.ipc.open_stream(stdout).read_all()
                self.assertEqual(actual.schema.names, names)
                self.assertEqual(actual.num_rows, 0)
                self.assertTrue(stdout.endswith(IPC_END))

    def test_scan_thread_start_failures_do_not_hang(self):
        table = self.repeated_table()
        self.env.update(LD_PRELOAD=str(self.fault_library), RUST_BACKTRACE="full")
        statuses = set()
        # Exercise failures during loading, planning, and starting the output thread.
        for limit in range(8):
            with self.subTest(thread_limit=limit):
                self.env["DAR_TEST_THREAD_LIMIT"] = str(limit)
                with self.start_scan(table) as process:
                    stdout, stderr = process.communicate(timeout=15)
                    statuses.add(process.returncode)
                    if process.returncode == 1:
                        self.assert_diagnostic(stderr, "execution", "runtime_initialization")
                    else:
                        self.assertEqual((process.returncode, stderr), (0, b""))
                        self.assertEqual(pa.ipc.open_stream(stdout).read_all().num_rows, 32 * 120)
        self.assertEqual(statuses, {0, 1})

    def test_blocked_output_bounds_reads_and_resumes_incrementally(self):
        table = self.repeated_table()
        with self.storage(table) as server, self.start_scan(
            server.url, "--storage-options-file", str(self.options),
        ) as process:
            self.wait_for_blocked_output(process)
            with server.lock:
                self.assertGreater(len(server.data_paths), 0)
                self.assertLess(len(server.data_paths), 32, "all files read before output drained")
            reader = self.read_with_timeout(process, lambda: pa.ipc.open_stream(process.stdout))
            first = self.read_with_timeout(process, reader.read_next_batch)
            self.assertGreater(first.num_rows, 0)
            self.assertIsNone(process.poll(), "scan finished before the first batch was consumed")
            remaining = self.read_with_timeout(process, reader.read_all)
            self.assertEqual(first.num_rows + remaining.num_rows, 32 * 120)
            self.assertEqual(process.wait(timeout=15), 0)
            self.assertEqual(process.stderr.read(), b"")
            self.assertEqual(len(server.data_paths), 32)

    def test_slow_consumer_does_not_timeout_pending_reads(self):
        headers_sent = threading.Event()
        release_body = threading.Event()
        body_sent = threading.Event()

        class DelayedBodyStorage(ScanStorage):
            def copyfile(self, source, output):
                if self.path == "/part-003.parquet":
                    headers_sent.set()
                    release_body.wait(15)
                elif (self.path == "/part-002.parquet"
                      and self.headers.get("Range", "").startswith("bytes=4-")):
                    headers_sent.wait(15)
                    # Let the client begin reading the pending response body.
                    time.sleep(0.1)
                with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                    super().copyfile(source, output)
                    if self.path == "/part-003.parquet":
                        body_sent.set()

        table = self.repeated_table(count=8)
        # Exceed the footer read size so data reads overlap with later file setup.
        original = pq.ParquetFile(table / "part-000.parquet").read()
        labels = pa.array([f"{row:04}-" + "x" * 4096 for row in range(original.num_rows)])
        expanded = original.set_column(original.schema.get_field_index("label"), "label", labels)
        pq.write_table(expanded, table / "part-000.parquet", compression=None, use_dictionary=False)
        content = (table / "part-000.parquet").read_bytes()
        log = table / "_delta_log/00000000000000000000.json"
        actions = [json.loads(line) for line in log.read_text().splitlines()]
        for action in actions:
            if "add" in action:
                (table / action["add"]["path"]).write_bytes(content)
                action["add"]["size"] = len(content)
                action["add"].pop("stats", None)
        log.write_text("".join(json.dumps(action) + "\n" for action in actions), encoding="utf-8")
        self.options.write_text('{"allow_http":"true","timeout":"5s"}', encoding="utf-8")
        with self.storage(table, handler=DelayedBodyStorage) as server, self.start_scan(
            server.url, "--storage-options-file", str(self.options),
        ) as process:
            try:
                self.wait_for_blocked_output(process)
                self.assertTrue(headers_sent.wait(3), "prefetch response did not start")
                # Let the current file fill the core's output buffer before releasing setup.
                time.sleep(0.5)
                release_body.set()
                self.assertTrue(body_sent.wait(1), "server did not finish the pending response")
                # Hold stdout past the request timeout, after the server has responded.
                time.sleep(6)
                with server.lock:
                    self.assertLess(len(server.data_paths), 8, "all files read while stdout blocked")
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual((process.returncode, stderr), (0, b""))
                self.assertEqual(pa.ipc.open_stream(stdout).read_all().num_rows, 8 * 120)
                self.assertTrue(stdout.endswith(IPC_END))
            finally:
                release_body.set()

    def test_later_read_failure_leaves_decodable_but_unsuccessful_output(self):
        table = self.repeated_table()
        with self.storage(table, fail_after=4) as server, self.start_scan(
            server.url, "--storage-options-file", str(self.options),
        ) as process:
            stdout, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 1)
            self.assert_diagnostic(stderr, "data_file_read", "data_file_read")
            self.assertFalse(stdout.endswith(IPC_END))
            partial = pa.ipc.open_stream(stdout).read_all()
            self.assertGreater(partial.num_rows, 0)
            self.assertLess(partial.num_rows, 32 * 120)
            # EOF at a batch boundary decodes successfully. The required child
            # status check still rejects this as an incomplete result.
            result = subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)
            with self.assertRaises(subprocess.CalledProcessError):
                result.check_returncode()
            with self.assertRaises((pa.ArrowInvalid, OSError)):
                pa.ipc.open_stream(stdout[:-1]).read_all()

    def test_early_pipe_close_stops_a_blocked_scan(self):
        table = self.repeated_table()
        with self.storage(table) as server, self.start_scan(
            server.url, "--storage-options-file", str(self.options),
        ) as process:
            self.wait_for_blocked_output(process)
            self.assert_no_children(process)
            process.stdout.close()
            self.assertEqual(process.wait(timeout=15), 3)
            self.assert_diagnostic(process.stderr.read(), "execution", "output_write")

    def test_signals_stop_blocked_output(self):
        table = self.repeated_table()
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=signum), self.storage(table) as server, self.start_scan(
                server.url, "--storage-options-file", str(self.options),
            ) as process:
                self.wait_for_blocked_output(process)
                self.assert_no_children(process)
                process.send_signal(signum)
                self.assertEqual(process.wait(timeout=15), -signum)
                self.assertEqual(process.stderr.read(), b"")

    def test_signals_stop_held_data_requests(self):
        table = self.repeated_table()
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=signum), self.storage(table, block_data=True) as server, self.start_scan(
                server.url, "--storage-options-file", str(self.options),
            ) as process:
                self.assertTrue(server.held.wait(10), "data request never started")
                reader = self.read_with_timeout(process, lambda: pa.ipc.open_stream(process.stdout))
                self.assertEqual(reader.schema.names, ["id", "value", "label", "region"])
                self.assert_no_children(process)
                process.send_signal(signum)
                self.assertEqual(process.wait(timeout=15), -signum)
                self.assertEqual(process.stderr.read(), b"")
                self.assertTrue(server.disconnected.wait(5), "terminated process retained a socket")

    def test_scan_exit_does_not_wait_for_blocking_file_prefetch(self):
        table = self.cwd / "prefetch-table"
        shutil.copytree(CORPUS / "partitioned/table", table)
        # The core orders full-column file tasks by size. Hold the second file's
        # blocking open and make the first wait until that prefetch is in flight.
        first, blocked, _ = sorted(table.rglob("*.parquet"), key=lambda path: path.stat().st_size, reverse=True)
        original = first.read_bytes()
        marker = self.cwd / "open-started"
        self.env.update(LD_PRELOAD=str(self.fault_library), DAR_TEST_BLOCKED_OPEN=str(blocked),
                        DAR_TEST_WAITING_OPEN=str(first), DAR_TEST_OPEN_STARTED=str(marker))
        for mode in ("limit", "reader_error", "output_error"):
            with self.subTest(mode=mode):
                marker.unlink(missing_ok=True)
                first.write_bytes(b"x" * len(original) if mode == "reader_error" else original)
                flags = ("--limit", "1") if mode == "limit" else ()
                with self.start_scan(table, *flags) as process:
                    if mode == "output_error":
                        self.wait_for_blocked_output(process)
                        process.stdout.close()
                        self.assertEqual(process.wait(timeout=15), 3)
                        self.assert_diagnostic(process.stderr.read(), "execution", "output_write")
                    else:
                        stdout, stderr = process.communicate(timeout=15)
                        if mode == "limit":
                            self.assertEqual((process.returncode, stderr), (0, b""))
                            self.assertEqual(pa.ipc.open_stream(stdout).read_all().num_rows, 1)
                        else:
                            self.assertEqual(process.returncode, 1)
                            self.assert_diagnostic(stderr, "data_file_read", "data_file_read")
                            self.assertFalse(stdout.endswith(IPC_END))
                    self.assertTrue(marker.exists(), "unused blocking prefetch never started")


if __name__ == "__main__":
    unittest.main()
