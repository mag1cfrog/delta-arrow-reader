from collections import UserDict
from concurrent.futures import ThreadPoolExecutor
import ctypes
from decimal import Decimal
from functools import partial
import gc
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from threading import Barrier, Event
from types import MappingProxyType
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from delta_arrow_reader import (
    DeltaReaderError, DeltaTable, RecordBatchStream, ScanExecutionOptions,
)


class TableTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="secret-table-")
        self.addCleanup(temporary.cleanup)
        self.location = Path(temporary.name)
        self.log = self.location / "_delta_log"
        self.log.mkdir()
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": False, "metadata": {}}
            ],
        }
        self.metadata = {
            "id": "python-test-table",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps(schema),
            "partitionColumns": [],
            "configuration": {},
        }
        self.write_log(
            0,
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {"metaData": self.metadata},
        )

    def write_log(self, version, *actions):
        (self.log / f"{version:020}.json").write_text(
            "".join(json.dumps(action) + "\n" for action in actions), encoding="utf-8"
        )

    def write_parquet(self, name, values, **options):
        path = self.location / name
        schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
        pq.write_table(pa.table({"id": values}, schema=schema), path, **options)
        return {"add": {
            "path": name, "partitionValues": {}, "size": path.stat().st_size,
            "modificationTime": 0, "dataChange": True,
        }}

    def http_support(self):
        support = (Path(__file__).resolve().parents[3] / "tests/reader/https.py")
        spec = importlib.util.spec_from_file_location("reader_https", support)
        http = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(http)
        return http

    def test_reader_and_batches_outlive_table(self):
        self.write_log(
            1, self.write_parquet("first.parquet", [1, 2]),
            self.write_parquet("second.parquet", [3, 4]),
        )
        table = DeltaTable(self.location)
        reader = table.to_reader()
        other = table.to_reader()
        schema = table.schema
        self.assertIsInstance(reader, pa.RecordBatchReader)
        del table
        gc.collect()

        self.assertEqual(reader.schema, schema)
        batch = reader.read_next_batch()
        self.assertEqual(batch.column(0).to_pylist(), [1, 2])
        self.assertEqual(reader.read_all().to_pydict(), {"id": [3, 4]})
        with self.assertRaises(StopIteration):
            reader.read_next_batch()
        reader.close()
        self.assertEqual(other.read_all().to_pydict(), {"id": [1, 2, 3, 4]})
        other.close()
        del reader, other
        gc.collect()
        self.assertEqual(batch.column(0).to_pylist(), [1, 2])

    def test_arrow_handoff_shares_primitive_buffer_after_reader_release(self):
        # Public Arrow C Data / C Stream layouts, used only to observe the handoff.
        class ArrowArray(ctypes.Structure):
            pass

        ArrowArray._fields_ = [
            ("length", ctypes.c_int64),
            ("null_count", ctypes.c_int64),
            ("offset", ctypes.c_int64),
            ("n_buffers", ctypes.c_int64),
            ("n_children", ctypes.c_int64),
            ("buffers", ctypes.POINTER(ctypes.c_void_p)),
            ("children", ctypes.POINTER(ctypes.POINTER(ArrowArray))),
            ("dictionary", ctypes.POINTER(ArrowArray)),
            ("release", ctypes.c_void_p),
            ("private_data", ctypes.c_void_p),
        ]

        class ArrowArrayStream(ctypes.Structure):
            _fields_ = [
                ("get_schema", ctypes.c_void_p),
                ("get_next", ctypes.c_void_p),
                ("get_last_error", ctypes.c_void_p),
                ("release", ctypes.c_void_p),
                ("private_data", ctypes.c_void_p),
            ]

        self.write_log(1, self.write_parquet("rows.parquet", [1, 2, 3]))
        stream = DeltaTable(self.location).scan()
        capsule = stream.__arrow_c_stream__()
        del stream
        get_pointer = ctypes.pythonapi["PyCapsule_GetPointer"]
        get_pointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
        get_pointer.restype = ctypes.POINTER(ArrowArrayStream)
        native = get_pointer(capsule, b"arrow_array_stream")
        get_next_type = ctypes.CFUNCTYPE(
            ctypes.c_int, ctypes.c_void_p, ctypes.POINTER(ArrowArray),
        )
        original_next = get_next_type(native.contents.get_next)
        addresses = []

        @get_next_type
        def observe_next(native_stream, output):
            status = original_next(native_stream, output)
            if status == 0 and output.contents.release:
                addresses.append(output.contents.children[0].contents.buffers[1])
            return status

        # Keep the observer alive until PyArrow closes the reader. The native
        # release callbacks retain ownership of the stream and its arrays.
        native.contents.get_next = ctypes.cast(observe_next, ctypes.c_void_p).value

        class ExportedStream:
            def __arrow_c_stream__(self, requested_schema=None):
                return capsule

        with pa.RecordBatchReader.from_stream(ExportedStream()) as reader:
            batch = reader.read_next_batch()
            self.assertEqual(addresses, [batch.column(0).buffers()[1].address])
        del reader, capsule, native
        gc.collect()
        self.assertEqual(batch.column(0).buffers()[1].address, addresses[0])
        self.assertEqual(batch.column(0).to_pylist(), [1, 2, 3])

    def test_reader_errors_stay_terminal_and_redacted(self):
        self.write_log(1, self.write_parquet("secret-data.parquet", [1, 2]))
        (self.location / "secret-data.parquet").unlink()
        # Planning can finish even though the data file is already missing.
        reader = DeltaTable(self.location).to_reader()
        try:
            for _ in range(2):
                with self.assertRaises(pa.ArrowInvalid) as caught:
                    reader.read_next_batch()
                self.assertIn("phase=data_file_read code=data_file_read", str(caught.exception))
                self.assertNotIn("secret", str(caught.exception))
        finally:
            reader.close()

    def test_unused_streams_and_zero_limit_do_not_read_parquet(self):
        http = self.http_support()
        data_requested = Event()

        class Storage(http.Storage):
            def send_head(self):
                if self.path.endswith(".parquet"):
                    data_requested.set()
                return super().send_head()

        self.write_log(1, self.write_parquet("rows.parquet", [1, 2]))
        with http.serve(partial(Storage, directory=str(self.location))) as server:
            table = DeltaTable(
                f"http://127.0.0.1:{server.server_port}/",
                storage_options={"allow_http": "true"},
            )
            self.assertTrue(server.requests, "snapshot loading must contact the server")
            # Give background work time to reach the server if laziness regresses.
            with table.scan():
                self.assertFalse(data_requested.wait(0.1), server.requests)
            unused = table.scan()
            del unused
            capsule = table.scan().__arrow_c_stream__()
            self.assertFalse(data_requested.wait(0.1), server.requests)
            del capsule
            gc.collect()
            with table.to_reader() as reader:
                self.assertEqual(reader.schema, table.schema)
                self.assertFalse(data_requested.wait(0.1), server.requests)
            with table.to_reader(limit=0) as reader:
                self.assertEqual(reader.read_all().num_rows, 0)
            self.assertFalse(data_requested.wait(0.1), server.requests)
            self.assertFalse(any(path.endswith(".parquet") for path, _, _ in server.requests))

            with table.to_reader() as reader:
                batch = reader.read_next_batch()
            self.assertEqual(batch.column(0).to_pylist(), [1, 2])
            self.assertTrue(data_requested.is_set())
            self.assertTrue(any(path.endswith(".parquet") for path, _, _ in server.requests))

    def test_reader_failure_cancels_pending_io(self):
        http = self.http_support()
        self.write_log(
            1, self.write_parquet("secret-failed.parquet", [1, 2]),
            self.write_parquet("secret-pending.parquet", [3, 4]),
        )
        pending = Event()
        disconnected = Event()
        failed_with_pending_read = Event()
        requests = []

        class Storage(http.Storage):
            def do_GET(self):
                if not self.path.endswith(".parquet"):
                    return super().do_GET()
                requests.append(self.path)
                if self.path == "/secret-failed.parquet":
                    if pending.wait(10):
                        failed_with_pending_read.set()
                    self.send_error(400, "secret-response-message")
                    return
                pending.set()
                self.wait_for_disconnect(disconnected)

        with http.serve(partial(Storage, directory=str(self.location))) as server:
            table = DeltaTable(
                f"http://127.0.0.1:{server.server_port}/",
                storage_options={"allow_http": "true"},
            )
            with table.to_reader() as reader:
                for _ in range(2):
                    with self.assertRaises(pa.ArrowInvalid) as caught:
                        reader.read_next_batch()
                    self.assertIn("phase=data_file_read code=data_file_read", str(caught.exception))
                    self.assertNotIn("secret", str(caught.exception))
                self.assertTrue(failed_with_pending_read.is_set(), "failure preceded the pending read")
                # Observe cancellation while both the failed reader and table live.
                self.assertTrue(disconnected.wait(10), "failure left the HTTP read pending")
                self.assertCountEqual(requests, ["/secret-failed.parquet", "/secret-pending.parquet"])

    def test_paused_reader_bounds_reads_and_close_cancels_pending_io(self):
        http = self.http_support()
        # One 1024-row batch per row group makes read-ahead visible as HTTP requests.
        self.write_log(1, self.write_parquet(
            "rows.parquet", range(32 * 1024), row_group_size=1024,
            compression="NONE", use_dictionary=False,
        ))
        metadata = pq.read_metadata(self.location / "rows.parquet")
        ranges = {}
        for index in range(metadata.num_row_groups):
            column = metadata.row_group(index).column(0)
            start = column.data_page_offset
            end = start + column.total_compressed_size - 1
            ranges[f"bytes={start}-{end}"] = index
        prepared = Event()
        pending = Event()
        disconnected = Event()
        unexpected = Event()
        groups = []

        class Storage(http.Storage):
            def do_GET(self):
                group = ranges.get(self.headers.get("Range")) if self.path.endswith(".parquet") else None
                if group is not None:
                    groups.append(group)
                    if group == blocked_group:
                        pending.set()
                        self.wait_for_disconnect(disconnected)
                        return
                    if group > blocked_group:
                        unexpected.set()
                super().do_GET()
                if group is not None and group == blocked_group - 1:
                    prepared.set()

        with http.serve(partial(Storage, directory=str(self.location))) as server:
            table = DeltaTable(
                f"http://127.0.0.1:{server.server_port}/",
                storage_options={"allow_http": "true"},
            )
            override = ScanExecutionOptions(output_buffer_batches_per_partition=4)
            for options, capacity in ((None, 1), (override, 4), (None, 1)):
                with self.subTest(capacity=capacity):
                    for event in (prepared, pending, disconnected, unexpected):
                        event.clear()
                    groups.clear()
                    # After the first batch, the core queues capacity batches
                    # and prepares one more before waiting for the consumer.
                    blocked_group = capacity + 2
                    with table.to_reader(execution_options=options) as reader:
                        first = reader.read_next_batch()
                        self.assertEqual(first.column(0).to_pylist(), list(range(1024)))
                        self.assertTrue(prepared.wait(10), groups)
                        self.assertFalse(pending.wait(0.2), groups)
                        self.assertEqual(groups, list(range(blocked_group)))
                        second = reader.read_next_batch()
                        self.assertEqual(second.column(0).to_pylist(), list(range(1024, 2048)))
                        self.assertTrue(pending.wait(10), groups)
                        reader.close()
                        self.assertTrue(disconnected.wait(10), "close left the HTTP read pending")
                        self.assertFalse(unexpected.wait(0.2), groups)
                        self.assertEqual(groups, list(range(blocked_group + 1)))

    def test_empty_reader_preserves_schema(self):
        table = DeltaTable(self.location)
        for columns in (None, []):
            with self.subTest(columns=columns):
                with table.to_reader(columns=columns) as reader:
                    empty = reader.read_all()
                self.assertEqual(empty.schema, table.schema if columns is None else pa.schema([]))
                self.assertEqual(empty.num_rows, 0)

    def test_external_writer_fixtures_match_spark(self):
        corpus = (Path(__file__).resolve().parents[3]
                  / "tests/reader/fixtures/external_writer/corpus")
        manifest = json.loads((corpus / "manifest.json").read_text(encoding="utf-8"))
        for fixture in manifest["fixtures"]:
            with self.subTest(fixture=fixture["name"]):
                directory = corpus / fixture["name"]
                with pa.ipc.open_file(directory / fixture["expected_schema_and_rows"]) as source:
                    expected = source.read_all()
                table = DeltaTable(directory / "table")
                schema = table.schema
                self.assertEqual(table.version, fixture["snapshot_version"])
                # Spark's oracle omits the Delta column-mapping metadata.
                self.assertTrue(schema.equals(expected.schema, check_metadata=False))
                for method in ("scan", "to_reader"):
                    for columns in (None, expected.schema.names[::-1], []):
                        with self.subTest(method=method, columns=columns):
                            selected = expected if columns is None else expected.select(columns)
                            projected_schema = schema if columns is None else pa.schema(
                                [schema.field(name) for name in columns], metadata=schema.metadata,
                            )
                            result = getattr(table, method)(columns=columns)
                            reader = (pa.RecordBatchReader.from_stream(result, schema=projected_schema)
                                      if method == "scan" else result)
                            with reader:
                                actual = reader.read_all()
                            self.assertTrue(actual.schema.equals(projected_schema, check_metadata=True))
                            self.assertEqual(actual.num_rows, fixture["expected_row_count"])
                            if actual.num_columns:
                                actual = actual.sort_by([("id", "ascending")])
                            self.assertTrue(actual.equals(selected, check_metadata=False))

    def test_projection_and_limits_preserve_order_values_and_row_counts(self):
        fixture = (Path(__file__).resolve().parents[3]
                   / "tests/reader/fixtures/external_writer/corpus/partitioned/table")
        table = DeltaTable(fixture)
        with table.to_reader() as reader:
            full = reader.read_all()
        self.assertEqual(full.num_rows, 360)
        for method in ("scan", "to_reader"):
            for columns in (None, ["region", "id"], ("label", "id"), ["region"], [], ()):
                for limit in (None, 0, 1, 100, 101, 121, 360, 361, 2 * sys.maxsize + 1):
                    with self.subTest(method=method, columns=columns, limit=limit):
                        expected = full if columns is None else full.select(columns)
                        if limit is not None:
                            expected = expected.slice(0, min(limit, full.num_rows))
                        result = getattr(table, method)(columns=columns, limit=limit)
                        reader = (pa.RecordBatchReader.from_stream(result)
                                  if method == "scan" else result)
                        with reader:
                            batches = list(reader)
                            actual = pa.Table.from_batches(batches, schema=reader.schema)
                        self.assertTrue(actual.equals(expected, check_metadata=True))
                        self.assertEqual(actual.num_rows, expected.num_rows)
                        if columns is not None and not columns and expected.num_rows:
                            self.assertTrue(batches)
                            for batch in batches:
                                self.assertEqual(batch.num_columns, 0)
                                self.assertGreater(batch.num_rows, 0)

    def test_zero_limit_preserves_schema_without_reading_data_files(self):
        self.write_log(1, self.write_parquet("rows.parquet", [1, 2]))
        (self.location / "rows.parquet").unlink()
        table = DeltaTable(self.location)
        for method in ("scan", "to_reader"):
            for columns in (None, []):
                with self.subTest(method=method, columns=columns):
                    result = getattr(table, method)(columns=columns, limit=0)
                    reader = (pa.RecordBatchReader.from_stream(result)
                              if method == "scan" else result)
                    with reader:
                        self.assertEqual(reader.schema, table.schema if columns is None else pa.schema([]))
                        self.assertEqual(list(reader), [])

    def test_limit_validation_is_shared_by_both_entrypoints(self):
        class Limit(int):
            def __lt__(self, other):
                return False

            def __index__(self):
                return 123

        self.write_log(1, self.write_parquet("rows.parquet", [1, 2]))
        table = DeltaTable(self.location)
        for method in ("scan", "to_reader"):
            with self.subTest(method=method):
                result = getattr(table, method)(limit=Limit(1))
                reader = (pa.RecordBatchReader.from_stream(result)
                          if method == "scan" else result)
                with reader:
                    self.assertEqual(reader.read_all().to_pydict(), {"id": [1]})
                for limit, error_type in (
                    (True, TypeError), (False, TypeError), (1.0, TypeError),
                    ("1", TypeError), (b"1", TypeError), (object(), TypeError),
                    (-1, ValueError), (-2**100, ValueError), (Limit(-1), ValueError),
                    (2 * sys.maxsize + 2, OverflowError), (2**100, OverflowError),
                ):
                    with self.subTest(limit=limit), self.assertRaises(error_type):
                        getattr(table, method)(limit=limit)
                with self.assertRaises(TypeError):
                    getattr(table, method)(None, 1)

    def test_partition_targets_preserve_rows_projection_and_limits(self):
        self.write_log(
            1, self.write_parquet("first.parquet", [1, 2]),
            self.write_parquet("second.parquet", [3, 4]),
            self.write_parquet("third.parquet", [5, 6]),
        )
        table = DeltaTable(self.location)
        for method in ("scan", "to_reader"):
            for target in (None, 1, 2, 8, 2 * sys.maxsize + 1):
                for columns, limit in ((["id"], None), (["id"], 3), ([], 3)):
                    with self.subTest(method=method, target=target, columns=columns, limit=limit):
                        result = getattr(table, method)(
                            columns=columns, limit=limit, target_partitions=target,
                        )
                        reader = (pa.RecordBatchReader.from_stream(result)
                                  if method == "scan" else result)
                        with reader:
                            actual = reader.read_all()
                        self.assertEqual(actual.num_rows, 6 if limit is None else limit)
                        self.assertEqual(actual.schema, table.schema if columns else pa.schema([]))
                        if columns:
                            values = actual.column("id").to_pylist()
                            if limit is None:
                                self.assertCountEqual(values, [1, 2, 3, 4, 5, 6])
                            else:
                                self.assertEqual(len(set(values)), limit)
                                self.assertTrue(set(values) <= {1, 2, 3, 4, 5, 6})

    def test_target_partitions_validation_is_shared_by_both_entrypoints(self):
        class Partitions(int):
            def __le__(self, other):
                return False

            def __index__(self):
                return 123

        table = DeltaTable(self.location)
        for method in (table.scan, table.to_reader):
            with self.subTest(method=method.__name__):
                method(target_partitions=Partitions(1)).close()
                for target, error_type in (
                    (True, TypeError), (False, TypeError), (1.0, TypeError),
                    ("1", TypeError), (b"1", TypeError), (object(), TypeError),
                    (0, ValueError), (Partitions(0), ValueError), (-1, ValueError),
                    (-2**100, ValueError), (Partitions(-1), ValueError),
                    (2 * sys.maxsize + 2, OverflowError), (2**100, OverflowError),
                ):
                    with self.subTest(target=target), self.assertRaises(error_type):
                        method(target_partitions=target)
                with self.assertRaises(TypeError):
                    method(None, None, 1)

    def test_scan_execution_options_are_immutable_and_validate_backends(self):
        self.assertEqual(ScanExecutionOptions().parquet_backend, "direct")
        for backend in ("direct", "delta_kernel"):
            with self.subTest(backend=backend):
                options = ScanExecutionOptions(parquet_backend=backend)
                self.assertEqual(options.parquet_backend, backend)
                with self.assertRaises(AttributeError):
                    options.parquet_backend = "direct"
                with self.assertRaises(AttributeError):
                    del options.parquet_backend
                with self.assertRaises(AttributeError):
                    options.extra = True
        for value in (None, True, 1, 0.0, b"direct", [], {}, object()):
            with self.subTest(value=value), self.assertRaises(TypeError):
                ScanExecutionOptions(parquet_backend=value)
        for value in ("", "DIRECT", "delta-kernel", "secret-invalid"):
            with self.subTest(value=value), self.assertRaises(ValueError) as caught:
                ScanExecutionOptions(parquet_backend=value)
            self.assertNotIn("secret", str(caught.exception))
        with self.assertRaises(TypeError):
            ScanExecutionOptions("direct")

    def test_execution_options_require_a_scan_execution_options_object(self):
        table = DeltaTable(self.location, execution_options=None)
        for method in (partial(DeltaTable, self.location / "missing"),
                       table.scan, table.to_reader):
            for value in ({}, {"parquet_backend": "direct"}, "direct", True, 1, []):
                with self.subTest(method=method, value=value), self.assertRaises(TypeError):
                    method(execution_options=value)

    def test_execution_capacity_defaults_and_validation(self):
        class Count(int):
            def __lt__(self, other):
                return False

            def __le__(self, other):
                return False

            def __index__(self):
                return 1

        maximum = sys.maxsize >> 2
        for name, default in (
            ("max_concurrent_file_reads_per_partition", 3),
            ("max_concurrent_file_reads_per_scan", None),
            ("output_buffer_batches_per_partition", 1),
        ):
            self.assertEqual(getattr(ScanExecutionOptions(), name), default)
            if default is not None:
                with self.subTest(name=name), self.assertRaises(TypeError):
                    ScanExecutionOptions(**{name: None})
            for count in (1, 2, Count(2), maximum, default):
                with self.subTest(name=name, count=count):
                    options = ScanExecutionOptions(**{name: count})
                    self.assertEqual(getattr(options, name), count)
                    with self.assertRaises(AttributeError):
                        setattr(options, name, 1)
            for count, error_type in (
                (True, TypeError), (False, TypeError),
                (1.0, TypeError), ("1", TypeError), (b"1", TypeError),
                ([], TypeError), ({}, TypeError), (object(), TypeError),
                (0, ValueError), (Count(0), ValueError), (-1, ValueError),
                (-2**100, ValueError), (Count(-1), ValueError),
                (maximum + 1, ValueError), (2 * sys.maxsize + 1, ValueError),
                (2 * sys.maxsize + 2, OverflowError), (2**100, OverflowError),
            ):
                with self.subTest(name=name, count=count), self.assertRaises(error_type):
                    ScanExecutionOptions(**{name: count})

    def test_prefetch_depth_defaults_and_validation(self):
        class Count(int):
            def __lt__(self, other):
                return False

            def __index__(self):
                return 1

        self.assertEqual(ScanExecutionOptions().prefetch_files_per_partition, 2)
        for count in (0, 1, 2, Count(0), Count(2), 2 * sys.maxsize + 1):
            with self.subTest(count=count):
                options = ScanExecutionOptions(prefetch_files_per_partition=count)
                self.assertEqual(options.prefetch_files_per_partition, count)
                with self.assertRaises(AttributeError):
                    options.prefetch_files_per_partition = 0
        for count, error_type in (
            (None, TypeError), (True, TypeError), (False, TypeError),
            (1.0, TypeError), ("1", TypeError), (b"1", TypeError),
            ([], TypeError), ({}, TypeError), (object(), TypeError),
            (-1, ValueError), (-2**100, ValueError), (Count(-1), ValueError),
            (2 * sys.maxsize + 2, OverflowError), (2**100, OverflowError),
        ):
            with self.subTest(count=count), self.assertRaises(error_type):
                ScanExecutionOptions(prefetch_files_per_partition=count)

    def test_metadata_hint_defaults_and_validation(self):
        class Size(int):
            def __lt__(self, other):
                return False

            def __index__(self):
                return 1

        self.assertEqual(ScanExecutionOptions().parquet_metadata_size_hint_bytes, 65536)
        for size in (None, 1, 65536, Size(2), 2 * sys.maxsize + 1):
            with self.subTest(size=size):
                options = ScanExecutionOptions(parquet_metadata_size_hint_bytes=size)
                self.assertEqual(options.parquet_metadata_size_hint_bytes, size)
                with self.assertRaises(AttributeError):
                    options.parquet_metadata_size_hint_bytes = None
        for size, error_type in (
            (True, TypeError), (False, TypeError), (1.0, TypeError),
            ("1", TypeError), (b"1", TypeError), ([], TypeError),
            ({}, TypeError), (object(), TypeError), (0, ValueError),
            (Size(0), ValueError), (-1, ValueError), (-2**100, ValueError),
            (Size(-1), ValueError), (2 * sys.maxsize + 2, OverflowError),
            (2**100, OverflowError),
        ):
            with self.subTest(size=size), self.assertRaises(error_type):
                ScanExecutionOptions(parquet_metadata_size_hint_bytes=size)

    def test_metadata_hint_controls_requests_and_preserves_table_defaults(self):
        http = self.http_support()
        action = self.write_parquet("rows.parquet", [1, 2, 3])
        self.write_log(1, action)
        size = action["add"]["size"]
        with http.serve(partial(http.Storage, directory=str(self.location))) as server:
            for backend in ("direct", "delta_kernel"):
                no_hint = ScanExecutionOptions(
                    parquet_backend=backend, parquet_metadata_size_hint_bytes=None,
                )
                table = DeltaTable(
                    f"http://127.0.0.1:{server.server_port}/",
                    storage_options={"allow_http": "true"}, execution_options=no_hint,
                )
                overrides = [
                    (None, 8),
                    (ScanExecutionOptions(parquet_backend=backend), size),
                    (no_hint, 8),
                ]
                for hint, first_bytes in ((1, 8), (64, 64), (2 * sys.maxsize + 1, size)):
                    overrides.append((ScanExecutionOptions(
                        parquet_backend=backend, parquet_metadata_size_hint_bytes=hint,
                    ), first_bytes))
                overrides.append((None, 8))
                for method in ("scan", "to_reader"):
                    for options, first_bytes in overrides:
                        with self.subTest(backend=backend, method=method, options=options):
                            server.requests.clear()
                            result = getattr(table, method)(execution_options=options)
                            reader = (pa.RecordBatchReader.from_stream(result)
                                      if method == "scan" else result)
                            with reader:
                                self.assertEqual(reader.read_all().to_pydict(), {"id": [1, 2, 3]})
                            ranges = [r for path, r, _ in server.requests
                                      if path.endswith(".parquet")]
                            # Parquet always starts with at least its 8-byte footer.
                            expected_bytes = 8 if backend == "delta_kernel" else first_bytes
                            self.assertEqual(ranges[0], f"bytes={size - expected_bytes}-{size - 1}")

    def test_execution_options_bound_file_admission(self):
        http = self.http_support()
        self.write_log(1, *(self.write_parquet(f"{i}.parquet", [i]) for i in range(4)))
        requested = set()
        admitted, exceeded, release = Event(), Event(), Event()

        class Storage(http.Storage):
            def send_head(self):
                if self.path.endswith(".parquet"):
                    requested.add(self.path)
                    if len(requested) >= expected_reads:
                        admitted.set()
                    if len(requested) > expected_reads:
                        exceeded.set()
                    if not release.wait(10):
                        self.send_error(400, "test did not release the file reads")
                        return None
                return super().send_head()

        with http.serve(partial(Storage, directory=str(self.location))) as server:
            for backend in ("direct", "delta_kernel"):
                for name, target, table_count, override_count in (
                    ("max_concurrent_file_reads_per_partition", 1, 1, 2),
                    ("max_concurrent_file_reads_per_scan", 2, 1, 2),
                    ("prefetch_files_per_partition", 1, 0, 1),
                ):
                    table = DeltaTable(
                        f"http://127.0.0.1:{server.server_port}/",
                        storage_options={"allow_http": "true"},
                        execution_options=ScanExecutionOptions(parquet_backend=backend, **{name: table_count}),
                    )
                    override = ScanExecutionOptions(parquet_backend=backend, **{name: override_count})
                    # Kernel reads files serially within each partition.
                    override_reads = 2 if backend == "direct" or target == 2 else 1
                    default_reads = min(4, 3 * target)
                    cases = [(None, 1), (override, override_reads), (None, 1),
                             (ScanExecutionOptions(), default_reads)]
                    if name == "max_concurrent_file_reads_per_scan":
                        cases.append((ScanExecutionOptions(
                            parquet_backend=backend, max_concurrent_file_reads_per_scan=None,
                        ), 4 if backend == "direct" else 2))
                    if name == "prefetch_files_per_partition":
                        cases.append((ScanExecutionOptions(
                            parquet_backend=backend, prefetch_files_per_partition=2 * sys.maxsize + 1,
                        ), 3 if backend == "direct" else 1))
                    for options, expected_reads in cases:
                        with self.subTest(backend=backend, name=name, expected_reads=expected_reads):
                            requested.clear()
                            for event in (admitted, exceeded, release):
                                event.clear()
                            with table.to_reader(target_partitions=target, execution_options=options) as reader:
                                with ThreadPoolExecutor(max_workers=1) as executor:
                                    result = executor.submit(reader.read_all)
                                    try:
                                        self.assertTrue(admitted.wait(10), requested)
                                        self.assertFalse(exceeded.wait(0.1), requested)
                                    finally:
                                        release.set()
                                    self.assertCountEqual(
                                        result.result(timeout=10).column("id").to_pylist(), range(4),
                                    )

    def test_backend_overrides_preserve_table_defaults_and_refresh(self):
        http = self.http_support()
        action = self.write_parquet("rows.parquet", [1, 2, 3])
        self.write_log(1, action)
        size = action["add"]["size"]
        # Observe the backend through its first metadata request: the direct
        # reader prefetches this small file, while Kernel starts with its footer.
        first_range = {
            "direct": f"bytes=0-{size - 1}",
            "delta_kernel": f"bytes={size - 8}-{size - 1}",
        }
        options = {
            backend: ScanExecutionOptions(parquet_backend=backend)
            for backend in first_range
        }
        with http.serve(partial(http.Storage, directory=str(self.location))) as server:
            for backend in options:
                original = DeltaTable(
                    f"http://127.0.0.1:{server.server_port}/",
                    storage_options={"allow_http": "true"},
                    execution_options=options[backend],
                )
                other = "delta_kernel" if backend == "direct" else "direct"
                for table in (original, original.refresh()):
                    for method in ("scan", "to_reader"):
                        for override, expected_backend in (
                            ({}, backend),
                            ({"execution_options": options[other]}, other),
                            ({"execution_options": None}, backend),
                            ({"execution_options": ScanExecutionOptions()}, "direct"),
                            ({}, backend),
                        ):
                            with self.subTest(backend=backend, method=method, override=override):
                                server.requests.clear()
                                result = getattr(table, method)(
                                    columns=["id"], limit=2, target_partitions=1, **override,
                                )
                                reader = (pa.RecordBatchReader.from_stream(result)
                                          if method == "scan" else result)
                                with reader:
                                    self.assertEqual(reader.read_all().to_pydict(), {"id": [1, 2]})
                                ranges = [r for path, r, _ in server.requests
                                          if path.endswith(".parquet")]
                                self.assertEqual(ranges[0], first_range[expected_backend])

    def test_projection_validation_is_shared_by_both_entrypoints(self):
        table = DeltaTable(self.location)
        for method in (table.scan, table.to_reader):
            with self.subTest(method=method.__name__):
                for columns in ("id", b"id", {"id"}, {"id": 1}, iter(["id"]), 1, True,
                                [1], [None], [b"id"], ["id", object()]):
                    with self.subTest(columns=columns), self.assertRaises(TypeError):
                        method(columns=columns)
                with self.assertRaises(TypeError):
                    method(["id"])
                for columns, reason in ((["secret-column"], "column_not_found"),
                                        (["id", "id"], "duplicate_column")):
                    with self.subTest(columns=columns), self.assertRaises(DeltaReaderError) as caught:
                        method(columns=columns)
                    self.assertEqual(caught.exception.phase, "scan_planning")
                    self.assertEqual(caught.exception.code, "invalid_projection")
                    self.assertIn(f"reason={reason}", str(caught.exception))
                    self.assertNotIn("secret", str(caught.exception))

    def test_stream_exports_once_and_consumer_survives_close(self):
        with self.assertRaises(TypeError):
            RecordBatchStream()
        self.write_log(1, self.write_parquet("rows.parquet", [1, 2]))
        stream = DeltaTable(self.location).scan()
        self.assertIsInstance(stream, RecordBatchStream)
        with pa.RecordBatchReader.from_stream(stream) as reader:
            with self.assertRaises(RuntimeError):
                stream.__arrow_c_stream__()
            stream.close()
            stream.close()
            del stream
            gc.collect()
            self.assertEqual(reader.read_all().to_pydict(), {"id": [1, 2]})

    def test_stream_context_closes_without_suppressing_errors(self):
        table = DeltaTable(self.location)
        for error in (None, ValueError("consumer failed")):
            with self.subTest(error=error):
                stream = table.scan()
                try:
                    with stream as entered:
                        self.assertIs(entered, stream)
                        if error is not None:
                            raise error
                except ValueError as caught:
                    self.assertIs(caught, error)
                else:
                    self.assertIsNone(error)
                stream.close()
                with self.assertRaises(RuntimeError):
                    stream.__arrow_c_stream__()

    def test_stream_capsule_outlives_exporter(self):
        self.write_log(1, self.write_parquet("rows.parquet", [1, 2]))
        stream = DeltaTable(self.location).scan()
        capsule = stream.__arrow_c_stream__()
        del stream
        gc.collect()

        class ExportedStream:
            def __arrow_c_stream__(self, requested_schema=None):
                return capsule

        with pa.RecordBatchReader.from_stream(ExportedStream()) as reader:
            del capsule
            gc.collect()
            batch = reader.read_next_batch()
        del reader
        gc.collect()
        self.assertEqual(batch.column(0).to_pylist(), [1, 2])

    def test_concurrent_stream_export_has_one_owner(self):
        stream = DeltaTable(self.location).scan()
        ready = Barrier(2)

        def export():
            ready.wait(timeout=5)
            try:
                return stream.__arrow_c_stream__()
            except RuntimeError:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(export) for _ in range(2)]
            results = [future.result(timeout=10) for future in futures]
        self.assertEqual(sum(result is not None for result in results), 1)
        # The unconsumed capsule owns cleanup even if the exporter is closed.
        stream.close()
        del futures, results, stream
        gc.collect()

    def test_requested_schema_is_borrowed_and_export_stays_single_use(self):
        self.write_log(1, self.write_parquet("rows.parquet", [1, 2]))
        table = DeltaTable(self.location)
        requested = table.__arrow_c_schema__()
        stream = table.scan()
        capsule = stream.__arrow_c_stream__(requested)
        with self.assertRaises(RuntimeError):
            stream.__arrow_c_stream__(requested)

        class ExportedSchema:
            def __arrow_c_schema__(self):
                return requested

        # Export borrows the request; its owner can still consume the capsule.
        self.assertEqual(pa.schema(ExportedSchema()), table.schema)

        class ExportedStream:
            def __arrow_c_stream__(self, requested_schema=None):
                return capsule

        with pa.RecordBatchReader.from_stream(ExportedStream()) as reader:
            self.assertEqual(reader.read_all().to_pydict(), {"id": [1, 2]})
        closed = table.scan()
        closed.close()
        with self.assertRaises(RuntimeError):
            closed.__arrow_c_stream__(table.__arrow_c_schema__())

    def test_invalid_schema_requests_leave_stream_available(self):
        table = DeltaTable(self.location)
        released = table.__arrow_c_schema__()

        class ExportedSchema:
            def __arrow_c_schema__(self):
                return released

        pa.schema(ExportedSchema())
        wrong_kind = pa.array([1]).__arrow_c_array__()[1]
        cases = [
            (object(), TypeError),
            (table.schema, TypeError),
            (wrong_kind, ValueError),
            (released, ValueError),
            (pa.int64().__arrow_c_schema__(), ValueError),
        ]
        incompatible = [
            pa.schema([]),
            pa.schema([pa.field("secret-field", pa.int64(), nullable=False)]),
            pa.schema([pa.field("id", pa.int64(), nullable=True)]),
            table.schema.with_metadata({b"secret": b"value"}),
            table.schema.with_metadata({b"secret": b"\xff"}),
            pa.schema([table.schema.field(0).with_metadata({b"secret": b"value"})]),
        ]
        cases.extend((schema.__arrow_c_schema__(), ValueError) for schema in incompatible)
        for requested, error_type in cases:
            with self.subTest(requested=requested), table.scan() as stream:
                with self.assertRaises(error_type) as caught:
                    stream.__arrow_c_stream__(requested)
                self.assertNotIn("secret", str(caught.exception))
                with pa.RecordBatchReader.from_stream(stream, schema=table.schema) as reader:
                    self.assertEqual(reader.schema, table.schema)

    def test_schema_requests_distinguish_representations_from_incompatible_types(self):
        array = {"type": "array", "elementType": "string", "containsNull": True}
        nested = {"type": "struct", "fields": [
            {"name": "child", "type": "long", "nullable": True, "metadata": {}},
        ]}
        mapping = {
            "type": "map", "keyType": "string", "valueType": "long",
            "valueContainsNull": True,
        }
        cases = [
            ("long", pa.int32(), NotImplementedError),
            ("long", pa.uint64(), NotImplementedError),
            ("long", pa.dictionary(pa.int8(), pa.int64()), NotImplementedError),
            ("long", pa.run_end_encoded(pa.int16(), pa.int64()), NotImplementedError),
            ("long", pa.string(), ValueError),
            ("string", pa.large_string(), NotImplementedError),
            ("string", pa.string_view(), NotImplementedError),
            ("binary", pa.large_binary(), NotImplementedError),
            ("float", pa.float64(), NotImplementedError),
            ("decimal(10,2)", pa.decimal256(10, 2), NotImplementedError),
            ("decimal(10,2)", pa.decimal128(11, 2), ValueError),
            ("timestamp", pa.timestamp("ns", tz="UTC"), NotImplementedError),
            ("timestamp", pa.timestamp("us"), ValueError),
            ("date", pa.date64(), NotImplementedError),
            (array, pa.large_list(pa.field("element", pa.large_string())), NotImplementedError),
            (array, pa.list_(pa.field("secret-child", pa.string())), ValueError),
            (nested, pa.struct([pa.field("child", pa.int32())]), NotImplementedError),
            (nested, pa.struct([pa.field("secret-child", pa.int64())]), ValueError),
            (mapping, pa.map_(pa.large_string(), pa.int64()), NotImplementedError),
            (mapping, pa.map_(pa.field("secret-key", pa.string(), nullable=False), pa.int64()), ValueError),
            (mapping, pa.map_(pa.string(), pa.int64(), keys_sorted=True), ValueError),
        ]
        for source_type, requested_type, error_type in cases:
            with self.subTest(source_type=source_type, requested_type=requested_type):
                schema = json.loads(self.metadata["schemaString"])
                schema["fields"][0]["type"] = source_type
                self.metadata["schemaString"] = json.dumps(schema)
                self.write_log(1, {"metaData": self.metadata})
                table = DeltaTable(self.location)
                requested = pa.schema([table.schema.field(0).with_type(requested_type)])
                with table.scan() as stream:
                    with self.assertRaises(error_type) as caught:
                        pa.RecordBatchReader.from_stream(stream, schema=requested)
                    self.assertNotIn("secret", str(caught.exception))
                    with pa.RecordBatchReader.from_stream(stream, schema=table.schema) as reader:
                        self.assertTrue(reader.schema.equals(table.schema, check_metadata=True))

    def test_latest_snapshot_is_immutable(self):
        original = DeltaTable(self.location)
        self.assertEqual(original.version, 0)
        self.assertEqual(repr(original), "DeltaTable(version=0)")
        self.write_log(1, {"commitInfo": {"operation": "WRITE"}})
        for location in (self.location, str(self.location), self.location.as_uri()):
            with self.subTest(location=location):
                table = DeltaTable(location)
                self.assertEqual(table.version, 1)
                self.assertNotIn(str(self.location), repr(table))
        self.assertEqual(original.version, 0)
        with self.assertRaises(AttributeError):
            original.version = 1

    def test_selects_snapshot_version(self):
        self.write_log(1, {"commitInfo": {"operation": "WRITE"}})
        for version, expected in ((None, 1), (0, 0), (1, 1)):
            with self.subTest(version=version):
                table = DeltaTable(self.location, version=version)
                self.assertEqual(table.version, expected)
                self.assertEqual(repr(table), f"DeltaTable(version={expected})")

    def test_refresh_preserves_original_table_and_reader(self):
        self.write_log(1, self.write_parquet("first.parquet", [1, 2]))
        table = DeltaTable(self.location, version=1)
        with table.to_reader() as original_reader:
            self.write_log(2, self.write_parquet("second.parquet", [3, 4]))
            refreshed = table.refresh()
            self.assertIsNot(refreshed, table)
            self.assertEqual(refreshed.version, 2)
            self.assertEqual(table.version, 1)
            with table.to_reader() as reader:
                self.assertEqual(reader.read_all().to_pydict(), {"id": [1, 2]})
            self.assertEqual(original_reader.read_all().to_pydict(), {"id": [1, 2]})

        del table
        gc.collect()
        with refreshed.to_reader() as reader:
            self.assertCountEqual(reader.read_all().column("id").to_pylist(), [1, 2, 3, 4])

    def test_refresh_without_new_commit_returns_a_new_table(self):
        self.write_log(1, self.write_parquet("rows.parquet", [1, 2]))
        table = DeltaTable(self.location)
        refreshed = table.refresh()
        self.assertIsNot(refreshed, table)
        self.assertEqual(refreshed.version, table.version)
        self.assertTrue(refreshed.schema.equals(table.schema, check_metadata=True))
        with refreshed.to_reader() as reader:
            self.assertEqual(reader.read_all().to_pydict(), {"id": [1, 2]})

    def test_failed_refresh_preserves_original_table(self):
        self.write_log(1, self.write_parquet("rows.parquet", [1, 2]))
        table = DeltaTable(self.location)
        (self.log / f"{2:020}.json").write_text("secret-invalid-json\n", encoding="utf-8")
        with self.assertRaises(DeltaReaderError) as caught:
            table.refresh()
        error = caught.exception
        self.assertEqual(error.phase, "snapshot")
        self.assertEqual(error.code, "snapshot_load")
        for text in (str(error), repr(error), repr(error.args), repr(vars(error))):
            self.assertNotIn("secret", text)
        self.assertIsNone(error.__cause__)
        self.assertIsNone(error.__context__)
        self.assertEqual(table.version, 1)
        with table.to_reader() as reader:
            self.assertEqual(reader.read_all().to_pydict(), {"id": [1, 2]})

    def test_warmup_and_refresh_reuse_metadata_without_reading_parquet(self):
        http = self.http_support()
        data_requested = Event()

        class Storage(http.Storage):
            def send_head(self):
                if self.path.endswith(".parquet"):
                    data_requested.set()
                return super().send_head()

        self.write_log(1, self.write_parquet("first.parquet", [1, 2]))
        with http.serve(partial(Storage, directory=str(self.location))) as server:
            table = DeltaTable(
                f"http://127.0.0.1:{server.server_port}/",
                storage_options={"allow_http": "true"},
                warmup="query_planning",
            )
            server.requests.clear()
            with table.scan():
                self.assertEqual(server.requests, [])
            self.assertFalse(data_requested.is_set())

            for version in (1, 2):
                with self.subTest(version=version):
                    if version == 2:
                        self.write_log(2, self.write_parquet("second.parquet", [3, 4]))
                    table = table.refresh()
                    self.assertEqual(table.version, version)
                    if version == 1:
                        self.assertEqual(server.requests, [])
                    server.requests.clear()
                    with table.scan():
                        self.assertEqual(server.requests, [])
                    self.assertFalse(data_requested.is_set())

            with table.to_reader() as reader:
                self.assertCountEqual(reader.read_all().column("id").to_pylist(), [1, 2, 3, 4])
            self.assertTrue(data_requested.is_set())

    def test_warmup_validation(self):
        for value in (None, True, 1, 0.0, b"none", [], {}, object()):
            with self.subTest(warmup=value), self.assertRaises(TypeError):
                DeltaTable(self.location / "missing", warmup=value)
        for value in ("", "NONE", "query-planning", "secret-invalid"):
            with self.subTest(warmup=value), self.assertRaises(ValueError) as caught:
                DeltaTable(self.location / "missing", warmup=value)
            self.assertNotIn("secret", str(caught.exception))

    def test_schema_and_reader_preserve_types_values_and_metadata(self):
        def field(name, datatype, nullable=True, metadata=None):
            return {
                "name": name, "type": datatype, "nullable": nullable,
                "metadata": metadata or {},
            }

        fields = [
            field("id", "long", False, {"comment": "identifier", "ordinal": 7}),
            field("profile", {"type": "struct", "fields": [
                field("age", "integer", False, {"comment": "years"}),
                field("nickname", "string"),
            ]}),
            field("tags", {
                "type": "array", "elementType": "string", "containsNull": False,
            }),
            field("attributes", {
                "type": "map", "keyType": "string", "valueType": "long",
                "valueContainsNull": False,
            }),
            field("amount", "decimal(10,2)", False),
            field("event_ts", "timestamp"),
            field("local_ts", "timestamp_ntz"),
        ]
        self.metadata["schemaString"] = json.dumps({"type": "struct", "fields": fields})
        self.write_log(
            1,
            {"protocol": {
                "minReaderVersion": 3, "minWriterVersion": 7,
                "readerFeatures": ["timestampNtz"], "writerFeatures": ["timestampNtz"],
            }},
            {"metaData": self.metadata},
        )
        expected = pa.schema([
            pa.field("id", pa.int64(), nullable=False,
                     metadata={b"comment": b"identifier", b"ordinal": b"7"}),
            pa.field("profile", pa.struct([
                pa.field("age", pa.int32(), nullable=False, metadata={b"comment": b"years"}),
                pa.field("nickname", pa.string()),
            ])),
            pa.field("tags", pa.list_(pa.field("element", pa.string(), nullable=False))),
            pa.field("attributes", pa.map_(
                pa.string(), pa.field("value", pa.int64(), nullable=False),
            )),
            pa.field("amount", pa.decimal128(10, 2), nullable=False),
            pa.field("event_ts", pa.timestamp("us", tz="UTC")),
            pa.field("local_ts", pa.timestamp("us")),
        ])
        values = pa.table({
            "id": [1, 2, 3],
            "profile": [{"age": 7, "nickname": ""}, None, {"age": 0, "nickname": None}],
            "tags": [["tag", ""], None, []],
            "attributes": [[("key", 4)], None, []],
            "amount": [Decimal("-123.45"), Decimal("0.00"), Decimal("99999999.99")],
            # Microseconds include fractional seconds and values before the epoch.
            "event_ts": [1_234_567, None, -1],
            "local_ts": [9_876_543, None, -9_876_543],
        }, schema=expected)
        path = self.location / "types.parquet"
        pq.write_table(values, path)
        self.write_log(2, {"add": {
            "path": path.name, "partitionValues": {}, "size": path.stat().st_size,
            "modificationTime": 0, "dataChange": True,
        }})
        table = DeltaTable(self.location)
        schema = table.schema
        self.assertIsInstance(schema, pa.Schema)
        self.assertTrue(schema.equals(expected, check_metadata=True), schema)
        self.assertTrue(pa.schema(table).equals(expected, check_metadata=True))
        self.assertTrue(table.schema.equals(expected, check_metadata=True))
        with pa.RecordBatchReader.from_stream(table.scan(), schema=expected) as reader:
            self.assertTrue(reader.schema.equals(expected, check_metadata=True))
            self.assertTrue(reader.read_all().equals(values, check_metadata=True))
        with table.to_reader() as reader:
            self.assertTrue(reader.read_all().equals(values, check_metadata=True))
        with self.assertRaises(AttributeError):
            table.schema = expected
        del table
        gc.collect()
        self.assertTrue(schema.equals(expected, check_metadata=True))
        self.assertEqual(
            DeltaTable(self.location, version=0).schema,
            pa.schema([pa.field("id", pa.int64(), nullable=False)]),
        )

    def test_schema_capsule_outlives_table(self):
        table = DeltaTable(self.location)
        capsule = table.__arrow_c_schema__()
        unused = table.__arrow_c_schema__()
        del unused, table
        gc.collect()

        class ExportedSchema:
            def __arrow_c_schema__(self):
                return capsule

        self.assertEqual(
            pa.schema(ExportedSchema()),
            pa.schema([pa.field("id", pa.int64(), nullable=False)]),
        )

    def test_schema_export_errors_are_redacted(self):
        schema = json.loads(self.metadata["schemaString"])
        schema["fields"][0]["name"] = "secret\0field"
        nested = {"type": "struct", "fields": [{
            "name": "nested", "type": schema, "nullable": True, "metadata": {},
        }]}
        for malformed in (schema, nested):
            with self.subTest(schema=malformed):
                self.metadata["schemaString"] = json.dumps(malformed)
                self.write_log(1, {"metaData": self.metadata})
                table = DeltaTable(self.location)
                with self.assertRaises(DeltaReaderError) as caught:
                    _ = table.schema
                error = caught.exception
                self.assertEqual(error.phase, "schema")
                self.assertEqual(error.code, "schema_conversion")
                self.assertNotIn("secret", str(error))
                self.assertIsNone(error.__cause__)
                self.assertIsNone(error.__context__)
                with self.assertRaises(DeltaReaderError) as caught:
                    table.to_reader()
                self.assertEqual(caught.exception.code, "schema_conversion")
                self.assertNotIn("secret", str(caught.exception))

    def test_version_validation(self):
        class Version(int):
            def __lt__(self, other):
                return False

            def __index__(self):
                return 123

        self.assertEqual(DeltaTable(self.location, version=Version(0)).version, 0)
        with self.assertRaises(TypeError):
            DeltaTable(self.location, 0)
        cases = (
            (True, TypeError),
            (False, TypeError),
            (0.0, TypeError),
            ("0", TypeError),
            (b"0", TypeError),
            (object(), TypeError),
            (-1, ValueError),
            (-(2**100), ValueError),
            (Version(-1), ValueError),
            (2**64, OverflowError),
            (2**100, OverflowError),
        )
        for version, error in cases:
            with self.subTest(version=version), self.assertRaises(error):
                DeltaTable(self.location / "missing", version=version)

    def test_missing_snapshot_version(self):
        for version in (1, 2**64 - 1):
            with self.subTest(version=version):
                with self.assertRaises(DeltaReaderError) as caught:
                    DeltaTable(self.location, version=version)
                self.assertEqual(caught.exception.phase, "snapshot")
                self.assertEqual(caught.exception.code, "snapshot_load")
                self.assertNotIn("secret", str(caught.exception))

    def test_storage_options_accepts_mappings(self):
        values = {"secret-option": "secret-value"}
        cases = (None, {}, values, UserDict(values), MappingProxyType(values))
        for options in cases:
            with self.subTest(options=options):
                table = DeltaTable(self.location, version=0, storage_options=options)
                self.assertEqual(table.version, 0)
                self.assertNotIn("secret", repr(table))
        self.assertEqual(values, {"secret-option": "secret-value"})

    def test_storage_options_requires_string_mapping(self):
        cases = (
            True,
            1,
            "secret-option",
            [],
            [("key", "value")],
            {1: "value"},
            {b"key": "value"},
            {"key": 1},
            {"key": None},
            {"key": b"value"},
            UserDict({"key": False}),
        )
        for options in cases:
            with self.subTest(options=options), self.assertRaises(TypeError):
                DeltaTable(self.location / "missing", storage_options=options)

    def test_storage_options_are_forwarded_and_redacted(self):
        # This invalid boolean fails during store construction, before any I/O.
        values = {"allow_http": "secret-invalid-value"}
        for options in (values, UserDict(values), MappingProxyType(values)):
            with self.subTest(options=options):
                with self.assertRaises(DeltaReaderError) as caught:
                    DeltaTable("http://127.0.0.1:9/table", storage_options=options)
                error = caught.exception
                self.assertEqual(error.phase, "storage")
                self.assertEqual(error.code, "storage_initialization")
                for text in (str(error), repr(error), repr(error.args), repr(vars(error))):
                    self.assertNotIn("secret", text)

    def test_location_requires_text(self):
        class BytesPath:
            def __fspath__(self):
                return b"secret-table"

        for location in (None, 42, True, object(), b"secret-table", BytesPath()):
            with self.subTest(location=location), self.assertRaises(TypeError):
                DeltaTable(location)

    def test_reader_errors_are_redacted(self):
        empty = self.location / "secret-empty-table"
        empty.mkdir()
        cases = (
            (
                self.location / "secret-missing-table",
                "table_location",
                "invalid_table_location",
            ),
            (empty, "snapshot", "snapshot_load"),
            (
                "unknown://secret-user:secret-password@host/table?token=secret-token",
                "storage",
                "storage_initialization",
            ),
        )
        for location, phase, code in cases:
            with self.subTest(location=location):
                with self.assertRaises(DeltaReaderError) as caught:
                    DeltaTable(location)
                error = caught.exception
                self.assertEqual(error.phase, phase)
                self.assertEqual(error.code, code)
                self.assertIn(f"phase={phase} code={code}", str(error))
                for text in (str(error), repr(error), repr(error.args), repr(vars(error))):
                    self.assertNotIn("secret", text)
                self.assertIsNone(error.__cause__)
                self.assertIsNone(error.__context__)
        self.assertEqual(DeltaTable(self.location).version, 0)

    def test_protocol_validation_is_deferred_without_warmup(self):
        self.write_log(
            1, {"protocol": {"minReaderVersion": 4, "minWriterVersion": 2}}
        )
        for options in ({}, {"warmup": "none"}):
            with self.subTest(options=options):
                table = DeltaTable(self.location, **options)
                self.assertEqual(table.version, 1)
                with self.assertRaises(DeltaReaderError) as caught:
                    table.to_reader()
                self.assertEqual(caught.exception.phase, "protocol")
        with self.assertRaises(DeltaReaderError) as caught:
            DeltaTable(self.location, warmup="query_planning")
        self.assertEqual(caught.exception.phase, "protocol")

    def test_process_exits_after_successful_and_failed_loading(self):
        empty = self.location / "empty-table"
        empty.mkdir()
        script = """
import sys
from delta_arrow_reader import DeltaReaderError, DeltaTable

try:
    table = DeltaTable(sys.argv[1])
except DeltaReaderError as error:
    assert error.code == "snapshot_load"
    print("failed")
else:
    assert table.version == 0
    print("loaded")
# Keep the successful table alive through interpreter shutdown.
"""
        for location, expected in ((self.location, "loaded"), (empty, "failed")):
            with self.subTest(result=expected):
                result = subprocess.run(
                    [sys.executable, "-I", "-c", script, str(location)],
                    capture_output=True, text=True, timeout=15,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, expected + "\n")
                self.assertEqual(result.stderr, "")


if __name__ == "__main__":
    unittest.main()
