from collections import UserDict
from concurrent.futures import ThreadPoolExecutor
import ctypes
from datetime import date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal, localcontext
from functools import partial
import gc
import importlib.util
import inspect
from itertools import product
import json
import operator
from pathlib import Path
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


class MisleadingInteger(int):
    def __lt__(self, other):
        return False

    def __le__(self, other):
        return False

    def __index__(self):
        return 123


class NoCoercion:
    def fail(self, *args):
        raise AssertionError("filter values must not be coerced or stringified")

    __str__ = __repr__ = __int__ = __index__ = __float__ = __bytes__ = __bool__ = fail
    as_tuple = toordinal = timestamp = fail
    __class__ = property(fail)


class DateTimeWithOverrides(datetime):
    __sub__ = timestamp = utcoffset = toordinal = __str__ = __repr__ = NoCoercion.fail
    year = microsecond = tzinfo = property(NoCoercion.fail)


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
        if not isinstance(values, pa.Table):
            schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
            values = pa.table({"id": values}, schema=schema)
        pq.write_table(values, path, **options)
        return {"add": {
            "path": name, "partitionValues": {}, "size": path.stat().st_size,
            "modificationTime": 0, "dataChange": True,
        }}

    def filter_table(self, columns):
        fields = [{"name": name, "type": delta_type, "nullable": True, "metadata": {}}
                  for name, delta_type, _ in columns]
        fields.insert(0, {"name": "id", "type": "long", "nullable": False, "metadata": {}})
        self.metadata["schemaString"] = json.dumps({"type": "struct", "fields": fields})
        schema = pa.schema([pa.field("id", pa.int64(), nullable=False)]
                           + [pa.field(name, values.type) for name, _, values in columns])
        values = pa.table({"id": range(len(columns[0][2])),
                           **{name: values for name, _, values in columns}}, schema=schema)
        protocol = ({"minReaderVersion": 3, "minWriterVersion": 7,
                     "readerFeatures": ["timestampNtz"], "writerFeatures": ["timestampNtz"]}
                    if any(delta_type == "timestamp_ntz" for _, delta_type, _ in columns)
                    else {"minReaderVersion": 1, "minWriterVersion": 2})
        self.write_log(1, {"protocol": protocol}, {"metaData": self.metadata},
                       self.write_parquet("values.parquet", values))
        return DeltaTable(self.location, warmup="none")

    def assert_filter_cases(self, table, column, cases):
        for method, (index, (symbol, scalar, expected)) in product(("scan", "to_reader"), enumerate(cases)):
            with self.subTest(column=column, method=method, index=index):
                result = getattr(table, method)(columns=["id"], filters=[(column, symbol, scalar)])
                reader = pa.RecordBatchReader.from_stream(result) if method == "scan" else result
                with reader:
                    self.assertCountEqual(reader.read_all().column("id").to_pylist(), expected)

    def assert_invalid_filter_values(self, table, column, cases):
        for method in (table.scan, table.to_reader):
            for index, (value, error_type) in enumerate(cases):
                with self.subTest(column=column, method=method.__name__, index=index):
                    with self.assertRaises(error_type) as caught:
                        method(filters=[(column, "==", value)])
                    self.assertIs(type(caught.exception), error_type)
                    self.assertNotIn("secret", str(caught.exception))
                    self.assertIsNone(caught.exception.__cause__)
                    self.assertIsNone(caught.exception.__context__)

    def http_support(self):
        support = Path(__file__).with_name("_http.py")
        spec = importlib.util.spec_from_file_location("reader_http", support)
        http = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(http)
        return http

    def test_installed_wheel_stream_smoke(self):
        fixture = (Path(__file__).resolve().parents[3]
                   / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned")
        with pa.ipc.open_file(fixture / "expected.arrow") as source:
            rows = source.read_all().to_pylist()
        expected = {row["id"]: row["label"] for row in rows
                    if row["id"] >= 100 and row["value"] is not None}
        table = DeltaTable(fixture / "table")
        self.assertEqual(table.version, 0)
        for method, backend in product(("scan", "to_reader"), ("direct", "delta_kernel")):
            with self.subTest(method=method, backend=backend):
                result = getattr(table, method)(
                    columns=["label", "id"],
                    filters=[("id", ">=", 100), ("value", "is not", None)],
                    target_partitions=1,
                    execution_options=ScanExecutionOptions(parquet_backend=backend),
                )
                reader = pa.RecordBatchReader.from_stream(result) if method == "scan" else result
                try:
                    batch = next(batch for batch in reader if batch.num_rows)
                finally:
                    reader.close()
                self.assertEqual(batch.schema.names, ["label", "id"])
                self.assertLess(batch.num_rows, len(expected), "smoke must close before exhaustion")
                actual = batch.to_pylist()
                self.assertEqual(len({row["id"] for row in actual}), batch.num_rows)
                for row in actual:
                    self.assertIn(row["id"], expected)
                    self.assertEqual(row["label"], expected[row["id"]])

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

    def test_installed_external_writer_fixtures_cross_the_arrow_boundary(self):
        corpus = (Path(__file__).resolve().parents[3]
                  / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus")
        for fixture in ("partitioned", "deletion_vectors", "nested_mapping"):
            with self.subTest(fixture=fixture):
                with pa.ipc.open_file(corpus / fixture / "expected.arrow") as source:
                    expected = source.read_all()
                table = DeltaTable(corpus / fixture / "table")
                self.assertTrue(table.schema.equals(expected.schema, check_metadata=False))
                with pa.RecordBatchReader.from_stream(table.scan(), schema=table.schema) as reader:
                    actual = reader.read_all().sort_by([("id", "ascending")])
                self.assertTrue(actual.schema.equals(table.schema, check_metadata=True))
                self.assertTrue(actual.equals(expected, check_metadata=False))

    def test_projection_and_limit_arguments_cross_the_arrow_boundary(self):
        fixture = (Path(__file__).resolve().parents[3]
                   / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned")
        table = DeltaTable(fixture / "table")
        with table.to_reader() as reader:
            full = reader.read_all()
        for method, (columns, limit) in product(("scan", "to_reader"), (
            (None, None), (("label", "id"), 3), ([], 2), ((), 0),
            (["id"], 0), (["id"], 2 * sys.maxsize + 1),
        )):
            with self.subTest(method=method, columns=columns, limit=limit):
                expected = full if columns is None else full.select(columns)
                if limit is not None:
                    expected = expected.slice(0, min(limit, full.num_rows))
                result = getattr(table, method)(columns=columns, limit=limit)
                reader = pa.RecordBatchReader.from_stream(result) if method == "scan" else result
                with reader:
                    actual = reader.read_all()
                self.assertTrue(actual.equals(expected, check_metadata=True))
                self.assertEqual(actual.num_rows, expected.num_rows)

    def test_python_filter_groups_preserve_and_or_and_null_semantics(self):
        fixture = (Path(__file__).resolve().parents[3]
                   / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned")
        with pa.ipc.open_file(fixture / "expected.arrow") as source:
            rows = source.read_all().to_pylist()
        all_ids = [row["id"] for row in rows]
        null_ids = [row["id"] for row in rows if row["value"] is None]
        cases = (
            (None, all_ids), ([], all_ids), ([[]], all_ids),
            ([("value", "is", None)], null_ids),
            ([("value", "is not", None)], [row["id"] for row in rows if row["value"] is not None]),
            ([("value", "is", None), ("id", "is not", None)], null_ids),
            ([("value", "is", None), ("value", "is not", None)], []),
            ([[("value", "is", None)], []], all_ids),
            ([("id", ">=", 2), ("id", "<", 5)], [2, 3, 4]),
            ([[("value", "is", None), ("id", "is not", None)], [("id", "<=", 2)]],
             [row["id"] for row in rows if row["value"] is None or row["id"] <= 2]),
        )
        table = DeltaTable(fixture / "table")
        for method, (filters, expected) in product(("scan", "to_reader"), cases):
            with self.subTest(method=method, filters=filters):
                result = getattr(table, method)(columns=["id"], filters=filters)
                reader = pa.RecordBatchReader.from_stream(result) if method == "scan" else result
                with reader:
                    self.assertCountEqual(reader.read_all().column("id").to_pylist(), expected)

    def test_boolean_and_integer_filters_use_stored_values(self):
        class Integer(int):
            __int__ = __index__ = NoCoercion.fail

        types = [("boolean", pa.bool_(), [False, True, None, False])]
        types += [(name, data_type, [-1, 0, 1, None]) for name, data_type in (
            ("byte", pa.int8()), ("short", pa.int16()), ("integer", pa.int32()), ("long", pa.int64()),
        )]
        table = self.filter_table([(name, name, pa.array(values, data_type))
                                   for name, data_type, values in types])
        for name, data_type, values in types:
            cases = [("==", False, [0, 3]), ("==", True, [1])] if name == "boolean" else [
                (symbol, Integer(0), [i for i, value in enumerate(values)
                                     if value is not None and compare(value, 0)])
                for symbol, compare in (("==", operator.eq), ("!=", operator.ne),
                                        ("<", operator.lt), ("<=", operator.le),
                                        (">", operator.gt), (">=", operator.ge))
            ]
            self.assert_filter_cases(table, name, cases)
            invalid = [(NoCoercion(), TypeError)]
            if name != "boolean":
                invalid.append((Integer(2 ** (data_type.bit_width - 1)), OverflowError))
            self.assert_invalid_filter_values(table, name, invalid)

    def test_float_filters_use_stored_values_without_coercion(self):
        class FloatWithOverride(float):
            __float__ = NoCoercion.fail

        table = self.filter_table([(name, name, pa.array([-1.5, 0.1, None], data_type))
                                   for name, data_type in (("float", pa.float32()), ("double", pa.float64()))])
        for name in ("float", "double"):
            self.assert_filter_cases(table, name, [("==", FloatWithOverride(-1.5), [0]), ("==", 0.1, [1])])
            self.assert_invalid_filter_values(table, name, [
                (NoCoercion(), TypeError), (FloatWithOverride("nan"), ValueError),
            ])

    def test_string_and_binary_filters_use_stored_values_without_coercion(self):
        class StringWithOverrides(str):
            __str__ = __repr__ = encode = NoCoercion.fail

        class BytesWithOverrides(bytes):
            __bytes__ = __repr__ = __iter__ = __len__ = NoCoercion.fail

        table = self.filter_table([
            ("string", "string", pa.array(["a\0b", "caf\u00e9", "cafe\u0301", None])),
            ("binary", "binary", pa.array([b"a\0b", b"\xff", b"", None])),
        ])
        for name, scalar in (("string", StringWithOverrides("a\0b")), ("binary", BytesWithOverrides(b"a\0b"))):
            self.assert_filter_cases(table, name, [("==", scalar, [0])])
            self.assert_invalid_filter_values(table, name, [(NoCoercion(), TypeError)])
        self.assert_filter_cases(table, "string", [("==", "cafe\u0301", [2])])
        self.assert_invalid_filter_values(table, "string", [(StringWithOverrides("\ud800secret"), ValueError)])

    def test_decimal_filters_ignore_subclass_overrides_and_decimal_context(self):
        class DecimalWithOverrides(Decimal):
            as_tuple = __str__ = __repr__ = __int__ = __float__ = is_finite = NoCoercion.fail

        types = (
            ("amount", 5, 2, "1.23", "1.23" + "0" * 200),
            ("whole", 5, 0, "123", "1.2300E2"),
            ("wide", 38, 0, "9" * 38, "9" * 38 + "0" * 200 + "E-200"),
            ("fraction", 38, 38, "1E-38", "1E-38"),
        )
        table = self.filter_table([
            (name, f"decimal({precision},{scale})", pa.array([Decimal(value), None], pa.decimal128(precision, scale)))
            for name, precision, scale, value, _ in types
        ])
        with localcontext() as context:
            context.prec = 1
            context.Emin, context.Emax = -2, 2
            for signal in context.traps:
                context.traps[signal] = True
            context.clear_flags()
            for name, _, _, _, scalar in types:
                self.assert_filter_cases(table, name, [("==", DecimalWithOverrides(scalar), [0])])
                self.assert_invalid_filter_values(table, name, [
                    (NoCoercion(), TypeError), (DecimalWithOverrides("NaN123"), ValueError),
                    (DecimalWithOverrides("1E-39"), ValueError),
                ])
            self.assertFalse(any(context.flags.values()))

    def test_date_filters_use_stored_dates_and_exclude_datetime_subclasses(self):
        class DateWithOverrides(date):
            toordinal = __str__ = __repr__ = __sub__ = NoCoercion.fail
            year = property(NoCoercion.fail)

        table = self.filter_table([("day", "date", pa.array([date(2000, 2, 29), None], pa.date32()))])
        self.assert_filter_cases(table, "day", [("==", DateWithOverrides(2000, 2, 29), [0])])
        self.assert_invalid_filter_values(table, "day", [
            (NoCoercion(), TypeError), (DateTimeWithOverrides(1970, 1, 1), TypeError),
        ])

    def test_timestamp_filters_validate_callbacks_subclasses_and_fold(self):
        class Offset(tzinfo):
            def __init__(self, offset):
                self.offset = offset

            def utcoffset(self, value):
                if isinstance(self.offset, BaseException):
                    raise self.offset
                return self.offset

        class FoldOffset(tzinfo):
            def __init__(self):
                self.calls = 0

            def utcoffset(self, value):
                self.calls += 1
                return timedelta(hours=-5 if value.fold else -4)

        class OffsetWithNanoseconds(timedelta):
            nanoseconds = 1
            __str__ = __repr__ = NoCoercion.fail

        table = self.filter_table([
            ("local_ts", "timestamp_ntz", pa.array([-1, 0, 1, None], pa.timestamp("us"))),
            ("event_ts", "timestamp", pa.array([1_636_263_000_000_000, 1_636_266_600_000_000, 0, None],
                                               pa.timestamp("us", tz="UTC"))),
        ])
        self.assert_filter_cases(table, "local_ts", [
            ("==", datetime(1969, 12, 31, 23, 59, 59, 999999), [0]),
            ("==", datetime(1970, 1, 1, tzinfo=Offset(None)), [1]),
            ("==", datetime(1970, 1, 1, fold=1), [1]),
        ])
        self.assert_filter_cases(table, "event_ts", [
            ("==", datetime(1970, 1, 1, 5, 30, tzinfo=timezone(timedelta(hours=5, minutes=30))), [2]),
        ])
        for method, fold in product(("scan", "to_reader"), (0, 1)):
            offset = FoldOffset()
            scalar = datetime(2021, 11, 7, 1, 30, tzinfo=offset, fold=fold)
            with self.subTest(method=method, fold=fold):
                result = getattr(table, method)(columns=["id"], filters=[("event_ts", "==", scalar)])
                reader = pa.RecordBatchReader.from_stream(result) if method == "scan" else result
                with reader:
                    self.assertEqual(reader.read_all().column("id").to_pylist(), [fold])
                self.assertEqual(offset.calls, 1)
        for column in ("local_ts", "event_ts"):
            invalid = [(value, TypeError) for value in (
                NoCoercion(), DateTimeWithOverrides(1970, 1, 1),
                DateTimeWithOverrides(1970, 1, 1, tzinfo=timezone.utc),
            )]
            invalid += [(datetime(1970, 1, 1, tzinfo=Offset(offset)), ValueError) for offset in (
                "secret-offset", timedelta(days=1), timedelta(days=-1), OffsetWithNanoseconds(),
                ValueError("secret-offset"), RuntimeError("secret-offset"),
            )]
            invalid.append((datetime(1970, 1, 1, tzinfo=Offset(KeyboardInterrupt())), KeyboardInterrupt))
            wrong_timezone = timezone.utc if column == "local_ts" else Offset(None)
            invalid.append((datetime(1970, 1, 1, tzinfo=wrong_timezone), ValueError))
            self.assert_invalid_filter_values(table, column, invalid)

    def test_filter_validation_is_shared_and_redacted_before_planning(self):
        class SecretValue:
            def __str__(self):
                raise AssertionError("filter values must not be stringified")

            __repr__ = __str__
            __index__ = __str__
            __int__ = __str__
            __bool__ = __str__

        table = DeltaTable(self.location, warmup="none")
        (self.log / f"{0:020}.json").unlink()
        with self.assertRaises(DeltaReaderError):
            table.scan()
        malformed = (
            True, 1, "secret-filter", {}, ("id", "is", None),
            [("id", "is")], [("id", "is", None, "secret-extra")],
            [["id", "is", None]], [[[("id", "is", None)]]],
            [("id", "is", None), [("id", "is", None)]],
            [[("id", "is", None)], ("id", "is", None)],
            [(1, "is", None)], [(b"id", "is", None)],
            [("id", 1, None)], [("id", b"is", None)],
            [[], [("id", "is")]],
            [("id", "==", SecretValue())], [("id", "==", True)],
            [[], [("id", "==", SecretValue())]],
        )
        invalid = (
            [("id", "secret-operator", None)], [("id", "IS", None)],
            [("id", "is", 1)], [("id", "is not", "secret-value")],
            [("id", "is", SecretValue())],
        ) + tuple([("id", symbol, None)] for symbol in ("==", "!=", "<", "<=", ">", ">="))
        for method in (table.scan, table.to_reader):
            for cases, error_type in ((malformed, TypeError), (invalid, ValueError)):
                for index, filters in enumerate(cases):
                    with self.subTest(method=method.__name__, error_type=error_type, index=index):
                        with self.assertRaises(error_type) as caught:
                            method(filters=filters)
                        self.assertNotIn("secret", str(caught.exception))
            for column in ("", "missing-secret", "id.secret"):
                for filters in ([(column, "is", None)], [[], [(column, "is not", None)]]):
                    with self.subTest(method=method.__name__, column=column):
                        with self.assertRaises(DeltaReaderError) as caught:
                            method(filters=filters)
                        self.assertEqual(caught.exception.phase, "scan_planning")
                        self.assertEqual(caught.exception.code, "unsupported_predicate")
                        self.assertNotIn("secret", str(caught.exception))

    def test_filter_inputs_are_copied_before_returning_a_reader(self):
        self.write_log(1, self.write_parquet("rows.parquet", [1, 2]))
        table = DeltaTable(self.location)
        for method in ("scan", "to_reader"):
            with self.subTest(method=method):
                filters = [[("id", "is", None)]]
                result = getattr(table, method)(filters=filters)
                filters[0].clear()
                reader = (pa.RecordBatchReader.from_stream(result)
                          if method == "scan" else result)
                with reader:
                    self.assertEqual(reader.read_all().num_rows, 0)

    def test_limit_validation_is_shared_by_both_entrypoints(self):
        self.write_log(1, self.write_parquet("rows.parquet", [1, 2]))
        table = DeltaTable(self.location)
        for method in ("scan", "to_reader"):
            with self.subTest(method=method):
                result = getattr(table, method)(limit=MisleadingInteger(1))
                reader = (pa.RecordBatchReader.from_stream(result)
                          if method == "scan" else result)
                with reader:
                    self.assertEqual(reader.read_all().to_pydict(), {"id": [1]})
                for limit, error_type in (
                    (True, TypeError), (False, TypeError), (1.0, TypeError),
                    ("1", TypeError), (b"1", TypeError), (object(), TypeError),
                    (-1, ValueError), (-2**100, ValueError), (MisleadingInteger(-1), ValueError),
                    (2 * sys.maxsize + 2, OverflowError), (2**100, OverflowError),
                ):
                    with self.subTest(limit=limit), self.assertRaises(error_type):
                        getattr(table, method)(limit=limit)
                with self.assertRaises(TypeError):
                    getattr(table, method)(None, 1)

    def test_target_partitions_validation_is_shared_by_both_entrypoints(self):
        table = DeltaTable(self.location)
        for method in (table.scan, table.to_reader):
            with self.subTest(method=method.__name__):
                for target in (None, MisleadingInteger(1), 2, 8, 2 * sys.maxsize + 1):
                    method(target_partitions=target).close()
                for target, error_type in (
                    (True, TypeError), (False, TypeError), (1.0, TypeError),
                    ("1", TypeError), (b"1", TypeError), (object(), TypeError),
                    (0, ValueError), (MisleadingInteger(0), ValueError), (-1, ValueError),
                    (-2**100, ValueError), (MisleadingInteger(-1), ValueError),
                    (2 * sys.maxsize + 2, OverflowError), (2**100, OverflowError),
                ):
                    with self.subTest(target=target), self.assertRaises(error_type):
                        method(target_partitions=target)
                with self.assertRaises(TypeError):
                    method(None, None, 1)

    def test_execution_options_signature_reports_actual_defaults(self):
        signature = inspect.signature(ScanExecutionOptions)
        arguments = signature.bind()
        arguments.apply_defaults()
        defaults = ScanExecutionOptions()
        forwarded = ScanExecutionOptions(**arguments.kwargs)
        for name, parameter in signature.parameters.items():
            with self.subTest(name=name):
                self.assertEqual(parameter.kind, inspect.Parameter.KEYWORD_ONLY)
                self.assertEqual(parameter.default, getattr(defaults, name))
                self.assertEqual(getattr(forwarded, name), getattr(defaults, name))

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

    def test_numeric_execution_options_validate_bounds_defaults_and_immutability(self):
        capacity = sys.maxsize >> 2
        usize_max = 2 * sys.maxsize + 1
        for name, default, minimum, maximum, optional in (
            ("max_concurrent_file_reads_per_partition", 3, 1, capacity, False),
            ("max_concurrent_file_reads_per_scan", None, 1, capacity, True),
            ("output_buffer_batches_per_partition", 1, 1, capacity, False),
            ("prefetch_files_per_partition", 2, 0, usize_max, False),
            ("parquet_metadata_size_hint_bytes", 65536, 1, usize_max, True),
            ("parquet_full_file_read_threshold_bytes", None, 1, usize_max, True),
        ):
            self.assertEqual(getattr(ScanExecutionOptions(), name), default)
            valid = [minimum, 2, MisleadingInteger(minimum), MisleadingInteger(2), maximum, default]
            if optional:
                valid.append(None)
            for count in valid:
                with self.subTest(name=name, count=count):
                    options = ScanExecutionOptions(**{name: count})
                    self.assertEqual(getattr(options, name), count)
                    with self.assertRaises(AttributeError):
                        setattr(options, name, 1)
            invalid = [(value, TypeError) for value in (
                True, False, 1.0, "secret-value", b"1", [], {}, object(),
            )]
            if not optional:
                invalid.append((None, TypeError))
            invalid += [
                (minimum - 1, ValueError), (MisleadingInteger(minimum - 1), ValueError),
                (-2**100, ValueError), (maximum + 1, ValueError if maximum < usize_max else OverflowError),
                (usize_max + 1, OverflowError), (2**100, OverflowError),
            ]
            for count, error_type in invalid:
                with self.subTest(name=name, count=count), self.assertRaises(error_type) as caught:
                    ScanExecutionOptions(**{name: count})
                self.assertIn(name, str(caught.exception))
                self.assertNotIn("secret", str(caught.exception))

    def test_execution_options_override_and_refresh_preserve_table_defaults(self):
        http = self.http_support()
        action = self.write_parquet("rows.parquet", [1, 2, 3])
        self.write_log(1, action)
        size = action["add"]["size"]
        footer = f"bytes={size - 8}-{size - 1}"
        hint = f"bytes={size - 64}-{size - 1}"
        with http.serve(partial(http.Storage, directory=str(self.location))) as server:
            for backend, full_file in product(("direct", "delta_kernel"), (False, True)):
                original = DeltaTable(
                    f"http://127.0.0.1:{server.server_port}/",
                    storage_options={"allow_http": "true"},
                    execution_options=ScanExecutionOptions(
                        parquet_backend=backend, parquet_metadata_size_hint_bytes=64,
                        parquet_full_file_read_threshold_bytes=size if full_file else None,
                    ),
                )
                default_range = hint if backend == "direct" else footer
                if full_file and backend == "direct":
                    default_range = None
                other = "delta_kernel" if backend == "direct" else "direct"
                overrides = (
                    ({}, default_range),
                    ({"execution_options": ScanExecutionOptions()}, f"bytes=0-{size - 1}"),
                    ({"execution_options": ScanExecutionOptions(
                        parquet_backend=other, parquet_metadata_size_hint_bytes=64,
                    )}, hint if other == "direct" else footer),
                    ({"execution_options": ScanExecutionOptions(
                        parquet_backend=backend, parquet_metadata_size_hint_bytes=None,
                    )}, footer),
                    ({"execution_options": ScanExecutionOptions(
                        parquet_backend=backend, parquet_full_file_read_threshold_bytes=size,
                    )}, None if backend == "direct" else footer),
                    ({"execution_options": None}, default_range),
                )
                for table, method, (override, expected) in product(
                    (original, original.refresh()), ("scan", "to_reader"), overrides,
                ):
                    with self.subTest(backend=backend, full_file=full_file, method=method, override=override):
                        server.requests.clear()
                        result = getattr(table, method)(**override)
                        reader = pa.RecordBatchReader.from_stream(result) if method == "scan" else result
                        with reader:
                            self.assertEqual(reader.read_all().to_pydict(), {"id": [1, 2, 3]})
                        ranges = [r for path, r, _ in server.requests if path.endswith(".parquet")]
                        self.assertEqual(ranges[0], expected)
                        if expected is None:
                            self.assertEqual(ranges, [None])
                        else:
                            self.assertNotIn(None, ranges)

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
            for name, target, count in (
                ("max_concurrent_file_reads_per_partition", 1, 1),
                ("max_concurrent_file_reads_per_scan", 2, 1),
                ("prefetch_files_per_partition", 1, 0),
            ):
                table = DeltaTable(
                    f"http://127.0.0.1:{server.server_port}/",
                    storage_options={"allow_http": "true"},
                    execution_options=ScanExecutionOptions(**{name: count}),
                )
                override = ScanExecutionOptions(**{name: count + 1})
                for options, expected_reads in ((None, 1), (override, 2), (None, 1)):
                    with self.subTest(name=name, expected_reads=expected_reads):
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
                                self.assertCountEqual(result.result(timeout=10).column("id").to_pylist(), range(4))

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
        for location, version in product(
            (self.location, str(self.location), self.location.as_uri()), (None, 0, 1),
        ):
            with self.subTest(location=location, version=version):
                table = DeltaTable(location, version=version)
                expected = 1 if version is None else version
                self.assertEqual(table.version, expected)
                self.assertEqual(repr(table), f"DeltaTable(version={expected})")
        self.assertEqual(original.version, 0)
        with self.assertRaises(AttributeError):
            original.version = 1

    def test_refresh_preserves_original_table_and_reader(self):
        self.write_log(1, self.write_parquet("first.parquet", [1, 2]))
        table = DeltaTable(self.location, version=1)
        unchanged = table.refresh()
        self.assertIsNot(unchanged, table)
        self.assertEqual(unchanged.version, table.version)
        self.assertTrue(unchanged.schema.equals(table.schema, check_metadata=True))
        with table.to_reader() as original_reader:
            self.write_log(2, self.write_parquet("second.parquet", [3, 4]))
            refreshed = table.refresh()
            self.assertIsNot(refreshed, table)
            self.assertEqual(refreshed.version, 2)
            self.assertEqual(table.version, 1)
            with table.to_reader() as reader:
                self.assertEqual(reader.read_all().to_pydict(), {"id": [1, 2]})
            self.assertEqual(original_reader.read_all().to_pydict(), {"id": [1, 2]})

        del table, unchanged
        gc.collect()
        with refreshed.to_reader() as reader:
            self.assertCountEqual(reader.read_all().column("id").to_pylist(), [1, 2, 3, 4])

    def test_warmup_is_forwarded_and_preserved_on_refresh(self):
        http = self.http_support()
        self.write_log(1, self.write_parquet("first.parquet", [1, 2]))
        with http.serve(partial(http.Storage, directory=str(self.location))) as server:
            for warmup in ("automatic", "none", "query_planning"):
                original = DeltaTable(
                    f"http://127.0.0.1:{server.server_port}/",
                    storage_options={"allow_http": "true"}, warmup=warmup,
                )
                for table in (original, original.refresh()):
                    with self.subTest(warmup=warmup):
                        server.requests.clear()
                        with table.scan():
                            self.assertEqual(bool(server.requests), warmup != "query_planning")
                        self.assertFalse(any(path.endswith(".parquet") for path, _, _ in server.requests))
            self.write_log(2, self.write_parquet("second.parquet", [3, 4]))
            table = table.refresh()
            self.assertEqual(table.version, 2)
            self.assertFalse(any(path.endswith(".parquet") for path, _, _ in server.requests))
            server.requests.clear()
            with table.scan():
                self.assertEqual(server.requests, [])
            with table.to_reader() as reader:
                self.assertCountEqual(reader.read_all().column("id").to_pylist(), [1, 2, 3, 4])

    def test_warmup_validation(self):
        self.assertEqual(inspect.signature(DeltaTable).parameters["warmup"].default, "automatic")
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

    def test_version_validation(self):
        self.assertEqual(DeltaTable(self.location, version=MisleadingInteger(0)).version, 0)
        with self.assertRaises(DeltaReaderError):
            DeltaTable(self.location, version=2**64 - 1)
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
            (MisleadingInteger(-1), ValueError),
            (2**64, OverflowError),
            (2**100, OverflowError),
        )
        for version, error in cases:
            with self.subTest(version=version), self.assertRaises(error):
                DeltaTable(self.location / "missing", version=version)

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


if __name__ == "__main__":
    unittest.main()
