from collections import UserDict
from concurrent.futures import ThreadPoolExecutor
import gc
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from threading import Barrier
from types import MappingProxyType
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from delta_arrow_reader import DeltaReaderError, DeltaTable, RecordBatchStream


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

    def write_parquet(self, name, values):
        path = self.location / name
        schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
        pq.write_table(pa.table({"id": values}, schema=schema), path)
        return {"add": {
            "path": name, "partitionValues": {}, "size": path.stat().st_size,
            "modificationTime": 0, "dataChange": True,
        }}

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

    def test_empty_reader_preserves_schema(self):
        table = DeltaTable(self.location)
        with table.to_reader() as reader:
            empty = reader.read_all()
        self.assertEqual(empty.schema, table.schema)
        self.assertEqual(empty.num_rows, 0)

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

    def test_schema_preserves_types_and_metadata(self):
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
        table = DeltaTable(self.location)
        schema = table.schema
        self.assertIsInstance(schema, pa.Schema)
        self.assertTrue(schema.equals(expected, check_metadata=True), schema)
        self.assertTrue(pa.schema(table).equals(expected, check_metadata=True))
        self.assertTrue(table.schema.equals(expected, check_metadata=True))
        with pa.RecordBatchReader.from_stream(table.scan(), schema=expected) as reader:
            self.assertTrue(reader.schema.equals(expected, check_metadata=True))
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

    def test_protocol_validation_stays_deferred(self):
        self.write_log(
            1, {"protocol": {"minReaderVersion": 4, "minWriterVersion": 2}}
        )
        table = DeltaTable(self.location)
        self.assertEqual(table.version, 1)
        with self.assertRaises(DeltaReaderError) as caught:
            table.to_reader()
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
