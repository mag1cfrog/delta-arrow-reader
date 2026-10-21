from concurrent.futures import ThreadPoolExecutor
import gc
import json
import logging
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import weakref

import pyarrow as pa

from delta_arrow_reader import DeltaReaderError, DeltaTable, ScanExecutionOptions, Session


FIXTURE = (Path(__file__).resolve().parents[3]
           / "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table")


class Collect(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


class LoggingTests(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("delta_arrow_reader")
        previous = (self.logger.handlers[:], self.logger.level,
                    self.logger.propagate, self.logger.disabled)
        self.addCleanup(self.restore, previous)
        self.collect = Collect()
        self.logger.handlers = [self.collect]
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.logger.disabled = False

    def restore(self, previous):
        handlers, level, propagate, disabled = previous
        self.logger.handlers = handlers
        self.logger.setLevel(level)
        self.logger.propagate = propagate
        self.logger.disabled = disabled

    def events(self):
        return [record.dar_fields["event"] for record in self.collect.records]

    def assert_failure(self, event, *, target="delta_arrow_reader", **fields):
        failures = [r for r in self.collect.records if r.levelno >= logging.ERROR]
        self.assertEqual(len(failures), 1)
        record = failures[0]
        self.assertEqual((record.levelno, record.dar_target, record.msg), (logging.ERROR, target, event))
        for name, value in {"event": event, **fields}.items():
            self.assertEqual(record.dar_fields[name], value)
        rendered = repr([r.__dict__ for r in self.collect.records])
        for secret in ("private", "secret", str(FIXTURE)):
            self.assertNotIn(secret, rendered)
        return record

    def assert_logged_error(self, error, record):
        for name in ("phase", "code", "reason", "source_kind"):
            self.assertEqual(getattr(error, name), record.dar_fields.get("error_" + name))
        self.assertIsNone(error.__cause__)
        self.assertIsNone(error.__context__)

    def assert_replayed_error(self, reader, phase, code):
        messages = []
        with reader:
            for _ in range(3):
                with self.assertRaises(pa.ArrowInvalid) as caught:
                    reader.read_all()
                messages.append(str(caught.exception))
        self.assertEqual(len(set(messages)), 1)
        self.assertIn(f"phase={phase} code={code}", messages[0])
        for secret in ("private", "secret", str(FIXTURE)):
            self.assertNotIn(secret, messages[0])
        return messages[0]

    def test_public_operations_forward_native_lifecycles_and_fields(self):
        table = DeltaTable(FIXTURE, warmup="none")
        self.assertEqual(table.refresh().version, table.version)
        table.scan(columns=["id"], limit=0).close()
        Session().register_table("private_events", table)
        operations = ("table_load", "table_refresh", "scan_planning", "provider_registration")
        self.assertEqual(self.events(), [f"{operation}.{event}" for operation in operations
                                        for event in ("started", "completed")])
        starts = self.collect.records[::2]
        ends = self.collect.records[1::2]
        for start, end in zip(starts, ends):
            self.assertEqual((start.levelno, end.levelno), (logging.INFO, logging.INFO))
            self.assertEqual(start.dar_fields["operation_id"], end.dar_fields["operation_id"])
            self.assertIs(type(end.dar_fields["operation_id"]), int)
            self.assertIs(type(end.dar_fields["elapsed_micros"]), int)
            self.assertEqual(end.dar_fields["snapshot_version"], table.version)
            self.assertEqual(end.getMessage(), end.dar_fields["event"])
            self.assertEqual(end.dar_target, "delta_arrow_reader")
        self.assertEqual(len({r.dar_fields["operation_id"] for r in starts}), len(operations))
        self.assertEqual(ends[1].dar_fields["previous_snapshot_version"], table.version)
        self.assertEqual(ends[2].dar_fields["plan_id"], starts[2].dar_fields["operation_id"])
        self.assertNotIn("private_events", repr([r.__dict__ for r in self.collect.records]))

    def test_failed_load_is_once_at_error_and_redacted_at_every_level(self):
        with tempfile.TemporaryDirectory(prefix="secret-log-path-") as temporary:
            location = Path(temporary) / "private-table"
            delta_log = location / "_delta_log"
            delta_log.mkdir(parents=True)
            (delta_log / "00000000000000000000.json").write_text("secret-source-message\n")
            for level in [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG, 5]:
                with self.subTest(level=level):
                    self.collect.records.clear()
                    self.logger.setLevel(level)
                    with self.assertRaises(DeltaReaderError) as raised:
                        DeltaTable(location, warmup="none")
                    failure = self.assert_failure("table_load.failed")
                    self.assert_logged_error(raised.exception, failure)
                    self.assertIn("error_reason", failure.dar_fields)
                    self.assertIn("error_source_kind", failure.dar_fields)
                    self.assertNotIn("table_load.completed", self.events())

    def test_planning_and_registration_failures_have_one_owner(self):
        table = DeltaTable(FIXTURE, warmup="none")
        session = Session()
        session.register_table("private_events", table)
        self.logger.setLevel(5)
        for operation, event, reason, phase in (
            (lambda: table.scan(columns=["private_column"]),
             "scan_planning.failed", "column_not_found", "scan_planning"),
            (lambda: session.register_table("private_events", table),
             "provider_registration.failed", "table_registration_failed", "datafusion"),
            (lambda: session.register_table("private-invalid", table),
             "provider_registration.failed", "table_registration_name_invalid", "datafusion"),
        ):
            with self.subTest(event=event, reason=reason):
                self.collect.records.clear()
                with self.assertRaises(DeltaReaderError) as raised:
                    operation()
                failure = self.assert_failure(event, error_reason=reason, error_phase=phase,
                                              snapshot_version=table.version)
                self.assert_logged_error(raised.exception, failure)
        with session.sql("SELECT id FROM private_events LIMIT 1") as reader:
            self.assertEqual(reader.read_all().num_rows, 1)

    def test_return_only_core_validation_has_safe_reasons_and_original_exceptions(self):
        table = DeltaTable(FIXTURE, warmup="none")
        cases = [
            (lambda: table.scan(filters=[("private_missing", "==", 1)]),
             DeltaReaderError, "filters", "column_not_found"),
            (lambda: table.scan(filters=[("private.nested", "==", 1)]),
             DeltaReaderError, "filters", "invalid_column_reference"),
        ]
        for option in ["max_concurrent_file_reads_per_scan",
                       "max_concurrent_file_reads_per_partition",
                       "output_buffer_batches_per_partition"]:
            cases.append((lambda option=option: ScanExecutionOptions(**{option: sys.maxsize}),
                          ValueError, "execution_options", option + "_exceeds_tokio_max_permits"))
        self.logger.setLevel(5)
        for operation, exception, argument, reason in cases:
            with self.subTest(reason=reason):
                self.collect.records.clear()
                with self.assertRaises(exception) as raised:
                    operation()
                self.assertEqual(self.events(), ["validation.failed"])
                phase, code = (("scan_planning", "unsupported_predicate") if argument == "filters"
                               else ("configuration", "invalid_configuration"))
                record = self.assert_failure("validation.failed", target="delta_arrow_reader_python",
                                             argument=argument, error_reason=reason,
                                             error_phase=phase, error_code=code)
                self.assert_logged_error(raised.exception, record)
                self.assertEqual(str(raised.exception),
                                 f"delta reader error: phase={phase} code={code} reason={reason}")
        self.logger.disabled = True
        self.collect.records.clear()
        for operation, exception, _, _ in cases:
            with self.assertRaises(exception):
                operation()
        self.assertEqual(self.collect.records, [])
        with table.to_reader(limit=1) as reader:
            self.assertEqual(reader.read_all().num_rows, 1)

    def test_core_error_fields_do_not_depend_on_logging(self):
        with tempfile.TemporaryDirectory(prefix="private-errors-") as temporary:
            location = Path(temporary) / "table"
            shutil.copytree(FIXTURE, location)
            table = DeltaTable(location, warmup="none")
            session = Session()
            (location / "_delta_log" / "00000000000000000001.json").write_text("private-invalid-json\n")
            cases = [
                (lambda: DeltaTable("", warmup="none"), DeltaReaderError,
                 "table_location", "invalid_table_location", "empty_table_location", None),
                (lambda: DeltaTable("unknown://private-user:private-password@host/table?token=private-token"),
                 DeltaReaderError, "storage", "storage_initialization", "storage_initialization_failed", "unknown"),
                (lambda: DeltaTable(location, warmup="none"), DeltaReaderError,
                 "snapshot", "snapshot_load", "snapshot_load_failed", "arrow_json"),
                (table.refresh, DeltaReaderError,
                 "snapshot", "snapshot_load", "snapshot_refresh_failed", "arrow_json"),
                (lambda: table.scan(columns=["private_missing"]), DeltaReaderError,
                 "scan_planning", "invalid_projection", "column_not_found", None),
                (lambda: table.scan(filters=[("private_missing", "==", 1)]), DeltaReaderError,
                 "scan_planning", "unsupported_predicate", "column_not_found", None),
                (lambda: ScanExecutionOptions(max_concurrent_file_reads_per_scan=sys.maxsize),
                 ValueError, "configuration", "invalid_configuration",
                 "max_concurrent_file_reads_per_scan_exceeds_tokio_max_permits", None),
                (lambda: session.register_table("private-invalid", table), DeltaReaderError,
                 "datafusion", "datafusion_adapter", "table_registration_name_invalid", "datafusion_plan"),
            ]
            for disabled in [False, True]:
                self.logger.disabled = disabled
                self.logger.setLevel(logging.CRITICAL)
                self.collect.records.clear()
                for operation, exception, phase, code, reason, source_kind in cases:
                    with self.subTest(disabled=disabled, code=code, reason=reason):
                        with self.assertRaises(exception) as raised:
                            operation()
                        error = raised.exception
                        self.assertEqual((error.phase, error.code, error.reason, error.source_kind),
                                         (phase, code, reason, source_kind))
                        for text in [str(error), repr(error), repr(error.args), repr(vars(error))]:
                            self.assertNotIn("private", text)
                        self.assertIsNone(error.__cause__)
                        self.assertIsNone(error.__context__)
                self.assertEqual(self.collect.records, [])
            self.assertEqual(table.version, 0)
            with table.to_reader(limit=1) as reader:
                self.assertEqual(reader.read_all().num_rows, 1)

    def test_python_argument_and_consumer_errors_remain_exception_only(self):
        table = DeltaTable(FIXTURE, warmup="none")
        stream = table.scan(limit=0)
        cases = [
            (lambda: DeltaTable(FIXTURE, version=True), TypeError),
            (lambda: DeltaTable(FIXTURE, warmup="private_mode"), ValueError),
            (lambda: table.scan(limit=-1), ValueError),
            (lambda: table.scan(limit=2**100), OverflowError),
            (lambda: table.scan(filters=[("id", "==", True)]), TypeError),
            (lambda: table.scan(filters=[("id", "private_operator", 1)]), ValueError),
            (lambda: ScanExecutionOptions(output_buffer_batches_per_partition=0), ValueError),
            (lambda: stream.__arrow_c_stream__(requested_schema="private_schema"), TypeError),
            (lambda: stream.__arrow_c_stream__(requested_schema=pa.schema([]).__arrow_c_schema__()), ValueError),
            (lambda: stream.__arrow_c_stream__(requested_schema=pa.int64().__arrow_c_schema__()), ValueError),
        ]
        self.logger.setLevel(5)
        self.collect.records.clear()
        for operation, exception in cases:
            with self.subTest(exception=exception):
                with self.assertRaises(exception):
                    operation()
                self.assertEqual(self.collect.records, [])
        with pa.RecordBatchReader.from_stream(stream) as reader:
            self.assertEqual(reader.read_all().num_rows, 0)
        self.collect.records.clear()
        with self.assertRaisesRegex(RuntimeError, "exported or closed"):
            stream.__arrow_c_stream__()
        self.assertEqual(self.collect.records, [])

    def test_schema_export_failures_log_once_without_schema_contents(self):
        session = Session()
        with tempfile.TemporaryDirectory(prefix="private-schema-") as temporary:
            delta_log = Path(temporary) / "_delta_log"
            delta_log.mkdir()
            schema = {"type": "struct", "fields": [{
                "name": "private_parent", "nullable": True, "metadata": {},
                "type": {"type": "struct", "fields": [{
                    "name": "private\0field", "type": "long", "nullable": True, "metadata": {},
                }]},
            }]}
            actions = [
                {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
                {"metaData": {"id": "private-id", "format": {"provider": "parquet", "options": {}},
                              "schemaString": json.dumps(schema), "partitionColumns": [], "configuration": {}}},
            ]
            (delta_log / "00000000000000000000.json").write_text(
                "".join(json.dumps(action) + "\n" for action in actions), encoding="utf-8")
            table = DeltaTable(temporary, warmup="none")
            for level in [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG, 5]:
                self.logger.setLevel(level)
                for operation in [lambda: table.schema, lambda: pa.schema(table),
                                  lambda: table.scan(), lambda: table.to_reader(),
                                  lambda: session.sql('SELECT 42 AS "private\0alias"')]:
                    self.collect.records.clear()
                    with self.assertRaises(DeltaReaderError) as raised:
                        operation()
                    self.assertEqual((raised.exception.phase, raised.exception.code), ("schema", "schema_conversion"))
                    self.assertEqual(str(raised.exception),
                                     "delta reader error: phase=schema code=schema_conversion reason=arrow_schema_export_failed")
                    self.assertIsNone(raised.exception.__cause__)
                    self.assertIsNone(raised.exception.__context__)
                    record = self.assert_failure("schema_export.failed", target="delta_arrow_reader_python",
                                                 error_reason="arrow_schema_contains_nul")
                    self.assertNotIn("error_source_kind", record.dar_fields)
                    self.assertNotIn(temporary, repr([r.__dict__ for r in self.collect.records]))
        with session.sql("SELECT 42 AS answer") as reader:
            self.assertEqual(reader.read_all().to_pylist(), [{"answer": 42}])

    def test_synchronous_failure_handlers_preserve_exceptions_and_reject_recursion(self):
        table = DeltaTable(FIXTURE, warmup="none")
        for exception in [RuntimeError, KeyboardInterrupt, SystemExit]:
            callbacks = []

            class Broken(logging.Handler):
                def emit(handler, record):
                    callbacks.append(record)
                    with self.assertRaisesRegex(RuntimeError, "logging callback"):
                        table.scan()
                    # Pure option validation is safe inside a handler and cannot log recursively.
                    with self.assertRaises(ValueError):
                        ScanExecutionOptions(output_buffer_batches_per_partition=sys.maxsize)
                    raise exception("private-handler-failure")

            self.logger.handlers = [Broken()]
            expected = DeltaReaderError if exception is RuntimeError else exception
            with self.assertRaises(expected) as raised:
                table.scan(filters=[("private_missing", "==", 1)])
            if exception is RuntimeError:
                self.assertEqual(raised.exception.code, "unsupported_predicate")
            else:
                self.assertEqual(str(raised.exception), "private-handler-failure")
            self.assertEqual(len(callbacks), 1)
        self.logger.handlers = [self.collect]
        self.collect.records.clear()
        table.scan(limit=0).close()
        self.assertEqual(self.events(), ["scan_planning.started", "scan_planning.completed"])

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires Linux file descriptor limits")
    def test_runtime_creation_failure_is_diagnosable_and_recoverable(self):
        result = subprocess.run([sys.executable, "-I", "-c", """
import errno
import logging
import resource
import sys
from delta_arrow_reader import DeltaTable, Session
records = []
class Collect(logging.Handler):
    def emit(self, record):
        records.append(record)
logger = logging.getLogger('delta_arrow_reader')
logger.handlers = [Collect()]
logger.propagate = False
for level in [40, 30, 20, 10, 5]:
    logger.setLevel(level)
    for kind, operation in [('reader', lambda: DeltaTable(sys.argv[1], warmup='none')), ('SQL', Session)]:
        records.clear()
        limits = resource.getrlimit(resource.RLIMIT_NOFILE)
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (0, limits[1]))
            try:
                operation()
            except RuntimeError as error:
                assert str(error) == f'failed to create the {kind} runtime', str(error)
            else:
                raise AssertionError('runtime started without file descriptors')
        finally:
            resource.setrlimit(resource.RLIMIT_NOFILE, limits)
        record, = records
        assert record.levelno == logging.ERROR
        fields = record.dar_fields
        assert fields['event'] == 'runtime_creation.failed'
        assert fields['runtime'] == kind
        assert fields['error_os_code'] == errno.EMFILE, fields
        assert fields['error_source_kind'] == 'unknown', fields
        assert sys.argv[1] not in repr(record.__dict__)
table = DeltaTable(sys.argv[1], warmup='none')
session = Session()
session.register_table('events', table)
with session.sql('SELECT id FROM events LIMIT 1') as reader:
    assert reader.read_all().num_rows == 1
""", str(FIXTURE)], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "")

    def test_sql_planning_failures_keep_safe_source_categories(self):
        table = DeltaTable(FIXTURE, warmup="none")
        session = Session()
        session.register_table("events", table)
        self.logger.setLevel(5)
        for query, category in (
            ("SELECT 'private-literal' FROM", "sql_parse"),
            ("SELECT * FROM private_table", "datafusion_plan"),
            ("SELECT private_column FROM events", "schema"),
            ("COPY events TO 's3://private-bucket/private-file' STORED AS CSV", "datafusion_plan"),
        ):
            with self.subTest(category=category):
                self.collect.records.clear()
                with self.assertRaises(DeltaReaderError) as raised:
                    session.sql(query)
                self.assertEqual(len(self.collect.records), 1)
                self.assert_failure("sql_planning.failed", target="delta_arrow_reader_python",
                                    error_source_kind=category, error_phase="datafusion",
                                    error_code="sql_planning")
                self.assertEqual((raised.exception.phase, raised.exception.code),
                                 ("datafusion", "sql_planning"))
                self.assertIsNone(raised.exception.reason)
                self.assertIsNone(raised.exception.source_kind)

    def test_planning_observes_logging_changes_after_session_creation(self):
        self.logger.setLevel(logging.CRITICAL)
        table = DeltaTable(FIXTURE, warmup="none")
        session = Session()
        session.register_table("events", table)
        with self.assertRaises(DeltaReaderError):
            session.sql("SELECT * FROM private_missing")
        self.assertEqual(self.collect.records, [])
        self.logger.setLevel(logging.INFO)
        table.scan(limit=0).close()
        with self.assertRaises(DeltaReaderError):
            session.sql("SELECT * FROM private_missing")
        self.assertEqual(self.events(), [
            "scan_planning.started", "scan_planning.completed", "sql_planning.failed",
        ])
        self.collect.records.clear()
        self.collect.setLevel(logging.CRITICAL)
        with self.assertRaises(DeltaReaderError):
            session.sql("SELECT * FROM private_missing")
        self.assertEqual(self.collect.records, [])

    def test_configuration_changes_apply_to_an_existing_table(self):
        self.logger.setLevel(logging.ERROR)
        table = DeltaTable(FIXTURE, warmup="none")
        self.assertEqual(self.collect.records, [])
        self.logger.setLevel(logging.INFO)
        table.refresh()
        self.assertEqual(self.events(), ["table_refresh.started", "table_refresh.completed"])
        self.collect.records.clear()
        self.collect.setLevel(logging.ERROR)
        table.refresh()
        self.assertEqual(self.collect.records, [])
        self.collect.setLevel(logging.INFO)
        self.logger.disabled = True
        table.refresh()
        self.assertEqual(self.collect.records, [])
        self.logger.disabled = False
        previous_disable = logging.root.manager.disable
        try:
            logging.disable(logging.CRITICAL)
            table.refresh()
        finally:
            logging.disable(previous_disable)
        self.assertEqual(self.collect.records, [])
        table.refresh()
        self.assertEqual(self.events(), ["table_refresh.started", "table_refresh.completed"])

    def test_streams_start_on_consumption_and_keep_distinct_contexts_across_threads(self):
        table = DeltaTable(FIXTURE, warmup="none")
        for backend in ["direct", "delta_kernel"]:
            with self.subTest(backend=backend):
                options = ScanExecutionOptions(parquet_backend=backend)
                first = pa.RecordBatchReader.from_stream(
                    table.scan(target_partitions=1, execution_options=options),
                )
                second = table.to_reader(target_partitions=2, execution_options=options)
                self.collect.records.clear()
                self.assertEqual(first.schema, second.schema)
                self.assertEqual(self.collect.records, [])
                try:
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        batches = list(pool.map(lambda r: r.read_next_batch(), [first, second]))
                    self.assertEqual(self.events(), ["scan_execution.started"] * 2)
                    self.assertCountEqual([r.dar_fields["partition_count"] for r in self.collect.records],
                                          [1, 2])
                    self.assertTrue(all(r.thread != threading.get_ident() for r in self.collect.records))
                    for reader, batch in zip([first, second], batches):
                        self.assertEqual(batch.num_rows + reader.read_all().num_rows, 360)
                        with self.assertRaises(StopIteration):
                            reader.read_next_batch()
                finally:
                    first.close()
                    second.close()
                by_operation = {}
                for record in self.collect.records:
                    by_operation.setdefault(record.dar_fields["operation_id"], []).append(record)
                self.assertEqual(len(by_operation), 2)
                self.assertEqual(len({r.dar_fields["plan_id"] for r in self.collect.records}), 2)
                for records in by_operation.values():
                    self.assertEqual([r.msg for r in records],
                                     ["scan_execution.started", "scan_execution.completed"])
                    start, end = records
                    self.assertEqual(start.dar_fields["plan_id"], end.dar_fields["plan_id"])
                    self.assertEqual(end.dar_fields["completion_reason"], "eof")
                    self.assertTrue(end.dar_fields["output_started"])
                    self.assertTrue(all(r.levelno == logging.INFO for r in records))
                    expected_backend = {"direct": "Direct", "delta_kernel": "DeltaKernel"}[backend]
                    self.assertTrue(all(r.dar_fields["backend"] == expected_backend for r in records))

    def test_stream_close_limits_and_live_levels(self):
        table = DeltaTable(FIXTURE, warmup="none")
        unused = table.to_reader()
        self.collect.records.clear()
        unused.close()
        self.assertEqual(self.collect.records, [])
        for backend in ["direct", "delta_kernel"]:
            options = ScanExecutionOptions(parquet_backend=backend)
            reader = table.to_reader(target_partitions=1, execution_options=options)
            self.collect.records.clear()
            self.logger.setLevel(logging.ERROR)
            batch = reader.read_next_batch()
            self.assertEqual(self.collect.records, [])
            self.logger.setLevel(logging.INFO)
            reader.close()
            reader.close()
            self.assertEqual(self.events(), ["scan_execution.dropped"])
            self.assertEqual(self.collect.records[0].levelno, logging.INFO)
            self.assertTrue(self.collect.records[0].dar_fields["output_started"])
            self.assertGreater(batch.num_rows, 0)
            self.assertLess(batch.num_rows, 360)
            for limit in [0, 1]:
                reader = table.to_reader(limit=limit, execution_options=options)
                self.collect.records.clear()
                with reader:
                    self.assertEqual(reader.read_all().num_rows, limit)
                self.assertEqual(self.events(), ["scan_execution.started", "scan_execution.completed"])
                self.assertEqual(self.collect.records[-1].dar_fields["completion_reason"], "row_limit")

    def test_stream_failures_log_once_even_when_arrow_replays_the_error(self):
        with tempfile.TemporaryDirectory(prefix="private-stream-") as temporary:
            location = Path(temporary) / "private-table"
            shutil.copytree(FIXTURE, location)
            for path in location.rglob("*.parquet"):
                path.unlink()
            table = DeltaTable(location, warmup="none")
            self.logger.setLevel(5)
            for backend in ["direct", "delta_kernel"]:
                with self.subTest(backend=backend):
                    reader = table.to_reader(target_partitions=1,
                                             execution_options=ScanExecutionOptions(parquet_backend=backend))
                    self.collect.records.clear()
                    message = self.assert_replayed_error(reader, "data_file_read", "data_file_read")
                    failure = self.assert_failure("scan_execution.failed", error_phase="data_file_read",
                                                  error_code="data_file_read", output_started=False)
                    self.assertIn("error_reason", failure.dar_fields)
                    self.assertIn("error_source_kind", failure.dar_fields)
                    self.assertNotIn(".parquet", repr([r.__dict__ for r in self.collect.records]) + message)
                    self.assertNotIn("scan_execution.completed", self.events())
                    self.assertNotIn("scan_execution.dropped", self.events())

    def test_callbacks_preserve_control_flow_without_deadlocks_or_retained_tracebacks(self):
        for action in ("stream-interrupt", "stream-close", "sql-interrupt", "sql-close",
                       "reentry", "interrupt", "registration"):
            with self.subTest(action=action):
                result = subprocess.run([sys.executable, "-I", __file__, action],
                                        capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stderr, "")

    def test_sql_consumption_forwards_partition_lifecycles_from_workers(self):
        table = DeltaTable(FIXTURE, warmup="none")
        for backend in ["direct", "delta_kernel"]:
            with self.subTest(backend=backend):
                session = Session()
                session.register_table("private_events", table,
                                       execution_options=ScanExecutionOptions(parquet_backend=backend))
                self.collect.records.clear()
                unused = session.sql("SELECT * FROM private_events")
                unused.close()
                readers = [session.sql("SELECT * FROM private_events") for _ in range(2)]
                self.assertEqual(self.collect.records, [])

                def consume(reader):
                    with reader:
                        return threading.get_ident(), reader.read_all().num_rows

                with ThreadPoolExecutor(max_workers=2) as pool:
                    results = list(pool.map(consume, readers))
                self.assertEqual([rows for _, rows in results], [360, 360])
                callers = {thread for thread, _ in results}
                partitions = [r for r in self.collect.records
                              if r.msg.startswith("datafusion_partition_execution.")]
                self.assertTrue(partitions)
                self.assertTrue(all(r.thread not in callers for r in partitions))
                plans = {r.dar_fields["plan_id"] for r in partitions}
                self.assertEqual(len(plans), 2)
                by_operation = {}
                for record in partitions:
                    self.assertEqual(record.levelno, logging.INFO)
                    self.assertEqual(record.dar_fields["snapshot_version"], table.version)
                    by_operation.setdefault(record.dar_fields["operation_id"], []).append(record)
                for records in by_operation.values():
                    self.assertEqual([r.msg for r in records],
                                     ["datafusion_partition_execution.started",
                                      "datafusion_partition_execution.completed"])
                    self.assertEqual(records[0].dar_fields["plan_id"], records[1].dar_fields["plan_id"])
                rendered = repr([r.__dict__ for r in self.collect.records])
                self.assertNotIn("private_events", rendered)
                self.assertNotIn(str(FIXTURE), rendered)

    def test_sql_operator_failures_keep_distinct_safe_categories_and_replay_once(self):
        session = Session()
        table = DeltaTable(FIXTURE, warmup="none")
        session.register_table("private_events", table)
        self.logger.setLevel(5)
        for query, category in (
            ("SELECT CAST('private-literal' AS INT)", "arrow_cast"),
            ("SELECT CAST(label AS INT) FROM private_events WHERE label IS NOT NULL", "arrow_cast"),
            ("SELECT id / (id - id) FROM private_events", "divide_by_zero"),
        ):
            with self.subTest(category=category, query=query):
                self.collect.setLevel(logging.CRITICAL)
                reader = session.sql(query)
                self.collect.records.clear()
                self.collect.setLevel(5)
                message = self.assert_replayed_error(reader, "datafusion", "sql_execution")
                self.assert_failure("sql_execution.failed", target="delta_arrow_reader_python",
                                    error_source_kind=category, error_code="sql_execution")
                self.assertNotIn("value-", repr([r.__dict__ for r in self.collect.records]) + message)
        with session.sql("SELECT 42 AS answer") as reader:
            self.assertEqual(reader.read_all().to_pylist(), [{"answer": 42}])

    def test_sql_core_failures_have_one_owner(self):
        with tempfile.TemporaryDirectory(prefix="private-sql-") as temporary:
            location = Path(temporary) / "private-table"
            shutil.copytree(FIXTURE, location)
            table = DeltaTable(location, warmup="none")
            for path in (location / "region=east").glob("*.parquet"):
                path.unlink()
            self.logger.setLevel(5)
            for backend in ["direct", "delta_kernel"]:
                with self.subTest(backend=backend):
                    session = Session()
                    session.register_table("private_events", table,
                                           execution_options=ScanExecutionOptions(parquet_backend=backend))
                    reader = session.sql("SELECT * FROM private_events")
                    self.collect.records.clear()
                    self.assert_replayed_error(reader, "datafusion", "sql_execution")
                    failure = self.assert_failure("datafusion_partition_execution.failed",
                                                  error_source_kind="not_found", error_code="data_file_read")
                    self.assertIn("operation_id", failure.dar_fields)
                    self.assertIn("plan_id", failure.dar_fields)
                    self.assertNotEqual(failure.thread, threading.get_ident())
                    rendered = repr([r.__dict__ for r in self.collect.records])
                    self.assertNotIn("region=east", rendered)
                    self.assertNotIn(".parquet", rendered)

    def test_sql_deferred_provider_planning_failure_is_not_reported_twice(self):
        with tempfile.TemporaryDirectory() as temporary:
            location = Path(temporary) / "private-table"
            shutil.copytree(FIXTURE, location)
            table = DeltaTable(location, warmup="none")
            session = Session()
            session.register_table("events", table)
            reader = session.sql("SELECT * FROM events")
            (location / "_delta_log/00000000000000000000.json").write_text("private-invalid-json\n")
            self.collect.records.clear()
            self.logger.setLevel(logging.ERROR)
            with reader:
                for _ in range(3):
                    with self.assertRaises(pa.ArrowInvalid):
                        reader.read_all()
            self.assertEqual(self.events(), ["provider_scan.failed"])
            self.assertEqual(self.collect.records[0].dar_fields["error_source_kind"], "arrow_json")
            rendered = repr(self.collect.records[0].__dict__)
            self.assertNotIn("private", rendered)
            self.assertNotIn(temporary, rendered)

    def test_interpreter_shutdown_with_live_logged_readers_is_quiet(self):
        result = subprocess.run([sys.executable, "-I", "-c", """
import logging
import sys
from delta_arrow_reader import DeltaTable, Session
records = []
class Collect(logging.Handler):
    def emit(self, record):
        records.append(record.msg)
logger = logging.getLogger('delta_arrow_reader')
logger.handlers = [Collect()]
logger.setLevel(logging.INFO)
logger.propagate = False
table = DeltaTable(sys.argv[1], warmup='none')
session = Session()
session.register_table('events', table)
ordinary = table.to_reader(target_partitions=1)
sql = session.sql('SELECT * FROM events')
assert ordinary.read_next_batch().num_rows > 0
assert sql.read_next_batch().num_rows > 0
assert 'datafusion_partition_execution.started' in records
# Keep both readers and their subscribers alive until interpreter finalization.
""", str(FIXTURE)], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "")

    def test_worker_events_and_concurrent_refreshes_keep_distinct_ids(self):
        self.logger.setLevel(logging.DEBUG)
        table = DeltaTable(FIXTURE, warmup="query_planning")
        self.assertTrue(any(r.thread != threading.get_ident() for r in self.collect.records))
        self.logger.setLevel(logging.INFO)
        self.collect.records.clear()
        with ThreadPoolExecutor(max_workers=3) as pool:
            versions = list(pool.map(lambda _: table.refresh().version, range(3)))
        self.assertEqual(versions, [table.version] * 3)
        by_operation = {}
        for record in self.collect.records:
            by_operation.setdefault(record.dar_fields["operation_id"], []).append(record.dar_fields["event"])
        self.assertEqual(len(by_operation), 3)
        for events in by_operation.values():
            self.assertEqual(events, ["table_refresh.started", "table_refresh.completed"])

    def test_handler_errors_do_not_change_results_or_leak_callback_state(self):
        class Broken(logging.Handler):
            def emit(self, record):
                raise RuntimeError("private handler failure")

        self.logger.handlers = [Broken()]
        table = DeltaTable(FIXTURE, warmup="none")
        self.assertEqual(table.refresh().version, table.version)
        with self.assertRaises(DeltaReaderError):
            DeltaTable(FIXTURE / "private-missing", warmup="none")
        with self.assertRaises(DeltaReaderError):
            table.scan(filters=[("private_missing", "==", 1)])
        with self.assertRaises(ValueError):
            ScanExecutionOptions(output_buffer_batches_per_partition=sys.maxsize)
        table.scan(limit=0).close()
        with table.to_reader() as reader:
            self.assertEqual(reader.read_all().num_rows, 360)
        session = Session()
        session.register_table("events", table)
        with self.assertRaises(DeltaReaderError):
            session.sql("SELECT * FROM private_missing")
        with session.sql("SELECT 42 AS answer") as reader:
            self.assertEqual(reader.read_all().to_pylist(), [{"answer": 42}])
        with session.sql("SELECT * FROM events") as reader:
            self.assertEqual(reader.read_all().num_rows, 360)
        self.logger.handlers = [self.collect]
        table.refresh()
        self.assertEqual(self.events(), ["table_refresh.started", "table_refresh.completed"])

    def test_import_and_caught_errors_are_quiet_without_configuration(self):
        result = subprocess.run([sys.executable, "-I", "-c", """
import logging
import sys
root = logging.getLogger()
previous = (root.handlers[:], root.level)
from delta_arrow_reader import DeltaTable, DeltaReaderError, ScanExecutionOptions, Session
assert (root.handlers, root.level) == previous
try:
    DeltaTable('unsupported-private-scheme://secret-token/private-table')
except DeltaReaderError:
    pass
else:
    raise AssertionError('invalid location was accepted')
table = DeltaTable(sys.argv[1], warmup='none')
session = Session()
for operation in [lambda: table.scan(columns=['private_missing']),
                  lambda: table.scan(filters=[('private_missing', '==', 1)]),
                  lambda: ScanExecutionOptions(output_buffer_batches_per_partition=sys.maxsize),
                  lambda: session.register_table('private-invalid', table),
                  lambda: session.sql('SELECT 42 AS "private\\0alias"'),
                  lambda: session.sql('SELECT * FROM private_missing')]:
    try:
        operation()
    except (DeltaReaderError, ValueError):
        pass
    else:
        raise AssertionError('invalid operation was accepted')
assert (root.handlers, root.level) == previous
with table.to_reader() as reader:
    assert reader.read_all().num_rows == 360
session.register_table('events', table)
with session.sql('SELECT * FROM events') as reader:
    assert reader.read_all().num_rows == 360
with session.sql("SELECT CAST('private-literal' AS INT)") as reader:
    try:
        reader.read_all()
    except Exception:
        pass
    else:
        raise AssertionError('invalid SQL cast succeeded')
""", str(FIXTURE)], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "")

    def test_root_handler_configured_before_import_receives_events(self):
        result = subprocess.run([sys.executable, "-I", "-c", """
import logging
records = []
class Collect(logging.Handler):
    def emit(self, record):
        records.append(record)
root = logging.getLogger()
handler = Collect()
root.addHandler(handler)
root.setLevel(logging.ERROR)
from delta_arrow_reader import DeltaTable, DeltaReaderError
try:
    DeltaTable('unsupported-private-scheme://secret/private-table')
except DeltaReaderError:
    pass
assert root.handlers == [handler]
assert root.level == logging.ERROR
assert len(records) == 1
assert records[0].name == 'delta_arrow_reader'
assert records[0].dar_fields['event'] == 'table_load.failed'
"""], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")


def check_reentry():
    table = DeltaTable(FIXTURE, warmup="none")
    stream = table.scan(limit=0)
    reader = table.to_reader(limit=1)
    session = Session()
    session.register_table("events", table)
    operations = [
        lambda: DeltaTable(FIXTURE), table.refresh, table.scan, table.to_reader,
        Session, lambda: session.register_table("other", table),
        lambda: session.sql("SELECT * FROM events"),
        stream.__arrow_c_stream__, stream.close,
    ]
    rejected = []
    unexpected = []

    class Reenter(logging.Handler):
        def emit(self, record):
            for operation in operations:
                try:
                    operation()
                except RuntimeError as error:
                    if str(error) == "reader operations cannot be called from a logging callback":
                        rejected.append(record.thread)
                    else:
                        unexpected.append(repr(error))
                except BaseException as error:
                    unexpected.append(repr(error))
                else:
                    unexpected.append("recursive operation succeeded")
            try:
                reader.read_next_batch()
            except pa.ArrowException as error:
                if "reader operations cannot be called from a logging callback" not in str(error):
                    unexpected.append(str(error))
            except BaseException as error:
                unexpected.append(repr(error))
            else:
                unexpected.append("recursive Arrow read succeeded")

    logger = logging.getLogger("delta_arrow_reader")
    logger.handlers = [Reenter()]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    assert table.refresh().version == table.version
    table.scan(limit=0).close()
    session.register_table("another", table)
    try:
        session.sql("SELECT * FROM private_missing")
    except DeltaReaderError:
        pass
    else:
        raise AssertionError("invalid SQL was accepted")
    logger.handlers = [logging.NullHandler()]
    assert not unexpected, unexpected
    assert rejected
    assert any(thread != threading.get_ident() for thread in rejected)
    # Rejected operations must leave the exporter available and the runtime usable.
    assert stream.__arrow_c_stream__() is not None
    assert reader.read_all().num_rows == 1
    reader.close()
    assert table.refresh().version == table.version


def check_concurrent_registration():
    table = DeltaTable(FIXTURE, warmup="none")
    session = Session()
    first_entered = threading.Event()
    second_attempted = threading.Event()
    overlapped = []

    class Pause(logging.Handler):
        def emit(self, record):
            if record.msg == "provider_registration.started" and not first_entered.is_set():
                first_entered.set()
                overlapped.append(second_attempted.wait(5))
                # Simulate a handler doing I/O while another caller takes the catalog lock.
                time.sleep(0.1)

    logger = logging.getLogger("delta_arrow_reader")
    logger.handlers = [Pause()]
    logger.setLevel(logging.INFO)
    logger.propagate = False

    def register(second):
        if second:
            assert first_entered.wait(5)
            second_attempted.set()
        try:
            session.register_table("events", table)
            return "registered"
        except DeltaReaderError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(register, [False, True]))
    assert first_entered.is_set() and second_attempted.is_set()
    assert overlapped == [True], overlapped
    assert sorted(results) == ["datafusion_adapter", "registered"], results
    with session.sql("SELECT id FROM events LIMIT 1") as reader:
        assert reader.read_all().num_rows == 1


def check_interrupt():
    table = DeltaTable(FIXTURE, warmup="none")
    session = Session()
    calls = []
    interruption = KeyboardInterrupt("original signal exception")

    def on_signal(number, frame):
        calls.append(number)
        raise interruption

    class Interrupt(logging.Handler):
        def emit(self, record):
            if record.msg == self.event:
                signal.raise_signal(signal.SIGINT)

    logger = logging.getLogger("delta_arrow_reader")
    handler = Interrupt()
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False
    previous = signal.signal(signal.SIGINT, on_signal)
    try:
        for event, operation in [
            ("table_load.started", lambda: DeltaTable(FIXTURE, warmup="none")),
            ("scan_planning.started", lambda: table.scan(limit=0)),
            ("sql_planning.failed", lambda: session.sql("SELECT * FROM private_missing")),
        ]:
            handler.event = event
            try:
                operation()
            except KeyboardInterrupt as error:
                assert error is interruption
            else:
                raise AssertionError("logging swallowed Ctrl+C")
    finally:
        signal.signal(signal.SIGINT, previous)
    assert calls == [signal.SIGINT] * 3, calls
    exit_request = SystemExit(17)

    class Exit(logging.Handler):
        def emit(self, record):
            raise exit_request

    logger.handlers = [Exit()]
    try:
        DeltaTable(FIXTURE, warmup="none")
    except SystemExit as error:
        assert error is exit_request
    else:
        raise AssertionError("logging swallowed SystemExit")
    logger.handlers = [logging.NullHandler()]
    assert DeltaTable(FIXTURE, warmup="none").version == 0


def check_stream_interrupt():
    table = DeltaTable(FIXTURE, warmup="none")
    logger = logging.getLogger("delta_arrow_reader")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    class Marker:
        pass

    for exception in [KeyboardInterrupt, SystemExit]:
        for event in ["scan_execution.started", "scan_execution.completed"]:
            reader = table.to_reader(target_partitions=1)
            references = []
            events = []
            if event.endswith("completed"):
                assert reader.read_next_batch().num_rows > 0

            class Interrupt(logging.Handler):
                def emit(self, record):
                    events.append(record.msg)
                    if record.msg == event:
                        marker = Marker()
                        references.append(weakref.ref(marker))
                        # A retained traceback would keep both the marker and reader alive.
                        current_reader = reader
                        raise exception("private handler control flow")

            logger.handlers = [Interrupt()]
            try:
                for _ in range(3):
                    try:
                        reader.read_all()
                    except pa.ArrowInvalid as error:
                        assert "phase=execution code=python_signal reason=reader_wait_interrupted" in str(error)
                        assert "private" not in str(error)
                    else:
                        raise AssertionError("stream swallowed a handler interruption")
                assert events.count(event) == 1, events
                assert not any(name.endswith("failed") for name in events), events
                gc.collect()
                assert len(references) == 1 and references[0]() is None
            finally:
                logger.handlers = [logging.NullHandler()]
                reader.close()
    with table.to_reader() as reader:
        assert reader.read_all().num_rows == 360


def check_stream_close(*, sql=False):
    table = DeltaTable(FIXTURE, warmup="none")
    session = Session()
    session.register_table("events", table)
    logger = logging.getLogger("delta_arrow_reader")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for worker in [False, True]:
        reader = (session.sql("SELECT * FROM events")
                  if sql else table.to_reader(target_partitions=1))
        idle = table.to_reader()
        rejected = []
        unexpected = []
        calls = []

        class Close(logging.Handler):
            def emit(self, record):
                expected = "datafusion_partition_execution.started" if sql else "scan_execution.started"
                if record.msg != expected or calls:
                    return
                calls.append(record.thread)
                for target in [reader, idle]:
                    try:
                        target.read_next_batch()
                    except pa.ArrowInvalid as error:
                        rejected.append(str(error))
                    except BaseException as error:
                        unexpected.append(repr(error))
                    else:
                        unexpected.append("recursive read succeeded")
                reader.close()
                idle.close()

        logger.handlers = [Close()]
        try:
            if worker:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    batch = pool.submit(reader.read_next_batch).result(10)
            else:
                batch = reader.read_next_batch()
            assert 0 < batch.num_rows < 360
            assert batch.to_pylist()
            assert len(rejected) == 2 and not unexpected, (rejected, unexpected)
            assert all("reader operations cannot be called from a logging callback" in x for x in rejected)
            assert len(calls) == 1
            assert (calls[0] != threading.get_ident()) == (worker or sql)
            for target in [reader, idle]:
                try:
                    target.read_next_batch()
                except pa.ArrowInvalid as error:
                    assert "closed" in str(error)
                else:
                    raise AssertionError("closed reader was still readable")
        finally:
            logger.handlers = [logging.NullHandler()]
            reader.close()
            idle.close()
    with table.to_reader() as reader:
        assert reader.read_all().num_rows == 360


def check_sql_interrupt():
    table = DeltaTable(FIXTURE, warmup="none")
    session = Session()
    session.register_table("events", table)
    logger = logging.getLogger("delta_arrow_reader")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for exception in [KeyboardInterrupt, SystemExit]:
        reader = session.sql("SELECT * FROM events")
        called = []

        class Interrupt(logging.Handler):
            def emit(self, record):
                if record.msg == "datafusion_partition_execution.started" and not called:
                    called.append(threading.get_ident())
                    raise exception("private handler interruption")

        logger.handlers = [Interrupt()]
        try:
            for _ in range(3):
                try:
                    reader.read_all()
                except pa.ArrowInvalid as error:
                    assert "phase=execution code=python_signal reason=reader_wait_interrupted" in str(error)
                    assert "private" not in str(error)
                else:
                    raise AssertionError("SQL swallowed a worker handler interruption")
            assert len(called) == 1 and called[0] != threading.get_ident(), called
        finally:
            logger.handlers = [logging.NullHandler()]
            reader.close()
    with session.sql("SELECT 42 AS answer") as reader:
        assert reader.read_all().to_pylist() == [{"answer": 42}]


if __name__ == "__main__":
    if sys.argv[1:] == ["reentry"]:
        check_reentry()
    elif sys.argv[1:] == ["interrupt"]:
        check_interrupt()
    elif sys.argv[1:] == ["registration"]:
        check_concurrent_registration()
    elif sys.argv[1:] == ["stream-interrupt"]:
        check_stream_interrupt()
    elif sys.argv[1:] == ["stream-close"]:
        check_stream_close()
    elif sys.argv[1:] == ["sql-close"]:
        check_stream_close(sql=True)
    elif sys.argv[1:] == ["sql-interrupt"]:
        check_sql_interrupt()
    else:
        unittest.main()
