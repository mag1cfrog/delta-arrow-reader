from concurrent.futures import ThreadPoolExecutor
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

import pyarrow as pa

from delta_arrow_reader import DeltaReaderError, DeltaTable, Session


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

    def test_load_and_refresh_keep_native_fields(self):
        table = DeltaTable(FIXTURE, warmup="none")
        refreshed = table.refresh()
        self.assertEqual(refreshed.version, table.version)
        self.assertEqual(self.events(), [
            "table_load.started", "table_load.completed",
            "table_refresh.started", "table_refresh.completed",
        ])
        load_start, load_end, refresh_start, refresh_end = self.collect.records
        for start, end in [(load_start, load_end), (refresh_start, refresh_end)]:
            self.assertEqual(start.levelno, logging.INFO)
            self.assertEqual(end.levelno, logging.INFO)
            self.assertEqual(start.dar_fields["operation_id"], end.dar_fields["operation_id"])
            self.assertIs(type(end.dar_fields["operation_id"]), int)
            self.assertIs(type(end.dar_fields["elapsed_micros"]), int)
            self.assertEqual(end.dar_fields["snapshot_version"], table.version)
            self.assertEqual(end.getMessage(), end.dar_fields["event"])
            self.assertEqual(end.dar_target, "delta_arrow_reader")
        self.assertNotEqual(load_start.dar_fields["operation_id"], refresh_start.dar_fields["operation_id"])
        self.assertEqual(refresh_end.dar_fields["previous_snapshot_version"], table.version)

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
                    failures = [r for r in self.collect.records if r.levelno >= logging.ERROR]
                    self.assertEqual(len(failures), 1)
                    failure = failures[0]
                    self.assertEqual(failure.dar_fields["event"], "table_load.failed")
                    self.assertEqual(failure.dar_fields["error_phase"], raised.exception.phase)
                    self.assertEqual(failure.dar_fields["error_code"], raised.exception.code)
                    self.assertIn("error_reason", failure.dar_fields)
                    self.assertIn("error_source_kind", failure.dar_fields)
                    rendered = repr([r.__dict__ for r in self.collect.records])
                    self.assertNotIn(temporary, rendered)
                    self.assertNotIn("private-table", rendered)
                    self.assertNotIn("secret-source-message", rendered)
                    self.assertNotIn("table_load.completed", self.events())

    def test_refresh_failure_preserves_original_table(self):
        with tempfile.TemporaryDirectory() as temporary:
            location = Path(temporary) / "table"
            shutil.copytree(FIXTURE, location)
            table = DeltaTable(location, warmup="none")
            (location / "_delta_log" / "00000000000000000001.json").write_text("invalid json\n")
            self.collect.records.clear()
            with self.assertRaises(DeltaReaderError):
                table.refresh()
            self.assertEqual(self.events(), ["table_refresh.started", "table_refresh.failed"])
            self.assertEqual(self.collect.records[-1].levelno, logging.ERROR)
            self.assertEqual(self.collect.records[-1].dar_fields["previous_snapshot_version"], 0)
            self.assertEqual(table.version, 0)

    def test_planning_and_registration_forward_core_lifecycles(self):
        table = DeltaTable(FIXTURE, warmup="none")
        session = Session()
        self.collect.records.clear()
        table.scan(columns=["id"], limit=0).close()
        session.register_table("private_events", table)
        self.assertEqual(self.events(), [
            "scan_planning.started", "scan_planning.completed",
            "provider_registration.started", "provider_registration.completed",
        ])
        start, end, registration_start, registration_end = self.collect.records
        for first, last in [(start, end), (registration_start, registration_end)]:
            self.assertEqual(first.levelno, logging.INFO)
            self.assertEqual(last.levelno, logging.INFO)
            self.assertEqual(first.dar_fields["operation_id"], last.dar_fields["operation_id"])
            self.assertEqual(last.dar_fields["snapshot_version"], table.version)
        self.assertEqual(end.dar_fields["plan_id"], start.dar_fields["operation_id"])
        self.assertNotEqual(end.dar_fields["operation_id"], registration_end.dar_fields["operation_id"])
        self.assertNotIn("private_events", repr([r.__dict__ for r in self.collect.records]))

    def test_planning_and_registration_failures_have_one_owner(self):
        table = DeltaTable(FIXTURE, warmup="none")
        session = Session()
        session.register_table("private_events", table)
        cases = [
            (lambda: table.scan(columns=["private_column"]),
             "scan_planning.failed", "column_not_found", "scan_planning"),
            (lambda: session.register_table("private_events", table),
             "provider_registration.failed", "table_registration_failed", "datafusion"),
            (lambda: session.register_table("private-invalid", table),
             "provider_registration.failed", "table_registration_name_invalid", "datafusion"),
        ]
        for level in [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG, 5]:
            self.logger.setLevel(level)
            for operation, event, reason, phase in cases:
                with self.subTest(level=level, event=event, reason=reason):
                    self.collect.records.clear()
                    with self.assertRaises(DeltaReaderError) as raised:
                        operation()
                    failures = [r for r in self.collect.records if r.levelno >= logging.ERROR]
                    self.assertEqual(len(failures), 1)
                    failure = failures[0]
                    self.assertEqual(failure.dar_target, "delta_arrow_reader")
                    self.assertEqual(failure.dar_fields["event"], event)
                    self.assertEqual(failure.dar_fields["error_reason"], reason)
                    self.assertEqual(failure.dar_fields["error_phase"], phase)
                    self.assertEqual(raised.exception.phase, phase)
                    self.assertEqual(failure.dar_fields["snapshot_version"], table.version)
                    if event == "provider_registration.failed":
                        self.assertEqual(raised.exception.code, "sql_registration")
                        self.assertEqual(failure.dar_fields["error_code"], "datafusion_adapter")
                    rendered = repr([r.__dict__ for r in self.collect.records])
                    self.assertNotIn("private_", rendered)
                    self.assertNotIn("private-invalid", rendered)
                    self.assertNotIn(str(FIXTURE), rendered)
        with session.sql("SELECT id FROM private_events LIMIT 1") as reader:
            self.assertEqual(reader.read_all().num_rows, 1)

    def test_sql_planning_failures_keep_safe_source_categories(self):
        table = DeltaTable(FIXTURE, warmup="none")
        session = Session()
        session.register_table("events", table)
        cases = [
            ("SELECT 'private-literal' FROM", "sql_parse"),
            ("SELECT * FROM private_table", "datafusion_plan"),
            ("SELECT private_column FROM events", "schema"),
            ("COPY events TO 's3://private-bucket/private-file' STORED AS CSV", "datafusion_plan"),
        ]
        for level in [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG, 5]:
            self.logger.setLevel(level)
            for query, category in cases:
                with self.subTest(level=level, category=category):
                    self.collect.records.clear()
                    with self.assertRaises(DeltaReaderError) as raised:
                        session.sql(query)
                    self.assertEqual(len(self.collect.records), 1)
                    failure = self.collect.records[0]
                    self.assertEqual(failure.levelno, logging.ERROR)
                    self.assertEqual(failure.dar_target, "delta_arrow_reader_python")
                    self.assertEqual(failure.dar_fields["event"], "sql_planning.failed")
                    self.assertEqual(failure.dar_fields["error_source_kind"], category)
                    self.assertEqual((raised.exception.phase, raised.exception.code),
                                     ("datafusion", "sql_planning"))
                    self.assertEqual(failure.dar_fields["error_phase"], raised.exception.phase)
                    self.assertEqual(failure.dar_fields["error_code"], raised.exception.code)
                    rendered = repr(failure.__dict__)
                    self.assertNotIn("private", rendered)
                    self.assertNotIn(str(FIXTURE), rendered)

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
        table.scan(limit=0).close()
        session = Session()
        session.register_table("events", table)
        with self.assertRaises(DeltaReaderError):
            session.sql("SELECT * FROM private_missing")
        with session.sql("SELECT 42 AS answer") as reader:
            self.assertEqual(reader.read_all().to_pylist(), [{"answer": 42}])
        self.logger.handlers = [self.collect]
        table.refresh()
        self.assertEqual(self.events(), ["table_refresh.started", "table_refresh.completed"])

    def test_import_and_caught_errors_are_quiet_without_configuration(self):
        result = subprocess.run([sys.executable, "-I", "-c", """
import logging
import sys
root = logging.getLogger()
previous = (root.handlers[:], root.level)
from delta_arrow_reader import DeltaTable, DeltaReaderError, Session
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
                  lambda: session.register_table('private-invalid', table),
                  lambda: session.sql('SELECT * FROM private_missing')]:
    try:
        operation()
    except DeltaReaderError:
        pass
    else:
        raise AssertionError('invalid operation was accepted')
assert (root.handlers, root.level) == previous
""", str(FIXTURE)], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "")

    def test_callbacks_reject_reader_reentry_without_deadlock(self):
        result = subprocess.run([sys.executable, "-I", __file__, "reentry"],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")

    def test_signal_in_a_logging_callback_still_interrupts_loading_and_planning(self):
        result = subprocess.run([sys.executable, "-I", __file__, "interrupt"],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")

    def test_concurrent_registration_can_log_without_holding_the_gil(self):
        result = subprocess.run([sys.executable, "-I", __file__, "registration"],
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
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
    assert sorted(results) == ["registered", "sql_registration"], results
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


if __name__ == "__main__":
    if sys.argv[1:] == ["reentry"]:
        check_reentry()
    elif sys.argv[1:] == ["interrupt"]:
        check_interrupt()
    elif sys.argv[1:] == ["registration"]:
        check_concurrent_registration()
    else:
        unittest.main()
