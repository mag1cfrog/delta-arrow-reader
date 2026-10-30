from collections import defaultdict
from functools import partial
import gc
import importlib.util
import logging
from pathlib import Path
from threading import Event
import unittest

import pyarrow as pa

from delta_arrow_reader import DeltaReaderError, DeltaTable, Session


class SqlTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parents[3]
        self.corpus = (self.root / "crates/delta-arrow-reader-core/tests/reader/fixtures"
                       / "external_writer/corpus")
        self.table = DeltaTable(self.corpus / "partitioned/table")
        self.session = Session()
        self.session.register_table("events", self.table)
        with pa.ipc.open_file(self.corpus / "partitioned/expected.arrow") as source:
            self.rows = source.read_all().to_pylist()

    def query(self, sql):
        with self.session.sql(sql) as reader:
            self.assertIsInstance(reader, pa.RecordBatchReader)
            return reader.read_all().to_pylist()

    def test_planning_errors_keep_categories_with_logging_disabled(self):
        logger = logging.getLogger("delta_arrow_reader")
        self.addCleanup(setattr, logger, "disabled", logger.disabled)
        logger.disabled = True
        for query, reason, category in [
            ("SELEC private_query FROM events", "sql_planning", "sql_parse"),
            ("SELECT private_column FROM events", "column_not_found", "schema"),
            ("SELECT id FROM events a JOIN events b ON a.id = b.id", "ambiguous_column", "schema"),
        ]:
            with self.subTest(reason=reason):
                with self.assertRaises(DeltaReaderError) as caught:
                    self.session.sql(query)
                error = caught.exception
                self.assertEqual((error.phase, error.code, error.reason, error.source_kind),
                                 ("datafusion", "sql_planning", reason, category))
                self.assertIsNone(error.cause)
                self.assertIsNone(error.__cause__)
                self.assertIsNone(error.__context__)
                self.assertNotIn("private_", str(error) + repr(vars(error)))

    def test_operator_errors_cross_arrow_boundary_with_logging_disabled(self):
        logger = logging.getLogger("delta_arrow_reader")
        self.addCleanup(setattr, logger, "disabled", logger.disabled)
        logger.disabled = True
        for query, category in [
            ("SELECT id / (id - id) FROM events", "divide_by_zero"),
            ("SELECT CAST('private_literal' AS INT)", "arrow_cast"),
        ]:
            with self.subTest(category=category), self.session.sql(query) as reader:
                messages = []
                for _ in range(2):
                    with self.assertRaises(pa.ArrowInvalid) as caught:
                        reader.read_next_batch()
                    messages.append(str(caught.exception))
                self.assertEqual(messages[0], messages[1])
                self.assertIn("code=sql_execution", messages[0])
                self.assertIn(f"source_kind={category}", messages[0])
                self.assertNotIn("private_", messages[0])
                self.assertNotIn("cause={", messages[0])

    def test_installed_sql_join_aggregate_and_order(self):
        self.session.register_table("details", self.table)
        totals = defaultdict(lambda: [0, 0])
        for row in self.rows:
            if row["id"] >= 100 and row["region"] is not None and row["value"] is not None:
                totals[row["region"]][0] += 1
                totals[row["region"]][1] += row["value"]
        expected = [{"region": region, "n": count, "total": total}
                    for region, (count, total) in sorted(totals.items())][:2]
        actual = self.query("""
            WITH selected AS (
                SELECT id, region FROM events WHERE id >= 100 AND region IS NOT NULL
            )
            SELECT s.region, COUNT(*) AS n, SUM(d.value) AS total
            FROM selected AS s JOIN details AS d ON s.id = d.id
            WHERE d.value IS NOT NULL
            GROUP BY s.region ORDER BY s.region LIMIT 2
        """)
        self.assertEqual(actual, expected)

    def test_installed_sql_empty_and_null_results(self):
        self.assertEqual(self.query("SELECT id FROM events WHERE id < 0"), [])
        self.assertEqual(self.query("SELECT COUNT(*) AS n FROM events WHERE id < 0"), [{"n": 0}])
        self.assertEqual(
            self.query("SELECT id FROM events WHERE region IS NULL ORDER BY id"),
            [{"id": row["id"]} for row in sorted(self.rows, key=lambda row: row["id"])
             if row["region"] is None],
        )

    def test_installed_sql_readers_outlive_session_and_table(self):
        table = DeltaTable(self.corpus / "partitioned/table")
        session = Session()
        session.register_table("events", table)
        reader = session.sql("SELECT id FROM events ORDER BY id LIMIT 10")
        unused = session.sql("SELECT id FROM events")
        unused.close()
        del session, table, unused
        gc.collect()
        try:
            batch = reader.read_next_batch()
            remaining = reader.read_all().to_pylist()
        finally:
            reader.close()
        del reader
        gc.collect()
        self.assertEqual(batch.to_pylist() + remaining,
                         [{"id": row["id"]} for row in sorted(
                             self.rows, key=lambda row: row["id"],
                         )[:10]])

    def test_sql_exports_timestamp_metadata_and_embedded_nul_values(self):
        with self.session.sql("""
            SELECT arrow_cast(NULL, 'Timestamp(Microsecond, Some("UTC"))') AS value
        """) as reader:
            self.assertEqual(reader.schema.field("value").type, pa.timestamp("us", tz="UTC"))
            self.assertEqual(reader.read_all().to_pylist(), [{"value": None}])
        self.assertEqual(self.query("SELECT 'value\0data' AS value"), [{"value": "value\0data"}])

    def test_sql_defers_data_requests_until_consumption(self):
        support = Path(__file__).with_name("_http.py")
        spec = importlib.util.spec_from_file_location("reader_http", support)
        http = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(http)
        data_requested = Event()

        class Storage(http.Storage):
            def send_head(self):
                if self.path.endswith(".parquet"):
                    data_requested.set()
                return super().send_head()

        location = self.corpus / "partitioned/table"
        with http.serve(partial(Storage, directory=str(location))) as server:
            table = DeltaTable(f"http://127.0.0.1:{server.server_port}/",
                               storage_options={"allow_http": "true"})
            session = Session()
            session.register_table("remote", table)
            query = "SELECT id FROM remote LIMIT 1"
            with session.sql(query) as reader:
                self.assertFalse(data_requested.wait(0.1), server.requests)
            self.assertFalse(data_requested.wait(0.1), server.requests)
            with session.sql(query) as reader:
                self.assertEqual(reader.read_all().num_rows, 1)
            self.assertTrue(data_requested.is_set(), server.requests)


if __name__ == "__main__":
    unittest.main()
