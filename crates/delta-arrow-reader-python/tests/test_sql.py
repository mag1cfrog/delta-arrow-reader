from collections import defaultdict
from functools import partial
import gc
import importlib.util
from pathlib import Path
import tempfile
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

    def test_registration_validates_python_arguments(self):
        for name, table in ((None, self.table), ("valid", object())):
            with self.subTest(name=name), self.assertRaises(TypeError):
                self.session.register_table(name, table)
        with self.assertRaises(TypeError):
            self.session.register_table("valid", self.table, execution_options={})
        self.assertEqual(self.query("SELECT COUNT(*) AS n FROM events"), [{"n": len(self.rows)}])

    def test_rejected_sql_has_no_side_effects_or_sensitive_errors(self):
        with tempfile.TemporaryDirectory(prefix="secret-sql-") as directory:
            output = Path(directory) / "output.csv"
            queries = [
                "", "SELECT", "SELECT * FROM secret_missing",
                "SELECT secret_missing FROM events", "SELECT 1; SELECT 2",
                "CREATE TABLE changed AS SELECT * FROM events",
                "DROP TABLE events", "INSERT INTO events SELECT * FROM events",
                "DELETE FROM events", "UPDATE events SET id = 1",
                "SET datafusion.execution.target_partitions = 1", "BEGIN TRANSACTION",
                f"COPY events TO '{output.as_posix()}' STORED AS CSV",
            ]
            for query in queries:
                with self.subTest(query=query), self.assertRaises(DeltaReaderError) as caught:
                    self.session.sql(query)
                error = caught.exception
                self.assertEqual((error.phase, error.code), ("datafusion", "sql_planning"))
                self.assertNotIn("secret", str(error))
                self.assertIsNone(error.__cause__)
                self.assertIsNone(error.__context__)
                self.assertEqual(self.query("SELECT COUNT(*) AS n FROM events"),
                                 [{"n": len(self.rows)}])
            self.assertFalse(output.exists())
            with self.assertRaises(DeltaReaderError):
                self.session.sql("SELECT * FROM changed")
        for query in (None, 42, b"SELECT 1"):
            with self.subTest(query=query), self.assertRaises(TypeError):
                self.session.sql(query)

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
