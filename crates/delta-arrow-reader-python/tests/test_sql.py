from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import gc
import importlib.util
from pathlib import Path
import tempfile
from threading import Event
import unittest
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.parquet as pq

from delta_arrow_reader import DeltaReaderError, DeltaTable, ScanExecutionOptions, Session


class SqlTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parents[3]
        self.corpus = (self.root / "crates/delta-arrow-reader-core/tests/reader/fixtures"
                       / "external_writer/corpus")
        self.table = DeltaTable(self.corpus / "partitioned/table")
        self.session = Session()
        self.session.register_table("events", self.table)
        self.rows = self.expected_rows("partitioned")

    def expected_rows(self, fixture):
        with pa.ipc.open_file(self.corpus / fixture / "expected.arrow") as source:
            return source.read_all().to_pylist()

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

    def test_installed_sql_delta_fixtures_and_backends(self):
        for fixture in ("partitioned", "deletion_vectors", "nested_mapping"):
            table = DeltaTable(self.corpus / fixture / "table")
            expected = sorted(
                (row for row in self.expected_rows(fixture) if row["id"] >= 1),
                key=lambda row: row["id"],
            )
            for backend in ("direct", "delta_kernel"):
                with self.subTest(fixture=fixture, backend=backend):
                    name = f"{fixture}_{backend}"
                    self.session.register_table(
                        name, table,
                        execution_options=ScanExecutionOptions(parquet_backend=backend),
                    )
                    self.assertEqual(self.query(
                        f"SELECT * FROM {name} WHERE id >= 1 ORDER BY id"
                    ), expected)

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

    def test_registration_errors_preserve_the_catalog(self):
        for name in ("events", "", "bad.name", "secret-name", "select"):
            with self.subTest(name=name), self.assertRaises(DeltaReaderError) as caught:
                self.session.register_table(name, self.table)
            self.assertEqual(caught.exception.phase, "datafusion")
            self.assertEqual(caught.exception.code, "sql_registration")
            self.assertNotIn("secret", str(caught.exception))
        for name, table in ((None, self.table), ("valid", object())):
            with self.subTest(name=name), self.assertRaises(TypeError):
                self.session.register_table(name, table)
        with self.assertRaises(TypeError):
            self.session.register_table("valid", self.table, execution_options={})
        self.assertEqual(self.query("SELECT COUNT(*) AS n FROM events"), [{"n": len(self.rows)}])

    def test_concurrent_duplicate_registration_has_one_winner(self):
        def register():
            try:
                self.session.register_table("concurrent", self.table)
                return "registered"
            except DeltaReaderError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: register(), range(2)))
        self.assertCountEqual(results, ["registered", "sql_registration"])
        self.assertEqual(self.query("SELECT COUNT(*) AS n FROM concurrent"), [{"n": len(self.rows)}])

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

    def test_execution_error_is_terminal_and_session_remains_usable(self):
        with self.session.sql("SELECT CAST(label AS INT) FROM events WHERE label IS NOT NULL") as reader:
            for _ in range(2):
                with self.assertRaises(pa.ArrowInvalid) as caught:
                    reader.read_next_batch()
                self.assertIn("phase=datafusion code=sql_execution", str(caught.exception))
                self.assertNotIn("label", str(caught.exception))
        self.assertEqual(self.query("SELECT 42 AS answer"), [{"answer": 42}])

    def test_schema_export_rejects_nul_alias_without_panicking(self):
        with self.assertRaises(DeltaReaderError) as caught:
            self.session.sql('SELECT 1 AS "secret\0alias"')
        self.assertEqual(caught.exception.code, "schema_conversion")
        self.assertNotIn("secret", str(caught.exception))

    def test_refresh_does_not_retarget_registered_or_planned_snapshots(self):
        location = self.corpus / "deletion_vectors/table"
        table = DeltaTable(location, version=0)
        self.session.register_table("before_refresh", table)
        reader = self.session.sql("SELECT id FROM before_refresh ORDER BY id")
        updated = table.refresh()
        self.session.register_table("after_refresh", updated)
        original_ids = sorted(pq.read_table(next(location.glob("*.parquet"))).column("id").to_pylist())
        with reader:
            self.assertEqual(reader.read_all().column("id").to_pylist(), original_ids)
        self.assertEqual(self.query("SELECT id FROM before_refresh ORDER BY id"),
                         [{"id": value} for value in original_ids])
        self.assertEqual(self.query("SELECT id FROM after_refresh ORDER BY id"),
                         [{"id": row["id"]} for row in sorted(
                             self.expected_rows("deletion_vectors"), key=lambda row: row["id"],
                         )])

    def test_lazy_execution_partition_pruning_and_residual_filter(self):
        support = self.root / "crates/delta-arrow-reader-core/tests/reader/https.py"
        spec = importlib.util.spec_from_file_location("reader_https", support)
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
            query = "SELECT id FROM remote WHERE region = 'east' AND id >= 100 AND id % 7 = 0"
            with session.sql(query) as reader:
                self.assertFalse(data_requested.wait(0.1), server.requests)
            self.assertFalse(data_requested.wait(0.1), server.requests)
            with session.sql(query) as reader:
                actual = reader.read_all().column("id").to_pylist()
            self.assertEqual(sorted(actual), sorted(row["id"] for row in self.rows
                             if row["region"] == "east" and row["id"] >= 100 and row["id"] % 7 == 0))
            paths = [unquote(path) for path, _, _ in server.requests if path.endswith(".parquet")]
            self.assertTrue(paths)
            self.assertTrue(all("region=east/" in path for path in paths), paths)


if __name__ == "__main__":
    unittest.main()
