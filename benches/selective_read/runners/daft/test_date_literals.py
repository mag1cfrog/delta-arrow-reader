"""Run with the pinned Daft interpreter; no benchmark fixtures are required."""

from datetime import date
import io
import json
from pathlib import Path
import runpy
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
adapter = runpy.run_path(str(HERE / "runner.py"), run_name="date_literal_check")
daft, pa = adapter["daft"], adapter["pa"]
import pyarrow.parquet as pq


class DateLiterals(unittest.TestCase):
    def test_historical_expression_hashes(self):
        catalog = json.loads((HERE.parents[1] / "query-matrix.json").read_text())
        for source in catalog["scales"].values():
            for queries in source["queries"].values():
                for row in queries.values():
                    for revision in (2, 3, 4):
                        expression = adapter["expression_identity"](row["sql"], revision)
                        self.assertEqual(adapter["json_hash"](expression), row["native_expression_sha256"]["daft"])

    def test_values_nulls_ranges_and_quoted_strings(self):
        table = pa.table({"key": [1, 2, 3, 4, 5],
                          "l_shipdate": pa.array([date(1995, 3, 15), date(1995, 3, 14), None,
                                                  date(1995, 3, 15), date(1995, 3, 16)], type=pa.date32()),
                          "label": ["AIR", "AIR", None, "DATE '1995-03-15'", "AIR"]})
        source = daft.from_arrow(table)
        for predicate, expected in (
            ("l_shipdate = DATE '1995-03-15' AND label = 'AIR'", [1]),
            ("l_shipdate >= DATE '1995-03-14' AND l_shipdate < DATE '1995-03-16'", [1, 2, 4]),
            ("label = 'DATE ''1995-03-15''' OR l_shipdate = DATE '1995-03-16'", [4, 5]),
        ):
            sql = "SELECT key FROM bench WHERE " + predicate
            for revision in (2, 5):
                actual = adapter["query"](source, sql, revision).to_arrow().column("key").to_pylist()
                self.assertEqual(sorted(actual), expected)
        sql = "SELECT key FROM bench WHERE l_shipdate = DATE '1995-03-15' LIMIT 1"
        self.assertEqual(adapter["expression_identity"](sql, 5)["limit"], 1)
        self.assertEqual(adapter["query"](source, sql, 5).to_arrow().num_rows, 1)
        self.assertIsNone(adapter["expression_identity"]("SELECT key FROM bench", 5)["predicate"])

    def test_actual_scan_pushdown_and_new_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dates.parquet"
            pq.write_table(pa.table({"key": [1, 2], "l_shipdate": [date(1995, 3, 15), date(1995, 3, 14)]}), path)
            source = daft.read_parquet(str(path))
            sql = "SELECT key FROM bench WHERE l_shipdate = DATE '1995-03-15'"
            plans, identities = {}, {}
            for revision in (2, 5):
                plan = adapter["query"](source, sql, revision)
                output = io.StringIO()
                plan.explain(show_all=True, file=output)
                plans[revision] = output.getvalue().split("== Optimized Logical Plan ==", 1)[1]
                identities[revision] = adapter["json_hash"](adapter["expression_identity"](sql, revision))
                self.assertEqual(plan.to_arrow().column("key").to_pylist(), [1])
            self.assertIn("to_date(", plans[2])
            self.assertIn("* Filter:", plans[2])
            self.assertNotIn("to_date(", plans[5])
            self.assertNotIn("* Filter:", plans[5])
            self.assertIn("Filter pushdown = col(l_shipdate) == cast(", plans[5])
            self.assertNotEqual(identities[2], identities[5])


if __name__ == "__main__":
    unittest.main()
