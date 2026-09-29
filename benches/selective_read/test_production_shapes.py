"""Check source predicates and fractional stripe boundaries without large data."""

from datetime import date
from pathlib import Path
import tempfile
import unittest

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

import production_shapes as shapes


class ProductionShapeCheck(unittest.TestCase):
    def test_predicates_and_file_membership(self):
        rows = []
        for i in range(10):
            rows.append({"l_orderkey": 2 * i + 2, "l_linenumber": 2 if i in (4, 6) else 1,
                         "l_shipdate": date(1995, 3, 14 if i < 2 else 15 if i < 7 else 16),
                         "l_shipmode": "RAIL" if i in (5, 6) else "AIR"})
            rows.append({"l_orderkey": 2 * i + 1, "l_linenumber": 2 if i == 4 else 1,
                         "l_shipdate": date(1995, 3, 14 if i < 3 else 15 if i < 5 else 16),
                         "l_shipmode": "AIR"})
        table = pa.Table.from_pylist(list(reversed(rows)))
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.parquet"
            pq.write_table(table, path)
            self.assertEqual(shapes.source_counts([path]), [20, 7, 5, 3])
            with duckdb.connect() as connection:
                connection.register("source", table)
                actual = shapes.file_geometry(connection, {"files": 5, "stripes": 2})
                # Stripe 0's fractional cuts are 4/3/3; stripe 1's are 5/5.
                # One nonmatching file survives min/max because its columns' bounds overlap.
                self.assertEqual(actual, [
                    {"stripe": 0, "file_index": 0, "rows": 4, "candidate": True, "matching_rows": 2},
                    {"stripe": 0, "file_index": 1, "rows": 3, "candidate": True, "matching_rows": 0},
                    {"stripe": 0, "file_index": 2, "rows": 3, "candidate": False, "matching_rows": 0},
                    {"stripe": 1, "file_index": 0, "rows": 5, "candidate": True, "matching_rows": 1},
                    {"stripe": 1, "file_index": 1, "rows": 5, "candidate": False, "matching_rows": 0},
                ])
                with self.assertRaisesRegex(ValueError, "populate"):
                    shapes.file_geometry(connection, {"files": 21, "stripes": 2})
        definitions = shapes.definitions()
        self.assertEqual([(v["stored_columns"], len(v["projection"])) for v in definitions.values()],
                         [(416, 69), (90, 71)])
        self.assertTrue(all(len(set(v["projection"])) == len(v["projection"])
                            and "l_linenumber IN (1)" in v["canonical_sql"] for v in definitions.values()))
        self.assertEqual(set(shapes.cases()), {"production." + name + suffix
                         for name in ("q2.localized", "q2.scattered", "q4.localized", "q4.scattered")
                         for suffix in ("", ".dv")})


if __name__ == "__main__":
    unittest.main()
