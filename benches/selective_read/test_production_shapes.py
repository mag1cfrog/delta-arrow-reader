"""Check source predicates and fractional stripe boundaries without large data."""

from datetime import date
import json
from pathlib import Path
import tempfile
import unittest

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

import production_shapes as shapes
import native


def file_geometry(connection, shape):
    query = native.call("workload", {"action": "layout-query", "shape": shape})
    statistics = connection.execute(query).to_arrow_table().to_pylist()
    return native.call("workload", {"action": "layout-geometry", "shape": shape,
                                   "statistics": json.loads(json.dumps(statistics, default=str))})


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
        table = pa.Table.from_pylist(list(reversed(rows)), schema=pa.schema([
            ("l_orderkey", pa.int64()), ("l_linenumber", pa.int32()),
            ("l_shipdate", pa.date32()), ("l_shipmode", pa.string())]))
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.parquet"
            pq.write_table(table, path)
            self.assertEqual(shapes.source_counts([path]), [20, 7, 5, 3])
            with duckdb.connect() as connection:
                connection.register("source", table)
                actual = file_geometry(connection, {"files": 5, "stripes": 2})
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
                    file_geometry(connection, {"files": 21, "stripes": 2})


if __name__ == "__main__":
    unittest.main()
