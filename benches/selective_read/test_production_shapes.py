"""Check source predicates and fractional stripe boundaries without large data."""

from datetime import date
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

import production_shapes as shapes
import production_fixtures as fixtures


class ProductionShapeCheck(unittest.TestCase):
    def test_probe_reserves_its_estimate_not_the_entire_allocation(self):
        files = [{"stripe": s, "file_index": i, "rows": 100,
                  "candidate": i == 0, "matching_rows": int(i == 0)}
                 for s in range(2) for i in range(2)]
        shape = {**shapes.definitions()["q4"], "stripes": 2}
        def check_limit(root, limits):
            # The probe fits on a disk with 80 GiB free even when its allocation is 192 GiB.
            self.assertLess(limits["disk_bytes"], 80 * fixtures.GIB)
            self.assertGreater(limits["disk_bytes"], fixtures.SPILL)
            request = json.loads((root / "writer-request.json").read_text())
            for field in ("data_page_bytes", "write_batch_rows", "data_page_rows"):
                self.assertEqual(request[field], shape[field])
            raise RuntimeError("checked before native generation")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = SimpleNamespace(source=root, plan=root / "plan.json", output=root / "output",
                                   command="probe", shape="q4", probe=None, layout=None,
                                   elapsed_limit_seconds=60, disk_limit_mib=196608)
            with patch.object(fixtures, "inputs", return_value=({}, {"bytes": 1000}, [], shape, files)), \
                 patch.object(fixtures.shutil, "disk_usage", return_value=SimpleNamespace(free=80 * fixtures.GIB)), \
                 patch.object(fixtures, "bounded", side_effect=check_limit), \
                 self.assertRaisesRegex(RuntimeError, "checked before native"):
                fixtures.generate(args)

    def test_capacity_keeps_false_positives_and_both_stripes(self):
        files = [{"stripe": s, "file_index": i, "rows": 11 + i,
                  "candidate": i == 0, "matching_rows": 0}
                 for s in range(2) for i in range(3)]
        self.assertEqual(fixtures.probe_selection(files, 2), [0, 2, 3, 5])
        samples, evidence = [], []
        for ordinal in fixtures.probe_selection(files, 2):
            planned = files[ordinal]
            samples.append({"rows": planned["rows"], "bytes": planned["rows"] * (100 + ordinal),
                            "geometry": {"bytes": 500}, "delta_stats": {"numRecords": planned["rows"]}})
            evidence.append({"source_file_ordinal": ordinal, "planned": planned,
                             "full_value_roundtrip": "passed", "maximum_page_rows": 2048})
        probe = {"writer": {"tables": [{"layout": "localized",
                 "writer": {"data_page_rows": 2048, "write_batch_rows": 1024}, "files": samples, "file_evidence": evidence}]}}
        result = fixtures.capacity(probe, files, "localized", 1000, 2000)
        # Use each stripe's maximum, including an interior file, then round up.
        self.assertEqual(result["estimated_parquet_bytes"], (36 * 102 * 5 + 3) // 4 + (36 * 105 * 5 + 3) // 4)
        self.assertGreater(result["estimated_phase_peak_bytes"], result["native_output_limit_bytes"])
        del samples[2:]
        del evidence[2:]
        with self.assertRaisesRegex(ValueError, "misses a stripe"):
            fixtures.capacity(probe, files, "localized", 1000, 2000)

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
        self.assertEqual([v["files"] for v in definitions.values()], [130, 60])
        control = shapes.definitions(256, 20000)
        self.assertEqual([v["files"] for v in control.values()], [260, 120])
        for name, value in definitions.items():
            self.assertEqual(value["canonical_sql"], control[name]["canonical_sql"])
            self.assertEqual(value["row_group_rows"], 131072)
            self.assertEqual(value["data_page_rows"], 20000)
            self.assertEqual(value["data_page_bytes"], 1048576)
            self.assertEqual(value["write_batch_rows"], 1024)
            self.assertFalse(value["dictionary"])
        with self.assertRaisesRegex(ValueError, "unsupported"):
            shapes.definitions(128)
        self.assertEqual([(v["stored_columns"], len(v["projection"])) for v in definitions.values()],
                         [(416, 69), (90, 71)])
        self.assertTrue(all(len(set(v["projection"])) == len(v["projection"])
                            and "l_linenumber IN (1)" in v["canonical_sql"] for v in definitions.values()))
        self.assertEqual(set(shapes.cases()), {"production." + name + suffix
                         for name in ("q2.localized", "q2.scattered", "q4.localized", "q4.scattered")
                         for suffix in ("", ".dv")})

    def test_byte_controls_keep_query_and_file_geometry(self):
        baseline = shapes.definitions(write_batch_rows=128)
        for size in (8192, 65536):
            control = shapes.definitions(page_bytes=size, write_batch_rows=128)
            for name, shape in baseline.items():
                self.assertEqual(control[name], dict(shape, data_page_bytes=size))
        for options in ({"page_bytes": 0}, {"page_bytes": 16384}, {"write_batch_rows": 0}):
            with self.assertRaisesRegex(ValueError, "unsupported"):
                shapes.definitions(**options)


if __name__ == "__main__":
    unittest.main()
