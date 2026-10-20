"""Small checks for production identity, sample counts and independent metrics."""

from datetime import date
import copy
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

import production_workloads as production
import run


class ProductionContract(unittest.TestCase):

    def test_staged_deletions_are_recomputed_and_default_pairs_remain_complete(self):
        extras = {(1, 1), (2, 1), (3, 1)}
        with tempfile.TemporaryDirectory() as temporary:
            root, tables = Path(temporary), []
            for layout in ("localized", "scattered"):
                case = "production.q2." + layout
                (root / case).mkdir()
                files = []
                for ordinal in range(2):
                    rows = [{"l_orderkey": key, "l_partkey": 1, "l_linenumber": 1,
                             "l_shipdate": date(1995, 3, 15 if key % 2 == 0 else 14), "l_shipmode": "AIR"}
                            for key in (1 + 2 * ordinal, 2 + 2 * ordinal)]
                    name = f"part-{ordinal}.parquet"
                    pq.write_table(pa.Table.from_pylist(rows if layout == "localized" else rows[::-1]), root / case / name)
                    files.append({"path": name})
                tables.append({"id": case, "path": case, "deletion_vectors": False, "files": files})
            manifest = {"status": "complete", "protocol": "selective-read-production-pairs-v1",
                        "writer": {"status": "complete", "tables": tables},
                        "production_dv": {"base_case_ids": [t["id"] for t in tables],
                                          "extra_logical_keys": [list(k) for k in sorted(extras)]}}
            self.assertEqual(production.extra_keys(root, manifest), extras)
            for layout, table in zip(("localized", "scattered"), tables, strict=True):
                staged = copy.deepcopy(manifest)
                staged["writer"]["tables"] = [table]
                staged["production_dv"].update(layout=layout, base_case_ids=[table["id"]])
                self.assertEqual(production.extra_keys(root, staged), extras)
                staged["production_dv"].pop("layout")
                with self.assertRaisesRegex(ValueError, "layout scope"):
                    production.extra_keys(root, staged)
                staged["production_dv"]["layout"] = layout
                staged["production_dv"]["extra_logical_keys"].pop()
                with self.assertRaisesRegex(ValueError, "deletion union changed"):
                    production.extra_keys(root, staged)


    def test_identity_and_sampling(self):
        identity = {"comparison_revision": 5, "protocol_sha256": "production documentation",
                    "base_protocol_sha256": "protocol documentation", "sampling_sha256": "sampling documentation",
                    "workload_manifest_sha256": "1" * 64, "sampling_stage": "pilot",
                    "workload_contract": {"comparison_revision": 5, "sampling_stage": "pilot", "cases": []}}
        self.assertEqual(run.comparison_identity(identity), identity)
        for change in ({"comparison_revision": 4}, {"sampling_stage": None}):
            with self.assertRaises(ValueError):
                run.comparison_identity(identity | change)
        for stage in ("pilot", "formal"):
            self.assertEqual(run.query_count(identity | {"sampling_stage": stage, "execution_mode": "reuse"}), 2)
        self.assertEqual(run.query_count({"comparison_revision": 3, "execution_mode": "reuse"}), 10)
        with self.assertRaises(ValueError):
            run.fixture_tables({"status": "preparing", "protocol": "selective-read-production-fixtures-v1"})

    def test_independent_predicates_and_stored_metrics(self):
        rows = [{"l_orderkey": order, "l_partkey": 1000, "l_suppkey": 24, "l_linenumber": line,
                 "l_shipdate": date(1995, 3, 15), "l_shipmode": mode,
                 "metric_000": None if (order + line) % 17 == 0 else (1000 + 24 + line) % 1024,
                 "metric_001": None if (order + line + 1) % 17 == 0 else (1000 + 48 + line) % 1024}
                for order, line, mode in ((15, 1, "AIR"), (16, 1, "AIR"), (17, 2, "AIR"), (18, 1, "RAIL"))]
        batch = pa.RecordBatch.from_pylist(rows)
        production.check_metrics(batch, 2)
        self.assertEqual(production.masks(batch)[-1].to_pylist(), [True, True, False, False])
        rows[0]["metric_000"] = 2
        with self.assertRaisesRegex(ValueError, "wrong stored metric"):
            production.check_metrics(pa.RecordBatch.from_pylist(rows), 2)


if __name__ == "__main__":
    unittest.main()
