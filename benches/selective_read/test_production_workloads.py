"""Small checks for production identity, sample counts and independent metrics."""

from datetime import date
import copy
import unittest

import pyarrow as pa

import production_workloads as production
import campaign
import run


class ProductionContract(unittest.TestCase):
    def test_large_geometry_is_bound_to_each_paired_shape(self):
        case = "production.q4.localized"
        definition = production.shapes.definitions()["q4"]
        table = {"id": case, "path": case, "deletion_vectors": False, "snapshot_version": 0,
                 "layout": "localized", "scale_factor": 10, "file_target_mib": 512,
                 "schema": {"fields": [{"name": c} for c in
                     (*production.oracle.ORIGINAL, *production.oracle.PAYLOADS,
                      *(f"metric_{j:03}" for j in range(10)))]},
                 "file_count": 1, "files": [{"rows": 10, "bytes": 100, "row_groups": [{}]}],
                 "file_evidence": [{}], "rows": 10, "bytes": 100, "writer": definition}
        manifest = {"protocol": "selective-read-production-pairs-v1", "status": "complete", "mode": "probe",
                    "contract_sha256": run.digest(production.shapes.CONTRACT),
                    "source_parent_manifest_sha256": "1" * 64,
                    "sources": [{"scale_factor": 10, "rows": 100}],
                    "shape_definitions": {"q4": definition}, "writer": {"status": "complete", "tables": [table]}}
        self.assertEqual(production.query_fields(case, manifest)["writer"]["data_page_rows"], 20000)
        for field in ("data_page_rows", "data_page_bytes", "write_batch_rows", "row_group_rows", "dictionary"):
            bad = copy.deepcopy(manifest)
            bad["writer"]["tables"][0]["writer"] = dict(definition, **{field: 123})
            with self.assertRaisesRegex(ValueError, "page/group"):
                production.query_fields(case, bad)
        bad = copy.deepcopy(manifest)
        bad["shape_definitions"]["q4"]["files"] = 61
        with self.assertRaisesRegex(ValueError, "shape definition"):
            production.query_fields(case, bad)

    def test_identity_and_sampling(self):
        identity = {"comparison_revision": 5, "protocol_sha256": run.digest(run.PRODUCTION),
                    "base_protocol_sha256": run.digest(run.PROTOCOL), "sampling_sha256": run.digest(run.SAMPLING),
                    "workload_manifest_sha256": "1" * 64, "sampling_stage": "pilot"}
        self.assertEqual(run.comparison_identity(identity), identity)
        for change in ({"comparison_revision": 4}, {"sampling_stage": None}, {"sampling_sha256": "0" * 64}):
            with self.assertRaises(ValueError):
                run.comparison_identity(identity | change)
        inventory = {"production.q4.localized": {r: {"runnable": True} for r in campaign.READERS}}
        for stage, expected in (("pilot", 2), ("formal", 5)):
            slots = campaign.schedule(inventory, "check", identity | {"sampling_stage": stage})
            for reader in campaign.READERS:
                self.assertEqual(sum(s["reader_id"] == reader and s["stage"] == "timing" for s in slots), expected)
                self.assertEqual(sum(s["reader_id"] == reader and s["stage"] == "warmup" for s in slots), 1)
            self.assertEqual(run.query_count(identity | {"execution_mode": "reuse"}), 2)
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
