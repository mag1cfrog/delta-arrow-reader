"""Reuse unchanged files, and still reject changed data and deletion positions."""
from datetime import date
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

import oracle
import production_workloads as production
import large_workloads
import run


class ValidationReuse(unittest.TestCase):
    def test_batch_cli_preserves_single_case_output_and_rejects_duplicates(self):
        from test_oracle import dataset
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixtures = root / "fixtures"
            dataset(fixtures)
            prefix = [sys.executable, "-B", oracle.__file__, "prepare", "--fixtures", str(fixtures)]
            cases = ["wide.clustered.eq2-in20", "li.clustered.date7-full"]
            batch = root / "batch"
            subprocess.run([*prefix, "--case", cases[0], "--case", cases[1], "--output", str(batch)],
                           check=True, capture_output=True, text=True)
            for case in cases:
                self.assertEqual(json.loads((batch / case / "reference.json").read_text())["case_id"], case)
            single = root / "single"
            subprocess.run([*prefix, "--case", cases[0], "--output", str(single)], check=True, capture_output=True)
            self.assertEqual(run.digest(single / "reference.parquet"), run.digest(batch / cases[0] / "reference.parquet"))
            rejected = root / "duplicate"
            result = subprocess.run([*prefix, "--case", cases[0], "--case", cases[0], "--output", str(rejected)],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("duplicate preparation case", result.stdout)
            self.assertFalse(rejected.exists())

    def test_checksums_share_hardlinks_and_detect_writes_replacement_and_races(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path, link = root / "file", root / "link"
            path.write_bytes(b"aaaa")
            link.hardlink_to(path)
            initial = path.stat()
            actual_digest = hashlib.file_digest
            with patch.object(hashlib, "file_digest", wraps=actual_digest) as calls:
                before = run.digest(path)
                self.assertEqual(oracle.digest_file(link), before)
                self.assertEqual(run.digest(path), before)
                self.assertEqual(calls.call_count, 1)
                path.write_bytes(b"bbbb")
                os.utime(path, ns=(initial.st_atime_ns, initial.st_mtime_ns))
                after = run.digest(link)
                self.assertNotEqual(after, before)
                self.assertEqual(calls.call_count, 2)
                with self.assertRaisesRegex(ValueError, "checksum changed"):
                    oracle.verify_object(root, {"path": "file", "bytes": 4, "sha256": before})
                replacement = root / "replacement"
                replacement.write_bytes(b"cccc")
                os.utime(replacement, ns=(initial.st_atime_ns, initial.st_mtime_ns))
                replacement.replace(path)
                self.assertNotEqual(run.digest(path), after)
                self.assertEqual(calls.call_count, 3)
            race = root / "race"
            race.write_bytes(b"dddd")

            def mutate(stream, algorithm):
                value = actual_digest(stream, algorithm)
                race.write_bytes(b"eeee")
                return value

            with patch.object(hashlib, "file_digest", side_effect=mutate):
                with self.assertRaisesRegex(ValueError, "changed during checksum"):
                    run.digest(race)
            self.assertEqual(run.digest(race), hashlib.sha256(b"eeee").hexdigest())

    def test_paired_scans_reuse_values_but_recheck_dv_coordinates_and_metrics(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            production._SOURCE_SCANS.clear()
            production._PHYSICAL_SCANS.clear()
            production._EXTRA_KEY_SCANS.clear()
            original = []
            for number in range(1, 601):
                original.append(dict(zip(oracle.ORIGINAL, [
                    number, 7, 3, 1 if number <= 300 else 2, Decimal("17.00"), Decimal("12.50"),
                    Decimal("0.06"), Decimal("0.02"), "N", "O",
                    date(1995, 3, 15 if number <= 400 else 16), date(1995, 3, 16), date(1995, 3, 20),
                    "DELIVER IN PERSON", "AIR" if number <= 350 else "SHIP", "comment",
                ])))
            (root / "source").mkdir()
            source_path = root / "source/source.parquet"
            pq.write_table(pa.Table.from_pylist(original, schema=oracle.result_schema(oracle.ORIGINAL)), source_path)

            def descriptor(path, rows):
                return {"path": path.name, "rows": len(rows), "bytes": path.stat().st_size, "sha256": run.digest(path)}

            source = {"path": "source", "scale_factor": 10, "rows": 600,
                      "files": [descriptor(source_path, original)]}
            base_case = "production.q2.localized"
            definition = production.shapes.definitions()["q2"]
            metric_count = definition["extra_numeric_columns"]
            schema = pa.schema([*oracle.result_schema(oracle.ORIGINAL + oracle.PAYLOADS),
                                *[pa.field(f"metric_{j:03}", pa.int64()) for j in range(metric_count)]])
            base = {"id": base_case, "path": base_case, "deletion_vectors": False, "snapshot_version": 0,
                    "files": [], "file_evidence": [], "rows": 600, "file_count": 2}
            dv = dict(base, id=base_case + ".dv", path=base_case + ".dv", deletion_vectors=True,
                      snapshot_version=1, files=[])
            for table in (base, dv):
                (root / table["path"]).mkdir()
            extras = {(1, 1), (301, 2), (351, 2)}
            for index, rows in enumerate((original[:350], original[350:])):
                stored = []
                for record in rows:
                    stored.append(oracle.record(record, oracle.ORIGINAL + oracle.PAYLOADS, derive_payloads=True) |
                                  {f"metric_{j:03}": None if (record["l_orderkey"] + record["l_linenumber"] + j) % 17 == 0
                                   else (7 + 3 * (j + 1) + record["l_linenumber"]) % 1024 for j in range(metric_count)})
                path = root / base["path"] / f"part-{index}.parquet"
                pq.write_table(pa.Table.from_pylist(stored, schema=schema), path)
                (root / dv["path"] / path.name).hardlink_to(path)
                stats = {"numRecords": len(rows), "minValues": {}, "maxValues": {}, "nullCount": {}}
                for column, _, _ in production.PREDICATES:
                    values = [r[column] for r in rows]
                    stats["minValues"][column], stats["maxValues"][column] = min(values), max(values)
                    stats["nullCount"][column] = 0
                item = descriptor(path, rows) | {"delta_stats": stats}
                base["files"].append(item)
                matches = [i for i, r in enumerate(rows) if r["l_linenumber"] == 1]
                base["file_evidence"].append({"matching_ordinals": matches,
                                             "matching_groups": [{"matching_output_pages": 1}] if matches else []})
                deleted = [(i, [r[k] for k in oracle.KEYS]) for i, r in enumerate(rows)
                           if oracle.deleted(r) or tuple(r[k] for k in oracle.KEYS) in extras]
                dv["files"].append(item | {"deletion_vector": {"physical_ordinals": [i for i, _ in deleted],
                                                               "logical_ids": [key for _, key in deleted]}})
            deleted_count = sum(len(f["deletion_vector"]["logical_ids"]) for f in dv["files"])
            dv["deletion_summary"] = {"physical_rows": 600, "deleted_rows": deleted_count,
                                      "live_rows": 600 - deleted_count, "dv_files": 2}
            manifest = {"status": "complete", "protocol": "selective-read-production-pairs-v1", "mode": "probe",
                        "contract_sha256": run.digest(production.shapes.CONTRACT), "sources": [source],
                        "writer": {"status": "complete", "tables": [base, dv]},
                        "production_dv": {"layout": "localized", "base_case_ids": [base_case],
                                          "extra_logical_keys": [list(k) for k in sorted(extras)]}}
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, default=str))
            workload = root / "workload.json"
            workload.write_text("{}")
            row = {"shape": "q2", "projection": list(oracle.WIDE69), "canonical_sql": definition["canonical_sql"],
                   "canonical_sql_sha256": oracle.digest_bytes(definition["canonical_sql"].encode()),
                   "native_expression_sha256": {"polars": "1" * 64}}

            def objects(fixtures, table, source):
                items = [{"path": str(Path(group["path"]) / item["path"]), "bytes": item["bytes"], "sha256": item["sha256"]}
                         for group in (source, table) for item in group["files"]]
                for item in items:
                    oracle.verify_object(fixtures, item)
                return items

            with patch.object(large_workloads, "load", return_value={"oracle_limits": {}}), \
                    patch.object(large_workloads, "identity", return_value={"comparison_revision": 6}), \
                    patch.object(production, "binding", return_value=row), patch.object(oracle, "objects", side_effect=objects), \
                    patch.object(production, "check_metrics", wraps=production.check_metrics) as metrics:
                cold = production.prepare_reference(root, base_case, root / "cold", workload, 1024**3)
                self.assertEqual(cold["output_rows"], 300)
                self.assertEqual(metrics.call_count, 2)
                warm = production.prepare_reference(root, base_case + ".dv", root / "warm", workload, 1024**3)
                self.assertEqual(metrics.call_count, 2)
                self.assertTrue(warm["physical_validation"]["source_scan_reused"])
                self.assertEqual(warm["physical_validation"]["reused_full_column_scans"], 2)
                self.assertEqual(warm["output_rows"], 300 - warm["deleted_qualifying_rows"])
                again = production.prepare_reference(root, base_case, root / "again", workload, 1024**3)
                self.assertEqual(again["reference_sha256"], cold["reference_sha256"])
                original_ordinal = dv["files"][0]["deletion_vector"]["physical_ordinals"][0]
                dv["files"][0]["deletion_vector"]["physical_ordinals"][0] += 1
                manifest_path.write_text(json.dumps(manifest, default=str))
                with self.assertRaisesRegex(ValueError, "physical deletion ordinal/key"):
                    production.prepare_reference(root, base_case + ".dv", root / "bad-dv", workload, 1024**3)
                dv["files"][0]["deletion_vector"]["physical_ordinals"][0] = original_ordinal
                manifest_path.write_text(json.dumps(manifest, default=str))
                production._SOURCE_SCANS.clear()
                production._PHYSICAL_SCANS.clear()
                fresh = production.prepare_reference(root, base_case + ".dv", root / "fresh", workload, 1024**3)
                self.assertEqual(fresh["reference_sha256"], warm["reference_sha256"])
                path = root / base["path"] / base["files"][0]["path"]
                bad = pq.read_table(path).to_pydict()
                bad["metric_000"][0] = 0
                pq.write_table(pa.Table.from_pydict(bad, schema=schema), path)
                for table in (base, dv):
                    table["files"][0].update(bytes=path.stat().st_size, sha256=run.digest(path))
                manifest_path.write_text(json.dumps(manifest, default=str))
                with self.assertRaisesRegex(ValueError, "wrong stored metric"):
                    production.prepare_reference(root, base_case, root / "bad-metric", workload, 1024**3)


if __name__ == "__main__":
    unittest.main()
