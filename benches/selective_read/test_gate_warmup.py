"""Gate clocks are never samples, and missing/failed gates admit no timing."""

import copy
from pathlib import Path
import tempfile
import unittest

import campaign
from check_campaign import observation
import run


class GateWarmup(unittest.TestCase):
    def test_merged_gate_keeps_samples_and_requires_diagnostic_evidence(self):
        comparison = {"comparison_revision": 6, "protocol_sha256": run.digest(run.SPARK_MATRIX),
                      "base_protocol_sha256": run.digest(run.PROTOCOL), "sampling_sha256": run.digest(run.SAMPLING),
                      "sampling_stage": "formal", "workload_manifest_sha256": "1" * 64}
        config = comparison | {"campaign_id": "check", "combined_diagnostics": True, "gate_warmup": True,
            "diagnostics_amendment_sha256": run.digest(campaign.COMBINED_DIAGNOSTICS),
            "warmup_amendment_sha256": run.digest(campaign.GATE_WARMUP), "gate_diagnostics": True,
            "gate_diagnostics_amendment_sha256": run.digest(campaign.GATE_DIAGNOSTICS)}
        self.assertTrue(campaign.validation_diagnostics(config))
        self.assertFalse(campaign.validation_diagnostics(comparison))
        for change in ({"gate_diagnostics": 1}, {"comparison_revision": 5}, {"gate_warmup": False},
                       {"combined_diagnostics": False}, {"gate_diagnostics_amendment_sha256": "0" * 64},
                       {"gate_diagnostics_amendment_sha256": None}, {"gate_diagnostics": False}):
            with self.assertRaises(ValueError):
                campaign.validation_diagnostics(config | change)
        jobs = ["production.q2.scattered.dv", "reuse.production.q2.scattered.dv"]
        readers = run.reader_roster(comparison)
        inventory = {job: {reader: {"runnable": True, "status": "success", "gate_warmup": True,
            "combined_diagnostics": True, "gate_diagnostics": True, "run_id": job + ".gate." + reader,
            "identity": {"reader_id": reader}} for reader in readers} for job in jobs}
        previous = campaign.schedule(inventory, "check", comparison, gate_diagnostics=False)
        current = campaign.schedule(inventory, "check", comparison)
        self.assertEqual(len(previous), 62)
        self.assertEqual(len(current), 52)
        self.assertEqual([s for s in previous if s["stage"] == "timing"],
                         [s for s in current if s["stage"] == "timing"])
        self.assertEqual([s["reader_id"] for s in current if s["stage"] != "timing"], ["duckdb", "duckdb"])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            rows = []
            for job, entries in inventory.items():
                for reader, gate in entries.items():
                    rows.append({"run_id": gate["run_id"], "job_id": job, "reader_id": reader, "stage": "gate",
                        "status": "success", "traced": True,
                        "request": {"purpose": "validation", "validation_diagnostics": True},
                        "observation": {"status": "success", "identity": gate["identity"], "validation_diagnostics": True,
                            "correctness": {"status": "passed"}, "cleanup": {"status": "passed"},
                            "storage_capture": {"status": "passed"}, "storage_environment": {"trace_enabled": True},
                            "storage_io": {"requests": 2, "response_bytes": 100}, "queries": []}})
            rows += [slot | {"status": "success", "request": {"purpose": campaign.slot_purpose(slot, True)},
                              "observation": {"status": "success", **observation(100, slot["job_id"].startswith("reuse."), 4)}} for slot in current]
            for row in rows:
                if row["stage"] == "plan" or row["stage"] == "gate" and row["reader_id"] != "duckdb":
                    destination = root / row["run_id"]
                    (destination / "reader").mkdir(parents=True)
                    count = 2 if row["job_id"].startswith("reuse.") else 1
                    row["observation"]["queries"] = [{"query_index": i, "physical_plan": f"query-{i}.plan"} for i in range(count)]
                    for query in row["observation"]["queries"]:
                        (destination / "reader" / query["physical_plan"]).write_text("native plan\n")
                    row["artifacts"] = str(destination)
                    row["observation"]["plan_artifacts"] = campaign.plan_evidence(row["observation"], destination)
            campaign.validate_diagnostics(config, inventory, current, rows)
            summary = campaign.summarize(inventory, current, rows, 1)
            for entries in summary.values():
                for record in entries.values():
                    self.assertTrue(record["eligible"])
                    self.assertEqual(record["scheduled_samples"], 5)
                    self.assertEqual(record["diagnostics"]["diagnostic"][0]["source"], "exact-validation-gate")
                    self.assertEqual(record["metrics"]["process_elapsed_ns"]["samples"], 5)
            for field in ("correctness", "cleanup", "storage_capture", "storage_io", "plan_artifacts", "traced", "request"):
                changed = copy.deepcopy(rows)
                if field == "traced":
                    changed[0][field] = False
                elif field == "request":
                    changed[0][field]["validation_diagnostics"] = False
                else:
                    changed[0]["observation"].pop(field)
                with self.assertRaises(ValueError):
                    campaign.validate_diagnostics(config, inventory, current, changed)
                self.assertFalse(campaign.summarize(inventory, current, changed, 1)[jobs[0]][readers[0]]["eligible"])
            plan = Path(rows[0]["observation"]["plan_artifacts"][0]["path"])
            plan.write_text("changed native plan\n")
            with self.assertRaises(ValueError):
                campaign.validate_diagnostics(config, inventory, current, rows)

    def test_order_counts_evidence_and_historical_compatibility(self):
        comparison = {"comparison_revision": 6, "protocol_sha256": run.digest(run.SPARK_MATRIX),
                      "base_protocol_sha256": run.digest(run.PROTOCOL), "sampling_sha256": run.digest(run.SAMPLING),
                      "sampling_stage": "formal", "workload_manifest_sha256": "1" * 64}
        readers = run.reader_roster(comparison)
        jobs = ["production.q2.scattered", "reuse.production.q2.scattered"]
        inventory = {job: {reader: {"runnable": True, "status": "success", "gate_warmup": True,
                                   "run_id": job + ".gate." + reader, "identity": {"reader_id": reader}}
                          for reader in readers} for job in jobs}
        config = comparison | {"campaign_id": "check", "gate_warmup": True,
                               "warmup_amendment_sha256": run.digest(campaign.GATE_WARMUP)}
        self.assertTrue(campaign.validation_warmup(config))
        self.assertFalse(campaign.validation_warmup(comparison))
        for change in ({"gate_warmup": 1}, {"comparison_revision": 5}, {"warmup_amendment_sha256": None},
                       {"warmup_amendment_sha256": "0" * 64}, {"gate_warmup": False}):
            with self.assertRaises(ValueError):
                campaign.validation_warmup(config | change)
        timed_order = lambda slots: [{k: v for k, v in s.items() if k != "run_id"}
                                    for s in slots if s["stage"] == "timing"]
        for combined in (False, True):
            previous = campaign.schedule(inventory, "check", comparison, combined=combined, gate_warmup=False)
            current = campaign.schedule(inventory, "check", comparison, combined=combined)
            self.assertEqual(len(previous), 72 if combined else 80)
            self.assertEqual(len(current), 62 if combined else 70)
            self.assertEqual(timed_order(previous), timed_order(current))
            self.assertFalse(any(s["stage"] == "warmup" for s in current))
            self.assertEqual(sum(s["stage"] == "timing" for s in current), 50)
        slots = campaign.schedule(inventory, "check", comparison)
        rows = [{"run_id": gate["run_id"], "job_id": job, "reader_id": reader, "stage": "gate", "status": "success",
                 "request": {"purpose": "validation"}, "observation": {"status": "success", "identity": gate["identity"],
                     "correctness": {"status": "passed"}, "cleanup": {"status": "passed"}}}
                for job, entries in inventory.items() for reader, gate in entries.items()]
        rows += [slot | {"status": "success", "request": {"purpose": campaign.slot_purpose(slot)},
                         "observation": observation(100, slot["job_id"].startswith("reuse."), 4)}
                 for slot in slots]
        campaign.validate_diagnostics(config, inventory, slots, rows)
        summary = campaign.summarize(inventory, slots, rows, 1)
        self.assertTrue(all(row["eligible"] and row["scheduled_samples"] == 5
                            and row["warmup"]["passed"] for readers in summary.values() for row in readers.values()))
        gate = rows[0]
        for field in ("missing", "cleanup", "correctness", "identity", "status"):
            changed = copy.deepcopy(rows)
            record = changed[0]["observation"]
            if field == "missing":
                changed.pop(0)
            elif field in ("cleanup", "correctness"):
                record[field]["status"] = "failed"
            elif field == "identity":
                record[field] = {"reader_id": "different"}
            else:
                record["status"] = "failed"
            with self.assertRaises(ValueError):
                campaign.validate_diagnostics(config, inventory, slots, changed)
            self.assertFalse(campaign.summarize(inventory, slots, changed, 1)[gate["job_id"]][gate["reader_id"]]["eligible"])
        with self.assertRaises(ValueError):
            campaign.schedule({jobs[0]: inventory[jobs[0]]}, "one-profile", comparison)
        with self.assertRaises(ValueError):
            campaign.schedule(inventory | {"production.q4.scattered": inventory[jobs[0]]}, "two-snapshots", comparison)
        legacy = {job: {reader: {"runnable": True, "status": "success"} for reader in readers} for job in jobs}
        self.assertTrue(any(s["stage"] == "warmup" for s in campaign.schedule(legacy, "old", comparison)))


if __name__ == "__main__":
    unittest.main()
