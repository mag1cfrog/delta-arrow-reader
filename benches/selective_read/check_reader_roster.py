"""Check revision 6 identities, schedules and frozen legacy reader order."""

from collections import Counter
import copy
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parent / "runners"))
import campaign
import run


def rejected(action):
    try:
        action()
    except ValueError:
        return
    raise AssertionError("invalid comparison accepted")


def main():
    old = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "daft")
    new = ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "spark")
    identity = {"comparison_revision": 6, "protocol_sha256": run.digest(run.SPARK_MATRIX),
                "base_protocol_sha256": run.digest(run.PROTOCOL), "sampling_sha256": run.digest(run.SAMPLING),
                "sampling_stage": "formal", "workload_manifest_sha256": "1" * 64}
    assert run.comparison_identity(identity) == identity and run.reader_roster(identity) == new
    for revision in (2, 3, 4, 5):
        assert run.reader_roster({"comparison_revision": revision}) == old
    for change in ({"protocol_sha256": run.digest(run.PRODUCTION)}, {"sampling_stage": None},
                   {"sampling_sha256": "0" * 64}, {"comparison_revision": 5}):
        rejected(lambda: run.comparison_identity(identity | change))
    rejected(lambda: run.reader_roster({"comparison_revision": 7}))

    inventory = {job: {reader: {"runnable": True, "status": "success"} for reader in new}
                 for job in ("production.q2.scattered", "reuse.production.q2.scattered")}
    slots = campaign.schedule(inventory, "new-roster", identity)
    assert Counter(s["reader_id"] for s in slots if s["stage"] == "timing") == dict.fromkeys(new, 10)
    assert {s["reader_id"] for s in slots} == set(new)
    assert len({s["run_id"] for s in slots}) == len(slots)
    joined = campaign.schedule(inventory, "new-roster", identity, combined=True)
    assert len(slots) == 80 and len(joined) == 72
    # All warmups and independent timings retain their reader order and inputs.
    without_id = lambda items: [{k: v for k, v in s.items() if k != "run_id"}
                                for s in items if s["stage"] in ("warmup", "timing")]
    assert without_id(joined) == without_id(slots)
    assert all(s["reader_id"] == "duckdb" for s in joined if s["stage"] == "plan")
    diagnostics = [s for s in joined if s["stage"] == "diagnostic"]
    assert len(diagnostics) == 10 and all(s["traced"] for s in diagnostics)
    assert all(campaign.slot_purpose(s, True) == ("io" if s["reader_id"] == "duckdb" else "diagnostic")
               for s in diagnostics)
    assert max(i for i, s in enumerate(joined) if s["stage"] == "timing") < min(
        i for i, s in enumerate(joined) if s["stage"] in ("plan", "diagnostic"))
    enabled = identity | {"combined_diagnostics": True,
                          "diagnostics_amendment_sha256": run.digest(campaign.COMBINED_DIAGNOSTICS)}
    assert campaign.combined_diagnostics(enabled) and not campaign.combined_diagnostics(identity)
    declared = {job: {r: entry | {"combined_diagnostics": True} for r, entry in entries.items()}
                for job, entries in inventory.items()}
    assert campaign.schedule(declared, "new-roster", identity) == joined
    for change in ({"combined_diagnostics": 1}, {"diagnostics_amendment_sha256": None},
                   {"diagnostics_amendment_sha256": "0" * 64}, {"comparison_revision": 5},
                   {"combined_diagnostics": False}):
        rejected(lambda: campaign.combined_diagnostics(enabled | change))
    for reader in new:
        subset = {job: {r: entry | {"runnable": r == reader} for r, entry in entries.items()}
                  for job, entries in inventory.items()}
        one = campaign.schedule(subset, "one", identity, combined=True)
        assert Counter(s["stage"] for s in one) == {"warmup": 2, "timing": 10, "diagnostic": 2,
                                                     **({"plan": 2} if reader == "duckdb" else {})}
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        (output / "reader").mkdir()
        plan = output / "reader/query-0.plan.txt"
        record = {"queries": [{"query_index": 0, "physical_plan": plan.name}]}
        rejected(lambda: campaign.plan_evidence(record, output))
        plan.touch()
        rejected(lambda: campaign.plan_evidence(record, output))
        plan.write_text("native plan\n")
        evidence = campaign.plan_evidence(record, output)
        plan.write_text("changed plan\n")
        assert evidence != campaign.plan_evidence(record, output)
        record["queries"][0]["physical_plan"] = "../query-0.plan.txt"
        rejected(lambda: campaign.plan_evidence(record, output))
        record["queries"][0]["physical_plan"] = plan.name
        plan.unlink()
        plan.symlink_to(campaign.__file__)
        rejected(lambda: campaign.plan_evidence(record, output))
        rows = []
        for slot in joined:
            purpose = campaign.slot_purpose(slot, True)
            observed = {"status": "success", "storage_capture": {"status": "passed"},
                        "storage_environment": {"trace_enabled": slot["traced"]}, "storage_io": {"requests": 1}}
            destination = output / slot["run_id"]
            if purpose == "diagnostic":
                (destination / "reader").mkdir(parents=True)
                count = 2 if slot["job_id"].startswith("reuse.") else 1
                observed["queries"] = []
                for index in range(count):
                    name = f"query-{index}.plan.txt"
                    (destination / "reader" / name).write_text("simulated native plan\n")
                    observed["queries"].append({"query_index": index, "physical_plan": name})
                observed["plan_artifacts"] = campaign.plan_evidence(observed, destination)
            rows.append(slot | {"request": {"purpose": purpose}, "observation": observed, "artifacts": str(destination)})
        config = enabled | {"campaign_id": "new-roster"}
        campaign.validate_diagnostics(config, declared, joined, rows)
        rejected(lambda: campaign.validate_diagnostics(config, inventory, joined, rows))
        rejected(lambda: campaign.validate_diagnostics(config, declared, joined, rows[:-1]))
        rejected(lambda: campaign.validate_diagnostics(config | {"combined_diagnostics": False,
                          "diagnostics_amendment_sha256": None}, declared, joined, rows))
        traced = next(row for row in rows if row["stage"] == "diagnostic")
        traced["observation"]["storage_environment"]["trace_enabled"] = False
        rejected(lambda: campaign.validate_diagnostics(config, declared, joined, rows))
        traced["observation"]["storage_environment"]["trace_enabled"] = True
        exported = Path(traced["observation"]["plan_artifacts"][0]["path"])
        exported.write_text("changed after capture\n")
        rejected(lambda: campaign.validate_diagnostics(config, declared, joined, rows))
    legacy = {job: {reader: {"runnable": True, "status": "success"} for reader in old} for job in inventory}
    assert {s["reader_id"] for s in campaign.schedule(legacy, "old", identity | {"comparison_revision": 5})} == set(old)
    rejected(lambda: campaign.schedule(legacy, "old", identity | {"comparison_revision": 5}, combined=True))
    missing = copy.deepcopy(inventory)
    missing["production.q2.scattered"].pop("spark")
    rejected(lambda: campaign.schedule(missing, "missing", identity))
    extra = copy.deepcopy(inventory)
    extra["production.q2.scattered"]["daft"] = {"runnable": True, "status": "success"}
    rejected(lambda: campaign.schedule(extra, "extra", identity))
    print("Revision 6 roster, unchanged timing, combined diagnostics, legacy ordering and native plan checks passed")


if __name__ == "__main__":
    main()
