"""Check revision 6 identities, schedules and frozen legacy reader order."""

from collections import Counter
import copy
from pathlib import Path
import sys

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
    legacy = {job: {reader: {"runnable": True, "status": "success"} for reader in old} for job in inventory}
    assert {s["reader_id"] for s in campaign.schedule(legacy, "old", identity | {"comparison_revision": 5})} == set(old)
    missing = copy.deepcopy(inventory)
    missing["production.q2.scattered"].pop("spark")
    rejected(lambda: campaign.schedule(missing, "missing", identity))
    extra = copy.deepcopy(inventory)
    extra["production.q2.scattered"]["daft"] = {"runnable": True, "status": "success"}
    rejected(lambda: campaign.schedule(extra, "extra", identity))
    print("Revision 6 identities, reader order, five independent samples and invalid roster rejection passed")


if __name__ == "__main__":
    main()
