"""Check the frozen queries, prepared metadata and failure-preserving matrix report."""

import argparse
from collections import Counter
from copy import deepcopy
import csv
import json
from pathlib import Path
import tempfile

import matrix
import oracle
import campaign
from check_campaign import observation
from run import digest, save


def check(fixtures, prepared):
    frozen = matrix.catalog()
    for scale, source in frozen["scales"].items():
        literals = source["in_literals"]
        assert literals == sorted(set(literals)) and len(literals) == (5 if scale == "0.01" else 20)
        for family, cases in (("li", oracle.ORIGINAL_CASES), ("wide", oracle.WIDE_CASES)):
            assert set(frozen["shapes"][family]) == set(cases)
            for name, (projection, predicate, limit) in cases.items():
                shape, query = frozen["shapes"][family][name], source["queries"][family][name]
                assert shape["projection"] == list(projection) and shape["predicate"] == predicate and shape["limit"] == limit
                assert query["sql"] == oracle.sql_for(projection, predicate, limit, literals)
                assert query["sql_sha256"] == oracle.digest_bytes(query["sql"].encode())
    for reader, lock in frozen["translation_locks"].items():
        assert digest(matrix.HERE / "runners" / reader / "lock.json") == lock
    value = matrix.load(prepared, fixtures)
    assert value["status"] == "complete"
    cases = {row["case_id"]: row for row in value["cases"]}
    for layout in matrix.LAYOUTS:
        narrow, wide = (cases[f"wide.{layout}.{name}"] for name in ("eq2-in20-keys", "eq2-in20"))
        assert narrow["predicate"] == wide["predicate"] and narrow["fixture_path"] == wide["fixture_path"]
        assert narrow["projection_columns"] == 2 and wide["projection_columns"] == 69
        assert narrow["oracle"] == wide["oracle"] and len(narrow["predicate_only_columns"]) == 3
        assert cases[f"li.{layout}.empty"]["oracle"]["output_rows"] == 0
        assert cases[f"li.{layout}.date7-limit"]["oracle"]["output_rows"] == min(100, cases[f"li.{layout}.date7-full"]["oracle"]["output_rows"])
        assert len(cases[f"li.{layout}.eq2-in1"]["in_literals"]) == 1
    for reader in campaign.READERS:
        sample = next(iter(cases.values()))
        record = {"status": "success", "identity": {"reader_id": reader,
                  "native_expression_sha256": sample["native_expression_sha256"].get(reader)}}
        matrix.check_translation(record, sample)
        record["identity"]["native_expression_sha256"] = "0" * 64
        try:
            matrix.check_translation(record, sample)
        except ValueError:
            pass
        else:
            raise AssertionError("changed translation accepted")
    with tempfile.TemporaryDirectory(prefix="selective-read-matrix-") as temporary:
        root = Path(temporary)
        for field, replacement in (("cases", value["cases"][:-1]), ("catalog_sha256", "0" * 64)):
            bad = deepcopy(value)
            bad[field] = replacement
            path = root / (field + ".json")
            save(path, bad)
            try:
                matrix.load(path, fixtures)
            except ValueError:
                pass
            else:
                raise AssertionError("changed matrix accepted: " + field)
        bad = deepcopy(value)
        bad["cases"][0]["oracle"]["output_rows"] += 1
        save(root / "counts.json", bad)
        try:
            matrix.load(root / "counts.json", fixtures)
        except ValueError:
            pass
        else:
            raise AssertionError("changed oracle counts accepted")
        inventory = {case: {r: {"status": "success", "runnable": True} for r in campaign.READERS} for case in cases}
        first = next(iter(cases))
        inventory[first]["daft"] = {"status": "unsupported", "runnable": False, "failure_reason": "test capability rejection"}
        slots = campaign.schedule(inventory, "check")
        rows = [{**slot, "status": "success", "artifacts": str(root / slot["run_id"]),
                 "observation": observation(100)} for slot in slots]
        next(row for row in rows if row["job_id"] == first and row["reader_id"] == "polars" and row["stage"] == "timing")["status"] = "timeout"
        summary = {"status": "incomplete", "campaign_id": "check", "integrity_passed": True,
                   "timer_resolution": {"ratio_floor_ns": 1}, "jobs": campaign.summarize(inventory, slots, rows, 1)}
        config = {"matrix": {"path": str(prepared.resolve()), "sha256": digest(prepared)}, "fixtures": str(fixtures.resolve())}
        for name, data in (("campaign", config), ("inventory", inventory), ("schedule", slots), ("summary", summary)):
            save(root / (name + ".json"), data)
        save(root / "frozen.json", {name: digest(root / name) for name in ("campaign.json", "inventory.json", "schedule.json")})
        (root / "observations.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
        assert matrix.report(root, root / "report") == {"status": "incomplete", "entries": 150}
        results = json.loads((root / "report/matrix-report.json").read_text())["rows"]
        assert Counter(row["reader_id"] for row in results) == dict.fromkeys(campaign.READERS, 30)
        assert next(row for row in results if row["case"]["case_id"] == first and row["reader_id"] == "daft")["gate"]["status"] == "unsupported"
        failed = next(row for row in results if row["case"]["case_id"] == first and row["reader_id"] == "polars")
        assert not failed["timing"]["eligible"] and failed["timing"]["sample_statuses"]["timeout"] == 1
        assert failed["timing"]["speedup_vs_dar"]["open_query_ns"]["value"] is None
        assert failed["planning"]["candidate_files"] is None
        with (root / "report/matrix-report.csv").open() as file:
            assert len(list(csv.DictReader(file))) == 150
    print("passed: 30 public shapes, all scale literals/SQL, paired projections, translation binding, changed metadata rejection and 150 report entries retaining failures")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--matrix", type=Path, required=True)
    args = parser.parse_args()
    check(args.fixtures, args.matrix)
