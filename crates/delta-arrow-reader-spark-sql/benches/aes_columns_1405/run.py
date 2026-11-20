#!/usr/bin/env python3
"""Check and time the four AES cases on the original #1391 table."""

import argparse
import json
from pathlib import Path
from statistics import median
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "functions_1406"))
from run import invoke, sha


MANIFEST = HERE / "manifest.json"
SOURCE_MANIFEST = HERE.parent / "functions/manifest.json"
MODES = ("spark-off", "spark-on")
INPUT_ROWS = 1_000_000


def cases():
    manifest = json.loads(MANIFEST.read_text())
    assert sha(SOURCE_MANIFEST) == manifest["source_manifest_sha256"]
    original = {case["id"]: case for case in json.loads(SOURCE_MANIFEST.read_text())["functions"]}
    cases = manifest["functions"]
    assert len(cases) == len({case["id"] for case in cases}) == 4
    for case in cases:
        for field in ("spark_sql", "projection_sql"):
            assert case[field] == original[case["id"]][field]
    return cases


def run(binary, destination):
    selection = cases()
    assert not subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    destination.mkdir(parents=True, exist_ok=False)
    (destination / "run.json").write_text(json.dumps({
        "protocol_commit": commit, "binary_sha256": sha(binary), "manifest_sha256": sha(MANIFEST),
    }, indent=2) + "\n")
    for case in selection:
        id_ = case["id"]
        work = destination / ("plan-" + id_)
        records = invoke(binary, ["preflight", MANIFEST, id_, work], destination / f"preflight-{id_}.jsonl", {}, 180)
        record, = records
        reference, *spark = record["results"]
        assert reference["configuration"] == "native" and reference["status"] == "ok"
        assert [result["configuration"] for result in spark] == list(MODES)
        for result in spark:
            assert result["status"] == "ok" and result["source_guard_rows"] == INPUT_ROWS
            assert result["comparison"]["equal"] and result["types"] == ["Int64", "Binary"]
    ns_per_row = {case["id"]: {mode: [] for mode in MODES} for case in selection}
    for replica in range(3):
        ordered = selection[replica:] + selection[:replica]
        ids = [case["id"] for case in ordered]
        order = destination / f"case-order-{replica}.json"
        order.write_text(json.dumps(ids))
        for mode in (MODES if replica % 2 == 0 else MODES[::-1]):
            path = destination / f"measure-{replica}-{mode}.jsonl"
            records = invoke(binary, ["measure-set", MANIFEST, order, mode], path, {}, 600)
            assert records[-1] == {"status": "complete"}
            results = [record for record in records[:-1] if "sample" not in record]
            assert [record["id"] for record in results] == ids
            for record in results:
                assert record["status"] == "ok" and record["types"] == ["Int64", "Binary"]
                assert len(record["samples"]) == 3
                for sample in record["samples"]:
                    assert sample["input_rows"] == sample["output_rows"] == INPUT_ROWS
                ns_per_row[record["id"]][mode].append({
                    "execution": median(s["execution_ns"] for s in record["samples"]) / INPUT_ROWS,
                    "above_projection": median(s["increment_ns"] for s in record["samples"]) / INPUT_ROWS,
                })
    (destination / "summary.json").write_text(json.dumps(ns_per_row, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    run(args.binary.resolve(), args.destination.resolve())


if __name__ == "__main__":
    main()
