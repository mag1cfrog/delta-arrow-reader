#!/usr/bin/env python3
"""Time verified Spark-only cases and reduced bitmap windows."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
from statistics import median
import subprocess
import sys

sys.dont_write_bytecode = True
from classify import MODES, write
from run import cases, invoke, probe, sha


def retain(records, ids, input_rows, mode, timings):
    completed = {r["id"]: r for r in records if r.get("status") == "ok" or "samples" in r}
    assert set(completed) == set(ids), (set(ids) - set(completed), mode)
    for case, record in completed.items():
        assert record["configuration"] == mode and len(record["samples"]) == 3
        for sample in record["samples"]:
            assert sample["input_rows"] == input_rows
            assert sample["execution_ns"] - sample["projection_ns"] == sample["increment_ns"]
        timings[case, mode, input_rows].append({
            "execution_ns_per_row": median(s["execution_ns"] / input_rows for s in record["samples"]),
            "increment_ns_per_row": median(s["increment_ns"] / input_rows for s in record["samples"]),
            "peak_rss_bytes": record["process_peak_rss_bytes"],
            "output_rows": record["samples"][0]["output_rows"],
            "types": record["types"],
        })


def measure(full, reduced, manifest, output):
    subprocess.run(["git", "ls-files", "--error-unmatch", str(manifest)], check=True,
                   stdout=subprocess.DEVNULL)
    subprocess.run(["git", "diff", "HEAD", "--exit-code", "--", str(manifest)], check=True)
    eligible = json.loads(manifest.read_text())["functions"]
    verified = {c["id"]: c for c in cases() if c["reference"] == "spark"}
    assert len({case["id"] for case in eligible}) == len(eligible)
    assert all(case == verified[case["id"]] | {"native_sql": None} for case in eligible)
    bitmaps = [c for c in cases() if c["reference"] == "bitmap-reduced"]
    output.mkdir(parents=True, exist_ok=False)
    write(output / "receipt.json", {"protocol_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                                   "manifest_sha256": sha(manifest), "full_sha256": sha(full), "reduced_sha256": sha(reduced)})
    bitmap_manifest = output / "bitmap-manifest.json"
    write(bitmap_manifest, {"functions": bitmaps})
    timings, footprint = defaultdict(list), []
    for replica in range(3):
        modes = MODES if replica % 2 == 0 else MODES[::-1]
        ids = [case["id"] for case in eligible[replica:] + eligible[:replica]]
        id_path = output / f"full-{replica}-ids.json"
        write(id_path, ids)
        for mode in modes:
            if ids:
                records = invoke(full, ["measure-set", manifest, id_path, mode],
                                 output / f"full-{replica}-{mode}.jsonl", {}, 600)
                assert records[-1] == {"status": "complete"}
                retain(records, ids, 1_000_000, mode, timings)
            for rows in (512, 2048):
                for case in bitmaps:
                    name = case["name"]
                    records = probe(reduced, f"bitmap-{rows}", ["measure", str(bitmap_manifest), case["id"], mode],
                                    output / f"{name}-{rows}-{replica}-{mode}.jsonl")
                    retain(records, [case["id"]], rows, mode, timings)
    for case in bitmaps:
        sql = case["spark_sql"]
        aggregate = "SELECT " + sql.split("SELECT id,", 1)[1].split(" OVER (", 1)[0] + " AS value FROM bench"
        for rows in (512, 2048):
            for mode in MODES:
                for shape, statement in (("window", sql), ("aggregate", aggregate)):
                    record, = probe(reduced, f"bitmap-{rows}", ["footprint", statement, mode],
                                    output / f"footprint-{case['name']}-{rows}-{mode}-{shape}.jsonl")
                    assert record["input_rows"] == rows and record["output_rows"] == (rows if shape == "window" else 1)
                    footprint.append({"id": case["id"], "configuration": mode, "shape": shape, **record})
    summary = [{"id": case, "configuration": mode, "input_rows": rows, "replicas": measurements}
               for (case, mode, rows), measurements in sorted(timings.items())]
    assert all(len(row["replicas"]) == 3 for row in summary)
    write(output / "summary.json", {"timings": summary, "bitmap_payloads": footprint})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for path in ("full", "reduced", "manifest", "output"):
        parser.add_argument(path, type=Path)
    args = parser.parse_args()
    measure(*(path.resolve() for path in vars(args).values()))


if __name__ == "__main__":
    main()
