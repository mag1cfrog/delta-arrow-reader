"""Retest only the frozen ten-byte to_date case in one two-build session."""

import argparse
import gzip
import json
from pathlib import Path
import runpy
import statistics
import subprocess
import sys
import time

if not __debug__:
    raise RuntimeError("validation requires Python assertions")

HERE = Path(__file__).resolve().parent
FROZEN = HERE.parent / "functions"
MANIFEST = HERE / "manifest.json"
CONFIGURATIONS = ("native", "spark-off", "spark-on")
ROWS = 1_000_000


def collect_timings(runner, binaries, case, schedule, types, output):
    ids = output / "ids.json"
    ids.write_text(json.dumps([case]))
    deadline = time.monotonic() + 600
    records = []
    for series in ("noise", "main"):
        for replica in range(3):
            ordered = schedule[f"{series}_orders"][replica]
            for arm in ordered:
                configuration = "native" if series == "noise" else arm
                offset = (arm == "b") if series == "noise" else CONFIGURATIONS.index(arm)
                order = tuple(binaries) if (replica + offset) % 2 else tuple(reversed(binaries))
                for role in order:
                    process = f"{series}-{replica + 1}-{arm}"
                    print(role, process, flush=True)
                    result = runner["invoke"](
                        binaries[role], ["measure-set", MANIFEST, ids, configuration],
                        output / role / process, schedule, deadline,
                    )
                    assert result["status"] == "ok", result
                    row, = runner["assemble_cases"](result, [case], configuration)
                    assert row["status"] == "ok" and row["types"] == types[configuration], row
                    assert len(row["samples"]) == 3
                    assert all(sample["input_rows"] == sample["output_rows"] == ROWS for sample in row["samples"])
                    records.append(dict(row, build=role, series=series, arm=arm, replica=replica + 1))
    assert len(records) == 30
    return records


def summarize(runner, records):
    def process_medians(role, series, arm):
        values = [statistics.median(s["increment_ns"] / ROWS for s in record["samples"])
                  for record in records
                  if (record["build"], record["series"], record["arm"]) == (role, series, arm)]
        assert len(values) == 3
        return values

    summary = []
    for role in ("staging", "candidate"):
        native = process_medians(role, "main", "native")
        native_repeat = runner["noise"](
            process_medians(role, "noise", "a"), process_medians(role, "noise", "b"),
        )
        for mode in CONFIGURATIONS[1:]:
            spark = process_medians(role, "main", mode)
            floor = max(native_repeat, (max(native) - min(native)) / 2, (max(spark) - min(spark)) / 2)
            gap = statistics.median(spark) - statistics.median(native)
            summary.append({
                "build": role, "mode": mode, "spark_ns_per_row": statistics.median(spark),
                "native_ns_per_row": statistics.median(native), "gap_ns_per_row": gap,
                "noise_ns_per_row": floor, "gap_above_noise": gap > floor,
            })
    return summary


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    for name in ("staging", "candidate", "output"):
        cli.add_argument(name, type=Path)
    args = cli.parse_args()
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=HERE, text=True).strip()
    sys.path.insert(0, str(FROZEN))
    runner = runpy.run_path(str(FROZEN / "run.py"))
    read, digest = runner["read"], runner["digest"]
    manifest = read(MANIFEST)
    assert digest(FROZEN / "manifest.json") == manifest["source_manifest_sha256"]
    case, = manifest["functions"]
    assert case in read(FROZEN / "manifest.json")["functions"]
    schedule = read(FROZEN / "schedule.json")
    schedule.update(process_timeout_seconds=60, preflight_process_timeout_seconds=120)
    binaries = {name: getattr(args, name).resolve() for name in ("staging", "candidate")}
    hashes = {name: digest(binary) for name, binary in binaries.items()}
    assert len(set(hashes.values())) == 2
    output = args.output.resolve()
    output.mkdir()
    (output / "session.json").write_text(json.dumps({"binaries": hashes, "manifest": manifest,
        "protocol_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=HERE, text=True).strip()
    }, indent=2) + "\n")
    deadline = time.monotonic() + 240
    types = {}
    for role, binary in binaries.items():
        work = output / "preflight" / role
        result = runner["invoke"](binary, ["preflight", MANIFEST, case["id"], work / "plans"],
                                  work, schedule, deadline, preflight=True)
        preflight = dict(result["records"][0], status="ok") if result["status"] == "ok" else result
        admission = runner["eligibility"](case, preflight)
        if admission["status"] != "eligible":
            (output / "excluded.json").write_text(json.dumps({role: admission}, indent=2) + "\n")
            return
        assert all(row["rows"] == ROWS for row in preflight["results"])
        assert all(row["rows"] == ROWS for row in preflight["baselines"])
        types[role] = {row["configuration"]: row["types"] for row in preflight["results"]}
    if types["staging"] != types["candidate"]:
        (output / "excluded.json").write_text(json.dumps({"type_mismatch": types}, indent=2) + "\n")
        return
    records = collect_timings(runner, binaries, case["id"], schedule, types["staging"], output)
    with gzip.GzipFile(filename=str(output / "samples.jsonl.gz"), mode="xb", mtime=0) as compressed:
        for record in records:
            compressed.write((json.dumps(record, separators=(",", ":")) + "\n").encode())
    (output / "summary.json").write_text(json.dumps({
        "processes": len(records), "samples": sum(len(record["samples"]) for record in records),
        "timings": summarize(runner, records),
    }, indent=2) + "\n")


if __name__ == "__main__":
    main()
