#!/usr/bin/env python3
"""Check the rebased NULLIF candidate against staging in one session."""
import argparse
from collections import defaultdict
import gzip
import hashlib
import json
from math import comb
import os
from pathlib import Path
from statistics import median
import subprocess
import time

if not __debug__:
    raise RuntimeError("validation requires Python without -O")

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
MANIFEST = json.loads((HERE / "manifest.json").read_text())
FIXTURE = json.loads((HERE.parent / "nullif_620/manifest.json").read_text())
BUILDS = ("staging", "candidate")
MODES = ("native", "spark-off", "spark-on")
PHASES = ("planning_ns", "execution_ns")
FAMILIES = ("nullif", "checked")


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def call(binary, args, destination, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("session deadline exhausted")
    command = ["taskset", "-c", ",".join(map(str, MANIFEST["cpus"])), str(binary),
               *map(str, args)]
    status = None
    try:
        result = subprocess.run(command, capture_output=True, check=False,
                                timeout=min(MANIFEST["process_seconds"], remaining))
        stdout, stderr, status = result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired as error:
        stdout, stderr = error.stdout or b"", error.stderr or b""
    destination.with_suffix(".jsonl.gz").write_bytes(gzip.compress(stdout, mtime=0))
    destination.with_suffix(".stderr").write_bytes(stderr)
    if status != 0:
        raise RuntimeError(f"{destination.name} exited {status}; evidence at {destination}")
    return [json.loads(line) for line in stdout.splitlines()]


def load_builds(receipts):
    binaries, identities = {}, {}
    for name, path in zip(BUILDS, receipts, strict=True):
        record = json.loads(path.read_text())
        binary = Path(record["binary"]).resolve(strict=True)
        assert record["source_commit"] == MANIFEST[name]
        assert record["binary_sha256"] == digest(binary)
        assert record["profile"] == "release" and record["target"] == "nullif_620"
        assert "--release" in record["command"] and "--locked" in record["command"]
        binaries[name] = binary
        identities[name] = {"receipt_sha256": digest(path), **record}
    assert identities["staging"]["environment"] == identities["candidate"]["environment"]
    assert identities["staging"]["toolchain"] == identities["candidate"]["toolchain"]
    return binaries, identities


def cell_ids(family):
    return {f"{family}-{case['name']}-{fraction}-{batch}" for case in FIXTURE["families"][family]
            for fraction in FIXTURE["null_fractions"] for batch in FIXTURE["batch_rows"]}


def preflight(output, binaries, identities):
    output.mkdir()
    deadline = time.monotonic() + MANIFEST["preflight_seconds"]
    records = {}
    for family in FAMILIES:
        for build in BUILDS:
            label = f"{build}-{family}"
            observed = call(binaries[build], ["preflight", family, output / label],
                            output / label, deadline)
            indexed = {(row["id"], row["configuration"]): row
                       for row in observed if row["kind"] == "preflight"}
            assert set(indexed) == {(cell, mode) for cell in cell_ids(family) for mode in MODES}
            effects = [row for row in observed if row["kind"] == "effects"]
            assert len(observed) == len(indexed) + len(effects)
            assert len(effects) == (16 if family == "nullif" else 0)
            records[build, family] = (indexed, effects)
    comparisons = []
    for family in FAMILIES:
        old, old_effects = records["staging", family]
        new, new_effects = records["candidate", family]
        assert old_effects == new_effects
        for cell in sorted(cell_ids(family)):
            reference = Path(old[cell, "native"]["ipc_path"])
            reference_hash = digest(reference)
            for build, rows in zip(BUILDS, (old, new), strict=True):
                for mode in MODES:
                    row = rows[cell, mode]
                    path = Path(row["ipc_path"])
                    assert row["rows"] == FIXTURE["rows"] and row["formula_match"], (build, cell, mode)
                    checksum = digest(path)
                    if checksum != reference_hash:
                        label = f"compare-{build}-{cell}-{mode}"
                        result = call(binaries[build], ["compare", reference, path],
                                      output / label, deadline)
                        assert len(result) == 1 and result[0]["equal"], (build, cell, mode)
                    comparisons.append({"id":cell,"configuration":mode,"build":build,
                                        "ipc_sha256":checksum,"native_ipc_sha256":reference_hash,
                                        "executed_types":sorted(set(row["executed_types"]))})
    write_json(output / "receipt.json", {"builds":identities,"comparisons":comparisons})
    print(f"{len(comparisons)} full-result and executed-type checks passed", flush=True)


def measure(output, binaries, identities, receipt):
    assert json.loads(receipt.read_text())["builds"] == identities
    assert receipt.is_file()
    source = subprocess.check_output(["git", "-C", str(WORKSPACE), "rev-parse", "HEAD"],
                                     text=True).strip()
    assert not subprocess.check_output(["git", "-C", str(WORKSPACE), "status", "--porcelain"])
    for path in (Path(__file__), HERE / "manifest.json",
                 HERE.parent / "reports/nullif-620-recheck-protocol.md"):
        subprocess.run(["git", "-C", str(WORKSPACE), "ls-files", "--error-unmatch", str(path)],
                       check=True, stdout=subprocess.DEVNULL)
    output.mkdir()
    selected = set(MANIFEST["native_gates"]) | set(MANIFEST["fallback_splits_ns"]) | set(MANIFEST["spark_speedup_anchors"])
    matrices = {"nullif":selected,"checked":cell_ids("checked")}
    schedules = []
    for block in range(1, max(MANIFEST["blocks"].values()) + 1):
        order = ("staging", "candidate", "candidate", "staging") if block % 2 else (
            "candidate", "staging", "staging", "candidate")
        for family in FAMILIES if block % 2 else reversed(FAMILIES):
            if block > MANIFEST["blocks"][family]:
                continue
            for position, build in enumerate(order, 1):
                schedules.append({"block":block,"family":family,"position":position,"build":build})
    write_json(output / "schedule.json", {"protocol_commit":source,"preflight_sha256":digest(receipt),
                                           "builds":identities,"slots":schedules})
    for family in FAMILIES:
        write_json(output / f"{family}-eligibility.json",
                   {"excluded":sorted(cell_ids(family) - matrices[family])})
    deadline = time.monotonic() + MANIFEST["timing_seconds"]
    for slot in schedules:
        block, family, position, build = (slot[key] for key in ("block","family","position","build"))
        label = f"{block:02}-{family}-{position}-{build}"
        records = call(binaries[build], ["measure", family, "main", block,
                                          output / f"{family}-eligibility.json"], output / label, deadline)
        expected = {(cell,mode,sample) for cell in matrices[family] for mode in MODES for sample in range(3)}
        assert {(r["id"],r["configuration"],r["sample"]) for r in records} == expected
        assert len(records) == len(expected)
        assert all(r["kind"]=="sample" and r["replica"]==block and r["rows"]==FIXTURE["rows"]
                   and r["planning_calls"]==r["execution_calls"]==1 for r in records)
        print(label, len(records), flush=True)
    write_json(output / "complete.json", {"processes":len(schedules)})


def sign_probability(deltas):
    positive = sum(v > 0 for v in deltas)
    trials = sum(v != 0 for v in deltas)
    return sum(comb(trials,n) for n in range(positive,trials+1)) / (2 ** trials)


def paired_metric(key, pairs):
    deltas = [median(b) - median(a) for a,b in pairs]
    repeat_noise = []
    for build in range(2):
        differences = [abs(pair[build][1] - pair[build][0])
                       for pair in pairs if len(pair[build]) == 2]
        repeat_noise.append(median(differences) if differences else None)
    noise = max((v for v in repeat_noise if v is not None), default=0)
    return {"key":key,"pairs":len(pairs),"delta":median(deltas) if deltas else None,
            "pair_deltas":deltas,"repeat_noise":repeat_noise,
            "staging":median([x for a,_ in pairs for x in a]) if pairs else None,
            "candidate":median([x for _,b in pairs for x in b]) if pairs else None,
            "noise":noise,"probability":sign_probability(deltas)}


def summarize(output):
    schedule = json.loads((output / "schedule.json").read_text())["slots"]
    complete = json.loads((output / "complete.json").read_text())
    assert complete["processes"] == len(schedule) == 4 * sum(MANIFEST["blocks"].values())
    observed = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for slot in schedule:
        block, family, position, build = (slot[key] for key in ("block","family","position","build"))
        label = f"{block:02}-{family}-{position}-{build}"
        samples = defaultdict(list)
        with gzip.open(output / f"{label}.jsonl.gz", "rt") as source:
            for row in map(json.loads,source):
                for phase in PHASES:
                    samples[row["id"],row["configuration"],phase].append(row[phase])
        for key, valid in samples.items():
            assert len(valid)==3
            observed[key][block][build].append(median(valid))

    results, controls, fallback_gates, regimes = [], [], [], []
    for key, blocks in sorted(observed.items()):
        cell, mode, phase = key
        pairs = [(b["staging"],b["candidate"]) for b in blocks.values()]
        family = cell.split("-",1)[0]
        assert len(pairs)==MANIFEST["blocks"][family] and all(len(a)==len(b)==2 for a,b in pairs)
        metric = paired_metric(key, pairs)
        results.append(metric)
        fallback = cell in MANIFEST["fallback_splits_ns"] and mode != "native" and phase == "execution_ns"
        if (cell.startswith("checked-") or
            (mode == "native" and MANIFEST["native_gates"].get(cell)==phase)):
            controls.append(metric)
        if not fallback:
            continue
        fallback_gates.append(metric)
        split = MANIFEST["fallback_splits_ns"][cell]
        occupancy = [([sum(v >= split for v in a)],[sum(v >= split for v in b)]) for a,b in pairs]
        fallback_gates.append(paired_metric((*key,"upper-mode-count"),occupancy))
        for label, high in (("lower",False),("upper",True)):
            allocated = [([v for v in a if (v>=split)==high],[v for v in b if (v>=split)==high]) for a,b in pairs]
            matched = [p for p in allocated if all(p)]
            counts = [sum(len(p[index]) for p in allocated) for index in (0,1)]
            regime = paired_metric((*key,label), matched)
            regime["counts"] = counts
            regime["steady_values"] = [median(values) if values else None
                                        for index in (0,1) for values in [[v for p in allocated for v in p[index]]]]
            usable = sum(delta != 0 for delta in regime["pair_deltas"])
            regime["sufficient"] = (usable >= MANIFEST["minimum_matched_fallback_blocks"]
                                     and all(value is not None for value in regime["repeat_noise"]))
            regimes.append(regime)
            fallback_gates.append(regime)

    for group, gates in (("native_and_checked", controls), ("fallback", fallback_gates)):
        ordered = sorted(gates, key=lambda row:row["probability"])
        blocked = False
        for index, metric in enumerate(ordered):
            threshold = .025 / (len(ordered)-index)
            blocked = blocked or metric["probability"] > threshold
            metric.update(group=group, threshold=threshold,
                          detected_slower=(not blocked and metric["delta"] is not None
                                           and metric["delta"] > metric["noise"]))
    gates = controls + fallback_gates
    regressions = [m["key"] for m in gates if m["detected_slower"]]
    unresolved = [m["key"] for m in regimes if not m["sufficient"]]
    write_json(output / "summary.json", {"all_metrics":results,"regimes":regimes,
                                        "detected_regressions":regressions,
                                        "inconclusive_regimes":unresolved,"gate_metrics":gates})
    print(json.dumps({"detected_regressions":regressions,"inconclusive_regimes":unresolved}),flush=True)


def check():
    assert len(cell_ids("nullif"))==72 and len(cell_ids("checked"))==36
    selected = set(MANIFEST["native_gates"]) | set(MANIFEST["fallback_splits_ns"]) | set(MANIFEST["spark_speedup_anchors"])
    assert len(selected)==7 and selected <= cell_ids("nullif")
    assert MANIFEST["blocks"]=={"nullif":48,"checked":24}
    assert 2 ** (-MANIFEST["minimum_matched_fallback_blocks"]) <= .025/16
    assert sign_probability([1]*24)==2**-24
    assert sign_probability([0]*24)==1
    result = paired_metric(("test",), [([10,12],[21,23])] * 24)
    assert result["delta"]==11 and result["noise"]==2 and result["probability"]==2**-24
    balanced = paired_metric(("test",), [([30,10],[20,40]),([10,30],[40,20])] * 12)
    assert balanced["delta"]==10 and balanced["noise"]==20


def main():
    check()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check","preflight","measure","summarize"))
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--builds", nargs=2, type=Path)
    parser.add_argument("--preflight-receipt", type=Path)
    args = parser.parse_args()
    if args.command=="check":
        return
    if args.command=="summarize":
        summarize(args.output)
        return
    binaries, identities = load_builds(args.builds)
    if args.command=="preflight":
        preflight(args.output,binaries,identities)
    else:
        measure(args.output,binaries,identities,args.preflight_receipt)


if __name__ == "__main__":
    main()
