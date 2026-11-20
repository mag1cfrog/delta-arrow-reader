#!/usr/bin/env python3
"""Recheck the two canonical #1099 builds under one bounded schedule."""

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import runpy
import shutil
from statistics import median
import subprocess
import tempfile
from types import SimpleNamespace


if not __debug__:
    raise RuntimeError("validation requires Python without -O")

HERE = Path(__file__).resolve().parent
range_bench = SimpleNamespace(**runpy.run_path(str(HERE.parent / "pending_range_1099/run.py")))
paired_metric = runpy.run_path(str(HERE.parent / "nullif_620_recheck/run.py"))["paired_metric"]
STAGING = "6a52046360244651eb7557b5c195438c05d3c6de"
CANDIDATE = "063b52e2ab6dc3de73bbe8330c9b090ba7370d97"
REFERENCE = "773dda5e219f7577a4af560692f3debccf62e03bb925ab5902733e2479ff0f1b"
BLOCKS = 12


def build_candidate(source, target, destination):
    destination = range_bench.fresh(destination)
    assembly = range_bench.read(source / "pending-range-1099-assembly.json")
    assert assembly["commit"] == CANDIDATE and not assembly["observed"]
    assert range_bench.tree_hash(source) == assembly["tree_sha256"]
    assert assembly["protocol"] == range_bench.protocol_hashes()
    assert not target.is_relative_to(source) and not target.is_relative_to(range_bench.REPO)
    workspace = source / range_bench.WORKSPACE.relative_to(range_bench.REPO)
    # Invalidate the files changed by this candidate in the shared target.
    patched_sources = {range_bench.STATE, range_bench.EXPR}
    for name in set(assembly["generated"]) | patched_sources:
        (workspace / name).touch()
    settings = dict(range_bench.RELEASE, CARGO_TARGET_DIR=str(target))
    env = os.environ.copy()
    for name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"):
        assert not env.get(name), name
    command = ["cargo", "+1.96.0", "build", "--locked", "--release", "--bench",
               "pending_range_1099", "--message-format=json-render-diagnostics"]
    log = destination / "build.jsonl"
    errors = destination / "build.stderr"
    with log.open("x") as stdout, errors.open("x") as stderr:
        subprocess.run(command, cwd=workspace, env=env | settings, stdout=stdout,
                       stderr=stderr, check=True)
    assert range_bench.tree_hash(source) == assembly["tree_sha256"]
    artifacts = (json.loads(line) for line in log.read_text().splitlines() if line.startswith("{"))
    artifact, = [row["executable"] for row in artifacts if row.get("executable")
                 and row.get("target", {}).get("name") == "pending_range_1099"]
    binary = destination / "pending_range_1099"
    shutil.copy2(artifact, binary)
    range_bench.write(destination / "receipt.json", {
        "assembly": assembly, "source": str(source), "binary": str(binary),
        "binary_sha256": range_bench.digest(binary), "compiler_reported_binary": artifact,
        "protocol": range_bench.protocol_hashes(), "command": command, "environment": settings,
        "rustc": subprocess.check_output(["rustc", "+1.96.0", "--version", "--verbose"], text=True),
        "build_log_sha256": range_bench.digest(log), "build_stderr_sha256": range_bench.digest(errors),
    })


def spark_references(reference):
    assert range_bench.digest(reference) == REFERENCE
    manifest_digest = range_bench.digest(range_bench.MANIFEST)
    with reference.open() as original, tempfile.NamedTemporaryFile(mode="w") as copy:
        header = json.loads(next(original))
        for corpus in header["corpora"]:
            assert corpus["source"]["sha256"] == manifest_digest
            # The first-round validator also checks the absolute path recorded at capture time.
            corpus["source"]["path"] = str(range_bench.MANIFEST)
        copy.write(json.dumps(header) + "\n")
        shutil.copyfileobj(original, copy)
        copy.flush()
        return range_bench.load_references(copy.name)[1:]


def preflight(receipts, reference, output):
    spark = spark_references(reference)
    arms = []
    for receipt, name in zip(receipts, ("staging", "candidate"), strict=True):
        records = range_bench.invoke(receipt, ["trace", range_bench.MANIFEST], output, name)
        arm = {record["id"]: record for record in records}
        assert len(arm) == len(records) == 63
        assert all(record["order_is_column"] for record in records)
        arms.append(arm)
    assert arms[0] == arms[1], "candidate changed a result, plan or source delivery"
    cases, excluded = {}, []
    for cell, record in arms[0].items():
        mode = record["configuration"]
        shape = record["shape"]
        if mode != "native":
            expected, = (r for r in spark if r["coverage"]["shape"] == shape
                         and r["coverage"]["rows"] == record["rows"]
                         and r["ansi"] == (mode == "spark-on"))
            range_bench.compare_reference(record, expected)
            native = arms[0][cell.rsplit(":", 1)[0] + ":native"]
            same = native["observation"] == record["observation"]
            assert same == (shape != "wrapped"), cell
            if not same:
                excluded.append(cell)
        if shape == "constant":
            cases[cell] = record["observation"]
    assert len(cases) == 27 and len(excluded) == 18
    proof = output / "eligibility.json"
    range_bench.write(proof, {"issue": 1099, "all_spark_matches": True, "cases": cases,
                             "excluded": sorted(excluded), "reference": REFERENCE})
    return proof


def gates(metrics):
    ordered = sorted(metrics, key=lambda row: row["probability"])
    rejected = True
    for index, metric in enumerate(ordered):
        threshold = .05 / (len(ordered) - index)
        rejected = rejected and metric["probability"] <= threshold
        metric.update(threshold=threshold, detected=bool(rejected and metric["delta"] > metric["noise"]))
    return [metric["key"] for metric in ordered if metric["detected"]]


def measure(receipts, proof, output):
    cells = set(range_bench.read(proof)["cases"])
    timings = defaultdict(lambda: defaultdict(lambda: [[], []]))
    for block in range(BLOCKS):
        order = (0, 1, 1, 0) if block % 2 == 0 else (1, 0, 0, 1)
        for position, arm in enumerate(order):
            name = f"{block:02}-{position}-{'candidate' if arm else 'staging'}"
            rows = range_bench.invoke(receipts[arm], ["measure", range_bench.MANIFEST, proof, block],
                                      output, name)
            assert len(rows) == len(cells) and {row["id"] for row in rows} == cells
            for row in rows:
                samples = row["samples_ns"]
                assert len(samples) == 7 and all(isinstance(ns, int) and ns > 0 for ns in samples)
                timings[row["id"]][block][arm].append(median(samples))
            print(name, len(rows), flush=True)

    native, spark, anchors = [], [], []
    for cell, blocks in sorted(timings.items()):
        pairs = list(blocks.values())
        assert len(pairs) == BLOCKS and all(len(a) == len(b) == 2 for a, b in pairs)
        metric = paired_metric(cell, pairs)
        if cell.endswith(":native"):
            native.append(metric)
        else:
            spark.append(metric)
            if cell.startswith("constant:4096:1:"):
                anchors.append(paired_metric(cell, [(b, a) for a, b in pairs]))
    assert (len(native), len(spark), len(anchors)) == (9, 18, 2)
    regressions = gates(native)
    faster = gates(anchors)
    anchor_pass = all(m["key"] in faster and m["candidate"] / m["staging"] >= 10 for m in anchors)
    summary = {"native": native, "spark": spark, "spark_anchors": anchors,
               "native_regressions": regressions, "spark_gain_confirmed": anchor_pass,
               "pass": not regressions and anchor_pass}
    range_bench.write(output / "summary.json", summary)
    print(json.dumps({key: summary[key] for key in ("native_regressions", "spark_gain_confirmed", "pass")}))


def run(staging, candidate, reference, output):
    receipts = [range_bench.verify_build(path) for path in (staging, candidate)]
    for receipt, commit in zip(receipts, (STAGING, CANDIDATE), strict=True):
        assert receipt["assembly"]["commit"] == commit and not receipt["assembly"]["observed"]
    base, change = receipts
    for name in ("command", "environment", "rustc"):
        assert base[name] == change[name], name
    assert base["assembly"]["generated"] == change["assembly"]["generated"]
    assert base["binary_sha256"] != change["binary_sha256"]
    repo = range_bench.REPO
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True).strip()
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    output = range_bench.fresh(output)
    range_bench.write(output / "session.json", {
        "protocol_commit": revision, "runner_sha256": range_bench.digest(__file__),
        "builds": receipts, "blocks": BLOCKS, "uname": list(os.uname()),
        "cpuinfo": Path("/proc/cpuinfo").read_text(),
        "affinity": sorted(os.sched_getaffinity(0)),
    })
    proof = preflight(receipts, reference, output)
    measure(receipts, proof, output)


def check():
    same = [([98, 102], [99, 101])] * BLOCKS
    slower = [([98, 102], [129, 131])] * BLOCKS
    assert gates([paired_metric("same", same), paired_metric("slower", slower)]) == ["slower"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check")
    for name, paths in (("build", ("source", "target", "destination")),
                        ("run", ("staging", "candidate", "reference", "output"))):
        command = commands.add_parser(name)
        for path in paths:
            command.add_argument(path, type=lambda value: Path(value).resolve())
    args = vars(parser.parse_args())
    command = args.pop("command")
    {"check": check, "build": build_candidate, "run": run}[command](**args)


if __name__ == "__main__":
    main()
