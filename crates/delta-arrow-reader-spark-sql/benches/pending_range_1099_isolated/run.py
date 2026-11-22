"""Compare Spark RANGE isolation and native controls in one bounded session."""

from collections import defaultdict
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import runpy
from statistics import median
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from build import HERE, LEGACY, digest, git

NATIVE_CONTROLS = HERE / "native-controls.json"
MANIFEST = LEGACY / "manifest.json"
FROZEN = runpy.run_path(str(LEGACY / "run.py"))
paired_metric = runpy.run_path(str(HERE.parent / "nullif_620_recheck/run.py"))["paired_metric"]
BLOCKS = 12
CAP_SECONDS = 35 * 60
PROCESS_TIMEOUT = 300
ENV = {"PATH": "/usr/bin:/bin", "TZ": "UTC"}
SPARK_REFERENCE = "773dda5e219f7577a4af560692f3debccf62e03bb925ab5902733e2479ff0f1b"


def read(path):
    return json.loads(path.read_text())


def append(archive, record):
    with gzip.open(archive, "at") as stream:
        stream.write(json.dumps(record, separators=(",", ":")) + "\n")


def invoke(binary, arguments, directory, name, deadline):
    destination = directory / f"{name}.jsonl"
    remaining = min(PROCESS_TIMEOUT, deadline - time.monotonic())
    if remaining <= 0:
        return "schedule_timeout", []
    command = ["taskset", "-c", "2", binary, *arguments]
    with destination.open("x") as output, (directory / f"{name}.stderr").open("x") as error:
        try:
            result = subprocess.run(command, env=ENV, stdout=output, stderr=error,
                                    timeout=remaining)
            status = "ok" if result.returncode == 0 else "process_error"
        except subprocess.TimeoutExpired:
            status = "process_timeout"
    contents = destination.read_text()
    lines = contents.splitlines()
    if status != "ok" and contents and not contents.endswith("\n"):
        lines.pop()
    return status, [json.loads(line) for line in lines]


def reference_rows(reference):
    assert digest(reference) == SPARK_REFERENCE
    with reference.open() as original, tempfile.NamedTemporaryFile(mode="w") as adjusted:
        header = json.loads(next(original))
        for corpus in header["corpora"]:
            assert corpus["source"]["sha256"] == digest(MANIFEST)
            corpus["source"]["path"] = str(MANIFEST)
        adjusted.write(json.dumps(header) + "\n")
        for row in original:
            adjusted.write(row)
        adjusted.flush()
        return FROZEN["load_references"](adjusted.name)[1:]


def preflight(binaries, reference, output, deadline):
    spark = reference_rows(reference)
    traces, ordinary, layouts = {}, {}, {}
    for arm, binary in binaries.items():
        for name, arguments, expected, output_map in (
            ("trace", [MANIFEST], 63, traces),
            ("ordinary-trace", [NATIVE_CONTROLS], 3, ordinary),
            ("layout", [], 1, layouts),
        ):
            status, records = invoke(binary, [name, *arguments], output, f"{arm}-{name}", deadline)
            if status != "ok" or len(records) != expected:
                raise ValueError(f"{arm} {name} incomplete ({status}, {len(records)} records)")
            output_map[arm] = records

    if traces["staging"] != traces["candidate"]:
        left, right = (traces[arm] for arm in binaries)
        differences = [a["id"] for a, b in zip(left, right, strict=True) if a != b]
        raise ValueError(f"retained trace changed in {', '.join(differences)}")
    if ordinary["staging"] != ordinary["candidate"]:
        raise ValueError("ordinary native trace changed")
    if layouts["staging"] != layouts["candidate"]:
        raise ValueError("native window state layout changed")
    by_id = {record["id"]: record for record in traces["staging"]}
    assert len(by_id) == 63
    eligible, excluded = {}, []
    for cell, record in by_id.items():
        shape, mode = record["shape"], record["configuration"]
        assert record["order_is_column"]
        if mode != "native":
            expected, = (row for row in spark if row["coverage"]["shape"] == shape
                         and row["coverage"]["rows"] == record["rows"]
                         and row["ansi"] == (mode == "spark-on"))
            FROZEN["compare_reference"](record, expected)
            native = by_id[cell.rsplit(":", 1)[0] + ":native"]
            same = native["observation"] == record["observation"]
            assert same == (shape != "wrapped"), cell
            if not same:
                excluded.append(cell)
        if shape == "constant":
            eligible[cell] = record["observation"]
    assert len(eligible) == 27 and len(excluded) == 18
    proof = output / "eligibility.json"
    proof.write_text(json.dumps({"issue": 1099, "all_spark_matches": True, "cases": eligible,
                                 "reference": SPARK_REFERENCE, "excluded": sorted(excluded)}) + "\n")
    return proof, ordinary["staging"], layouts["staging"][0]


def detected_shifts(metrics):
    """Apply the one-sided paired sign test with Holm correction."""
    ranked = sorted(metrics, key=lambda row: row["probability"])
    reject = True
    for index, metric in enumerate(ranked):
        cutoff = 0.05 / (len(ranked) - index)
        reject = reject and metric["probability"] <= cutoff
        metric["detected"] = reject and metric["delta"] > metric["noise"]
    return [metric["key"] for metric in ranked if metric["detected"]]


def summarize(timings):
    metrics = {"native": [], "spark": []}
    improvements = []
    for cell, blocks in sorted(timings.items()):
        pairs = [blocks[block] for block in range(BLOCKS)]
        assert all(len(a) == len(b) == 2 for a, b in pairs)
        kind = "native" if cell.endswith(":native") or cell.startswith("native_") else "spark"
        metrics[kind].append(paired_metric(cell, pairs))
        if cell.startswith("constant:4096:1:spark-"):
            improvements.append(paired_metric(cell, [(b, a) for a, b in pairs]))
    assert (len(metrics["native"]), len(metrics["spark"]), len(improvements)) == (12, 18, 2)
    regressions = detected_shifts(metrics["native"])
    gains = detected_shifts(improvements)
    spark_pass = all(row["key"] in gains and row["candidate"] / row["staging"] >= 10
                     for row in improvements)
    anchors = [{"id": row["key"], "paired_improvement_ns": row["delta"],
                "repeat_noise_ns": row["noise"], "probability": row["probability"],
                "speedup": row["candidate"] / row["staging"]} for row in improvements]
    return metrics | {"native_regressions": regressions, "spark_anchors": anchors, "spark_gain": spark_pass,
                      "pass": not regressions and spark_pass}


def run(staging, candidate, reference, output):
    assert not git("status", "--porcelain")
    sources = read(HERE / "sources.json")
    binaries, receipts = {}, {}
    for arm, directory in (("staging", staging), ("candidate", candidate)):
        binary = directory / "pending_range_isolated"
        receipt = read(directory / "receipt.json")
        assert receipt["source"] == sources[arm] and receipt["sha256"] == digest(binary)
        assert receipt["profile"] == "release" and receipt["rustc"].startswith("rustc 1.96.0 ")
        binaries[arm], receipts[arm] = binary, receipt
    assert receipts["staging"]["drivers"] == receipts["candidate"]["drivers"]
    assert receipts["staging"]["sha256"] != receipts["candidate"]["sha256"]
    output.mkdir()
    archive = output / "samples.jsonl.gz"
    append(archive, {"protocol": git("rev-parse", "HEAD"), "started": datetime.now(timezone.utc).isoformat(),
                     "builds": receipts, "legacy_manifest": digest(MANIFEST),
                     "native_controls": digest(NATIVE_CONTROLS)})
    deadline = time.monotonic() + CAP_SECONDS
    try:
        proof, controls, layout = preflight(binaries, reference, output, deadline)
    except (ValueError, AssertionError) as error:
        append(archive, {"preflight": "incomplete", "reason": str(error)})
        return 1
    ordinary = {row["id"]: row for row in controls}
    expected = set(read(proof)["cases"]) | set(ordinary)
    append(archive, {"preflight": "passed", "layout": layout, "excluded": read(proof)["excluded"],
                     "spark_reference": SPARK_REFERENCE, "ordinary": ordinary})
    timings = defaultdict(lambda: defaultdict(lambda: [[], []]))
    for block in range(BLOCKS):
        outer, inner = ("candidate", "staging") if block % 2 else ("staging", "candidate")
        order = (outer, inner, inner, outer)
        for position, arm in enumerate(order):
            arguments = ["measure", MANIFEST, proof, str(block), NATIVE_CONTROLS]
            status, records = invoke(binaries[arm], arguments, output, f"{block:02}-{position}-{arm}", deadline)
            append(archive, {"block": block, "position": position, "arm": arm,
                             "process_status": status, "records": records})
            completed = [row for row in records if row.get("kind") != "retained_sample"]
            observed = defaultdict(list)
            for row in records:
                if row.get("kind") == "retained_sample":
                    observed[row["id"]].append((row["iteration"], row["elapsed_ns"]))
            if (status != "ok" or len(completed) != 30
                or {row["id"] for row in completed} != expected or set(observed) != expected):
                append(archive, {"validation": "incomplete", "block": block, "position": position})
                return 1
            for row in completed:
                cell, samples = row["id"], row["samples_ns"]
                if (len(samples) != 7 or not all(type(ns) is int and ns > 0 for ns in samples)
                    or observed[cell] != list(enumerate(samples, start=1))):
                    append(archive, {"validation": "invalid_samples", "id": cell, "block": block})
                    return 1
                if cell in ordinary and row != ordinary[cell] | {"samples_ns": samples}:
                    append(archive, {"validation": "ordinary_plan_changed", "id": cell, "block": block})
                    return 1
                timings[cell][block][arm == "candidate"].append(median(samples))
        print("RANGE block", block + 1, "complete", flush=True)
    summary = summarize(timings)
    append(archive, {"summary": summary, "finished": datetime.now(timezone.utc).isoformat()})
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return int(not summary["pass"])


if __name__ == "__main__":
    sys.exit(run(*(Path(arg).resolve() for arg in sys.argv[1:])))
