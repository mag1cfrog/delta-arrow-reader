#!/usr/bin/env python3
"""Run only the committed NULLIF #620 matrix; never build or retry a binary."""

import argparse
import gzip
import hashlib
import json
import os
import pathlib
import platform
import statistics
import subprocess
import tempfile
import time

if not __debug__:
    raise RuntimeError("validation requires Python without -O")

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[3]
MANIFEST_PATH = HERE / "manifest.json"
MANIFEST = json.loads(MANIFEST_PATH.read_text())
EVIDENCE = pathlib.Path("/home/hanbo/.local/share/delta-arrow-reader-review/costs-168-2026-11-19/620")


def digest(path):
    with pathlib.Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])


def binding(binary):
    if git("status", "--porcelain").strip():
        raise RuntimeError("commit the reviewed source before execution")
    paths = git("ls-files", "-z", "*.rs", "*.toml", "*.lock", ".cargo/*").decode().split("\0")
    source = hashlib.sha256()
    for path in sorted(filter(None, paths)):
        source.update(path.encode() + b"\0" + digest(ROOT / path).encode() + b"\0")
    return {
        "source_commit": git("rev-parse", "HEAD").decode().strip(),
        "source_index_sha256": hashlib.sha256(git("ls-files", "-s")).hexdigest(),
        "source_sha256": source.hexdigest(),
        "binary": str(binary), "binary_sha256": digest(binary),
        "manifest_sha256": digest(MANIFEST_PATH),
        "harness_sha256": digest(HERE.parent / "nullif_620.rs"),
        "controller_sha256": digest(__file__),
        "protocol_sha256": digest(HERE.parent / "reports/nullif-620-protocol.md"),
        "host": platform.node(), "platform": platform.platform(),
        "cpu": pathlib.Path("/proc/cpuinfo").read_text(),
        "affinity": MANIFEST["cpus"],
    }


def check_space():
    size = sum(p.stat().st_size for p in EVIDENCE.rglob("*") if p.is_file())
    if size > MANIFEST["evidence_limit_bytes"]:
        raise RuntimeError(f"evidence cap exceeded: {size}")
    return size


def call(binary, arguments, output, deadline, allowed=(0,)):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("campaign timeout")
    try:
        result = subprocess.run(
            [str(binary), *map(str, arguments)], capture_output=True,
            timeout=min(MANIFEST["process_timeout_seconds"], remaining),
            preexec_fn=lambda: os.sched_setaffinity(0, MANIFEST["cpus"]),
        )
    except subprocess.TimeoutExpired as error:
        with gzip.open(str(output) + ".jsonl.gz", "wb") as stream:
            stream.write(error.stdout or b"")
        pathlib.Path(str(output) + ".stderr").write_bytes(error.stderr or b"")
        raise RuntimeError(f"process timed out; partial evidence at {output}") from error
    with gzip.open(str(output) + ".jsonl.gz", "wb") as stream:
        stream.write(result.stdout)
    pathlib.Path(str(output) + ".stderr").write_bytes(result.stderr)
    check_space()
    if result.returncode not in allowed:
        raise RuntimeError(f"{arguments}: exit {result.returncode}; see {output}.stderr")
    records = [json.loads(line) for line in result.stdout.splitlines()]
    return result.returncode, records


def cells(family):
    return [f"{family}-{case['name']}-{fraction}-{batch}" for batch in MANIFEST["batch_rows"]
            for fraction in MANIFEST["null_fractions"] for case in MANIFEST["families"][family]]


def compare(binary, left, right, output, deadline):
    status, records = call(binary, ["compare", left, right], output, deadline, (0, 2))
    if len(records) != 1 or records[0].get("equal") != (status == 0):
        raise RuntimeError("invalid complete IPC comparison")
    return records[0]


def preflight(binary, output, family, baseline, metadata, deadline):
    _, records = call(binary, ["preflight", family, output / "ipc"], output / "preflight", deadline)
    results = [record for record in records if record["kind"] == "preflight"]
    indexed = {(record["id"], record["configuration"]): record for record in results}
    expected = {(cell, mode) for cell in cells(family) for mode in MANIFEST["configurations"]}
    if set(indexed) != expected or len(results) != len(expected):
        raise RuntimeError("incomplete preflight cell inventory")
    old = json.loads(baseline.read_text()) if baseline else None
    if old and (old["family"] != family or old["binding"]["manifest_sha256"] != metadata["manifest_sha256"]):
        raise RuntimeError("baseline input manifest changed")
    old_results = {(r["id"], r["configuration"]): r for r in old["results"]} if old else {}
    excluded, comparisons = [], []
    for cell in cells(family):
        for mode in ["spark-off", "spark-on"]:
            comparison = compare(binary, indexed[cell, "native"]["ipc_path"], indexed[cell, mode]["ipc_path"], output / f"compare-{cell}-{mode}", deadline)
            comparisons.append({"id": cell, "configuration": mode, **comparison})
            if not comparison["equal"] and cell not in excluded:
                excluded.append(cell)
        if cell not in excluded and not all(indexed[cell, mode]["formula_match"] for mode in MANIFEST["configurations"]):
            raise RuntimeError(f"native-equivalent output violates the independent formula: {cell}")
        for mode in MANIFEST["configurations"]:
            record = indexed[cell, mode]
            path = pathlib.Path(record["ipc_path"])
            record["ipc_sha256"] = digest(path)
            if old:
                prior = old_results[cell, mode]
                compressed = pathlib.Path(prior["ipc_gzip_path"])
                if digest(compressed) != prior["ipc_gzip_sha256"]:
                    raise RuntimeError("baseline IPC checksum changed")
                with tempfile.TemporaryDirectory(dir=output) as temporary:
                    decoded = pathlib.Path(temporary) / "baseline.arrow"
                    with gzip.open(compressed, "rb") as source, decoded.open("wb") as target:
                        target.write(source.read())
                    if digest(decoded) != prior["ipc_sha256"]:
                        raise RuntimeError("decoded baseline IPC checksum changed")
                    comparison = compare(binary, decoded, path, output / f"old-new-{cell}-{mode}", deadline)
                    if not comparison["equal"]:
                        raise RuntimeError(f"baseline/candidate result changed: {cell} {mode}")
            compressed = path.with_suffix(".arrow.gz")
            with path.open("rb") as source, gzip.open(compressed, "wb") as target:
                target.write(source.read())
            record["ipc_gzip_path"] = str(compressed)
            record["ipc_gzip_sha256"] = digest(compressed)
            path.unlink()
            check_space()
    receipt = {"binding": metadata, "family": family, "excluded": excluded, "results": results,
               "native_comparisons": comparisons, "effects": [r for r in records if r["kind"] == "effects"],
               "baseline_receipt_sha256": digest(baseline) if baseline else None}
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


def measure(binary, output, receipt_path, metadata, deadline):
    receipt = json.loads(receipt_path.read_text())
    for key in ["source_sha256", "binary_sha256", "manifest_sha256", "harness_sha256", "controller_sha256", "protocol_sha256"]:
        if receipt["binding"][key] != metadata[key]:
            raise RuntimeError(f"preflight binding changed: {key}")
    family = receipt["family"]
    expected_cells = set(cells(family)) - set(receipt["excluded"])
    for process in MANIFEST["processes"]:
        arm, replica = process["arm"], process["replica"]
        _, records = call(binary, ["measure", family, arm, replica, receipt_path], output / f"{arm}-{replica}", deadline)
        modes = ["native"] if arm == "native-b" else MANIFEST["configurations"]
        expected = {(cell, mode, sample) for cell in expected_cells for mode in modes for sample in range(MANIFEST["samples"])}
        actual = {(r["id"], r["configuration"], r["sample"]) for r in records}
        if actual != expected or len(records) != len(expected):
            raise RuntimeError("incomplete or duplicate timing samples")
        if any(r["rows"] != MANIFEST["rows"] or r["planning_calls"] != 1 or r["execution_calls"] != 1 for r in records):
            raise RuntimeError("timing counters changed")
    (output / "eligibility.json").write_text(json.dumps({"family": family, "excluded": receipt["excluded"], "receipt_sha256": digest(receipt_path)}, indent=2) + "\n")


def summarize(output):
    grouped = {}
    for path in output.glob("*.jsonl.gz"):
        with gzip.open(path, "rt") as source:
            for line in source:
                record = json.loads(line)
                if record.get("kind") == "sample":
                    grouped.setdefault((record["id"], record["configuration"], record["arm"], record["replica"]), []).append(record)
    summary = {}
    for cell in sorted({key[0] for key in grouped}):
        entry = {}
        for mode, arm in [("native", "main"), ("native", "native-b"), ("spark-off", "main"), ("spark-on", "main")]:
            phases = {}
            for phase in ["planning_ns", "execution_ns"]:
                process_medians = []
                for replica in [1, 2, 3]:
                    samples = grouped[cell, mode, arm, replica]
                    assert len(samples) == MANIFEST["samples"]
                    process_medians.append(statistics.median(r[phase] for r in samples))
                quartiles = statistics.quantiles(process_medians, n=4, method="inclusive")
                median = statistics.median(process_medians)
                phases[phase] = {"median": median, "ms": median / 1e6, "ns_per_row": median / MANIFEST["rows"], "process_medians": process_medians,
                                 "min": min(process_medians), "max": max(process_medians), "iqr": quartiles[2] - quartiles[0]}
            entry[f"{mode}-{arm}"] = phases
        native = entry["native-main"]["execution_ns"]["median"]
        noise = abs(native - entry["native-native-b"]["execution_ns"]["median"])
        entry["noise_ns"] = noise
        for mode in ["spark-off", "spark-on"]:
            gap = entry[f"{mode}-main"]["execution_ns"]["median"] - native
            entry[f"{mode}_gap_ns"] = gap
            entry[f"{mode}_flag"] = gap > noise
        summary[cell] = entry
    if not summary:
        raise RuntimeError("no retained samples")
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


def check():
    assert len(cells("nullif")) * 3 == 216
    assert len(cells("checked")) * 3 == 108
    assert len(set(cells("nullif"))) == 72
    assert MANIFEST["warmups"] == 1 and MANIFEST["samples"] == 3
    assert MANIFEST["workers"] == MANIFEST["target_partitions"] == 2
    assert sorted((p["arm"], p["replica"]) for p in MANIFEST["processes"]) == sorted((arm, replica) for arm in ["main", "native-b"] for replica in [1, 2, 3])
    for family in MANIFEST["families"].values():
        assert len({case["name"] for case in family}) == len(family)
    with tempfile.TemporaryDirectory() as temporary:
        output = pathlib.Path(temporary)
        records = []
        for mode, arm, ns in [("native", "main", 100), ("native", "native-b", 101), ("spark-off", "main", 102), ("spark-on", "main", 101)]:
            for replica in [1, 2, 3]:
                for sample in range(3):
                    records.append({"kind": "sample", "id": "synthetic-self-check", "configuration": mode, "arm": arm, "replica": replica, "sample": sample, "planning_ns": ns, "execution_ns": ns})
        with gzip.open(output / "synthetic.jsonl.gz", "wt") as stream:
            stream.writelines(json.dumps(record) + "\n" for record in records)
        summarize(output)
        result = json.loads((output / "summary.json").read_text())["synthetic-self-check"]
        assert result["noise_ns"] == 1
        assert result["spark-off_flag"] and not result["spark-on_flag"]
        assert result["native-main"]["execution_ns"]["process_medians"] == [100, 100, 100]
    print("Fixed manifest, inventory, schedule and strict-noise checks passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["check", "preflight", "measure", "attribute", "summarize"])
    parser.add_argument("arguments", nargs="*")
    args = parser.parse_args()
    if args.command == "check":
        check()
        return
    if args.command == "summarize":
        summarize(pathlib.Path(args.arguments[0]))
        return
    binary, output = pathlib.Path(args.arguments[0]).resolve(), pathlib.Path(args.arguments[1]).resolve()
    if not output.is_relative_to(EVIDENCE):
        raise RuntimeError("keep this protocol's evidence under its owned directory")
    metadata = binding(binary)
    output.mkdir(parents=True, exist_ok=False)
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    deadline = time.monotonic() + MANIFEST["campaign_timeout_seconds"]
    if args.command == "preflight":
        preflight(binary, output, args.arguments[2], pathlib.Path(args.arguments[3]) if len(args.arguments) > 3 else None, metadata, deadline)
    elif args.command == "measure":
        measure(binary, output, pathlib.Path(args.arguments[2]).resolve(), metadata, deadline)
    else:
        family = args.arguments[2]
        for mode in ["spark-off", "spark-on"]:
            _, records = call(binary, ["attribute", family, mode], output / mode, deadline)
            expected = {(cell, mode, sample) for cell in cells(family) for sample in range(3)}
            actual = {(r["id"], r["configuration"], r["sample"]) for r in records}
            if actual != expected or len(records) != len(expected):
                raise RuntimeError("incomplete stage replay")
    print(json.dumps({"output": str(output), "retained_evidence_bytes": check_space()}))


if __name__ == "__main__":
    main()
