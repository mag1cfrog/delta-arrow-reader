#!/usr/bin/env python3
"""Replay the frozen planning schedule on one optimized production build."""

import csv
import gzip
import json
import platform
from pathlib import Path
import statistics
import subprocess
import sys

sys.dont_write_bytecode = True
import assemble
import run
import summarize

WORKSPACE = assemble.WORKSPACE
REPORTS = WORKSPACE / "benches/reports"


def read_gzip(path):
    with gzip.open(path, "rt") as source:
        return json.load(source)


def setup(manifest_path):
    manifest = run.read(manifest_path)
    commit = run.protocol_commit()
    assert manifest_path.resolve().is_relative_to(assemble.HERE)
    assert subprocess.check_output(
        ["git", "show", f"{commit}:{manifest_path.resolve().relative_to(assemble.REPO)}"],
        cwd=assemble.REPO,
    ) == manifest_path.read_bytes()
    subprocess.run(["git", "merge-base", "--is-ancestor", manifest["base"], commit],
                   cwd=assemble.REPO, check=True)
    frozen = run.read(assemble.HERE / "frozen.json")
    changed_sources = set(manifest["changed_sources"])
    for name, digest in frozen["files"].items():
        if name not in changed_sources:
            assert assemble.digest(WORKSPACE / name) == digest, name
    for name in changed_sources:
        assert (WORKSPACE / name).is_file(), name

    delta_hashes = run.read(REPORTS / "planning-1379-metadata.json")["delta_files"]
    all_runs = run.read(assemble.HERE / "schedule.json")["runs"]
    schedule = {mode: [item for item in all_runs if item["mode"] == mode]
                for mode in ("canonical", "observed")}
    canonical = schedule["canonical"]
    assert len(canonical) == 30 and len({item["process"] for item in canonical}) == 30
    assert len(schedule["observed"]) == 40
    for config in run.CONFIGURATIONS:
        assert sum(item["configuration"] == config and item["series"] == "main"
                   for item in canonical) == 5
    assert sum(item["series"] == "noise" and item["configuration"] == "native-configured"
               for item in canonical) == 10
    manifest_digest = assemble.digest(manifest_path)
    return manifest, commit, manifest_digest, delta_hashes, schedule


def build(manifest, commit, source, target, build_directory, *, observed=False):
    receipt = assemble.assemble(source, observed=observed)
    assert receipt["protocol_commit"] == commit
    receipt["tree_sha256"] = run.tree_hash(source)
    run.write(source / "planning-1390-assembly.json", receipt)
    # A prior run in the shared cache may have used the attribution overlay.
    original_overlay = read_gzip(REPORTS / "planning-1390-observed-receipt.json.gz")
    observed_sources = [name for name in original_overlay["assembly"]["assembly"]
                        if not name.startswith("benches/") and (WORKSPACE / name).is_file()]
    refresh_paths = sorted(set(observed_sources) | set(manifest["changed_sources"]))
    run.build(source, target, build_directory, refresh_paths=refresh_paths)


def trace(commit, manifest_digest, expected_delta, receipt_path, delta, output):
    receipt = run.verify_build(receipt_path)
    assert receipt["mode"] in ("canonical", "observed") and receipt["protocol_commit"] == commit
    assert run.delta_hashes(delta) == expected_delta
    output = run.fresh(output)
    baseline = REPORTS / "planning-1390-plans.jsonl.gz"
    references = {mode: [] for mode in run.CONFIGURATIONS}
    with gzip.open(baseline, "rt") as source:
        for line in source:
            row = json.loads(line)
            if row.pop("mode") == "canonical":
                references[row["configuration"]].append(row)
    records = []
    for configuration in run.CONFIGURATIONS:
        reference = output / (configuration + "-reference.jsonl")
        expected = references[configuration]
        assert len(expected) == (309 if configuration.startswith("spark-") else 30)
        reference.write_text("".join(json.dumps(row) + "\n" for row in expected))
        arguments = ["trace", str(delta), configuration, configuration, str(reference)]
        captured = run.invoke(receipt["binary"], arguments, output, configuration)
        assert len(captured) == len(expected)
        assert [row["id"] for row in captured] == [row["id"] for row in expected]
        assert not (output / (configuration + ".stderr")).stat().st_size
        records.extend(captured)
    assert run.delta_hashes(delta) == expected_delta
    run.compressed(output / "plans.jsonl.gz", records)
    run.write(output / "complete.json", {
        "records": len(records), "processes": 4, "delta_files": expected_delta,
        "manifest_sha256": manifest_digest, "receipt": receipt,
        "reference_sha256": assemble.digest(baseline),
        "plan_sha256": assemble.digest(output / "plans.jsonl.gz"),
    })


def measure(commit, manifest_digest, expected_delta, schedules, traces, delta, output):
    assert platform.system() == "Linux"
    proof = run.read(traces / "complete.json")
    assert proof["records"] == 678 and proof["processes"] == 4
    assert proof["manifest_sha256"] == manifest_digest and proof["delta_files"] == expected_delta
    assert assemble.digest(traces / "plans.jsonl.gz") == proof["plan_sha256"]
    assert assemble.digest(REPORTS / "planning-1390-plans.jsonl.gz") == proof["reference_sha256"]
    receipt = proof["receipt"]
    assert receipt["mode"] in ("canonical", "observed") and receipt["protocol_commit"] == commit
    assert run.tree_hash(receipt["source"]) == receipt["tree_sha256"]
    assert assemble.digest(Path(receipt["binary"])) == receipt["binary_sha256"]
    assert run.delta_hashes(delta) == expected_delta
    schedule = schedules[receipt["mode"]]
    output = run.fresh(output)
    run.write(output / "metadata.json", {
        "trace_proof": proof, "schedule": schedule, "host": platform.uname()._asdict(),
        "cpu": Path("/proc/cpuinfo").read_text(),
        "governor": Path("/sys/devices/system/cpu/cpu2/cpufreq/scaling_governor").read_text().strip(),
    })
    queries = run.read(WORKSPACE / "benches/planning/queries.json")["queries"]
    expected_ids = {query["id"] for query in queries if query["group"] in ("paired", "stress")}
    assert len(expected_ids) == 30
    rows = []
    for item in schedule:
        process, configuration = item["process"], item["configuration"]
        print(process, flush=True)
        captured = run.invoke(receipt["binary"], ["measure", str(delta), configuration, process, "-"],
                              output, process)
        assert not (output / (process + ".stderr")).stat().st_size
        assert len(captured) == 30 and {row["id"] for row in captured} == expected_ids
        for row in captured:
            assert row["configuration"] == configuration and row["process"] == process
            assert all(len(row[phase + "_ns"]) == 9 for phase in summarize.PHASES)
            if receipt["mode"] == "canonical":
                assert row["events"] == [[]] * 9
            else:
                assert len(row["events"]) == 9 and all(row["events"])
            assert row["total_ns"] == [left + right for left, right in zip(row["logical_ns"], row["physical_ns"])]
            rows.append(row | item)
    assert run.delta_hashes(delta) == expected_delta
    run.compressed(output / "samples.jsonl.gz", rows)
    run.write(output / "complete.json", {"records": len(rows), "processes": len(schedule),
              "samples_sha256": assemble.digest(output / "samples.jsonl.gz")})


def check_timing_source(timings, commit, manifest_digest, expected_delta, schedule):
    metadata = run.read(timings / "metadata.json")
    assert metadata["schedule"] == schedule
    proof = metadata["trace_proof"]
    assert proof["records"] == 678 and proof["processes"] == 4
    assert proof["manifest_sha256"] == manifest_digest and proof["delta_files"] == expected_delta
    assert assemble.digest(REPORTS / "planning-1390-plans.jsonl.gz") == proof["reference_sha256"]
    receipt = proof["receipt"]
    assert receipt["protocol_commit"] == commit and receipt["mode"] == schedule[0]["mode"]
    assert run.tree_hash(receipt["source"]) == receipt["tree_sha256"]
    assert assemble.digest(Path(receipt["binary"])) == receipt["binary_sha256"]


def summarize_run(before_path, prior_noise_path, timings, table):
    proof = run.read(timings / "complete.json")
    assert proof["records"] == 900 and proof["processes"] == 30
    samples = timings / "samples.jsonl.gz"
    assert assemble.digest(samples) == proof["samples_sha256"]
    with gzip.open(samples, "rt") as source:
        rows = [json.loads(line) for line in source]
    assert len(rows) == proof["records"]
    data = {}
    for row in rows:
        for phase in summarize.PHASES:
            key = (row["configuration"], row["id"], phase, row["series"], row["arm"])
            data.setdefault(key, []).append(statistics.median(row[phase + "_ns"]) / 1e6)

    prior_data = {}
    with before_path.open() as source:
        for row in csv.DictReader(source):
            prior_data[(row["configuration"], row["id"], row["phase"])] = row
    assert len(prior_data) == 360
    with prior_noise_path.open() as source:
        prior_noise = {(row["id"], row["stage"]): float(row["absolute_difference_ms"])
                       for row in csv.DictReader(source) if row["mode"] == "canonical"}
    assert len(prior_noise) == 90
    output = []
    for key, observations in sorted(data.items(), key=lambda item: str(item[0])):
        configuration, query, phase, series, arm = key
        if series != "main":
            continue
        assert arm is None and len(observations) == 5
        median = statistics.median(observations)
        low, _, high = statistics.quantiles(observations, n=4, method="inclusive")
        noise_a = data[("native-configured", query, phase, "noise", "a")]
        noise_b = data[("native-configured", query, phase, "noise", "b")]
        assert len(noise_a) == len(noise_b) == 5
        baseline = prior_data[(configuration, query, phase)]
        before = float(baseline["canonical_ms"])
        prior_a_b = baseline.get("native_ab_difference_ms", prior_noise[(query, phase)])
        native = statistics.median(data[("native-configured", query, phase, "main", None)])
        output.append(dict(configuration=configuration, id=query, phase=phase,
                           before_ms=before, canonical_ms=median, change_ms=median-before,
                           native_configured_ms=native, difference_ms=median-native,
                           before_native_ab_difference_ms=float(prior_a_b),
                           native_ab_difference_ms=abs(statistics.median(noise_a)-statistics.median(noise_b)),
                           min_ms=min(observations), max_ms=max(observations), iqr_ms=high-low))
    assert len(output) == 360
    summarize.write_csv(table, output)


def summarize_projection_rule(timings, destination):
    proof = run.read(timings / "complete.json")
    assert proof["records"] == 1200 and proof["processes"] == 40
    samples = timings / "samples.jsonl.gz"
    assert assemble.digest(samples) == proof["samples_sha256"]
    rule = "physical.optimizer.rule/optimize_projections/body"
    before = {}
    noise = {}
    with (REPORTS / "planning-1390-stages.csv").open() as source:
        for row in csv.DictReader(source):
            if row["mode"] == "observed" and row["stage"] == rule:
                before[(row["configuration"], row["id"])] = row
    with (REPORTS / "planning-1390-noise.csv").open() as source:
        for row in csv.DictReader(source):
            if row["mode"] == "observed" and row["stage"] == rule:
                noise[(row["configuration"], row["id"])] = row["absolute_difference_ms"]
    assert len(before) == len(noise) == 120
    observations, extracted = {}, []
    with gzip.open(samples, "rt") as source:
        for line in source:
            row = json.loads(line)
            keys = ("configuration", "id", "process", "arm")
            identity = {key: row[key] for key in keys}
            pair = (row["configuration"], row["id"])
            entries = [[(name, value) for name, value in sample
                        if name.startswith("physical.optimizer.rule/")
                        and name.endswith("/optimize_projections/body")]
                       for sample in row["events"]]
            passes = before[pair]["optimizer_pass_indices"].split(";")
            assert len(entries) == 9
            assert all([name.split("/")[1] for name, _ in sample] == passes for sample in entries)
            values = [sum(value for _, value in sample) for sample in entries]
            observations.setdefault((*pair, row["arm"]), []).append(statistics.median(values))
            extracted.append(dict(identity, values_ns=values, optimizer_pass_indices=passes))
    assert len(extracted) == 1200
    result = []
    for pair, baseline in sorted(before.items()):
        a, b = (observations[(*pair, arm)] for arm in ("a", "b"))
        assert len(a) == len(b) == 5
        measured = statistics.median(a)
        q1, _, q3 = statistics.quantiles(a, n=4, method="inclusive")
        result.append(dict(configuration=pair[0], id=pair[1],
                           before_ms=baseline["median_ms"], after_ms=measured / 1e6,
                           before_ab_difference_ms=noise[pair],
                           after_ab_difference_ms=abs(measured-statistics.median(b)) / 1e6,
                           min_ms=min(a) / 1e6, max_ms=max(a) / 1e6, iqr_ms=(q3-q1) / 1e6,
                           optimizer_pass_indices=baseline["optimizer_pass_indices"]))
    destination = run.fresh(destination)
    summarize.write_csv(destination / "projection-rule.csv", result)
    run.compressed(destination / "projection-rule-samples.jsonl.gz", extracted)


def main():
    command, manifest_path, *args = sys.argv[1:]
    manifest, commit, digest, expected_delta, schedule = setup(Path(manifest_path))
    args = [Path(arg).resolve() for arg in args]
    if command in ("build", "build-observed") and len(args) == 3:
        build(manifest, commit, *args, observed=command == "build-observed")
    elif command == "trace" and len(args) == 3:
        trace(commit, digest, expected_delta, *args)
    elif command == "measure" and len(args) == 3:
        measure(commit, digest, expected_delta, schedule, *args)
    elif command == "summarize" and len(args) == 2:
        check_timing_source(args[0], commit, digest, expected_delta, schedule["canonical"])
        summarize_run(REPORTS / manifest["baseline_walls"], REPORTS / manifest["baseline_noise"], *args)
    elif command == "summarize-projection-rule" and len(args) == 2:
        check_timing_source(args[0], commit, digest, expected_delta, schedule["observed"])
        summarize_projection_rule(*args)
    else:
        raise SystemExit("usage: replay.py build|build-observed|trace|measure|summarize|summarize-projection-rule MANIFEST ARGUMENTS")


if __name__ == "__main__":
    main()
