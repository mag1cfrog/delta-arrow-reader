#!/usr/bin/env python3
"""Time the bounded #1409 attribution schedule on one release binary."""

import argparse
from pathlib import Path
import platform
import subprocess
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
MANIFEST = HERE / "manifest.json"
sys.path.insert(0, str(HERE.parent / "window_aggregates_1408"))
from compare import summarize
from run import FROZEN, WORKSPACE, observe, one_process, process_medians, runner, schedule, write


def settings():
    manifest = runner.read(MANIFEST)
    frozen = FROZEN / "manifest.json"
    assert runner.digest(frozen) == manifest["frozen_manifest_sha256"]
    originals = {case["id"]: case for case in runner.read(frozen)["functions"]}
    functions = manifest["functions"]
    assert len(functions) == len({case["id"] for case in functions}) == 16
    assert sum(case["native_sql"] is not None for case in functions) == 10
    zones = {"los_angeles": "America/Los_Angeles", "offset": "+05:45"}
    extra = set()
    for case in functions:
        case_id = case["id"]
        if case_id in originals:
            assert case == originals[case_id]
            continue
        base_id, variant = case_id.rsplit(".", 1)
        original = originals[base_id]
        start, utc, end = original["spark_sql"].rpartition("'UTC'")
        assert utc and case["spark_sql"] == start + f"'{zones[variant]}'" + end
        assert case["native_sql"] is None and case["class"] == "Spark only"
        altered = {"id", "spark_sql", "native_sql", "class", "class_evidence"}
        assert all(case[key] == value for key, value in original.items() if key not in altered)
        extra.add((original["name"], variant))
    assert extra == {(name, variant) for name in
                     ("convert_timezone", "from_utc_timestamp", "to_utc_timestamp") for variant in zones}
    return manifest


def measure(build, output, protocol):
    assert platform.system() == "Linux"
    assert not runner.git("status", "--porcelain"), "commit protocol and manifest before clocks"
    commit = runner.git("rev-parse", "HEAD")
    binary = build / "functions"
    receipt = runner.read(build / "receipt.json")
    source = protocol["executable_source"]
    staging = protocol["staging"]
    assert receipt["source"] == source and receipt["variant"] == "plain"
    assert receipt["sha256"] == runner.digest(binary)
    assert receipt["patch_sha256"] == runner.digest(build / "patch.json")
    assert set(runner.read(build / "patch.json")) == {"benches/functions.rs", "tests/support/spark_results.rs"}
    assert receipt["profile"]["opt_level"] == "3" and receipt["profile"]["debuginfo"] == 1
    subprocess.run(["git", "merge-base", "--is-ancestor", staging, commit], cwd=HERE, check=True)
    changed = runner.git("diff", "--name-only", source, commit).splitlines()
    assert all(path.startswith(str(WORKSPACE / "benches") + "/") for path in changed)

    limits = schedule()
    deadline = time.monotonic() + protocol["overall_timeout_seconds"]
    output.mkdir()
    cases, excluded = {}, {}
    for case in protocol["functions"]:
        case_id = case["id"]
        print("preflight", case_id, flush=True)
        try:
            result = observe(binary, case, output / "preflight" / case_id, limits, deadline, MANIFEST)
            cases[case_id] = {key: value for key, value in result.items() if key != "plans"}
        except ValueError as error:
            excluded[case_id] = str(error)
    write(output / "session.json", {
        "protocol_commit": commit, "manifest_sha256": runner.digest(MANIFEST),
        "build": receipt, "schedule": limits, "eligible": cases, "excluded": excluded,
    })

    ids = sorted(cases)
    timings = {case_id: {mode: [] for mode in config["modes"]} for case_id, config in cases.items()}
    controls = {case_id: {arm: [] for arm in ("a", "b")} for case_id in ids if "native" in timings[case_id]}
    failed = set()
    for replica in range(limits["replicas"]):
        offset = replica * len(ids) // limits["replicas"]
        order = -1 if replica == 1 else 1
        for case_id in ids[offset:] + ids[:offset]:
            if case_id in failed:
                continue
            configs = tuple(timings[case_id])[::order]
            jobs = [(mode, mode, timings[case_id]) for mode in configs]
            if case_id in controls:
                jobs.extend((arm, "native", controls[case_id]) for arm in limits["noise_orders"][replica])
            for label, mode, samples in jobs:
                work = output / "processes" / str(replica) / case_id / label
                print(work, flush=True)
                result = one_process(binary, case_id, mode, work, cases[case_id], limits, deadline, MANIFEST)
                samples[label].append(result)
                if result["status"] != "ok":
                    failed.add(case_id)
                    break

    summary = {}
    for case_id, results in timings.items():
        result = {mode: summarize(arm) for mode, arm in results.items()}
        if case_id in controls and case_id not in failed:
            a, b = (process_medians(controls[case_id][label]) for label in ("a", "b"))
            assert len(a) == len(b) == limits["replicas"]
            result["native_noise_ns"] = runner.noise(a, b)
        summary[case_id] = result
    write(output / "summary.json", summary)


if __name__ == "__main__":
    if not __debug__:
        raise SystemExit("Run with Python validation enabled.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    protocol = settings()
    if bool(args.build) != bool(args.output):
        parser.error("--build and --output must be provided together")
    if args.build:
        measure(args.build, args.output, protocol)
    else:
        print(f"Checked {len(protocol['functions'])} time conversion statements")
