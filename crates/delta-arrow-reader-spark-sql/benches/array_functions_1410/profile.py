#!/usr/bin/env python3
"""Capture the bounded #1410 array-function plans and CPU profiles."""

import argparse
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
MANIFEST = HERE / "manifest.json"
sys.path.insert(0, str(HERE.parent / "window_aggregates_1408"))
from run import FROZEN, WORKSPACE, observe, runner, schedule, write


def load_manifest():
    settings = runner.read(MANIFEST)
    frozen = FROZEN / "manifest.json"
    assert runner.digest(frozen) == settings["frozen_manifest_sha256"]
    originals = {case["id"]: case for case in runner.read(frozen)["functions"]}
    cases = settings["functions"]
    assert len(cases) == len({case["id"] for case in cases}) == 9
    assert all(case == originals[case["id"]] and case["native_sql"] for case in cases)
    return settings


def verify_build(build, settings, protocol_commit):
    binary = build / "functions"
    receipt = runner.read(build / "receipt.json")
    assert receipt["source"] == settings["executable_source"] and receipt["variant"] == "plain"
    assert receipt["sha256"] == runner.digest(binary)
    assert receipt["patch_sha256"] == runner.digest(build / "patch.json")
    assert set(runner.read(build / "patch.json")) == {"benches/functions.rs", "tests/support/spark_results.rs"}
    assert receipt["profile"]["opt_level"] == "3" and receipt["profile"]["debuginfo"] == 1
    for earlier, later in ((receipt["source"], settings["staging"]), (settings["staging"], protocol_commit)):
        subprocess.run(["git", "merge-base", "--is-ancestor", earlier, later], cwd=HERE, check=True)
        changed = runner.git("diff", "--name-only", earlier, later).splitlines()
        assert all(path.startswith(f"{WORKSPACE}/benches/") for path in changed)
    return binary, receipt


def remaining(deadline, bound):
    timeout = min(deadline - time.monotonic(), bound)
    if timeout <= 0:
        raise TimeoutError("array attribution schedule deadline exhausted")
    return timeout


def profile_case(binary, case_id, expected, work, limits, profile, deadline):
    work.mkdir(parents=True)
    ids = work / "ids.json"
    write(ids, [case_id] * profile["repeats"])
    capture = work / "perf.data"
    command = [
        "perf", "record", "-e", "cpu-clock:u", "--user-callchains",
        "--call-graph", f"dwarf,{profile['stack_bytes']}", "-F", str(profile["frequency_hz"]),
        "--max-size", f"{profile['max_mib']}M", "-o", str(capture), "--",
        "taskset", "-c", ",".join(map(str, limits["cpus"])),
        str(binary), "measure-set", str(MANIFEST), str(ids), "spark-off",
    ]
    write(work / "command.json", command)
    stdout = work / "stdout.jsonl"
    with stdout.open("x") as output, (work / "stderr.log").open("x") as errors:
        process = subprocess.Popen(
            command, stdout=output, stderr=errors, start_new_session=True,
            preexec_fn=runner.limit(limits["address_space_bytes"]),
        )
        try:
            if process.wait(timeout=remaining(deadline, profile["timeout_seconds"])):
                raise RuntimeError(f"{case_id} profiler exited {process.returncode}")
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
    records = runner.parse_lines(stdout.read_text())
    runs = [row for row in records if row.get("status") == "ok"]
    assert len(runs) == profile["repeats"]
    for entry in runs:
        assert entry["id"] == case_id and entry["configuration"] == "spark-off"
        assert entry["types"] == expected["types"]
        assert len(entry["samples"]) == limits["samples_per_case"]
        assert all(
            sample["input_rows"] == limits["input_rows"] and sample["output_rows"] == expected["output_rows"]
            for sample in entry["samples"]
        )
    assert capture.stat().st_size <= profile["max_mib"] * 1024**2
    for filename, args in (
        ("stacks.txt", ["script"]),
        ("report.txt", ["report", "--stdio", "--no-children", "--sort=symbol,srcline"]),
    ):
        with (work / filename).open("x") as output, (work / (filename + ".stderr")).open("x") as errors:
            subprocess.run(
                ["perf", *args, "-i", str(capture)], stdout=output, stderr=errors,
                check=True, timeout=remaining(deadline, 120),
            )


def capture_profiles(build, output, settings):
    assert platform.system() == "Linux"
    assert not runner.git("status", "--porcelain"), "commit the protocol before sampling"
    commit = runner.git("rev-parse", "HEAD")
    binary, receipt = verify_build(build, settings, commit)
    limits = schedule()
    deadline = time.monotonic() + settings["overall_timeout_seconds"]
    output.mkdir()
    eligible, excluded = {}, {}
    for case in settings["functions"]:
        case_id = case["id"]
        print("preflight", case_id, flush=True)
        try:
            result = observe(binary, case, output / "preflight" / case_id, limits, deadline, MANIFEST)
            eligible[case_id] = {key: value for key, value in result.items() if key != "plans"}
        except ValueError as error:
            excluded[case_id] = str(error)
    write(output / "preflight.json", {
        "protocol_commit": commit, "manifest_sha256": runner.digest(MANIFEST), "build": receipt,
        "schedule": limits, "profile": settings["profile"], "eligible": eligible, "excluded": excluded,
    })
    for case_id, expected in eligible.items():
        print("profile", case_id, flush=True)
        profile_case(binary, case_id, expected, output / "profiles" / case_id, limits, settings["profile"], deadline)
    write(output / "complete.json", {"profiled": list(eligible), "excluded": excluded})


if __name__ == "__main__":
    if not __debug__:
        raise SystemExit("Run with Python validation enabled.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("build", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    capture_profiles(args.build.resolve(), args.output.resolve(), load_manifest())
