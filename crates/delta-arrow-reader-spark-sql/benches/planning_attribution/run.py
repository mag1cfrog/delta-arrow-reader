#!/usr/bin/env python3
"""Reproduce #1390 from a committed source, with no SQL execution mode."""

import gzip
import hashlib
import json
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
import assemble
import summarize

HERE, WORKSPACE, REPO = assemble.HERE, assemble.WORKSPACE, assemble.REPO
CONFIGURATIONS = ("native-plain", "native-configured", "spark-off", "spark-on")


def read(path):
    return json.loads(pathlib.Path(path).read_text())


def write(path, value):
    pathlib.Path(path).write_text(json.dumps(value, indent=2) + "\n")


def fresh(path):
    path = pathlib.Path(path).resolve()
    path.mkdir(parents=True, exist_ok=False)
    return path


def compressed(path, rows):
    with pathlib.Path(path).open("xb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as stream:
            for row in rows:
                stream.write((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())


def tree_hash(root):
    records = []
    for path in sorted(pathlib.Path(root).rglob("*")):
        if path.is_file() and path.name != "planning-1390-assembly.json":
            records.append(str(path.relative_to(root)) + "\0" + assemble.digest(path))
    return hashlib.sha256("\n".join(records).encode()).hexdigest()


def protocol_commit():
    assert not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=all"], cwd=REPO, text=True).strip(), "use the committed clean protocol"
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()


def frozen_check():
    frozen = read(HERE / "frozen.json")
    assert frozen["baseline"] == assemble.BASELINE
    for name, expected in frozen["files"].items():
        assert assemble.digest(WORKSPACE / name) == expected, f"frozen source changed: {name}"
    queries = read(WORKSPACE / "benches/planning/queries.json")["queries"]
    assert len(queries) == len({query["id"] for query in queries}) == 309
    assert sum(query["group"] == "paired" for query in queries) == 25
    assert sum(query["group"] == "stress" for query in queries) == 5
    with gzip.open(WORKSPACE / "benches/reports/planning-1379-preflight.jsonl.gz", "rt") as source:
        preflight = [json.loads(line) for line in source]
    references = [row for row in preflight if row["kind"] == "equivalence"]
    assert len(references) == 120
    expected = {(query["id"], mode) for query in queries if query["group"] in ("paired", "stress") for mode in CONFIGURATIONS}
    assert {(row["id"], row["configuration"]) for row in references} == expected
    assert all(row["matches"] and not row["extra_delta_io"] for row in references)
    eligibility = [json.loads(line) for line in (WORKSPACE / "benches/planning/eligibility.jsonl").read_text().splitlines()]
    expected = {(query["id"], "spark-on" if ansi else "spark-off") for query in queries if query["group"] == "sweep" for ansi in query["ansi"]}
    assert len(eligibility) == len(expected) == 558
    assert {(row["id"], row["configuration"]) for row in eligibility} == expected
    assert all(row["status"] == "included" for row in eligibility)
    return queries


def check():
    frozen_check()
    changed, modules, anchors = assemble.patch_sources()
    assert len(changed) == 9 and len(modules) == 4 and anchors
    driver = (HERE / "driver.rs").read_text()
    for forbidden in (r"\bexecute_preflight_plan\s*\(", r"\bexecute_stream\s*\(",
                      r"\.execute\s*\(", r"\bpreflight\s*\(", r"physical_plan::collect", r"\bcollect\s*\("):
        assert not re.search(forbidden, driver), forbidden
    assert "super::*" in driver and "build_registered_sessions(" in driver and "scan_evidence(" in driver
    for observed in (False, True):
        generated = assemble.driver_source(observed)
        assert "@event_functions@" not in generated
    schedule = read(HERE / "schedule.json")
    assert schedule["cpu"] == 2 and schedule["warmups"] == 2 and schedule["samples"] == 9
    assert schedule["process_timeout_seconds"] == 300 and len(schedule["runs"]) == 70
    assert schedule["trace_processes"] == 8 and schedule["process_cap"] == 78
    assert len({run["process"] for run in schedule["runs"]}) == 70
    for configuration in CONFIGURATIONS:
        for mode in ("canonical", "observed"):
            assert sum(run["mode"] == mode and run["configuration"] == configuration and run["series"] == "main" for run in schedule["runs"]) == 5
        assert sum(run["mode"] == "observed" and run["configuration"] == configuration and run["arm"] == "b" for run in schedule["runs"]) == 5
    assert sum(run["mode"] == "canonical" and run["series"] == "noise" for run in schedule["runs"]) == 10
    summarize.self_check()
    print(f"#1390 static checks passed: {len(anchors)} exact-once anchors, 120 references, 558 sweep requests, 78 process cap")


def verify_build(path):
    receipt = read(path)
    assert receipt["baseline"] == assemble.BASELINE
    assert assemble.digest(pathlib.Path(receipt["binary"])) == receipt["binary_sha256"]
    root = pathlib.Path(receipt["source"])
    assert tree_hash(root) == receipt["tree_sha256"], "assembled source changed"
    assembly = read(root / "planning-1390-assembly.json")
    assert assembly == receipt["assembly"]
    assert assembly["protocol_commit"] == receipt["protocol_commit"]
    return receipt


def build(source, target, destination):
    check()
    source, target = pathlib.Path(source).resolve(), pathlib.Path(target).resolve()
    assert not target.is_relative_to(source), "use an external owned target directory"
    destination = fresh(destination)
    assembly = read(source / "planning-1390-assembly.json")
    assert assembly["protocol_commit"] == protocol_commit()
    workspace = source / WORKSPACE.relative_to(REPO)
    assert assembly["baseline"] == assemble.BASELINE
    for name, expected in assembly["assembly"].items():
        assert assemble.digest(workspace / name) == expected
    before = tree_hash(source)
    assert before == assembly["tree_sha256"], "assembled source changed before build"
    command = ["cargo", "+1.96.0", "build", "--locked", "--release", "--bench", "planning_attribution", "--message-format=json-render-diagnostics"]
    environment = os.environ.copy()
    for name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"):
        assert not environment.get(name), f"unexpected compiler override: {name}"
    environment.update(CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS="2", CARGO_PROFILE_RELEASE_DEBUG="1")
    with (destination / "build.jsonl").open("x") as output, (destination / "build.stderr").open("x") as errors:
        subprocess.run(command, cwd=workspace, env=environment, stdout=output, stderr=errors, check=True)
    assert tree_hash(source) == before, "Cargo changed assembled source or lockfile"
    artifacts = [json.loads(line) for line in (destination / "build.jsonl").read_text().splitlines() if line.startswith("{")]
    binaries = [row["executable"] for row in artifacts if row.get("executable") and row.get("target", {}).get("name") == "planning_attribution"]
    assert len(binaries) == 1
    compiler_binary = pathlib.Path(binaries[0]).resolve()
    binary = destination / "planning_attribution"
    shutil.copy2(compiler_binary, binary)
    receipt = {"baseline": assemble.BASELINE, "protocol_commit": assembly["protocol_commit"],
               "mode": "observed" if assembly["observed"] else "canonical", "source": str(source), "tree_sha256": before,
               "binary": str(binary), "binary_sha256": assemble.digest(binary), "assembly": assembly,
               "compiler_reported_binary": str(compiler_binary),
               "command": command, "build_environment": {key: environment[key] for key in ("CARGO_TARGET_DIR", "CARGO_BUILD_JOBS", "CARGO_PROFILE_RELEASE_DEBUG")},
               "rustc": subprocess.check_output(["rustc", "+1.96.0", "--version", "--verbose"], text=True),
               "build_log_sha256": assemble.digest(destination / "build.jsonl"), "build_stderr_sha256": assemble.digest(destination / "build.stderr")}
    write(destination / "receipt.json", receipt)
    print(destination / "receipt.json")


def invoke(binary, arguments, destination, process):
    command = ["taskset", "-c", "2", binary, *arguments]
    with (destination / (process + ".jsonl")).open("x") as output, (destination / (process + ".stderr")).open("x") as errors:
        subprocess.run(command, stdout=output, stderr=errors, timeout=300, check=True)
    return [json.loads(line) for line in (destination / (process + ".jsonl")).read_text().splitlines()]


def delta_hashes(root):
    root = pathlib.Path(root).resolve()
    return {str(path.relative_to(root)): assemble.digest(path) for path in sorted(root.rglob("*")) if path.is_file()}


def code_sources():
    sources = {}
    for directory in ("vendor/dependencies/datafusion-optimizer/src", "vendor/sail/crates/sail-plan/src"):
        for path in sorted((WORKSPACE / directory).rglob("*.rs")):
            text = path.read_text()
            if "AnalyzerRule for" not in text and "OptimizerRule for" not in text:
                continue
            for name in re.findall(r'fn name\(&self\) -> &[^{}]*str\s*\{\s*"([^"]+)"', text):
                sources[name] = str(path.relative_to(WORKSPACE))
    return sources


def trace(canonical, observed, delta_root, destination):
    check()
    receipts = [verify_build(canonical), verify_build(observed)]
    assert [receipt["mode"] for receipt in receipts] == ["canonical", "observed"]
    assert receipts[0]["protocol_commit"] == receipts[1]["protocol_commit"]
    assert receipts[0]["protocol_commit"] == protocol_commit()
    assert platform.system() == "Linux"
    destination = fresh(destination)
    delta = delta_hashes(delta_root)
    historical_delta = read(WORKSPACE / "benches/reports/planning-1379-metadata.json")["delta_files"]
    assert delta == historical_delta, "reuse the exact #1379 Delta fixture bytes"
    rows = []
    for receipt in receipts:
        for configuration in CONFIGURATIONS:
            process = receipt["mode"] + "-" + configuration
            reference = str(destination / ("canonical-" + configuration + ".jsonl")) if receipt["mode"] == "observed" else "-"
            print(process, flush=True)
            captured = invoke(receipt["binary"], ["trace", str(pathlib.Path(delta_root).resolve()), configuration, process, reference], destination, process)
            assert len(captured) == (309 if configuration.startswith("spark-") else 30)
            rows.extend(dict(row, mode=receipt["mode"]) for row in captured)
    assert delta_hashes(delta_root) == delta
    compressed(destination / "plans.jsonl.gz", rows)
    write(destination / "complete.json", {"processes": 8, "records": len(rows), "receipts": receipts, "delta_files": delta,
                                         "plans_sha256": assemble.digest(destination / "plans.jsonl.gz"),
                                         "trace_hashes": {path.name: assemble.digest(path) for path in sorted(destination.glob("*.jsonl"))}})


def measure(trace_directory, delta_root, destination):
    check()
    trace_directory = pathlib.Path(trace_directory).resolve()
    proof = read(trace_directory / "complete.json")
    assert proof["processes"] == 8 and proof["records"] == 1356
    for name, expected in proof["trace_hashes"].items():
        assert assemble.digest(trace_directory / name) == expected
    assert assemble.digest(trace_directory / "plans.jsonl.gz") == proof["plans_sha256"]
    receipts = {receipt["mode"]: receipt for receipt in proof["receipts"]}
    assert set(receipts) == {"canonical", "observed"}
    current_protocol = protocol_commit()
    for receipt in receipts.values():
        assert receipt["protocol_commit"] == current_protocol
        assert assemble.digest(pathlib.Path(receipt["binary"])) == receipt["binary_sha256"]
        assert tree_hash(receipt["source"]) == receipt["tree_sha256"]
    assert delta_hashes(delta_root) == proof["delta_files"]
    destination = fresh(destination)
    schedule = read(HERE / "schedule.json")
    write(destination / "metadata.json", {"schedule": schedule, "receipts": receipts, "trace_proof": proof,
          "delta_files": proof["delta_files"], "uname": platform.uname()._asdict(),
          "cpu": pathlib.Path("/proc/cpuinfo").read_text(),
          "governor": pathlib.Path("/sys/devices/system/cpu/cpu2/cpufreq/scaling_governor").read_text().strip(),
          "measurement_files": {path.name: assemble.digest(path) for path in sorted(HERE.iterdir()) if path.is_file()},
          "code_sources": code_sources()})
    rows = []
    for item in schedule["runs"]:
        print(item["process"], flush=True)
        captured = invoke(receipts[item["mode"]]["binary"], ["measure", str(pathlib.Path(delta_root).resolve()), item["configuration"], item["process"], "-"], destination, item["process"])
        assert len(captured) == len({row["id"] for row in captured}) == 30
        for row in captured:
            assert row["configuration"] == item["configuration"] and row["process"] == item["process"]
            assert all(len(row[key]) == 9 for key in ("logical_ns", "physical_ns", "total_ns", "events"))
            for index in range(9):
                assert row["total_ns"][index] == row["logical_ns"][index] + row["physical_ns"][index]
                summarize.parts(row, index, item["mode"])
            row.update(item)
            rows.append(row)
    assert delta_hashes(delta_root) == proof["delta_files"]
    compressed(destination / "samples.jsonl.gz", rows)
    write(destination / "complete.json", {"processes": 70, "records": len(rows), "samples_sha256": assemble.digest(destination / "samples.jsonl.gz")})


def main():
    args = sys.argv[1:]
    if args == ["--check"]:
        check()
    elif len(args) == 3 and args[0] == "assemble" and args[1] in ("canonical", "observed"):
        check()
        receipt = assemble.assemble(args[2], args[1] == "observed")
        destination = pathlib.Path(args[2]).resolve()
        receipt["tree_sha256"] = tree_hash(destination)
        write(destination / "planning-1390-assembly.json", receipt)
    elif len(args) == 4 and args[0] == "build":
        build(*args[1:])
    elif len(args) == 5 and args[0] == "trace":
        trace(*args[1:])
    elif len(args) == 4 and args[0] == "measure":
        measure(*args[1:])
    elif len(args) == 3 and args[0] == "summarize":
        summarize.run(*args[1:])
    else:
        raise SystemExit("usage: run.py --check | assemble canonical/observed NEW_REPO | build ASSEMBLED_REPO NEW_TARGET NEW_BUILD_DIR | trace CANONICAL_RECEIPT OBSERVED_RECEIPT DELTA_ROOT NEW_DIR | measure TRACE_DIR DELTA_ROOT NEW_DIR | summarize RUN_DIR NEW_DIR")


if __name__ == "__main__":
    main()
