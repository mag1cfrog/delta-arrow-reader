#!/usr/bin/env python3
"""Run the bounded #1408 baseline and scratch phase profiles."""

import argparse
import filecmp
import importlib.util
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from instrument import PHASES, changes

HERE = Path(__file__).resolve().parent
MANIFEST = HERE / "manifest.json"
FROZEN = HERE.parent / "functions"
WORKSPACE = HERE.relative_to(HERE.parents[3]).parents[1]
MODES = ("spark-off", "spark-on")

sys.path.insert(0, str(FROZEN))
spec = importlib.util.spec_from_file_location("frozen_functions", FROZEN / "run.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def write(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def protocol():
    settings = runner.read(MANIFEST)
    source = FROZEN / "manifest.json"
    assert runner.digest(source) == settings["frozen_manifest_sha256"]
    original = {case["id"]: case for case in runner.read(source)["functions"]}
    selected = settings["functions"]
    assert len({case["id"] for case in selected}) == 18
    assert sum(case["native_sql"] is not None for case in selected) == 8
    for case in selected:
        assert all(value == original[case["id"]][key] for key, value in case.items())
    return settings


def schedule():
    frozen = runner.read(FROZEN / "schedule.json")
    keys = ("cpus", "input_rows", "batch_rows", "target_partitions", "runtime_workers",
            "pool_bytes", "address_space_bytes", "case_timeout_seconds",
            "preflight_process_timeout_seconds", "warmups_per_case", "samples_per_case",
            "replicas", "noise_orders")
    result = {key: frozen[key] for key in keys}
    result["process_timeout_seconds"] = 130
    return result


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()


def build(repository, target, output, variant):
    source = protocol()["source"]
    assert git(repository, "rev-parse", "HEAD") == source and not git(repository, "status", "--porcelain")
    workspace = repository / WORKSPACE
    files = dict(changes(workspace, variant == "phases"))
    original = {path: path.read_bytes() for path in files}
    output.mkdir()
    command = ["cargo", "+1.96.0", "build", "--locked", "--release", "--bench",
               "functions", "--message-format=json-render-diagnostics", "-j", "3"]
    env = {"CARGO_TARGET_DIR": str(target), "CARGO_PROFILE_RELEASE_DEBUG": "1"}
    log = output / "build.jsonl"
    try:
        for path, data in files.items():
            path.write_text(data)
        with log.open("x") as stdout, (output / "build.stderr").open("x") as stderr:
            subprocess.run(command, cwd=workspace, env=os.environ | env, stdout=stdout,
                           stderr=stderr, check=True, timeout=3600)
    finally:
        for path, data in original.items():
            path.write_bytes(data)

    artifacts = runner.parse_lines(log.read_text())
    assert artifacts[-1] == {"reason": "build-finished", "success": True}
    artifact, = [row for row in artifacts if row.get("executable")
                 and row.get("target", {}).get("name") == "functions"]
    profile = artifact["profile"]
    assert profile["opt_level"] == "3" and profile["debuginfo"] == 1
    binary = output / "functions"
    subprocess.run(["cp", "--reflink=auto", artifact["executable"], binary], check=True)
    assert runner.digest(binary) == runner.digest(Path(artifact["executable"]))
    patch = output / "patch.json"
    write(patch, {str(path.relative_to(workspace)): data for path, data in files.items()})
    write(output / "receipt.json", {
        "source": source, "variant": variant, "sha256": runner.digest(binary),
        "patch_sha256": runner.digest(patch), "command": command, "environment": env,
        "profile": profile,
        "rustc": subprocess.check_output(["rustc", "+1.96.0", "--version", "--verbose"], text=True),
    })


def observe(binary, case, work, settings, deadline):
    modes = (("native",) if case["native_sql"] else ()) + MODES
    result = runner.invoke(binary, ["preflight", MANIFEST, case["id"], work / "plans"],
                           work, settings, deadline, preflight=True)
    if result["status"] != "ok":
        raise ValueError(f"preflight {result['status']}")
    observed = result["records"][-1]
    rows = settings["input_rows"]
    output_rows = 1 if case["id"].startswith("aggregate.") else rows
    outcomes = {row["configuration"]: row for row in observed["results"]}
    if (
        tuple(outcomes) != modes
        or any(r["status"] != "ok" or r["rows"] != output_rows or r["source_guard_rows"] != rows
               for r in outcomes.values())
        or any(r.get("comparison") and not r["comparison"]["equal"] for r in outcomes.values())
        or len(observed["baselines"]) != 3 or any(x["rows"] != rows for x in observed["baselines"])
    ):
        raise ValueError("native comparison, executed type or input/output row guard failed")
    if any(outcomes[m]["types"] != outcomes[modes[0]]["types"] for m in modes):
        raise ValueError("executed types differ across configurations")
    plans = work / "plans"
    for mode in modes:
        if not filecmp.cmp(plans / f"{modes[0]}.rows.json", plans / f"{mode}.rows.json", shallow=False):
            raise ValueError(f"{mode}: result rows differ from reference")
    return {"output_rows": output_rows, "types": outcomes[modes[0]]["types"],
            "modes": modes, "plans": plans}


def preflight(binaries, cases, output, settings, deadline):
    eligible, excluded = {}, {}
    for case in cases:
        id = case["id"]
        try:
            base = observe(binaries["plain"], case, output / id / "plain", settings, deadline)
            phase = observe(binaries["phases"], case, output / id / "phases", settings, deadline)
            for mode in base["modes"]:
                for suffix in ("plan", "rows.json"):
                    name = f"{mode}.{suffix}"
                    if not filecmp.cmp(base["plans"] / name, phase["plans"] / name, shallow=False):
                        raise ValueError(f"{mode}: different {suffix} across builds")
            if phase["types"] != base["types"]:
                raise ValueError("executed types differ across builds")
            eligible[id] = {key: value for key, value in base.items() if key != "plans"}
        except ValueError as error:
            excluded[id] = str(error)
    return eligible, excluded


def one_process(binary, id, mode, work, expected, settings, deadline):
    ids = work.with_suffix(".ids.json")
    ids.parent.mkdir(parents=True, exist_ok=True)
    write(ids, [id])
    execution = runner.invoke(binary, ["measure-set", MANIFEST, ids, mode], work, settings, deadline)
    result, = runner.assemble_cases(execution, [id], mode)
    if execution["status"] != "ok":
        result["status"] = execution["status"]
    if result["status"] == "ok":
        assert result["types"] == expected["types"]
        assert all(s["input_rows"] == settings["input_rows"] and s["output_rows"] == expected["output_rows"]
                   for s in result["samples"])
    write(work / "verified.json", result)
    return result


def classify_phases(path, samples, rows):
    marker = "aggregate-phases "
    recorded = [json.loads(line[len(marker):]) for line in path.read_text().splitlines()
                if line.startswith(marker)]
    assert len(recorded) == len(samples) + 1
    retained = recorded[1:]
    assert all(len(record) == len(PHASES) for record in retained)
    indexed = [dict(zip(PHASES, record)) for record in retained]
    assert all(record["create"][1] and record["evaluate"][1] and record["materialize"][1]
               for record in indexed), (path, "missing expected implementation hook")
    assert all(len(values) == 3 and min(values) >= 0
               for record in indexed for values in record.values())
    return {
        "units": "time is ns per input row; calls and rows are per execution",
        "phases": {phase: {
            "ns_per_input_row": statistics.median(record[phase][0] / rows for record in indexed),
            "calls": [record[phase][1] for record in indexed],
            "rows": [record[phase][2] for record in indexed],
        } for phase in PHASES},
        "execution_ns_per_input_row": [sample["execution_ns"] / rows for sample in samples],
        "remainder_ns_per_input_row": [
            (sample["execution_ns"] - sum(record[phase][0] for phase in PHASES)) / rows
            for sample, record in zip(samples, indexed)
        ],
    }


def increments(observations):
    return [s["increment_ns"] / s["input_rows"] for run in observations for s in run["samples"]]


def measure(directories, output):
    assert not runner.git("status", "--porcelain", "--", str(HERE))
    frozen = protocol()
    settings = schedule()
    deadline = time.monotonic() + frozen["overall_timeout_seconds"]
    output.mkdir()
    binaries, receipts = {}, {}
    for variant, directory in directories.items():
        receipt = runner.read(directory / "receipt.json")
        assert receipt["source"] == frozen["source"] and receipt["variant"] == variant
        assert runner.digest(directory / "patch.json") == receipt["patch_sha256"]
        binaries[variant] = directory / "functions"
        assert runner.digest(binaries[variant]) == receipt["sha256"]
        receipts[variant] = receipt
    eligible, excluded = preflight(binaries, frozen["functions"], output / "preflight", settings, deadline)
    write(output / "session.json", {
        "protocol_commit": runner.git("rev-parse", "HEAD"), "source": frozen["source"],
        "manifest_sha256": runner.digest(MANIFEST), "schedule": settings, "preflight": eligible,
        "excluded": excluded, "builds": receipts, "system": platform.uname()._asdict(),
    })
    ids = sorted(eligible)
    timings = {id: {mode: [] for mode in eligible[id]["modes"]} for id in ids}
    controls = {id: {arm: [] for arm in ("a", "b")} for id in ids if "native" in timings[id]}
    profiles = {id: {} for id in ids}
    rows = settings["input_rows"]
    for replica in range(settings["replicas"]):
        offset = replica * 6 % max(1, len(ids))
        for id in ids[offset:] + ids[:offset]:
            case = eligible[id]
            for mode in case["modes"][:: -1 if replica % 2 else 1]:
                previous = timings[id][mode]
                if previous and previous[-1]["status"] != "ok":
                    continue
                work = output / "ordinary" / str(replica) / id / mode
                print(work.relative_to(output), flush=True)
                previous.append(one_process(binaries["plain"], id, mode, work, case, settings, deadline))
            if id in controls:
                for arm in settings["noise_orders"][replica]:
                    previous = controls[id][arm]
                    if previous and previous[-1]["status"] != "ok":
                        continue
                    work = output / "noise" / str(replica) / id / arm
                    print(work.relative_to(output), flush=True)
                    previous.append(one_process(binaries["plain"], id, "native", work, case, settings, deadline))
    for id in ids:
        case = eligible[id]
        modes = case["modes"] if "native" in case["modes"] else ("spark-off",)
        for mode in modes:
            work = output / "profile" / id / mode
            print(work.relative_to(output), flush=True)
            result = one_process(binaries["phases"], id, mode, work, case, settings, deadline)
            if result["status"] == "ok":
                profiles[id][mode] = classify_phases(work / "stderr.log", result["samples"], rows)
            else:
                profiles[id][mode] = {"status": result["status"]}

    def summary(observations):
        states = [observation["status"] for observation in observations]
        if states != ["ok"] * settings["replicas"]:
            return {"states": states}
        return {
            "projection_subtracted_ns": runner.signed_stats(increments(observations)),
            "execution_ns": runner.signed_stats(
                [sample["execution_ns"] / rows for result in observations for sample in result["samples"]]),
        }

    stats = {}
    for id in ids:
        noise = None
        if id in controls and all(len(arm) == settings["replicas"]
                                  and all(o["status"] == "ok" for o in arm)
                                  for arm in controls[id].values()):
            noise = runner.noise(*[increments(arm) for arm in controls[id].values()])
        stats[id] = {"ordinary": {mode: summary(results) for mode, results in timings[id].items()},
                     "native_noise_ns": noise, "profile": profiles[id]}
    write(output / "summary.json", stats)


def main():
    assert __debug__, "Do not disable validation"
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="action", required=True)
    builder = subs.add_parser("build")
    builder.add_argument("variant", choices=("plain", "phases"))
    for name in ("repository", "target", "output"):
        builder.add_argument(name, type=Path)
    timing = subs.add_parser("measure")
    for name in ("plain", "phases", "output"):
        timing.add_argument(name, type=Path)
    subs.add_parser("check")
    args = parser.parse_args()
    if args.action == "build":
        build(args.repository.resolve(), args.target.resolve(), args.output.resolve(), args.variant)
    elif args.action == "measure":
        measure({variant: getattr(args, variant).resolve() for variant in ("plain", "phases")},
                args.output.resolve())
    else:
        protocol()
        assert schedule()["input_rows"] == 1_000_000
        assert len(list(changes(HERE.parents[1], False))) == 2
        assert len(list(changes(HERE.parents[1], True))) == 10
        print("All 18 frozen queries and aggregate phase anchors verified")


if __name__ == "__main__":
    main()
