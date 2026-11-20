#!/usr/bin/env python3
"""Build and run the bounded #1407 XPath attribution protocol."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from instrument import changes

HERE = Path(__file__).resolve().parent
MANIFEST = HERE / "manifest.json"
WORKSPACE = HERE.relative_to(HERE.parents[3]).parents[1]
FROZEN = HERE.parent / "functions"

sys.path.insert(0, str(FROZEN))
spec = importlib.util.spec_from_file_location("frozen_functions", FROZEN / "run.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

MODES = ("spark-off", "spark-on")
VARIANTS = ("plain", "phases")
PHASES = ("document", "queries_setup_drop", "compile", "evaluate", "output", "residual")


def write(path, value):
    with path.open("x") as out:
        json.dump(value, out, indent=2)
        out.write("\n")


def git(repository, *args):
    return subprocess.check_output(["git", "-C", str(repository), *args], text=True).strip()


def manifest():
    declared = runner.read(MANIFEST)
    frozen = FROZEN / "manifest.json"
    assert runner.digest(frozen) == declared["frozen_manifest_sha256"]
    ids = {f["id"]: f for f in runner.read(frozen)["functions"] if f["id"].startswith("scalar.xml.xpath")}
    assert set(ids) == {f["id"] for f in declared["functions"]} and len(ids) == 9
    for function in declared["functions"]:
        assert all(v == ids[function["id"]][k] for k, v in function.items())
        assert function["native_sql"] is None
    return declared


def build(repository, target, output, variant, protocol):
    assert git(repository, "rev-parse", "HEAD") == protocol["baseline_source"]
    assert not git(repository, "status", "--porcelain")
    workspace = repository / WORKSPACE
    files = dict(changes(workspace, protocol["reduced_rows"], variant == "phases"))
    originals = {path: path.read_bytes() if path.exists() else None for path in files}
    output.mkdir(parents=True)
    command = ["cargo", "+1.96.0", "build", "--locked", "--release", "--bench", "functions",
               "--message-format=json-render-diagnostics", "-j", "2"]
    env = {"CARGO_TARGET_DIR": str(target), "CARGO_PROFILE_RELEASE_DEBUG": "1"}
    log = output / "build.jsonl"
    try:
        for path, source in files.items():
            path.write_text(source)
        with log.open("x") as stdout, (output / "build.stderr").open("x") as stderr:
            subprocess.run(command, cwd=workspace, env=os.environ | env, stdout=stdout,
                           stderr=stderr, check=True, timeout=1500)
    finally:
        for path, source in originals.items():
            if source is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(source)
    binary, = [r for r in runner.parse_lines(log.read_text()) if r.get("executable")
               and r.get("target", {}).get("name") == "functions"]
    assert binary["profile"]["opt_level"] == "3" and binary["profile"]["debuginfo"] == 1
    exe = output / "functions"
    subprocess.run(["cp", "--reflink=auto", binary["executable"], exe], check=True)
    assert runner.digest(exe) == runner.digest(Path(binary["executable"]))
    (output / "patch.json").write_text(json.dumps({str(p.relative_to(workspace)): s for p, s in files.items()}))
    write(output / "receipt.json", {"source": protocol["baseline_source"], "sha256": runner.digest(exe),
          "variant": variant, "patch_sha256": runner.digest(output / "patch.json"), "build": command,
          "environment": env, "profile": binary["profile"]})


def phase_samples(path, rows):
    records = []
    for line in path.read_text().splitlines():
        if not line.startswith("xpath-phases "):
            continue
        _, batch_rows, wall, counters = line.split(" ", 3)
        document, queries_setup_drop, compile_ns, evaluate, output, documents, compiled = json.loads(counters)
        spent = [document, queries_setup_drop, compile_ns, evaluate, output]
        residual = int(wall) - sum(spent)
        assert int(batch_rows) == rows and documents == compiled == rows - rows // 16 and residual >= 0, line
        phases = dict(zip(PHASES, [*(v / rows for v in spent), residual / rows]))
        records.append(phases | {"batch_wall": int(wall) / rows})
    assert len(records) == 4, path
    return records[1:]


def median_and_spread(values):
    return [statistics.median(values), min(values), max(values)]


def measure(builds, out, protocol):
    assert not runner.git("status", "--porcelain", "--", str(HERE))
    protocol_commit = runner.git("rev-parse", "HEAD")
    out.mkdir()
    receipt = {name: runner.read(path / "receipt.json") for name, path in builds.items()}
    for name, path in builds.items():
        assert runner.digest(path / "functions") == receipt[name]["sha256"]
        assert runner.digest(path / "patch.json") == receipt[name]["patch_sha256"]
        assert receipt[name]["source"] == protocol["baseline_source"] and receipt[name]["variant"] == name
    frozen = runner.read(FROZEN / "schedule.json")
    rows = protocol["reduced_rows"]
    schedule = {key: frozen[key] for key in (
        "cpus", "batch_rows", "target_partitions", "runtime_workers", "warmups_per_case",
        "samples_per_case", "address_space_bytes", "pool_bytes",
    )}
    schedule.update(input_rows=rows, case_timeout_seconds=frozen["case_timeout_seconds"],
                    process_timeout_seconds=protocol["per_process_seconds"],
                    preflight_process_timeout_seconds=protocol["per_process_seconds"],
                    schedule_timeout_seconds=4 * 3600, replicas=protocol["replicas"],
                    case_order="sorted eligible IDs rotated left by three per replica",
                    mode_and_build_order="reverse on the second replica")
    deadline = time.monotonic() + schedule["schedule_timeout_seconds"]
    eligible, excluded = {}, {}
    for function in protocol["functions"]:
        name = function["id"]
        reference = None
        physical_function = f'name: "{name.rsplit(".", 1)[1]}",'
        for variant in VARIANTS:
            work = out / "preflight" / name / variant
            observed = runner.invoke(builds[variant] / "functions",
                                     ["preflight", MANIFEST, name, work / "plans"],
                                     work, schedule, deadline, preflight=True)
            result = observed["records"][-1] if observed["status"] == "ok" else {}
            results = result.get("results", [])
            if (
                [r["configuration"] for r in results] != list(MODES)
                or any(r["status"] != "ok" or r["rows"] != rows or r["source_guard_rows"] != rows
                       or not any(physical_function in expr for expr in r["function_evidence"]["expressions"])
                       for r in results)
                or len(result.get("baselines", [])) != 3
                or any(r["rows"] != rows for r in result["baselines"])
            ):
                excluded[name] = f"{variant}: failed source, physical function or result guard"
                break
            actual = [(r["types"], runner.read(work / "plans" / f"{r['configuration']}.rows.json"))
                      for r in results]
            if actual[0] != actual[1] or (reference is not None and reference != actual[0]):
                excluded[name] = f"{variant}: executed types or values differ"
                break
            reference = actual[0]
        else:
            eligible[name] = reference[0]

    write(out / "session.json", {"protocol_commit": protocol_commit, "schedule": schedule,
          "eligible": eligible, "excluded": excluded, "builds": receipt,
          "manifest_sha256": hashlib.sha256(MANIFEST.read_bytes()).hexdigest()})
    ids = sorted(eligible)
    observations = {name: {mode: [] for mode in MODES} for name in ids}
    for replica in range(protocol["replicas"]):
        offset = replica * 3 % max(1, len(ids))
        for name in ids[offset:] + ids[:offset]:
            for mode in MODES[:: -1 if replica == 1 else 1]:
                classified = {}
                for variant in VARIANTS[:: -1 if replica == 1 else 1]:
                    work = out / str(replica) / name / mode / variant
                    id_file = work.with_suffix(".ids.json")
                    id_file.parent.mkdir(parents=True, exist_ok=True)
                    write(id_file, [name])
                    execution = runner.invoke(builds[variant] / "functions",
                                              ["measure-set", MANIFEST, id_file, mode],
                                              work, schedule, deadline)
                    result, = runner.assemble_cases(execution, [name], mode)
                    assert result["status"] == "ok" and result["types"] == eligible[name], result
                    assert all(s["output_rows"] == s["input_rows"] == rows for s in result["samples"])
                    if variant == "phases":
                        data = phase_samples(work / "stderr.log", rows)
                        classified.update({phase: statistics.median(row[phase] for row in data)
                                           for phase in (*PHASES, "batch_wall")})
                    else:
                        for component in ("execution_ns", "projection_ns", "increment_ns"):
                            classified[component] = statistics.median(
                                sample[component] / rows for sample in result["samples"])
                observations[name][mode].append(classified)
    summary = {name: {mode: {key: median_and_spread([row[key] for row in samples])
                            for key in samples[0]} for mode, samples in modes.items()}
               for name, modes in observations.items()}
    write(out / "summary.json", summary)


def main():
    assert __debug__, "Do not disable validation"
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    prepare = actions.add_parser("build")
    prepare.add_argument("repository", type=Path)
    prepare.add_argument("target", type=Path)
    prepare.add_argument("output", type=Path)
    prepare.add_argument("variant", choices=VARIANTS)
    collect = actions.add_parser("measure")
    for name in (*VARIANTS, "output"):
        collect.add_argument(name, type=Path)
    actions.add_parser("check")
    args = parser.parse_args()
    protocol = manifest()
    if args.action == "build":
        build(args.repository.resolve(), args.target.resolve(), args.output.resolve(), args.variant, protocol)
    elif args.action == "measure":
        measure({name: getattr(args, name).resolve() for name in VARIANTS}, args.output.resolve(), protocol)
    else:
        assert len(list(changes(HERE.parents[1], protocol["reduced_rows"], False))) == 1
        assert len(list(changes(HERE.parents[1], protocol["reduced_rows"], True))) == 5
        print("XPath manifest and scratch phase anchors verified")


if __name__ == "__main__":
    main()
