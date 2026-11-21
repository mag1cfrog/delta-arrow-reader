#!/usr/bin/env python3
"""Compare a SQL function candidate and staging in one release session."""

import argparse
import filecmp
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from run import HERE, MANIFEST, WORKSPACE, build, increments, observe, one_process, process_medians, protocol, runner, schedule, write

ARMS = ("staging", "candidate")


def statement_manifest(settings_file, settings):
    if "statement_manifest" not in settings:
        return MANIFEST
    path = (settings_file.parent / settings["statement_manifest"]).resolve()
    assert runner.digest(path) == settings["statement_manifest_sha256"]
    return path


def selected_cases(settings, manifest=MANIFEST):
    statements = protocol() if manifest == MANIFEST else runner.read(manifest)
    cases = {case["id"]: case for case in statements["functions"]}
    assert settings["case_ids"] == sorted(set(settings["case_ids"]))
    return [cases[case_id] for case_id in settings["case_ids"]]


def preflight(binaries, cases, output, settings, deadline, plan_checks, manifest):
    eligible, excluded = {}, {}
    for case in cases:
        case_id = case["id"]
        try:
            before = observe(binaries["staging"], case, output / case_id / "staging", settings, deadline, manifest)
            after = observe(binaries["candidate"], case, output / case_id / "candidate", settings, deadline, manifest)
            if before["types"] != after["types"]:
                raise ValueError("executed types differ between staging and candidate")
            for mode in before["modes"]:
                filename = f"{mode}.rows.json"
                if not filecmp.cmp(before["plans"] / filename, after["plans"] / filename, shallow=False):
                    raise ValueError(f"{mode}: executed rows differ from staging")
                if mode != "native":
                    for arm, observed in zip(ARMS, (before, after)):
                        fragments = plan_checks.get(arm, {})
                        group = case_id.split(".", 1)[0]
                        required = fragments.get(group, [])
                        plan = (observed["plans"] / f"{mode}.plan").read_text().lower()
                        if any(fragment not in plan for fragment in required):
                            raise ValueError(f"{mode}: {arm} plan lacks expected expression")
            eligible[case_id] = {k: v for k, v in before.items() if k != "plans"}
        except ValueError as error:
            excluded[case_id] = str(error)
    return eligible, excluded


def summarize(observations):
    states = [result["status"] for result in observations]
    if states != ["ok"] * 3:
        return {"states": states}
    return runner.signed_stats(increments(observations))


def timed_case(binary, case_id, mode, work, expected, settings, deadline, manifest):
    print(str(work), flush=True)
    return one_process(binary, case_id, mode, work, expected, settings, deadline, manifest)


def measure(settings_file, directories, output):
    assert not runner.git("status", "--porcelain"), "freeze the comparison source before clocks"
    candidate = runner.read(settings_file)
    head = runner.git("rev-parse", "HEAD")
    for key in ("staging_source", "candidate_source", "staging_tip"):
        assert len(candidate[key]) == 40
    differences = runner.git("diff", "--name-only", candidate["staging_source"], candidate["staging_tip"]).splitlines()
    assert all(path.startswith(str(WORKSPACE / "benches") + "/") for path in differences)
    subprocess.run(["git", "merge-base", "--is-ancestor", candidate["staging_tip"], candidate["candidate_source"]],
                   cwd=HERE, check=True)
    assert not runner.git("diff", "--name-only", candidate["candidate_source"], head, "--",
                          str(WORKSPACE / "src"), str(WORKSPACE / "vendor"), str(WORKSPACE / "tests"),
                          str(WORKSPACE / "Cargo.toml"), str(WORKSPACE / "Cargo.lock"))
    manifest = statement_manifest(settings_file, candidate)
    cases = selected_cases(candidate, manifest)
    settings = schedule()
    assert settings["replicas"] == 3
    deadline = time.monotonic() + candidate["overall_timeout_seconds"]
    output.mkdir()
    binaries, receipts = {}, {}
    for arm, directory in directories.items():
        binary = directory / "functions"
        receipt = runner.read(directory / "receipt.json")
        assert receipt["source"] == candidate[f"{arm}_source"] and receipt["variant"] == "plain"
        assert receipt["patch_sha256"] == runner.digest(directory / "patch.json")
        assert receipt["sha256"] == runner.digest(binary)
        assert set(runner.read(directory / "patch.json")) == {"benches/functions.rs", "tests/support/spark_results.rs"}
        binaries[arm], receipts[arm] = binary, receipt
    assert receipts["staging"]["patch_sha256"] == receipts["candidate"]["patch_sha256"]
    eligible, excluded = preflight(binaries, cases, output / "preflight", settings, deadline,
                                   candidate.get("spark_plan_contains", {}), manifest)
    write(output / "session.json", {
        "protocol_commit": head, "comparison_sha256": runner.digest(settings_file),
        "comparison": candidate, "builds": receipts, "schedule": settings,
        "eligible": eligible, "excluded": excluded,
    })
    ids = sorted(eligible)
    paired = {case_id: {mode: {arm: [] for arm in ARMS} for mode in expected["modes"]}
              for case_id, expected in eligible.items()}
    noise = {case_id: {arm: [] for arm in ("a", "b")} for case_id in ids
             if "native" in eligible[case_id]["modes"]}
    for replica in range(settings["replicas"]):
        offset = replica % max(1, len(ids))
        order = -1 if replica == 1 else 1
        for case_id in ids[offset:] + ids[:offset]:
            expected = eligible[case_id]
            for mode in expected["modes"][::order]:
                if any(results and results[-1]["status"] != "ok" for results in paired[case_id][mode].values()):
                    continue
                for arm in ARMS[::order]:
                    work = output / "paired" / str(replica) / case_id / mode / arm
                    result = timed_case(binaries[arm], case_id, mode, work, expected, settings, deadline, manifest)
                    paired[case_id][mode][arm].append(result)
            if case_id in noise:
                for arm in settings["noise_orders"][replica]:
                    previous = noise[case_id][arm]
                    if previous and previous[-1]["status"] != "ok":
                        continue
                    work = output / "noise" / str(replica) / case_id / arm
                    previous.append(timed_case(binaries["candidate"], case_id, "native", work,
                                               expected, settings, deadline, manifest))
    summary = {}
    for case_id, modes in paired.items():
        result = {mode: {arm: summarize(runs) for arm, runs in comparisons.items()}
                  for mode, comparisons in modes.items()}
        floor = None
        if case_id in noise and all(len(arm) == settings["replicas"]
                                     and all(o["status"] == "ok" for o in arm)
                                     for arm in noise[case_id].values()):
            floor = runner.noise(*[process_medians(arm) for arm in noise[case_id].values()])
        result["native_noise_ns"] = floor
        summary[case_id] = result
    write(output / "summary.json", summary)


def main():
    if not __debug__:
        raise SystemExit("Run with Python validation enabled.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("comparison", type=Path)
    subs = parser.add_subparsers(dest="action", required=True)
    prep = subs.add_parser("build")
    prep.add_argument("arm", choices=ARMS)
    for name in ("repository", "target", "output"):
        prep.add_argument(name, type=Path)
    timing = subs.add_parser("measure")
    for name in (*ARMS, "output"):
        timing.add_argument(name, type=Path)
    subs.add_parser("check")
    args = parser.parse_args()
    settings = runner.read(args.comparison)
    if args.action == "build":
        build(args.repository.resolve(), args.target.resolve(), args.output.resolve(), "plain",
              settings[f"{args.arm}_source"])
    elif args.action == "measure":
        measure(args.comparison.resolve(), {arm: getattr(args, arm).resolve() for arm in ARMS},
                args.output.resolve())
    else:
        manifest = statement_manifest(args.comparison.resolve(), settings)
        print(f"Checked {len(selected_cases(settings, manifest))} frozen comparison cases")


if __name__ == "__main__":
    main()
