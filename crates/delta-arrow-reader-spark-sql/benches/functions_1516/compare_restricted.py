"""Compare executable results and time only equivalent restricted SQL cases."""

import argparse
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "time_conversions_1409_round3"))
from run_pair import archive, git, write
from run import increments, one_process, process_medians, runner, schedule

MANIFEST = HERE / "manifest.json"
MODES = ("native", "spark-off", "spark-on")
STAGING = "35e67f91ff008babc6f65a5934c7be3c8f1b249e"


def verified_build(build):
    assert not git("status", "--porcelain")
    assert git("merge-base", "--is-ancestor", STAGING, "HEAD") == ""
    receipt = runner.read(build / "receipt.json")
    binary = build / "functions"
    assert runner.digest(binary) == receipt["sha256"]
    assert runner.digest(build / "patch.json") == receipt["patch_sha256"]
    assert receipt["variant"] == "plain"
    assert not git("diff", "--name-only", receipt["source"], "HEAD", "--",
                   str(HERE.parents[1] / "src"), str(HERE.parents[1] / "vendor"),
                   str(HERE.parents[1] / "Cargo.toml"), str(HERE.parents[1] / "Cargo.lock"))
    return binary, receipt


def preflight(binary, cases, output, tuning, deadline):
    observations, excluded = {}, {}
    for case in cases:
        case_id = case["id"]
        if "restricted_input_sql" not in case:
            excluded[case_id] = case["source_review"]
            continue
        work = output / "preflight" / case_id
        started = runner.invoke(binary, ["preflight", MANIFEST, case_id, work / "plans"],
                                work, tuning, deadline, preflight=True)
        if started["status"] != "ok":
            excluded[case_id] = started["status"]
            continue
        result = started["records"][-1]
        outcomes = {row["configuration"]: row for row in result["results"]}
        rows = result["input_rows"]
        declared = 1 if case_id.startswith("aggregate.") else rows
        eligible = (
            tuple(outcomes) == MODES and 0 < rows <= tuning["input_rows"]
            and len(result["baselines"]) == 3
            and all(row["rows"] == rows for row in result["baselines"])
            and all(row["status"] == "ok" and row["rows"] == declared
                    and row["source_guard_rows"] == rows for row in outcomes.values())
            and all(outcomes[mode].get("comparison", {}).get("equal") for mode in MODES[1:])
            and all(row["types"] == outcomes["native"]["types"] for row in outcomes.values())
        )
        if eligible:
            observations[case_id] = {
                "input_rows": rows, "output_rows": declared, "types": outcomes["native"]["types"],
            }
        else:
            excluded[case_id] = "Executed rows, types, planning or source-guard checks differ."
        write(work / "classification.json", {
            "input_rows": rows, "output_rows": declared, "eligible": eligible,
            "results": [{key: row.get(key) for key in ("configuration", "status", "rows", "types",
                                                       "comparison")}
                        for row in outcomes.values()],
        })
    return observations, excluded


def measure(binary, cases, output, tuning, deadline, observations):
    samples = {case_id: {mode: [] for mode in MODES} for case_id in observations}
    repeats = {case_id: [] for case_id in observations}
    failures = {}
    for block in range(tuning["replicas"]):
        ordered = tuple(cases) if block != 1 else tuple(reversed(cases))
        modes = MODES if block != 1 else MODES[::-1]
        for case in ordered:
            case_id = case["id"]
            if case_id not in observations or case_id in failures:
                continue
            expected = observations[case_id]
            settings = dict(tuning, input_rows=expected["input_rows"])
            for mode in modes:
                if case_id in failures:
                    break
                primary = ("paired", samples[case_id][mode])
                extra = ("noise", repeats[case_id])
                if mode != "native":
                    arms = (primary,)
                else:
                    arms = (extra, primary) if block == 1 else (primary, extra)
                for arm, records in arms:
                    work = output / arm / str(block) / case_id / mode
                    observed = one_process(binary, case_id, mode, work, expected, settings,
                                           deadline, MANIFEST)
                    records.append(observed)
                    if observed["status"] != "ok":
                        failures[case_id] = dict(block=block, mode=mode, arm=arm,
                                                 status=observed["status"])
                        break
    summary = {}
    for case_id, outcomes in samples.items():
        modes = {mode: runner.signed_stats(increments(rows)) if completed(rows) else {
            "states": [r["status"] for r in rows],
        } for mode, rows in outcomes.items()}
        if completed(outcomes["native"]) and completed(repeats[case_id]):
            modes["native_floor"] = runner.noise(process_medians(outcomes["native"]),
                                                  process_medians(repeats[case_id]))
        if case_id in failures:
            modes["incomplete"] = failures[case_id]
        summary[case_id] = modes
    write(output / "summary.json", summary)


def completed(rows):
    return len(rows) == 3 and all(row["status"] == "ok" for row in rows)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("action", choices=("preflight", "measure", "archive"))
    cli.add_argument("input", type=lambda v: Path(v).resolve())
    cli.add_argument("output", type=lambda v: Path(v).resolve())
    args = cli.parse_args()
    if args.action == "archive":
        archive(args.output, args.input)
        return
    binary, receipt = verified_build(args.input)
    tuning = schedule()
    cases = runner.read(MANIFEST)["functions"]
    assert len(cases) == 29
    output = args.output
    output.mkdir()
    deadline = time.monotonic() + (7200 if args.action == "measure" else 3600)
    observations, excluded = preflight(binary, cases, output, tuning, deadline)
    write(output / "session.json", {
        "staging": STAGING, "run_commit": git("rev-parse", "HEAD"), "build": receipt,
        "manifest_sha256": runner.digest(MANIFEST), "schedule": tuning,
        "observations": observations, "excluded": excluded,
    })
    if args.action == "measure":
        measure(binary, cases, output, tuning, deadline, observations)


if __name__ == "__main__":
    main()
