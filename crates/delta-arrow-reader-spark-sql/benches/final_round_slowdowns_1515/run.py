"""Repeat the historical shifts and isolate the one traced source change."""

from collections import defaultdict
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import platform
from statistics import median
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
BEFORE = "e1580ef0056bdfe70161fe07be94ed10957ed7ec"
FINAL = "51b0c8eac613aec22a56a6d1e8f4828e84b1d316"
WITHOUT_LITERAL_TIME_DIFF = "b442c02c164cf466f7c3c898604bcbd34a707d59"
OVERLAY = "8b5c9c81dd39342aeded485c5f87f3efde4bdb17f9840c3a7dc536c5797b28ad"
HISTORICAL_SHA = {
    BEFORE: "b432334fbca058bd100f6418fce6cc6267f950f37221d2ee950ffdacbd1020d8",
    FINAL: "be845264945635e7d9801d57906a9664331421a1b8173966e926c3c047d4c0ab",
}

sys.path.insert(0, str(HERE.parent / "window_aggregates_1408"))
from compare import preflight
from run import runner, schedule, write


def read_artifact(directory, source):
    file = directory / "functions"
    receipt = runner.read(directory / "receipt.json")
    reported_commit = receipt.get("source") or receipt["source_commit"]
    reported_digest = receipt.get("sha256") or receipt["binary_sha256"]
    assert source == reported_commit and runner.digest(file) == reported_digest
    assert receipt["patch_sha256"] == OVERLAY
    if source in HISTORICAL_SHA:
        assert reported_digest == HISTORICAL_SHA[source] and receipt["variant"] == "plain"
        assert receipt["profile"]["opt_level"] == "3" and receipt["profile"]["debuginfo"] == 1
    else:
        assert source == WITHOUT_LITERAL_TIME_DIFF
        assert receipt["target"] == "functions" and receipt["profile"] == "release"
        assert receipt["debug_info"] == 1 and receipt["toolchain"].startswith("rustc 1.96.0 ")
    return file, {"source_commit": source, "binary_sha256": reported_digest, "patch_sha256": OVERLAY}


def append(archive, rows):
    with gzip.open(archive, "at") as output:
        for row in rows:
            output.write(json.dumps(row, separators=(",", ":")) + "\n")


def compare_modes(arms):
    if any(set(samples) != {0, 1, 2} for samples in arms.values()):
        return {"status": "incomplete"}
    staging, candidate = [list(medians.values()) for medians in arms.values()]
    deltas = [new - old for old, new in zip(staging, candidate)]
    delta = median(deltas)
    floor = max(max(values) - min(values) for values in (staging, candidate)) / 2
    if sum(d > 0 for d in deltas) >= 2 and delta > floor:
        status = "slower"
    elif sum(d < 0 for d in deltas) >= 2 and -delta > floor:
        status = "faster"
    else:
        status = "within_floor"
    return {"status": status, "staging": median(staging), "candidate": median(candidate),
            "paired_increase": delta, "floor": floor}


def run_trial(trial, base, after, output):
    assert platform.system() == "Linux" and not runner.git("status", "--porcelain")
    protocol = runner.git("rev-parse", "HEAD")
    timing_modes = defaultdict(list)
    if trial == "repeat":
        manifest = HERE.parent / "spark_only_functions_1414/final-coverage-manifest.json"
        assert runner.digest(manifest) == "2ee09bc0dbb13f90cce2f14f82891e705dd9954fbdb12d32408d0338dec3769e"
        slowdowns = runner.read(HERE / "original-slowdowns.json")
        for row in slowdowns:
            if row["attribution"] == "whole_build":
                timing_modes[row["id"]].append(row["mode"])
        assert len(timing_modes) == 28 and sum(map(len, timing_modes.values())) == 39
        source = BEFORE
        timeout = 1800
    else:
        assert trial == "isolate"
        manifest = HERE / "timediff-manifest.json"
        for case_id in ("scalar.datetime.timestampdiff", "scalar.datetime.timediff"):
            timing_modes[case_id] = ["spark-off", "spark-on"]
        source = WITHOUT_LITERAL_TIME_DIFF
        timeout = 900
    by_id = {case["id"]: case for case in runner.read(manifest)["functions"]}
    cases = [by_id[case_id] for case_id in sorted(timing_modes)]
    assert all(case["native_sql"] is None for case in cases)
    binaries, receipts = {}, {}
    for arm, root, commit in (("staging", base, source), ("candidate", after, FINAL)):
        binaries[arm], receipts[arm] = read_artifact(root, commit)
    assert receipts["staging"]["binary_sha256"] != receipts["candidate"]["binary_sha256"]
    output.mkdir()
    limits = schedule()
    deadline = time.monotonic() + timeout
    eligible, excluded = preflight(binaries, cases, output / "preflight", limits, deadline, {}, manifest)
    ids = sorted(eligible)
    archive = output / "samples.jsonl.gz"
    header = {
        "trial": trial, "protocol_commit": protocol, "manifest_sha256": runner.digest(manifest),
        "selection_sha256": runner.digest(HERE / "original-slowdowns.json"),
        "builds": receipts, "eligible": eligible, "excluded": excluded, "timing_modes": timing_modes,
        "started_utc": datetime.now(timezone.utc).isoformat(),
    }
    append(archive, [header])
    write(output / "eligibility.json", header)
    medians = {(case_id, mode): {"staging": {}, "candidate": {}}
               for case_id in ids for mode in timing_modes[case_id]}
    for replica in range(3):
        offset = replica % max(1, len(ids))
        order = -1 if replica == 1 else 1
        for case_id in ids[offset:] + ids[:offset]:
            expected = eligible[case_id]
            for mode in timing_modes[case_id][::order]:
                for arm in ("staging", "candidate")[::order]:
                    context = {"replica": replica, "id":case_id, "mode": mode, "arm": arm}
                    work = output / str(replica) / case_id / mode / arm
                    work.parent.mkdir(parents=True, exist_ok=True)
                    selection = work.with_suffix(".ids.json")
                    write(selection, [case_id])
                    process = runner.invoke(
                        binaries[arm], ["measure-set", manifest, selection, mode],
                        work, limits, deadline,
                    )
                    events = [context | {"observation": row} for row in process.get("records", [])
                              if row.get("kind") == "retained_sample"]
                    append(archive, events)
                    result, = runner.assemble_cases(process, [case_id], mode)
                    samples = result["samples"]
                    valid = (process["status"] == result["status"] == "ok"
                             and [item["iteration"] for item in samples] == [1, 2, 3]
                             and result["types"] == expected["types"]
                             and all(item["input_rows"] == limits["input_rows"]
                                     and item["output_rows"] == expected["output_rows"]
                                     and item["execution_ns"] - item["projection_ns"] == item["increment_ns"]
                                     for item in samples))
                    if valid:
                        medians[case_id, mode][arm][replica] = median(
                            sample["increment_ns"] / sample["input_rows"] for sample in samples
                        )
                    append(archive, [context | {"process_status": "ok" if valid else "incomplete"}])
        print(trial, "replica", replica + 1, "complete", flush=True)
    summary = {f"{case_id}/{mode}": compare_modes(arms)
               for (case_id, mode), arms in medians.items()}
    append(archive, [{"finished_utc": datetime.now(timezone.utc).isoformat(), "summary": summary}])
    write(output / "summary.json", summary)
    return bool(excluded or any(item["status"] == "incomplete" for item in summary.values()))


if __name__ == "__main__":
    sys.exit(run_trial(sys.argv[1], *(Path(item).resolve() for item in sys.argv[2:])))
