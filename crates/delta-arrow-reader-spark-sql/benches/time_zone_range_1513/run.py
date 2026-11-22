"""Compare #1409 release execution in a session with staging self-pairs."""

from collections import defaultdict
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
from statistics import median
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
MANIFEST = HERE.parent / "time_conversions_1409/manifest.json"
STAGING = "689be9bc1117c55f0bdb85d93051fa81afe3f40d"
ARMS = ("staging_a", "candidate", "staging_b")
sys.path.insert(0, str(HERE.parent / "window_aggregates_1408"))

from compare import preflight
from run import runner, schedule, write


def emit(output, row):
    output.write(json.dumps(row, separators=(",", ":")) + "\n")


def receipt(directory):
    binary = directory / "functions"
    identity = runner.read(directory / "receipt.json")
    assert identity["binary_sha256"] == runner.digest(binary)
    assert identity["target"] == "functions" and identity["profile"] == "release"
    assert identity["debug_info"] == 1
    assert identity["toolchain"].startswith("rustc 1.96.0 ")
    assert identity["observation_overlay_sha256"] == (
        "fcba9875a935e6724de0f51a2b5a5e556512ce353ca574de7afd8a05f8ad693c"
    )
    return binary, identity


def main(baseline, candidate, destination):
    binaries, receipts = {}, {}
    for arm, directory in (("staging", baseline), ("candidate", candidate)):
        binaries[arm], receipts[arm] = receipt(directory)
    before, after = receipts["staging"], receipts["candidate"]
    head = runner.git("rev-parse", "HEAD")
    assert not runner.git("status", "--porcelain")
    assert before["source_commit"] == STAGING and after["source_commit"] == head
    assert before["binary_sha256"] != after["binary_sha256"]
    assert before["observation_overlay_sha256"] == after["observation_overlay_sha256"]
    assert before["toolchain"] == after["toolchain"]

    assert runner.digest(MANIFEST) == "f79bbdb2e8920f590a13ccaa153d8dd9244fd5856253130b29cc0db38d4f46f6"
    manifest = runner.read(MANIFEST)
    cases = manifest["functions"]
    assert len(cases) == len({case["id"] for case in cases}) == 16
    assert sum(case["native_sql"] is not None for case in cases) == 10
    destination.mkdir()
    limits = schedule()
    limits["process_timeout_seconds"] = 300
    deadline = time.monotonic() + 45 * 60
    eligible, excluded = preflight(
        binaries, cases, destination / "preflight", limits, deadline, {}, MANIFEST
    )
    identity = {
        "protocol_commit": head, "manifest_sha256": runner.digest(MANIFEST),
        "builds": receipts, "eligible": eligible, "excluded": excluded,
    }
    write(destination / "eligibility.json", identity)
    groups = defaultdict(dict)
    failed = []
    archive = destination / "samples.jsonl.gz"
    with gzip.open(archive, "xt") as raw:
        emit(raw, identity | {"started_utc": datetime.now(timezone.utc).isoformat()})
        for block in range(4):
            modes = ("spark-off", "spark-on", "native")
            if block % 2:
                modes = modes[::-1]
            arms = ARMS if block % 2 == 0 else ARMS[::-1]
            for mode in modes:
                ids = [case["id"] for case in cases
                       if case["id"] in eligible and mode in eligible[case["id"]]["modes"]]
                if not ids:
                    continue
                id_file = destination / f"block-{block}-{mode}.json"
                write(id_file, ids)
                for label in arms:
                    binary = binaries["candidate" if label == "candidate" else "staging"]
                    context = {"block": block, "mode": mode, "arm": label}
                    proc = runner.invoke(
                        binary, ["measure-set", MANIFEST, id_file, mode],
                        destination / f"run-{block}-{mode}-{label}", limits, deadline,
                    )
                    results = runner.assemble_cases(proc, ids, mode)
                    for row in proc.get("records", []):
                        if row.get("kind") == "retained_sample":
                            emit(raw, context | {"observation": row})
                    for result in results:
                        case_id = result["id"]
                        types = eligible[case_id]["types"]
                        count = eligible[case_id]["output_rows"]
                        samples = result["samples"]
                        if proc["status"] != "ok" or result["status"] != "ok" or result.get("types") != types or len(samples) != 3 or not all(
                            item["input_rows"] == limits["input_rows"] and item["output_rows"] == count
                            and item["execution_ns"] - item["projection_ns"] == item["increment_ns"]
                            for item in samples
                        ):
                            failed.append(context | {"case": case_id})
                            continue
                        groups[case_id, mode, block][label] = median(
                            item["increment_ns"] / item["input_rows"] for item in samples
                        )
                    emit(raw, context | {"process_status": proc["status"], "cases": len(results)})
                    raw.flush()
                    print(block, mode, label, "complete" if proc["status"] == "ok" else proc["status"], flush=True)
        summary = {}
        for case_id, expected in eligible.items():
            for mode in expected["modes"]:
                blocks = [groups.get((case_id, mode, block), {}) for block in range(4)]
                name = f"{case_id}/{mode}"
                if not all(set(block) == set(ARMS) for block in blocks):
                    summary[name] = {"status": "incomplete"}
                    continue
                staging = [(b["staging_a"] + b["staging_b"]) / 2 for b in blocks]
                after = [b["candidate"] for b in blocks]
                delta = median(new - base for base, new in zip(staging, after))
                noise = max(abs(b["staging_a"] - b["staging_b"]) for b in blocks)
                summary[name] = {
                    "status": "pass" if delta <= noise else "slower",
                    "staging_ns_per_row": median(staging), "candidate_ns_per_row": median(after),
                    "paired_delta_ns_per_row": delta, "same_session_noise_ns_per_row": noise,
                }
        emit(raw, {"finished_utc": datetime.now(timezone.utc).isoformat(),
                   "summary": summary, "failed": failed})
    write(destination / "summary.json", summary)
    return bool(excluded or failed or any(row["status"] != "pass" for row in summary.values()))


if __name__ == "__main__":
    sys.exit(main(*(Path(arg).resolve() for arg in sys.argv[1:])))
