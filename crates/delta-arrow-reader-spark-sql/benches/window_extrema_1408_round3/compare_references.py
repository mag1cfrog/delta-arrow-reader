"""Measure position reads and native extrema in the same window session."""

import argparse
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(HERE.parent / "window_aggregates_1408"))
from run import increments, observe, one_process, process_medians, runner, schedule, write

MANIFEST = HERE / "manifest.json"
SOURCE = "f5ea78cb8485523738775056d28a2232e9c32762"
STAGING = "8e790f97f4730ad4c46c5c11a7ae320cb8c54cf6"


def git(*args):
    return subprocess.check_output(["git", "-C", str(REPO), *args])


def verify_release(build):
    assert not git("status", "--porcelain")
    assert MANIFEST.read_bytes() == git("show", f"HEAD:{MANIFEST.relative_to(REPO)}")
    delta = git("diff", "--name-only", SOURCE, STAGING).decode().splitlines()
    assert all(path.startswith(str(HERE.parent.relative_to(REPO)) + "/") for path in delta)
    receipt = runner.read(build / "receipt.json")
    binary = build / "functions"
    assert receipt["source"] == receipt["protocol_commit"] == SOURCE
    assert receipt["variant"] == "plain" and runner.digest(binary) == receipt["sha256"]
    assert runner.digest(build / "patch.json") == receipt["patch_sha256"]
    return binary, receipt


def measure(build, output):
    binary, receipt = verify_release(build)
    settings = schedule()
    assert settings["replicas"] == 3 and settings["samples_per_case"] == 3
    deadline = time.monotonic() + 3600
    output.mkdir()
    eligible, excluded = {}, {}
    for case in runner.read(MANIFEST)["functions"]:
        case_id = case["id"]
        try:
            observed = observe(binary, case, output / "preflight" / case_id,
                               settings, deadline, MANIFEST)
            eligible[case_id] = {key: value for key, value in observed.items() if key != "plans"}
        except ValueError as error:
            excluded[case_id] = str(error)
    write(output / "session.json", {
        "protocol_commit": git("rev-parse", "HEAD").decode().strip(),
        "manifest_sha256": runner.digest(MANIFEST), "build": receipt, "staging": STAGING,
        "schedule": settings, "eligible": eligible, "excluded": excluded,
    })
    comparisons = (
        ("max_by", "spark-off"), ("max_by", "spark-on"),
        ("max_by", "native"), ("max", "native"),
        ("min_by", "spark-off"), ("min_by", "spark-on"),
        ("min_by", "native"), ("min", "native"),
    )
    pairs = {f"window.window.{name}": {} for name, _ in comparisons}
    repeats = {f"window.window.{name}": [] for name, mode in comparisons if mode == "native"}
    for block in range(settings["replicas"]):
        order = comparisons[::-1] if block == 1 else comparisons
        for name, mode in order:
            case_id = "window.window." + name
            if case_id not in eligible:
                continue
            arms = ("noise", "paired") if mode == "native" and block == 1 else (
                ("paired", "noise") if mode == "native" else ("paired",)
            )
            for arm in arms:
                work = output / arm / str(block) / case_id / mode
                observed = one_process(binary, case_id, mode, work, eligible[case_id],
                                       settings, deadline, MANIFEST)
                series = repeats[case_id] if arm == "noise" else pairs[case_id].setdefault(mode, [])
                series.append(observed)
    summary = {}
    for case_id, modes in pairs.items():
        summary[case_id] = {}
        for mode, results in modes.items():
            summary[case_id][mode] = runner.signed_stats(increments(results)) if complete(results) else {
                "states": [r["status"] for r in results],
            }
        native = modes.get("native", [])
        if complete(native) and complete(repeats.get(case_id, [])):
            summary[case_id]["native_floor"] = runner.noise(
                process_medians(native), process_medians(repeats[case_id]),
            )
    write(output / "summary.json", summary)


def complete(results):
    return len(results) == 3 and all(result["status"] == "ok" for result in results)


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("sealed_build", type=Path)
    cli.add_argument("new_output", type=Path)
    opts = cli.parse_args()
    measure(opts.sealed_build.resolve(), opts.new_output.resolve())
