"""Build and compare the two authorized native macOS release pairs."""

import argparse
import json
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "functions_1516"))
import release
from release import run_pair, replace
import compare_candidate

compare = compare_candidate.compare
runner = compare.runner

QOS = """
fn prefer_performance_cores() {
    // pthread/qos.h on macOS.
    const USER_INTERACTIVE: u32 = 0x21;
    unsafe extern "C" {
        fn pthread_set_qos_class_self_np(class: u32, relative_priority: i32) -> i32;
        fn qos_class_self() -> u32;
    }
    // Set and verify QoS before executing any measurement.
    unsafe {
        assert_eq!(pthread_set_qos_class_self_np(USER_INTERACTIVE, 0), 0);
        assert_eq!(qos_class_self(), USER_INTERACTIVE);
    }
}

"""


def macos_changes(base_changes, workspace, instrumented):
    files = dict(base_changes(workspace, instrumented))
    path = workspace / "benches/functions.rs"
    source = files[path]
    start = source.index('    let status = fs::read_to_string("/proc/self/status")?;')
    end = source.index("    Ok(", start)
    source = source[:start] + "    let peak_rss: Option<u64> = None;\n" + source[end:]
    source = replace(source, "fn main() -> Result {",
                     QOS + "fn main() -> Result {\n    prefer_performance_cores();")
    files[path] = replace(source, "        .worker_threads(1)\n",
                          "        .worker_threads(1)\n        .on_thread_start(prefer_performance_cores)\n")
    return files.items()


def build(settings, arm, worktree, target, output):
    pins = runner.read(settings)
    recipe = release.restricted_changes if pins.get("restricted_input_rows") else run_pair.changes
    with patch.object(run_pair, "changes",
                      lambda ws, instrumented: macos_changes(recipe, ws, instrumented)):
        run_pair.build(pins[f"{arm}_source"], worktree, target, output)
    receipt_file = output / "receipt.json"
    receipt = runner.read(receipt_file)
    receipt["macos_recipe_sha256"] = runner.digest(Path(__file__))
    receipt_file.write_text(json.dumps(receipt, indent=2) + "\n")


def command(*args):
    return subprocess.check_output(args, text=True, timeout=15).strip()


def check_host():
    model = command("/usr/sbin/sysctl", "-n", "hw.model")
    chip = command("/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string")
    macos = command("/usr/bin/sw_vers", "-productVersion")
    build = command("/usr/bin/sw_vers", "-buildVersion")
    power = command("/usr/bin/pmset", "-g", "batt").splitlines()[0]
    modes = {parts[0]: parts[1] for line in command("/usr/bin/pmset", "-g").splitlines()
             if (parts := line.split()) and parts[0] in ("lowpowermode", "powermode")}
    if (model, chip, macos, build) != ("Mac14,6", "Apple M2 Max", "26.7.3", "25G342"):
        raise ValueError("The host differs from the machine in the committed protocol")
    if "'AC Power'" not in power or modes.get("powermode") != "2" or modes.get("lowpowermode", "0") != "0":
        raise ValueError("The host must use mains power and High Power mode")
    running = {Path(line.strip()).name for line in command("/bin/ps", "-A", "-o", "comm=").splitlines()}
    builds = running & {"cargo", "rustc", "sccache", "clang", "clang++", "swiftc",
                        "xcodebuild", "ld", "ld64", "functions", "limactl"}
    if builds:
        raise ValueError(f"Build, benchmark or VM processes are active: {sorted(builds)}")
    return dict(model=model, chip=chip, macos=macos, build=build, power="mains, High Power")


def run_without_linux_controls(args, **kwargs):
    # Only the frozen invoke() subprocess is adapted.
    assert args[:2] == ["taskset", "-c"]
    kwargs.pop("preexec_fn")
    return subprocess.run(args[3:], **kwargs)


def summarize(pairs, repeats):
    if any(len(arm) != 3 or any(row["status"] != "ok" for row in arm)
           for arm in (*pairs.values(), *repeats.values())):
        return {"status": "incomplete"}
    before, after = (compare.process_medians(pairs[arm]) for arm in compare.ARMS)
    repeat_a, repeat_b = (compare.process_medians(repeats[arm]) for arm in ("a", "b"))
    floor = runner.noise(repeat_a, repeat_b)
    deltas = [candidate - staging for staging, candidate in zip(before, after)]
    paired = statistics.median(deltas)
    return {
        "status": "ok",
        **{arm: compare.summarize(pairs[arm]) for arm in compare.ARMS},
        "paired_ns_per_row": deltas,
        "paired_median_ns_per_row": paired,
        "staging_repeat_medians": {"a": repeat_a, "b": repeat_b},
        "aa_floor_ns_per_row": floor,
        "decision": "faster" if paired < -floor else "slower" if paired > floor else "within floor",
    }


def measure(settings_path, staging, candidate, output):
    assert not runner.git("status", "--porcelain")
    commit = runner.git("rev-parse", "HEAD")
    assert subprocess.check_output(
        ["git", "show", f"HEAD:{settings_path.relative_to(run_pair.REPO)}"],
        cwd=run_pair.REPO,
    ) == settings_path.read_bytes()
    pins = runner.read(settings_path)
    manifest = compare.statement_manifest(settings_path, pins)
    cases = compare.selected_cases(pins, manifest)
    schedule = compare_candidate.schedule()
    assert schedule["replicas"] == 3
    builds = dict(staging=staging, candidate=candidate)
    receipts = {}
    for arm, build_dir in builds.items():
        receipt = runner.read(build_dir / "receipt.json")
        assert receipt["source"] == pins[f"{arm}_source"]
        assert receipt["protocol_commit"] == commit
        assert receipt["recipe_sha256"] == runner.digest(Path(run_pair.__file__))
        assert receipt["macos_recipe_sha256"] == runner.digest(Path(__file__))
        assert receipt["sha256"] == runner.digest(build_dir / "functions")
        assert receipt["patch_sha256"] == runner.digest(build_dir / "patch.json")
        receipts[arm] = receipt
    assert receipts["staging"]["patch_sha256"] == receipts["candidate"]["patch_sha256"]
    binaries = {arm: build / "functions" for arm, build in builds.items()}

    host = check_host()
    output.mkdir()
    compare_candidate.write(output / "session.json", dict(
        host=host, commit=commit, pins=pins, receipts=receipts,
        schedule={k: v for k, v in schedule.items() if k not in ("cpus", "address_space_bytes")},
    ))
    macos_subprocess = SimpleNamespace(run=run_without_linux_controls, TimeoutExpired=subprocess.TimeoutExpired)
    with patch.object(runner, "subprocess", macos_subprocess):
        if restricted_rows := pins.get("restricted_input_rows"):
            compare_candidate.verify_full(builds, pins["case_ids"], output / "full-input")
            schedule = dict(schedule, input_rows=restricted_rows)

        deadline = time.monotonic() + pins["overall_timeout_seconds"]
        eligible, excluded = compare.preflight(
            binaries, cases, output / "preflight", schedule, deadline,
            pins.get("spark_plan_contains", {}), manifest,
        )
        compare_candidate.write(output / "preflight.json", dict(eligible=eligible, excluded=excluded))
        cells = [(case_id, mode) for case_id, expected in sorted(eligible.items())
                 for mode in pins["timed_modes"] if mode in expected["modes"]]
        pairs = {cell: {arm: [] for arm in compare.ARMS} for cell in cells}
        repeats = {cell: {arm: [] for arm in ("a", "b")} for cell in cells}
        for block in range(schedule["replicas"]):
            order = -1 if block == 1 else 1
            for case_id, mode in cells[::order]:
                cell = (case_id, mode)
                expected = eligible[case_id]
                for scope, series, arms in (
                    ("paired", pairs, compare.ARMS[::order]),
                    ("noise", repeats, schedule["noise_orders"][block]),
                ):
                    for arm in arms:
                        check_host()
                        binary = binaries[arm] if scope == "paired" else binaries["staging"]
                        work = output / scope / str(block) / case_id / mode / arm
                        series[cell][arm].append(compare.timed_case(
                            binary, case_id, mode, work, expected, schedule, deadline, manifest,
                        ))
        check_host()

    report = {f"{case_id}/{mode}": summarize(pairs[case_id, mode], repeats[case_id, mode])
              for case_id, mode in cells}
    compare_candidate.write(output / "summary.json", report)


def main():
    if sys.platform != "darwin" or platform.machine() != "arm64" or not __debug__:
        raise SystemExit("Run with Python validation enabled on native Apple Silicon macOS")
    cli = argparse.ArgumentParser(description=__doc__)
    ops = cli.add_subparsers(dest="operation", required=True)
    prepare = ops.add_parser("build")
    prepare.add_argument("arm", choices=compare.ARMS)
    timing = ops.add_parser("measure")
    for parser, names in (
        (prepare, ("settings", "worktree", "target", "output")),
        (timing, ("settings_path", "staging", "candidate", "output")),
    ):
        for name in names:
            parser.add_argument(name, type=lambda path: Path(path).resolve())
    args = vars(cli.parse_args())
    if args.pop("operation") == "build":
        build(**args)
    else:
        output = args["output"]
        try:
            measure(**args)
        finally:
            if (output / "paired").is_dir() or (output / "noise").is_dir():
                run_pair.archive(output / "samples.jsonl.gz", output)


if __name__ == "__main__":
    main()
