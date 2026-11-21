"""Observe the retained Spark query planner on the wide statement."""

import pathlib
import platform
import statistics
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
LEGACY = HERE.parent / "planning_attribution"
sys.dont_write_bytecode = True
sys.path.insert(0, str(LEGACY))
import assemble
import run

PLANNER = "vendor/sail/crates/sail-plan/src"


def apply(workspace):
    changed = set()

    def replace(name, anchor, replacement):
        path = workspace / name
        source = path.read_text()
        assert source.count(anchor) == 1, (name, anchor)
        path.write_text(source.replace(anchor, replacement))
        changed.add(name)

    library = workspace / PLANNER / "lib.rs"
    library.write_text(library.read_text() + '\n#[doc(hidden)]\npub mod planning_attribution;\n')
    events = workspace / PLANNER / "planning_attribution.rs"
    events.write_bytes((LEGACY / "events.rs").read_bytes())
    changed.update((str(library.relative_to(workspace)), str(events.relative_to(workspace))))
    planner = PLANNER + "/physical_plan/mod.rs"
    anchor = "        let mut state = Cow::Borrowed(session_state);"
    replace(planner, anchor,
            '        let _planner = crate::planning_attribution::Span::new("spark_planner");\n' + anchor)
    anchor = "        let runtime_plan = if spark_semantics {\n            align_result_metadata_for_execution(runtime_plan)?"
    replace(planner, anchor,
            '        let align = crate::planning_attribution::Span::new("align_result_metadata");\n' + anchor)
    anchor = "        let physical =\n            DefaultPhysicalPlanner::with_extension_planners"
    replace(planner, anchor, '        drop(align);\n'
            '        let default = crate::planning_attribution::Span::new("default_physical_planner");\n' + anchor)
    anchor = "        seeded_rand::bind_seeded_rand_streams(physical)"
    replace(planner, anchor, "        drop(default);\n" + anchor)

    driver = "benches/planning_attribution/driver.rs"
    replace(driver, "fn reset_events() {}", "fn reset_events() { sail_plan::planning_attribution::reset(); }")
    replace(driver, "fn take_events() -> TestResult<Vec<(String, u64)>> { Ok(Vec::new()) }",
            "fn take_events() -> TestResult<Vec<(String, u64)>> { Ok(sail_plan::planning_attribution::take()?) }")
    replace(driver, '|| !["trace", "measure"].contains(&args[0].as_str())',
            '|| !["measure", "profile"].contains(&args[0].as_str())')
    replace(driver, '    let trace = args[0] == "trace";',
            '    let trace = false;\n    let samples = if args[0] == "profile" { 240 } else { SAMPLES };')
    replace(driver, "    for query in &manifest.queries {",
            '    for query in &manifest.queries {\n        if query.id != "stress/wide_400" { continue; }')
    replace(driver, "for sample in 0..WARMUPS + SAMPLES", "for sample in 0..WARMUPS + samples")
    replace(driver, '    let expected = if trace && configuration.starts_with("spark-") {\n'
            '        309\n    } else {\n        30\n    };', '    let expected = 1;')
    return changed


def observe(config, receipt, delta, destination):
    modes = ("native-configured", "spark-off", "spark-on")
    rows = {mode: [] for mode in modes}
    for replica in range(config["processes_per_path"]):
        for mode in modes[replica % len(modes):] + modes[:replica % len(modes)]:
            label = f"{replica + 1:02}-{mode}"
            captured = run.invoke(receipt["binary"], ["measure", str(delta), mode, label, "-"],
                                  destination, label)
            assert len(captured) == 1
            sample = captured[0]
            assert sample["configuration"] == mode and sample["id"] == "stress/wide_400"
            assert not (destination / (label + ".stderr")).stat().st_size
            assert len(sample["events"]) == 9
            values = [dict(events) for events in sample["events"]]
            keys = {"spark_planner", "align_result_metadata", "default_physical_planner"}
            assert all(set(value) == keys for value in values)
            assert all(value["spark_planner"] >= value["align_result_metadata"] + value["default_physical_planner"]
                       for value in values)
            rows[mode].append({key: statistics.median(value[key] for value in values) for key in sorted(keys)})
    summary = {mode: {key: statistics.median(process[key] for process in values)
                      for key in values[0]} for mode, values in rows.items()}
    run.write(destination / "summary.json", {"process_medians_ns": rows, "medians_ns": summary})


def profile(receipt, delta, destination):
    binary = receipt["binary"]
    command = ["perf", "record", "-e", "cpu-clock:u", "--user-callchains",
               "--call-graph", "dwarf,8192", "-F", "199", "--max-size", "128M",
               "-o", "perf.data", "--", "taskset", "-c", "2", binary,
               "profile", str(delta), "spark-off", "wide-profile", "-"]
    run.write(destination / "profile-command.json", command)
    with (destination / "profile.jsonl").open("x") as stdout, (destination / "profile.stderr").open("x") as stderr:
        subprocess.run(command, cwd=destination, stdout=stdout, stderr=stderr, check=True, timeout=120)
    row = run.read(destination / "profile.jsonl")
    assert row["id"] == "stress/wide_400" and row["configuration"] == "spark-off"
    assert len(row["events"]) == 240
    with (destination / "profile-stacks.txt").open("x") as stdout:
        subprocess.run(["perf", "script", "-i", "perf.data", "--inline", "--full-source-path",
                        "-F", "+srcline"], cwd=destination, stdout=stdout, check=True, timeout=120)


def main():
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    if len(sys.argv) != 4 or platform.system() != "Linux":
        raise SystemExit("usage on Linux: wide_planner_observer.py BUILD_RECEIPT DELTA NEW_EVIDENCE")
    build, delta, destination = (pathlib.Path(arg).resolve() for arg in sys.argv[1:])
    config = run.read(HERE / "wide-planner-observation.json")
    assemble.BASELINE = config["staging_source"]
    receipt = run.verify_build(build)
    assert receipt["mode"] == "observed" and receipt["protocol_commit"] == run.protocol_commit()
    assert 3 * config["processes_per_path"] == 15
    fixture = run.read(assemble.WORKSPACE / "benches/reports/planning-1379-metadata.json")["delta_files"]
    assert run.delta_hashes(delta) == fixture
    destination.mkdir()
    run.write(destination / "receipt.json", receipt)
    observe(config, receipt, delta, destination)
    profile(receipt, delta, destination)
    assert run.delta_hashes(delta) == fixture


if __name__ == "__main__":
    main()
