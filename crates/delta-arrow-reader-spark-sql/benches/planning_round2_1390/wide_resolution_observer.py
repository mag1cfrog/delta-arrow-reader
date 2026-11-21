"""Locate the retained wide query's resolution cost."""

import pathlib
import platform
import statistics
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(HERE))
import logical_phase_observer as logical

assemble, run = logical.assemble, logical.run
SAIL = "vendor/sail/crates/sail-plan/src/"
READ = SAIL + "resolver/query/read.rs"
PROJECT = SAIL + "resolver/query/project.rs"
PHASES = ("scan_setup", "register_fields", "scan_projection", "scan_timezone",
          "select_projection", "select_schema", "resolution")


def apply(workspace):
    changed = logical.apply(workspace)

    def replace(name, anchor, replacement):
        path = workspace / name
        source = path.read_text()
        assert source.count(anchor) == 1, (name, anchor)
        path.write_text(source.replace(anchor, replacement))
        changed.add(name)

    library = workspace / SAIL / "lib.rs"
    library.write_text(library.read_text() + '\n#[doc(hidden)]\npub mod planning_attribution;\n')
    (workspace / SAIL / "planning_attribution.rs").write_bytes((logical.LEGACY / "events.rs").read_bytes())
    changed.update((SAIL + "lib.rs", SAIL + "planning_attribution.rs"))

    def start(name, variable, indent=8):
        return " " * indent + f'let {variable} = crate::planning_attribution::Span::new("{name}");\n'

    anchor = "        let (table_source, metadata) = if let Some(adapter) = &self.file_metadata_adapter {"
    replace(READ, anchor, start("scan_setup", "setup") + anchor)
    anchor = "        let plan = if !has_duplicates {\n"
    replace(READ, anchor, "        drop(setup);\n" + anchor + start("register_fields", "register", 12))
    anchor = "            rename_logical_plan(table_scan, &names)?"
    replace(READ, anchor, "            drop(register);\n"
            + start("scan_projection", "_projection", 12) + anchor)
    anchor = "        self.annotate_with_session_time_zone(plan)"
    replace(READ, anchor, start("scan_timezone", "_timezone") + anchor)

    anchor = "        let schema = input.schema();\n        let expr = self\n            .resolve_projection_expressions"
    replace(PROJECT, anchor, start("select_projection", "_select") + anchor)
    anchor = "            Ok(LogicalPlan::Projection(Projection::try_new("
    replace(PROJECT, anchor, start("select_schema", "_schema", 12) + anchor)

    driver = "benches/planning_attribution/driver.rs"
    anchor = "    sail_sql_analyzer::planning_attribution::reset();"
    replace(driver, anchor, anchor + "\n    sail_plan::planning_attribution::reset();")
    anchor = "    Ok(phases)"
    replace(driver, anchor, "    phases.extend(sail_plan::planning_attribution::take()?);\n" + anchor)
    anchor = '''        if query.group != "paired" && !["stress/in_1000", "stress/case_200"].contains(&query.id.as_str()) {'''
    replace(driver, anchor, '''        if query.id != "stress/wide_400" {''')
    replace(driver, "    let expected = 27;", "    let expected = 1;")
    return changed


def main():
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    if len(sys.argv) != 4 or platform.system() != "Linux":
        raise SystemExit("usage on Linux: wide_resolution_observer.py BUILD_RECEIPT DELTA NEW_EVIDENCE")
    build, delta, destination = (pathlib.Path(arg).resolve() for arg in sys.argv[1:])
    settings = run.read(HERE / "wide-resolution-observation.json")
    assemble.BASELINE = settings["staging_source"]
    receipt = run.verify_build(build)
    assert receipt["mode"] == "observed" and receipt["protocol_commit"] == run.protocol_commit()
    logical.verify_overlay(receipt, apply, (SAIL + "lib.rs", READ, PROJECT))
    fixture = run.read(assemble.WORKSPACE / "benches/reports/planning-1379-metadata.json")["delta_files"]
    assert run.delta_hashes(delta) == fixture
    assert settings["processes_per_path"] == 5
    destination.mkdir()
    run.write(destination / "receipt.json", receipt)
    processes = {mode: [] for mode in logical.MODES}
    for replica in range(settings["processes_per_path"]):
        for mode in logical.MODES if replica % 2 == 0 else reversed(logical.MODES):
            label = f"{replica+1:02}-{mode}"
            (row,) = run.invoke(receipt["binary"], ["measure", str(delta), mode, label, "-"], destination, label)
            assert row["id"] == "stress/wide_400" and row["configuration"] == mode
            assert row["process"] == label and len(row["events"]) == 9
            assert not (destination / (label + ".stderr")).stat().st_size
            specimens = []
            for events in row["events"]:
                spans = dict(events)
                assert all(sum(name == actual for actual, _ in events) == 1 for name in PHASES)
                assert all(spans[name] > 0 for name in PHASES)
                assert spans["select_schema"] <= spans["select_projection"]
                assert sum(spans[name] for name in PHASES[:5]) <= spans["resolution"]
                specimens.append(spans)
            processes[mode].append({name: statistics.median(row[name] for row in specimens) for name in PHASES})
    medians = {mode: {name: statistics.median(row[name] for row in samples) for name in PHASES}
               for mode, samples in processes.items()}
    run.write(destination / "summary.json", dict(process_medians_ns=processes, medians_ns=medians))
    assert run.delta_hashes(delta) == fixture


if __name__ == "__main__":
    main()
