"""Split the cost of planning valid Spark statements in a disposable build."""

import csv
import pathlib
import platform
import statistics
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
LEGACY = HERE.parent / "planning_attribution"
sys.dont_write_bytecode = True
sys.path.insert(0, str(LEGACY))
import assemble
import run

ANALYZER = "vendor/sail/crates/sail-sql-analyzer/src/"
MODES = ("spark-off", "spark-on")
PHASES = ("lexing", "parser_construction", "grammar", "resolution")
STRESS = ("stress/in_1000", "stress/case_200")


def apply(workspace):
    changed = set()

    def replace(name, anchor, replacement):
        path = workspace / name
        source = path.read_text()
        assert source.count(anchor) == 1, (name, anchor)
        path.write_text(source.replace(anchor, replacement))
        changed.add(name)

    for module in ("src/", ANALYZER):
        library = workspace / module / "lib.rs"
        library.write_text(library.read_text() + '\n#[doc(hidden)]\npub mod planning_attribution;\n')
        (workspace / module / "planning_attribution.rs").write_bytes((LEGACY / "events.rs").read_bytes())
        changed.update((module + "lib.rs", module + "planning_attribution.rs"))

    source = ANALYZER + "parser.rs"
    anchor = "fn parse_statements_without_diagnostics(s: &str) -> Option<Vec<Statement>> {"
    replace(source, anchor, anchor + '''
    use crate::planning_attribution::Span;
    let lexing = Span::new("lexing");''')
    anchor = "    let parser = create_scoped_statement_parser"
    replace(source, anchor, '''    drop(lexing);
    let construction = Span::new("parser_construction");
''' + anchor)
    anchor = "    // Only complete, error-free parses bypass the original Rich diagnostic path."
    replace(source, anchor, '''    drop(construction);
    let grammar = Span::new("grammar");
''' + anchor)
    replace(source, "    parser.parse(tokens).into_result().ok()",
            '''    let statements = parser.parse(tokens).into_result().ok();
    drop(grammar);
    drop(parser);
    statements''')

    source = "src/statement_planning.rs"
    anchor = "        let named = resolver"
    replace(source, anchor, '''        let resolution = crate::planning_attribution::Span::new("resolution");
''' + anchor)
    anchor = "        Ok::<_, DataFusionError>(SparkSqlQuery {"
    replace(source, anchor, "        drop(resolution);\n" + anchor)

    driver = "benches/planning_attribution/driver.rs"
    replace(driver, "fn reset_events() {}", '''fn reset_events() {
    delta_arrow_reader_spark_sql::planning_attribution::reset();
    sail_sql_analyzer::planning_attribution::reset();
}''')
    replace(driver, "fn take_events() -> TestResult<Vec<(String, u64)>> { Ok(Vec::new()) }",
            '''fn take_events() -> TestResult<Vec<(String, u64)>> {
    let mut phases = sail_sql_analyzer::planning_attribution::take()?;
    phases.extend(delta_arrow_reader_spark_sql::planning_attribution::take()?);
    Ok(phases)
}''')
    anchor = "    for query in &manifest.queries {"
    replace(driver, anchor, anchor + '''
        if query.group != "paired" && !["stress/in_1000", "stress/case_200"].contains(&query.id.as_str()) {
            continue;
        }''')
    replace(driver, '''    let expected = if trace && configuration.starts_with("spark-") {
        309
    } else {
        30
    };''', "    let expected = 27;")
    return changed


def verify_overlay(receipt):
    workspace_path = assemble.WORKSPACE.relative_to(assemble.REPO)
    sealed = pathlib.Path(receipt["source"]) / workspace_path
    assembly = receipt["assembly"]["assembly"]
    with tempfile.TemporaryDirectory(prefix="dar-1390-logical-") as directory:
        reference = pathlib.Path(directory)
        for name in ("src/lib.rs", "src/statement_planning.rs", ANALYZER + "lib.rs", ANALYZER + "parser.rs"):
            path = reference / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(subprocess.check_output(
                ["git", "show", f"{receipt['baseline']}:{workspace_path / name}"], cwd=assemble.REPO))
        driver = reference / "benches/planning_attribution/driver.rs"
        driver.parent.mkdir(parents=True)
        driver.write_text(assemble.driver_source(observed=False))
        overlay = {name: assemble.digest(reference / name) for name in apply(reference)}
    generated = {"Cargo.toml", "benches/planning_attribution_fixture.rs", "benches/planning_attribution_entry.rs"}
    assert set(assembly) == set(overlay) | generated
    assert all(assembly[name] == digest == assemble.digest(sealed / name)
               for name, digest in overlay.items())
    manifest = "benches/planning/queries.json"
    frozen = run.read(LEGACY / "frozen.json")["files"][manifest]
    assert assemble.digest(sealed / manifest) == assemble.digest(assemble.WORKSPACE / manifest) == frozen


def collect(config, receipt, delta, destination):
    manifest = run.read(assemble.WORKSPACE / "benches/planning/queries.json")
    ids = sorted(q["id"] for q in manifest["queries"] if q["group"] == "paired" or q["id"] in STRESS)
    assert len(ids) == 27
    observations = {(name, mode): [] for name in ids for mode in MODES}
    for replica in range(config["processes_per_path"]):
        for mode in MODES if replica % 2 == 0 else reversed(MODES):
            label = f"{replica+1:02}-{mode}"
            rows = run.invoke(receipt["binary"], ["measure", str(delta), mode, label, "-"],
                              destination, label)
            assert sorted(row["id"] for row in rows) == ids
            assert not (destination / (label + ".stderr")).stat().st_size
            for row in rows:
                assert row["process"] == label and row["configuration"] == mode
                samples = row["events"]
                wall = row["logical_ns"]
                assert len(samples) == len(wall) == 9
                breakdown = []
                for events, total in zip(samples, wall, strict=True):
                    parts = dict(events)
                    assert len(events) == len(PHASES) and set(parts) == set(PHASES)
                    assert all(x > 0 for x in parts.values()) and sum(parts.values()) <= total
                    breakdown.append(dict(parts, other=total-sum(parts.values()), logical=total))
                observations[row["id"], mode].append(
                    {key: statistics.median(sample[key] for sample in breakdown) for key in breakdown[0]})
    with (destination / "phase-medians.csv").open("x", newline="") as output:
        columns = ("id", "configuration", *PHASES, "other", "logical")
        writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for (name, mode), processes in observations.items():
            assert len(processes) == config["processes_per_path"]
            writer.writerow(dict(id=name, configuration=mode,
                                 **{key: statistics.median(row[key] for row in processes)
                                    for key in columns[2:]}))


def main():
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    if len(sys.argv) != 4 or platform.system() != "Linux":
        raise SystemExit("usage on Linux: logical_phase_observer.py BUILD_RECEIPT DELTA NEW_EVIDENCE")
    build, delta, destination = (pathlib.Path(arg).resolve() for arg in sys.argv[1:])
    settings = run.read(HERE / "logical-phase-observation.json")
    assemble.BASELINE = settings["staging_source"]
    receipt = run.verify_build(build)
    assert receipt["mode"] == "observed" and receipt["protocol_commit"] == run.protocol_commit()
    verify_overlay(receipt)
    assert len(MODES) * settings["processes_per_path"] == 10
    fixture = run.read(assemble.WORKSPACE / "benches/reports/planning-1379-metadata.json")["delta_files"]
    assert run.delta_hashes(delta) == fixture
    destination.mkdir()
    run.write(destination / "receipt.json", receipt)
    collect(settings, receipt, delta, destination)
    assert run.delta_hashes(delta) == fixture


if __name__ == "__main__":
    main()
