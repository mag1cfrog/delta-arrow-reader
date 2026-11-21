"""Measure where the Spark resolver spends time within the logical phase."""

import csv
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
PLAN = SAIL + "resolver/plan.rs"
SET_OP = SAIL + "resolver/query/set_op.rs"
SINGLE_STATEMENTS = {
    "resolve_query": "        let plan = resolver.resolve_query_plan(query, &mut state).await?;",
    "aggregate_validation": "        Self::validate_aggregate_arguments(&plan)?;",
    "percentiles": "        let plan = Self::lower_distinct_percentiles(plan, &mut state)?;",
    "set_lowering": "        let plan = resolver.lower_resolved_set_operations(plan, &mut state)?;",
    "aggregate_ordering": "        let plan = Self::preserve_order_sensitive_aggregate_sorts(plan)?;",
    "output_fields": "        let fields = Self::get_field_names(plan.schema(), &state)?;",
}
PARTS = ("setup", *SINGLE_STATEMENTS, "alias_validation")


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

    def span(name):
        return f'        let timing = crate::planning_attribution::Span::new("{name}");\n'

    end = "        drop(timing);\n"
    for name, statement in SINGLE_STATEMENTS.items():
        replace(PLAN, statement + "\n", span(name) + statement + "\n" + end)

    anchor = "        let resolver = PlanResolver {"
    replace(PLAN, anchor, span("setup") + anchor)
    anchor = "        let mut state = PlanResolverState::new();\n"
    replace(PLAN, anchor, anchor + end)
    anchor = "        let mut pending_alias = false;"
    replace(PLAN, anchor, span("alias_validation") + anchor)
    anchor = span("output_fields")
    replace(PLAN, anchor, end + anchor)

    anchor = "        Ok(pending::lower_query_sets(&plan, self.ctx.state(), state)?)"
    replace(SET_OP, anchor, span("set_session_snapshot") + "        let session = self.ctx.state();\n"
            + end + "        Ok(pending::lower_query_sets(&plan, session, state)?)")
    driver = "benches/planning_attribution/driver.rs"
    anchor = "    sail_sql_analyzer::planning_attribution::reset();"
    replace(driver, anchor, anchor + "\n    sail_plan::planning_attribution::reset();")
    anchor = "    Ok(phases)"
    replace(driver, anchor, "    phases.extend(sail_plan::planning_attribution::take()?);\n" + anchor)
    return changed


def main():
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    if len(sys.argv) != 4 or platform.system() != "Linux":
        raise SystemExit("usage on Linux: resolver_observer.py BUILD_RECEIPT DELTA NEW_EVIDENCE")
    build, delta, destination = (pathlib.Path(arg).resolve() for arg in sys.argv[1:])
    settings = run.read(HERE / "logical-phase-observation.json")
    assemble.BASELINE = settings["staging_source"]
    receipt = run.verify_build(build)
    assert receipt["mode"] == "observed" and receipt["protocol_commit"] == run.protocol_commit()
    logical.verify_overlay(receipt, apply, (SAIL + "lib.rs", PLAN, SET_OP))
    fixture = run.read(assemble.WORKSPACE / "benches/reports/planning-1379-metadata.json")["delta_files"]
    assert run.delta_hashes(delta) == fixture
    assert settings["processes_per_path"] == 5
    query_manifest = run.read(assemble.WORKSPACE / "benches/planning/queries.json")["queries"]
    ids = sorted(q["id"] for q in query_manifest if q["group"] == "paired" or q["id"] in logical.STRESS)
    assert len(ids) == 27
    destination.mkdir()
    run.write(destination / "receipt.json", receipt)
    columns = (*PARTS, "set_session_snapshot", "resolution")
    medians = {}
    for replica in range(settings["processes_per_path"]):
        for mode in logical.MODES if replica % 2 == 0 else reversed(logical.MODES):
            label = f"{replica+1:02}-{mode}"
            rows = run.invoke(receipt["binary"], ["measure", str(delta), mode, label, "-"], destination, label)
            assert sorted(row["id"] for row in rows) == ids
            assert not (destination / (label + ".stderr")).stat().st_size
            for row in rows:
                assert row["configuration"] == mode and row["process"] == label and len(row["events"]) == 9
                samples = []
                for events in row["events"]:
                    spans = dict(events)
                    assert all(name in spans for name in (*logical.PHASES, *columns))
                    assert all(sum(name == actual for actual, _ in events) == 1 for name in PARTS)
                    assert sum(spans[name] for name in PARTS) <= spans["resolution"]
                    # A table expression can take its own session snapshot inside resolve_query.
                    assert spans["set_session_snapshot"] <= spans["set_lowering"]
                    samples.append(spans)
                medians.setdefault((row["id"], mode), []).append({
                    name: statistics.median(spans[name] for spans in samples) for name in columns})
    with (destination / "resolver-medians.csv").open("x", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=("id", "configuration", *columns), lineterminator="\n")
        writer.writeheader()
        for (query, mode), processes in sorted(medians.items()):
            assert len(processes) == settings["processes_per_path"]
            writer.writerow(dict(id=query, configuration=mode, **{
                name: statistics.median(row[name] for row in processes) for name in columns}))
    assert run.delta_hashes(delta) == fixture


if __name__ == "__main__":
    main()
