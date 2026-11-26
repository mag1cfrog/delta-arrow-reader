"""Build and run the disposable #1390 phase observer on macOS."""

import argparse
import json
from pathlib import Path
import runpy
import sys
import tarfile
import tempfile
import tomllib
from unittest.mock import patch

sys.dont_write_bytecode = True
import planning
import build as mac_build
from build import digest, git, replace
from native import BENCHES, REPO, archive, call, emit, session
from run import PROTOCOL, source_receipt, trial

LEGACY = BENCHES / "planning_attribution"
ROUND3 = BENCHES / "planning_round3_1390"
PLANNER = "vendor/sail/crates/sail-plan/src/physical_plan/mod.rs"
PARSER = "vendor/sail/crates/sail-sql-analyzer/src/parser.rs"
FRAME = "let frame = DataFrame::new(state, planning_input);"
OLD_FRAME = "let frame = DataFrame::new(state, self.logical_plan.clone());"


def snapshot_hooks(workspace, driver):
    assembler = runpy.run_path(str(LEGACY / "assemble.py"))
    library = workspace / "src/lib.rs"
    original = library.read_bytes()
    try:
        library.write_text(replace(original.decode(), FRAME, OLD_FRAME))
        sources, modules, _ = assembler["patch_sources"](workspace)
    finally:
        library.write_bytes(original)
    sources["src/lib.rs"] = replace(sources["src/lib.rs"], OLD_FRAME, FRAME)
    for path in modules:
        name = Path(path).with_name("planning_attribution.rs")
        sources[str(name)] = (LEGACY / "events.rs").read_text()
    parser = sources[PARSER]
    start = "fn parse_statements_without_diagnostics(s: &str) -> Option<Vec<Statement>> {"
    parser = replace(parser, start, start + '\n    use crate::planning_attribution::Span;\n'
                     '    let lexing = Span::new("logical.parsing.fast_lexing");')
    anchor = "    let parser = create_scoped_statement_parser"
    parser = replace(parser, anchor, '    drop(lexing);\n'
                     '    let construction = Span::new("logical.parsing.fast_construction");\n' + anchor)
    anchor = "    // Only complete, error-free parses bypass"
    parser = replace(parser, anchor, '    drop(construction);\n'
                     '    let grammar = Span::new("logical.parsing.fast_grammar");\n' + anchor)
    anchor = "    parser.parse(tokens).into_result().ok()"
    sources[PARSER] = replace(parser, anchor, '    let result = parser.parse(tokens).into_result().ok();\n'
                              '    drop(grammar);\n    result')
    empty = assembler["driver_source"](False)
    observed = assembler["driver_source"](True)
    events_index = "fn schema_snapshot("
    empty_events = empty.split(events_index, 1)[0].split("fn reset_events()", 1)[1]
    measured_events = observed.split(events_index, 1)[0].split("fn reset_events()", 1)[1]
    driver = replace(driver, empty_events, measured_events)
    driver = replace(driver, "    let started = Instant::now();",
                     "    let logical_events;\n    let started = Instant::now();")
    # Keep event extraction outside the recorded phases.
    assert driver.count("        let second = Instant::now();") == 2
    driver = driver.replace("        let second = Instant::now();",
                            "        logical_events = take_events()?;\n"
                            "        reset_events();\n        let second = Instant::now();")
    driver = replace(driver, "    let events = take_events()?;",
                     '    let mut events = logical_events.into_iter()\n'
                     '        .map(|(key, value)| (format!("logical/{key}"), value)).collect::<Vec<_>>();\n'
                     '    events.extend(take_events()?.into_iter()\n'
                     '        .map(|(key, value)| (format!("physical/{key}"), value)));')
    return sources, driver


def observed_registry(workspace, scratch, driver, planner):
    lock = tomllib.loads((workspace / "Cargo.lock").read_text())
    datafusion, = (entry for entry in lock["package"] if entry["name"] == "datafusion")
    name = f"datafusion-{datafusion['version']}"
    crate, = (Path.home() / ".cargo/registry/cache").glob(f"*/{name}.crate")
    assert digest(crate) == datafusion["checksum"]
    with tarfile.open(crate) as archive_file:
        archive_file.extractall(scratch, filter="data")
    package = scratch / name
    driver_path, planner_path = scratch / "driver.rs", scratch / "planner.rs"
    driver_path.write_text(driver)
    planner_path.write_text(planner)
    observe = runpy.run_path(str(ROUND3 / "observe_default_planner.py"))["observe"]
    observe(package, driver_path, LEGACY / "events.rs", planner_path)
    path = package / "src/execution/session_state.rs"
    source = path.read_text()
    for function, anchor, phase in (
        ("pub fn sql_to_statement(", "let dialect = dialect_from_str(dialect).ok_or_else(|| {", "parsing"),
        ("pub async fn statement_to_plan(", "let references = self.resolve_table_references(&statement)?;", "resolution"),
    ):
        start = source.index(function)
        end = source.index("\n    }\n", start)
        span = f'let _observer = crate::planning_attribution::Span::new("native.{phase}");'
        body = replace(source[start:end], anchor, f"{span}\n        {anchor}")
        source = source[:start] + body + source[end:]
    path.write_text(source)
    registry = {"crate_sha256": datafusion["checksum"], "observer_files": {
        str(path.relative_to(package)): digest(path)
        for path in (path, package / "src/lib.rs", package / "src/planning_attribution.rs",
                     package / "src/physical_planner.rs")
    }}
    return package, driver_path.read_text(), planner_path.read_text(), registry


def build(source, worktree, target, output):
    base_overlay = mac_build.overlay
    with tempfile.TemporaryDirectory(prefix="dar-planning-observer-", dir=output.parent) as scratch:
        registry = {}

        def overlay(workspace, targets):
            rust = base_overlay(workspace, targets)
            driver = workspace / "benches/planning_attribution/driver.rs"
            sources, observed = snapshot_hooks(workspace, rust[driver])
            assert not set(rust) & {workspace / name for name in sources}
            package, rust[driver], sources[PLANNER], details = observed_registry(
                workspace, Path(scratch), observed, sources[PLANNER])
            registry.update(details)
            configuration = workspace / ".cargo/config.toml"
            assert not configuration.exists()
            configuration.parent.mkdir(exist_ok=True)
            return {**rust, **{workspace / name: value for name, value in sources.items()},
                    configuration: f"paths = [{json.dumps(str(package))}]\n"}

        with patch.object(mac_build, "overlay", overlay):
            mac_build.build(source, worktree, target, output, (planning.SUITE,))
    receipt = output / "receipt.json"
    data = planning.cells.read(receipt)
    data.update(observer_sha256=digest(Path(__file__)), registry=registry)
    planning.cells.write(receipt, data)


def preflight(args, canonical, observed):
    args.output.mkdir()
    fixture = args.output / "delta"
    planning.cells.prepare_planning(args.canonical, fixture)
    traces = {}
    for mode in planning.MODES:
        before = args.output / "traces" / mode / "canonical"
        after = args.output / "traces" / mode / "observed"
        status, rows = call(args.canonical / planning.SUITE, ["trace", fixture, mode, "canonical", "-"], before, 180)
        expected = 309 if mode.startswith("spark-") else 30
        assert status == "ok" and len(rows) == expected, (mode, "canonical", status)
        reference = before / "stdout.jsonl"
        status, rows = call(args.binaries / planning.SUITE, ["trace", fixture, mode, "observed", reference], after, 180)
        assert status == "ok" and len(rows) == expected, (mode, "observed", status)
        traces[mode] = {"rows": expected, "canonical_sha256": digest(reference),
                        "observed_sha256": digest(after / "stdout.jsonl")}
    proof = {"canonical": canonical, "observed": observed, "traces": traces,
             "protocol_sha256": digest(args.protocol)}
    planning.cells.write(args.output / "proof.json", proof)


def observe(args, protocol, canonical, observed):
    proof = planning.cells.read(args.proof)
    assert proof["canonical"] == canonical and proof["observed"] == observed
    assert proof["protocol_sha256"] == digest(args.protocol)
    assert sum(t["rows"] for t in proof["traces"].values()) == 678
    noise = planning.cells.read(planning.NOISE)
    assert digest(planning.NOISE) == protocol["schedule_sha256"]
    assert noise["blocks"] == protocol["blocks"] == 9
    assert noise["samples"][planning.SUITE] == protocol["samples_per_process"] == 9
    args.candidate = args.fixture = None
    with session(args.protocol) as binding:
        args.output.mkdir()
        fixture = args.output / "delta"
        planning.cells.prepare_planning(args.canonical, fixture)
        with archive(args.output / "samples.jsonl.gz") as stream:
            emit(stream, {"kind": "session", **binding, "observed": observed, "canonical": canonical,
                          "protocol_sha256": digest(args.protocol), "proof_sha256": digest(args.proof)})
            settings = {**planning.cells.read(PROTOCOL), "cell_session_seconds": protocol["max_seconds"]}
            result = trial(planning.manifest(protocol), args, settings, noise["samples"], stream,
                           {"planning_delta": fixture}, "attribution", aa_only=True)
    planning.cells.write(args.output / "session.json", result)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest="action", required=True)
    builder = commands.add_parser("build")
    builder.add_argument("source")
    for arg in ("worktree", "target", "output"):
        builder.add_argument(arg, type=lambda p: Path(p).resolve())
    for action in ("preflight", "observe"):
        parser = commands.add_parser(action)
        for arg in ("protocol", "canonical", "binaries", "output"):
            parser.add_argument(arg, type=lambda p: Path(p).resolve())
        if action == "observe":
            parser.add_argument("proof", type=lambda p: Path(p).resolve())
    args = cli.parse_args()
    assert not args.output.is_relative_to(REPO)
    if args.action == "build":
        build(args.source, args.worktree, args.target, args.output)
        return
    protocol = planning.cells.read(args.protocol)
    assert git(REPO, "show", f"HEAD:{args.protocol.relative_to(REPO)}") == args.protocol.read_text().strip()
    canonical, observed = (source_receipt(path) for path in (args.canonical, args.binaries))
    assert canonical["source"] == protocol["canonical"]
    assert observed["source"] == protocol["staging"]
    assert observed["observer_sha256"] == digest(Path(__file__))
    if args.action == "preflight":
        preflight(args, canonical, observed)
    else:
        observe(args, protocol, canonical, observed)


if __name__ == "__main__":
    main()
