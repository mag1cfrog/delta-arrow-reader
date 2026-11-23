"""Build a sealed planning driver in a clean scratch worktree."""
import argparse
import json
from pathlib import Path
import os
import shutil
import subprocess
import sys
import tarfile
import tomllib

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE.parent / name) for name in ("planning_attribution", "planning_round2_1390")]
import assemble
import inventory
import run
from observe_default_planner import observe

SPARK_PATH = assemble.WORKSPACE.relative_to(assemble.REPO)
FRAME = "let frame = DataFrame::new(state, planning_input);"
OLD_FRAME = "let frame = DataFrame::new(state, self.logical_plan.clone());"


def verify_compiler_inputs(source, protocol):
    current_tools = SPARK_PATH / "benches/planning_round3_1390"
    reports = SPARK_PATH / "benches/reports"
    subprocess.run(["git", "diff", "--exit-code", source, protocol, "--", ".",
                    f":!{current_tools}", f":!{reports}/planning-1390-round3-*"],
                   cwd=assemble.REPO, check=True, capture_output=True)


def stages(workspace):
    # Reuse the frozen timing hooks with the current composed planning input.
    lib = workspace / "src/lib.rs"
    source = lib.read_bytes()
    assert source.count(FRAME.encode()) == 1
    try:
        lib.write_bytes(source.replace(FRAME.encode(), OLD_FRAME.encode()))
        changed, modules, anchors = assemble.patch_sources(workspace)
    finally:
        lib.write_bytes(source)
    assert changed["src/lib.rs"].count(OLD_FRAME) == 1
    changed["src/lib.rs"] = changed["src/lib.rs"].replace(OLD_FRAME, FRAME)
    for path in modules:
        module = str(Path(path).parent / "planning_attribution.rs")
        changed[module] = (assemble.HERE / "events.rs").read_text()
    driver = assemble.driver_source(True)
    assert driver.count("    for query in &manifest.queries {") == 1
    driver = driver.replace("    for query in &manifest.queries {",
                            '    for query in &manifest.queries {\n        if query.id != "stress/wide_400" { continue; }')
    for anchor, replacement in (
        ('!["trace", "measure"].contains', '!["trace", "measure", "profile"].contains'),
        ('    let trace = args[0] == "trace";',
         '    let trace = args[0] == "trace";\n    let samples = if args[0] == "profile" { 240 } else { SAMPLES };'),
        ("0..WARMUPS + SAMPLES", "0..WARMUPS + samples"),
    ):
        assert driver.count(anchor) == 1
        driver = driver.replace(anchor, replacement)
    before = """    let expected = if trace && configuration.starts_with("spark-") {
        309
    } else {
        30
    };"""
    assert driver.count(before) == 1
    changed["benches/planning_attribution/driver.rs"] = driver.replace(before, "    let expected = 1;")
    for name, contents in changed.items():
        (workspace / name).write_text(contents)
    return changed.keys(), anchors


def observe_registry_planner(assembled, workspace):
    lock = tomllib.loads((workspace / "Cargo.lock").read_text())
    entry, = [entry for entry in lock["package"] if entry["name"] == "datafusion"]
    name = f"datafusion-{entry['version']}"
    archive, = (Path.home() / ".cargo/registry/cache").glob(f"*/{name}.crate")
    assert assemble.digest(archive) == entry["checksum"]
    with tarfile.open(archive) as source:
        source.extractall(assembled, filter="data")
    package = assembled / name
    observe(package, workspace / "benches/planning_attribution/driver.rs",
            assemble.HERE / "events.rs", workspace / (assemble.SAIL + "sail-plan/src/physical_plan/mod.rs"))
    return package, {"name": name, "registry_sha256": entry["checksum"],
                     "observed_source_sha256": run.tree_hash(package)}


def build(kind, source_commit, worktree, target, output):
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    assert worktree != assemble.REPO and not target.is_relative_to(worktree)
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=worktree)
    protocol = run.protocol_commit()
    # The assembled protocol and the compiled product must use these same inputs.
    verify_compiler_inputs(source_commit, protocol)
    assemble.BASELINE = source_commit
    output.mkdir()
    assembled = output / "sealed-source"
    description = assemble.assemble(assembled, observed=False)
    staged = assembled / SPARK_PATH
    override = None
    if kind in ("stages", "planner-stages"):
        names, description["anchors"] = stages(staged)
        description["observed"] = True
        if kind == "planner-stages":
            override, description["registry_observer"] = observe_registry_planner(assembled, staged)
    elif kind == "inventory":
        names = inventory.patch(staged, "stress/wide_400")
    else:
        names = ()
    description["assembly"].update({name: assemble.digest(staged / name) for name in sorted(names)})
    description["tree_sha256"] = run.tree_hash(assembled)
    run.write(assembled / "planning-1390-assembly.json", description)
    subprocess.run(["git", "switch", "--detach", source_commit], cwd=worktree, check=True)
    workspace = worktree / SPARK_PATH
    command = ["cargo", "+1.96.0", "build", "--offline", "--locked", "--release", "--jobs", "2",
               "--config", "profile.release.debug=1", "--target-dir", str(target),
               "--bench", "planning_attribution", "--message-format=json"]
    if override is not None:
        command.extend(["--config", "paths=" + json.dumps([str(override)])])
    settings = {"PATH": os.environ["PATH"], "HOME": str(Path.home()), "TZ": "UTC"}
    original = {}
    try:
        for name, expected in description["assembly"].items():
            path = workspace / name
            original[path] = path.read_bytes() if path.exists() else None
            assert assemble.digest(staged / name) == expected
            path.write_bytes((staged / name).read_bytes())
        with subprocess.Popen(command, cwd=workspace, env=settings, stdout=subprocess.PIPE, text=True) as process:
            artifacts = [row["executable"] for row in map(json.loads, process.stdout)
                         if row["reason"] == "compiler-artifact"
                         and row["target"]["name"] == "planning_attribution" and row.get("executable")]
            assert process.wait() == 0 and len(artifacts) == 1
    finally:
        for path, contents in original.items():
            if contents is None:
                path.unlink()
            else:
                path.write_bytes(contents)
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=worktree)
    assert description["tree_sha256"] == run.tree_hash(assembled)
    output_binary = output / "planning_attribution"
    shutil.copy2(artifacts[0], output_binary)
    toolchain = subprocess.check_output(["rustc", "+1.96.0", "--version"], env=settings, text=True).strip()
    assert toolchain.startswith("rustc 1.96.0 ")
    run.write(output / "receipt.json", {
        "source_commit": source_commit, "protocol_commit": protocol, "kind": kind,
        "binary_sha256": assemble.digest(output_binary), "assembly": description,
        "toolchain": toolchain,
    })
    print(kind, source_commit, assemble.digest(output_binary))


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("kind", choices=("canonical", "inventory", "stages", "planner-stages"))
    cli.add_argument("source_commit")
    for name in ("worktree", "target", "output"):
        cli.add_argument(name, type=lambda value: Path(value).resolve())
    build(**vars(cli.parse_args()))
