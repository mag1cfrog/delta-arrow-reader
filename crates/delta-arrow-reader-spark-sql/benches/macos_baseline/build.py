"""Build the frozen benchmark drivers with native macOS thread controls."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
WORKSPACE = HERE.parents[1].relative_to(REPO)
TARGETS = ("functions", "execution", "planning", "nullif_620", "nullif", "array_contains",
           "bounded_windows", "pending_range_isolated", "planning_attribution", "functions_1406_driver")

QOS = """
fn prefer_performance_cores() {
    const USER_INTERACTIVE: u32 = 0x21;
    unsafe extern "C" {
        fn pthread_set_qos_class_self_np(class: u32, relative_priority: i32) -> i32;
        fn qos_class_self() -> u32;
    }
    // Verify the requested QoS on the calling thread before it runs the workload.
    unsafe {
        assert_eq!(pthread_set_qos_class_self_np(USER_INTERACTIVE, 0), 0);
        assert_eq!(qos_class_self(), USER_INTERACTIVE);
    }
}

"""

SAMPLES = """
fn retained_samples(args: &mut Vec<String>, default: usize) -> usize {
    if args.first().is_none_or(|arg| arg != "--macos-samples") {
        return default;
    }
    assert!(args.len() > 2, "missing retained sample count or command");
    let count = args[1].parse().expect("invalid retained sample count");
    assert!((3..=9).contains(&count), "retained sample count must be in 3..=9");
    args.drain(..2);
    count
}

"""


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def replace(source, before, after):
    assert source.count(before) == 1, before
    return source.replace(before, after)


def sync_main(source):
    match, = re.finditer(r"^fn main\(\) -> .*? \{\n", source, re.MULTILINE)
    return replace(source, match[0], QOS + match[0] + "    prefer_performance_cores();\n")


def async_main(source):
    match, = re.finditer(
        r'^#\[tokio::main\(flavor = "(current_thread|multi_thread)"(?:, worker_threads = (\d+))?\)\]\n'
        r'async fn main\(\) -> (.*?) \{\n', source, re.MULTILINE)
    flavor, workers, result = match.groups()
    builder = f"tokio::runtime::Builder::new_{flavor}()"
    if workers:
        builder += f"\n        .worker_threads({workers})"
    main = f"""fn main() -> {result} {{
    prefer_performance_cores();
    {builder}
        .on_thread_start(prefer_performance_cores)
        .enable_all()
        .build()
        .expect("Failed building the Runtime")
        .block_on(run_macos_benchmark())
}}

async fn run_macos_benchmark() -> {result} {{
"""
    return replace(source, match[0], QOS + main)


def worker_callback(source):
    anchor = "tokio::runtime::Builder::new_multi_thread()"
    return replace(source, anchor, anchor + "\n        .on_thread_start(prefer_performance_cores)")


def functions(source):
    start = source.index('    let status = fs::read_to_string("/proc/self/status")?;')
    end = source.index("    Ok(", start)
    source = source[:start] + "    let peak_rss: Option<u64> = None;\n" + source[end:]
    source = replace(source, "for iteration in 0..4 {", "for iteration in 0..=sample_count {")
    source = replace(source, "case: &Case, configuration: &str) -> Result<Value>",
                     "case: &Case, configuration: &str, sample_count: usize) -> Result<Value>")
    source = replace(source, "async fn run(args: &[String])", "async fn run(args: &[String], sample_count: usize)")
    call = "measure(contexts.get(&args[3]), case, &args[3])"
    assert source.count(call) == 2
    source = source.replace(call, call[:-1] + ", sample_count)")
    source = replace(source, "    let args = std::env::args().skip(1).collect::<Vec<_>>();",
                     "    let mut args = std::env::args().skip(1).collect::<Vec<_>>();\n"
                     "    let sample_count = retained_samples(&mut args, 3);")
    source = replace(source, "runtime.block_on(run(&args))", "runtime.block_on(run(&args, sample_count))")
    return worker_callback(sync_main(source)) + SAMPLES


def execution(source):
    source = replace(source, "    let parsers = args.first()", "    let sample_count = retained_samples(&mut args, 0);\n"
                     "    let parsers = args.first()")
    source = replace(source, "    let schedule: Schedule = serde_json::from_str(schedule_json)?;",
                     "    let mut schedule: Schedule = serde_json::from_str(schedule_json)?;\n"
                     "    if sample_count != 0 {\n        schedule.samples = sample_count;\n    }")
    source = replace(source, 'include_str!("parsers/queries.json")', 'include_str!("macos_1560_parsers.json")')
    return worker_callback(sync_main(source)) + SAMPLES


def planning(source):
    source = replace(source, "async fn preflight(manifest: &Manifest, root: &Path) -> TestResult {",
                     "async fn preflight(manifest: &Manifest, root: &Path, only: Option<&str>) -> TestResult {")
    source = replace(source, "            // Untimed correctness includes stress in both native sessions.",
                     "            if only.is_some_and(|id| query.id != id) { continue; }\n"
                     "            // Untimed correctness includes stress in both native sessions.")
    source = replace(source, '"preflight" if args.len() == 2 => preflight(&manifest, root).await,',
                     '"preflight" if (2..=3).contains(&args.len()) => {\n'
                     '            preflight(&manifest, root, args.get(2).map(String::as_str)).await\n        }')
    source = replace(source, "    eligibility: &Path,\n", "    eligibility: &Path,\n"
                     "    only: Option<&str>,\n    sample_count: usize,\n")
    source = replace(source, "        if !applies(query, configuration, noise) {",
                     "        if only.is_some_and(|id| query.id != id) || !applies(query, configuration, noise) {")
    source = replace(source, "for sample in 0..WARMUPS + SAMPLES {", "for sample in 0..WARMUPS + sample_count {")
    source = replace(source, "    let args = std::env::args().skip(1).collect::<Vec<_>>();",
                     "    let mut args = std::env::args().skip(1).collect::<Vec<_>>();\n"
                     "    let sample_count = retained_samples(&mut args, SAMPLES);")
    source = replace(source, '"measure" if args.len() == 6 => {', '"measure" if (6..=7).contains(&args.len()) => {')
    source = replace(source, "                Path::new(&args[5]),\n", "                Path::new(&args[5]),\n"
                     "                args.get(6).map(String::as_str),\n                sample_count,\n")
    return async_main(source) + SAMPLES


def planning_attribution(source):
    source = replace(source, "    let args = std::env::args().skip(1).collect::<Vec<_>>();",
                     "    let mut args = std::env::args().skip(1).collect::<Vec<_>>();\n"
                     "    let sample_count = crate::retained_samples(&mut args, SAMPLES);")
    source = replace(source, "    if args.len() != 5\n", "    if !(5..=6).contains(&args.len())\n")
    source = replace(source, "    for query in &manifest.queries {", "    let only = args.get(5);\n"
                     "    for query in &manifest.queries {\n"
                     "        if only.is_some_and(|id| query.id != *id) { continue; }")
    source = replace(source, "for sample in 0..WARMUPS + SAMPLES {", "for sample in 0..WARMUPS + sample_count {")
    return replace(source, '    let expected = if trace && configuration.starts_with("spark-") {',
                   '    let expected = if only.is_some() { 1 } else if trace && configuration.starts_with("spark-") {')


def nullif(source):
    source = replace(source, "    let args = std::env::args().skip(1).collect::<Vec<_>>();",
                     "    let mut args = std::env::args().skip(1).collect::<Vec<_>>();\n"
                     "    let sample_count = retained_samples(&mut args, 3);")
    source = replace(source, '    } else if arm == "native-b" {',
                     '    } else if command == "measure" && configurations.contains(&arm) {\n'
                     '        configurations = vec![arm];\n    } else if arm == "native-b" {')
    source = replace(source, "                if excluded.contains(&id) {", "                if excluded.contains(&id)\n"
                     "                    || args.get(5).is_some_and(|only| id != *only) {\n")
    source = replace(source, "for sample in 0..if output.is_some() { 1 } else { 4 } {",
                     "for sample in 0..if output.is_some() { 1 } else { 1 + sample_count } {")
    source = replace(source, 'if command == "preflight" && family == "nullif" {',
                     'if command == "preflight" && family == "nullif" && args.get(5).is_none() {')
    return async_main(source) + SAMPLES


def reduced_functions():
    driver = runpy.run_path(str(HERE.parent / "functions_1406/run.py"))["reduced_driver"]()
    driver = replace(driver, '#[test]\n#[ignore = "Run with the #1406 measurement protocol."]\nfn run_1406()',
                     "fn main()")
    start = driver.index('    let status = fs::read_to_string("/proc/self/status")?;')
    end = driver.index("    Ok(", start)
    driver = driver[:start] + "    let peak_rss: Option<u64> = None;\n" + driver[end:]
    return worker_callback(sync_main(driver))


def overlay(workspace, targets):
    paths = {}
    history = workspace / "benches"
    manifest = workspace / "Cargo.toml"
    cargo = manifest.read_text()
    for name in targets:
        if name == "pending_range_isolated":
            build = runpy.run_path(str(HERE.parent / "pending_range_1099_isolated/build.py"))
            driver = history / "pending_range_1099/driver.rs"
            paths[driver] = build["render_pending_driver"](driver.read_text())
            entry = history / "pending_range_1099_isolated/entry.rs"
            source = entry.read_text()
            source = replace(source, '''            native_controls::run(
                Path::new(args.get(4).ok_or("missing controls")?),
                true,
                block,
            )
            .await?;''', '''            if let Some(controls) = args.get(4) {
                native_controls::run(Path::new(controls), true, block).await?;
            }''')
            paths[entry] = async_main(source)
        elif name == "planning_attribution":
            sys.path.insert(0, str(HERE.parent / "planning_attribution"))
            try:
                import assemble
                driver = history / "planning_attribution/driver.rs"
                assert driver.read_bytes() == (HERE.parent / "planning_attribution/driver.rs").read_bytes()
                paths[driver] = planning_attribution(assemble.driver_source(False))
            finally:
                sys.path.pop(0)
            fixture = history / "planning_attribution_fixture.rs"
            paths[fixture] = (history / "planning.rs").read_text() + (
                '\n#[path = "planning_attribution/driver.rs"]\npub mod attribution;\n')
            entry = history / "planning_attribution_entry.rs"
            paths[entry] = async_main('''//! Run the planning-only attribution benchmark on macOS.

#[allow(dead_code)]
#[path = "planning_attribution_fixture.rs"]
mod frozen;
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    frozen::attribution::run().await
}
''') + SAMPLES
        elif name == "functions_1406_driver":
            assert (history / "functions.rs").read_bytes() == (HERE.parent / "functions.rs").read_bytes()
            entry = history / f"{name}.rs"
            paths[entry] = reduced_functions()
        else:
            entry = history / f"{name}.rs"
        if name == "execution":
            parsers = json.loads((history / "parsers/queries.json").read_text())
            override = json.loads((history / "interval_1389_round3/quoted-query.json").read_text())
            base = next(query for query in parsers["queries"] if query["id"] == "interval_year_month")
            parsers["queries"].append({**base, **override})
            paths[history / "macos_1560_parsers.json"] = json.dumps(parsers, indent=2) + "\n"
        if name in ("pending_range_isolated", "planning_attribution", "functions_1406_driver"):
            cargo += f'\n[[bench]]\nname = "{name}"\npath = "{entry.relative_to(workspace)}"\nharness = false\n'
        if entry not in paths:
            transform = {"functions": functions, "execution": execution, "planning": planning,
                         "nullif_620": nullif}.get(name, async_main)
            paths[entry] = transform(entry.read_text())
    if cargo != manifest.read_text():
        paths[manifest] = cargo
    return paths


def build(source, repo, target_dir, destination, targets):
    assert sys.platform == "darwin" and os.uname().machine == "arm64" and __debug__
    assert repo != REPO and not destination.is_relative_to(repo)
    if target_dir.is_relative_to(repo):
        assert subprocess.run(["git", "check-ignore", "-q", str(target_dir)], cwd=repo).returncode == 0
    assert not git(repo, "status", "--porcelain")
    source = git(REPO, "rev-parse", source)
    recipe_sha256 = digest(Path(__file__))
    head = git(repo, "rev-parse", "HEAD")
    assert not git(repo, "branch", "--show-current"), "use a detached scratch worktree"
    destination.mkdir(parents=True)
    subprocess.run(["git", "switch", "--detach", source], cwd=repo, check=True, stdout=subprocess.DEVNULL)
    workspace = repo / WORKSPACE
    originals = {}
    try:
        paths = overlay(workspace, targets)
        originals = {path: path.read_bytes() if path.exists() else None for path in paths}
        for path, contents in paths.items():
            path.write_text(contents)
        cmd = ["cargo", "+1.96.0", "build", "--locked", "--release", "-j", "3",
               "--config", "profile.release.debug=1", "--target-dir", str(target_dir),
               "--message-format=json-render-diagnostics"]
        for name in targets:
            cmd += ["--bench", name]
        clean = {"PATH": os.environ["PATH"], "HOME": str(Path.home()), "TZ": "UTC"}
        artifacts = {}
        with subprocess.Popen(cmd, cwd=workspace, env=clean, stdout=subprocess.PIPE, text=True) as process:
            for line in process.stdout:
                row = json.loads(line)
                if row["reason"] == "compiler-message" and row["message"]["level"] == "error":
                    print(row["message"]["message"], file=sys.stderr)
                if row["reason"] == "compiler-artifact" and row.get("executable"):
                    name = row["target"]["name"]
                    assert name in targets and row["profile"]["opt_level"] == "3"
                    binary = destination / name
                    shutil.copy2(row["executable"], binary)
                    artifacts[name] = digest(binary)
            assert process.wait(timeout=30) == 0 and set(artifacts) == set(targets)
        receipt = {"source": source, "profile": "release", "rustc": "1.96.0", "binaries": artifacts,
                   "recipe_sha256": recipe_sha256,
                   "overlays": {str(path.relative_to(workspace)): digest(path) for path in paths}}
        (destination / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    finally:
        for path, contents in originals.items():
            if contents is None:
                path.unlink()
            else:
                path.write_bytes(contents)
        subprocess.run(["git", "switch", "--detach", head], cwd=repo, check=True, stdout=subprocess.DEVNULL)
        assert not git(repo, "status", "--porcelain")


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("source")
    for name in ("repo", "target_dir", "destination"):
        cli.add_argument(name, type=lambda path: Path(path).resolve())
    cli.add_argument("targets", nargs="*", metavar="TARGET")
    args = cli.parse_args()
    if unknown := set(args.targets) - set(TARGETS):
        cli.error(f"unknown targets: {', '.join(sorted(unknown))}")
    build(args.source, args.repo, args.target_dir, args.destination, args.targets or TARGETS)
