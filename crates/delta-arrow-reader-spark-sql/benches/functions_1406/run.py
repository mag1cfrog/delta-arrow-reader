#!/usr/bin/env python3
"""Run #1391 statements on fixed reduced slices of its input table."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess

if not __debug__:
    raise RuntimeError("validation requires Python without -O")

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
ORIGINAL = HERE.parent / "functions.rs"
GENERATED = HERE.parent / "functions_1406_driver.rs"
FROZEN_MANIFEST = HERE.parent / "functions/manifest.json"
FROZEN_RECEIPT = HERE.parent / "functions/preflight-receipt.json"
MANIFEST = HERE / "manifest.json"


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def replace(source, old, new, count=1):
    assert source.count(old) == count, old
    return source.replace(old, new)


def reduced_driver():
    source = ORIGINAL.read_text()
    source = replace(source, "const ROWS: usize = 1_000_000;\n", "")
    source = replace(source, "    for start in (0..ROWS).step_by(BATCH) {\n"
                     "        let end = (start + BATCH).min(ROWS);\n"
                     "        let values = (start..end).map(seeded).collect::<Vec<_>>();\n"
                     "        let valid = |index: usize| !(start + index).is_multiple_of(16);",
                     "    for ids in selected_rows().chunks(BATCH) {\n"
                     "        let values = ids.iter().copied().map(seeded).collect::<Vec<_>>();\n"
                     "        let valid = |index: usize| !ids[index].is_multiple_of(16);")
    source = replace(source, "(start..end)", "ids.iter().copied()", 3)
    source = replace(source, "end - start", "ids.len()", 2)
    source = replace(source, "ROWS", "selected_rows().len()", 10)
    source = replace(source, '"source guard consumed {consumed} rows; expected {selected_rows().len()}"',
                     '"source guard consumed {consumed} rows; expected {}", selected_rows().len()')
    source = replace(source, "fn main() -> Result {\n    let args = std::env::args().skip(1).collect::<Vec<_>>();",
                     '#[test]\n#[ignore = "Run with the #1406 measurement protocol."]\n'
                     'fn run_1406() -> Result {\n'
                     '    let args: Vec<String> = serde_json::from_str(&std::env::var("DAR_1406_ARGS")?)?;')
    source = replace(source, "    let result = runtime.block_on(run(&args))?;",
                     '    let result = match args.first().map(String::as_str) {\n'
                     '        Some("input") => export_input()?,\n'
                     '        Some("observe") => runtime.block_on(observe_set(&args))?,\n'
                     '        Some("footprint") => runtime.block_on(bitmap_footprint(&args))?,\n'
                     '        _ => runtime.block_on(run(&args))?,\n'
                     '    };')
    return source + "\n" + (HERE / "observe.rs").read_text()


def build(target, output):
    output.mkdir(parents=True, exist_ok=False)
    source = reduced_driver()
    command = ["cargo", "+1.96.0", "build", "--locked", "--release", "--bench",
               "functions_1406_driver", "--message-format=json-render-diagnostics"]
    settings = {"CARGO_TARGET_DIR": str(target), "CARGO_BUILD_JOBS": "2", "CARGO_PROFILE_RELEASE_DEBUG": "1"}
    log = output / "build.jsonl"
    with GENERATED.open("x") as generated:
        generated.write(source)
    try:
        with log.open("x") as stdout, (output / "build.stderr").open("x") as stderr:
            subprocess.run(command, cwd=WORKSPACE, env=os.environ | settings, check=True,
                           stdout=stdout, stderr=stderr)
    finally:
        GENERATED.unlink()
    artifacts = (json.loads(line) for line in log.read_text().splitlines() if line.startswith("{"))
    binary, = [row["executable"] for row in artifacts if row.get("executable")
               and row.get("target", {}).get("name") == "functions_1406_driver"]
    record = {"binary": binary, "sha256": sha(Path(binary)), "source_sha256": sha(ORIGINAL),
              "generated_sha256": hashlib.sha256(source.encode()).hexdigest(),
              "command": command, "environment": settings}
    (output / "receipt.json").write_text(json.dumps(record, indent=2) + "\n")


def probe(binary, profile, arguments, output):
    return invoke(binary, ["--ignored", "--exact", "run_1406", "--nocapture"], output,
                  {"DAR_1406_INPUT": profile, "DAR_1406_ARGS": json.dumps(arguments)}, 180)


def invoke(binary, arguments, output, settings, timeout):
    cap = 12 * 1024 ** 3
    command = ["taskset", "-c", "2,3,4,5", str(binary), *map(str, arguments)]
    with output.open("x") as stdout, output.with_suffix(".stderr").open("x") as stderr:
        subprocess.run(command, check=True, env=os.environ | settings, stdout=stdout, stderr=stderr,
                       timeout=timeout, preexec_fn=lambda: resource.setrlimit(resource.RLIMIT_AS, (cap, cap)))
    records = [json.loads(line) for line in output.read_text().splitlines() if line.startswith("{")]
    assert records, output
    return records


def cases():
    manifest = json.loads(MANIFEST.read_text())
    assert sha(FROZEN_MANIFEST) == manifest["frozen_manifest_sha256"]
    assert sha(FROZEN_RECEIPT) == manifest["frozen_preflight_sha256"]
    all_cases = {c["id"]: c for c in json.loads(FROZEN_MANIFEST.read_text())["functions"]}
    excluded = {c["id"]: c["status"] for c in json.loads(FROZEN_RECEIPT.read_text())["results"]
                if c["status"] in {"semantic_mismatch", "setup_failure"}}
    selected = manifest["functions"]
    assert len(selected) == len(excluded) == 42 and {c["id"]: c["1391_status"] for c in selected} == excluded
    for case in selected:
        assert all(case[part] == all_cases[case["id"]][part] for part in
                   ("spark_sql", "native_sql", "projection_sql", "name", "kind"))
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="command", required=True)
    subs.add_parser("check")
    prepare = subs.add_parser("build")
    prepare.add_argument("target", type=Path)
    prepare.add_argument("output", type=Path)
    extract = subs.add_parser("input")
    extract.add_argument("binary", type=Path)
    extract.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.command == "check":
        assert "run_1406" in reduced_driver()
        cases()
    elif args.command == "build":
        build(args.target.resolve(), args.output.resolve())
    else:
        record, = probe(args.binary.resolve(), "reference", ["input"], args.output.resolve())
        assert len(record["rows"]) == 96
        print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()
