"""Generate and verify the pinned SF1 snapshot on macOS."""

import argparse
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tarfile
import tempfile

sys.dont_write_bytecode = True
from build import REPO, digest, git, replace

BENCHES = Path(__file__).resolve().parents[1]
RECEIPT = BENCHES / "reports/execution-1380-fixture.json"
COMMANDS = BENCHES / "reports/execution-1380-fixture-commands.json"
GENERATOR = Path("benches/selective_read")


def adapt(source):
    fixtures = source / GENERATOR / "src/fixtures.rs"
    text = fixtures.read_text()
    text = replace(text, '#[cfg(not(target_os = "linux"))]\n    {\n        let _ = bytes;\n'
                   '        Err("the frozen generator profile requires Linux".into())\n    }',
                   '#[cfg(target_os = "macos")]\n    {\n        let _ = bytes;\n        Ok(())\n    }')
    for name in ("available_disk", "peak_rss"):
        start = text.index(f"pub fn {name}(")
        next_fn = text.find("\npub fn ", start + 1)
        end = len(text) if next_fn == -1 else next_fn
        body = text[start:end]
        body = replace(body, '#[cfg(target_os = "linux")]', '#[cfg(any(target_os = "linux", target_os = "macos"))]')
        body = replace(body, '#[cfg(not(target_os = "linux"))]', '#[cfg(not(any(target_os = "linux", target_os = "macos")))]')
        if name == "available_disk":
            body = replace(body, "stats.f_bavail.saturating_mul(stats.f_frsize)",
                           "u64::from(stats.f_bavail).saturating_mul(stats.f_frsize)")
        else:
            body = replace(body, "Ok(u64::try_from(usage.ru_maxrss)? * 1024)",
                           'Ok(u64::try_from(usage.ru_maxrss)? * if cfg!(target_os = "macos") { 1 } else { 1024 })')
        text = text[:start] + body + text[end:]
    fixtures.write_text(text)
    large = source / GENERATOR / "src/large.rs"
    text = replace(large.read_text(), '#[cfg(target_os = "linux")]',
                   '#[cfg(any(target_os = "linux", target_os = "macos"))]')
    large.write_text(replace(text, '#[cfg(not(target_os = "linux"))]',
                             '#[cfg(not(any(target_os = "linux", target_os = "macos")))]'))
    main = source / GENERATOR / "src/main.rs"
    main.write_text(replace(main.read_text(), 'Path::new("/proc/self/exe")', '&env::current_exe()?'))


def read(path):
    return json.loads(path.read_text())


def build(source, target, binary, expected):
    with tempfile.TemporaryFile() as archive:
        subprocess.run(["git", "archive", expected["source_commit"], str(GENERATOR)],
                       cwd=REPO, stdout=archive, check=True, timeout=120)
        archive.seek(0)
        with tarfile.open(fileobj=archive) as stream:
            stream.extractall(source, filter="data")
    package = source / GENERATOR
    assert digest(package / "Cargo.lock") == expected["generator"]["lockfile_sha256"]
    adapt(source)
    clean = {"PATH": os.environ["PATH"], "HOME": str(Path.home()), "TZ": "UTC"}
    command = ["cargo", "+1.98.1", "build", "--release", "--locked", "-j", "2", "--bin",
               "selective-read-fixtures", "--target-dir", str(target), "--message-format=json"]
    with subprocess.Popen(command, cwd=package, env=clean, stdout=subprocess.PIPE, text=True) as cargo:
        executable = None
        for line in cargo.stdout:
            row = json.loads(line)
            if row["reason"] == "compiler-message" and row["message"]["level"] == "error":
                print(row["message"]["message"], file=sys.stderr)
            if row.get("executable") and row["target"]["name"] == "selective-read-fixtures":
                assert executable is None
                executable = Path(row["executable"])
        assert cargo.wait(timeout=30) == 0 and executable
    shutil.copy2(executable, binary)


def generate(binary, output):
    commands = read(COMMANDS)
    for phase in ("capacity_command", "generation_command"):
        original = commands[phase]
        arguments = original[1:]
        data = arguments.index("--output") + 1
        arguments[data] = str(output)
        env = {"PATH": os.defpath, "TZ": "UTC"}
        result = subprocess.run([binary, *arguments], env=env, capture_output=True, timeout=1860)
        if result.returncode != 0:
            # The prepare command has no inputs beyond generated data and local paths.
            stderr = result.stderr.decode(errors="replace").replace(str(output), "BASE_SF1")
            raise RuntimeError(f"SF1 {phase} failed: {stderr[-1200:]}")


def prove(actual, expected):
    checks = {}
    common = ("source_scale_factor", "source_commit", "source_table", "stored_columns", "snapshot_columns",
              "snapshot_version", "rows")
    for key in common:
        assert actual[key] == expected[key], f"fixture metadata differs: {key}"
    for key in ("lockfile_sha256", "tpchgen", "tpchgen_git", "arrow", "parquet", "datafusion_sort"):
        assert actual["generator"][key] == expected["generator"][key], f"generator inputs differ: {key}"
    keys = ("base_delta_log_sha256", "derived_delta_log_sha256", "source_manifest_sha256")
    for key in keys:
        checks[key] = {"macos": actual[key], "linux_reference": expected[key], "match": actual[key] == expected[key]}
    for key in ("executable_sha256", "lockfile_sha256"):
        mac, linux = (receipt["generator"][key] for receipt in (actual, expected))
        checks[f"generator_{key}"] = {"macos": mac, "linux_reference": linux, "match": mac == linux}
    checks["files"] = []
    for mac, linux in zip(actual["files"], expected["files"], strict=True):
        assert (mac["path"], mac["file_bucket"], mac["rows"]) == (linux["path"], linux["file_bucket"], linux["rows"])
        checks["files"].append({"path": mac["path"], "file_bucket": mac["file_bucket"], "rows": mac["rows"],
                                "macos": mac["sha256"], "linux_reference": linux["sha256"],
                                "match": mac["sha256"] == linux["sha256"] and mac["bytes"] == linux["bytes"]})
    snapshot_matches = all(checks[key]["match"] for key in keys[:2]) and all(row["match"] for row in checks["files"])
    return {"snapshot_matches": snapshot_matches, **{key: actual[key] for key in common}, "checks": checks,
            "generator": actual["generator"], "fixture_recipe_sha256": digest(Path(__file__)),
            "reference_receipt_sha256": digest(RECEIPT), "reference_commands_sha256": digest(COMMANDS)}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("output", type=lambda path: Path(path).resolve())
    cli.add_argument("target", type=lambda path: Path(path).resolve())
    cli.add_argument("--generator", type=lambda path: Path(path).resolve())
    args = cli.parse_args()
    output = args.output
    assert sys.platform == "darwin" and os.uname().machine == "arm64" and __debug__
    assert not output.is_relative_to(REPO)
    output.mkdir()
    expected = read(RECEIPT)
    binary = args.generator or output / "selective-read-fixtures"
    if args.generator is None:
        build(output / "source", args.target, binary, expected)
    base, snapshot = output / "base-sf1", output / "delta-snapshot"
    generate(binary, base)
    actual = output / "fixture-local.json"
    prepare = runpy.run_path(str(BENCHES / "execution/prepare.py"))["prepare"]
    prepare(base, snapshot, actual)
    proof = prove(read(actual), expected)
    (output / "fixture-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
    print(f"SF1 snapshot hash match: {proof['snapshot_matches']}")


if __name__ == "__main__":
    main()
