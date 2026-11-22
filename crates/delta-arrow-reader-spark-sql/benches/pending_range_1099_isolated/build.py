"""Build the two RANGE workloads from a committed, disposable source tree."""

import hashlib
import json
import os
from pathlib import Path
import platform
import runpy
import shutil
import subprocess
import sys
import tarfile
import tempfile

HERE = Path(__file__).resolve().parent
LEGACY = HERE.parent / "pending_range_1099"
RELEASE = runpy.run_path(str(LEGACY / "run.py"))["RELEASE"]
CRATE = "crates/delta-arrow-reader-spark-sql"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def git(*args):
    return subprocess.check_output(["git", *args], cwd=HERE, text=True).strip()


def render_pending_driver(source):
    observation_hook = "// @observations@"
    assert source.count(observation_hook) == 1
    source = source.replace(observation_hook,
                            "fn reset_events(_: bool) {}\nfn snapshot_events() -> Value { Value::Null }")
    sample = "                            let elapsed = started.map(|start| start.elapsed().as_nanos() as u64);"
    assert source.count(sample) == 1
    return source.replace(sample, sample + """
                            if repeat >= warmups && let Some(ns) = elapsed {
                                super::record_sample(&id, repeat - warmups + 1, ns)?;
                            }""")


def build(source, target, output):
    assert platform.system() == "Linux"
    assert git("rev-parse", source) == source
    assert not git("status", "--porcelain")
    repo = Path(git("rev-parse", "--show-toplevel"))
    assert not target.is_relative_to(repo) and not output.is_relative_to(repo)
    output.mkdir()
    with tempfile.TemporaryDirectory(prefix="range-source-", dir=output.parent) as scratch:
        with tempfile.TemporaryFile() as archive:
            subprocess.run(["git", "archive", "--format=tar", source], cwd=repo, stdout=archive, check=True)
            archive.seek(0)
            with tarfile.open(fileobj=archive) as contents:
                contents.extractall(scratch, filter="data")
        workspace = Path(scratch) / CRATE
        driver = workspace / "benches/pending_range_1099/driver.rs"
        assert digest(driver) == digest(LEGACY / "driver.rs")
        driver.write_text(render_pending_driver(driver.read_text()))
        bench = workspace / "benches/pending_range_1099_isolated"
        bench.mkdir(exist_ok=True)
        for item in ("entry.rs", "native_controls.rs"):
            shutil.copyfile(HERE / item, bench / item)
        with (workspace / "Cargo.toml").open("a") as manifest:
            manifest.write('\n[[bench]]\nname = "pending_range_isolated"\n'
                           'path = "benches/pending_range_1099_isolated/entry.rs"\nharness = false\n')

        cargo = shutil.which("cargo")
        rustc = shutil.which("rustc")
        assert cargo and rustc
        tools = os.pathsep.join((str(Path(cargo).parent), str(Path(rustc).parent), os.defpath))
        settings = {"HOME": str(Path.home()), "PATH": tools, "TZ": "UTC",
                    "CARGO_TARGET_DIR": str(target), **RELEASE}
        command = ["cargo", "+1.96.0", "build", "--locked", "--offline", "--release",
                   "--bench", "pending_range_isolated", "--message-format=json"]
        with subprocess.Popen(command, cwd=workspace, env=settings, stdout=subprocess.PIPE,
                              text=True) as build_process:
            artifacts = [row["executable"] for row in map(json.loads, build_process.stdout)
                         if row["reason"] == "compiler-artifact"
                         and row["target"]["name"] == "pending_range_isolated"
                         and row.get("executable")]
            assert build_process.wait() == 0 and len(artifacts) == 1
        binary = output / "pending_range_isolated"
        shutil.copy2(artifacts[0], binary)
        receipt = {
            "source": source, "sha256": digest(binary), "profile": "release",
            "rustc": subprocess.check_output(["rustc", "+1.96.0", "--version"],
                                           env=settings, text=True).strip(),
            "drivers": {f.name: digest(f) for f in (driver, bench / "entry.rs", bench / "native_controls.rs")},
        }
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(source, receipt["sha256"], flush=True)


if __name__ == "__main__":
    build(sys.argv[1], *(Path(path).resolve() for path in sys.argv[2:]))
