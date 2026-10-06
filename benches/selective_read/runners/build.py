"""Build one pinned Rust reader and save its executable and resolved provenance."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
TARGET = "x86_64-unknown-linux-gnu"


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def sources():
    paths = [ROOT / name for name in ("Cargo.toml", "Cargo.lock", "README.md",
             "benches/selective_read/oracle.py", "benches/selective_read/rust-toolchain.toml",
             "docs/content/benchmarks/selective-read-protocol.md",
             "docs/content/benchmarks/selective-read-large-workloads.md",
             "docs/content/benchmarks/selective-read-sampling.md",
             "docs/content/benchmarks/selective-read-production-workloads.md",
             "docs/content/benchmarks/selective-read-spark-matrix.md")]
    paths += [path for base in (ROOT / "src", HERE) for path in base.rglob("*")
              if path.is_file() and path.suffix in (".rs", ".toml", ".lock", ".py")]
    return {str(path.relative_to(ROOT)): digest(path) for path in sorted(paths)}


def build(reader, output):
    output = output.resolve()
    output.mkdir()
    manifest = HERE / reader / "Cargo.toml"
    executable = f"selective-read-{reader}"
    target_dir = ROOT / "target/selective-read-runners" / reader
    env = dict(os.environ, RUSTFLAGS="-C target-cpu=x86-64", CARGO_INCREMENTAL="0",
               CARGO_TARGET_DIR=str(target_dir))
    env.pop("CARGO_ENCODED_RUSTFLAGS", None)
    cargo = ["cargo", "+1.98.1"]
    command = cargo + ["build", "--release", "--locked", "--target", TARGET, "-j8", "--manifest-path", str(manifest)]
    before = sources()
    subprocess.run(command, cwd=ROOT, env=env, check=True)
    metadata = subprocess.check_output(cargo + ["metadata", "--locked", "--format-version=1",
        "--filter-platform", TARGET, "--manifest-path", str(manifest)], cwd=ROOT, env=env)
    tree = subprocess.check_output(cargo + ["tree", "--locked", "--target", TARGET,
        "--manifest-path", str(manifest), "--edges", "normal,build,features"], cwd=ROOT, env=env)
    if sources() != before:
        raise ValueError("source files changed during build; use a new output directory")
    shutil.copy2(target_dir / TARGET / "release" / executable, output / executable)
    shutil.copyfile(manifest.with_name("Cargo.lock"), output / "Cargo.lock")
    (output / "metadata.json").write_bytes(metadata)
    (output / "cargo-tree.txt").write_bytes(tree)
    resolved = json.loads(metadata)
    nodes = {node["id"]: node["features"] for node in resolved["resolve"]["nodes"]}
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    published_vcs = {}
    for package in resolved["packages"]:
        if package["name"].startswith("deltalake"):
            vcs = Path(package["manifest_path"]).with_name(".cargo_vcs_info.json")
            if vcs.exists():
                published_vcs[package["name"]] = json.loads(vcs.read_text())
    if reader == "delta-rs" and published_vcs["deltalake"]["git"]["sha1"] != "41f1ce23377f088298a3ed196ad6c8e40cb9bfed":
        raise ValueError("delta-rs release source differs from the protocol pin")
    record = {
        "format": "selective-read-build-v1",
        "reader_id": "delta-arrow-reader" if reader == "dar" else "delta-rs",
        "harness_git_commit": commit,
        "reader_git_commit": commit if reader == "dar" else published_vcs["deltalake"]["git"]["sha1"],
        "published_vcs": published_vcs,
        "git_status": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
        "source_sha256": before,
        "command": command,
        "rustflags": env["RUSTFLAGS"],
        "target": TARGET,
        "rustc": subprocess.check_output(["rustc", "+1.98.1", "-vV"], text=True),
        "cargo": subprocess.check_output(cargo + ["--version"], text=True).strip(),
        "linker": subprocess.check_output(["cc", "--version"], text=True).splitlines()[0],
        "executable_sha256": digest(output / executable),
        "lockfile_sha256": digest(output / "Cargo.lock"),
        "metadata_sha256": digest(output / "metadata.json"),
        "cargo_tree_sha256": digest(output / "cargo-tree.txt"),
        "dependencies": [{"name": p["name"], "version": p["version"], "source": p["source"],
                          "features": nodes[p["id"]]} for p in resolved["packages"] if p["id"] in nodes],
    }
    (output / "build.json").write_text(json.dumps(record, sort_keys=True, indent=2) + "\n")
    print(output / executable)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reader", choices=("dar", "delta-rs"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.reader, args.output)
