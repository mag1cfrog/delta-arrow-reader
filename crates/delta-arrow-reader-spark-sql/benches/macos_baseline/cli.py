"""Build the stock CLI for the footprint harness in a disposable worktree."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
from build import REPO, digest, git, replace, sync_main


def overlay(source):
    root = source / "crates/delta-arrow-reader-cli/src"
    files = {root / "main.rs": sync_main((root / "main.rs").read_text())}
    for relative in ("runtime/discovery.rs", "runtime/table_command.rs", "runtime/table_command/worker.rs"):
        path = root / relative
        builder = "tokio::runtime::Builder::new_current_thread()"
        files[path] = replace(path.read_text(), builder, builder + "\n        .on_thread_start(crate::prefer_performance_cores)")
    for relative, anchor in (("runtime/table_command.rs", '.name("dar-command".into()).spawn(move || {'),
                             ("output.rs", ".spawn(move || {")):
        path = root / relative
        raw = files[path] if path in files else path.read_text()
        files[path] = replace(raw, anchor, anchor + "\n                crate::prefer_performance_cores();")
    return files


def build(source, scratch, target, destination):
    assert sys.platform == "darwin" and os.uname().machine == "arm64" and __debug__
    assert not scratch.exists() and not destination.exists() and not destination.is_relative_to(REPO)
    source = git(REPO, "rev-parse", source)
    destination.mkdir(parents=True)
    subprocess.run(["git", "worktree", "add", "--detach", str(scratch), source], cwd=REPO, check=True,
                   stdout=subprocess.DEVNULL)
    originals = {}
    try:
        paths = overlay(scratch)
        originals = {path: path.read_bytes() for path in paths}
        for path, content in paths.items():
            path.write_text(content)
        output = destination / "dar"
        clean = {"PATH": os.environ["PATH"], "HOME": str(Path.home()), "TZ": "UTC"}
        command = ["cargo", "+1.96.0", "build", "--release", "--locked", "-j", "3", "--bin", "dar",
                   "--target-dir", str(target), "--message-format=json"]
        with subprocess.Popen(command, cwd=scratch, env=clean, stdout=subprocess.PIPE, text=True) as cargo:
            executable = None
            for line in cargo.stdout:
                row = json.loads(line)
                if row["reason"] == "compiler-message" and row["message"]["level"] == "error":
                    print(row["message"]["message"], file=sys.stderr)
                if row.get("executable") and row["target"]["name"] == "dar":
                    assert executable is None and row["profile"]["opt_level"] == "3"
                    executable = Path(row["executable"])
            assert cargo.wait(timeout=30) == 0 and executable
        shutil.copy2(executable, output)
        proof = {"source": source, "build": "stock CLI, no Spark dependency", "binary_sha256": digest(output),
                 "overlays": {str(path.relative_to(scratch)): digest(path) for path in paths}}
        (destination / "receipt.json").write_text(json.dumps(proof, indent=2) + "\n")
    finally:
        for path, content in originals.items():
            path.write_bytes(content)
        subprocess.run(["git", "worktree", "remove", str(scratch)], cwd=REPO, check=True)


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("source")
    for name in ("scratch", "target", "destination"):
        cli.add_argument(name, type=lambda path: Path(path).resolve())
    args = cli.parse_args()
    build(args.source, args.scratch, args.target, args.destination)
