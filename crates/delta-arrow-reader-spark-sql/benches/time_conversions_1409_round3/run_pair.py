"""Build and compare the frozen function workload with sealed release receipts."""

import argparse
import gzip
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(HERE.parent / "window_aggregates_1408"))
from compare import measure, runner
from instrument import changes
from run import WORKSPACE, write


def git(*args, repository=REPO):
    return subprocess.check_output(["git", "-C", str(repository), *args], text=True).strip()


def build(source, repository, target, output):
    assert repository != REPO and not target.is_relative_to(repository)
    assert not git("status", "--porcelain", repository=repository)
    assert not git("status", "--porcelain")
    protocol_commit = git("rev-parse", "HEAD")
    source = git("rev-parse", source)
    subprocess.run(["git", "switch", "--detach", source], cwd=repository, check=True)
    workspace = repository / WORKSPACE
    patched = dict(changes(workspace, False))
    originals = {path: path.read_bytes() for path in patched}
    command = ["cargo", "+1.96.0", "build", "--offline", "--locked", "--release", "--bench",
               "functions", "-j", "3", "--config", "profile.release.debug=1",
               "--target-dir", str(target), "--message-format=json-render-diagnostics"]
    clean = {"PATH": os.environ["PATH"], "HOME": str(Path.home()), "TZ": "UTC"}
    output.mkdir()
    try:
        for path, contents in patched.items():
            path.write_text(contents)
        result = subprocess.run(command, cwd=workspace, env=clean, stdout=subprocess.PIPE,
                                text=True, timeout=3600, check=True)
        artifacts = [row for row in map(json.loads, result.stdout.splitlines())
                     if row.get("executable") and row.get("target", {}).get("name") == "functions"]
        artifact, = artifacts
    finally:
        for path, contents in originals.items():
            path.write_bytes(contents)
    assert not git("status", "--porcelain", repository=repository)
    profile = artifact["profile"]
    assert profile["opt_level"] == "3" and profile["debuginfo"] == 1
    binary = output / "functions"
    shutil.copy2(artifact["executable"], binary)
    assert runner.digest(binary) == runner.digest(Path(artifact["executable"]))
    patch = output / "patch.json"
    write(patch, {str(path.relative_to(workspace)): contents for path, contents in patched.items()})
    rustc = subprocess.check_output(["rustc", "+1.96.0", "--version"], env=clean, text=True).strip()
    assert rustc.startswith("rustc 1.96.0 ")
    write(output / "receipt.json", dict(source=source, variant="plain", sha256=runner.digest(binary),
              patch_sha256=runner.digest(patch), protocol_commit=protocol_commit,
              recipe_sha256=runner.digest(Path(__file__)), rustc=rustc, profile=profile))
    print(source, runner.digest(binary))


def run(settings_path, staging, candidate, output):
    committed = subprocess.check_output(["git", "show", f"HEAD:{settings_path.relative_to(REPO)}"], cwd=REPO)
    assert committed == settings_path.read_bytes()
    settings = runner.read(settings_path)
    for arm, directory in (("staging", staging), ("candidate", candidate)):
        receipt = runner.read(directory / "receipt.json")
        assert receipt["protocol_commit"] == settings[f"{arm}_build_protocol"]
        assert receipt["recipe_sha256"] == runner.digest(Path(__file__))
    measure(settings_path, {"staging": staging, "candidate": candidate}, output)


def archive(destination, evidence):
    scopes = ("paired", "noise")
    with destination.open("xb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed:
            for scope in scopes:
                for path in sorted((evidence / scope).rglob("stdout.jsonl")):
                    entries = runner.parse_lines(path.read_text(), interrupted=True)
                    verified = runner.read(path.with_name("verified.json"))
                    for row in entries:
                        record = dict(process=str(path.parent.relative_to(evidence)), record=row)
                        compressed.write((json.dumps(record, sort_keys=True) + "\n").encode())
                    if verified["status"] == "ok":
                        assert sum(row.get("kind") == "retained_sample" for row in entries) == len(verified["samples"])


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    sub = cli.add_subparsers(dest="action", required=True)
    prepare = sub.add_parser("build")
    prepare.add_argument("source")
    timing = sub.add_parser("run")
    pack = sub.add_parser("archive")
    for parser, names in ((prepare, ("repository", "target", "output")),
                          (timing, ("settings_path", "staging", "candidate", "output")),
                          (pack, ("destination", "evidence"))):
        for name in names:
            parser.add_argument(name, type=lambda value: Path(value).resolve())
    args = vars(cli.parse_args())
    assert __debug__ and platform.system() == "Linux"
    operation = {"build": build, "run": run, "archive": archive}[args.pop("action")]
    operation(**args)


if __name__ == "__main__":
    main()
