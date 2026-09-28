"""Prepare a frozen DuckDB runner, including offline extension and wheel artifacts."""

import argparse
import gzip
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import urllib.request
import venv

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(HERE.parent))
from run import digest, save


def prepare(output, artifacts):
    lock = json.loads((HERE / "lock.json").read_text())
    if (platform.python_implementation(), platform.python_version(), sys.platform, platform.machine()) != (
            "CPython", lock["python"], "linux", "x86_64"):
        raise ValueError("use CPython 3.14.6 on Linux x86-64")
    output = output.resolve()
    output.mkdir()
    sources = [*HERE.glob("*.py"), HERE / "lock.json", HERE.parent / "run.py",
               HERE.parent.parent / "oracle.py", ROOT / "docs/content/benchmarks/selective-read-protocol.md"]
    hashes = {str(p.relative_to(ROOT)): digest(p) for p in sorted(sources)}
    downloads = output / "artifacts"
    downloads.mkdir()
    for item in lock["wheels"] + lock["extensions"]:
        dest = downloads / item["filename"]
        if artifacts:
            shutil.copyfile(artifacts / item["filename"], dest)
        else:
            request = urllib.request.Request(item["url"], headers={"User-Agent": "selective-read-benchmark/1"})
            with urllib.request.urlopen(request, timeout=60) as source, dest.open("xb") as target:
                shutil.copyfileobj(source, target)
        if digest(dest) != item["sha256"]:
            raise ValueError(f"artifact checksum mismatch: {item['filename']}")
    requirements = output / "requirements.lock"
    requirements.write_text("".join(f"{w['name']}=={w['version']} --hash=sha256:{w['sha256']}\n" for w in lock["wheels"]))
    venv.EnvBuilder(with_pip=False).create(output / "venv")
    python = output / "venv/bin/python"
    command = ["uv", "pip", "sync", "--python", str(python), "--no-index", "--find-links", str(downloads),
               "--require-hashes", str(requirements)]
    subprocess.run(command, check=True)
    for item in lock["extensions"]:
        dest = output / f"{item['name']}.duckdb_extension"
        with gzip.open(downloads / item["filename"], "rb") as source, dest.open("xb") as target:
            shutil.copyfileobj(source, target)
        if digest(dest) != item["binary_sha256"]:
            raise ValueError(f"extension checksum mismatch: {item['name']}")
    for source, name in ((HERE / "lock.json", "lock.json"), (HERE.parent / "run.py", "run.py"),
                         (HERE.parent.parent / "oracle.py", "oracle.py"),
                         (ROOT / "docs/content/benchmarks/selective-read-protocol.md", "protocol.md")):
        shutil.copyfile(source, output / name)
    executable = output / "selective-read-duckdb"
    executable.write_text(f"#!{python} -I\n" + (HERE / "runner.py").read_text())
    executable.chmod(0o755)
    runtime = json.loads(subprocess.check_output([str(executable), "--describe-build"], text=True))
    if {str(p.relative_to(ROOT)): digest(p) for p in sorted(sources)} != hashes:
        raise ValueError("source files changed during preparation; use a new output directory")
    record = {
        "format": "selective-read-build-v1", "reader_id": "duckdb",
        "harness_git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "reader_git_commit": lock["duckdb_source"],
        "git_status": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
        "source_sha256": hashes, "command": command,
        "uv_version": subprocess.check_output(["uv", "--version"], text=True).strip(),
        "executable_sha256": digest(executable), "lockfile_sha256": digest(HERE / "lock.json"),
        "bundled_sha256": {name: digest(output / name) for name in ("run.py", "oracle.py", "protocol.md")},
        "runtime": runtime,
    }
    save(output / "build.json", record)
    print(json.dumps({"executable": str(executable), "build_sha256": digest(output / "build.json")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, help="reuse previously saved artifacts without downloads")
    args = parser.parse_args()
    prepare(args.output, args.artifacts)
