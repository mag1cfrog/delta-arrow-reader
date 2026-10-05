"""Prepare the optional Spark Delta pilot from pinned, reusable artifacts."""

import argparse
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import urllib.request
import venv

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(HERE.parent))
from run import digest, save


def prepare(output, java_home, artifacts):
    tracked = [*HERE.glob("*.py"), HERE / "lock.json", HERE.parent / "python_common.py", HERE.parent / "run.py",
               HERE.parents[1] / "oracle.py", *[ROOT / "docs/content/benchmarks" / name for name in (
                   "selective-read-protocol.md", "selective-read-large-workloads.md", "selective-read-sampling.md",
                   "selective-read-production-workloads.md", "selective-read-spark-matrix.md")]]
    hashes = {str(p.relative_to(ROOT)): digest(p) for p in sorted(tracked)}
    lock = json.loads((HERE / "lock.json").read_text())
    if (platform.python_implementation(), platform.python_version(), sys.platform, platform.machine()) != (
            "CPython", lock["python"], "linux", "x86_64"):
        raise ValueError("use CPython 3.14.6 on Linux x86-64")
    release = (java_home / "release").read_text()
    if (f'JAVA_RUNTIME_VERSION="{lock["java_runtime_version"]}"' not in release or
            f'IMPLEMENTOR="{lock["java_implementor"]}"' not in release):
        raise ValueError("use the pinned Temurin JDK")
    output = output.resolve()
    output.mkdir()
    downloads = output / "artifacts"
    downloads.mkdir()
    items = [lock["spark_distribution"], *lock["wheels"], *lock["jars"]]
    for item in items:
        target = downloads / item["filename"]
        if artifacts:
            shutil.copyfile(artifacts / item["filename"], target)
        else:
            with urllib.request.urlopen(item["url"], timeout=60) as source, target.open("xb") as sink:
                shutil.copyfileobj(source, sink)
        if digest(target) != item["sha256"]:
            raise ValueError("artifact checksum mismatch: " + item["filename"])
    with tarfile.open(downloads / lock["spark_distribution"]["filename"]) as archive:
        archive.extractall(output / "unpack", filter="data")
    shutil.move(output / "unpack" / ("pyspark-" + lock["spark_version"]), output / "spark")
    (output / "unpack").rmdir()
    # Spark is supplied by the hashed source tree, not an installed wheel.
    for metadata in (output / "spark").glob("*.egg-info"):
        shutil.rmtree(metadata)
    shutil.copytree(java_home, output / "java", symlinks=True)
    (output / "jars").mkdir()
    for item in lock["jars"]:
        shutil.copyfile(downloads / item["filename"], output / "jars" / item["filename"])
    requirements = output / "requirements.lock"
    requirements.write_text("".join(f"{w['name']}=={w['version']} --hash=sha256:{w['sha256']}\n" for w in lock["wheels"]))
    venv.EnvBuilder(with_pip=False).create(output / "venv")
    python = output / "venv/bin/python"
    command = ["uv", "pip", "sync", "--python", str(python), "--no-index", "--find-links", str(downloads),
               "--require-hashes", "--no-cache", "--link-mode=copy", str(requirements)]
    subprocess.run(command, check=True)
    sources = {"lock.json": HERE / "lock.json", "python_common.py": HERE.parent / "python_common.py",
               "run.py": HERE.parent / "run.py", "oracle.py": HERE.parents[1] / "oracle.py"}
    for name, document in (("protocol.md", "selective-read-protocol.md"),
                           ("large-workloads.md", "selective-read-large-workloads.md"),
                           ("sampling.md", "selective-read-sampling.md"),
                           ("production-workloads.md", "selective-read-production-workloads.md"),
                           ("spark-matrix.md", "selective-read-spark-matrix.md")):
        sources[name] = ROOT / "docs/content/benchmarks" / document
    for name, source in sources.items():
        shutil.copyfile(source, output / name)
    for name in ("conf", "spill"):
        (output / name).mkdir()
    executable = output / "selective-read-spark"
    executable.write_text(f"#!{python} -IB\n" + (HERE / "runner.py").read_text())
    executable.chmod(0o755)
    runtime = json.loads(subprocess.check_output([str(executable), "--describe-build"], text=True))
    if {str(p.relative_to(ROOT)): digest(p) for p in sorted(tracked)} != hashes:
        raise ValueError("source files changed during preparation; use a new output directory")
    save(output / "build.json", {
        "format": "selective-read-build-v1", "reader_id": "spark", "pilot_only": False,
        "harness_git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
        "reader_git_commit": lock["spark_source"], "delta_git_commit": lock["delta_source"],
        "source_sha256": hashes,
        "command": command, "uv_version": subprocess.check_output(["uv", "--version"], text=True).strip(),
        "executable_sha256": digest(executable), "lockfile_sha256": digest(HERE / "lock.json"),
        "bundled_sha256": {name: digest(output / name) for name in sources}, "runtime": runtime,
    })
    print(json.dumps({"executable": str(executable), "build_sha256": digest(output / "build.json")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, help="reuse saved downloads without network access")
    args = parser.parse_args()
    prepare(args.output, args.java_home.resolve(), args.artifacts)
