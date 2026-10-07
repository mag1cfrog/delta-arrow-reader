"""Prepare the Python readers from their complete wheel and extension locks."""

import argparse
import gzip
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
ROOT = HERE.parents[2]
from run import digest, save

DOCUMENTS = ("protocol", "large-workloads", "sampling", "production-workloads", "spark-matrix")


def prepare(adapter, output, artifacts, java_home=None):
    reader = adapter.name
    spark = reader == "spark"
    bundled = {"lock.json": adapter / "lock.json", "run.py": HERE / "run.py",
               "python_common.py": HERE / "python_common.py", "oracle.py": HERE.parent / "oracle.py",
               **{name + ".md": ROOT / "docs/content/benchmarks" / f"selective-read-{name}.md" for name in DOCUMENTS}}
    sources = [*adapter.glob("*.py"), *bundled.values(), HERE / "check.py", HERE / "capabilities.py", Path(__file__)]
    hashes = {str(p.relative_to(ROOT)): digest(p) for p in sorted(sources)}
    lock = json.loads((adapter / "lock.json").read_text())
    if (platform.python_implementation(), platform.python_version(), sys.platform, platform.machine()) != (
            "CPython", lock["python"], "linux", "x86_64"):
        raise ValueError("use CPython 3.14.6 on Linux x86-64")
    if spark:
        release = (java_home / "release").read_text()
        if (f'JAVA_RUNTIME_VERSION="{lock["java_runtime_version"]}"' not in release or
                f'IMPLEMENTOR="{lock["java_implementor"]}"' not in release):
            raise ValueError("use the pinned Temurin JDK")
    output = output.resolve()
    output.mkdir()
    downloads = output / "artifacts"
    downloads.mkdir()
    items = ([lock["spark_distribution"], *lock["wheels"], *lock["jars"]] if spark else
             lock["wheels"] + lock.get("extensions", []) + lock.get("source_artifacts", []))
    for item in items:
        dest = downloads / item["filename"]
        if artifacts:
            shutil.copyfile(artifacts / item["filename"], dest)
        else:
            request = (item["url"] if spark else
                       urllib.request.Request(item["url"], headers={"User-Agent": "selective-read-benchmark/1"}))
            with urllib.request.urlopen(request, timeout=60) as source, dest.open("xb") as target:
                shutil.copyfileobj(source, target)
        if digest(dest) != item["sha256"]:
            raise ValueError(f"artifact checksum mismatch: {item['filename']}")
    if spark:
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
               "--require-hashes", *(["--no-cache", "--link-mode=copy"] if spark else []), str(requirements)]
    subprocess.run(command, check=True)
    for item in lock.get("extensions", []):
        dest = output / f"{item['name']}.duckdb_extension"
        with gzip.open(downloads / item["filename"], "rb") as source, dest.open("xb") as target:
            shutil.copyfileobj(source, target)
        if digest(dest) != item["binary_sha256"]:
            raise ValueError(f"extension checksum mismatch: {item['name']}")
    for name, source in bundled.items():
        shutil.copyfile(source, output / name)
    if spark:
        for name in ("conf", "spill"):
            (output / name).mkdir()
    executable = output / f"selective-read-{reader}"
    executable.write_text(f"#!{python} {'-IB' if spark else '-I'}\n" + (adapter / "runner.py").read_text())
    executable.chmod(0o755)
    runtime = json.loads(subprocess.check_output([str(executable), "--describe-build"], text=True))
    if {str(p.relative_to(ROOT)): digest(p) for p in sorted(sources)} != hashes:
        raise ValueError("source files changed during preparation; use a new output directory")
    record = {
        "format": "selective-read-build-v1", "reader_id": reader,
        **({"pilot_only": False, "delta_git_commit": lock["delta_source"]} if spark else {}),
        "harness_git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "reader_git_commit": lock[f"{reader}_source"],
        "git_status": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
        "source_sha256": hashes, "command": command,
        "uv_version": subprocess.check_output(["uv", "--version"], text=True).strip(),
        "executable_sha256": digest(executable), "lockfile_sha256": digest(adapter / "lock.json"),
        "bundled_sha256": {name: digest(output / name) for name in bundled if spark or name != "lock.json"},
        "runtime": runtime,
    }
    save(output / "build.json", record)
    print(json.dumps({"executable": str(executable), "build_sha256": digest(output / "build.json")}))


def prepare_cli(adapter, description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--output", type=Path, required=True)
    if adapter.name == "spark":
        parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, help=("reuse saved downloads without network access" if adapter.name == "spark"
                                                    else "reuse saved artifacts without downloads"))
    args = parser.parse_args()
    prepare(adapter, args.output, args.artifacts, args.java_home.resolve() if adapter.name == "spark" else None)
