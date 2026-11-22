"""Build the existing execution harness with the additional interval query."""
import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import subprocess
import sys

from run import workload

assert platform.system() == "Linux"
workspace, output, target = (Path(arg).resolve() for arg in sys.argv[1:])
assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=workspace)
commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=workspace, text=True).strip()
output.mkdir()
bench = workspace / "benches/execution.rs"
original = bench.read_bytes()
original_include = b'include_str!("parsers/queries.json")'
assert original.count(original_include) == 1
overlay = workspace / "benches/interval-1389-round3-overlay.json"
assert not overlay.exists()
workload_manifest, _ = workload()
rendered = original.replace(original_include, b'include_str!("interval-1389-round3-overlay.json")')
command = ["cargo", "+1.96.0", "build", "--offline", "--locked", "--release", "--jobs", "2",
           "--config", "profile.release.debug=1", "--target-dir", str(target),
           "--bench", "execution", "--message-format=json"]
settings = {"PATH": os.environ["PATH"], "HOME": str(Path.home()), "TZ": "UTC"}
try:
    with overlay.open("x") as file:
        json.dump(workload_manifest, file)
    bench.write_bytes(rendered)
    with subprocess.Popen(command, cwd=workspace, env=settings, stdout=subprocess.PIPE, text=True) as build:
        artifacts = [row["executable"] for row in map(json.loads, build.stdout)
                     if row["reason"] == "compiler-artifact"
                     and row["target"]["name"] == "execution" and row.get("executable")]
        assert build.wait() == 0 and len(artifacts) == 1
finally:
    bench.write_bytes(original)
    overlay.unlink(missing_ok=True)
assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=workspace)
binary = output / "execution"
shutil.copy2(artifacts[0], binary)
with binary.open("rb") as file:
    digest = hashlib.file_digest(file, "sha256").hexdigest()
toolchain = subprocess.check_output(["rustc", "+1.96.0", "--version"], env=settings, text=True).strip()
assert toolchain.startswith("rustc 1.96.0 ")
(output / "receipt.json").write_text(json.dumps({
    "source_commit": commit, "sha256": digest, "harness_sha256": hashlib.sha256(rendered).hexdigest(),
    "toolchain": toolchain, "queries": workload_manifest,
}, indent=2) + "\n")
print(commit, digest)
