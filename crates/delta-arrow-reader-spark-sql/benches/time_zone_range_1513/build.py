"""Build the release harness with the #1409 row-capture overlay."""

import hashlib
import json
import os
import platform
import runpy
from pathlib import Path
import shutil
import subprocess
import sys

assert platform.system() == "Linux"
workspace, output, target = (Path(arg).resolve() for arg in sys.argv[1:])
output.mkdir()
assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=workspace)
commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=workspace, text=True).strip()
command = ["cargo", "+1.96.0", "build", "--offline", "--locked", "--release",
           "--config", "profile.release.debug=1", "--jobs", "2", "--target-dir",
           str(target), "--bench", "functions", "--message-format=json"]
cargo = shutil.which("cargo")
rustc = shutil.which("rustc")
assert cargo and rustc, "cargo and rustc must be on PATH"
tool_dirs = [str(Path(cargo).parent), str(Path(rustc).parent), os.defpath]
settings = {"HOME": str(Path.home()), "PATH": ":".join(tool_dirs), "TZ": "UTC"}
patch = runpy.run_path(str(workspace / "benches/window_aggregates_1408/instrument.py"))["changes"]
overlay = dict(patch(workspace, False))
original = {file: file.read_bytes() for file in overlay}
assert set(original) == {workspace / "benches/functions.rs", workspace / "tests/support/spark_results.rs"}
encoded_overlay = json.dumps({str(file.relative_to(workspace)): content
                              for file, content in overlay.items()}, sort_keys=True).encode()
expected = "fcba9875a935e6724de0f51a2b5a5e556512ce353ca574de7afd8a05f8ad693c"
assert hashlib.sha256(encoded_overlay).hexdigest() == expected
try:
    for file, content in overlay.items():
        file.write_text(content)
    with subprocess.Popen(command, cwd=workspace, env=settings,
                      stdout=subprocess.PIPE, text=True) as build:
        artifacts = [row["executable"] for row in map(json.loads, build.stdout)
                     if row["reason"] == "compiler-artifact"
                     and row["target"]["name"] == "functions" and row.get("executable")]
        assert build.wait() == 0 and len(artifacts) == 1
finally:
    for file, content in original.items():
        file.write_bytes(content)
assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=workspace)
binary = output / "functions"
shutil.copy2(artifacts[0], binary)
toolchain = subprocess.check_output(["rustc", "+1.96.0", "--version"],
                                   env=settings, text=True).strip()
assert toolchain.startswith("rustc 1.96.0 ")
with binary.open("rb") as stream:
    digest = hashlib.file_digest(stream, "sha256").hexdigest()
receipt = {"source_commit": commit, "binary_sha256": digest, "profile": "release",
           "toolchain": toolchain, "debug_info": 1, "target": "functions",
           "observation_overlay_sha256": hashlib.sha256(encoded_overlay).hexdigest()}
(output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(commit, digest)
