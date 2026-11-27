"""Build and sample the frozen interval row probe natively on macOS."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "macos_baseline"))
from build import QOS, digest, git
from native import BENCHES, REPO, archive, bounded_timeout, check_host, emit, session

WORKSPACE = Path("crates/delta-arrow-reader-spark-sql")
PROBE = BENCHES / "interval_1389_round3/profile_rows.rs"
NAME = "macos_interval_profile"


def render_probe():
    text = PROBE.read_text()
    loop = "    for _ in 0..256 {"
    result = '    println!("{}", 256 * 8192);'
    assert text.count("fn main()") == text.count(loop) == text.count(result) == 1
    return (text.replace("fn main()", QOS + "fn main()")
            .replace(loop, "    prefer_performance_cores();\n"
                     "    let started = std::time::Instant::now();\n" + loop)
            .replace(result, '    println!(r#"{{"calls":{},"wall_ns":{}}}"#, '
                     '256 * 8192, started.elapsed().as_nanos());'))


def build(args, protocol):
    assert not git(args.worktree, "status", "--porcelain")
    start = git(args.worktree, "rev-parse", "HEAD")
    assert git(args.worktree, "branch", "--show-current") == ""
    workspace = args.worktree / WORKSPACE
    manifest, probe = workspace / "Cargo.toml", workspace / f"benches/{NAME}.rs"
    assert not probe.exists()
    try:
        subprocess.run(["git", "switch", "--detach", protocol["source"]], cwd=args.worktree, check=True)
        original = manifest.read_bytes()
        try:
            text = render_probe()
            probe.write_text(text)
            manifest.write_bytes(original + f'\n[[bench]]\nname = "{NAME}"\nharness = false\n'.encode())
            command = ["cargo", "+1.96.0", "build", "--locked", "--release", "-j", "3",
                       "--config", "profile.release.debug=1", "--target-dir", args.target,
                       "--bench", NAME, "--message-format=json"]
            completed = subprocess.run(command, cwd=workspace, stdout=subprocess.PIPE,
                                       text=True, timeout=1800, check=True)
            artifacts = [row["executable"] for line in completed.stdout.splitlines()
                         if (row := json.loads(line))["reason"] == "compiler-artifact"
                         and row["target"]["name"] == NAME and row.get("executable")]
            executable, = artifacts
            args.output.mkdir()
            sealed = args.output / NAME
            shutil.copy2(executable, sealed)
            receipt = {"source": protocol["source"], "rustc": "1.96.0", "profile": "release",
                       "frozen_probe_sha256": digest(PROBE), "probe_sha256": digest(probe),
                       "executable_sha256": digest(sealed)}
            (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        finally:
            manifest.write_bytes(original)
            probe.unlink(missing_ok=True)
    finally:
        assert not git(args.worktree, "status", "--porcelain")
        subprocess.run(["git", "switch", "--detach", start], cwd=args.worktree, check=True)


def capture(binary, shape, work, deadline, protocol):
    check_host()
    work.mkdir(parents=True)
    path = work / "sample.txt"
    with subprocess.Popen([binary, shape], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True, env={"PATH": os.defpath, "TZ": "UTC"}) as worker:
        try:
            sampler = subprocess.run(["/usr/bin/sample", str(worker.pid), str(protocol["sample_seconds"]),
                                      str(protocol["sample_interval_ms"]), "-mayDie", "-file", path],
                                     capture_output=True, timeout=bounded_timeout(deadline, 15))
            stdout, _ = worker.communicate(timeout=bounded_timeout(deadline, 20))
            status = "ok" if worker.returncode == sampler.returncode == 0 and path.is_file() else "failed"
        except (subprocess.TimeoutExpired, TimeoutError):
            if worker.poll() is None:
                worker.kill()
            stdout, _ = worker.communicate()
            status = "timeout"
    result = json.loads(stdout) if status == "ok" else None
    if result is not None and (result.get("calls") != protocol["calls_per_process"] or not result.get("wall_ns")):
        status = "invalid call count"
    return {"status": status, "result": result, "stacks": path.read_text() if path.is_file() else ""}


def measure(args, protocol):
    binary = args.build / NAME
    receipt = json.loads((args.build / "receipt.json").read_text())
    assert receipt["source"] == protocol["source"] and receipt["executable_sha256"] == digest(binary)
    assert receipt["frozen_probe_sha256"] == protocol["frozen_probe_sha256"]
    assert protocol["blocks"] >= 9
    with session(args.protocol) as binding:
        args.output.mkdir()
        names = ((args.output, "EVIDENCE"), (args.build, "RELEASE"),
                 (REPO, "REPOSITORY"), (Path.home(), "HOME_ROOT"))
        deadline = time.monotonic() + protocol["max_seconds"]
        with archive(args.output / "samples.jsonl.gz") as stream:
            emit(stream, {"kind": "session", **binding, "build": receipt,
                          "protocol_sha256": digest(args.protocol)})
            for block in range(1, protocol["blocks"] + 1):
                order = 1 if block % 2 else -1
                for shape in protocol["shapes"][::order]:
                    for arm in ("a", "b")[::order]:
                        work = args.output / f"{block:02}" / shape / arm
                        sample = capture(binary, shape, work, deadline, protocol)
                        emit(stream, {"kind": "process", "block": block, "shape": shape,
                                      "arm": arm, **sample}, names)
                print(f"interval profile: block {block}/{protocol['blocks']}", flush=True)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest="action", required=True)
    for action, names in (("build", ("worktree", "target")), ("measure", ("build",))):
        command = commands.add_parser(action)
        for name in ("protocol", *names, "output"):
            command.add_argument(name, type=lambda path: Path(path).resolve())
    args = cli.parse_args()
    assert not args.output.is_relative_to(REPO)
    assert git(REPO, "show", f"HEAD:{args.protocol.relative_to(REPO)}") == args.protocol.read_text().strip()
    protocol = json.loads(args.protocol.read_text())
    assert digest(PROBE) == protocol["frozen_probe_sha256"]
    (build if args.action == "build" else measure)(args, protocol)


if __name__ == "__main__":
    main()
