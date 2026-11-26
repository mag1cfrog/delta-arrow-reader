"""Capture call stacks for the 400-column planning statement on macOS."""

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
import planning
from build import digest
from native import REPO, archive, bounded_timeout, check_host, emit, session
from run import source_receipt

MODES = ("spark-off", "spark-on")
ARMS = ("a", "b")


def capture(binary, fixture, destination, mode, deadline):
    check_host()
    destination.mkdir(parents=True)
    report = destination / "sample.txt"
    with subprocess.Popen([binary, "profile", fixture, mode, "stress/wide_400", "physical"],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          env={"PATH": os.defpath, "TZ": "UTC"}) as worker:
        try:
            result = subprocess.run(["/usr/bin/sample", str(worker.pid), "1", "1", "-mayDie", "-file", report],
                                    capture_output=True, timeout=bounded_timeout(deadline, 20))
            worker.communicate(timeout=bounded_timeout(deadline, 30))
        except (subprocess.TimeoutExpired, TimeoutError):
            worker.kill()
            worker.communicate()
            return "timeout", report.read_text() if report.exists() else ""
    status = "ok" if worker.returncode == result.returncode == 0 and report.is_file() else "capture failed"
    return status, report.read_text() if report.exists() else ""


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    for arg in ("protocol", "binary", "output"):
        cli.add_argument(arg, type=lambda p: Path(p).resolve())
    args = cli.parse_args()
    assert not args.output.is_relative_to(REPO)
    receipt = source_receipt(args.binary)
    assert receipt["source"] == "1d74a82d50d68c75c119a5f7f9411e9d81767c0f"
    with session(args.protocol) as binding:
        args.output.mkdir()
        fixture = args.output / "delta"
        planning.cells.prepare_planning(args.binary, fixture)
        names = ((args.output, "EVIDENCE"), (args.binary, "RELEASE"),
                 (REPO, "REPOSITORY"), (Path.home(), "HOME_ROOT"))
        deadline = time.monotonic() + 900
        with archive(args.output / "samples.jsonl.gz") as stream:
            emit(stream, {"kind": "session", **binding, "build": receipt,
                          "protocol_sha256": digest(args.protocol), "sample_seconds": 1, "interval_ms": 1})
            for block in range(1, 10):
                order = 1 if block % 2 else -1
                for mode in MODES[::order]:
                    for arm in ARMS[::order]:
                        path = args.output / f"{block:02}" / mode / arm
                        status, stacks = capture(args.binary / "planning", fixture, path, mode, deadline)
                        emit(stream, {"kind": "process", "block": block, "mode": mode, "arm": arm,
                                      "status": status, "stacks": stacks}, names)
                print(f"wide planning stacks: block {block}/9", flush=True)


if __name__ == "__main__":
    main()
