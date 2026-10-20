"""Run the native reader contract checks with the pinned independent oracle."""

import os
from pathlib import Path
import subprocess
import sys

import native

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import oracle


def check(binaries, fixtures, output):
    command = [str(native.executable()), "selective-read", "check-readers", "--python", sys.executable,
               "--harness", str(HERE), "--fixtures", str(fixtures), "--output", str(output)]
    for binary in binaries:
        command += ["--binary", str(binary)]
    subprocess.run(command, check=True)


if __name__ == "__main__":
    binary = str(native.executable())
    os.execv(binary, [binary, "selective-read", "check-readers", *sys.argv[1:],
                     "--python", sys.executable, "--harness", str(HERE)])
