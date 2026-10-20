"""Run the native reader contract checks with the pinned independent oracle."""

import os
from pathlib import Path
import sys

import native

HERE = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    binary = str(native.executable())
    os.execv(binary, [binary, "selective-read", "check-readers", *sys.argv[1:],
                     "--python", sys.executable, "--harness", str(HERE)])
