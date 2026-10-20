"""Compatibility entry point for native partial-page experiments."""

import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
import native

if __name__ == "__main__":
    os.execv(native.executable(), [str(native.executable()), "selective-read", "run-intra-page",
        "--python", sys.executable, "--harness", str(HERE), *sys.argv[1:]])
