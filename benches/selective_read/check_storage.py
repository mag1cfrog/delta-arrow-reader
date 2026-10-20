"""Run the native MinIO, remote reader and observer checks."""

import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
import native


if __name__ == "__main__":
    binary = str(native.executable())
    os.execv(binary, [binary, "selective-read", "check-storage", *sys.argv[1:],
                     "--python", sys.executable, "--harness", str(HERE)])
