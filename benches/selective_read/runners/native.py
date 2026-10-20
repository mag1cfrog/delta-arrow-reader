"""Call the compiled benchmark controller without building inside a measured run."""

import json
import os
from pathlib import Path
import subprocess


def executable():
    bundled = Path(__file__).with_name("selective-read-controller")
    if bundled.is_file():
        return bundled
    root = Path(__file__).resolve().parents[3]
    path = Path(os.environ.get("SELECTIVE_READ_XTASK",
                Path(os.environ.get("CARGO_TARGET_DIR", root / "target")) / "debug/xtask")).resolve()
    if not path.is_file():
        raise FileNotFoundError("build the controller with cargo build --locked -p xtask; "
                                "set SELECTIVE_READ_XTASK if using another target directory")
    return path


def call(operation, payload):
    result = subprocess.run([str(executable()), "selective-read", operation],
                            input=json.dumps(payload), text=True, capture_output=True)
    if result.returncode:
        raise ValueError(result.stderr.strip() or f"native controller exited with {result.returncode}")
    return json.loads(result.stdout)
