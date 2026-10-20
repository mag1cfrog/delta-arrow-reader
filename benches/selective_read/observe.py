"""Capture native MinIO requests and reconcile them before publishing I/O metrics."""

import argparse
from contextlib import nullcontext
import json
from pathlib import Path
import sys

import storage
from storage import save
import run
import native


def invoke(directory, binary, payload, output, fixtures=None, reference=None, *, traced=None, _locked=False):
    with nullcontext() if _locked else storage.exclusive(directory):
        result = native.call("observe", {"action": "invoke", "directory": str(directory),
            "binary": str(binary), "payload": payload, "output": str(output), "traced": traced,
            "reference": str(reference) if reference is not None else None, "protocols": str(run.PROTOCOL.parent)})
        if result["status"] == "success" and payload["purpose"] == "validation":
            run.check_result(result, output, fixtures, reference, payload)
        save(output / "storage-observation.json", result)
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run",))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--fixtures", type=Path)
    parser.add_argument("--reference", type=Path)
    args = parser.parse_args()
    payload = json.loads(args.request.read_text())
    if args.binary is None or (payload["purpose"] == "validation" and (args.fixtures is None or args.reference is None)):
        parser.error("run requires --binary; validation also requires --fixtures and --reference")
    result = invoke(args.state, args.binary, payload, args.output, args.fixtures, args.reference)
    print(json.dumps({"status": result["status"], "observation": str(args.output / "storage-observation.json")}))
    sys.exit(0 if result["status"] == "success" else 1)
