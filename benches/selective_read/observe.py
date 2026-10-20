"""Capture native MinIO requests and reconcile them before publishing I/O metrics."""

import argparse
from contextlib import nullcontext
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import time

import storage
from storage import save
import run
import native


def metrics(directory, timeout=10):
    return native.call("observe", {"action": "metrics", "directory": str(directory), "timeout": timeout})


def reconcile(before, after, counts, size):
    return native.call("observe", {"action": "reconcile", "before": before, "after": after,
                                  "counts": counts, "size": size})


class Trace:
    """Control the native capture process used by the storage boundary checks."""

    def __init__(self, directory, output, scope, run_id, case_id):
        self.output = output
        request = output / "trace-request.json"
        save(request, {"table_uri": f"s3://{storage.BUCKET}/{scope}", "run_id": run_id, "case_id": case_id})
        self.log = (output / "observer.stderr.log").open("x")
        self.process = subprocess.Popen([str(native.executable()), "selective-read", "capture", "--state", str(directory),
            "--request", str(request), "--output", str(output)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True, start_new_session=True)

    def begin(self):
        ready = self.process.stdout.readline() if select.select([self.process.stdout], [], [], 25)[0] else ""
        if ready.strip() != '{"ready": true}':
            raise RuntimeError("observer failed to become ready; inspect its artifacts")

    def finish(self, deadline=None):
        deadline = time.monotonic() + 60 if deadline is None else deadline
        self.process.communicate(json.dumps({"deadline": deadline}) + "\n", timeout=max(1, deadline - time.monotonic()) + 15)
        proof = json.loads((self.output / "capture.json").read_text())
        if self.process.returncode or proof["status"] != "passed":
            raise RuntimeError(proof.get("failure_reason", "native capture failed"))
        return proof

    def close(self):
        if self.process.poll() is None:
            os.killpg(self.process.pid, 9)
        self.process.wait()
        self.process.stdin.close()
        self.process.stdout.close()
        self.log.close()


def summarize(output, record):
    return native.call("observe", {"action": "summarize", "output": str(output), "record": record})


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
