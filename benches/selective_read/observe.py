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
import uuid
from urllib.parse import urlsplit

import storage
import network
from storage import digest, save
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


def drain(directory, unit, deadline):
    """Prove both the reader scope and server requests have stopped before reuse."""
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError("reader scope or server requests exceeded the cleanup deadline")
        name = storage.properties(unit)["ControlGroup"]
        group = Path("/sys/fs/cgroup" + name) if name else None
        try:
            populated = group is not None and "populated 1" in (group / "cgroup.events").read_text()
        except FileNotFoundError:
            populated = False
        proxy = network.control(directory, timeout=min(10, max(.001, deadline - time.monotonic())))
        if not populated and not (proxy or {}).get("active", 0) and not metrics(directory, min(10, max(.001, deadline - time.monotonic())))["active"]:
            if time.monotonic() < deadline:
                return
        time.sleep(.02)


def invoke(directory, binary, payload, output, fixtures=None, reference=None, *, traced=None, _locked=False):
    if traced is None:
        traced = payload["purpose"] == "io"
    assert not traced or payload["purpose"] in ("io", "diagnostic") or (
        payload["purpose"] == "validation" and payload.get("validation_diagnostics") is True
        and payload.get("comparison_revision") == 6), "detailed tracing requires an untimed diagnostic or declared exact gate"
    config = storage.state(directory)
    table = urlsplit(payload["table_uri"])
    assert table.scheme == "s3" and table.netloc == storage.BUCKET and table.path.strip("/")
    with nullcontext() if _locked else storage.exclusive(directory):
        storage.verify_server(directory)
        assert metrics(directory)["active"] == 0, "pending server requests before reader launch"
        if output.exists():
            raise FileExistsError(output)
        network.control(directory, reset=True, traced=traced)
        observer = None
        # The shared helper creates the invocation directory. The observer has a
        # separate new sibling directory, so even startup failure is retained.
        trace_output = output.with_name(output.name + "-io")
        trace_output.mkdir()
        save(trace_output / "request.json", payload)
        unit = "selective-read-" + uuid.uuid4().hex + ".scope"
        deadline = time.monotonic() + 60
        if traced:
            log = (trace_output / "observer.stderr.log").open("x")
            observer = subprocess.Popen([str(native.executable()), "selective-read", "capture", "--state", str(directory.resolve()),
                "--request", str((trace_output / "request.json").resolve()), "--output", str(trace_output.resolve())],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True, start_new_session=True)
            ready = observer.stdout.readline() if select.select([observer.stdout], [], [], 25)[0] else ""
            if ready.strip() != '{"ready": true}':
                os.killpg(observer.pid, 15) if observer.poll() is None else None
                observer.wait(); log.close()
                raise RuntimeError("observer failed to become ready; inspect its artifacts")
        try:
            result = run.invoke(binary, payload, output, fixtures, reference, env=storage.reader_environment(directory),
                                command_prefix=storage.reader_prefix(directory, unit), supervised=True, defer_validation=True)
            started = result.get("supervision", {}).get("cleanup_started_monotonic_ns")
            deadline = (started / 10**9 if started is not None else time.monotonic()) + 60
            if result.get("supervision", {}).get("returncode", 0) is None:
                deadline = time.monotonic()  # The watchdog already exhausted process cleanup.
            if (output / "limits.json").exists():
                limits = json.loads((output / "limits.json").read_text())
                result["external_resource_limits"] = limits
            if result["status"] != "success":
                subprocess.run(["systemctl", "--user", "kill", "--signal=KILL", unit], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                drain(directory, unit, deadline)
                result["cleanup"] = {"status": "passed", "scope_unit": unit, "drained_monotonic_ns": time.monotonic_ns()}
            except (OSError, ValueError, AssertionError, subprocess.SubprocessError) as error:
                subprocess.run(["systemctl", "--user", "kill", "--signal=KILL", unit], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                result["cleanup"] = {"status": "operational_failure", "scope_unit": unit, "failure_reason": str(error)}
                result.update(status="timeout" if isinstance(error, TimeoutError) else "operational_failure", failure_reason=str(error))
        except Exception as error:
            subprocess.run(["systemctl", "--user", "kill", "--signal=KILL", unit], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            result = {"status": "operational_failure", "failure_reason": type(error).__name__ + ": " + str(error),
                      "cleanup": {"status": "unverified", "scope_unit": unit}}
            output.mkdir(exist_ok=True)
        finally:
            if observer is not None:
                try:
                    observer.communicate(json.dumps({"deadline": deadline}) + "\n",
                                         timeout=max(1, deadline - time.monotonic()) + 15)
                except subprocess.TimeoutExpired:
                    os.killpg(observer.pid, 9)
                    observer.wait()
                finally:
                    log.close()
        result["storage_environment"] = {"server_record": str((directory / "server.json").resolve()),
            "server_sha256": digest(directory / "server.json"), "trace_enabled": traced,
            "source_sha256": {p.name: digest(p) for p in (Path(__file__), Path(storage.__file__), Path(run.__file__))},
            "reader_affinity": config["cpus"]["reader"], "server_affinity": config["cpus"]["server"], "observer_affinity": config["cpus"]["observer"]}
        if network.config(directory) is not None:
            result["storage_environment"]["network"] = network.config(directory)
            result["storage_environment"]["source_sha256"]["network.py"] = digest(Path(network.__file__))
        result.setdefault("external_resource_limits", {"status": "unverified", "reason": "reader launcher did not record effective limits"})
        if traced:
            proof = json.loads((trace_output / "capture.json").read_text()) if (trace_output / "capture.json").exists() else {
                "status": "operational_failure", "failure_reason": "observer exited without a capture record"}
            result["storage_capture"] = proof
            if proof["status"] != "passed" or observer.returncode:
                result.update(status="operational_failure", failure_reason="storage capture failed reconciliation")
            else:
                io = summarize(trace_output, result)
                assert io["response_bytes"] == proof["reader_counters"]["response_bytes"]
                assert io["requests"] == sum(proof["reader_counters"]["requests"].values())
                result["storage_io"] = io
                result.setdefault("external_metrics", {}).update(requests=io["requests"], response_bytes=io["response_bytes"],
                    touched_parquet_objects=len(io["touched_parquet_objects"]), reason=None)
        else:
            result["storage_capture"] = {"status": "disabled", "reason": "no detailed tracing in this invocation"}
            result.setdefault("external_metrics", {})["reason"] = "request/byte metrics require a separate I/O diagnostic"
        try:
            transport = network.finish(directory, trace_output)
            if transport is not None:
                result["network_io"] = transport
                if traced:
                    result.setdefault("external_metrics", {}).update(requests=transport["requests"],
                        response_bytes=transport["response_bytes"], response_bytes_boundary=network.BOUNDARY)
        except (OSError, ValueError, AssertionError) as error:
            result.update(status="operational_failure", failure_reason="proxy capture failed: " + str(error))
        if result["status"] == "success" and payload["purpose"] == "validation":
            run.check_result(result, output, fixtures, reference, payload)
        # Preserve the reader's raw record and the shared helper's observation.
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
