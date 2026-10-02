"""Capture native MinIO requests and reconcile them before publishing I/O metrics."""

import argparse
from collections import Counter
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import select
import subprocess
import sys
import threading
import time
import uuid
from urllib.parse import parse_qs, unquote, urlsplit

import storage
import network
from storage import digest, save
import run


def timestamp(value):
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.([0-9]{1,9}))?Z", value)
    assert match, "unexpected native timestamp"
    seconds = int(datetime.fromisoformat(match[1]).replace(tzinfo=timezone.utc).timestamp())
    return seconds * 10**9 + int((match[2] or "").ljust(9, "0"))


def metrics(directory, timeout=10):
    endpoint = storage.state(directory)["endpoint"]
    with storage.HTTP.open(endpoint + "/minio/metrics/v3/api/requests", timeout=timeout) as response:
        text = response.read().decode()
    result = {"requests": {}, "response_bytes": 0, "active": 0, "rejected": 0, "canceled": 0}
    found = False
    for line in text.splitlines():
        if not line.startswith("minio_api_requests_"):
            continue
        metric, labels, value = re.fullmatch(r"(\w+)\{(.*)\} ([^ ]+)", line).groups()
        if metric not in ("minio_api_requests_total", "minio_api_requests_traffic_sent_bytes",
                          "minio_api_requests_inflight_total", "minio_api_requests_waiting_total",
                          "minio_api_requests_canceled_total") and not metric.startswith("minio_api_requests_rejected_"):
            continue
        value = Decimal(value)
        assert value == int(value) and 0 <= value < 2**53, "counter cannot be represented exactly"
        value = int(value)
        if metric == "minio_api_requests_total":
            result["requests"][re.search(r'name="([^"]+)"', labels)[1]] = value
        elif metric == "minio_api_requests_traffic_sent_bytes":
            result["response_bytes"] = value
            found = True
        elif metric in ("minio_api_requests_inflight_total", "minio_api_requests_waiting_total"):
            result["active"] += value
        elif metric.startswith("minio_api_requests_rejected_"):
            result["rejected"] += value
        elif metric == "minio_api_requests_canceled_total":
            result["canceled"] += value
    assert found, "MinIO traffic counter missing"
    return result


def delta(before, after):
    counts = Counter(after["requests"])
    counts.subtract(before["requests"])
    assert all(n >= 0 for n in counts.values()) and all(after[k] >= before[k] for k in ("response_bytes", "rejected", "canceled")), "server counters reset"
    return {"requests": +counts, "response_bytes": after["response_bytes"] - before["response_bytes"],
            "rejected": after["rejected"] - before["rejected"], "canceled": after["canceled"] - before["canceled"]}


def reconcile(before, after, counts, size):
    expected = delta(before, after)
    if after["active"] or expected["rejected"] or expected["requests"] != +counts or expected["response_bytes"] != size:
        raise ValueError("trace differs from native request/byte counters or server is not idle")
    return expected


def sanitize(raw, scope, run_id, case_id):
    http = raw["http"]
    req, resp = http["request"], http["response"]
    path = unquote(req["path"])
    root = "/" + storage.BUCKET
    query = parse_qs(req.get("rawquery", ""), keep_blank_values=True)
    key = path.removeprefix(root + "/") if path.startswith(root + "/") else ""
    prefix = query.get("prefix", [""])[0]
    if path not in (root, root + "/") and not key.startswith(scope):
        raise ValueError("unrelated object request")
    if prefix and not prefix.startswith(scope):
        raise ValueError("unrelated listing request")
    if raw["funcname"].startswith("s3.List") and not prefix:
        raise ValueError("unscoped listing request")
    headers = {k.lower(): v for k, v in req.get("headers", {}).items()}
    response_headers = {k.lower(): v for k, v in resp.get("headers", {}).items()}
    def header(mapping, name):
        values = mapping.get(name)
        return values[0] if values else None
    api = raw["funcname"].removeprefix("s3.")
    size = http["stats"]["outputbytes"]
    length = header(response_headers, "content-length")
    length = int(length) if length is not None else None
    method = req["method"]
    category = "listing" if api.startswith("List") else "bucket" if not key else "delta_log" if "/_delta_log/" in "/" + key else (
        "parquet" if key.endswith(".parquet") else "deletion_vector" if Path(key).name.startswith("deletion_vector_") else "other")
    sdk_request = header(headers, "amz-sdk-request")
    attempt = re.search(r"(?:^|[; ,])attempt=([0-9]+)", sdk_request or "")
    return {"run_id": run_id, "case_id": case_id, "api": api, "method": method, "object": key,
            "object_class": category, "list_prefix": prefix or None, "range": header(headers, "range"),
            "content_range": header(response_headers, "content-range"), "status": resp["statuscode"],
            "response_bytes": size, "advertised_content_length": length,
            "incomplete_body": method != "HEAD" and length is not None and size < length,
            "request_id": header(response_headers, "x-amz-request-id"),
            "sdk_invocation_id": header(headers, "amz-sdk-invocation-id"),
            "sdk_attempt": int(attempt[1]) if attempt else None,
            "started_ns": timestamp(req["time"]), "ended_ns": timestamp(resp["time"])}


class Trace:
    def __init__(self, directory, output, scope, run_id, case_id):
        self.directory, self.output, self.scope = directory, output, scope
        self.run_id, self.case_id = run_id, case_id
        self.control = f"_observer/{os.getpid()}-{time.time_ns()}/"
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.marks = set()
        self.counts = Counter()
        self.size = 0
        self.error = None
        self.running = True
        self.file = (output / "requests.jsonl").open("x")
        self.process = storage.curl(directory, "/minio/admin/v3/trace?s3=true", "--no-buffer", "--fail")
        self.thread = threading.Thread(target=self.read)
        self.thread.start()

    def read(self):
        try:
            for line in self.process.stdout:
                if not line.strip():
                    continue
                raw = json.loads(line)
                key = unquote(raw["http"]["request"]["path"]).removeprefix("/" + storage.BUCKET + "/")
                with self.condition:
                    if key.startswith(self.control):
                        self.marks.add(key)
                    else:
                        record = sanitize(raw, self.scope, self.run_id, self.case_id)
                        self.file.write(json.dumps(record, separators=(",", ":")) + "\n")
                    self.counts[raw["funcname"].removeprefix("s3.")] += 1
                    self.size += raw["http"]["stats"]["outputbytes"]
                    self.condition.notify_all()
            if self.running:
                raise RuntimeError("native trace disconnected")
        except Exception as error:
            with self.condition:
                self.error = str(error)
                self.condition.notify_all()

    def marker(self, name):
        key = self.control + name
        # HEAD on a unique absent control object contributes one request, zero body
        # bytes. It is accounted for but never reported as reader traffic.
        process = storage.curl(self.directory, "/" + storage.BUCKET + "/" + key, "--head", "--max-time", "10", "--output", "/dev/null")
        _, error = process.communicate()
        assert process.returncode == 0, error
        return key

    def begin(self):
        deadline = time.monotonic() + 20
        # Repeated readiness markers handle subscription setup without a fixed sleep.
        while True:
            key = self.marker("ready-" + str(time.time_ns()))
            with self.condition:
                self.condition.wait_for(lambda: key in self.marks or self.error, timeout=.1)
                if self.error:
                    raise RuntimeError(self.error)
                if key in self.marks:
                    break
            if time.monotonic() >= deadline:
                raise TimeoutError("native trace did not subscribe")
        baseline = metrics(self.directory)
        assert baseline["active"] == 0, "S3 requests were active before capture"
        with self.lock:
            self.counts.clear()
            self.size = 0
        self.before = baseline
        self.started_ns = time.time_ns()

    def finish(self, deadline=None):
        deadline = time.monotonic() + 60 if deadline is None else deadline
        while metrics(self.directory)["active"]:
            if time.monotonic() >= deadline:
                raise TimeoutError("pending S3 requests did not finish")
            time.sleep(.02)
        key = self.marker("finished")
        after = metrics(self.directory)
        while True:
            with self.condition:
                if self.error:
                    raise RuntimeError(self.error)
                try:
                    expected = reconcile(self.before, after, self.counts, self.size)
                    assert key in self.marks
                    break
                except (ValueError, AssertionError):
                    if time.monotonic() >= deadline:
                        raise ValueError("incomplete native trace: request/byte reconciliation failed")
                    self.condition.wait(timeout=.02)
            after = metrics(self.directory)
        expected["requests"]["HeadObject"] -= 1
        return {"status": "passed", "boundary": "MinIO HTTP ResponseWriter accepted body bytes",
                "before": self.before, "after": after, "reader_counters": expected,
                "started_ns": self.started_ns, "drained_ns": time.time_ns(), "control_requests": 1}

    def close(self):
        self.running = False
        if self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.thread.join(timeout=10)
        assert not self.thread.is_alive(), "trace thread did not finish"
        self.file.close()
        self.process.stdout.close()
        self.process.stderr.close()


def capture(directory, output, payload):
    os.sched_setaffinity(0, storage.state(directory)["cpus"]["observer"])
    table = urlsplit(payload["table_uri"])
    assert table.scheme == "s3" and table.netloc == storage.BUCKET
    trace = Trace(directory, output, unquote(table.path).strip("/") + "/", payload["run_id"], payload["case_id"])
    try:
        trace.begin()
        print(json.dumps({"ready": True}), flush=True)
        control = json.loads(sys.stdin.readline())
        summary = trace.finish(control["deadline"])
    except Exception as error:
        summary = {"status": "operational_failure", "failure_reason": str(error)}
    finally:
        try:
            trace.close()
        except Exception as error:
            summary = {"status": "operational_failure", "failure_reason": "trace shutdown failed: " + str(error)}
    summary.update(observer_affinity=sorted(os.sched_getaffinity(0)), observer_source_sha256=digest(Path(__file__)))
    save(output / "capture.json", summary)
    return summary["status"] == "passed"


def summarize(output, record):
    summary = {"requests": 0, "response_bytes": 0, "by_class": {}, "by_api_status": {}, "parquet_get_objects": [],
               "parquet_head_objects": [], "touched_parquet_objects": [], "incomplete_responses": 0,
               "explicit_retry_attempts": 0, "retry_note": "all attempts count; retry identity is unknown without SDK metadata",
               "started_after_final_stream": 0, "ended_after_final_stream": 0, "response_bytes_after_stream": None}
    ends = [e["time_ns"] for e in record.get("diagnostic_events", []) if e["event"] == "stream_complete"]
    last = max(ends) if ends else None
    touched, gets, heads = set(), set(), set()
    with (output / "requests.jsonl").open() as source:
        for line in source:
            item = json.loads(line)
            summary["requests"] += 1
            summary["response_bytes"] += item["response_bytes"]
            category = summary["by_class"].setdefault(item["object_class"], {"requests": 0, "response_bytes": 0})
            category["requests"] += 1
            category["response_bytes"] += item["response_bytes"]
            key = f'{item["api"]}/{item["status"]}'
            summary["by_api_status"][key] = summary["by_api_status"].get(key, 0) + 1
            if item["object_class"] == "parquet":
                touched.add(item["object"])
                if item["method"] == "GET": gets.add(item["object"])
                if item["method"] == "HEAD": heads.add(item["object"])
            summary["incomplete_responses"] += item["incomplete_body"]
            summary["explicit_retry_attempts"] += (item["sdk_attempt"] or 1) > 1
            summary["started_after_final_stream"] += last is not None and item["started_ns"] >= last
            summary["ended_after_final_stream"] += last is not None and item["ended_ns"] > last
    summary.update(touched_parquet_objects=sorted(touched), parquet_get_objects=sorted(gets), parquet_head_objects=sorted(heads),
                   last_stream_complete_ns=last, post_stream_byte_reason="request totals cannot split a response that crosses the stream boundary")
    return summary


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
    assert not traced or payload["purpose"] == "io", "detailed tracing requires an I/O diagnostic without plan export"
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
            observer = subprocess.Popen([sys.executable, "-B", str(Path(__file__).resolve()), "capture", "--state", str(directory.resolve()),
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
            run.check_result(result, output, fixtures, reference)
        # Preserve the reader's raw record and the shared helper's observation.
        save(output / "storage-observation.json", result)
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("capture", "run"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--fixtures", type=Path)
    parser.add_argument("--reference", type=Path)
    args = parser.parse_args()
    payload = json.loads(args.request.read_text())
    if args.command == "capture":
        sys.exit(0 if capture(args.state, args.output, payload) else 1)
    if args.binary is None or (payload["purpose"] == "validation" and (args.fixtures is None or args.reference is None)):
        parser.error("run requires --binary; validation also requires --fixtures and --reference")
    result = invoke(args.state, args.binary, payload, args.output, args.fixtures, args.reference)
    print(json.dumps({"status": result["status"], "observation": str(args.output / "storage-observation.json")}))
    sys.exit(0 if result["status"] == "success" else 1)
