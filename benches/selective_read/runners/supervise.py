"""Linux process watchdog and per-child wait4 resource accounting."""

import json
import os
import select
import signal
import subprocess
import time

QUERY_SECONDS = 1800
CLEANUP_SECONDS = 60


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def integer(value):
    return type(value) is int and value >= 0


class Progress:
    def __init__(self, payload, query_seconds, cleanup_seconds):
        self.count = 10 if payload["execution_mode"] == "reuse" else 1
        self.timed = payload["purpose"] == "timing"
        self.query_seconds, self.cleanup_seconds = query_seconds, cleanup_seconds
        self.phase = "setup"
        self.queries = []
        self.initialization_ns = None
        self.cleanup_started_ns = None
        self.deadline = time.monotonic() + query_seconds

    def receive(self, message):
        require(isinstance(message, dict) and set(message) == {"phase", "query_index", "query", "initialization_ns"}, "malformed watchdog message")
        phase, index, query = message["phase"], message["query_index"], message["query"]
        if phase == "cleanup":
            require(index is None and query is None, "malformed cleanup message")
            if self.phase == "cleanup":
                return  # Final stream completion already started this deadline.
        elif phase in ("open", "initialization"):
            require(self.phase == "setup" and phase == ("open" if self.count == 1 else "initialization")
                    and index is None and query is None, "invalid snapshot phase")
        elif phase == "query":
            require(self.count == 10 and self.phase in ("initialization", "between") and integer(index)
                    and index == len(self.queries) < self.count and query is None, "invalid query sequence")
            self.initialization_ns = message["initialization_ns"]
            if self.timed:
                require(integer(self.initialization_ns), "missing initialization time")
                if self.initialization_ns > self.query_seconds * 10**9:
                    raise TimeoutError("initialization exceeded deadline")
        elif phase == "query_end":
            require(self.phase in ("open", "query") and integer(index) and index == len(self.queries)
                    and isinstance(query, dict) and set(query) == {"query_index", "output_rows", "output_batches", "completion_ns", "first_batch_ns"}, "invalid completed query")
            require(query["query_index"] == index and integer(query["output_rows"]) and integer(query["output_batches"]), "invalid query counts")
            if self.timed:
                require(integer(query["completion_ns"]) and (query["first_batch_ns"] is None or
                    integer(query["first_batch_ns"]) and query["first_batch_ns"] <= query["completion_ns"]), "invalid query clocks")
                if query["completion_ns"] > self.query_seconds * 10**9:
                    raise TimeoutError("query exceeded deadline")
            else:
                require(query["completion_ns"] is None and query["first_batch_ns"] is None, "untimed query has timing values")
            self.queries.append(query)
            phase = "cleanup" if len(self.queries) == self.count else "between"
        else:
            raise ValueError("unknown watchdog phase")
        self.phase = phase
        if phase == "cleanup":
            self.cleanup_started_ns = time.monotonic_ns()
        self.deadline = time.monotonic() + (self.cleanup_seconds if phase in ("cleanup", "between") else self.query_seconds)


def launch(command, payload, output, stdout, stderr, env=None, *, query_seconds=QUERY_SECONDS, cleanup_seconds=CLEANUP_SECONDS):
    progress = Progress(payload, query_seconds, cleanup_seconds)
    readfd, writefd = os.pipe()
    os.set_blocking(readfd, False)
    environment = dict(os.environ if env is None else env, SELECTIVE_READ_CONTROL_FD=str(writefd))
    process = None
    pending = b""
    failure = None
    kill_deadline = None
    usage = None
    pipe_open = True
    start = time.monotonic_ns()
    with (output / "progress.jsonl").open("xb") as events:
        def drain():
            nonlocal pending, pipe_open, failure
            while pipe_open:
                try:
                    chunk = os.read(readfd, 65536)
                except BlockingIOError:
                    return
                if not chunk:
                    pipe_open = False
                    if pending and failure is None:
                        failure = {"status": "operational_failure", "failure_reason": "truncated watchdog message"}
                    return
                events.write(chunk)
                events.flush()
                if failure:
                    continue
                pending += chunk
                try:
                    while b"\n" in pending:
                        line, pending = pending.split(b"\n", 1)
                        require(len(line) <= 4096, "oversized watchdog message")
                        progress.receive(json.loads(line))
                    require(len(pending) <= 4096, "oversized watchdog message")
                except (ValueError, TypeError, KeyError, TimeoutError) as error:
                    failure = {"status": "timeout" if isinstance(error, TimeoutError) else "operational_failure", "failure_reason": str(error)}

        try:
            process = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=environment,
                                       pass_fds=(writefd,), start_new_session=True)
            os.close(writefd)
            writefd = None
            while True:
                drain()
                pid, status, reaped_usage = os.wait4(process.pid, os.WNOHANG)
                if pid:
                    usage = reaped_usage
                    process.returncode = os.waitstatus_to_exitcode(status)
                    drain()  # Include events written between the first drain and exit.
                    break
                if failure is None and time.monotonic() >= progress.deadline:
                    failure = {"status": "timeout", "failure_reason": f"{progress.phase} exceeded its deadline"}
                if failure:
                    if kill_deadline is None:
                        if progress.cleanup_started_ns is None:
                            progress.cleanup_started_ns = time.monotonic_ns()
                        # Reap a killed child for resource accounting. This grace
                        # never extends the original successful-cleanup deadline.
                        kill_deadline = time.monotonic() + cleanup_seconds
                    elif time.monotonic() >= kill_deadline:
                        failure = {"status": "timeout", "failure_reason": "reader did not exit after SIGKILL; cleanup unverified"}
                        break
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                if pipe_open:
                    select.select([readfd], [], [], min(.02, max(0, progress.deadline - time.monotonic())))
                else:
                    time.sleep(.01)
        finally:
            os.close(readfd)
            if writefd is not None:
                os.close(writefd)
            if process is not None and process.returncode is None and kill_deadline is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=cleanup_seconds)
                except subprocess.TimeoutExpired:
                    pass
    return {"status": "success", "failure_reason": None, **(failure or {}), "returncode": process.returncode,
            "last_phase": progress.phase, "completed_queries": progress.queries,
            "initialization_ns": progress.initialization_ns, "cleanup_started_monotonic_ns": progress.cleanup_started_ns,
            "query_deadline_seconds": query_seconds, "cleanup_deadline_seconds": cleanup_seconds,
            "process_elapsed_ns": time.monotonic_ns() - start,
            "process_user_cpu_ns": round(usage.ru_utime * 10**9) if usage else None,
            "process_system_cpu_ns": round(usage.ru_stime * 10**9) if usage else None,
            "process_cpu_ns": round((usage.ru_utime + usage.ru_stime) * 10**9) if usage else None,
            "peak_rss_bytes": usage.ru_maxrss * 1024 if usage else None,
            "resource_unavailable_reason": None if usage else "reader did not exit; wait4 accounting unavailable",
            "resource_boundary": "Linux wait4 for the whole reader process including startup/shutdown; waited descendants contribute CPU and maximum individual RSS",
            "cpu_resolution_ns": 1000, "rss_resolution_bytes": 1024}
