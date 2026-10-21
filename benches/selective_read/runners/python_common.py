"""Shared Python reader identity, input, and correctness checks."""

import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import sys
import sysconfig
from time import perf_counter_ns as clock, time_ns

import native

HERE = Path(__file__).resolve().parent


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def scan_sql(sql):
    """Split the public scan shape; native parsers handle the typed predicate."""
    match = re.fullmatch(r"SELECT (\*|[a-z_][a-z_0-9]*(?:, [a-z_][a-z_0-9]*)*) FROM bench"
                         r"(?: WHERE (.+?))?(?: LIMIT ([0-9]+))?", sql)
    require(match is not None and ";" not in sql, "expected a canonical SELECT from bench")
    columns, predicate, limit = match.groups()
    return columns.split(", "), predicate, int(limit) if limit is not None else None


def runtime_metadata(engine):
    packages = {dist.metadata["Name"].lower(): {
        "version": dist.version, "files": {str(p): str(dist.locate_file(p)) for p in dist.files if p.suffix != ".pyc"},
        "wheel": dist.read_text("WHEEL"), "requires_dist": dist.requires or [],
    } for dist in importlib.metadata.distributions()}
    metadata = {"python": sys.version, "python_abi": sysconfig.get_config_var("SOABI"),
                "python_build": list(platform.python_build()), "python_config_args": sysconfig.get_config_var("CONFIG_ARGS"),
                "libc": list(platform.libc_ver())}
    return native.call("reader-runtime", {"action": "runtime", "directory": str(HERE), "engine": engine,
        "packages": packages, "metadata": metadata, "executable": f"/proc/{os.getpid()}/exe"})


def checked_build(executable, reader):
    path, build = native.call("reader-runtime", {"action": "checked-build", "executable": str(executable), "reader": reader})
    return Path(path), build


def reader_identity(request, reader, build_path, settings, expression=None):
    return native.call("reader-runtime", {"action": "identity", "request": request, "reader": reader,
        "build_path": str(build_path), "settings": settings, "native_expression": expression})


def export_result(output, index, identity):
    return native.call("reader-runtime", {"action": "export", "output": str(output), "index": index, "identity": identity})


def redact_credentials(message):
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        if os.environ.get(name):
            message = message.replace(os.environ[name], "[redacted]")
    return message


def write_observation(output, record):
    record = native.call("reader-runtime", {"action": "finish", "output": str(output), "record": record})
    print(json.dumps(record))
    return 0 if record["status"] == "success" else 1


def cleanup(record, close=None, failure_prefix="cleanup failed: "):
    start = record.pop("_cleanup_start", clock())
    checkpoint(record, "cleanup")
    if close is not None:
        try:
            close()
        except Exception as error:
            record.update(status="operational_failure", phase="cleanup",
                          failure_reason=failure_prefix + str(error))
    if record["purpose"] == "timing":
        record["cleanup_ns"] = clock() - start
    event(record, "cleanup_complete")


def runner_cli(run, describe_build):
    """Keep the request/output arguments and JSON error contract common to readers."""
    try:
        if sys.argv[1:] == ["--describe-build"]:
            describe_build()
        elif len(sys.argv) == 3:
            sys.exit(run(Path(sys.argv[1]), Path(sys.argv[2])))
        else:
            raise ValueError("expected REQUEST.json NEW_OUTPUT_DIRECTORY")
    except Exception as error:
        print(json.dumps({"status": "invalid_input", "failure_reason": str(error)}), file=sys.stderr)
        sys.exit(1)


def storage_options(reader, uri):
    return native.call("storage", {"action": "reader-options", "reader": reader, "uri": uri})


def correctness(request, identity):
    return native.call("reader-runtime", {"action": "correctness", "request": request, "identity": identity})


def observation(request):
    return native.call("reader-runtime", {"action": "observation", "request": request})


def event(record, name, query_index=None):
    if record["purpose"] in ("diagnostic", "io") or record.get("validation_diagnostics"):
        record["diagnostic_events"].append({"event": name, "time_ns": time_ns(), "query_index": query_index})


def checkpoint(record, phase, query_index=None, query=None):
    """Notify the process watchdog outside query clocks; never emit per-batch I/O."""
    descriptor = os.environ.get("SELECTIVE_READ_CONTROL_FD")
    if descriptor is not None:
        message = {"phase": phase, "query_index": query_index, "query": query,
                   "initialization_ns": record["initialization_ns"]}
        data = (json.dumps(message, separators=(",", ":")) + "\n").encode()
        require(len(data) <= 4096 and os.write(int(descriptor), data) == len(data), "watchdog pipe write failed")
