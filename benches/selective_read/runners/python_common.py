"""Shared Python reader identity, input, and correctness checks."""

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import sys
import sysconfig
from time import time_ns

import native
from run import digest, query_count, save

HERE = Path(__file__).resolve().parent


def sha(value):
    return hashlib.sha256(value).hexdigest()


def json_hash(value):
    return sha(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


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
    """Hash installed files, not just the package's self-reported version or RECORD."""
    packages = {}
    for dist in importlib.metadata.distributions():
        files = {str(p): digest(Path(dist.locate_file(p))) for p in dist.files
                 if p.suffix != ".pyc" and Path(dist.locate_file(p)).is_file()}
        packages[dist.metadata["Name"].lower()] = {
            "version": dist.version, "installed_files_sha256": json_hash(files),
            "wheel": dist.read_text("WHEEL"), "requires_dist": dist.requires or [],
        }
    lock = json.loads((HERE / "lock.json").read_text())
    require({k: v["version"] for k, v in packages.items()} == {w["name"]: w["version"] for w in lock["wheels"]},
            "installed packages differ from the lock")
    return {**engine, "packages": packages, "python": sys.version, "python_executable_sha256": digest(Path("/proc/self/exe")),
            "python_abi": sysconfig.get_config_var("SOABI"), "python_build": list(platform.python_build()),
            "python_config_args": sysconfig.get_config_var("CONFIG_ARGS"), "libc": list(platform.libc_ver())}


def checked_build(executable, reader):
    directory = executable.resolve().parent
    path = directory / "build.json"
    build = json.loads(path.read_text())
    require(build["reader_id"] == reader and build["executable_sha256"] == digest(executable)
            and build["lockfile_sha256"] == digest(directory / "lock.json")
            and all(digest(directory / name) == value for name, value in build["bundled_sha256"].items()),
            "stale runner build")
    return path, build


def reader_identity(request, reader, build_path, settings, expression_sha256=None):
    return native.call("reader-contract", {"action": "identity", "value": request, "reader": reader,
        "build": json.loads(build_path.read_text()), "hashes": {"reader_build_sha256": digest(build_path),
        "reader_config_sha256": json_hash(settings), "canonical_sql_sha256": sha(request["canonical_sql"].encode()),
        "native_expression_sha256": expression_sha256}})


def timing_totals(record):
    """Summarize captured query clocks without starting or ending any timer."""
    durations = [query["completion_ns"] for query in record["queries"]]
    if record["execution_mode"] == "reuse":
        record["initialization_plus_query1_ns"] = record["initialization_ns"] + durations[0]
        record["initialization_plus_all_queries_ns"] = record["initialization_ns"] + sum(durations)
    else:
        record["open_query_ns"] = durations[0]


def redact_credentials(message):
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        if os.environ.get(name):
            message = message.replace(os.environ[name], "[redacted]")
    return message


def write_observation(output, record):
    save(output / "record.json", record)
    print(json.dumps(record))
    return 0 if record["status"] == "success" else 1


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


def validate(request):
    native.call("reader-contract", {"action": "validate", "value": request})


def correctness(request, identity):
    if request["purpose"] != "timing":
        return {"status": "not_checked", "reason": "untimed invocation"}
    require(request["correctness_file"], "timing requires a correctness certificate")
    path = Path(request["correctness_file"])
    rows = native.call("reader-contract", {"action": "correctness", "proof": json.loads(path.read_text()),
        "identity": identity, "query_count": query_count(request)})
    return {"status": "passed", "path": str(path), "artifact_sha256": digest(path), "expected_output_rows": rows}


def observation(request):
    return {"format": "selective-read-observation-v1", "status": "success", "failure_reason": None,
            "native_phases": {"planning_ns": None, "scan_ns": None,
                              "unavailable_reason": "native APIs do not expose comparable separate planning/scan clocks"},
            "diagnostic_events": [], "diagnostic_session_ns": None,
            **({"validation_diagnostics": True} if request.get("validation_diagnostics") else {}),
            "phase": "setup", "queries": [], "partial_query": None, "provider_evidence": None,
            "capability": {"status": "not_checked", "scope": "requested query and snapshot"}, "correctness": None,
            **{name: None for name in ("open_query_ns", "initialization_ns", "session_elapsed_ns", "cleanup_ns",
                                      "initialization_plus_query1_ns", "initialization_plus_all_queries_ns")},
            **{name: request[name] for name in ("campaign_id", "run_id", "repetition", "order", "profile", "purpose",
                                               "execution_mode", "table_uri", "canonical_sql")},
            "external_metrics": {"requests": None, "response_bytes": None, "touched_parquet_objects": None,
                                 "process_cpu_ns": None, "peak_rss_bytes": None,
                                 "reason": "storage observer and process scheduler are separate roadmap slices"},
            "external_resource_limits": {"cpu_affinity": None, "process_memory_bytes": None,
                                         "reason": "launcher must enforce and record the CPU affinity and process memory limit"}}


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
