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
from urllib.parse import urlsplit

from run import BUDGET, LARGE_IDENTITY_FIELDS, SAMPLING_IDENTITY_FIELDS, PRODUCTION_IDENTITY_FIELDS, comparison_identity, digest, query_count, save

HERE = Path(__file__).resolve().parent
REQUEST_FIELDS = set("table_uri snapshot_version case_id canonical_sql comparison_revision protocol_sha256 "
                     "fixture_manifest_sha256 profile execution_mode purpose resource_budget correctness_file "
                     "campaign_id run_id repetition order".split())


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
    return {"reader_id": reader, "reader_build_sha256": digest(build_path), "reader_config_sha256": json_hash(settings),
            **comparison_identity(request),
            **{name: request[name] for name in ("fixture_manifest_sha256", "case_id", "snapshot_version")},
            "canonical_sql_sha256": sha(request["canonical_sql"].encode()), "native_expression_sha256": expression_sha256}


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
    require(isinstance(request, dict), "expected request object")
    extra = set(LARGE_IDENTITY_FIELDS) if request.get("comparison_revision") in (3, 4, 5, 6) else set()
    if request.get("comparison_revision") in (4, 5, 6):
        extra.update(SAMPLING_IDENTITY_FIELDS)
    if request.get("comparison_revision") in (5, 6):
        extra.update(PRODUCTION_IDENTITY_FIELDS)
    if "validation_diagnostics" in request:
        extra.add("validation_diagnostics")
        require(type(request["validation_diagnostics"]) is bool and (not request["validation_diagnostics"] or
                request.get("comparison_revision") == 6 and request.get("purpose") == "validation"),
                "diagnostic validation requires revision 6 and validation purpose")
    require(set(request) == REQUEST_FIELDS | extra, "unknown or missing request fields")
    comparison_identity(request)
    uri = urlsplit(request["table_uri"])
    require(uri.scheme in ("file", "s3") and not (uri.username or uri.password or uri.query or uri.fragment)
            and (uri.scheme != "file" or (uri.netloc in ("", "localhost") and uri.path.startswith("/")))
            and (uri.scheme != "s3" or bool(uri.netloc)), "expected a file/s3 URL without credentials, query or fragment")
    require(type(request["snapshot_version"]) is int and 0 <= request["snapshot_version"] < 2**64,
            "invalid snapshot version")
    require(request["execution_mode"] in ("open", "reuse") and request["purpose"] in ("validation", "timing", "diagnostic", "io")
            and re.fullmatch("[0-9a-f]{64}", request["fixture_manifest_sha256"])
            and request["resource_budget"] == BUDGET, "request differs from the frozen protocol")
    for field in ("case_id", "run_id", "canonical_sql", "profile"):
        require(isinstance(request[field], str) and request[field].strip(), f"invalid {field}")
    for field in ("campaign_id", "correctness_file"):
        require(request[field] is None or isinstance(request[field], str), f"invalid {field}")
    for field in ("repetition", "order"):
        require(request[field] is None or (type(request[field]) is int and request[field] >= 0), f"invalid {field}")


def correctness(request, identity):
    if request["purpose"] != "timing":
        return {"status": "not_checked", "reason": "untimed invocation"}
    require(request["correctness_file"], "timing requires a correctness certificate")
    path = Path(request["correctness_file"])
    proof = json.loads(path.read_text())
    count = query_count(request)
    require(proof["status"] == "passed" and len(proof["checks"]) == count, "failed or incomplete correctness certificate")
    rows = []
    for check in proof["checks"]:
        require(check["status"] == "passed" and check["oracle_sha256"] == digest(HERE / "oracle.py"), "failed or stale oracle check")
        for key, value in identity.items():
            require(type(check.get(key)) is type(value) and check.get(key) == value, f"stale correctness field: {key}")
        require(type(check["output_rows"]) is int and check["output_rows"] >= 0, "missing validated row count")
        rows.append(check["output_rows"])
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
