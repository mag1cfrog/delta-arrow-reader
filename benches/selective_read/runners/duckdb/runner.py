"""Pinned DuckDB Delta adapter for the shared selective-read request/record contract."""

from contextlib import ExitStack
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import sys
import sysconfig
from time import perf_counter_ns as clock
from urllib.parse import urlsplit

import duckdb
import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from run import BUDGET, digest, save

CONFIG = {"threads": 8, "memory_limit": "4GiB", "enable_external_file_cache": False,
          "autoload_known_extensions": False, "autoinstall_known_extensions": False,
          "allow_persistent_secrets": False, "python_enable_replacements": False}
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


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def connect():
    # The extension's default Tokio executor honors this before initialization.
    os.environ["TOKIO_WORKER_THREADS"] = "8"
    lock = json.loads((HERE / "lock.json").read_text())
    require(platform.python_implementation() == "CPython" and platform.python_version() == lock["python"],
            "wrong Python interpreter")
    connection = duckdb.connect(config=CONFIG | {"temp_directory": str(HERE / "spill")})
    try:
        for item in lock["extensions"]:
            path = HERE / f"{item['name']}.duckdb_extension"
            require(digest(path) == item["binary_sha256"], f"extension checksum mismatch: {item['name']}")
            connection.load_extension(str(path))
        version = connection.sql("PRAGMA version").fetchone()
        abi = connection.sql("PRAGMA platform").fetchone()[0]
        require(version[0] == lock["duckdb_abi"] and version[1] == lock["duckdb_source"][:10]
                and abi == lock["platform"], "wrong DuckDB build or ABI")
        extensions = dict(connection.sql(
            "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded").fetchall())
        expected = dict.fromkeys(lock["builtin_extensions"], lock["duckdb_abi"])
        expected.update({item["name"]: item["extension_version"] for item in lock["extensions"]})
        require(extensions == expected, "loaded extensions differ from the lock")
        return connection, {"duckdb_version": list(version), "extension_abi": abi, "loaded_extensions": extensions}
    except Exception:
        connection.close()
        raise


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


def validate(request):
    require(isinstance(request, dict) and set(request) == REQUEST_FIELDS, "unknown or missing request fields")
    uri = urlsplit(request["table_uri"])
    require(uri.scheme in ("file", "s3") and not (uri.username or uri.password or uri.query or uri.fragment)
            and (uri.scheme != "file" or (uri.netloc in ("", "localhost") and uri.path.startswith("/")))
            and (uri.scheme != "s3" or bool(uri.netloc)), "expected a file/s3 URL without credentials, query or fragment")
    require(type(request["snapshot_version"]) is int and 0 <= request["snapshot_version"] < 2**64,
            "invalid snapshot version")
    require(request["execution_mode"] in ("open", "reuse") and request["purpose"] in ("validation", "timing", "diagnostic")
            and type(request["comparison_revision"]) is int and request["comparison_revision"] == 2
            and request["protocol_sha256"] == digest(HERE / "protocol.md")
            and re.fullmatch("[0-9a-f]{64}", request["fixture_manifest_sha256"])
            and request["resource_budget"] == BUDGET, "request differs from the frozen protocol")
    for field in ("case_id", "run_id", "canonical_sql", "profile"):
        require(isinstance(request[field], str) and request[field].strip(), f"invalid {field}")
    for field in ("campaign_id", "correctness_file"):
        require(request[field] is None or isinstance(request[field], str), f"invalid {field}")
    for field in ("repetition", "order"):
        require(request[field] is None or (type(request[field]) is int and request[field] >= 0), f"invalid {field}")


def storage(connection, uri):
    if urlsplit(uri).scheme != "s3":
        return {"credentials": "none"}
    options = {"REGION": os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))}
    for option, env in (("KEY_ID", "AWS_ACCESS_KEY_ID"), ("SECRET", "AWS_SECRET_ACCESS_KEY"), ("SESSION_TOKEN", "AWS_SESSION_TOKEN")):
        if env in os.environ:
            options[option] = os.environ[env]
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        parsed = urlsplit(endpoint)
        require(parsed.scheme in ("http", "https") and parsed.netloc and not (parsed.username or parsed.password
                or parsed.query or parsed.fragment) and parsed.path in ("", "/"), "invalid AWS_ENDPOINT_URL")
        options.update(ENDPOINT=parsed.netloc, URL_STYLE="path")
    sql = "CREATE SECRET bench_s3 (TYPE s3, " + ", ".join(f"{k} {literal(v)}" for k, v in options.items())
    if endpoint:
        sql += ", USE_SSL " + ("true" if parsed.scheme == "https" else "false")
    try:
        connection.execute(sql + ")")
    except duckdb.Error:
        # A parser error can include the secret-bearing SQL. Never put it in a record.
        raise ValueError("could not configure the S3 secret from AWS environment variables") from None
    return {"credentials": "AWS environment", "region": options["REGION"], "endpoint": endpoint,
            "url_style": options.get("URL_STYLE", "vhost")}


def attach(connection, request):
    reuse = request["execution_mode"] == "reuse"
    connection.execute(f"ATTACH {literal(request['table_uri'])} AS bench (TYPE delta, READ_ONLY, "
                       f"VERSION {request['snapshot_version']}, PIN_SNAPSHOT {'true' if reuse else 'false'}, "
                       "PUSHDOWN_FILTERS 'all', PUSHDOWN_PARTITION_INFO true)")
    if reuse:
        # ATTACH is lazy. Binding the schema loads and pins the selected snapshot.
        connection.sql("DESCRIBE bench").fetchall()


def native_settings(connection):
    settings = dict(connection.sql("SELECT name, value FROM duckdb_settings()").fetchall())
    # httpfs imports legacy S3 settings from the environment even when we use a
    # secret. Scrub their serialized values without changing native execution.
    for name in ("s3_access_key_id", "s3_secret_access_key", "s3_session_token",
                 "http_proxy_username", "http_proxy_password", "password"):
        if settings.get(name):
            settings[name] = "[redacted]"
    proxy = urlsplit(settings["http_proxy"])
    if proxy.username or proxy.password:
        settings["http_proxy"] = proxy._replace(netloc=proxy.netloc.rsplit("@", 1)[-1]).geturl()
    return settings


def correctness(request, identity):
    if request["purpose"] != "timing":
        return {"status": "not_checked", "reason": "untimed invocation"}
    require(request["correctness_file"], "timing requires a correctness certificate")
    path = Path(request["correctness_file"])
    proof = json.loads(path.read_text())
    count = 10 if request["execution_mode"] == "reuse" else 1
    require(proof["status"] == "passed" and len(proof["checks"]) == count, "failed or incomplete correctness certificate")
    rows = []
    for check in proof["checks"]:
        require(check["status"] == "passed" and check["oracle_sha256"] == digest(HERE / "oracle.py"), "failed or stale oracle check")
        for key, value in identity.items():
            require(type(check.get(key)) is type(value) and check.get(key) == value, f"stale correctness field: {key}")
        require(type(check["output_rows"]) is int and check["output_rows"] >= 0, "missing validated row count")
        rows.append(check["output_rows"])
    return {"status": "passed", "path": str(path), "artifact_sha256": digest(path), "expected_output_rows": rows}


def failure_status(error):
    # Kernel protocol rejections arrive through DuckDB's IOException. Do not classify
    # missing objects, corrupt DVs, or arbitrary SQL errors as unsupported features.
    if isinstance(error, duckdb.NotImplementedException) or re.search(
            r"DeltaKernel UnsupportedError \(35\)|Unsupported reader features", str(error)):
        return "unsupported"
    return "operational_failure"


def execute(connection, request, output, record):
    timed = request["purpose"] == "timing"
    reuse = request["execution_mode"] == "reuse"
    record["phase"] = "snapshot_open"
    session_start = clock()
    attach(connection, request)
    initialization = clock() - session_start
    if timed and reuse:
        record["initialization_ns"] = initialization
    for index in range(10 if reuse else 1):
        record["phase"] = "query"
        start = clock() if reuse else session_start
        rows = batches = 0
        first = None
        try:
            relation = connection.sql(request["canonical_sql"])
            with ExitStack() as stack:
                stream = stack.enter_context(relation.to_arrow_reader(8192))
                result = output / f"query-{index}.arrow"
                writer = None
                if request["purpose"] == "validation":
                    sink = stack.enter_context(result.open("xb"))
                    writer = stack.enter_context(pa.ipc.new_stream(sink, stream.schema))
                for batch in stream:
                    if batch.num_rows and first is None:
                        first = clock() - start
                    rows += batch.num_rows
                    batches += 1
                    if writer is not None:
                        writer.write_batch(batch)
                    del batch
                completion = clock() - start
                if index == (9 if reuse else 0):
                    if timed:
                        record["session_elapsed_ns"] = clock() - session_start
                    record["_cleanup_start"] = clock()
            del relation
        except Exception:
            record["partial_query"] = {"query_index": index, "output_rows": rows, "output_batches": batches,
                                       "elapsed_ns": clock() - start if timed else None, "first_batch_ns": first if timed else None}
            raise
        query = {"query_index": index, "output_rows": rows, "output_batches": batches,
                 "completion_ns": completion if timed else None, "first_batch_ns": first if timed else None,
                 "first_batch_unavailable_reason": "untimed invocation" if not timed else "empty result" if not rows else None,
                 "result": None, "identity": None, "physical_plan": None}
        if writer is not None:
            exported = record["identity"] | {"result_sha256": digest(result)}
            identity_name = f"query-{index}.identity.json"
            save(output / identity_name, exported)
            query.update(result=result.name, identity=identity_name)
        if request["purpose"] == "diagnostic":
            name = f"query-{index}.plan.txt"
            plan = connection.sql("EXPLAIN " + request["canonical_sql"]).fetchall()
            (output / name).write_text("\n".join(f"{k}\n{v}" for k, v in plan) + "\n")
            query["physical_plan"] = name
        record["queries"].append(query)
    if timed:
        durations = [q["completion_ns"] for q in record["queries"]]
        if reuse:
            record["initialization_plus_query1_ns"] = initialization + durations[0]
            record["initialization_plus_all_queries_ns"] = initialization + sum(durations)
        else:
            record["open_query_ns"] = durations[0]
    else:
        record["provider_evidence"] = {"schema": connection.sql("DESCRIBE bench").fetchall(),
                                       "attach_options": record["settings"]["provider"]}
    record["phase"] = "complete"
    record["capability"] = {"status": "supported", "scope": "requested query and snapshot", "evidence_run_id": request["run_id"]}


def run(request_path, output):
    request = json.loads(request_path.read_text())
    validate(request)
    output.mkdir()
    connection = None
    record = {"format": "selective-read-observation-v1", "status": "success", "failure_reason": None,
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
    try:
        build_path = HERE / "build.json"
        build = json.loads(build_path.read_text())
        require(build["reader_id"] == "duckdb" and build["executable_sha256"] == digest(Path(__file__))
                and build["lockfile_sha256"] == digest(HERE / "lock.json")
                and all(digest(HERE / name) == value for name, value in build["bundled_sha256"].items()), "stale runner build")
        connection, engine = connect()
        require(runtime_metadata(engine) == build["runtime"], "runtime differs from the prepared build")
        statements = connection.extract_statements(request["canonical_sql"])
        require(len(statements) == 1 and statements[0].type == duckdb.StatementType.SELECT, "expected a single SELECT")
        storage_options = storage(connection, request["table_uri"])
        settings = {"duckdb": native_settings(connection),
                    "provider": {"api": "ATTACH ... (TYPE delta, READ_ONLY, VERSION n)",
                                 "pin_snapshot": request["execution_mode"] == "reuse", "pushdown_filters": "all",
                                 "pushdown_partition_info": True, "initialization": "ATTACH + DESCRIBE bench" if request["execution_mode"] == "reuse" else "ATTACH",
                                 "query_api": "connection.sql(canonical_sql).to_arrow_reader(8192)"},
                    "delta_executor_environment": {"TOKIO_WORKER_THREADS": "8"}, "storage": storage_options,
                    "resource_budget": request["resource_budget"], "table_uri": request["table_uri"],
                    "execution_mode": request["execution_mode"], "output_delivery": "streaming"}
        identity = {"reader_id": "duckdb", "reader_build_sha256": digest(build_path), "reader_config_sha256": json_hash(settings),
                    **{name: request[name] for name in ("comparison_revision", "protocol_sha256", "fixture_manifest_sha256", "case_id", "snapshot_version")},
                    "canonical_sql_sha256": sha(request["canonical_sql"].encode()), "native_expression_sha256": None}
        record.update(identity=identity, settings=settings, build_record=str(build_path), phase="correctness_gate")
        record["correctness"] = correctness(request, identity)
        execute(connection, request, output, record)
        if request["purpose"] == "timing":
            if [q["output_rows"] for q in record["queries"]] != record["correctness"]["expected_output_rows"]:
                record.update(status="validation_failed", failure_reason="timed output row count differs from the validated result")
    except Exception as error:
        record["status"] = "validation_failed" if record["phase"] == "correctness_gate" else failure_status(error)
        record["failure_reason"] = str(error)
        if record["phase"] != "correctness_gate":
            record["capability"]["status"] = "unsupported" if record["status"] == "unsupported" else "probe_failed"
    finally:
        cleanup = record.pop("_cleanup_start", clock())
        if connection is not None:
            try:
                connection.close()
            except Exception as error:
                record.update(status="operational_failure", failure_reason=f"cleanup failed: {error}", phase="cleanup")
        if request["purpose"] == "timing":
            record["cleanup_ns"] = clock() - cleanup
    save(output / "record.json", record)
    print(json.dumps(record))
    return 0 if record["status"] == "success" else 1


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["--describe-build"]:
            connection, engine = connect()
            try:
                print(json.dumps(runtime_metadata(engine)))
            finally:
                connection.close()
        elif len(sys.argv) == 3:
            sys.exit(run(Path(sys.argv[1]), Path(sys.argv[2])))
        else:
            raise ValueError("expected REQUEST.json NEW_OUTPUT_DIRECTORY")
    except Exception as error:
        print(json.dumps({"status": "invalid_input", "failure_reason": str(error)}), file=sys.stderr)
        sys.exit(1)
