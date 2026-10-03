"""Pinned DuckDB Delta adapter for the shared selective-read request/record contract."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import platform
import re
import sys
from time import perf_counter_ns as clock
from urllib.parse import urlsplit

import duckdb
import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from run import comparison_identity, digest, query_count, save
from python_common import checkpoint, correctness, event, json_hash, observation, require, runtime_metadata, sha, validate

CONFIG = {"threads": 8, "memory_limit": "4GiB", "enable_external_file_cache": False,
          "autoload_known_extensions": False, "autoinstall_known_extensions": False,
          "allow_persistent_secrets": False, "python_enable_replacements": False}


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
    checkpoint(record, "initialization" if reuse else "open")
    session_start = clock()
    event(record, "snapshot_open")
    attach(connection, request)
    initialization = clock() - session_start
    if timed and reuse:
        record["initialization_ns"] = initialization
    count = query_count(request)
    for index in range(count):
        record["phase"] = "query"
        if reuse:
            checkpoint(record, "query", index)
        start = clock() if reuse else session_start
        event(record, "query_start", index)
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
                event(record, "stream_complete", index)
                if index + 1 == count:
                    if timed:
                        record["session_elapsed_ns"] = clock() - session_start
                    elif request["purpose"] in ("diagnostic", "io"):
                        record["diagnostic_session_ns"] = clock() - session_start
                    record["_cleanup_start"] = clock()
                checkpoint(record, "query_end", index, {"query_index": index, "output_rows": rows, "output_batches": batches,
                    "completion_ns": completion if timed else None, "first_batch_ns": first if timed else None})
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
    elif request["purpose"] != "io":
        record["provider_evidence"] = {"schema": connection.sql("DESCRIBE bench").fetchall(),
                                       "attach_options": record["settings"]["provider"]}
    record["phase"] = "complete"
    record["capability"] = {"status": "supported", "scope": "requested query and snapshot", "evidence_run_id": request["run_id"]}


def run(request_path, output):
    request = json.loads(request_path.read_text())
    validate(request)
    output.mkdir()
    connection = None
    record = observation(request)
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
                    **comparison_identity(request),
                    **{name: request[name] for name in ("fixture_manifest_sha256", "case_id", "snapshot_version")},
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
        checkpoint(record, "cleanup")
        if connection is not None:
            try:
                connection.close()
            except Exception as error:
                record.update(status="operational_failure", failure_reason=f"cleanup failed: {error}", phase="cleanup")
        if request["purpose"] == "timing":
            record["cleanup_ns"] = clock() - cleanup
        event(record, "cleanup_complete")
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
