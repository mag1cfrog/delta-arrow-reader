"""Pinned Daft read_deltalake adapter for the selective-read observation contract."""

from contextlib import ExitStack, closing
import json
import os
from pathlib import Path
import pickle
import platform
import sys
from time import perf_counter_ns as clock
from urllib.parse import urlsplit

HERE = Path(__file__).resolve().parent
for name in list(os.environ):
    if name.startswith("DAFT_"):
        del os.environ[name]
ENVIRONMENT = {"DAFT_ANALYTICS_ENABLED": "0", "DAFT_MEMORY_LIMIT": str(4 * 1024**3),
               "DAFT_RUNNER": "native", "TOKIO_WORKER_THREADS": "8"}
os.environ.update(ENVIRONMENT)

import daft
import deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError
import pyarrow as pa

sys.path.insert(0, str(HERE))
from run import comparison_identity, digest, query_count, save
from python_common import checkpoint, correctness, event, json_hash, observation, require, runtime_metadata, scan_sql, sha, validate

EXECUTION = {"default_morsel_size": 8192, "scantask_max_parallel": 8, "maintain_order": False}


def engine():
    lock = json.loads((HERE / "lock.json").read_text())
    require(platform.python_implementation() == "CPython" and platform.python_version() == lock["python"],
            "wrong Python interpreter")
    runner = daft.set_runner_native(num_threads=8)
    daft.set_execution_config(**EXECUTION)
    daft.set_event_log_config(enabled=False)
    require(runner.name == "native" and daft.get_build_type() == "release", "wrong Daft runner or build type")
    return {"daft": daft.get_version(), "daft_build_type": daft.get_build_type(), "deltalake": deltalake.__version__}


def expressions(sql):
    columns, predicate, limit = scan_sql(sql)
    return [daft.col(c) for c in columns], daft.sql_expr(predicate) if predicate else None, limit


def expression_identity(sql):
    projection, predicate, limit = expressions(sql)
    # Daft's Expression.serialize transforms column values. Python's pickle
    # protocol serializes the expression itself, including its native typed AST.
    def frozen(expr):
        return {"text": str(expr), "pickle_sha256": sha(pickle.dumps(expr, protocol=5))}
    return {"projection": [frozen(e) for e in projection], "predicate": frozen(predicate) if predicate is not None else None,
            "limit": limit, "order": "where, select, limit", "pickle_protocol": 5}


def query(source, sql):
    projection, predicate, limit = expressions(sql)
    plan = source.where(predicate) if predicate is not None else source
    plan = plan.select(*projection)
    return plan.limit(limit) if limit is not None else plan


def storage(uri):
    if urlsplit(uri).scheme != "s3":
        return daft.io.IOConfig(), {"credentials": "none"}
    options = {"region_name": os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))}
    for name, env in (("key_id", "AWS_ACCESS_KEY_ID"), ("access_key", "AWS_SECRET_ACCESS_KEY"), ("session_token", "AWS_SESSION_TOKEN")):
        if env in os.environ:
            options[name] = os.environ[env]
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        parsed = urlsplit(endpoint)
        require(parsed.scheme in ("http", "https") and parsed.netloc and not (parsed.username or parsed.password
                or parsed.query or parsed.fragment) and parsed.path in ("", "/"), "invalid AWS_ENDPOINT_URL")
        options.update(endpoint_url=endpoint, use_ssl=parsed.scheme == "https", force_virtual_addressing=False)
    config = daft.io.IOConfig(s3=daft.io.S3Config(**options))
    return config, {"credentials": "AWS environment with native fallback", "region": options["region_name"], "endpoint": endpoint,
                    "url_style": "path" if endpoint else "native default"}


def scan(request, config):
    # Daft opens the latest snapshot then loads the explicit version internally.
    # Keep that native work inside initialization/open-and-query, including errors.
    return daft.read_deltalake(request["table_uri"], version=request["snapshot_version"],
                               io_config=config, ignore_deletion_vectors=False)


def native_settings():
    ctx = daft.context.get_context()
    config = ctx.daft_execution_config
    fields = {name: getattr(config, name) for name in dir(config)
              if not name.startswith("_") and not callable(getattr(config, name))}
    fields["broadcast_join_size_bytes_threshold"] = config.get_broadcast_join_size_bytes_threshold()
    return {"execution": fields, "strict_filter_pushdown": ctx.daft_planning_config.enable_strict_filter_pushdown,
            "event_log_enabled": ctx.daft_event_log_config.enabled, "compute_workers": 8,
            "io_workers": "min(8, Rust available_parallelism)", "compute_max_blocking_threads": 1,
            "io_max_blocking_threads": "pinned Tokio default", "dashboard": "disabled", "environment": ENVIRONMENT}


def execute(request, config, output, record):
    timed = request["purpose"] == "timing"
    reuse = request["execution_mode"] == "reuse"
    record["phase"] = "snapshot_open"
    checkpoint(record, "initialization" if reuse else "open")
    session_start = clock()
    event(record, "snapshot_open")
    source = scan(request, config)
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
            plan = query(source, request["canonical_sql"])
            with ExitStack() as stack:
                result = output / f"query-{index}.arrow"
                writer = None
                if request["purpose"] == "validation":
                    sink = stack.enter_context(result.open("xb"))
                    writer = stack.enter_context(pa.ipc.new_stream(sink, plan.schema().to_pyarrow_schema()))
                stream = stack.enter_context(closing(plan.to_arrow_iter(results_buffer_size=8)))
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
        except Exception:
            record["partial_query"] = {"query_index": index, "output_rows": rows, "output_batches": batches,
                                       "elapsed_ns": clock() - start if timed else None, "first_batch_ns": first if timed else None}
            raise
        result_record = {"query_index": index, "output_rows": rows, "output_batches": batches,
                         "completion_ns": completion if timed else None, "first_batch_ns": first if timed else None,
                         "first_batch_unavailable_reason": "untimed invocation" if not timed else "empty result" if not rows else None,
                         "result": None, "identity": None, "physical_plan": None}
        if writer is not None:
            name = f"query-{index}.identity.json"
            save(output / name, record["identity"] | {"result_sha256": digest(result)})
            result_record.update(result=result.name, identity=name)
        if request["purpose"] == "diagnostic":
            name = f"query-{index}.plan.txt"
            with (output / name).open("x") as target:
                plan.explain(show_all=True, file=target)
            result_record["physical_plan"] = name
        record["queries"].append(result_record)
        del stream, plan
    if timed:
        durations = [q["completion_ns"] for q in record["queries"]]
        if reuse:
            record["initialization_plus_query1_ns"] = initialization + durations[0]
            record["initialization_plus_all_queries_ns"] = initialization + sum(durations)
        else:
            record["open_query_ns"] = durations[0]
    elif request["purpose"] != "io":
        record["provider_evidence"] = {"schema": str(source.schema().to_pyarrow_schema()),
                                       "native_expression": expression_identity(request["canonical_sql"])}
    record["phase"] = "complete"
    record["capability"] = {"status": "supported", "scope": "requested query and snapshot", "evidence_run_id": request["run_id"]}


def run(request_path, output):
    request = json.loads(request_path.read_text())
    validate(request)
    output.mkdir()
    record = observation(request)
    try:
        build_path = HERE / "build.json"
        build = json.loads(build_path.read_text())
        require(build["reader_id"] == "daft" and build["executable_sha256"] == digest(Path(__file__))
                and build["lockfile_sha256"] == digest(HERE / "lock.json")
                and all(digest(HERE / name) == value for name, value in build["bundled_sha256"].items()), "stale runner build")
        require(runtime_metadata(engine()) == build["runtime"], "runtime differs from the prepared build")
        config, storage_record = storage(request["table_uri"])
        settings = {"daft": native_settings(), "provider": {"api": "daft.read_deltalake", "ignore_deletion_vectors": False,
                    "initialization": "read_deltalake (eager snapshot and schema)", "multithreaded_io": "native runner default: true"},
                    "query_api": "to_arrow_iter(results_buffer_size=8)",
                    "results_buffer_note": "the pinned native runner ignores this argument; its own channels control buffering",
                    "storage": storage_record, "resource_budget": request["resource_budget"], "table_uri": request["table_uri"],
                    "execution_mode": request["execution_mode"], "output_delivery": "streaming",
                    "native_memory_budget_bytes": 4 * 1024**3, "memory_limit_reason": "native memory manager, not a process cap; launcher must enforce 8 GiB",
                    "cache": "no collect/result cache; reuse retains the native Delta data source and runner"}
        if request["purpose"] == "timing":
            record["phase"] = "correctness_gate"
        expression = expression_identity(request["canonical_sql"])
        identity = {"reader_id": "daft", "reader_build_sha256": digest(build_path), "reader_config_sha256": json_hash(settings),
                    **comparison_identity(request),
                    **{name: request[name] for name in ("fixture_manifest_sha256", "case_id", "snapshot_version")},
                    "canonical_sql_sha256": sha(request["canonical_sql"].encode()), "native_expression_sha256": json_hash(expression)}
        record.update(identity=identity, settings=settings, build_record=str(build_path), phase="correctness_gate")
        record["correctness"] = correctness(request, identity)
        execute(request, config, output, record)
        if request["purpose"] == "timing" and [q["output_rows"] for q in record["queries"]] != record["correctness"]["expected_output_rows"]:
            record.update(status="validation_failed", failure_reason="timed output row count differs from the validated result")
    except Exception as error:
        unsupported = isinstance(error, (NotImplementedError, DeltaProtocolError)) or (
            isinstance(error, DeltaError) and str(error).startswith("Kernel error: Unsupported:"))
        record["status"] = "validation_failed" if record["phase"] == "correctness_gate" else "unsupported" if unsupported else "operational_failure"
        message = str(error)
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
            if os.environ.get(name):
                message = message.replace(os.environ[name], "[redacted]")
        record["failure_reason"] = message
        if record["phase"] != "correctness_gate":
            record["capability"]["status"] = "unsupported" if unsupported else "probe_failed"
    finally:
        cleanup = record.pop("_cleanup_start", clock())
        checkpoint(record, "cleanup")
        if request["purpose"] == "timing":
            record["cleanup_ns"] = clock() - cleanup
        event(record, "cleanup_complete")
    save(output / "record.json", record)
    print(json.dumps(record))
    return 0 if record["status"] == "success" else 1


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["--describe-build"]:
            print(json.dumps(runtime_metadata(engine())))
        elif len(sys.argv) == 3:
            sys.exit(run(Path(sys.argv[1]), Path(sys.argv[2])))
        else:
            raise ValueError("expected REQUEST.json NEW_OUTPUT_DIRECTORY")
    except Exception as error:
        print(json.dumps({"status": "invalid_input", "failure_reason": str(error)}), file=sys.stderr)
        sys.exit(1)
