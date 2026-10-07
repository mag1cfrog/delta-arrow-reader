"""Pinned Polars scan_delta adapter for the selective-read observation contract."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import platform
import sys
from time import perf_counter_ns as clock
from urllib.parse import urlsplit

HERE = Path(__file__).resolve().parent
# Set these before importing native runtimes. Do not inherit experimental Polars
# flags, credentials/providers, cache overrides, or debugging from the shell.
for name in list(os.environ):
    if name.startswith("POLARS_"):
        del os.environ[name]
ENVIRONMENT = {"POLARS_MAX_THREADS": "8", "POLARS_ASYNC_THREAD_COUNT": "8",
               "POLARS_MAX_BLOCKING_THREAD_COUNT": "64", "POLARS_FILE_CACHE_TTL": "0",
               "POLARS_OOC_MEMORY_BUDGET_MB": "4000", "POLARS_OOC_SPILL_DIR": str(HERE / "spill"),
               "TOKIO_WORKER_THREADS": "8"}
os.environ.update(ENVIRONMENT)

import deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError
import polars as pl
import pyarrow as pa

sys.path.insert(0, str(HERE))
from run import comparison_identity, digest, query_count, save
from python_common import checkpoint, correctness, event, json_hash, observation, require, runtime_metadata, scan_sql, sha, validate

DELTA_OPTIONS = {"without_files": False, "log_buffer_size": 8, "skip_stats": False}
COLLECT_OPTIONS = {"chunk_size": 8192, "maintain_order": False, "lazy": False, "engine": "streaming"}


def engine():
    lock = json.loads((HERE / "lock.json").read_text())
    require(platform.python_implementation() == "CPython" and platform.python_version() == lock["python"],
            "wrong Python interpreter")
    require(pl.thread_pool_size() == 8, "wrong Polars thread pool size")
    return {"polars": pl.build_info(), "deltalake": deltalake.__version__, "polars_index_type": str(pl.get_index_type())}


def expressions(sql):
    columns, predicate, limit = scan_sql(sql)
    projection = [pl.col(c) for c in columns]
    predicate = pl.sql_expr(predicate) if predicate else None
    return projection, predicate, limit


def expression_identity(sql):
    projection, predicate, limit = expressions(sql)
    return {"projection": [e.meta.serialize(format="json") for e in projection],
            "predicate": predicate.meta.serialize(format="json") if predicate is not None else None,
            "limit": limit, "order": "filter, select, limit"}


def query(source, sql):
    projection, predicate, limit = expressions(sql)
    plan = source.clone()
    if predicate is not None:
        plan = plan.filter(predicate)
    plan = plan.select(projection)
    return plan.limit(limit) if limit is not None else plan


def storage(uri):
    if urlsplit(uri).scheme != "s3":
        return {}, {"credentials": "none"}
    options = {"AWS_REGION": os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))}
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        if name in os.environ:
            options[name] = os.environ[name]
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        parsed = urlsplit(endpoint)
        require(parsed.scheme in ("http", "https") and parsed.netloc and not (parsed.username or parsed.password
                or parsed.query or parsed.fragment) and parsed.path in ("", "/"), "invalid AWS_ENDPOINT_URL")
        options.update(AWS_ENDPOINT_URL=endpoint, AWS_ALLOW_HTTP=str(parsed.scheme == "http").lower(),
                       AWS_VIRTUAL_HOSTED_STYLE_REQUEST="false")
    return options, {"credentials": "AWS environment", "region": options["AWS_REGION"], "endpoint": endpoint,
                     "url_style": "path" if endpoint else "native default"}


def scan(request, options):
    return pl.scan_delta(request["table_uri"], version=request["snapshot_version"],
                         storage_options=options, credential_provider=None,
                         delta_table_options=DELTA_OPTIONS, use_pyarrow=False)


def execute(request, options, output, record):
    timed = request["purpose"] == "timing"
    reuse = request["execution_mode"] == "reuse"
    record["phase"] = "snapshot_open"
    checkpoint(record, "initialization" if reuse else "open")
    session_start = clock()
    event(record, "snapshot_open")
    source = scan(request, options)
    if reuse:
        # Resolve the native dataset's snapshot/schema, retaining only the source
        # logical scan. Each query below constructs and plans fresh expressions.
        source.collect_schema()
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
                    schema = pl.DataFrame(schema=plan.collect_schema()).to_arrow().schema
                    sink = stack.enter_context(result.open("xb"))
                    writer = stack.enter_context(pa.ipc.new_stream(sink, schema))
                stream = plan.collect_batches(**COLLECT_OPTIONS)
                for frame in stream:
                    if frame.height and first is None:
                        first = clock() - start
                    rows += frame.height
                    batches += 1
                    if writer is not None:
                        writer.write_table(frame.to_arrow())
                    del frame
                completion = clock() - start
                event(record, "stream_complete", index)
                if index + 1 == count:
                    if timed:
                        record["session_elapsed_ns"] = clock() - session_start
                    elif request["purpose"] in ("diagnostic", "io") or request.get("validation_diagnostics"):
                        record["diagnostic_session_ns"] = clock() - session_start
                    record["_cleanup_start"] = clock()
                checkpoint(record, "query_end", index, {"query_index": index, "output_rows": rows, "output_batches": batches,
                    "completion_ns": completion if timed else None, "first_batch_ns": first if timed else None})
                del stream
        except Exception:
            record["partial_query"] = {"query_index": index, "output_rows": rows, "output_batches": batches,
                                       "elapsed_ns": clock() - start if timed else None, "first_batch_ns": first if timed else None}
            raise
        observation = {"query_index": index, "output_rows": rows, "output_batches": batches,
                       "completion_ns": completion if timed else None, "first_batch_ns": first if timed else None,
                       "first_batch_unavailable_reason": "untimed invocation" if not timed else "empty result" if not rows else None,
                       "result": None, "identity": None, "physical_plan": None}
        if writer is not None:
            name = f"query-{index}.identity.json"
            save(output / name, record["identity"] | {"result_sha256": digest(result)})
            observation.update(result=result.name, identity=name)
        if request["purpose"] == "diagnostic" or request.get("validation_diagnostics"):
            name = f"query-{index}.plan.dot"
            (output / name).write_text(plan.show_graph(raw_output=True, show=False, engine="streaming", plan_stage="physical"))
            observation["physical_plan"] = name
        record["queries"].append(observation)
        del plan
    if timed:
        durations = [q["completion_ns"] for q in record["queries"]]
        if reuse:
            record["initialization_plus_query1_ns"] = initialization + durations[0]
            record["initialization_plus_all_queries_ns"] = initialization + sum(durations)
        else:
            record["open_query_ns"] = durations[0]
    elif request["purpose"] != "io":
        record["provider_evidence"] = {"schema": {k: str(v) for k, v in source.collect_schema().items()},
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
        require(build["reader_id"] == "polars" and build["executable_sha256"] == digest(Path(__file__))
                and build["lockfile_sha256"] == digest(HERE / "lock.json")
                and all(digest(HERE / name) == value for name, value in build["bundled_sha256"].items()), "stale runner build")
        require(runtime_metadata(engine()) == build["runtime"], "runtime differs from the prepared build")
        options, storage_record = storage(request["table_uri"])
        settings = {"polars_environment": ENVIRONMENT, "polars_config": pl.Config.state(), "thread_pool_size": pl.thread_pool_size(),
                    "optimizations": str(pl.QueryOptFlags()), "collect_batches": COLLECT_OPTIONS,
                    "provider": {"api": "polars.scan_delta", "use_pyarrow": False, "credential_provider": None,
                                 "delta_table_options": DELTA_OPTIONS,
                                 "initialization": "scan_delta + collect_schema" if request["execution_mode"] == "reuse" else "scan_delta"},
                    "storage": storage_record, "resource_budget": request["resource_budget"], "table_uri": request["table_uri"],
                    "execution_mode": request["execution_mode"], "output_delivery": "streaming",
                    "native_memory_budget_bytes": 4_000_000_000,
                    "memory_limit_reason": "native spill threshold, not a process cap; launcher must enforce 8 GiB",
                    "cache": "no result cache; native dataset snapshot retained within a session; file cache TTL 0"}
        # Invalid/stale SQL in a timing request must fail the certificate gate too.
        if request["purpose"] == "timing":
            record["phase"] = "correctness_gate"
        expression = expression_identity(request["canonical_sql"])
        identity = {"reader_id": "polars", "reader_build_sha256": digest(build_path), "reader_config_sha256": json_hash(settings),
                    **comparison_identity(request),
                    **{name: request[name] for name in ("fixture_manifest_sha256", "case_id", "snapshot_version")},
                    "canonical_sql_sha256": sha(request["canonical_sql"].encode()), "native_expression_sha256": json_hash(expression)}
        record.update(identity=identity, settings=settings, build_record=str(build_path), phase="correctness_gate")
        record["correctness"] = correctness(request, identity)
        execute(request, options, output, record)
        if request["purpose"] == "timing" and [q["output_rows"] for q in record["queries"]] != record["correctness"]["expected_output_rows"]:
            record.update(status="validation_failed", failure_reason="timed output row count differs from the validated result")
    except Exception as error:
        unsupported = isinstance(error, DeltaProtocolError) or (
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
