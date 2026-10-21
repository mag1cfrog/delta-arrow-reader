"""Pinned Polars scan_delta adapter for the selective-read observation contract."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import platform
import sys
from time import perf_counter_ns as clock

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
from python_common import (checked_build, checkpoint, correctness, event, export_result, observation,
                           reader_identity, redact_credentials, require, runner_cli, runtime_metadata,
                           scan_sql, storage_options, write_observation)

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
    count = record.pop("_query_count")
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
                observation = {"query_index": index, "output_rows": rows, "output_batches": batches,
                               "completion_ns": completion if timed else None, "first_batch_ns": first if timed else None}
                checkpoint(record, "query_end", index, observation)
                del stream
        except Exception:
            record["partial_query"] = {"query_index": index, "output_rows": rows, "output_batches": batches,
                                       "elapsed_ns": clock() - start if timed else None, "first_batch_ns": first if timed else None}
            raise
        if writer is not None:
            observation.update(export_result(output, index, record["identity"]))
        if request["purpose"] == "diagnostic" or request.get("validation_diagnostics"):
            name = f"query-{index}.plan.dot"
            (output / name).write_text(plan.show_graph(raw_output=True, show=False, engine="streaming", plan_stage="physical"))
            observation["physical_plan"] = name
        record["queries"].append(observation)
        del plan
    if not timed and request["purpose"] != "io":
        record["provider_evidence"] = {"schema": {k: str(v) for k, v in source.collect_schema().items()},
                                       "native_expression": expression_identity(request["canonical_sql"])}
    record["phase"] = "complete"
    record["capability"] = {"status": "supported", "scope": "requested query and snapshot", "evidence_run_id": request["run_id"]}


def run(request_path, output):
    request = json.loads(request_path.read_text())
    record = observation(request)
    output.mkdir()
    try:
        build_path, build = checked_build(Path(__file__), "polars")
        require(runtime_metadata(engine()) == build["runtime"], "runtime differs from the prepared build")
        storage = storage_options("polars", request["table_uri"])
        settings = {"polars_environment": ENVIRONMENT, "polars_config": pl.Config.state(), "thread_pool_size": pl.thread_pool_size(),
                    "optimizations": str(pl.QueryOptFlags()), "collect_batches": COLLECT_OPTIONS,
                    "provider": {"api": "polars.scan_delta", "use_pyarrow": False, "credential_provider": None,
                                 "delta_table_options": DELTA_OPTIONS,
                                 "initialization": "scan_delta + collect_schema" if request["execution_mode"] == "reuse" else "scan_delta"},
                    "storage": storage["settings"], "resource_budget": request["resource_budget"], "table_uri": request["table_uri"],
                    "execution_mode": request["execution_mode"], "output_delivery": "streaming",
                    "native_memory_budget_bytes": 4_000_000_000,
                    "memory_limit_reason": "native spill threshold, not a process cap; launcher must enforce 8 GiB",
                    "cache": "no result cache; native dataset snapshot retained within a session; file cache TTL 0"}
        # Invalid/stale SQL in a timing request must fail the certificate gate too.
        if request["purpose"] == "timing":
            record["phase"] = "correctness_gate"
        expression = expression_identity(request["canonical_sql"])
        identity = reader_identity(request, "polars", build_path, settings, expression)
        record.update(identity=identity, settings=settings, build_record=str(build_path), phase="correctness_gate")
        record["correctness"] = correctness(request, identity)
        execute(request, storage["options"], output, record)
    except Exception as error:
        unsupported = isinstance(error, DeltaProtocolError) or (
            isinstance(error, DeltaError) and str(error).startswith("Kernel error: Unsupported:"))
        record["status"] = "validation_failed" if record["phase"] == "correctness_gate" else "unsupported" if unsupported else "operational_failure"
        record["failure_reason"] = redact_credentials(str(error))
        if record["phase"] != "correctness_gate":
            record["capability"]["status"] = "unsupported" if unsupported else "probe_failed"
    finally:
        cleanup = record.pop("_cleanup_start", clock())
        checkpoint(record, "cleanup")
        if request["purpose"] == "timing":
            record["cleanup_ns"] = clock() - cleanup
        event(record, "cleanup_complete")
    return write_observation(output, record)


def describe_build():
    print(json.dumps(runtime_metadata(engine())))


if __name__ == "__main__":
    runner_cli(run, describe_build)
