"""Spark Delta reader for revision 6 and historical untimed pilots."""

import json
import os
from pathlib import Path
import re
import sys
from time import perf_counter_ns as clock

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE / "spark")]
import pyarrow as pa
import pyspark
from pyspark.sql import SparkSession
from run import query_count
from python_common import (checked_build, checkpoint, correctness, event, export_result, observation, reader_identity,
                           redact_credentials, require, runner_cli, runtime_metadata, scan_sql,
                           storage_options, validate, write_observation)

CONFIG = {"spark.master": "local[8]", "spark.driver.memory": "4g", "spark.default.parallelism": "8",
          "spark.sql.shuffle.partitions": "8", "spark.databricks.delta.snapshotPartitions": "8",
          "spark.sql.session.timeZone": "UTC", "spark.sql.ansi.enabled": "true",
          "spark.sql.execution.arrow.maxRecordsPerBatch": "8192",
          "spark.sql.parquet.filterPushdown": "true", "spark.sql.parquet.enableVectorizedReader": "true",
          "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
          "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
          "spark.driver.extraJavaOptions": "-XX:ActiveProcessorCount=8",
          "spark.driver.host": "127.0.0.1", "spark.driver.bindAddress": "127.0.0.1",
          "spark.ui.enabled": "false", "spark.ui.showConsoleProgress": "false"}


def runtime():
    lock = json.loads((HERE / "lock.json").read_text())
    require(pyspark.__version__ == lock["spark_version"], "wrong Spark version")
    return runtime_metadata({"spark_version": pyspark.__version__, "delta_version": lock["delta_version"]})


def connect():
    os.environ.update(JAVA_HOME=str(HERE / "java"), SPARK_HOME=str(HERE / "spark/deps"),
                      SPARK_CONF_DIR=str(HERE / "conf"), SPARK_LOCAL_DIRS=str(HERE / "spill"),
                      SPARK_LOCAL_HOSTNAME="127.0.0.1", PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell")
    require(not list((HERE / "conf").iterdir()), "external Spark configuration is not allowed")
    builder = SparkSession.builder.appName("selective-read-spark")
    for key, value in CONFIG.items():
        builder = builder.config(key, value)
    # local[8] shares the driver JVM. Classpaths avoid copying the large AWS JAR
    # into an executor scratch file covered by the validation-export size limit.
    classpath = os.pathsep.join(str(HERE / "jars" / j["filename"])
                               for j in json.loads((HERE / "lock.json").read_text())["jars"])
    builder = builder.config("spark.driver.extraClassPath", classpath).config("spark.executor.extraClassPath", classpath)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    require(session.version == pyspark.__version__, "wrong JVM Spark version")
    lock = json.loads((HERE / "lock.json").read_text())
    require(session._jvm.java.lang.System.getProperty("java.runtime.version") == lock["java_runtime_version"], "wrong JVM")
    return session


def storage(session, uri):
    options = storage_options("spark", uri)
    config = session.sparkContext._jsc.hadoopConfiguration()
    for key, value in options["options"].items():
        config.set(key, value)
    return options["settings"], options["uri"]


def failure_status(error):
    return "unsupported" if re.search(r"DELTA_UNSUPPORTED_FEATURES_FOR_READ|Unsupported Delta read feature", str(error)) else "operational_failure"


def run(request_path, output):
    request = json.loads(request_path.read_text())
    validate(request)
    scan_sql(request["canonical_sql"])
    require(request["comparison_revision"] == 6 or request["purpose"] != "timing" and request["campaign_id"] is None,
            "Spark is a pilot only; formal timing needs the new reader-roster contract")
    output.mkdir()
    record = observation(request)
    timed = request["purpose"] == "timing"
    record.update(pilot_only=request.get("sampling_stage") != "formal" or request["comparison_revision"] < 6,
                  publication_ready=False, startup_ns=None, pilot_initialization_ns=None)
    session = None
    try:
        build_path, build = checked_build(Path(__file__), "spark")
        require(not build["pilot_only"] and runtime() == build["runtime"], "stale Spark runtime/build")
        start = clock()
        session = connect()
        record["startup_ns"] = clock() - start
        options, native_uri = storage(session, request["table_uri"])
        settings = {"spark": CONFIG, "storage": options, "resource_budget": request["resource_budget"],
                    "table_uri": request["table_uri"], "execution_mode": request["execution_mode"],
                    "provider": {"api": "spark.read.format(delta).option(versionAsOf, n).load(uri)",
                                 "query_api": "spark.sql(canonical_sql).toArrow()", "cache_or_persist": False},
                    "output_delivery": "collected", "runtime_scope": "revision 6; historical untimed pilot only"}
        identity = reader_identity(request, "spark", build_path, settings)
        record.update(identity=identity, settings=settings, build_record=str(build_path),
                      native_session_configuration=dict(session.sparkContext.getConf().getAll()))
        record["phase"] = "correctness_gate"
        record["correctness"] = correctness(request, identity)
        record["phase"] = "snapshot_open"
        reuse = request["execution_mode"] == "reuse"
        checkpoint(record, "initialization" if reuse else "open")
        event(record, "snapshot_open")
        session_start = clock()
        source = session.read.format("delta").option("versionAsOf", request["snapshot_version"]).load(native_uri)
        source.schema
        source.createOrReplaceTempView("bench")
        record["pilot_initialization_ns"] = clock() - session_start
        if timed and reuse:
            record["initialization_ns"] = record["pilot_initialization_ns"]
        for index in range(query_count(request)):
            record["phase"] = "query"
            if reuse:
                checkpoint(record, "query", index)
            start = clock() if reuse else session_start
            event(record, "query_start", index)
            relation = session.sql(request["canonical_sql"])
            table = relation.toArrow()
            completion = clock() - start
            event(record, "stream_complete", index)
            query = {"query_index": index, "output_rows": table.num_rows,
                     "output_batches": len(table.to_batches(8192)), "completion_ns": completion if timed else None, "first_batch_ns": None,
                     "first_batch_unavailable_reason": "collected API; no streaming first-batch clock",
                     "pilot_completion_ns": completion, "result": None, "identity": None, "physical_plan": None}
            if index + 1 == query_count(request):
                if timed:
                    record["session_elapsed_ns"] = clock() - session_start
                elif request["purpose"] in ("diagnostic", "io") or request.get("validation_diagnostics"):
                    record["diagnostic_session_ns"] = clock() - session_start
                record["_cleanup_start"] = clock()
            checkpoint(record, "query_end", index, {k: query[k] for k in
                       ("query_index", "output_rows", "output_batches", "completion_ns", "first_batch_ns")})
            if request["purpose"] == "validation":
                result = output / f"query-{index}.arrow"
                with result.open("xb") as sink, pa.ipc.new_stream(sink, table.schema) as writer:
                    for batch in table.to_batches(8192):
                        writer.write_batch(batch)
                query.update(export_result(output, index, identity))
            if request["purpose"] == "diagnostic" or request.get("validation_diagnostics"):
                name = f"query-{index}.plan.txt"
                (output / name).write_text(relation._jdf.queryExecution().toString() + "\n")
                query["physical_plan"] = name
            record["queries"].append(query)
            del relation, table
        record.update(phase="complete", capability={"status": "supported", "scope": "requested query and snapshot"})
    except Exception as error:
        reason = redact_credentials(str(error))
        record.update(status="validation_failed" if record["phase"] == "correctness_gate" else failure_status(error),
                      failure_reason=reason)
        if record["phase"] == "query":
            record["partial_query"] = {"query_index": len(record["queries"]), "output_rows": 0, "output_batches": 0,
                                       "elapsed_ns": clock() - start if timed else None, "first_batch_ns": None}
    finally:
        cleanup_start = record.pop("_cleanup_start", clock())
        checkpoint(record, "cleanup")
        if session is not None:
            try:
                session.stop()
            except Exception as error:
                record.update(status="operational_failure", phase="cleanup", failure_reason="Spark cleanup failed: " + str(error))
        if timed:
            record["cleanup_ns"] = clock() - cleanup_start
        event(record, "cleanup_complete")
    return write_observation(output, record)


def describe_build():
    print(json.dumps(runtime()))


if __name__ == "__main__":
    runner_cli(run, describe_build)
