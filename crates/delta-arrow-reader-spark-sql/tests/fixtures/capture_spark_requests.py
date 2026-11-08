"""Capture validated source-only requests with the maintained Spark fixture protocol."""

import json
import os
from pathlib import Path
import sys
import traceback

sys.dont_write_bytecode = True

from capture_date_range_747_spark import (
    OBSERVED_SETTINGS, REVISION, SETTINGS, STAGES, VERSION, capture, cleanup,
)


def capture_requested_queries(request, output_path, source_paths, request_paths, *,
                              capture_query=capture, spark_settings=None):
    assert sys.version_info[:2] == (3, 12), "use Python 3.12"
    assert callable(capture_query), "provide a query capture callback"
    run_settings = dict(SETTINGS)
    if spark_settings is not None:
        assert isinstance(spark_settings, dict), "Spark settings overrides must be a dict"
        assert all(isinstance(key, str) and key.startswith("spark.") and len(key) > 6
                   and isinstance(value, str) for key, value in spark_settings.items()), (
            "Spark settings overrides must map spark.* keys to strings")
        run_settings.update(spark_settings)
    observed_settings = list(dict.fromkeys([*OBSERVED_SETTINGS, *run_settings]))
    assert output_path is not None and output_path.parent.is_dir(), "provide a fresh output path"
    lifecycle_path = output_path.with_suffix(".lifecycle.json")
    for path in [output_path, lifecycle_path]:
        assert not path.exists() and not path.is_symlink(), path
    assert os.environ.get("TZ") == "UTC", "run with TZ=UTC"
    assert not any(key in os.environ for key in [
        "PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET", "SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED",
    ])
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from capture_floating_group_keys_spark import encode
    from capture_window_name_stages_spark import decode
    from recapture_string_binary_spark import file_sha256, runtime_identity
    from pyspark import SparkContext
    from pyspark.sql import SparkSession, types

    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert SparkSession._instantiatedSession is None and SparkSession._activeSession is None
    sources = [Path(path).resolve() for path in source_paths]
    sources.extend(Path(sys.modules[function.__module__].__file__).resolve()
                   for function in [capture_query, capture, encode, decode, file_sha256])
    sources.append(Path(__file__).resolve())
    sources = list(dict.fromkeys(sources))
    source_receipts = [{"path": str(path), "sha256": file_sha256(path)} for path in sources]
    receipt_by_path = {receipt["path"]: receipt for receipt in source_receipts}
    request_receipts = [receipt_by_path[str(Path(path).resolve())] for path in request_paths]
    assert request_receipts, "at least one source request required"
    spark = gateway = None
    lifecycle = {"spark_only": True, "reads_expected_outputs": False, "capture_pid": os.getpid(),
                 "foreign_gateway_reuse_rejected": True}
    with output_path.open("x") as output, lifecycle_path.open("x") as lifecycle_file:
        try:
            builder = SparkSession.builder.master("local[1]").appName(f"dar-{request['issue']}-reference-recapture")
            builder = builder.config("spark.ui.enabled", "false")
            for key, value in run_settings.items():
                builder = builder.config(key, value)
            spark = builder.getOrCreate()
            gateway = SparkContext._gateway
            assert gateway is not None and gateway.proc is not None, "owned JVM required"
            spark.sparkContext.setLogLevel("ERROR")
            identity = runtime_identity(spark)
            assert identity["spark_version"] == VERSION
            assert identity["spark_build"]["SPARK_REVISION"]["value"] == REVISION
            assert identity["java_properties"]["java.specification.version"] == "21"
            assert spark.sparkContext.master == "local[1]"
            assert spark.sparkContext.getConf().get("spark.ui.enabled") == "false"
            sessions, registered = [], []
            for corpus in request["corpora"]:
                session = spark.newSession()
                for key, value in run_settings.items():
                    if key.startswith("spark.sql."):
                        session.conf.set(key, value)
                providers = []
                for item in corpus["metadata"]["inputs"]:
                    schema = types.StructType.fromJson(item["schema"])
                    rows = [decode(row, schema) for row in item["rows"]]
                    assert [encode(row, schema, input_value=True) for row in rows] == item["rows"]
                    frame = session.createDataFrame(rows, schema)
                    frame.createOrReplaceTempView(item["name"])
                    assert frame.schema.jsonValue() == item["schema"]
                    providers.append({**item, "schema_observed": frame.schema.jsonValue(),
                                      "num_partitions": frame.rdd.getNumPartitions(),
                                      "construction": "createDataFrame(list, original StructType)"})
                sessions.append((corpus, session))
                registered.append({key: corpus[key] for key in ["corpus", "source", "metadata"]} | {
                    "registered_inputs": providers, "session_uuid": session._jsparkSession.sessionUUID()})
            header = {**identity, "issue": request["issue"], "spark_only": True, "reads_expected_outputs": False,
                      "corpora": registered, "schema_stage_protocol": STAGES,
                      "input_request": request_receipts[0], "input_requests": request_receipts,
                      "capture_driver": source_receipts[0],
                      "capture_session_helper": receipt_by_path[str(Path(__file__).resolve())],
                      "capture_files": source_receipts, "requested_settings": run_settings,
                      "effective_settings": {
                          key: (spark.conf.get(key) if key.startswith("spark.sql.") else
                                spark.sparkContext.getConf().get(key))
                          for key in observed_settings},
                      "master": spark.sparkContext.master,
                      "spark_context_settings": dict(spark.sparkContext.getConf().getAll()),
                      "owned_gateway": {"pid": gateway.proc.pid, "parent_pid": os.getpid()},
                      "expected_record_count": request["expected_record_count"],
                      "adaptive_policy": ("Explicitly configured; actual effective value observed."
                                          if "spark.sql.adaptive.enabled" in run_settings else
                                          "Not explicitly set; actual effective default observed."),
                      "historical_source_provenance": "Historical source paths and hashes are provenance labels. Only the caller's validated source_paths are opened and hashed."}
            output.write(json.dumps(header, separators=(",", ":")) + "\n")
            output.flush()
            observations = []
            for corpus, session in sessions:
                for case in corpus["cases"]:
                    session.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
                    observed = capture_query(session, case, encode)
                    observed["effective_settings"] = {
                        key: (session.conf.get(key) if key.startswith("spark.sql.") else
                              session.sparkContext.getConf().get(key))
                        for key in observed_settings}
                    observations.append(observed)
                    output.write(json.dumps(observed, separators=(",", ":")) + "\n")
                    output.flush()
            assert len(observations) == request["expected_record_count"]
            assert len({(row["corpus"], row["id"], row["ansi"]) for row in observations}) == request["expected_record_count"]
            assert all(file_sha256(path) == receipt["sha256"] for path, receipt in zip(sources, source_receipts))
            failures = [(row["corpus"], row["id"], row["ansi"]) for row in observations if "capture_failure" in row]
            print(json.dumps({"output": str(output_path), "observations": len(observations),
                              "capture_failures": failures}), flush=True)
            assert not failures, ("instrumentation failures retained", failures)
            lifecycle["capture_status"] = "completed"
        except BaseException:
            lifecycle.update(capture_status="failed", original_failure=traceback.format_exc())
            raise
        finally:
            gateway = gateway or SparkContext._gateway
            try:
                lifecycle["shutdown"] = cleanup(spark, gateway)
            except BaseException:
                lifecycle["shutdown_failure"] = traceback.format_exc()
            lifecycle_file.write(json.dumps(lifecycle, indent=2) + "\n")
            lifecycle_file.flush()
            if sys.exc_info()[0] is None:
                assert "shutdown_failure" not in lifecycle and not lifecycle["shutdown"]["errors"], lifecycle
                assert lifecycle["shutdown"].get("reaped"), lifecycle

