"""Capture all 598 NULL RANGE requests with pinned Spark; --self-check starts no JVM.

The two sibling source-only requests and original DATE manifest supply SQL and providers.
Historical source paths are metadata. No recorded query outcomes are opened.
"""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys
import traceback

sys.dont_write_bytecode = True

from capture_date_range_747_spark import (
    OBSERVED_SETTINGS, REVISION, SETTINGS, STAGES, VERSION,
    capture, cleanup, self_check as check_capture_protocol,
)

HERE = Path(__file__).resolve().parent
REQUEST = HERE / "null-range-754-cases.json"
BOUNDARY_REQUEST = HERE / "null-range-754-boundary-cases.json"
ORIGINAL_MANIFEST = HERE / "date-range-747-cases.json"


def load_capture_requests():
    request = json.loads(REQUEST.read_text())
    assert request["issue"] == 754 and request["spark_version"] == VERSION
    assert request["spark_revision"] == REVISION
    assert request["expected_record_count"] == 582
    assert request["original_record_count"] == 14 and request["supplement_record_count"] == 568
    assert request["batch_sizes"] == [1, 32]
    assert request["target_partitions"] == request["provider_partitions"] == 2
    with ORIGINAL_MANIFEST.open("rb") as source:
        original_hash = hashlib.file_digest(source, "sha256").hexdigest()
    assert original_hash == request["original_source_manifest"]["sha256"]
    original = json.loads(ORIGINAL_MANIFEST.read_text())
    selected = []
    for corpus in original["corpora"]:
        cases = [case for case in corpus["cases"] if case["ownership"] == "null_offset_owner"]
        if cases:
            selected.append(dict(corpus, cases=cases))
    assert len(request["corpora"]) == 3 and request["corpora"][:-1] == selected
    assert sum(len(corpus["cases"]) for corpus in selected) == 14
    supplement = request["corpora"][-1]
    assert supplement["corpus"] == "null-range-754-supplement"
    assert len(supplement["cases"]) == 568
    assert dict(Counter(case["coverage"]["family"] for case in supplement["cases"])) == request["supplement_family_counts"] == {
        "core": 64, "types": 96, "bare": 16, "partners": 72, "provider_edges": 48,
        "consumers": 52, "outer": 16, "date_extreme": 16, "null_minimum": 144,
        "numeric_coercion": 8, "unaliased": 8, "admission": 28,
    }
    cases = [case for corpus in request["corpora"] for case in corpus["cases"]]
    assert len(cases) == len({(case["corpus"], case["id"], case["ansi"]) for case in cases}) == 582
    assert sum(case["ordered"] for case in cases) == 24
    source_fields = {"corpus", "id", "ansi", "sql", "ordered", "ownership"}
    for corpus in request["corpora"]:
        assert corpus["metadata"]["spark_version"] == VERSION
        assert corpus["metadata"]["timezone"] == "UTC"
        for case in corpus["cases"]:
            assert set(case) == source_fields | ({"coverage"} if corpus is supplement else set())
            assert case["corpus"] == corpus["corpus"] and isinstance(case["sql"], str)
            assert isinstance(case["id"], str) and case["ownership"] == "null_offset_owner"
            assert type(case["ansi"]) is bool and type(case["ordered"]) is bool
        inputs = corpus["metadata"]["inputs"]
        assert len({item["name"] for item in inputs}) == len(inputs)
        for item in inputs:
            fields = item["schema"]["fields"]
            assert item["schema"]["type"] == "struct" and fields
            for field in fields:
                assert field["type"] == "integer" and type(field["nullable"]) is bool
                assert isinstance(field["name"], str) and isinstance(field["metadata"], dict)
            for row in item["rows"]:
                assert len(row) == len(fields)
                for value, field in zip(row, fields):
                    if value is None:
                        assert field["nullable"]
                    else:
                        assert str(int(value)) == value and -(2**31) <= int(value) < 2**31
    boundary_request = json.loads(BOUNDARY_REQUEST.read_text())
    assert boundary_request["issue"] == 754 and boundary_request["spark_version"] == VERSION
    assert boundary_request["spark_revision"] == REVISION
    assert boundary_request["expected_record_count"] == 16
    boundary, = boundary_request["corpora"]
    assert boundary["corpus"] == "null-range-boundary-754-controls"
    assert boundary["metadata"] == {"spark_version": VERSION, "timezone": "UTC", "inputs": []}
    boundary_cases = boundary["cases"]
    assert len(boundary_cases) == len({(case["id"], case["ansi"]) for case in boundary_cases}) == 16
    assert len({case["sql"] for case in boundary_cases}) == 8
    assert dict(Counter(case["coverage"]["key_type"] for case in boundary_cases)) == {
        "DECIMAL(38,0)": 8, "FLOAT": 4, "DOUBLE": 4,
    }
    for case in boundary_cases:
        assert set(case) == (source_fields - {"ownership"}) | {"coverage"}
        assert case["corpus"] == boundary["corpus"] and case["ordered"] is True
        assert isinstance(case["id"], str) and type(case["ansi"]) is bool
        peers = [other for other in boundary_cases if other["id"] == case["id"]]
        assert {other["ansi"] for other in peers} == {True, False}
        assert len({other["sql"] for other in peers}) == 1
        coverage = case["coverage"]
        assert set(coverage) == {"key_type", "order", "frame", "inline_values"}
        values = coverage["inline_values"]
        assert isinstance(values, list) and values
        assert all(isinstance(row, list) and len(row) == 2 and type(row[0]) is int
                   and isinstance(row[1], str) for row in values)
        assert [row[0] for row in values] == list(range(len(values)))
        sql_values = ",".join(f"({row[0]},{row[1]})" for row in values)
        assert case["sql"] == (
            f"SELECT id,COUNT(*) OVER (ORDER BY k {coverage['order']} {coverage['frame']}) AS n "
            f"FROM VALUES {sql_values} AS t(id,k) ORDER BY id")
    decimal = [case for case in boundary_cases if case["coverage"]["key_type"] == "DECIMAL(38,0)"]
    maximum = "CAST('" + "9" * 38 + "' AS DECIMAL(38,0))"
    null = "CAST(NULL AS DECIMAL(38,0))"
    frames = {f"RANGE BETWEEN {null} PRECEDING AND {maximum} FOLLOWING",
              f"RANGE BETWEEN {maximum} FOLLOWING AND {null} PRECEDING"}
    orders = {"ASC NULLS FIRST", "ASC NULLS LAST"}
    combinations = Counter((case["coverage"]["frame"], case["coverage"]["order"]) for case in decimal)
    assert combinations == Counter({(frame, order): 2 for frame in frames for order in orders})
    assert all(case["coverage"]["inline_values"] == [[0, maximum], [1, null]] for case in decimal)
    all_cases = cases + boundary_cases
    assert len(all_cases) == len({(case["corpus"], case["id"], case["ansi"]) for case in all_cases}) == 598
    assert sum(case["ordered"] for case in all_cases) == 40
    return dict(request, expected_record_count=598, boundary_record_count=16,
                corpora=request["corpora"] + [boundary])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    assert sys.version_info[:2] == (3, 12), "use Python 3.12"
    request = load_capture_requests()
    if args.self_check:
        assert args.output is None and "pyspark" not in sys.modules
        check_capture_protocol(request)
        return
    assert args.output is not None and args.output.parent.is_dir(), "provide a fresh output path"
    lifecycle_path = args.output.with_suffix(".lifecycle.json")
    for path in [args.output, lifecycle_path]:
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
    sources = [Path(__file__).resolve(), REQUEST, BOUNDARY_REQUEST, ORIGINAL_MANIFEST,
               HERE / "capture_date_range_747_spark.py", HERE / "capture_floating_group_keys_spark.py",
               HERE / "capture_window_name_stages_spark.py", HERE / "recapture_string_binary_spark.py"]
    source_receipts = [{"path": str(path), "sha256": file_sha256(path)} for path in sources]
    spark = gateway = None
    lifecycle = {"spark_only": True, "reads_expected_outputs": False, "capture_pid": os.getpid(),
                 "foreign_gateway_reuse_rejected": True}
    with args.output.open("x") as output, lifecycle_path.open("x") as lifecycle_file:
        try:
            builder = SparkSession.builder.master("local[1]").appName("dar-754-null-range-recapture")
            builder = builder.config("spark.ui.enabled", "false")
            for key, value in SETTINGS.items():
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
                for key, value in SETTINGS.items():
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
            header = {**identity, "issue": 754, "spark_only": True, "reads_expected_outputs": False,
                      "corpora": registered, "schema_stage_protocol": STAGES,
                      "input_request": {"path": str(REQUEST), "sha256": source_receipts[1]["sha256"]},
                      "input_requests": [source_receipts[1], source_receipts[2]],
                      "capture_files": source_receipts, "requested_settings": SETTINGS,
                      "effective_settings": {key: spark.conf.get(key) for key in OBSERVED_SETTINGS},
                      "master": spark.sparkContext.master,
                      "spark_context_settings": dict(spark.sparkContext.getConf().getAll()),
                      "owned_gateway": {"pid": gateway.proc.pid, "parent_pid": os.getpid()},
                      "expected_record_count": request["expected_record_count"],
                      "adaptive_policy": "Not explicitly set; actual effective default observed.",
                      "historical_source_provenance": "Source paths and hashes are provenance labels. Validation reads the sibling original DATE manifest; historical source paths are not opened."}
            output.write(json.dumps(header, separators=(",", ":")) + "\n")
            output.flush()
            observations = []
            for corpus, session in sessions:
                for case in corpus["cases"]:
                    session.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
                    observed = capture(session, case, encode)
                    observed["effective_settings"] = {key: session.conf.get(key) for key in OBSERVED_SETTINGS}
                    observations.append(observed)
                    output.write(json.dumps(observed, separators=(",", ":")) + "\n")
                    output.flush()
            assert len(observations) == request["expected_record_count"]
            assert len({(row["corpus"], row["id"], row["ansi"]) for row in observations}) == request["expected_record_count"]
            assert all(file_sha256(path) == receipt["sha256"] for path, receipt in zip(sources, source_receipts))
            failures = [(row["corpus"], row["id"], row["ansi"]) for row in observations if "capture_failure" in row]
            print(json.dumps({"output": str(args.output), "observations": len(observations),
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


if __name__ == "__main__":
    main()
