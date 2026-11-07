"""Recapture aggregate Fields and typed rows from the committed query manifest."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from capture_global_aggregate_admission_spark import capture, encode
from recapture_string_binary_spark import runtime_identity


HERE = Path(__file__).resolve().parent
STAGE_KEYS = {
    "analyzed": "analyzed",
    "optimized": "optimized",
    "executed_before_collect": "executed",
    "executed": "executed_after_collect",
}


def build_aggregate_reference_record(case, observation):
    schemas = observation["schema_stages"]
    stages = {stage: schemas[source] for stage, source in STAGE_KEYS.items() if source in schemas}
    record = dict(case, status="ok" if observation.get("phase") == "success" else "error",
                  schema_stages=stages, stage_availability={
                      "public": "public" in schemas,
                      **{stage: stage in stages for stage in STAGE_KEYS},
                  })
    if "public" in schemas:
        record["public_schema"] = schemas["public"]
    if record["status"] == "ok":
        rows = observation["raw_rows"]
        record["rows_raw"] = rows
        record["expected"] = {
            "schema": schemas["public"],
            "rows": rows if case["ordered"] else sorted(
                rows, key=lambda row: json.dumps(row, separators=(",", ":"))),
        }
    else:
        record["spark_observation"] = observation
    return record


def reap_owned_gateway(process):
    try:
        return process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise


def self_check():
    from unittest.mock import Mock, call

    normal = Mock()
    normal.wait.return_value = 0
    assert reap_owned_gateway(normal) == 0
    assert normal.mock_calls == [call.wait(timeout=30)]
    timeout = subprocess.TimeoutExpired("owned gateway", 30)
    timed_out = Mock()
    timed_out.wait.side_effect = [timeout, -9]
    try:
        reap_owned_gateway(timed_out)
    except subprocess.TimeoutExpired as error:
        assert error is timeout
    else:
        raise AssertionError("owned gateway timeout was swallowed")
    assert timed_out.mock_calls == [call.wait(timeout=30), call.kill(), call.wait()]

    schemas = {name: {"name": name} for name in [
        "public", "analyzed", "optimized", "executed", "executed_after_collect"]}
    observation = {"phase": "success", "schema_stages": schemas,
                   "raw_rows": [["2"], ["1"], ["2"], [None]]}
    ordered = build_aggregate_reference_record({"ordered": True}, observation)
    unordered = build_aggregate_reference_record({"ordered": False}, observation)
    assert ordered["expected"]["rows"] == [["2"], ["1"], ["2"], [None]]
    assert unordered["expected"]["rows"] == [["1"], ["2"], ["2"], [None]]
    assert unordered["rows_raw"] == [["2"], ["1"], ["2"], [None]]
    assert ordered["schema_stages"] == {
        "analyzed": {"name": "analyzed"}, "optimized": {"name": "optimized"},
        "executed_before_collect": {"name": "executed"},
        "executed": {"name": "executed_after_collect"},
    }
    assert ordered["public_schema"] == {"name": "public"}
    assert ordered["stage_availability"] == {
        "public": True, "analyzed": True, "optimized": True,
        "executed_before_collect": True, "executed": True,
    }
    failure = {"phase": "planning", "failure_stage": "optimized",
               "expected": {"condition": "EXAMPLE"},
               "schema_stages": {"public": schemas["public"]}}
    failed = build_aggregate_reference_record({"ordered": False}, failure)
    assert failed["status"] == "error" and failed["spark_observation"] == failure
    assert "expected" not in failed and failed["schema_stages"] == {}
    assert failed["stage_availability"] == {
        "public": True, "analyzed": False, "optimized": False,
        "executed_before_collect": False, "executed": False,
    }
    print(json.dumps({"self_check": "passed"}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("--manifest", type=Path, default=HERE / "aggregate-query-results-cases.json")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        assert args.output is None
        self_check()
        return
    assert args.output is not None and not args.output.exists() and not args.output.is_symlink()
    manifest = json.loads(args.manifest.read_text())
    definitions = manifest["cases"]
    assert len(definitions) == len({case["id"] for case in definitions}) == 18
    metadata = manifest["capture"]
    assert "PYSPARK_GATEWAY_PORT" not in os.environ
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark import SparkContext
    from pyspark.sql import SparkSession, types
    from capture_window_name_stages_spark import decode

    assert SparkContext._gateway is None
    spark, process = None, None
    failures, mismatches, count = [], [], 0
    try:
        builder = SparkSession.builder.master("local[1]").appName("dar-aggregate-query-results")
        builder = builder.config("spark.ui.enabled", "false")
        for key, value in metadata["requested_spark_sql_settings"].items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()
        process = spark.sparkContext._gateway.proc
        assert process is not None
        spark.sparkContext.setLogLevel("ERROR")
        identity = runtime_identity(spark)
        assert identity["spark_version"] == metadata["spark_version"]
        assert identity["spark_build"]["SPARK_REVISION"]["value"] == metadata["spark_revision"]
        assert identity["java_properties"]["java.specification.version"] == "21"
        registered = []
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            rows = [decode(row, schema) for row in provider["rows"]]
            frame = spark.createDataFrame(rows, schema)
            frame.createOrReplaceTempView(provider["name"])
            receipt = {"name": provider["name"], "schema": frame.schema.jsonValue(),
                       "rows": [encode(row, schema) for row in rows]}
            assert receipt == provider
            registered.append(receipt)
        setting_keys = list(metadata["effective_spark_sql_settings"])
        settings = {key: spark.conf.get(key) for key in setting_keys}
        assert all(settings[key] == value for key, value in metadata["requested_spark_sql_settings"].items())
        header = {
            **metadata, **identity, "inputs": registered,
            "legacy_source": manifest["legacy_source"],
            "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "request_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
            "request_path": str(args.manifest), "effective_spark_sql_settings": settings,
            "settings": {row.key: row.value for row in spark.sql("SET -v").collect()},
            "spark_context_settings": dict(spark.sparkContext.getConf().getAll()),
        }
        with args.output.open("x", encoding="utf-8", newline="\n") as output:
            output.write(json.dumps(header, separators=(",", ":"), ensure_ascii=True) + "\n")
            output.flush()
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for definition in definitions:
                    case = {key: definition[key] for key in ["id", "sql", "ordered"]}
                    record = build_aggregate_reference_record(case, capture(spark, case["sql"]))
                    record.update(ansi=ansi, timezone=metadata["timezone"],
                                  requested_spark_sql_settings={
                                      **metadata["requested_spark_sql_settings"],
                                      "spark.sql.ansi.enabled": str(ansi).lower()},
                                  effective_spark_sql_settings={
                                      key: spark.conf.get(key) for key in setting_keys})
                    if record["status"] == "ok":
                        matches = {key: record["expected"][key] == definition["expected"][key]
                                   for key in ["schema", "rows"]}
                        record["legacy_reference_match"] = matches
                        if not all(matches.values()):
                            mismatches.append([case["id"], ansi, matches])
                    else:
                        failures.append([case["id"], ansi])
                    output.write(json.dumps(record, separators=(",", ":"), ensure_ascii=True) + "\n")
                    output.flush()
                    count += 1
        assert count == 36 and not failures and not mismatches, (count, failures, mismatches)
    finally:
        if process is None:
            process = getattr(SparkContext._gateway, "proc", None)
        try:
            if spark is not None:
                spark.stop()
        finally:
            if process is not None:
                try:
                    if process.stdin is not None:
                        process.stdin.close()
                finally:
                    exit_code = reap_owned_gateway(process)
                    print(json.dumps({"owned_gateway_pid": process.pid, "exit_code": exit_code,
                                      "reaped": True}), file=sys.stderr)
                    assert exit_code == 0
    print(json.dumps({"observations": count, "failures": failures, "legacy_mismatches": mismatches,
                      "output_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))


if __name__ == "__main__":
    main()
