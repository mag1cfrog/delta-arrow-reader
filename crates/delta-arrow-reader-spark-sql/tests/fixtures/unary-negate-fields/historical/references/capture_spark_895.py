"""Capture standalone unary-negation fields and bounded overflow controls from pinned Spark.

The manifest contains inputs and SQL only. --self-check never imports Spark.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

VERSION = "4.2.0"
REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
TIMEZONE = "UTC"
GROUP = "unary-negate-fields-current-v1"
ANSI_MODES = [True, False]
SCENARIO_COUNT = 36
OBSERVATION_COUNT = 72


def decode_input(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, dict) and data_type["type"] == "struct":
        assert isinstance(value, list) and len(value) == len(data_type["fields"])
        return tuple(decode_input(item, field["type"])
                     for item, field in zip(value, data_type["fields"]))
    if isinstance(data_type, dict) and data_type["type"] == "array":
        assert isinstance(value, list)
        return [decode_input(item, data_type["elementType"]) for item in value]
    if data_type == "integer":
        assert isinstance(value, str)
        return int(value)
    assert data_type == "string" and isinstance(value, str), data_type
    return value


def encode_result(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [encode_result(item) for item in value]
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    assert isinstance(value, str), type(value)
    return value


def self_check(manifest):
    assert manifest["spark_version"] == VERSION
    assert manifest["spark_revision"] == REVISION
    assert manifest["timezone"] == TIMEZONE
    assert manifest["group"] == GROUP
    assert manifest["ansi_modes"] == ANSI_MODES
    reference = Path(manifest["reused_observer"])
    assert not reference.is_absolute() and ".." not in reference.parts
    reference = Path(__file__).resolve().parent / reference
    assert hashlib.sha256(reference.read_bytes()).hexdigest() == manifest["reused_observer_sha256"]
    scenarios, inputs = manifest["scenarios"], manifest["inputs"]
    assert len(scenarios) == manifest["scenario_count"] == SCENARIO_COUNT
    assert SCENARIO_COUNT * len(ANSI_MODES) == manifest["observation_count"] == OBSERVATION_COUNT
    assert len({case["id"] for case in scenarios}) == SCENARIO_COUNT
    assert len({case["sql"] for case in scenarios}) == SCENARIO_COUNT
    assert manifest["identities"] == [{"id": case["id"], "ansi": ansi}
                                     for ansi in ANSI_MODES for case in scenarios]
    assert {provider["name"] for provider in inputs} == {"unary_input", "unary_empty"}
    assert len(inputs) == 2
    assert inputs[0]["schema"] == inputs[1]["schema"]
    fields = inputs[0]["schema"]["fields"]
    assert [(field["name"], field["type"], field["nullable"]) for field in fields] == [
        ("k", "integer", False), ("required_int", "integer", False),
        ("nullable_int", "integer", True)]
    assert fields[1]["metadata"] == {"source_tag": "required_int", "unit": "count"}
    assert fields[2]["metadata"] == {"source_tag": "nullable_int", "unit": "count"}
    assert len(inputs[0]["rows"]) == 4 and inputs[1]["rows"] == []
    for provider in inputs:
        keys = [int(row[0]) for row in provider["rows"]]
        assert keys == sorted(set(keys))
        for row in provider["rows"]:
            decode_input(row, provider["schema"])
            for value, field in zip(row, fields):
                if value is None:
                    assert field["nullable"]
                else:
                    assert isinstance(value, str) and str(int(value)) == value
                    assert -(2**31) <= int(value) < 2**31
    assert {case["family"]: sum(c["family"] == case["family"] for c in scenarios)
            for case in scenarios} == {"standalone_int": 24, "empty_provider": 4,
                                      "source_metadata": 2, "standalone_bigint": 4,
                                      "constant_int_min": 2}
    for case in scenarios:
        assert case["sql"].startswith("SELECT ") and case["sql"].isascii()
        assert case["consumer"] in ("combined", "direct", "is_null", "struct")
        assert not {"expected", "schema_stages", "owner_issue"} & case.keys()
    assert {(case["declaration"], case["syntax"], case["partition"], case["consumer"])
            for case in scenarios if case["family"] == "standalone_int"} == {
                (declaration, syntax, partition, consumer)
                for declaration in ("required", "nullable")
                for syntax in ("operator", "function")
                for partition in ("ordinary", "int_min")
                for consumer in ("direct", "is_null", "struct")}
    assert encode_result((None, [1, True, "x", 2147483648])) == [
        None, ["1", "true", "x", "2147483648"]]
    assert decode_input(["1", None], {"type": "array", "elementType": "integer"}) == [1, None]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    package = Path(__file__).resolve().parent
    source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    manifest_path = package / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    assert manifest["capture_script_sha256"] == source_sha256
    self_check(manifest)
    if args.self_check:
        assert args.output is None, "--self-check does not write capture output"
        assert "pyspark" not in sys.modules
        print(json.dumps({"self_check": "passed", "scenarios": SCENARIO_COUNT,
                          "observations": OBSERVATION_COUNT,
                          "capture_sha256": source_sha256,
                          "manifest_sha256": manifest_sha256}))
        return
    assert args.output is not None, "an output path is required"
    assert args.output.parent.is_dir(), args.output.parent
    assert not args.output.exists(), args.output

    os.environ["PYSPARK_PYTHON"] = sys.executable
    import pyspark
    from pyspark.errors import PySparkException
    from pyspark.sql import SparkSession, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={REVISION}\n" in build, build
    assert f"version={VERSION}\n" in build, build
    spark = (SparkSession.builder.master("local[1]").appName(GROUP)
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", TIMEZONE).getOrCreate())
    try:
        assert spark.version == VERSION, spark.version
        assert spark.conf.get("spark.sql.session.timeZone") == TIMEZONE
        spark.sparkContext.setLogLevel("ERROR")
        observed_inputs = []
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            rows = [decode_input(row, provider["schema"]) for row in provider["rows"]]
            frame = spark.createDataFrame(rows, schema)
            frame.createOrReplaceTempView(provider["name"])
            actual_schema = frame.schema.jsonValue()
            assert actual_schema == provider["schema"], provider["name"]
            assert frame.count() == len(rows), provider["name"]
            actual_rows = [encode_result(row) for row in frame.orderBy("k").collect()]
            assert actual_rows == provider["rows"], (provider["name"], actual_rows)
            observed_inputs.append({"name": provider["name"], "schema": actual_schema,
                                    "rows": actual_rows})
        # Syntax/setup failures must abort before any SQL error is recorded.
        for definition in manifest["scenarios"]:
            spark._jsparkSession.sessionState().sqlParser().parsePlan(definition["sql"])
        records = [{"spark_version": spark.version, "spark_revision": REVISION,
                    "spark_build": build, "timezone": TIMEZONE, "group": GROUP,
                    "capture_sha256": source_sha256, "manifest_sha256": manifest_sha256,
                    "observations": OBSERVATION_COUNT, "inputs": observed_inputs,
                    "input_encoding": manifest["input_encoding"],
                    "provider_input_roundtrip_checked": True}]
        for ansi in ANSI_MODES:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            assert spark.conf.get("spark.sql.ansi.enabled") == str(ansi).lower()
            for definition in manifest["scenarios"]:
                record = dict(definition, ansi=ansi, timezone=TIMEZONE)
                stages, plans = {}, {}
                phase = "sql"
                try:
                    frame = spark.sql(record["sql"])
                    execution = frame._jdf.queryExecution()
                    phase = "public_schema"
                    public = frame.schema.jsonValue()
                    stages["public"] = public
                    for name, getter, phase in [
                        ("analyzed", execution.analyzed, "analyzed_schema"),
                        ("optimized", execution.optimizedPlan, "optimized_schema"),
                        ("executed_before_collect", execution.executedPlan, "executed_schema"),
                    ]:
                        plan = getter()
                        stages[name] = json.loads(plan.schema().json())
                        plans[name] = plan.toString()
                    phase = "collect"
                    rows = [encode_result(row) for row in frame.collect()]
                    if not record.get("ordered", False):
                        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                    phase = "executed_schema_after_collect"
                    plan = execution.executedPlan()
                    stages["executed"] = json.loads(plan.schema().json())
                    plans["executed"] = plan.toString()
                    assert public == stages["analyzed"]
                    record["expected"] = {"schema": public, "rows": rows}
                except Exception as error:
                    condition = error.getCondition() if isinstance(error, PySparkException) else None
                    # Only actual cast/overflow SQL errors enter the reference, never setup errors.
                    assert condition in ("CAST_INVALID_INPUT", "ARITHMETIC_OVERFLOW"), (
                        record["id"], phase, type(error).__name__, str(error))
                    record.update(expected={"condition": condition}, spark_error=str(error),
                                  spark_error_phase=phase, spark_error_class=type(error).__name__)
                record.update(schema_stages=stages, spark_plans=plans)
                records.append(record)
        assert len(records) - 1 == OBSERVATION_COUNT
        assert [(record["id"], record["ansi"]) for record in records[1:]] == [
            (definition["id"], ansi) for ansi in ANSI_MODES for definition in manifest["scenarios"]]
        assert len({(record["id"], record["ansi"]) for record in records[1:]}) == OBSERVATION_COUNT
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": OBSERVATION_COUNT,
                          "capture_sha256": source_sha256,
                          "manifest_sha256": manifest_sha256,
                          "raw_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
