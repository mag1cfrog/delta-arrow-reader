"""Capture #880 SLICE parameter boundaries and retained #881 controls from pinned Spark.

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
GROUP = "slice-parameters-880-current-v1"
ANSI_MODES = [True, False]
SCENARIO_COUNT = 154
OBSERVATION_COUNT = 308


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
        decoded = int(value)
        assert str(decoded) == value and -(2**31) <= decoded < 2**31
        return decoded
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
    for path_key, hash_key in (("reused_observer", "reused_observer_sha256"),
                               ("inherited_reference", "inherited_reference_sha256")):
        relative = Path(manifest[path_key])
        assert not relative.is_absolute() and ".." not in relative.parts
        reference = Path(__file__).resolve().parent / relative
        assert hashlib.sha256(reference.read_bytes()).hexdigest() == manifest[hash_key]
    scenarios, inputs = manifest["scenarios"], manifest["inputs"]
    assert len(scenarios) == manifest["scenario_count"] == SCENARIO_COUNT
    assert SCENARIO_COUNT * len(ANSI_MODES) == manifest["observation_count"] == OBSERVATION_COUNT
    assert len({case["id"] for case in scenarios}) == SCENARIO_COUNT
    assert len({case["sql"] for case in scenarios}) == SCENARIO_COUNT
    assert manifest["identities"] == [{"id": case["id"], "ansi": ansi}
                                     for ansi in ANSI_MODES for case in scenarios]
    assert len(inputs) == len({provider["name"] for provider in inputs}) == 3
    assert {provider["name"] for provider in inputs} == {"slice_input", "slice_fields", "slice_empty"}
    assert next(provider for provider in inputs if provider["name"] == "slice_empty")["rows"] == []
    assert all("expected" not in case and "schema_stages" not in case for case in scenarios)

    def check_nullability(value, data_type, nullable):
        if value is None:
            assert nullable
        elif isinstance(data_type, dict) and data_type["type"] == "array":
            for item in value:
                check_nullability(item, data_type["elementType"], data_type["containsNull"])
        elif isinstance(data_type, dict) and data_type["type"] == "struct":
            assert len(value) == len(data_type["fields"])
            for item, field in zip(value, data_type["fields"]):
                check_nullability(item, field["type"], field["nullable"])

    for provider in inputs:
        assert provider["schema"]["type"] == "struct"
        keys = [int(row[0]) for row in provider["rows"]]
        assert keys == sorted(set(keys))
        for row in provider["rows"]:
            decode_input(row, provider["schema"])
            check_nullability(row, provider["schema"], False)
    for case in scenarios:
        assert case["sql"].startswith("SELECT ")
        assert case["consumer"] in ("combined", "direct", "is_null", "struct", "overlap")
    assert {case["shape"] for case in scenarios if case["family"] == "shape_parameters"} == {"empty", "all_null", "null"}
    assert {case["evaluation"] for case in scenarios if case["family"] == "evaluation_priority"} == {"foldable", "live"}
    assert len([case for case in scenarios if case["family"] == "inherited_881_control"]) == 4
    assert len([case for case in scenarios if case["family"] == "zero_row_provider"]) == 8
    assert len([case for case in scenarios if case["family"] == "zero_row_constant"]) == 4
    assert len([case for case in scenarios if case["family"] == "unary_length"]) == 6
    assert len([case for case in scenarios if case["family"] == "constant_consumers"]) == 4
    assert len([case for case in scenarios if case["family"] == "discarded_branch"]) == 1
    assert {case["branch"] for case in scenarios if case["family"] == "conditional_constant"} == {"selected", "discarded"}
    assert len([case for case in scenarios if case["family"] == "literal_parameters"]) == 31
    assert len([case for case in scenarios if case["family"] == "provider_parameters"]) == 12
    assert {case["consumer"] for case in scenarios if case["family"] == "consumer_parameters"} == {"direct", "is_null", "struct"}
    assert {(case["array_parent_nullable"], case["array_element_nullable"])
            for case in scenarios if case["family"] == "provider_declarations"} == {
                (False, False), (False, True), (True, False), (True, True)}
    assert {case["numeric_nullability"] for case in scenarios
            if case["family"] == "provider_declarations"} == {
                "required", "nullable_start", "nullable_length", "nullable_both"}
    assert encode_result((None, [1, True, "x"])) == [None, ["1", "true", "x"]]
    for value in ("-2147483648", "2147483647"):
        assert str(decode_input(value, "integer")) == value
    for value in ("2147483648", "-2147483649", "01"):
        try:
            decode_input(value, "integer")
        except AssertionError:
            pass
        else:
            raise AssertionError(f"invalid INT32 input accepted: {value}")
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
                    # Only actual cast/parameter SQL errors enter the reference, never setup errors.
                    assert condition in ("CAST_INVALID_INPUT", "INVALID_PARAMETER_VALUE.START",
                                         "INVALID_PARAMETER_VALUE.LENGTH"), (
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
