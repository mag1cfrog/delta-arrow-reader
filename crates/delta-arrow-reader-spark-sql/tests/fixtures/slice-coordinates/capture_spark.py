"""Capture the missing SLICE operand-placement and nullable-coordinate boundaries.

The manifest contains inputs and SQL only. --self-check never imports Spark.
"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import traceback
from zipfile import ZipFile

sys.dont_write_bytecode = True
PACKAGE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location(
    "slice_880_encoding", PACKAGE / "references" / "capture_spark_880.py")
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)
decode_input = observer.decode_input
encode_result = observer.encode_result
VERSION, REVISION, TIMEZONE = observer.VERSION, observer.REVISION, observer.TIMEZONE
GROUP = "slice-894-coordinates-current-v1"
ANSI_MODES = [True, False]
SCENARIO_COUNT = 5
OBSERVATION_COUNT = 10


def self_check(manifest):
    assert manifest["spark_version"] == VERSION
    assert manifest["spark_revision"] == REVISION
    assert manifest["timezone"] == TIMEZONE
    assert manifest["group"] == GROUP
    assert manifest["ansi_modes"] == ANSI_MODES
    for relative, expected_hash in manifest["references"].items():
        path = Path(relative)
        assert not path.is_absolute() and ".." not in path.parts
        assert hashlib.sha256((PACKAGE / path).read_bytes()).hexdigest() == expected_hash
    scenarios, inputs = manifest["scenarios"], manifest["inputs"]
    assert len(scenarios) == manifest["scenario_count"] == SCENARIO_COUNT
    assert SCENARIO_COUNT * len(ANSI_MODES) == manifest["observation_count"] == OBSERVATION_COUNT
    assert len({case["id"] for case in scenarios}) == SCENARIO_COUNT
    assert len({case["sql"] for case in scenarios}) == SCENARIO_COUNT
    assert [case["id"] for case in scenarios] == [
        "provider_array_literal_last_max_length",
        "literal_array_provider_last_max_length",
        "literal_negative_last_max_length",
        "provider_negative_last_max_length",
        "nullable_boundary_coordinates"]
    assert manifest["identities"] == [{"id": case["id"], "ansi": ansi}
                                     for ansi in ANSI_MODES for case in scenarios]
    header = json.loads((PACKAGE / "references" / "provider-header-880.json").read_text())
    assert inputs[:3] == header["inputs"]
    assert manifest["input_encoding"] == header["input_encoding"]
    assert manifest["source_reference_sha256"] == (
        "d3d5001f84003b5756483f8f2699100746b1fd105ffcf92d8a974c219abafeb0")
    assert len(inputs) == len({provider["name"] for provider in inputs}) == 4
    assert {provider["name"] for provider in inputs} == {
        "slice_input", "slice_fields", "slice_empty", "slice_coordinate_nullable"}
    assert len(inputs[-1]["rows"]) == 8
    assert all(set(case) == {"id", "sql", "ordered"} for case in scenarios)

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
            assert encode_result(decode_input(row, provider["schema"])) == row
            check_nullability(row, provider["schema"], False)
    assert encode_result((None, [1, True, "x"])) == [None, ["1", "true", "x"]]
    assert error_details(ValueError("self-check"), ()) == {
        "condition": None, "sql_state": None, "message_parameters": None,
        "java_causes": [], "metadata_errors": []}
    class SparkError(Exception):
        def getCondition(self):
            return "FULL.CONDITION"

        def getSqlState(self):
            return "22023"

        def getMessageParameters(self):
            return {"parameter": "value"}

    details = error_details(SparkError("self-check"), SparkError)
    assert details["condition"] == "FULL.CONDITION"
    assert details["sql_state"] == "22023"
    assert details["message_parameters"] == {"parameter": "value"}


def error_details(error, spark_exception_type):
    details = {"condition": None, "sql_state": None, "message_parameters": None,
               "java_causes": [], "metadata_errors": []}
    if isinstance(error, spark_exception_type):
        details.update(condition=error.getCondition(), sql_state=error.getSqlState(),
                       message_parameters=error.getMessageParameters())
    cause = getattr(error, "java_exception", None)
    seen = set()
    while cause is not None:
        identity = cause._target_id
        if identity in seen:
            break
        seen.add(identity)
        item = {"class": str(cause.getClass().getName()), "message": str(cause.toString())}
        methods = {str(method.getName()) for method in cause.getClass().getMethods()}
        for key, method in (("condition", "getCondition"), ("sql_state", "getSqlState"),
                            ("message_parameters", "getMessageParameters")):
            if method not in methods:
                continue
            try:
                value = getattr(cause, method)()
                if key == "message_parameters" and value is not None:
                    entries, parameters = value.entrySet().iterator(), {}
                    while entries.hasNext():
                        entry = entries.next()
                        parameters[str(entry.getKey())] = str(entry.getValue())
                    value = parameters
                item[key] = value
            except Exception as metadata_error:
                details["metadata_errors"].append({"class": item["class"], "method": method,
                                                   "error": str(metadata_error)})
        details["java_causes"].append(item)
        if details["condition"] is None and item.get("condition") is not None:
            for key in ("condition", "sql_state", "message_parameters"):
                details[key] = item.get(key)
        cause = cause.getCause()
    return details


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    manifest_path = PACKAGE / "manifest.json"
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
    assert os.environ.get("TZ") == TIMEZONE, "run with TZ=UTC"

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
        # Syntax/setup failures abort before query errors are recorded.
        for definition in manifest["scenarios"]:
            spark._jsparkSession.sessionState().sqlParser().parsePlan(definition["sql"])
        records = [{"spark_version": spark.version, "spark_revision": REVISION,
                    "spark_build": build, "timezone": TIMEZONE, "group": GROUP,
                    "process_timezone": os.environ["TZ"],
                    "capture_sha256": source_sha256, "manifest_sha256": manifest_sha256,
                    "references": manifest["references"], "source_commit": manifest["source_commit"],
                    "source_reference_sha256": manifest["source_reference_sha256"],
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
                    stages["resolved"] = public
                    for name, getter, phase in [
                        ("analyzed", execution.analyzed, "analyzed_schema"),
                        ("optimized", execution.optimizedPlan, "optimized_schema"),
                        ("physical", execution.sparkPlan, "physical_schema"),
                        ("executed_before_collect", execution.executedPlan, "executed_schema"),
                    ]:
                        plan = getter()
                        stages[name] = json.loads(plan.schema().json())
                        plans[name] = plan.toString()
                    phase = "collect"
                    rows = [encode_result(row) for row in frame.collect()]
                    record["spark_collected_rows"] = list(rows)
                    if not record["ordered"]:
                        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                    phase = "executed_schema_after_collect"
                    plan = execution.executedPlan()
                    stages["executed"] = json.loads(plan.schema().json())
                    plans["executed"] = plan.toString()
                    assert public == stages["analyzed"]
                    record["expected"] = {"schema": public, "rows": rows}
                except Exception as error:
                    if not isinstance(error, PySparkException) and getattr(error, "java_exception", None) is None:
                        raise
                    details = error_details(error, PySparkException)
                    record.update(expected={"condition": details["condition"]}, spark_error=str(error),
                                  spark_error_phase=phase, spark_error_class=type(error).__name__,
                                  spark_error_details=details, spark_error_traceback=traceback.format_exc())
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
