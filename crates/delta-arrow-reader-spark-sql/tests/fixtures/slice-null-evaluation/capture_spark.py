"""Capture bounded #881 SLICE NULL evaluation from pinned Spark.

The manifest has inputs and SQL only. --self-check never imports Spark.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import traceback
from zipfile import ZipFile

sys.dont_write_bytecode = True
PACKAGE = Path(__file__).resolve().parent
VERSION = "4.2.0"
REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
BASELINE = "085986ca2a53d8e249222ecd4052ea884386a203"
GROUP = "slice-881-null-evaluation-current-v1"
TIMEZONE = "UTC"
ANSI_MODES = [True, False]
SQL_CONDITIONS = {"CAST_INVALID_INPUT", "INVALID_PARAMETER_VALUE.START",
                  "INVALID_PARAMETER_VALUE.LENGTH"}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def decode_input(value, data_type, nullable=False):
    if value is None:
        assert nullable, "NULL violates provider schema"
        return None
    if isinstance(data_type, dict) and data_type["type"] == "struct":
        assert isinstance(value, list) and len(value) == len(data_type["fields"])
        return tuple(decode_input(item, field["type"], field["nullable"])
                     for item, field in zip(value, data_type["fields"]))
    if isinstance(data_type, dict) and data_type["type"] == "array":
        assert isinstance(value, list)
        return [decode_input(item, data_type["elementType"], data_type["containsNull"])
                for item in value]
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


def error_details(error):
    details = {"condition": None, "sql_state": None, "message_parameters": None,
               "condition_source": None, "java_causes": [], "metadata_errors": []}
    methods = (("condition", "getCondition"), ("sql_state", "getSqlState"),
               ("message_parameters", "getMessageParameters"))
    for key, method in methods:
        getter = getattr(error, method, None)
        if getter is not None:
            try:
                details[key] = getter()
            except Exception as metadata_error:
                details["metadata_errors"].append({"source": "python", "method": method,
                                                   "error": str(metadata_error)})
    if details["condition"] is not None:
        details["condition_source"] = "python"
    cause = getattr(error, "java_exception", None)
    seen = set()
    while cause is not None:
        try:
            identity = cause._target_id
            if identity in seen:
                break
            seen.add(identity)
            item = {"class": str(cause.getClass().getName()),
                    "message": str(cause.toString())}
            available = {str(method.getName()) for method in cause.getClass().getMethods()}
            for key, method in methods:
                if method not in available:
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
                    details["metadata_errors"].append({"source": item["class"], "method": method,
                                                       "error": str(metadata_error)})
            details["java_causes"].append(item)
            if details["condition"] is None and item.get("condition") is not None:
                for key, _ in methods:
                    details[key] = item.get(key)
                details["condition_source"] = "java_causes[{}]".format(len(details["java_causes"]) - 1)
            cause = cause.getCause()
        except Exception as metadata_error:
            details["metadata_errors"].append({"source": "java_causes", "method": "inspect",
                                               "error": str(metadata_error)})
            break
    return details


def self_check(manifest):
    assert manifest["spark_version"] == VERSION
    assert manifest["spark_revision"] == REVISION
    assert manifest["source_commit"] == BASELINE
    assert manifest["group"] == GROUP
    assert manifest["timezone"] == TIMEZONE
    assert manifest["ansi_modes"] == ANSI_MODES
    assert manifest["capture_script_sha256"] == sha256(Path(__file__))
    scenarios, inputs = manifest["scenarios"], manifest["inputs"]
    assert len(scenarios) == manifest["scenario_count"]
    assert len(scenarios) * len(ANSI_MODES) == manifest["observation_count"]
    assert len({case["id"] for case in scenarios}) == len(scenarios)
    assert len({case["sql"] for case in scenarios}) == len(scenarios)
    assert manifest["identities"] == [{"id": case["id"], "ansi": ansi}
                                     for ansi in ANSI_MODES for case in scenarios]
    for case in scenarios:
        assert set(case) == {"id", "sql", "ordered", "family", "consumer"}
        assert case["sql"].startswith("SELECT ")
        assert case["ordered"] is (" ORDER BY k" in case["sql"])
        assert case["consumer"] in ("direct", "is_null", "case")
    assert {provider["name"] for provider in inputs} == {
        "slice_input", "slice_eval", "slice_empty", "slice_priority"}
    for provider in inputs:
        assert set(provider) == {"name", "schema", "rows"}
        assert provider["schema"]["type"] == "struct"
        keys = [int(row[0]) for row in provider["rows"]]
        assert keys == sorted(set(keys))
        for row in provider["rows"]:
            assert encode_result(decode_input(row, provider["schema"])) == row
    providers = {provider["name"]: provider for provider in inputs}
    assert providers["slice_empty"]["rows"] == []
    assert providers["slice_empty"]["schema"] == providers["slice_eval"]["schema"]
    rows = providers["slice_eval"]["rows"]
    assert rows[0][1:] == rows[10][1:]
    assert rows[1][1:] == rows[11][1:]
    assert rows[2][1:] == rows[12][1:]
    assert encode_result((None, [1, True, False, "x"])) == [None, ["1", "true", "false", "x"]]
    assert decode_input(["1", None], {"type": "array", "elementType": "integer", "containsNull": True}) == [1, None]
    for value in ("-2147483648", "2147483647"):
        assert str(decode_input(value, "integer")) == value
    for value in ("2147483648", "-2147483649", "01", None):
        try:
            decode_input(value, "integer")
        except AssertionError:
            pass
        else:
            raise AssertionError("invalid provider INT32 accepted: {!r}".format(value))
    assert error_details(ValueError("self-check")) == {
        "condition": None, "sql_state": None, "message_parameters": None,
        "condition_source": None, "java_causes": [], "metadata_errors": []}

    class SparkError(Exception):
        def getCondition(self):
            return "FULL.CONDITION"

        def getSqlState(self):
            return "22023"

        def getMessageParameters(self):
            return {"parameter": "value"}

    details = error_details(SparkError("self-check"))
    assert details["condition"] == "FULL.CONDITION"
    assert details["condition_source"] == "python"
    assert details["sql_state"] == "22023"
    assert details["message_parameters"] == {"parameter": "value"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    manifest_path = PACKAGE / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    self_check(manifest)
    source_hash, manifest_hash = sha256(Path(__file__)), sha256(manifest_path)
    if args.self_check:
        assert args.output is None, "--self-check does not write capture output"
        assert "pyspark" not in sys.modules
        print(json.dumps({"self_check": "passed", "scenarios": manifest["scenario_count"],
                          "observations": manifest["observation_count"],
                          "capture_sha256": source_hash, "manifest_sha256": manifest_hash}))
        return
    assert args.output is not None and args.output.parent.is_dir()
    assert os.environ.get("TZ") == TIMEZONE, "run with TZ=UTC"
    # Reserve the output exclusively before starting Spark; setup failures leave no SQL records.
    with args.output.open("x", encoding="utf-8") as output:
        os.environ["PYSPARK_PYTHON"] = sys.executable
        import pyspark
        from pyspark.sql import SparkSession, types

        jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
        with ZipFile(jar) as archive:
            build = archive.read("spark-version-info.properties").decode()
        assert f"revision={REVISION}\n" in build, build
        assert f"version={VERSION}\n" in build, build
        spark = (SparkSession.builder.master("local[1]").appName(GROUP)
                 .config("spark.ui.enabled", "false")
                 .config("spark.sql.shuffle.partitions", "1")
                 .config("spark.sql.session.timeZone", TIMEZONE)
                 .config("spark.sql.ansi.enabled", "true").getOrCreate())
        try:
            assert spark.version == VERSION, spark.version
            assert spark.conf.get("spark.sql.session.timeZone") == TIMEZONE
            assert spark.conf.get("spark.sql.ansi.enabled") == "true"
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
            for case in manifest["scenarios"]:
                spark._jsparkSession.sessionState().sqlParser().parsePlan(case["sql"])
            header = {"spark_version": spark.version, "spark_revision": REVISION,
                      "spark_build": build, "timezone": TIMEZONE, "group": GROUP,
                      "process_timezone": os.environ["TZ"], "source_commit": BASELINE,
                      "capture_sha256": source_hash, "manifest_sha256": manifest_hash,
                      "observations": manifest["observation_count"], "inputs": observed_inputs,
                      "input_encoding": manifest["input_encoding"],
                      "provider_input_roundtrip_checked": True}
            output.write(json.dumps(header, separators=(",", ":")) + "\n")
            output.flush()
            identities = []
            for ansi in ANSI_MODES:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                assert spark.conf.get("spark.sql.ansi.enabled") == str(ansi).lower()
                for case in manifest["scenarios"]:
                    record = dict(case, ansi=ansi, timezone=TIMEZONE)
                    stages, plans = {}, {}
                    phase = "sql"
                    try:
                        frame = spark.sql(case["sql"])
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
                            schema, plan_text = json.loads(plan.schema().json()), plan.toString()
                            stages[name], plans[name] = schema, plan_text
                        phase = "collect"
                        collected = frame.collect()
                        phase = "encode_rows"
                        rows = [encode_result(row) for row in collected]
                        record["spark_collected_rows"] = list(rows)
                        if not case["ordered"]:
                            rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                        phase = "executed_schema_after_collect"
                        plan = execution.executedPlan()
                        schema, plan_text = json.loads(plan.schema().json()), plan.toString()
                        stages["executed"], plans["executed"] = schema, plan_text
                        phase = "schema_consistency"
                        assert public == stages["analyzed"]
                        record["expected"] = {"schema": public, "rows": rows}
                    except Exception as error:
                        details = error_details(error)
                        recognized = details["condition"] in SQL_CONDITIONS and phase not in (
                            "encode_rows", "schema_consistency")
                        expected_error = {"phase": phase, **{key: details[key] for key in (
                            "condition", "sql_state", "message_parameters")}}
                        record.update(expected={"condition": details["condition"]},
                                      expected_error=expected_error,
                                      query_failure_kind="sql_error" if recognized else "unknown_failure",
                                      spark_error=str(error), spark_error_phase=phase,
                                      spark_error_class=type(error).__name__,
                                      spark_error_details=details,
                                      spark_error_traceback=traceback.format_exc())
                    record.update(schema_stages=stages, spark_plans=plans)
                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
                    output.flush()
                    identities.append({"id": case["id"], "ansi": ansi})
            assert identities == manifest["identities"]
            assert sha256(Path(__file__)) == source_hash
            assert sha256(manifest_path) == manifest_hash
        finally:
            spark.stop()
    print(json.dumps({"observations": len(identities), "capture_sha256": source_hash,
                      "manifest_sha256": manifest_hash, "raw_sha256": sha256(args.output)}))


if __name__ == "__main__":
    main()
