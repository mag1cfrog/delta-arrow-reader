"""Capture the bounded #823 preparation manifest on independent pinned Spark.

Run only by the root. This script writes one fresh output and edits no product files.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cases", type=Path,
                        default=Path(__file__).with_name("823-preparation-v2-cases.json"))
    args = parser.parse_args()
    assert args.output.parent.is_dir(), args.output.parent
    assert not args.output.exists(), args.output
    manifest = json.loads(args.cases.read_text())
    scenarios = manifest["scenarios"]
    assert len(scenarios) == manifest["scenario_count"] == 44
    assert len({case["id"] for case in scenarios}) == 44
    assert manifest["ansi_modes"] == [True, False]
    assert all("expected" not in case and "schema_stages" not in case for case in scenarios)
    original = manifest["original_controls"]
    assert len(original) == 4
    assert {(case["ansi"], case["batch_size"]) for case in original} == {
        (True, 1), (True, 32), (False, 1), (False, 32)}
    for dependency in manifest["source_dependencies"]:
        assert digest(Path(dependency["path"])) == dependency["sha256"], dependency["path"]

    os.environ["PYSPARK_PYTHON"] = sys.executable
    import pyspark
    from pyspark.errors import PySparkException
    from pyspark.sql import SparkSession, types

    helper = Path(manifest["leaf_encoder_path"])
    sys.path.insert(0, str(helper.parent))
    from capture_floating_group_keys_spark import encode as encode_leaf

    # Existing Map-owner recursive encoding, with leaf policy from the shared helper.
    def encode(value, data_type, *, input_value=False):
        if value is None:
            return None
        if isinstance(data_type, types.MapType):
            return [[encode(key, data_type.keyType, input_value=input_value),
                     encode(item, data_type.valueType, input_value=input_value)]
                    for key, item in value.items()]
        if isinstance(data_type, types.StructType):
            return [encode(item, field.dataType, input_value=input_value)
                    for item, field in zip(value, data_type)]
        if isinstance(data_type, types.ArrayType):
            return [encode(item, data_type.elementType, input_value=input_value) for item in value]
        return encode_leaf(value, data_type, input_value=input_value)

    def decode(value, data_type):
        if value is None:
            return None
        if isinstance(data_type, types.StructType):
            assert len(value) == len(data_type)
            return tuple(decode(item, field.dataType) for item, field in zip(value, data_type))
        if isinstance(data_type, types.ArrayType):
            return [decode(item, data_type.elementType) for item in value]
        if isinstance(data_type, types.MapType):
            decoded = {decode(key, data_type.keyType): decode(item, data_type.valueType)
                       for key, item in value}
            assert len(decoded) == len(value), "duplicate provider Map keys"
            return decoded
        if isinstance(data_type, types.IntegerType):
            assert isinstance(value, str)
            result = int(value)
            assert str(result) == value and -(2 ** 31) <= result < 2 ** 31
            return result
        assert isinstance(data_type, types.StringType) and isinstance(value, str), data_type
        return value

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={manifest['spark_revision']}\n" in build, build
    assert f"version={manifest['spark_version']}\n" in build, build
    spark = (SparkSession.builder.master("local[1]").appName("dar-823-preparation-v2")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", manifest["timezone"])
             .config("spark.sql.caseSensitive", str(manifest["case_sensitive"]).lower())
             .getOrCreate())
    try:
        assert spark.version == manifest["spark_version"], spark.version
        spark.sparkContext.setLogLevel("ERROR")
        actual_inputs = []
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            values = [decode(row, schema) for row in provider["rows"]]
            assert [encode(row, schema, input_value=True) for row in values] == provider["rows"]
            frame = spark.createDataFrame(values, schema)
            assert frame.schema.jsonValue() == provider["schema"], provider["name"]
            frame.createOrReplaceTempView(provider["name"])
            actual_rows = [encode(row, schema, input_value=True)
                           for row in frame.orderBy("id").collect()]
            # Dict equality checks Map contents while preserving raw pair order separately.
            assert [decode(row, schema) for row in actual_rows] == values, provider["name"]
            actual_inputs.append(dict(provider, observed_rows_raw=actual_rows,
                                      raw_map_pair_order_matches=actual_rows == provider["rows"]))
        # Syntax/setup errors never enter SQL failure observations.
        for case in scenarios:
            spark._jsparkSession.sessionState().sqlParser().parsePlan(case["sql"])
        config_keys = ("spark.sql.ansi.enabled", "spark.sql.session.timeZone",
                       "spark.sql.caseSensitive", "spark.sql.shuffle.partitions")
        header = {key: value for key, value in manifest.items()
                  if key not in ("scenarios", "inputs", "original_controls")}
        header.update(kind="independent_spark_preparation_not_native_acceptance", inputs=actual_inputs,
                      capture_sha256=digest(Path(__file__)), cases_sha256=digest(args.cases),
                      spark_build=build,
                      spark_build_sha256=hashlib.sha256(build.encode()).hexdigest(),
                      original_control_count=len(original))
        records = [header]
        for ansi in manifest["ansi_modes"]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for case in scenarios:
                config = {key: spark.conf.get(key) for key in config_keys}
                assert config["spark.sql.ansi.enabled"] == str(ansi).lower()
                assert config["spark.sql.session.timeZone"] == manifest["timezone"]
                assert config["spark.sql.caseSensitive"] == str(manifest["case_sensitive"]).lower()
                record = dict(case, ansi=ansi, timezone=manifest["timezone"],
                              case_sensitive=manifest["case_sensitive"], actual_config=config)
                stages, plans = {}, {}
                phase, execution, collected = "sql", None, None
                query_succeeded = False
                try:
                    frame = spark.sql(case["sql"])
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
                    collected = frame.collect()
                    phase = "executed_schema_after_collect"
                    plan = execution.executedPlan()
                    stages["executed"] = json.loads(plan.schema().json())
                    plans["executed"] = plan.toString()
                    query_succeeded = True
                except Exception as error:
                    cause = getattr(error, "java_exception", None)
                    causes = []
                    for _ in range(32):
                        if cause is None:
                            break
                        causes.append({"class": str(cause.getClass().getName()),
                                       "message": str(cause.getMessage())})
                        cause = cause.getCause()
                    condition = error.getCondition() if isinstance(error, PySparkException) else None
                    java_sql_state, java_parameters = None, None
                    if condition is None:
                        cause = getattr(error, "java_exception", None)
                        for _ in range(32):
                            if cause is None:
                                break
                            try:
                                condition = cause.getCondition()
                            except Exception:
                                pass
                            if condition is not None:
                                # Retain typed SQL details even for a Py4J-wrapped Java cause.
                                try:
                                    java_sql_state = str(cause.getSqlState())
                                    parameters = cause.getMessageParameters()
                                    java_parameters = {str(entry.getKey()): str(entry.getValue())
                                                       for entry in parameters.entrySet()}
                                except Exception as details_error:
                                    raise AssertionError((case["id"], "SQL cause details", str(details_error))) from details_error
                                break
                            cause = cause.getCause()
                    assert condition is not None, (case["id"], phase, type(error).__name__, str(error))
                    record.update(expected={"condition": str(condition)}, spark_error_phase=phase,
                                  spark_error=str(error), spark_error_class=type(error).__name__,
                                  spark_error_details={
                                      "condition": str(condition), "sql_state": (
                                          error.getSqlState() if isinstance(error, PySparkException) else java_sql_state),
                                      "message_parameters": (
                                          dict(error.getMessageParameters() or {}) if isinstance(error, PySparkException) else java_parameters),
                                      "cause_chain": causes}, phase="error")
                    if execution is not None and phase == "collect":
                        stages["executed"] = json.loads(execution.executedPlan().schema().json())
                if query_succeeded:
                    # Encoding/assertion failures are infrastructure errors, outside SQL catch.
                    rows = [encode(row, frame.schema) for row in collected]
                    assert public == stages["analyzed"]
                    record.update(expected={"schema": public, "rows": rows}, rows_raw=rows,
                                  phase="success")
                record.update(schema_stages=stages, spark_plans=plans)
                if case["id"] == "original_unaliased":
                    matching = [old for old in original if old["ansi"] == ansi]
                    assert len(matching) == 2
                    for old in matching:
                        reference = old["reference"]
                        assert record["sql"] == reference["sql"]
                        assert record["expected"] == reference["expected"], (ansi, record)
                        assert all(stages[name] == field for name, field
                                   in reference["schema_stages"].items()), (ansi, record)
                    record["original_control_batches_verified"] = [1, 32]
                records.append(record)
        assert len(records) - 1 == manifest["spark_observation_count"] == 88
        assert len({(r["id"], r["ansi"], r["timezone"], r["case_sensitive"])
                    for r in records[1:]}) == 88
        assert sum(r.get("original_control_batches_verified") == [1, 32]
                   for r in records[1:]) == 2
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":"), ensure_ascii=True) + "\n")
        print(json.dumps({"observations": 88, "output": str(args.output),
                          "sha256": digest(args.output), "capture_sha256": digest(Path(__file__)),
                          "cases_sha256": digest(args.cases),
                          "errors": sum(r["phase"] == "error" for r in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
