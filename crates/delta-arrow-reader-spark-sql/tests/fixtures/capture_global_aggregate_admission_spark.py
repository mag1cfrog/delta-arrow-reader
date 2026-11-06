"""Capture independent aggregate admission, window controls, and complete schema stages."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


HERE = Path(__file__).resolve().parent


def inventory():
    manifest = json.loads((HERE / "global-aggregate-admission-cases.json").read_text())
    cases = manifest["cases"]
    assert manifest["spark_revision"] == "32f7299601108917fb01920a54e084595b7b3bf8"
    assert manifest["input_version"] == 2
    assert manifest["ansi_modes"] == [True, False]
    assert len(cases) == 249
    assert len({case["id"] for case in cases}) == len(cases)
    assert all("expected" not in case for case in cases)
    assert sum("udf_sql" in case for case in cases) == 141
    assert {scope: sum(case["scope"] == scope for case in cases) for scope in [
        "required_argument_rejection", "window_contract", "allowed_control",
        "preserve_category", "category_precedence"]} == {
            "required_argument_rejection": 141, "window_contract": 68,
            "allowed_control": 33, "preserve_category": 4, "category_precedence": 3}
    assert {"window_inner_sum", "window_inner_count_array", "sum_invalid_volatile_array",
            "sum_invalid_array", "nested_deterministic_sum", "window_count_if_volatile",
            "window_count_if_volatile_false_filter"}.issubset({case["id"] for case in cases})
    assert all(case["success_assertions"] in ["admission_fields_and_cardinality",
                                              "exact_schema_and_rows"] for case in cases)
    assert len(manifest["retained_cases"]) == 1
    return manifest


def encode(value, data_type):
    from pyspark.sql import types
    from capture_floating_group_keys_spark import encode as encode_numeric

    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(data_type, types.ArrayType):
        return [encode(item, data_type.elementType) for item in value]
    if isinstance(data_type, types.StructType):
        return [encode(item, field.dataType) for item, field in zip(value, data_type)]
    return encode_numeric(value, data_type)


def capture(spark, sql):
    from capture_null_base_extraction_spark import error_details

    schemas = {}
    stage = "spark.sql"
    try:
        frame = spark.sql(sql)
        stage = "public_schema"
        public_schema = frame.schema
        schemas["public"] = public_schema.jsonValue()
        execution = frame._jdf.queryExecution()
        for stage, plan in [("analyzed", execution.analyzed),
                            ("optimized", execution.optimizedPlan),
                            ("executed", execution.executedPlan)]:
            schemas[stage] = json.loads(plan().schema().json())
        stage = "collect"
        materialized = frame.collect()
        stage = "executed_schema_after_collect"
        schemas["executed_after_collect"] = json.loads(execution.executedPlan().schema().json())
        stage = "encode"
        raw_rows = [encode(row, public_schema) for row in materialized]
        rows = sorted(raw_rows, key=lambda row: json.dumps(row, separators=(",", ":")))
        return {"expected": {"schema": schemas["public"], "rows": rows},
                "phase": "success", "schema_stages": schemas, "raw_rows": raw_rows}
    except Exception as error:
        try:
            condition, details = error_details(error)
        except AssertionError:
            return {"capture_failure": {"stage": stage, "python_type": type(error).__name__,
                                        "message": str(error)}, "schema_stages": schemas}
        return {"expected": {"condition": condition}, "failure_stage": stage,
                "phase": "execution" if stage == "collect" else "planning",
                "schema_stages": schemas, "error": details}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    manifest = inventory()
    if args.self_check:
        print(json.dumps({"cases_per_ansi": len(manifest["cases"]),
                          "observations": len(manifest["cases"]) * 2}))
        return
    assert args.output is not None, "provide a new output JSONL path"
    assert not args.output.exists(), args.output
    retained_output = args.output.with_suffix(".retained.jsonl")
    assert not retained_output.exists(), retained_output

    import pyspark
    from pyspark.sql import SparkSession, functions, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + manifest["spark_revision"] in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-aggregate-admission-733")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"], spark.version
        spark.sparkContext.setLogLevel("ERROR")
        spark.udf.register("observed_arg", functions.udf(
            lambda _: 0.5, types.DoubleType()).asNondeterministic())
        spark.udf.register("observed_null", functions.udf(
            lambda _: None, types.NullType()).asNondeterministic())
        header = {key: manifest[key] for key in [
            "spark_version", "spark_revision", "timezone", "input_version"]}
        header.update({"spark_build": build, "manifest_sha256": hashlib.sha256(
            (HERE / "global-aggregate-admission-cases.json").read_bytes()).hexdigest(),
            "settings": {key: spark.conf.get(key) for key in [
                "spark.sql.session.timeZone", "spark.sql.shuffle.partitions",
                "spark.sql.optimizer.excludedRules", "spark.sql.caseSensitive"]}})
        failures = []
        with args.output.open("x") as output, retained_output.open("x") as retained:
            output.write(json.dumps(header, separators=(",", ":")) + "\n")
            retained.write(json.dumps(header, separators=(",", ":")) + "\n")
            for ansi in manifest["ansi_modes"]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for case in manifest["cases"]:
                    record = {**case, "ansi": ansi, **capture(spark, case["sql"])}
                    if "udf_sql" in case:
                        record["udf_reference"] = capture(spark, case["udf_sql"])
                    if "capture_failure" in record or "capture_failure" in record.get("udf_reference", {}):
                        failures.append((case["id"], ansi))
                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
                    output.flush()
                for case in manifest["retained_cases"]:
                    record = {**case, "ansi": ansi, **capture(spark, case["sql"])}
                    if "capture_failure" in record:
                        failures.append((case["id"], ansi))
                    retained.write(json.dumps(record, separators=(",", ":")) + "\n")
                    retained.flush()
        assert not failures, ("capture failures retained in output", failures)
        print(json.dumps({"observations": len(manifest["cases"]) * 2,
                          "udf_observations": 282, "retained_observations": 2,
                          "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
