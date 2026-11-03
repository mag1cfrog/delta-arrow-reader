"""Capture required sources and the unchanged scalar guards at every Spark stage."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent
MANIFEST = ROOT / "supplement-inputs-and-cases.json"


def validate_manifest(manifest):
    assert manifest["observations"] == len(manifest["cases"]) == 68
    assert Counter(case["group"] for case in manifest["cases"]) == manifest["counts"]
    assert len({(case["group"], case["id"], case.get("input"), case["ansi"])
                for case in manifest["cases"]}) == 68
    for source in manifest["source_files"]:
        data = (ROOT / source["path"]).read_bytes()
        assert len(data) == source["bytes"], source["path"]
        assert hashlib.sha256(data).hexdigest() == source["sha256"], source["path"]
    required = [case for case in manifest["cases"] if case["group"] == "required_source"]
    assert len(required) == 18
    assert {case["input"] for case in required} == {
        "struct_mixed", "struct_nulls", "struct_empty"}
    assert {case["ansi"] for case in required} == {True, False}
    for provider in manifest["inputs"]:
        if provider["name"].startswith("struct_"):
            field, = (field for field in provider["schema"]["fields"]
                      if field["name"] == "id")
            assert field == {"name": "id", "type": "integer", "nullable": False,
                             "metadata": {"source": "id"}}


def decode(value, data_type, types):
    if value is None:
        return None
    if isinstance(data_type, types.StructType):
        assert len(value) == len(data_type)
        return tuple(decode(item, field.dataType, types)
                     for item, field in zip(value, data_type))
    if isinstance(data_type, types.ArrayType):
        return [decode(item, data_type.elementType, types) for item in value]
    if isinstance(data_type, types.IntegerType):
        return int(value)
    if isinstance(data_type, types.StringType):
        return value
    raise AssertionError((value, data_type))


def capture_case(spark, case, calls, encode):
    record = dict(case)
    phase = "resolve"
    try:
        calls.value = 0
        frame = spark.sql(case["sql"])
        record["public_schema"] = frame.schema.jsonValue()
        execution = frame._jdf.queryExecution()
        record["analyzed_schema"] = json.loads(execution.analyzed().schema().json())
        record["analyzed_plan"] = execution.analyzed().toString()
        phase = "optimize"
        record["optimized_schema"] = json.loads(execution.optimizedPlan().schema().json())
        record["optimized_plan"] = execution.optimizedPlan().toString()
        phase = "physical"
        record["executed_schema_before_collect"] = json.loads(
            execution.executedPlan().schema().json())
        record["calls_after_planning"] = calls.value
        phase = "execute"
        rows = [encode(row, frame.schema) for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        record["executed_schema_after_collect"] = json.loads(
            execution.executedPlan().schema().json())
        record["physical_plan"] = execution.executedPlan().toString()
        record["evaluated_rows"] = calls.value
        record["expected"] = {"status": "ok", "schema": record["public_schema"],
                              "rows": rows}
        if "original_observation" in case:
            original = case["original_observation"]
            record["original_public_matches"] = (
                record["public_schema"] == original["expected"]["schema"])
            record["original_rows_match"] = rows == original["expected"]["rows"]
            if "evaluated_rows" in original:
                record["original_evaluations_match"] = (
                    calls.value == original["evaluated_rows"])
    except Exception as error:
        from py4j.protocol import Py4JError

        condition = getattr(error, "getCondition", lambda: None)()
        cause = getattr(error, "java_exception", None)
        while condition is None and cause is not None:
            try:
                condition = cause.getCondition()
            except Py4JError:
                pass
            cause = cause.getCause()
        record["error"] = {"condition": condition, "message": str(error), "phase": phase}
        record.pop("expected", None)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh JSONL output path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    manifest = json.loads(MANIFEST.read_text())
    validate_manifest(manifest)

    import pyspark
    from pyspark.sql import SparkSession, functions, types

    sys.path.insert(0, str(ROOT / "sources"))
    from capture_floating_group_keys_spark import encode

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={manifest['spark_revision']}" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-818-correlated-metadata-stage-supplement")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"]
        assert spark.conf.get("spark.sql.session.timeZone") == manifest["timezone"]
        spark.sparkContext.setLogLevel("ERROR")
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            rows = [decode(row, schema, types) for row in provider["rows"]]
            assert [encode(row, schema, input_value=True) for row in rows] == provider["rows"]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(provider["name"])
        calls = spark.sparkContext.accumulator(0)

        def count_row(value):
            calls.add(1)
            return value

        spark.udf.register("count_row", functions.udf(
            count_row, types.IntegerType()).asNondeterministic())
        records = [{key: value for key, value in manifest.items() if key != "cases"}]
        records[0].update({"spark_build": build,
                           "capture_script_sha256": hashlib.sha256(
                               Path(__file__).read_bytes()).hexdigest(),
                           "capture_manifest_sha256": hashlib.sha256(
                               MANIFEST.read_bytes()).hexdigest()})
        for case in manifest["cases"]:
            spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
            records.append(capture_case(spark, case, calls, encode))
        assert len(records) - 1 == 68
        assert all(("expected" in record) != ("error" in record) for record in records[1:])
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": 68, "output": str(args.output),
                          "errors": sum("error" in record for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
