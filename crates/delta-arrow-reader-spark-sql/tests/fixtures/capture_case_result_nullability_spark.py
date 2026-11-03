"""Capture independent CASE declarations, execution schemas, rows and required errors."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh JSONL output path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    manifest_path = ROOT / "case-result-nullability-cases.json"
    manifest = json.loads(manifest_path.read_text())
    definitions = manifest["cases"]
    assert len(definitions) == manifest["case_count"] == 162
    assert len({(case["id"], case["input"]) for case in definitions}) == len(definitions)

    import pyspark
    from pyspark.sql import SparkSession, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={manifest['spark_revision']}" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-case-result-nullability-721")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"], spark.version
        assert spark.conf.get("spark.sql.session.timeZone") == manifest["timezone"]
        spark.sparkContext.setLogLevel("ERROR")
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            rows = [tuple(None if value is None else int(value) for value in row)
                    for row in provider["rows"]]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(provider["name"])

        def encode(value, data_type):
            if value is None:
                return None
            if isinstance(data_type, types.StructType):
                return [encode(item, field.dataType) for item, field in zip(value, data_type)]
            if isinstance(value, bool):
                return str(value).lower()
            return str(value)

        records = [{key: value for key, value in manifest.items() if key != "cases"}]
        records[0].update({"spark_build": build, "capture_sha256": hashlib.sha256(
            Path(__file__).read_bytes()).hexdigest(), "manifest_sha256": hashlib.sha256(
                manifest_path.read_bytes()).hexdigest()})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for definition in definitions:
                record = dict(definition, ansi=ansi)
                stages = {}
                try:
                    frame = spark.sql(record["sql"])
                    execution = frame._jdf.queryExecution()
                    public_schema = frame.schema.jsonValue()
                    stages["analyzed"] = json.loads(execution.analyzed().schema().json())
                    stages["optimized"] = json.loads(execution.optimizedPlan().schema().json())
                    stages["executed_before_collect"] = json.loads(
                        execution.executedPlan().schema().json())
                    rows = [[encode(value, field.dataType) for value, field in zip(row, frame.schema)]
                            for row in frame.collect()]
                    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                    stages["executed"] = json.loads(execution.executedPlan().schema().json())
                    assert public_schema == stages["analyzed"]
                    record["expected"] = {"schema": public_schema, "rows": rows}
                except Exception as error:
                    message = str(error)
                    condition = error.getCondition() if hasattr(error, "getCondition") else None
                    if condition is None:
                        found = re.search(r"\[(CAST_INVALID_INPUT|USER_RAISED_EXCEPTION|INVALID_INLINE_TABLE\.FAILED_SQL_EXPRESSION_EVALUATION)\]", message)
                        condition = found.group(1) if found else None
                    assert condition in {
                        "CAST_INVALID_INPUT", "USER_RAISED_EXCEPTION",
                        "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
                        "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION",
                    }, (record, condition, message)
                    record["expected"] = {"condition": condition}
                    record["spark_error"] = message
                record["schema_stages"] = stages
                records.append(record)
        assert len(records) - 1 == manifest["observations"] == 324
        assert len({(case["id"], case["input"], case["ansi"])
                    for case in records[1:]}) == manifest["observations"]
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1,
                          "errors": sum("condition" in case["expected"] for case in records[1:]),
                          "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
