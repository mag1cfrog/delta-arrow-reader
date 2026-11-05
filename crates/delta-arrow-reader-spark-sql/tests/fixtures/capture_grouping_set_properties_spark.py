"""Capture issue 970 grouping-set Fields, rows and errors at each Spark boundary."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
from zipfile import ZipFile

from capture_floating_group_keys_spark import encode


ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--manifest", type=Path, default=ROOT / "grouping-set-properties-cases.json")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    manifest = json.loads(args.manifest.read_text())
    definitions = manifest["cases"]
    assert len(definitions) == manifest["case_count"]
    assert len({case["id"] for case in definitions}) == len(definitions)

    import pyspark
    from pyspark.sql import SparkSession, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={manifest['spark_revision']}" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-grouping-set-properties-970")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"], spark.version
        spark.sparkContext.setLogLevel("ERROR")

        def decode(value, data_type):
            if value is None:
                return None
            if isinstance(data_type, types.StructType):
                return tuple(decode(item, field.dataType) for item, field in zip(value, data_type))
            if isinstance(data_type, types.ArrayType):
                return [decode(item, data_type.elementType) for item in value]
            if isinstance(data_type, (types.FloatType, types.DoubleType)):
                return struct.unpack("!f" if isinstance(data_type, types.FloatType) else "!d",
                                     bytes.fromhex(value))[0]
            if isinstance(data_type, types.IntegerType):
                return int(value)
            raise AssertionError(data_type)

        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            rows = [decode(row, schema) for row in provider["rows"]]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(provider["name"])

        metadata = {key: value for key, value in manifest.items() if key != "cases"}
        metadata.update({"spark_build": build,
                         "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                         "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
                         "encoder_sha256": hashlib.sha256(
                             (ROOT / "capture_floating_group_keys_spark.py").read_bytes()).hexdigest(),
                         "settings": {key: spark.conf.get(key) for key in [
                             "spark.sql.shuffle.partitions", "spark.sql.session.timeZone",
                             "spark.sql.adaptive.enabled"]}})
        count = 0
        missing_conditions = []
        with args.output.open("x") as output:
            output.write(json.dumps(metadata, separators=(",", ":")) + "\n")
            output.flush()
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for definition in definitions:
                    record = dict(definition, ansi=ansi)
                    stages = {}
                    phase = "sql"
                    try:
                        frame = spark.sql(record["sql"])
                        phase = "public_schema"
                        record["public_schema"] = frame.schema.jsonValue()
                        execution = frame._jdf.queryExecution()
                        phase = "analyzed_schema"
                        stages["analyzed"] = json.loads(execution.analyzed().schema().json())
                        record["analyzed_plan"] = execution.analyzed().toString()
                        phase = "optimized_schema"
                        stages["optimized"] = json.loads(execution.optimizedPlan().schema().json())
                        record["optimized_plan"] = execution.optimizedPlan().toString()
                        phase = "executed_schema"
                        stages["executed_before_collect"] = json.loads(
                            execution.executedPlan().schema().json())
                        record["physical_plan_before_collect"] = execution.executedPlan().toString()
                        phase = "collect"
                        collected = frame.collect()
                        phase = "encode"
                        record["rows_raw"] = [encode(row, frame.schema, input_value=True)
                                              for row in collected]
                        rows = [encode(row, frame.schema) for row in collected]
                        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                        phase = "executed_schema_after_collect"
                        stages["executed"] = json.loads(execution.executedPlan().schema().json())
                        record["physical_plan_after_collect"] = execution.executedPlan().toString()
                        record["expected"] = {"schema": record["public_schema"], "rows": rows}
                    except Exception as error:
                        if phase == "encode":
                            raise
                        condition = getattr(error, "getCondition", lambda: None)()
                        record["expected"] = {"condition": condition}
                        record["spark_error"] = str(error)
                        record["spark_error_phase"] = phase
                        record["spark_error_class"] = type(error).__name__
                        record["spark_error_details"] = {
                            "message_parameters": getattr(error, "getMessageParameters", lambda: {})()}
                        if condition is None:
                            missing_conditions.append((definition["id"], ansi))
                    record["schema_stages"] = stages
                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
                    output.flush()
                    count += 1
        assert count == manifest["observations"], count
        assert not missing_conditions, missing_conditions
        print(json.dumps({"observations": count,
                          "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
