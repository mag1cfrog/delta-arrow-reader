"""Capture Decimal-to-integer CAST and TRY_CAST overflow boundaries from pinned Spark."""

import argparse
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import sys
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession, types


def encode(value, data_type, *, input_value=False):
    if value is None:
        return None
    if isinstance(data_type, (types.FloatType, types.DoubleType)):
        if math.isnan(value) and not input_value:
            return "nan"
        return struct.pack("!f" if isinstance(data_type, types.FloatType) else "!d", value).hex()
    if isinstance(data_type, types.StructType):
        return [encode(item, field.dataType, input_value=input_value)
                for item, field in zip(value, data_type)]
    if isinstance(data_type, types.DecimalType):
        return format(value, f".{data_type.scale}f")
    return str(value).lower() if isinstance(value, bool) else str(value)


def decode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.StructType):
        return tuple(decode(item, field.dataType) for item, field in zip(value, data_type))
    if isinstance(data_type, (types.FloatType, types.DoubleType)):
        return struct.unpack("!f" if isinstance(data_type, types.FloatType) else "!d",
                             bytes.fromhex(value))[0]
    if isinstance(data_type, types.DecimalType):
        return Decimal(value)
    if isinstance(data_type, types.StringType):
        return value
    return int(value)


def capture_case(spark, case):
    phase = "sql"
    stages = {}
    record = dict(case, schema_stages=stages)
    try:
        frame = spark.sql(case["sql"])
        execution = frame._jdf.queryExecution()
        stages["analyzed"] = json.loads(execution.analyzed().schema().json())
        assert frame.schema.jsonValue() == stages["analyzed"], case
        phase = "optimized"
        stages["optimized"] = json.loads(execution.optimizedPlan().schema().json())
        phase = "executed_before_collect"
        stages["executed_before_collect"] = json.loads(execution.executedPlan().schema().json())
        phase = "collect"
        rows = [encode(row, frame.schema) for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        stages["executed"] = json.loads(execution.executedPlan().schema().json())
        record["expected"] = {"schema": stages["analyzed"], "rows": rows}
        record["analyzed_plan"] = execution.analyzed().toString()
        record["optimized_plan"] = execution.optimizedPlan().toString()
        record["executed_plan"] = execution.executedPlan().toString()
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        assert condition is not None, (case, phase, str(error))
        record.update(expected={"condition": condition}, spark_error_phase=phase,
                      spark_error=str(error))
        if phase == "collect":
            stages["executed"] = json.loads(execution.executedPlan().schema().json())
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    manifest_path = Path(__file__).with_name("manifest.json")
    manifest = json.loads(manifest_path.read_text())
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + manifest["spark_revision"] in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-661-decimal-integer-overflow")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", manifest["timezone"])
             .getOrCreate())
    try:
        assert spark.version == manifest["spark_version"], spark.version
        spark.sparkContext.setLogLevel("ERROR")
        header = {key: value for key, value in manifest.items() if key != "cases"}
        header.update(spark_build=build,
                      manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                      capture_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        records = [header]
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            values = [decode(row, schema) for row in provider["rows"]]
            assert [encode(row, schema, input_value=True) for row in values] == provider["rows"]
            spark.createDataFrame(values, schema).createOrReplaceTempView(provider["name"])
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            records.extend(capture_case(spark, dict(case, ansi=ansi))
                           for case in manifest["cases"])
        assert len(records) - 1 == manifest["observations"]
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == manifest["observations"]
        with args.output.open("x") as output:
            output.writelines(json.dumps(record, separators=(",", ":")) + "\n"
                              for record in records)
        print(json.dumps({"observations": len(records) - 1,
                          "errors": sum("condition" in r["expected"] for r in records[1:]),
                          "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
