"""Capture only nullable-column IF/CASE CAST visibility for review 710."""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-review-710-cast-visibility")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        records = [{"spark_version": spark.version, "timezone": "UTC",
                    "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "inputs": []}]
        for tagged in [False, True]:
            table = "annotation_visibility_tagged" if tagged else "annotation_visibility_plain"
            schema = types.StructType([types.StructField(
                "k", types.IntegerType(), True, {"source": "provider"} if tagged else {})])
            values = [(1,), (None,)]
            spark.createDataFrame(values, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                         "rows": [["1"], [None]]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for tagged in [False, True]:
                table = "annotation_visibility_tagged" if tagged else "annotation_visibility_plain"
                for label, expression in [
                    ("if", "IF(k IS NOT NULL, CAST(k AS BIGINT), 7L)"),
                    ("case", "CASE WHEN k IS NOT NULL THEN CAST(k AS BIGINT) ELSE 7L END"),
                ]:
                    sql = f"SELECT {expression} AS r FROM {table}"
                    record = {"id": label, "ansi": ansi, "tagged": tagged, "sql": sql}
                    try:
                        frame = spark.sql(sql)
                        record["public_schema"] = frame.schema.jsonValue()
                        execution = frame._jdf.queryExecution()
                        record["analyzed_schema"] = json.loads(execution.analyzed().schema().json())
                        record["optimized_schema"] = json.loads(execution.optimizedPlan().schema().json())
                        record["analyzed_plan"] = execution.analyzed().toString()
                        record["optimized_plan"] = execution.optimizedPlan().toString()
                        rows = [[None if value is None else str(value) for value in row]
                                for row in frame.collect()]
                        record["expected"] = {"schema": frame.schema.jsonValue(), "rows": rows}
                    except Exception as error:
                        record["error"] = {"message": str(error),
                                           "condition": getattr(error, "getCondition", lambda: None)()}
                    records.append(record)
        assert len(records) == 9
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records)-1, "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
