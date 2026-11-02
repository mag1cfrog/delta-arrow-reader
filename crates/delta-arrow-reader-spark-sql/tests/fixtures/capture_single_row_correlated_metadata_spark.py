"""Capture scalar-result metadata and volatile siblings with Spark 4.2.0."""

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions, types

from capture_single_row_correlated_spark import capture, encode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh output JSONL path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    schema = types.StructType([
        types.StructField("id", types.IntegerType(), True,
                          {"origin": "outer", "comment": "source id"}),
    ])
    inputs = [("metadata_inputs", [(0,), (1,), (None,)]),
              ("metadata_empty", []), ("metadata_null", [(None,), (None,)])]
    spark = (SparkSession.builder.master("local[1]").appName("dar-single-row-metadata")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for name, rows in inputs:
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": [
            {"name": name, "schema": schema.jsonValue(), "rows": [
                [encode(value, field.dataType) for value, field in zip(row, schema)]
                for row in rows]} for name, rows in inputs]}]
        calls = spark.sparkContext.accumulator(0)

        def count_row(value):
            calls.add(1)
            return value

        spark.udf.register("count_row", functions.udf(
            count_row, types.IntegerType()).asNondeterministic())
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for table, _ in inputs:
                for sibling in [False, True]:
                    for cast in [False, True]:
                        suffix = ",count_row(p.id) AS observed" if sibling else ""
                        suffix += ",CAST(7 AS INT) AS incidental" if cast else ""
                        sql = ("SELECT p.id AS direct_id,(SELECT p.id) AS scalar_id"
                               f"{suffix} FROM {table} p")
                        calls.value = 0
                        expected = capture(spark, sql)
                        records.append({"id": f"{table}_{'sibling' if sibling else 'plain'}_"
                                              f"{'cast' if cast else 'bare'}",
                                        "ansi": ansi, "sql": sql, "expected": expected,
                                        "evaluated_rows": calls.value})
        assert len(records) == 25
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == 24
        with args.output.open("x") as output:
            output.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                 for record in records))
        print(json.dumps({"observations": len(records)-1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
