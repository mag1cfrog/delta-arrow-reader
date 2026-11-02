"""Capture Spark 4.2.0 results for windows whose offsets simplify alike."""
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

from capture_window_admission_spark import capture_query


def queries(table):
    explicit = (
        "LAG(v,1) OVER (ORDER BY id ROWS BETWEEN -1 FOLLOWING AND -1 FOLLOWING)",
        "LEAD(v,2) OVER (ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 2 FOLLOWING)",
        "LEAD(v,1+1) OVER (ORDER BY id ROWS BETWEEN (1+1) FOLLOWING AND (1+1) FOLLOWING)",
    )
    yield "explicit", "SELECT id," + ",".join(
        f"{expression} AS {alias}"
        for expression, alias in zip(explicit, ["previous", "literal", "folded"])
    ) + f" FROM {table}"
    for function in ["LEAD", "LAG"]:
        expressions = [f"{function}(v,{offset},-99) OVER (ORDER BY id) AS {alias}"
                       for offset, alias in [("2", "literal"), ("1+1", "folded"),
                                             ("CAST(2 AS INT)", "cast_offset")]]
        yield function.lower() + "_default", f"SELECT id,{','.join(expressions)} FROM {table}"
    yield "explicit_cast", (
        "SELECT id,LEAD(v,2) OVER (ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 2 FOLLOWING) AS literal,"
        "LEAD(v,CAST(2 AS INT)) OVER (ORDER BY id ROWS BETWEEN CAST(2 AS INT) FOLLOWING AND CAST(2 AS INT) FOLLOWING) AS cast_offset "
        f"FROM {table}"
    )
    yield "duplicates", (
        "SELECT id,LEAD(v,2) OVER w AS first_copy,LEAD(v,2) OVER w AS second_copy,"
        f"LEAD(v,1+1) OVER w AS folded FROM {table} WINDOW w AS (ORDER BY id)"
    )
    yield "different_frames", (
        "SELECT id,SUM(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS literal,"
        "SUM(v) OVER (ORDER BY id ROWS BETWEEN (1+1) PRECEDING AND CURRENT ROW) AS folded,"
        f"SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS different FROM {table}"
    )
    yield "outer_consumer", (
        "SELECT id,literal,folded,literal+folded AS combined FROM ("
        "SELECT id,LEAD(v,2) OVER w AS literal,LEAD(v,1+1) OVER w AS folded "
        f"FROM {table} WINDOW w AS (ORDER BY id)) q WHERE id>1 ORDER BY folded,id"
    )
    yield "grouped", (
        "SELECT g,SUM(v) AS total,LEAD(SUM(v),2) OVER (ORDER BY g) AS literal,"
        f"LEAD(SUM(v),1+1) OVER (ORDER BY g) AS folded FROM {table} GROUP BY g"
    )
    yield "offset_mismatch", (
        "SELECT LEAD(v,2) OVER (ORDER BY id) AS literal,"
        "LEAD(v,1+1) OVER (ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 2 FOLLOWING) AS invalid "
        f"FROM {table}"
    )
    yield "frame_mismatch", (
        "SELECT LEAD(v,2) OVER (ORDER BY id) AS literal,"
        "LEAD(v,2) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS invalid "
        f"FROM {table}"
    )
    yield "missing_order", (
        f"SELECT LEAD(v,2) OVER (ORDER BY id) AS literal,LEAD(v,1+1) OVER () AS invalid FROM {table}"
    )


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-equivalent-windows")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([types.StructField(name, types.IntegerType(), True)
                                   for name in ["id", "g", "k", "v"]])
        rows = [(0, 0, None, None), (1, 0, 1, 10), (2, 0, 1, 20), (3, 0, 3, None),
                (4, 0, 5, 10), (5, 1, None, None), (6, 1, 2, None),
                (7, None, 1, -4), (8, None, 1, 6), (9, None, 4, 6), (10, 0, 1, 20)]
        inputs = []
        for name, values in [("inputs", rows), ("empty_inputs", []),
                             ("null_inputs", [(0, None, None, None), (1, None, None, None)])]:
            spark.createDataFrame(values, schema).createOrReplaceTempView(name)
            inputs.append({"name": name, "schema": schema.jsonValue(),
                           "rows": [[None if value is None else str(value) for value in row]
                                    for row in values]})
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": inputs}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for table in ["inputs", "empty_inputs", "null_inputs"]:
                for name, sql in queries(table):
                    records.append({"id": name, "input": table, "ansi": ansi, "sql": sql,
                                    "expected": capture_query(spark, sql)})
        assert len(records) == 67
        assert len({(record["id"], record["input"], record["ansi"])
                    for record in records[1:]}) == 66
        Path(__file__).with_name("equivalent-windows-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1,
                          "errors": sum("condition" in record["expected"] for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
