"""Capture aggregate windows independently of DAR's pending FILTER frontend."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types


INPUTS = [
    [0, 0, 0, True, None], [1, 0, 0, False, None], [2, 0, 1, None, 2],
    [3, 0, 4, True, 3], [4, 0, 4, False, 3], [5, 0, 5, True, None],
    [6, 1, 9, True, 4], [7, 1, 9, None, 4], [8, 1, 12, False, 4],
    [9, 1, 15, True, 6], [10, 1, 15, True, 6], [11, 1, 19, True, 7],
]
FRAMES = [
    ("rows", "ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"),
    ("following", "ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING"),
    ("partition", "PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING"),
    ("range", "ORDER BY k RANGE BETWEEN 2 PRECEDING AND CURRENT ROW"),
    ("range_gaps", "ORDER BY k RANGE BETWEEN 2 FOLLOWING AND 3 FOLLOWING"),
    ("cumulative", "ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
    ("suffix", "ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"),
]
FIRST_VALUE_REWRITE = "org.apache.spark.sql.catalyst.optimizer.OptimizeWindowFunctions"


def build_window_query_pair(frame, filtered):
    predicate = "keep" if filtered else "true"
    clause = f" FILTER (WHERE {predicate})"
    over = f" OVER ({frame})"
    spark, native = [], []
    for function in ["first_value", "last_value"]:
        for ignore_nulls in [False, True]:
            treatment = " IGNORE NULLS" if ignore_nulls else " RESPECT NULLS"
            alias = f"{function}_{str(ignore_nulls).lower()}"
            spark.append(f"{function}(value){clause}{treatment}{over} AS {alias}")
            native.append(f"{function}(value){clause}{treatment}{over} AS {alias}")
    spark.append(f"approx_count_distinct(value){clause}{over} AS distinct_count")
    native.append(f"COALESCE(CAST(approx_distinct(value){clause}{over} AS BIGINT),"
                  "CAST(0 AS BIGINT)) AS distinct_count")
    # Native ARRAY_AGG returns NULL for no selected values. State that contract
    # explicitly in the reference instead of changing Spark's COLLECT_SET result.
    spark.append(
        f"CASE WHEN COUNT(value){clause}{over}=0 THEN CAST(NULL AS ARRAY<INT>) "
        f"ELSE SORT_ARRAY(COLLECT_SET(value){clause}{over}) END AS distinct_values")
    native.append(
        f"ARRAY_SORT(ARRAY_AGG(DISTINCT value) FILTER "
        f"(WHERE ({predicate}) AND value IS NOT NULL){over}) AS distinct_values")
    for function in ["sum", "count", "min", "max"]:
        expression = f"{function}(value){clause}{over} AS {function}_value"
        spark.append(expression)
        native.append(expression)
    return tuple(f"SELECT id,{','.join(columns)} FROM inputs ORDER BY id"
                 for columns in [spark, native])


def encode_reference_value(value):
    if value is None:
        return None
    if isinstance(value, list):
        return [encode_reference_value(item) for item in value]
    return str(value)


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-bounded-windows")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([
            types.StructField(name, kind, True)
            for name, kind in [("id", types.IntegerType()), ("g", types.IntegerType()),
                               ("k", types.IntegerType()), ("keep", types.BooleanType()),
                               ("value", types.IntegerType())]
        ])
        spark.createDataFrame(INPUTS, schema).createOrReplaceTempView("inputs")
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": INPUTS}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for filtered in [False, True]:
                for name, frame in FRAMES:
                    spark_sql, native_sql = build_window_query_pair(frame, filtered)
                    result = spark.sql(spark_sql)
                    record = {"id": f"{name}_{filtered}", "ansi": ansi,
                              "spark_sql": spark_sql, "native_sql": native_sql,
                              "schema": result.schema.jsonValue(),
                              "rows": [[encode_reference_value(v) for v in row] for row in result.collect()]}
                    if name == "cumulative" and filtered:
                        record["optimized_plan"] = result._jdf.queryExecution().optimizedPlan().toString()
                        spark.conf.set("spark.sql.optimizer.excludedRules", FIRST_VALUE_REWRITE)
                        try:
                            reference = spark.sql(spark_sql)
                            record["filter_preserving_reference"] = {
                                "excluded_rule": FIRST_VALUE_REWRITE,
                                "optimized_plan": reference._jdf.queryExecution().optimizedPlan().toString(),
                                "schema": reference.schema.jsonValue(),
                                "rows": [[encode_reference_value(v) for v in row] for row in reference.collect()],
                            }
                        finally:
                            spark.conf.set("spark.sql.optimizer.excludedRules", "")
                    records.append(record)
        assert len(records) == 29
        Path(__file__).with_name("bounded-window-spark.jsonl").write_text(
            "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records))
        print(json.dumps({"observations": len(records) - 1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
