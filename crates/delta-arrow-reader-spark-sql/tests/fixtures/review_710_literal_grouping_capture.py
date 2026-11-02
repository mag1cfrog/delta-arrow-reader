"""Capture bounded issue 710 literal grouping and ORDER BY identity cases."""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions, types


QUERIES = [
    ("generated_alias", "SELECT CAST(NULL AS INT), GROUPING(CAST(NULL AS INT)) AS g, COUNT(*) AS n FROM VALUES (1),(2) t(k) GROUP BY ROLLUP(`CAST(NULL AS INT)`)"),
    ("explicit_alias", "SELECT CAST(NULL AS INT) AS value, GROUPING(CAST(NULL AS INT)) AS g, COUNT(*) AS n FROM VALUES (1),(2) t(k) GROUP BY ROLLUP(value)"),
    ("direct_expression", "SELECT CAST(NULL AS INT), GROUPING(CAST(NULL AS INT)) AS g, COUNT(*) AS n FROM VALUES (1),(2) t(k) GROUP BY ROLLUP(CAST(NULL AS INT))"),
]


def counter_case(spark, ansi, label, projection, order):
    counter = spark.sparkContext.accumulator(0)

    def count_value(value):
        counter.add(1)
        return value

    spark.udf.register(
        "review_annotation_counter",
        functions.udf(count_value, types.IntegerType()).asNondeterministic())
    sql = f"SELECT {projection} FROM annotation_counter_inputs ORDER BY {order}"
    record = {"id": label, "kind": "order_by_counter", "ansi": ansi, "sql": sql,
              "count_before_planning": counter.value, "collect_actions": 0}
    try:
        frame = spark.sql(sql)
        record["public_schema"] = frame.schema.jsonValue()
        execution = frame._jdf.queryExecution()
        record["analyzed_schema"] = json.loads(execution.analyzed().schema().json())
        record["optimized_schema"] = json.loads(execution.optimizedPlan().schema().json())
        record["analyzed_plan"] = execution.analyzed().toString()
        record["optimized_plan"] = execution.optimizedPlan().toString()
        record["physical_plan"] = execution.executedPlan().toString()
        record["count_after_planning"] = counter.value
        record["count_before_collect"] = counter.value
        record["collect_actions"] += 1
        collected = frame.collect()
        record["count_after_collect"] = counter.value
        rows = [[None if value is None else str(value) for value in row] for row in collected]
        record["expected"] = {"schema": frame.schema.jsonValue(), "rows": rows}
    except Exception as error:
        record["count_after_error"] = counter.value
        record["error"] = {"message": str(error),
                           "condition": getattr(error, "getCondition", lambda: None)()}
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-review-710-literal-grouping")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        counter_schema = types.StructType([types.StructField("k", types.IntegerType(), True)])
        spark.createDataFrame([(1,), (None,)], counter_schema).createOrReplaceTempView(
            "annotation_counter_inputs")
        records = [{"spark_version": spark.version, "timezone": "UTC",
                    "inputs": [{"name": "annotation_counter_inputs",
                                "schema": counter_schema.jsonValue(), "rows": [["1"], [None]]}],
                    "counter_limitations": [
                        "Accumulator counts executor Python UDF invocations; retries or recomputation can add increments.",
                        "One local[1] collect action per counter query; this is not a distributed exactly-once guarantee.",
                        "Schema and plan inspection requests analysis/planning but no extra collect or count action."],
                    "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for label, sql in QUERIES:
                record = {"id": label, "kind": "literal_grouping", "ansi": ansi, "sql": sql}
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
                    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                    record["expected"] = {"schema": frame.schema.jsonValue(), "rows": rows}
                except Exception as error:
                    record["error"] = {"message": str(error),
                                       "condition": getattr(error, "getCondition", lambda: None)()}
                records.append(record)
            for label, projection, order in [
                ("order_alias", "review_annotation_counter(k) AS r", "r"),
                ("order_text", "review_annotation_counter(k)", "review_annotation_counter(k)"),
            ]:
                records.append(counter_case(spark, ansi, label, projection, order))
        assert len(records) == 11
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations":len(records)-1,"output":str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
