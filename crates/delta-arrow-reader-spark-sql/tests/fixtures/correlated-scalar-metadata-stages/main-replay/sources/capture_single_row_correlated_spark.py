"""Capture the guaranteed-one-row scalar boundary with independent Spark 4.2.0."""

import argparse
import json
import os
import struct
import sys
from pathlib import Path

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, functions, types


def queries(table):
    source = f"(SELECT id,g,v,txt FROM {table}) p"
    for name, expression in [
        ("plain", "(SELECT p.id)"),
        ("nested_alias", "(SELECT q.x FROM (SELECT p.id AS x) q)"),
        ("nested_reuse", "(SELECT q.x+q.x FROM (SELECT p.id AS x) q)"),
        ("nested_scalar", "(SELECT (SELECT p.id))"),
        ("nullable_case", "(SELECT CASE WHEN p.id IS NULL THEN NULL ELSE p.id END)"),
        ("fallible_live", "(SELECT 7 DIV p.v)"),
        ("fallible_case", "CASE WHEN p.v=0 THEN NULL ELSE (SELECT 7 DIV p.v) END"),
        ("fallible_dead", "CASE WHEN true THEN p.id ELSE (SELECT 7 DIV 0) END"),
        ("cast_live", "(SELECT CAST(p.txt AS INT))"),
        ("cast_case", "CASE WHEN p.txt='bad' THEN p.id ELSE (SELECT CAST(p.txt AS INT)) END"),
    ]:
        for cast in [False, True]:
            suffix = ",CAST(7 AS INT) AS incidental" if cast else ""
            yield f"{name}_{'cast' if cast else 'bare'}", (
                f"SELECT p.id AS id,{expression} AS correlated_id{suffix} FROM {source}")
    for name, inner in [
        ("zero", "SELECT i.id FROM inputs i WHERE false"),
        ("one", "SELECT i.id FROM one_input i"),
        ("multiple", "SELECT i.id FROM inputs i"),
    ]:
        yield f"uncorrelated_{name}", f"SELECT p.id,({inner}) AS r FROM {source}"
        yield f"correlated_{name}", (
            f"SELECT p.id,(SELECT p.id FROM ({inner}) i) AS r FROM {source}")
    yield "projected_in", (
        f"SELECT p.id,p.id IN (SELECT i.id FROM inputs i) AS r FROM {source}")
    yield "projected_exists", (
        f"SELECT p.id,EXISTS(SELECT i.id FROM inputs i WHERE i.id=p.id) AS r FROM {source}")


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.DoubleType):
        return struct.pack("!d", value).hex()
    return str(value).lower() if isinstance(value, bool) else str(value)


def capture(spark, sql):
    try:
        frame = spark.sql(sql)
        schema = frame.schema
        rows = [[encode(value, field.dataType) for value, field in zip(row, schema)]
                for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        return {"status": "ok", "schema": schema.jsonValue(), "rows": rows}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        cause = getattr(error, "java_exception", None)
        while condition is None and cause is not None:
            try:
                condition = cause.getCondition()
            except Py4JError:
                pass
            cause = cause.getCause()
        assert condition is not None, (sql, str(error))
        return {"status": "error", "condition": condition}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh output JSONL path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    schema = types.StructType([
        types.StructField("id", types.IntegerType(), True),
        types.StructField("g", types.IntegerType(), True),
        types.StructField("v", types.IntegerType(), True),
        types.StructField("txt", types.StringType(), True),
    ])
    inputs = [
        ("inputs", [(0, 0, 0, "bad"), (1, 0, 1, "7"), (2, 1, -2, "-2"),
                    (None, None, None, None)]),
        ("empty_inputs", []),
        ("null_inputs", [(0, None, None, None), (None, None, None, None)]),
        ("one_input", [(1, 0, 1, "7")]),
    ]
    spark = (SparkSession.builder.master("local[1]").appName("dar-single-row-correlated")
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
            for table in ["inputs", "empty_inputs", "null_inputs"]:
                for name, sql in queries(table):
                    records.append({"id": f"{table}_{name}", "ansi": ansi, "sql": sql,
                                    "expected": capture(spark, sql)})
            for name, expression in [
                ("volatile_direct", "(SELECT count_row(p.id))"),
                ("volatile_nested_once", "(SELECT q.x FROM (SELECT count_row(p.id) AS x) q)"),
                ("volatile_nested_reuse", "(SELECT q.x+q.x FROM (SELECT count_row(p.id) AS x) q)"),
                ("volatile_unused", "(SELECT q.x FROM (SELECT p.id AS x,count_row(p.id) AS unused) q)"),
            ]:
                for table in ["inputs", "empty_inputs"]:
                    for cast in [False, True]:
                        suffix = ",CAST(7 AS INT) AS incidental" if cast else ""
                        sql = f"SELECT p.id,{expression} AS r{suffix} FROM {table} p"
                        calls.value = 0
                        expected = capture(spark, sql)
                        records.append({"id": f"{table}_{name}_{'cast' if cast else 'bare'}",
                                        "ansi": ansi, "sql": sql, "expected": expected,
                                        "evaluated_rows": calls.value})
            sql = "SELECT p.id,(SELECT p.id+rand()) AS r FROM inputs p"
            records.append({"id": "volatile_rand_raw", "ansi": ansi, "sql": sql,
                            "expected": capture(spark, sql), "raw_observation_only": True})
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records)-1
        with args.output.open("x") as output:
            output.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                 for record in records))
        print(json.dumps({"observations": len(records)-1,
                          "errors": sum(r.get("expected", {}).get("status") == "error"
                                        for r in records)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
