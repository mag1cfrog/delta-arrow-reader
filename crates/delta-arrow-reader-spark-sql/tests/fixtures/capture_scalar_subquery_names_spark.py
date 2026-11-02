"""Capture scalar-subquery generated names and fields with Spark 4.2.0."""

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types

from capture_single_row_correlated_spark import capture, encode


def queries():
    for name, expression in [
        ("direct", "(SELECT 1)"),
        ("null_direct", "(SELECT NULL)"),
        ("empty_direct", "(SELECT id FROM scalar_empty)"),
        ("null_add", "NULL + (SELECT 1)"),
        ("null_divide", "(SELECT 1)/CAST(NULL AS INT)"),
        ("round_null_scale", "ROUND((SELECT 1),NULL)"),
        ("nested_scalar", "(SELECT (SELECT 1))"),
        ("nested_round", "ROUND((SELECT (SELECT 1)),NULL)"),
    ]:
        for explicit in [False, True]:
            alias = " AS value" if explicit else ""
            yield f"{name}_{'explicit' if explicit else 'generated'}", (
                f"SELECT {expression}{alias}")
    yield "nested_projection", "SELECT * FROM (SELECT NULL + (SELECT 1)) q"
    for table in ["scalar_inputs", "scalar_nulls", "scalar_empty"]:
        for explicit in [False, True]:
            alias = " AS value" if explicit else ""
            yield f"{table}_{'explicit' if explicit else 'generated'}", (
                f"SELECT p.id,(SELECT p.id){alias} FROM {table} p")
    for name, expression in [
        ("outer_order_ab", "p.a + p.b"),
        ("outer_order_ba", "p.b + p.a"),
        ("outer_repeated", "p.b + p.a + p.b"),
        ("outer_case", "p.A + p.b"),
    ]:
        yield name, f"SELECT (SELECT {expression}) FROM VALUES (1,2) p(a,B)"
    yield "outer_duplicate_names", (
        "SELECT (SELECT l.id + r.id) FROM scalar_inputs l CROSS JOIN scalar_inputs r")
    yield "outer_nested_local", (
        "SELECT (SELECT p.A + (SELECT q.a FROM VALUES (3) q(a))) FROM VALUES (1) p(a)")
    yield "outer_nested_shadowed", (
        "SELECT (SELECT (SELECT p.a) FROM VALUES (3) p(a)) FROM VALUES (1) p(a)")
    yield "outer_projection_and_filter", (
        "SELECT (SELECT p.a FROM VALUES (1) q(x) WHERE q.x=p.b) "
        "FROM VALUES (1,1) p(a,b)")
    yield "outer_wildcard", "SELECT (SELECT p.*) FROM VALUES (1) p(a)"
    yield "multiple_rows", "SELECT (SELECT id FROM scalar_inputs)"
    yield "multiple_columns", "SELECT (SELECT 1,2)"
    yield "fallible_scalar", "SELECT (SELECT CAST('bad' AS INT))"
    yield "dead_scalar", "SELECT CASE WHEN true THEN 7 ELSE (SELECT CAST('bad' AS INT)) END"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh output JSONL path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    schema = types.StructType([
        types.StructField("id", types.IntegerType(), True, {"origin": "scalar source"}),
    ])
    inputs = [("scalar_inputs", [(1,), (2,), (None,)]),
              ("scalar_nulls", [(None,), (None,)]), ("scalar_empty", [])]
    spark = (SparkSession.builder.master("local[1]").appName("dar-scalar-subquery-names")
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
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in queries():
                records.append({"id": name, "ansi": ansi, "sql": sql,
                                "expected": capture(spark, sql)})
        assert len(records) == 73
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == 72
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
