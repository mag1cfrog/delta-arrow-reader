"""Capture Spark 4.2.0 field projection and enclosing expression names."""
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode


def queries(table):
    for name, expression in [
        ("plain", "id"), ("qualified_plain", "q.id"),
        ("nested", "nested.item"), ("qualified_nested", "q.nested.item"),
        ("deep", "deep.inner.item"), ("case", "NESTED.ITEM"),
        ("quoted_dot", "quoted.`a.b`"), ("quoted_space", "quoted.`sp ace`"),
        ("quoted_tick", "quoted.`back``tick`"),
        ("quoted_column", "`plain.dot`"),
        ("bracket", "nested['item']"), ("parenthesized", "(nested).item"),
        ("constructed", "(STRUCT(v AS item)).item"),
        ("function", "ABS(nested.item)"),
        ("qualified_function", "ABS(q.nested.item)"),
        ("deep_function", "ABS(deep.inner.item)"),
        ("quoted_function", "ABS(quoted.`a.b`)"),
        ("plain_dot_function", "ABS(`plain.dot`)"),
        ("case_function", "ABS(NESTED.ITEM)"),
        ("bracket_function", "ABS(nested['item'])"),
        ("arithmetic", "nested.item+1"),
        ("cast", "CAST(nested.item AS BIGINT)"),
        ("cast_function", "ABS(CAST(nested.item AS BIGINT))"),
        ("null_predicate", "nested.item IS NULL"),
        ("array_field", "items.item"),
        ("array_element", "items[0].item"),
        ("array_function", "ABS(items[0].item)"),
        ("struct", "STRUCT(nested.item)"),
        ("struct_alias", "STRUCT(nested.item AS renamed)"),
        ("explicit_alias", "ABS(nested.item) AS chosen"),
        ("plain_function", "ABS(q.v)"),
        ("window_partition", "SUM(v) OVER (PARTITION BY nested.item ORDER BY id)"),
        ("window_order", "SUM(v) OVER (ORDER BY deep.inner.item,id)"),
        ("window_argument", "SUM(nested.item) OVER (ORDER BY id)"),
        ("window_qualified", "SUM(v) OVER (PARTITION BY q.nested.item ORDER BY q.id)"),
        ("window_quoted", "SUM(v) OVER (PARTITION BY quoted.`a.b` ORDER BY id)"),
        ("window_alias", "SUM(nested.item) OVER (PARTITION BY nested.item ORDER BY id) AS total"),
        ("missing_field", "nested.missing"),
        ("missing_qualifier", "missing.nested.item"),
    ]:
        yield name, f"SELECT {expression} FROM {table} q"
    yield "outer_projection", f"SELECT item FROM (SELECT nested.item FROM {table}) q"
    yield "outer_alias", f"SELECT chosen FROM (SELECT ABS(nested.item) AS chosen FROM {table}) q"


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-nested-field-labels")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        item = types.StructType([
            types.StructField("item", types.IntegerType(), True, {"source": "nested"})])
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False),
            types.StructField("v", types.IntegerType(), True, {"source": "plain"}),
            types.StructField("nested", item, True),
            types.StructField("deep", types.StructType([
                types.StructField("inner", item, True)]), True),
            types.StructField("quoted", types.StructType([
                types.StructField(name, types.IntegerType(), True)
                for name in ["a.b", "sp ace", "back`tick"]]), True),
            types.StructField("plain.dot", types.IntegerType(), True),
            types.StructField("items", types.ArrayType(item), True),
        ])
        rows = [(0, -2, (-3,), ((-4,),), (-5, -6, -7), -8, [(-9,), None]),
                (1, 4, (2,), ((3,),), (4, 5, 6), 7, [(8,)]),
                (2, None, (None,), (None,), (None, None, None), None, [(None,)]),
                (3, 5, None, None, None, None, None)]
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": []}]
        for table, values in [("inputs", rows), ("empty_inputs", [])]:
            spark.createDataFrame(values, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                         "rows": [encode(row, schema, input_value=True)
                                                  for row in values]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for table in ["inputs", "empty_inputs"]:
                definitions = list(queries(table))
                for name, inner in [
                    ("outer_generated", f"SELECT ABS(nested.item) FROM {table}"),
                    ("outer_generated_window", f"SELECT SUM(v) OVER (PARTITION BY nested.item ORDER BY id) FROM {table}"),
                ]:
                    label = spark.sql(inner).columns[0].replace("`", "``")
                    definitions.append((name, f"SELECT `{label}` FROM ({inner}) q"))
                for name, sql in definitions:
                    records.append({"id": name, "input": table, "ansi": ansi, "sql": sql,
                                    "expected": capture_query(spark, sql)})
        assert len(records) == 173
        assert len({(record["id"], record["input"], record["ansi"])
                    for record in records[1:]}) == 172
        Path(__file__).with_name("nested-field-labels-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1,
                          "errors": sum("condition" in record["expected"] for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
