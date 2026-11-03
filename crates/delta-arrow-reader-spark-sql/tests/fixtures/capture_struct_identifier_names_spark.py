"""Capture independent STRUCT/IDENTIFIER names and complete schemas and rows."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode


def queries(table):
    source = f"FROM {table} q"
    for name, expression in [
        ("identifier_nested", "STRUCT(IDENTIFIER('nested.item')) AS s"),
        ("identifier_qualified", "STRUCT(IDENTIFIER('q.nested.item')) AS s"),
        ("identifier_deep", "STRUCT(IDENTIFIER('nested.inner.leaf')) AS s"),
        ("identifier_bracket", "STRUCT(IDENTIFIER('nested')['item']) AS s"),
        ("identifier_explicit_alias", "STRUCT(IDENTIFIER('nested.item') AS renamed) AS s"),
        ("identifier_plain", "STRUCT(IDENTIFIER('v')) AS s"),
        ("identifier_generated", "STRUCT(IDENTIFIER('nested.item')), STRUCT(IDENTIFIER('nested.item') AS renamed)"),
    ]:
        yield name, f"SELECT {expression} {source}"
    yield "identifier_consumer", f"SELECT s.item AS result FROM (SELECT STRUCT(IDENTIFIER('nested.item')) AS s {source})"


def main():
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_name("struct-identifier-names-spark.jsonl")
    assert not output.exists(), output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-struct-expression-names")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        nested = types.StructType([
            types.StructField("item", types.IntegerType(), True, {"source": "nested"}),
            types.StructField("inner", types.StructType([
                types.StructField("leaf", types.IntegerType(), True, {"source": "deep"})]),
                True, {"source": "inner"}),
        ])
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False, {"source": "id"}),
            types.StructField("v", types.IntegerType(), True, {"source": "plain"}),
            types.StructField("nested", nested, True, {"source": "record"}),
            types.StructField("quoted", types.StructType([
                types.StructField(name, types.IntegerType(), True, {"source": name})
                for name in ["a.b", "sp ace", "back`tick"]]), True),
            types.StructField("items", types.ArrayType(types.StructType([
                types.StructField("item", types.IntegerType(), True, {"source": "array"})])), True),
            types.StructField("required", types.StructType([
                types.StructField("item", types.IntegerType(), False, {"source": "required"})]), False),
        ])
        rows = [(0, -2, (-3, (-4,)), (-5, -6, -7), [(-8,), None], (9,)),
                (1, 2, (3, (4,)), (5, 6, 7), [(8,)], (10,)),
                (2, None, (None, None), (None, None, None), [(None,)], (11,)),
                (3, None, None, None, None, (12,))]
        nulls = [(0, None, None, None, None, (1,))]
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": []}]
        for table, values in [("struct_mixed", rows), ("struct_nulls", nulls), ("struct_empty", [])]:
            spark.createDataFrame(values, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                         "rows": [encode(row, schema, input_value=True)
                                                  for row in values]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for table in ["struct_mixed", "struct_nulls", "struct_empty"]:
                definitions = list(queries(table))
                for name, sql in definitions:
                    records.append({"id": name, "input": table, "ansi": ansi, "sql": sql,
                                    "expected": capture_query(spark, sql)})
        assert len(records) == 49
        assert len({(record["id"], record["input"], record["ansi"])
                    for record in records[1:]}) == 48
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1, "output": str(output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
