"""Capture STRUCT child names, values and executed types with Spark 4.2.0."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode


def capture_struct_query(spark, sql):
    try:
        frame = spark.sql(sql)
    except Exception:
        return capture_query(spark, sql)
    rows = [encode(row, frame.schema) for row in frame.collect()]
    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
    schema = json.loads(frame._jdf.queryExecution().executedPlan().schema().json())
    return {"schema": schema, "rows": rows}


def queries(table):
    source = f"FROM {table} q"
    for name, expression in [
        ("inferred", "STRUCT(nested.item) AS s"),
        ("json_nested", "TO_JSON(STRUCT(nested.item)) AS result"),
        ("json_quoted", "TO_JSON(STRUCT(quoted.`a.b`, quoted.`sp ace`)) AS result"),
        ("json_explicit", "TO_JSON(STRUCT(nested.item AS renamed)) AS result"),
        ("qualified_deep", "STRUCT(q.nested.item, q.nested.inner.leaf) AS s"),
        ("case", "STRUCT(NESTED.ITEM, NESTED.INNER.LEAF) AS s"),
        ("quoted", "STRUCT(quoted.`a.b`, quoted.`sp ace`, quoted.`back``tick`) AS s"),
        ("bracket", "STRUCT(nested['item'], nested['inner']['leaf']) AS s"),
        ("parenthesized", "STRUCT((nested).item) AS s"),
        ("array_field", "STRUCT(items.item, items[0].item) AS s"),
        ("explicit_children", "STRUCT(nested.item AS renamed, nested.inner.leaf AS leaf_copy, quoted.`a.b` AS `dot.alias`, v AS plain_copy, ABS(v) AS abs_value, 1 AS literal) AS s"),
        ("unnamed", "STRUCT(id, v, v+1, ABS(v), NULL, 1, CAST(nested.item AS BIGINT)) AS s"),
        ("nonnullable", "STRUCT(required.item, required) AS s"),
        ("constructed_child", "STRUCT((STRUCT(v AS item)).item) AS s"),
        ("whole_record", "STRUCT(nested) AS s"),
        ("wildcard", "STRUCT(q.*) AS s"),
        ("wildcard_then_literal", "STRUCT(nested.*, 1) AS s"),
        ("wildcard_then_computed", "STRUCT(q.*, ABS(v)) AS s"),
        ("wildcard_then_identifier", "STRUCT(nested.*, IDENTIFIER('v')) AS s"),
        ("correlated", "(SELECT STRUCT(q.nested.item)) AS s"),
        ("nested_wildcard", "STRUCT(nested.*) AS s"),
        ("empty_constructor", "STRUCT() AS s"),
        ("generated", "STRUCT(nested.item), STRUCT(q.nested.inner.leaf), STRUCT(nested.item AS renamed), STRUCT(v AS item), STRUCT(quoted.`a.b` AS `dot.alias`), STRUCT(ABS(v) AS computed), STRUCT(1, NULL)"),
        ("generated_extract", "(STRUCT(v AS item)).item, (STRUCT(nested.item AS renamed)).renamed, (STRUCT(quoted.`a.b` AS `dot.alias`)).`dot.alias`"),
        ("generated_enclosing", "ARRAY(STRUCT(nested.item AS renamed)), STRUCT(STRUCT(v AS item) AS record)"),
        ("generated_wildcard", "STRUCT(nested.*), STRUCT(required.*)"),
    ]:
        yield name, f"SELECT {expression} {source}"
    yield "inferred_consumer", f"SELECT s.item AS result FROM (SELECT STRUCT(nested.item) AS s {source})"
    yield "qualified_consumer", f"SELECT s.leaf AS result FROM (SELECT STRUCT(q.nested.inner.leaf) AS s {source})"
    yield "quoted_consumer", f"SELECT s.`a.b` AS result FROM (SELECT STRUCT(quoted.`a.b`) AS s {source})"
    yield "explicit_consumer", f"SELECT s.renamed AS result FROM (SELECT STRUCT(nested.item AS renamed) AS s {source})"


def main():
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_name("struct-expression-names-spark.jsonl")
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
                for name, expression in [("outer_generated", "STRUCT(nested.item AS renamed)"),
                                         ("outer_generated_extract", "(STRUCT(v AS item)).item")]:
                    inner = f"SELECT {expression} FROM {table}"
                    label = spark.sql(inner).columns[0].replace("`", "``")
                    definitions.append((name, f"SELECT `{label}` FROM ({inner}) q"))
                for name, sql in definitions:
                    records.append({"id": name, "input": table, "ansi": ansi, "sql": sql,
                                    "expected": capture_struct_query(spark, sql)})
        assert len(records) == 193
        assert len({(record["id"], record["input"], record["ansi"])
                    for record in records[1:]}) == 192
        assert all("schema" in record["expected"] for record in records[1:])
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1, "output": str(output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
