"""Capture Spark 4.2.0 output-alias metadata without rewriting observed fields."""
import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(data_type, types.ArrayType):
        return [encode(item, data_type.elementType) for item in value]
    if isinstance(data_type, types.StructType):
        return [encode(item, field.dataType) for item, field in zip(value, data_type)]
    return str(value)


def queries(table):
    expressions = [
        ("columns", "id,k,s,nested,items"),
        ("literals", "1,'literal',NULL,true"),
        ("direct_cast", "CAST(k AS BIGINT)"),
        ("direct_try_cast", "TRY_CAST(k AS BIGINT)"),
        ("text_cast", "CAST(s AS INT)"),
        ("text_try_cast", "TRY_CAST(s AS INT)"),
        ("nested_cast", "CAST(CAST(k AS BIGINT) AS DOUBLE)"),
        ("nested_try_cast", "TRY_CAST(TRY_CAST(k AS BIGINT) AS DOUBLE)"),
        ("mixed_cast", "CAST(TRY_CAST(k AS BIGINT) AS DOUBLE)"),
        ("cast_literal", "CAST(1 AS BIGINT),TRY_CAST('2' AS INT)"),
        ("scalar", "ABS(k),k+1,COALESCE(k,1)"),
        ("aggregate", "SUM(k),MAX(k),COUNT(k),COUNT(*)"),
        ("explicit", "k AS value,CAST(k AS BIGINT) AS cast_value,ABS(k) AS absolute"),
        ("explicit_aggregate", "SUM(k) AS total,MAX(k) AS maximum,COUNT(*) AS count"),
        ("extract", "nested.item,nested.inner.leaf,items[0]"),
        ("cast_extract", "CAST(nested.item AS BIGINT),TRY_CAST(nested.item AS BIGINT)"),
        ("constructed_extract", "(STRUCT(k AS item)).item"),
        ("constructed_extract_explicit", "(STRUCT(k AS item)).item AS result"),
        ("explicit_extract", "nested.item AS result,items[0] AS first"),
        ("window", "LAG(k) OVER (ORDER BY id),SUM(k) OVER (ORDER BY id)"),
    ]
    for label, expression in expressions:
        yield label, f"SELECT {expression} FROM {table}"
    for label, inner in [
        ("scalar", f"SELECT ABS(k) AS result FROM {table}"),
        ("generated", f"SELECT ABS(k) FROM {table}"),
        ("aggregate", f"SELECT SUM(k) FROM {table}"),
        ("extracted", f"SELECT nested.item FROM {table}"),
        ("constructed", f"SELECT (STRUCT(k AS item)).item FROM {table}"),
    ]:
        yield f"subquery_{label}", f"SELECT * FROM ({inner}) q"
        yield f"subquery_rename_{label}", f"SELECT * FROM ({inner}) q(result)"
    yield "subquery_generated_reference", f"SELECT `abs(k)` FROM (SELECT ABS(k) FROM {table}) q"
    yield "subquery_generated_explicit", f"SELECT `abs(k)` AS result FROM (SELECT ABS(k) FROM {table}) q"
    yield "subquery_generated_cast", f"SELECT CAST(`abs(k)` AS BIGINT) FROM (SELECT ABS(k) FROM {table}) q"
    yield "pivot", f"SELECT * FROM (SELECT k,id FROM {table}) PIVOT(SUM(id),COUNT(*) AS n FOR k IN (2,-3))"
    yield "pivot_explicit", f"SELECT * FROM (SELECT k,id FROM {table}) PIVOT(SUM(id) AS total,COUNT(*) AS n FOR k IN (2 AS positive,-3 AS negative))"


def capture(spark, sql):
    result = {}
    try:
        frame = spark.sql(sql)
        result["public_schema"] = frame.schema.jsonValue()
        execution = frame._jdf.queryExecution()
        result["analyzed_schema"] = json.loads(execution.analyzed().schema().json())
        result["optimized_schema"] = json.loads(execution.optimizedPlan().schema().json())
        rows = [encode(row, frame.schema) for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        result["expected"] = {"schema": frame.schema.jsonValue(), "rows": rows}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        cause = getattr(error, "java_exception", None)
        while condition is None and cause is not None:
            condition = cause.getCondition()
            cause = cause.getCause()
        assert condition is not None, (sql, str(error))
        result["expected"] = {"condition": condition}
        result["error"] = {"condition": condition, "message": str(error)}
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), f"Refusing to overwrite {args.output}"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-generated-alias-metadata")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        leaf = types.StructType([
            types.StructField("leaf", types.IntegerType(), True, {"source": "deep"})])
        nested = types.StructType([
            types.StructField("item", types.IntegerType(), True, {"source": "nested"}),
            types.StructField("inner", leaf, True, {"source": "inner"})])
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False, {"source": "id"}),
            types.StructField("k", types.IntegerType(), True, {"source": "plain"}),
            types.StructField("s", types.StringType(), True, {"source": "text"}),
            types.StructField("nested", nested, True, {"source": "record"}),
            types.StructField("items", types.ArrayType(types.IntegerType(), True), True,
                              {"source": "array"}),
        ])
        mixed = [(0, 2, "2", (2, (4,)), [2, None]),
                 (1, None, None, None, None),
                 (2, -3, "bad", (None, (None,)), [-3]),
                 (3, 4, "4", (4, None), [4])]
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": []}]
        definitions = []
        for label, rows in [("mixed", mixed), ("nulls", [(0, None, None, None, None)]),
                            ("empty", [])]:
            table = f"alias_{label}"
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                         "rows": [encode(row, schema) for row in rows]})
            definitions.extend((f"{table}_{name}", sql) for name, sql in queries(table))
        assert len(definitions) == 105, len(definitions)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                records.append({"id": name, "ansi": ansi, "sql": sql, **capture(spark, sql)})
        assert len({(record["id"], record["ansi"]) for record in records[1:]}) == 210
        args.output.write_text("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                       for record in records))
        print(json.dumps({"observations": len(records) - 1, "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
