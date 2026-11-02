"""Capture collection results with PySpark 4.2.0, including nested NULLs and windows."""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode, inputs


def collection_inputs():
    for table, _, _, key_type, values in inputs():
        if table.endswith(("_mixed", "_nulls", "_empty")):
            yield table, key_type, values
    for kind, value_type, values in [
        ("integer", types.IntegerType(), [1, 1, -2, None]),
        ("string", types.StringType(), ["one", "one", "two", None]),
    ]:
        record = types.StructType([
            types.StructField("value", value_type, True, {"label": "measurement"})])
        for shape, key_type, wrapped in [
            ("struct", record, [(v,) for v in values] + [None]),
            ("nested", types.ArrayType(record), [[(v,), None] for v in values] + [None, []]),
            ("struct_array", types.StructType([
                types.StructField("values", types.ArrayType(value_type), True,
                                  {"label": "measurements"})]),
             [([v, None],) for v in values] + [None, (None,), ([],)]),
        ]:
            for source, rows in [("mixed", wrapped), ("nulls", [None, None]), ("empty", [])]:
                yield f"{kind}_{shape}_{source}", key_type, rows


def collection_queries(table):
    yield "global", f"""SELECT SORT_ARRAY(COLLECT_SET(k)) AS unique_values,
      COLLECT_LIST(k) AS repeated_values,
      SORT_ARRAY(COLLECT_LIST(DISTINCT k)) AS distinct_values,
      ARRAY_AGG(k) AS array_values FROM {table}"""
    yield "filtered", f"""SELECT
      SORT_ARRAY(COLLECT_SET(k) FILTER(WHERE id%2=0)) AS unique_values,
      COLLECT_LIST(k) FILTER(WHERE id%2=0) AS repeated_values,
      COLLECT_SET(k) FILTER(WHERE FALSE) AS empty_values FROM {table}"""
    yield "grouped", f"""SELECT id%3 AS bucket,
      SORT_ARRAY(COLLECT_SET(k)) AS unique_values,
      COLLECT_LIST(k) AS repeated_values
      FROM {table} GROUP BY id%3 HAVING COUNT(*)>0"""
    yield "respect_nulls", f"""SELECT
      SORT_ARRAY(COLLECT_SET(k) RESPECT NULLS) AS unique_values,
      COLLECT_LIST(k) RESPECT NULLS AS repeated_values
      FROM {table}"""
    for label, frame in [("sliding", "2 PRECEDING AND CURRENT ROW"),
                         ("following", "1 FOLLOWING AND 2 FOLLOWING"),
                         ("full", "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING")]:
        window = f"PARTITION BY id%2 ORDER BY id ROWS BETWEEN {frame}"
        yield label, f"""SELECT id,
          SORT_ARRAY(COLLECT_SET(k) OVER ({window})) AS unique_values,
          COLLECT_LIST(k) OVER ({window}) AS ordered_values FROM {table}"""


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-nested-collections")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "inputs": []}
        definitions = []
        for table, key_type, values in collection_inputs():
            schema = types.StructType([types.StructField("k", key_type, True),
                                       types.StructField("id", types.IntegerType(), False)])
            rows = [(value, i) for i, value in enumerate(values)]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            metadata["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                      "rows": [encode(row, schema, input_value=True) for row in rows]})
            definitions.extend((f"{table}_{label}", sql,
                                (["repeated_values", "array_values"] if label == "global" else
                                 ["repeated_values"] if label in ["filtered", "grouped", "respect_nulls"] else []))
                               for label, sql in collection_queries(table))
        definitions.append(("untyped_null", "SELECT COLLECT_LIST(NULL) AS repeated_values, COLLECT_SET(NULL) AS unique_values", []))
        for kind in ["float", "double"]:
            value = f"CAST('-0.0' AS {kind})"
            for shape, argument in [
                ("array", f"ARRAY({value})"),
                ("struct", f"NAMED_STRUCT('value',{value})"),
                ("nested", f"ARRAY(NAMED_STRUCT('value',{value}))"),
                ("struct_array", f"NAMED_STRUCT('values',ARRAY({value}))"),
            ]:
                if shape == "nested":
                    argument = f"CAST({argument} AS ARRAY<STRUCT<value:{kind}>>)"
                elif shape == "struct_array":
                    argument = f"CAST({argument} AS STRUCT<values:ARRAY<{kind}>>)"
                for source in ["mixed", "empty"]:
                    sql = f"""SELECT COLLECT_SET({argument}) AS unique_values,
                      COLLECT_LIST({argument}) AS repeated_values,
                      COLLECT_SET({argument}) RESPECT NULLS AS nullable_unique,
                      COLLECT_LIST({argument}) RESPECT NULLS AS nullable_repeated
                      FROM {kind}_scalar_{source}"""
                    definitions.append((f"constant_{kind}_{shape}_{source}", sql, []))
                window = "ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING"
                sql = f"""SELECT id,COLLECT_SET({argument}) OVER ({window}) AS unique_values,
                  COLLECT_LIST({argument}) OVER ({window}) AS repeated_values
                  FROM {kind}_scalar_mixed"""
                definitions.append((f"constant_{kind}_{shape}_window", sql, []))
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql, unordered_fields in definitions:
                records.append({"id": name, "ansi": ansi, "sql": sql,
                                "unordered_fields": unordered_fields, "expected": capture_query(spark, sql)})
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records)-1
        Path(__file__).with_name("nested-collections-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1, "inputs": len(metadata["inputs"])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
