"""Capture floating window partitions, peers and frame membership with Spark 4.2.0."""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode, inputs


def window_inputs():
    for table, kind, shape, key_type, values in inputs():
        if table.endswith(("_mixed", "_nulls", "_empty", "_finite")):
            yield table, kind, shape, key_type, values
    yield "integer_control", "integer", "scalar", types.IntegerType(), [2, None, 0, 2, -3, None]
    yield "string_control", "string", "scalar", types.StringType(), ["b", None, "a", "b", "c", None]


def window_queries(table, kind, shape):
    yield "partition", f"""SELECT id,k,COUNT(*) OVER (PARTITION BY k) AS members,
      COUNT(k) OVER (PARTITION BY k) AS nonnull,
      SORT_ARRAY(COLLECT_LIST(id) OVER (PARTITION BY k)) AS member_ids FROM {table}"""
    yield "partition_multi", f"""SELECT id,k,COUNT(*) OVER (PARTITION BY k,id%2) AS members,
      SORT_ARRAY(COLLECT_LIST(id) OVER (PARTITION BY k,id%2)) AS member_ids FROM {table}"""
    yield "named_partition", f"""SELECT id,k,ROW_NUMBER() OVER w AS position,
      LAG(k,1) OVER w AS previous_value,
      COLLECT_LIST(id) OVER (PARTITION BY k ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS member_ids
      FROM {table} WINDOW w AS (PARTITION BY k ORDER BY id)"""
    directions = (["", "ASC NULLS FIRST", "ASC NULLS LAST", "DESC", "DESC NULLS FIRST", "DESC NULLS LAST"]
                  if not table.endswith(("_empty", "_nulls")) else ["", "DESC NULLS FIRST"])
    for index, direction in enumerate(directions):
        for partition_label, partition in [("global", ""), ("partitioned", "PARTITION BY id%2 ")]:
            peers = f"{partition}ORDER BY k {direction}"
            ordered = f"{peers},id"
            yield f"order_{partition_label}_{index}", f"""SELECT id,k,
              RANK() OVER ({peers}) AS rank,DENSE_RANK() OVER ({peers}) AS dense_rank,
              COUNT(*) OVER ({peers} RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS peers,
              SORT_ARRAY(COLLECT_LIST(id) OVER ({peers} RANGE BETWEEN CURRENT ROW AND CURRENT ROW)) AS peer_ids,
              ROW_NUMBER() OVER ({ordered}) AS position,LAG(k,1) OVER ({ordered}) AS previous_value,
              COLLECT_LIST(id) OVER ({ordered} ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS nearby_ids
              FROM {table}"""
        if shape == "scalar" and kind != "string":
            yield f"bounded_range_{index}", f"""SELECT id,k,
              COUNT(*) OVER (ORDER BY k {direction} RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS members,
              SORT_ARRAY(COLLECT_LIST(id) OVER (ORDER BY k {direction} RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS member_ids
              FROM {table}"""
    if not table.endswith(("_empty", "_nulls")):
        for label, direction in [("asc", "ASC"), ("desc", "DESC NULLS FIRST")]:
            order = f"ORDER BY k {direction},id%2"
            yield f"multi_order_{label}", f"""SELECT id,k,
              RANK() OVER ({order}) AS rank,
              SORT_ARRAY(COLLECT_LIST(id) OVER ({order} RANGE BETWEEN CURRENT ROW AND CURRENT ROW)) AS peer_ids
              FROM {table}"""
    if shape == "scalar" and kind in ["float", "double"] and table.endswith(("_mixed", "_finite")):
        for label, key in [
            ("sign", f"ARRAY(CAST(CASE WHEN CAST(k AS STRING)='-0.0' THEN -1 ELSE 1 END AS {kind}))"),
            ("payload", "NAMED_STRUCT('key',k,'original',CAST(k AS STRING))"),
        ]:
            yield f"computed_{label}", f"""SELECT id,k,
              COUNT(*) OVER (PARTITION BY {key}) AS members,
              RANK() OVER (ORDER BY {key}) AS rank,
              ROW_NUMBER() OVER (ORDER BY {key},id) AS position FROM {table}"""


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-floating-windows")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "inputs": []}
        definitions = []
        for table, kind, shape, key_type, values in window_inputs():
            schema = types.StructType([
                types.StructField("k", key_type, True, {"purpose": "window-key"}),
                types.StructField("id", types.IntegerType(), False),
            ])
            rows = [(value, i) for i, value in enumerate(values)]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            metadata["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                      "rows": [encode(row, schema, input_value=True) for row in rows]})
            definitions.extend((f"{table}_{label}", sql) for label, sql in window_queries(table, kind, shape))
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                records.append({"id": name, "ansi": ansi, "sql": sql,
                                "expected": capture_query(spark, sql)})
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records)-1
        Path(__file__).with_name("floating-windows-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1, "inputs": len(metadata["inputs"])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
