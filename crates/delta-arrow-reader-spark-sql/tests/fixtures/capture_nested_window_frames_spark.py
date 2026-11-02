"""Capture RANGE peer membership over nested integer/string keys with Spark 4.2.0."""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode


def window_inputs():
    for kind, value_type, values in [
        ("integer", types.IntegerType(), [2, None, 0, 2, -3, None, 0]),
        ("string", types.StringType(), ["b", None, "a", "b", "c", None, "a"]),
    ]:
        record = types.StructType([types.StructField("value", value_type, True)])
        for shape, key_type, wrap, extra in [
            ("scalar", value_type, lambda v: v, []),
            ("array", types.ArrayType(value_type), lambda v: [v, None], [None, [], [None]]),
            ("struct", record, lambda v: (v,), [None]),
            ("nested", types.ArrayType(record), lambda v: [(v,), None], [None, [], [None]]),
            ("struct_array", types.StructType([types.StructField("values", types.ArrayType(value_type))]),
             lambda v: ([v, None],), [None, (None,), ([],)]),
        ]:
            for source, keys in [("mixed", [wrap(v) for v in values] + extra),
                                 ("nulls", [None, None]), ("empty", [])]:
                yield f"{kind}_{shape}_{source}", key_type, keys


def queries(table):
    directions = ["ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST"]
    for index, direction in enumerate(directions):
        for partition_label, partition in [("global", ""), ("partitioned", "PARTITION BY id%2 ")]:
            order = f"{partition}ORDER BY k {direction}"
            fields = ["id", "k"]
            for label, bounds in [
                ("peer", "CURRENT ROW AND CURRENT ROW"),
                ("prefix", "UNBOUNDED PRECEDING AND CURRENT ROW"),
                ("suffix", "CURRENT ROW AND UNBOUNDED FOLLOWING"),
                ("whole", "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),
            ]:
                window = f"{order} RANGE BETWEEN {bounds}"
                fields.append(f"COUNT(*) OVER ({window}) AS {label}_count")
                fields.append(f"SORT_ARRAY(COLLECT_LIST(id) OVER ({window})) AS {label}_ids")
            fields.append(f"COUNT(*) OVER ({order}) AS default_count")
            yield f"{partition_label}_{index}", f"SELECT {','.join(fields)} FROM {table}"
    for direction in ["ASC", "DESC"]:
        window = f"ORDER BY k {direction},id%2 RANGE BETWEEN CURRENT ROW AND CURRENT ROW"
        yield f"multiple_{direction}", f"""SELECT id,k,COUNT(*) OVER ({window}) AS peer_count,
          SORT_ARRAY(COLLECT_LIST(id) OVER ({window})) AS peer_ids FROM {table}"""


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-nested-window-frames")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "inputs": []}
        definitions = []
        for table, key_type, values in window_inputs():
            schema = types.StructType([types.StructField("k", key_type, True),
                                       types.StructField("id", types.IntegerType(), False)])
            rows = [(value, i) for i, value in enumerate(values)]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            metadata["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                      "rows": [encode(row, schema) for row in rows]})
            definitions.extend((f"{table}_{label}", sql) for label, sql in queries(table))
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                expected = capture_query(spark, sql)
                assert "condition" not in expected and "schema" in expected and "rows" in expected, (name, expected)
                records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
        Path(__file__).with_name("nested-window-frames-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1, "inputs": len(metadata["inputs"])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
