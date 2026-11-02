"""Capture scalar FLOAT/DOUBLE query order independently with Spark 4.2.0."""
import argparse
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, functions, types

from capture_floating_group_keys_spark import capture_query, encode, inputs


DIRECTIONS = ["ASC", "DESC", "ASC NULLS FIRST", "ASC NULLS LAST",
              "DESC NULLS FIRST", "DESC NULLS LAST"]


def queries(table):
    for direction in DIRECTIONS:
        order = f"ORDER BY k {direction},id"
        yield direction, "order", f"SELECT id,k FROM {table} {order}"
        yield direction, "limit", f"SELECT id,k FROM {table} {order} LIMIT 1"
        if table.endswith("_mixed"):
            yield direction, "limit_ties", f"SELECT id,k FROM {table} {order} LIMIT 8"
            yield direction, "group", f"SELECT id,MAX(k) AS k FROM {table} GROUP BY id {order}"
            yield direction, "window", (f"SELECT id,k,RANK() OVER (ORDER BY k {direction}) AS rank,"
                                        f"ROW_NUMBER() OVER ({order}) AS position FROM {table} {order}")
    if table.endswith("_mixed"):
        yield "DESC NULLS LAST", "hidden", f"SELECT id FROM {table} ORDER BY k DESC NULLS LAST,id"
        yield "ASC NULLS LAST", "ordinal", f"SELECT id,k FROM {table} ORDER BY 2 ASC NULLS LAST,1"
        yield "DESC NULLS FIRST", "subquery", (f"SELECT id,k FROM (SELECT id,k FROM {table} "
                                               "ORDER BY k DESC NULLS FIRST,id LIMIT 8) ordered "
                                               "ORDER BY k DESC NULLS FIRST,id")


def assert_raw_roundtrip(spark, table, kind, rows):
    owner, method, width = (("Float", "floatToRawIntBits", 32) if kind == "float"
                            else ("Double", "doubleToRawLongBits", 64))
    frame = spark.sql(f"SELECT id,CASE WHEN k IS NULL THEN NULL ELSE "
                      f"java_method('java.lang.{owner}','{method}',k) END AS bits FROM {table}")
    actual = [[None if row.bits is None else
               f"{int(row.bits) & ((1 << width) - 1):0{width // 4}x}", str(row.id)]
              for row in frame.collect()]
    assert sorted(actual, key=lambda row: int(row[1])) == rows, (table, actual, rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path,
                        default=Path(__file__).with_name("scalar-float-ordering-spark.jsonl"))
    args = parser.parse_args()
    grouped_output = args.output.with_name("scalar-float-grouped-ordering-spark.jsonl")
    assert args.output.resolve() != grouped_output.resolve(), "fixture output paths must differ"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-scalar-float-ordering")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "3")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "inputs": []}
        definitions = []
        for table, kind, shape, key_type, values in inputs():
            if shape != "scalar" or not table.endswith(("_mixed", "_nulls", "_empty")):
                continue
            schema = types.StructType([
                types.StructField("k", key_type, True, {"purpose": "sort-payload"}),
                types.StructField("id", types.IntegerType(), False),
            ])
            rows = [(value, index) for index, value in enumerate(values)]
            frame = spark.createDataFrame(rows, schema).repartition(3)
            frame.createOrReplaceTempView(table)
            recorded = [encode(row, schema, input_value=True) for row in rows]
            assert_raw_roundtrip(spark, table, kind, recorded)
            assert frame.rdd.getNumPartitions() == 3
            metadata["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                       "rows": recorded, "spark_partitions": 3})
            definitions.extend((table, direction, label, sql)
                               for direction, label, sql in queries(table))
        records = [metadata]
        grouped_records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for table, direction, label, sql in definitions:
                expected = capture_query(spark, sql)
                assert "condition" not in expected, (sql, expected)
                ordered_ids = [str(row.id) for row in spark.sql(sql).select(functions.col("id")).collect()]
                output_records = grouped_records if label == "group" else records
                output_records.append({"id": f"{table}_{label}_{direction}", "table": table,
                                "direction": direction, "label": label, "ansi": ansi,
                                "sql": sql, "expected": expected, "ordered_ids": ordered_ids})
        assert len(records) - 1 == 204
        assert len(grouped_records) - 1 == 24
        assert len({(row["id"], row["ansi"])
                    for row in records[1:] + grouped_records[1:]}) == 228
        for output, captured in [
            (args.output, records),
            (grouped_output, grouped_records),
        ]:
            output.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n"
                                      for row in captured))
        print(json.dumps({"observations": len(records) - 1,
                          "grouped_observations": len(grouped_records) - 1,
                          "inputs": len(metadata["inputs"])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
