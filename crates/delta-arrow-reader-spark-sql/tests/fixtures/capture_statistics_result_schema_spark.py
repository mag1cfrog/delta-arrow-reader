"""Capture analyzed result schemas and rows independently with Spark 4.2.0."""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode


def queries(table):
    yield "min", f"SELECT MIN(k) AS result FROM {table}"
    yield "max", f"SELECT MAX(k) AS result FROM {table}"
    yield "counts", f"SELECT COUNT(*) AS rows,COUNT(k) AS present FROM {table}"
    yield "combined", f"SELECT MIN(k) AS low,MAX(k) AS high,COUNT(k) AS present FROM {table}"
    yield "arithmetic", f"SELECT MIN(k)+CAST(1 AS DOUBLE) AS result FROM {table}"
    yield "null_consumers", (f"SELECT COALESCE(MIN(k),CAST(-9 AS DOUBLE)) AS fallback,"
                             f"MIN(k) IS NULL AS missing FROM {table}")
    yield "scalar_subquery", f"SELECT (SELECT MIN(k) FROM {table}) AS result"
    yield "filtered_subquery", (f"SELECT low AS result FROM (SELECT MIN(k) AS low FROM {table}) t "
                                "WHERE low>CAST(0 AS DOUBLE)")


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-statistics-result-schema")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "inputs": [], "views": []}
        tables = []
        for shape, values in [("empty", []), ("one", [0]), ("many", [0, 1, 2])]:
            table = f"statistics_ids_{shape}"
            schema = types.StructType([
                types.StructField("id", types.IntegerType(), False, {"purpose": "statistics-row"}),
            ])
            rows = [(value,) for value in values]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            metadata["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                      "rows": [encode(row, schema, input_value=True) for row in rows]})
            for kind, expression in [("numeric", "CAST(1.5 AS DOUBLE)"),
                                     ("nulls", "CAST(NULL AS DOUBLE)"),
                                     ("zero", "CAST('-0.0' AS DOUBLE)"),
                                     ("nan", "CAST('NaN' AS DOUBLE)")]:
                name = f"statistics_{shape}_{kind}"
                sql = f"SELECT {expression} AS k FROM {table}"
                spark.sql(sql).createOrReplaceTempView(name)
                metadata["views"].append({"name": name, "sql": sql})
                tables.append(name)
        for nullable, values in [(False, [1.5, 2.5, 3.5]), (True, [1.5, None, 3.5])]:
            table = f"statistics_input_{str(nullable).lower()}"
            schema = types.StructType([
                types.StructField("k", types.DoubleType(), nullable, {"purpose": "statistics-value"}),
            ])
            rows = [(value,) for value in values]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            metadata["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                      "rows": [encode(row, schema, input_value=True) for row in rows]})
            tables.append(table)
        for name, sql in [
            ("statistics_unknown_bounds",
             "SELECT CASE WHEN id=0 THEN CAST(1.5 AS DOUBLE) ELSE CAST(2.5 AS DOUBLE) END AS k "
             "FROM statistics_ids_many"),
            ("statistics_inexact",
             "SELECT CAST(1.5 AS DOUBLE) AS k FROM statistics_ids_many WHERE id>0"),
        ]:
            spark.sql(sql).createOrReplaceTempView(name)
            metadata["views"].append({"name": name, "sql": sql})
            tables.append(name)
        definitions = [(f"{table}_{label}", sql) for table in tables for label, sql in queries(table)]
        definitions.extend((f"{table}_projection", f"SELECT * FROM {table}")
                           for table in ["statistics_ids_many", "statistics_input_false", "statistics_input_true"])
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                expected = capture_query(spark, sql)
                assert "schema" in expected, (name, expected)
                for row in expected["rows"]:
                    for index, field in enumerate(expected["schema"]["fields"]):
                        if field["type"] == "boolean" and row[index] is not None:
                            row[index] = row[index].lower()
                records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
        assert len({(record["id"], record["ansi"]) for record in records[1:]}) == len(records) - 1
        Path(__file__).with_name("statistics-result-schema-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records) - 1, "inputs": len(metadata["inputs"]),
                          "views": len(metadata["views"])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
