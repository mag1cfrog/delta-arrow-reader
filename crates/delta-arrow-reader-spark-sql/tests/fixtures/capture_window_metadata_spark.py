"""Capture Spark 4.2.0 metadata on projected inputs and derived window values."""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode


def inputs():
    child = types.StructType([
        types.StructField("value", types.IntegerType(), True, {"unit": "count"}),
    ])
    for shape, data_type, values in [
        ("integer", types.IntegerType(), [2, None, -3, 4]),
        ("double", types.DoubleType(), [-0.0, None, 1.5, -2.0]),
        ("struct", child, [(2,), None, (None,), (-3,)]),
        ("nested", types.ArrayType(child), [[(2,), None], None, [], [(None,), (-3,)]]),
    ]:
        for source, rows in [("mixed", values), ("nulls", [None, None]), ("empty", [])]:
            yield f"{shape}_{source}", data_type, rows


def queries(table):
    functions = [("lag", "LAG(k)"), ("lead", "LEAD(k)"),
                 ("first_value", "FIRST_VALUE(k)"), ("last_value", "LAST_VALUE(k)"),
                 ("nth_value", "NTH_VALUE(k,2)"), ("first", "FIRST(k)"),
                 ("last", "LAST(k)"), ("any_value", "ANY_VALUE(k)")]
    expressions = [f"{function} OVER (ORDER BY id)" for _, function in functions]
    aliased = ",".join(f"{expression} AS {name}_result"
                       for (name, _), expression in zip(functions, expressions))
    explicit = f"SELECT id,k,{aliased} FROM {table}"
    yield "explicit", explicit
    yield "subquery", f"SELECT * FROM ({explicit}) derived WHERE id>=0"
    # Keep generated metadata in the capture; #710 owns its alias marker parity.
    generated = f"SELECT id,k,{','.join(expressions)} FROM {table}"
    names = ",".join(["id", "k"] + [f"{name}_result" for name, _ in functions])
    yield "generated", f"SELECT * FROM ({generated}) derived({names})"
    ignored = ",".join(f"{function} IGNORE NULLS OVER (ORDER BY id) AS {name}_result"
                       for name, function in functions[:5])
    yield "ignore_nulls", f"SELECT id,k,{ignored} FROM {table}"
    if table.startswith("integer"):
        yield "defaults", (f"SELECT id,k,LAG(k,1,-99) OVER (ORDER BY id) AS lag_result,"
                           f"LEAD(k,1,-99) OVER (ORDER BY id) AS lead_result FROM {table}")


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-window-metadata")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "inputs": []}
        definitions = []
        for table, data_type, values in inputs():
            schema = types.StructType([
                types.StructField("id", types.IntegerType(), False),
                types.StructField("k", data_type, True, {"purpose": "window-key"}),
            ])
            rows = list(enumerate(values))
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            metadata["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                      "rows": [encode(row, schema, input_value=True) for row in rows]})
            definitions.extend((f"{table}_{label}", label, sql)
                               for label, sql in queries(table))
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, context, sql in definitions:
                expected = capture_query(spark, sql)
                assert "schema" in expected, (name, expected)
                records.append({"id": name, "context": context, "ansi": ansi,
                                "sql": sql, "expected": expected})
        assert len(records) == 103, len(records)
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records) - 1
        Path(__file__).with_name("window-metadata-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records) - 1, "inputs": len(metadata["inputs"])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
