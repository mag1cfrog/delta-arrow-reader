"""Capture FLOAT/DOUBLE set keys with PySpark 4.2.0 and Java 21.

Input bits retain zero signs and NaN payloads. Finite output bits are exact;
NaN outputs compare as SQL NaNs because Spark's Python transport may canonicalize.
Set comparisons equate zero signs; raw captures retain Spark's representative bits.
"""
import json
import math
import os
import struct
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types


os.environ["PYSPARK_PYTHON"] = sys.executable
inputs = []
pairs = []
queries = []
for kind, encoding, nan_bits in [
    ("float", "!f", ["7fc01234", "ffc04321", "7fc00001"]),
    ("double", "!d", ["7ff8000000001234", "fff8000000004321", "7ff8000000000001"]),
]:
    nan_a, nan_b, nan_c = [struct.unpack(encoding, bytes.fromhex(bits))[0] for bits in nan_bits]
    data = {
        "positive": [(0.0, "same"), (0.0, "same")],
        "negative": [(-0.0, "same")],
        "nan_left": [(nan_a, "same"), (nan_b, "same")],
        "nan_right": [(nan_c, "same")],
        "mixed_left": [(0.0, "same"), (-0.0, "same"), (0.0, "other"),
                       (nan_a, "same"), (nan_b, "same"), (None, "same"),
                       (None, "same"), (float("inf"), "same"), (-float("inf"), None),
                       (1.5, "same"), (-2.0, None)],
        "mixed_right": [(-0.0, "same"), (nan_c, "same"), (None, "same"),
                        (float("inf"), "same"), (5.0, None)],
        "empty": [],
    }
    for name, rows in data.items():
        inputs.append({"name": f"{kind}_{name}", "type": kind, "rows": [
            [None if value is None else struct.pack(encoding, value).hex(), label]
            for value, label in rows]})
    for label, left, right in [
        ("zero", "positive", "negative"), ("nan", "nan_left", "nan_right"),
        ("mixed", "mixed_left", "mixed_right"), ("empty", "mixed_left", "empty"),
        ("both_empty", "empty", "empty"),
    ]:
        for columns in ["k", "k,label"]:
            pairs.append((f"{kind}_{label}_{columns.replace(',', '_')}",
                          f"SELECT {columns} FROM {kind}_{left}",
                          f"SELECT {columns} FROM {kind}_{right}"))
    queries += [
        (f"{kind}_plain", f"SELECT k FROM {kind}_mixed_left", "exact_bits"),
        (f"{kind}_computed", f"SELECT -k AS k FROM {kind}_negative", "exact_bits"),
    ]
pairs += [
    ("mixed_width", "SELECT k FROM float_mixed_left", "SELECT k FROM double_mixed_right"),
    ("mixed_integer", "SELECT k FROM float_negative", "SELECT CAST(0 AS INT) AS k"),
    ("mixed_string", "SELECT k FROM double_negative", "SELECT '0.0' AS k"),
    ("computed", "SELECT -k AS k FROM double_positive", "SELECT k FROM double_positive"),
    ("string_output", "SELECT CAST(k AS STRING) AS k FROM double_negative",
     "SELECT CAST(k AS STRING) AS k FROM double_positive"),
    ("literal", "SELECT CAST('-0.0' AS DOUBLE) AS k", "SELECT CAST('0.0' AS DOUBLE) AS k"),
    ("literal_same", "SELECT CAST('-0.0' AS DOUBLE) AS k", "SELECT CAST('-0.0' AS DOUBLE) AS k"),
]
for operation in ["UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]:
    for name, left, right in pairs:
        for reverse in [False, True]:
            first, second = (right, left) if reverse else (left, right)
            queries.append((f"{name}_{operation.lower().replace(' ', '_')}_{reverse}",
                            f"{first} {operation} {second}",
                            "exact_bits" if operation == "UNION ALL" else "float_equality"))
queries += [
    ("literal_intersect_string", "SELECT CAST(k AS STRING) AS k FROM "
     "(SELECT CAST('-0.0' AS DOUBLE) AS k INTERSECT SELECT 0.0D AS k)", "exact_bits"),
    ("literal_union_string", "SELECT CAST(k AS STRING) AS k FROM "
     "(SELECT CAST('-0.0' AS DOUBLE) AS k UNION SELECT CAST('-0.0' AS DOUBLE) AS k)",
     "exact_bits"),
]
assert len({name for name, _, _ in queries}) == len(queries)


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, (types.FloatType, types.DoubleType)):
        return "nan" if math.isnan(value) else struct.pack(
            "!f" if isinstance(data_type, types.FloatType) else "!d", value).hex()
    return str(value)


spark = (SparkSession.builder.master("local[1]").appName("dar-set-operation-float-keys")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    for source in inputs:
        data_type = types.FloatType() if source["type"] == "float" else types.DoubleType()
        encoding = "!f" if source["type"] == "float" else "!d"
        rows = [(None if value is None else struct.unpack(encoding, bytes.fromhex(value))[0], label)
                for value, label in source["rows"]]
        schema = types.StructType([types.StructField("k", data_type, True),
                                   types.StructField("label", types.StringType(), True)])
        spark.createDataFrame(rows, schema).createOrReplaceTempView(source["name"])
    records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": inputs}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql, comparison in queries:
            frame = spark.sql(sql)
            schema = frame.schema.jsonValue()
            rows = [[encode(value, field.dataType) for value, field in zip(row, frame.schema)]
                    for row in frame.collect()]
            rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
            records.append({"id": name, "ansi": ansi, "sql": sql, "comparison": comparison,
                            "expected": {"status": "ok", "schema": schema, "rows": rows}})
    Path(__file__).with_name("set-operation-float-keys-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1, "input_tables": len(inputs)}))
finally:
    spark.stop()
