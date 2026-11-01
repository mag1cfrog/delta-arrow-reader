"""Capture PIVOT floating keys with PySpark 4.2.0 and Java 21.

Both engines register the recorded FLOAT/DOUBLE bit patterns and nullable schemas.
Results retain names, types, nullability, metadata and every output value.
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
queries = []
for kind, encoding, nan_bits in [
    ("float", "!f", ["7fc01234", "ffc04321"]),
    ("double", "!d", ["7ff8000000001234", "fff8000000004321"]),
]:
    nan_a, nan_b = [struct.unpack(encoding, bytes.fromhex(bits))[0] for bits in nan_bits]
    data = {
        "positive": [(0.0, "a")],
        "negative": [(-0.0, "a")],
        "mixed": [(0.0, "a"), (-0.0, "a"), (0.0, "bbb"), (-0.0, None),
                  (nan_a, "nan"), (nan_b, "nan"), (float("inf"), "inf"),
                  (-float("inf"), "-inf"), (1.5, "value"), (-2.0, "nonzero"),
                  (None, "null"), (None, None), (0.0, "a")],
        "empty": [],
    }
    for name, rows in data.items():
        inputs.append({"name": f"{kind}_{name}", "type": kind, "rows": [
            [None if value is None else struct.pack(encoding, value).hex(), label]
            for value, label in rows]})

    def literal(value):
        return f"CAST('{value}' AS {kind})"

    for source in data:
        for sign in ["0.0", "-0.0"]:
            queries.append((f"{kind}_{source}_count_{sign}",
                            f"SELECT * FROM (SELECT k FROM {kind}_{source}) "
                            f"PIVOT(COUNT(*) FOR (k) IN ({literal(sign)}))"))
    for sign in ["0.0", "-0.0"]:
        values = ",".join([literal(sign), literal("NaN"), literal("Infinity"),
                           literal("-Infinity"), literal("1.5"), literal("-2"),
                           "NULL", f"{literal('9')} AS absent"])
        for label, aggregate in [
            ("numeric", "COUNT(*) AS n,SUM(LENGTH(label)) AS total,"
             "AVG(LENGTH(label)) AS mean,MIN(LENGTH(label)) AS low,"
             "MAX(LENGTH(label)) AS high,COUNT(DISTINCT label) AS distinct_labels"),
            ("text", "COUNT(*) AS n,MAX(label) AS text"),
            ("list", "SORT_ARRAY(COLLECT_LIST(label))"),
            ("key_values", "SORT_ARRAY(COLLECT_LIST(CAST(k AS STRING))) AS keys,MAX(label) AS text"),
        ]:
            queries.append((f"{kind}_{label}_{sign}",
                            f"SELECT * FROM {kind}_mixed PIVOT({aggregate} FOR (k) IN ({values}))"))
        queries.append((f"{kind}_grouped_{sign}",
                        f"SELECT * FROM {kind}_mixed PIVOT(COUNT(*) FOR (k) IN ({values}))"))
        for aggregate in ["COUNT(*)", "SUM(LENGTH(label))", "MAX(label)"]:
            multi_values = ",".join([
                f"({literal(sign)},'a') AS zero_a", f"({literal(sign)},NULL) AS zero_null",
                f"({literal('NaN')},'nan') AS nan", f"({literal('Infinity')},'inf') AS inf",
                f"({literal('1.5')},'value') AS finite", "(NULL,'null') AS null_key",
                "(NULL,NULL) AS all_null", f"({literal('9')},'a') AS absent"])
            queries.append((f"{kind}_multiple_{aggregate}_{sign}",
                            f"SELECT * FROM {kind}_mixed PIVOT({aggregate} FOR (k,label) "
                            f"IN ({multi_values}))"))
        queries.append((f"{kind}_two_float_keys_{sign}",
                        f"SELECT * FROM (SELECT k,-k AS other FROM {kind}_mixed) "
                        f"PIVOT(COUNT(*) FOR (k,other) IN (({literal(sign)},{literal(sign)}) AS zero,"
                        f"({literal('NaN')},{literal('NaN')}) AS nan,(NULL,NULL) AS nulls,"
                        f"({literal('1.5')},{literal('-1.5')}) AS finite))"))
    for aggregate in ["SUM(LENGTH(label))", "MAX(label)", "COLLECT_LIST(label)"]:
        queries.append((f"{kind}_empty_{aggregate}",
                        f"SELECT * FROM {kind}_empty PIVOT({aggregate} FOR (k) "
                        f"IN ({literal('-0.0')} AS zero,NULL AS nulls))"))
    for first, second in [("0.0", "-0.0"), ("-0.0", "0.0"), ("1.5", "1.5")]:
        queries.append((f"{kind}_duplicate_general_{first}_{second}",
                        f"SELECT * FROM {kind}_mixed PIVOT(COUNT(*) AS n,MAX(label) AS text "
                        f"FOR (k) IN ({literal(first)} AS a,{literal(second)} AS b))"))

assert len({name for name, _ in queries}) == len(queries)


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, (types.FloatType, types.DoubleType)):
        return "nan" if math.isnan(value) else struct.pack(
            "!f" if isinstance(data_type, types.FloatType) else "!d", value).hex()
    if isinstance(data_type, types.ArrayType):
        return [encode(item, data_type.elementType) for item in value]
    return str(value)


spark = (SparkSession.builder.master("local[1]").appName("dar-pivot-float-keys")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    for source in inputs:
        data_type = types.FloatType() if source["type"] == "float" else types.DoubleType()
        encoding = "!f" if source["type"] == "float" else "!d"
        rows = [(None if bits is None else struct.unpack(encoding, bytes.fromhex(bits))[0], label)
                for bits, label in source["rows"]]
        schema = types.StructType([types.StructField("k", data_type, True),
                                   types.StructField("label", types.StringType(), True)])
        spark.createDataFrame(rows, schema).createOrReplaceTempView(source["name"])
    records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": inputs}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            frame = spark.sql(sql)
            rows = [[encode(value, field.dataType) for value, field in zip(row, frame.schema)]
                    for row in frame.collect()]
            rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
            records.append({"id": name, "ansi": ansi, "sql": sql,
                            "expected": {"schema": frame.schema.jsonValue(), "rows": rows}})
    Path(__file__).with_name("pivot-float-keys-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1, "input_tables": len(inputs)}))
finally:
    spark.stop()
