"""Capture numeric-to-Decimal schemas, values and required errors with Spark 4.2.0."""
import decimal
import json
import os
import sys
import struct
from pathlib import Path

from pyspark.sql import SparkSession, types


os.environ["PYSPARK_PYTHON"] = sys.executable
inputs = []
for name, data_type, values in [
    ("byte", "byte", ["1", "-1", "127", "-128"]),
    ("short", "short", ["1", "-1", "32767", "-32768"]),
    ("int", "integer", ["1", "-1", "2147483647", "-2147483648"]),
    ("long", "long", ["1", "-1", "9223372036854775807", "-9223372036854775808"]),
    ("float", "float", ["1.25", "-2.5", "999.99", "-0.0"]),
    ("double", "double", ["1.25", "-2.5", "9999.999", "-0.0"]),
    ("decimal", "decimal(8,2)", ["1.25", "-1.25", "999999.99", "-999999.99"]),
]:
    for suffix, rows, nullable in [
        ("valid", values[:2], False),
        ("nullable", [None, *values[:2]], True),
        ("boundary", values, False),
    ]:
        inputs.append({"name": f"{name}_{suffix}", "type": data_type,
                       "nullable": nullable, "values": rows})
for kind in ["float", "double"]:
    inputs.append({"name": f"{kind}_special", "type": kind, "nullable": False,
                   "values": ["NaN", "Infinity", "-Infinity", "1.25"]})

for kind, data_type, values in [
    ("int_metadata", "integer", ["1", "2"]),
    ("decimal_metadata", "decimal(8,2)", [None, "1.25"]),
]:
    inputs.append({"name": kind, "type": data_type, "nullable": None in values,
                   "values": values, "metadata": {"note": "source-only"}})

queries = []
for source in inputs:
    name = source["name"]
    for precision, scale in [(2, 0), (10, 2), (20, 0), (38, 6)]:
        for cast in ["CAST", "TRY_CAST"]:
            queries.append((f"{name}_{cast.lower()}_{precision}_{scale}",
                            f"SELECT {cast}(v AS DECIMAL({precision},{scale})) AS r FROM {name}"))
    queries.append((f"{name}_decimal_function", f"SELECT decimal(v) AS r FROM {name}"))
    if name.endswith("boundary"):
        expression = "CAST(v AS DECIMAL(2,0))"
        grouped = (f"SELECT {expression} AS r FROM {name} GROUP BY {expression} "
                   f"HAVING {expression} IS NULL")
        for consumer, sql in [
            ("null_predicate", f"SELECT 1 AS r FROM {name} WHERE {expression} IS NULL"),
            ("exists", f"SELECT EXISTS({grouped}) AS r"),
            ("count", f"SELECT COUNT(*) AS r FROM ({grouped} LIMIT 1)t"),
            ("coalesce", f"SELECT COALESCE({expression},CAST(2 AS DECIMAL(10,0))) AS r FROM {name}"),
            ("arithmetic", f"SELECT {expression}+CAST(1 AS DECIMAL(10,0)) AS r FROM {name}"),
            ("empty", f"SELECT {expression} AS r FROM {name} WHERE false"),
            ("limit_zero", f"SELECT {expression} AS r FROM {name} LIMIT 0"),
            ("unreachable", f"SELECT CASE WHEN false THEN {expression} ELSE CAST(2 AS DECIMAL(10,0)) END AS r FROM {name}"),
            ("nonnull_coalesce", f"SELECT COALESCE(v,CAST(99999 AS DECIMAL(2,0))) AS r FROM {name}"),
        ]:
            queries.append((f"{name}_{consumer}", sql))
queries.extend([
    ("range_narrow", "SELECT CAST(id AS DECIMAL(2,0)) AS r FROM range(3)"),
    ("range_wide", "SELECT CAST(id AS DECIMAL(20,0)) AS r FROM range(3)"),
    ("values_coalesce", "SELECT COALESCE(k,CAST(99999 AS DECIMAL(2,0))) AS r FROM VALUES(0),(1)t(k) LIMIT 0"),
])
assert len(queries) == len({name for name, _ in queries})

spark = (SparkSession.builder.master("local[1]").appName("dar-numeric-decimal-cast")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    for source in inputs:
        data_type = types._parse_datatype_json_string(json.dumps(source["type"]))
        def decode(value):
            if value is None:
                return None
            if isinstance(data_type, types.DecimalType):
                return decimal.Decimal(value)
            if isinstance(data_type, (types.FloatType, types.DoubleType)):
                return float(value)
            return int(value)
        schema = types.StructType([types.StructField("v", data_type, source["nullable"], source.get("metadata", {}))])
        spark.createDataFrame([(decode(value),) for value in source["values"]], schema).createOrReplaceTempView(source["name"])
    records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": inputs}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            try:
                frame = spark.sql(sql)
                def encode(value, data_type):
                    if value is None:
                        return None
                    if isinstance(value, decimal.Decimal):
                        return format(value, "f")
                    if isinstance(data_type, (types.FloatType, types.DoubleType)):
                        return struct.pack("!f" if isinstance(data_type, types.FloatType) else "!d", value).hex()
                    return str(value).lower() if isinstance(value, bool) else str(value)
                rows = [[encode(value, field.dataType) for value, field in zip(row, frame.schema)]
                        for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                expected = {"schema": frame.schema.jsonValue(), "rows": rows}
            except Exception as error:
                message = str(error)
                assert "NUMERIC_VALUE_OUT_OF_RANGE" in message, message
                expected = {"condition": "NUMERIC_VALUE_OUT_OF_RANGE"}
            records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
    Path(__file__).with_name("numeric-decimal-cast-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1, "inputs": len(inputs)}))
finally:
    spark.stop()
