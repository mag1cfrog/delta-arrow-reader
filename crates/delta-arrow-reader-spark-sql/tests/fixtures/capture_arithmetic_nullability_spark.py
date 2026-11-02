"""Capture remainder and Decimal arithmetic schemas and error evaluation with Spark 4.2.0."""
import decimal
import json
import math
import os
import struct
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types


os.environ["PYSPARK_PYTHON"] = sys.executable
queries = []
widths = [("TINYINT", -128), ("SMALLINT", -32768), ("INT", -2147483648),
          ("BIGINT", -9223372036854775808)]
for kind, minimum in widths:
    left = f"CAST(id-1 AS {kind})"
    for name, expression in [
        ("positive", f"{left} % CAST(2 AS {kind})"),
        ("negative", f"{left} % CAST(-2 AS {kind})"),
        ("minimum", f"CAST(CASE WHEN id=0 THEN {minimum} ELSE 7 END AS {kind}) % CAST(-1 AS {kind})"),
        ("zero", f"{left} % CAST(0 AS {kind})"),
        ("nullable_left", f"CAST(CASE WHEN id=0 THEN NULL ELSE id END AS {kind}) % CAST(2 AS {kind})"),
        ("nullable_right", f"{left} % CAST(CASE WHEN id=0 THEN NULL ELSE 2 END AS {kind})"),
        ("masked_zero", f"CAST(CASE WHEN id=0 THEN NULL ELSE 7 END AS {kind}) % CAST(id AS {kind})"),
        ("null_left", f"CAST(NULL AS {kind}) % CAST(id AS {kind})"),
        ("null_right", f"{left} % CAST(NULL AS {kind})"),
    ]:
        queries.append((f"{kind.lower()}_{name}", f"SELECT {expression} AS r FROM range(3)"))
for left, _ in widths:
    for right, _ in widths:
        queries.append((f"mixed_{left.lower()}_{right.lower()}",
                        f"SELECT CAST(id+7 AS {left}) % CAST(id+1 AS {right}) AS r FROM range(3)"))
for kind in ["FLOAT", "DOUBLE", "DECIMAL(8,2)", "DECIMAL(38,18)"]:
    for name, expression in [
        ("positive", f"CAST(id-1 AS {kind}) % CAST(2 AS {kind})"),
        ("zero", f"CAST(id AS {kind}) % CAST(0 AS {kind})"),
        ("nullable", f"CAST(CASE WHEN id=0 THEN NULL ELSE 5 END AS {kind}) % CAST(id AS {kind})"),
        ("scalar_left", f"CAST(5 AS {kind}) % CAST(id+1 AS {kind})"),
    ]:
        queries.append((f"{kind}_{name}", f"SELECT {expression} AS r FROM range(3)"))
for kind in ["FLOAT", "DOUBLE"]:
    for name, expression in [
        ("special_left", f"CAST(v AS {kind}) % CAST(2 AS {kind})"),
        ("special_right", f"CAST(5 AS {kind}) % CAST(v AS {kind})"),
    ]:
        queries.append((f"{kind}_{name}", f"SELECT {expression} AS r FROM VALUES('-0.0'),('NaN'),('Infinity'),('-Infinity'),('2.5'),(NULL)t(v)"))
for name, expression in [("integer", "id % 0"), ("decimal", "CAST(id AS DECIMAL(8,2)) % CAST(0 AS DECIMAL(8,2))"),
                         ("float", "CAST(id AS DOUBLE) % 0D"), ("valid", "id % 2")]:
    grouped = f"SELECT {expression} AS v FROM range(3) GROUP BY {expression} HAVING {expression} IS NULL"
    for consumer, sql in [
        ("where", f"SELECT id AS r FROM range(3) WHERE {expression} IS NULL"),
        ("having", grouped),
        ("exists", f"SELECT EXISTS({grouped}) AS r"),
        ("count", f"SELECT COUNT(*) AS r FROM({grouped} LIMIT 1)t"),
        ("coalesce", f"SELECT COALESCE({expression},1) AS r FROM range(3)"),
        ("empty", f"SELECT {expression} AS r FROM range(0)"),
        ("limit_zero", f"SELECT {expression} AS r FROM range(3) LIMIT 0"),
        ("unreachable", f"SELECT CASE WHEN false THEN {expression} ELSE 2 END AS r FROM range(3)"),
        ("unused", f"SELECT 1 AS r FROM(SELECT {expression} AS v FROM range(3))t"),
    ]:
        queries.append((f"{name}_{consumer}", sql))
for index, (left, right) in enumerate([
    ("DECIMAL(8,2)", "DECIMAL(6,3)"),
    ("DECIMAL(38,0)", "DECIMAL(38,18)"),
    ("DECIMAL(38,18)", "DECIMAL(38,0)"),
    ("DECIMAL(8,2)", "BIGINT"),
    ("TINYINT", "DECIMAL(8,2)"),
]):
    for op_name, op in [("add", "+"), ("subtract", "-"), ("multiply", "*"), ("remainder", "%")]:
        queries.append((f"decimal_pair_{index}_{op_name}",
                        f"SELECT CAST(id+4 AS {left}) {op} CAST(id+1 AS {right}) AS r FROM range(3)"))
for index, op in enumerate(["%", "DIV", "/", "+", "-", "*"]):
    for side in ["left", "right"]:
        bad = "CAST(v AS DECIMAL(2,0))"
        null = "CAST(CAST(NULL AS INT) AS DECIMAL(2,0))"
        left, right = (null, bad) if side == "left" else (bad, null)
        queries.append((f"typed_null_{index}_{side}", f"SELECT {left} {op} {right} AS r FROM VALUES(999)t(v)"))
queries.extend([
    ("untyped_null_right", "SELECT id % NULL AS r FROM range(3)"),
    ("untyped_null_left", "SELECT NULL % id AS r FROM range(3)"),
    ("untyped_null_both", "SELECT NULL % NULL AS r"),
    ("divide_untyped_null_right", "SELECT id / NULL AS r FROM range(3)"),
    ("divide_untyped_null_left", "SELECT NULL / id AS r FROM range(3)"),
    ("divide_untyped_null_both", "SELECT NULL / NULL AS r"),
    ("literal", "SELECT 7 % 2 AS r"),
    ("local_values", "SELECT a % b AS r FROM VALUES(5,2),(-5,2),(0,2)t(a,b)"),
    ("local_having", "SELECT EXISTS(SELECT id % 0 AS r FROM VALUES(1),(2)t(id) GROUP BY id % 0 HAVING id % 0 IS NULL) AS r"),
    ("values_coalesce", "SELECT COALESCE(k,7 % 0) AS r FROM VALUES(0),(1)t(k) LIMIT 0"),
    ("decimal_overflow", "SELECT CAST(v AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0)) AS r FROM VALUES('99999999999999999999999999999999999999'),('1'),(NULL)t(v)"),
])
for kind, values in [("integer", "(1),(999)"), ("float", "(1.25D),(999.25D)"),
                     ("decimal", "(CAST(1 AS DECIMAL(38,0))),(CAST(999 AS DECIMAL(38,0)))")]:
    source = f"VALUES{values}t(v)"
    expression = "CAST(v AS DECIMAL(2,0))"
    for name, sql in [
        ("projection", f"SELECT {expression} AS r FROM {source}"),
        ("limit_zero", f"SELECT {expression} AS r FROM {source} LIMIT 0"),
        ("filtered_empty", f"SELECT {expression} AS r FROM {source} WHERE false"),
        ("unused", f"SELECT 1 AS r FROM(SELECT {expression} AS v FROM {source})t"),
        ("coalesce", f"SELECT COALESCE(CAST(1 AS DECIMAL(20,0)),{expression}) AS r FROM {source}"),
        ("unreachable", f"SELECT CASE WHEN false THEN {expression} ELSE CAST(1 AS DECIMAL(20,0)) END AS r FROM {source}"),
    ]:
        queries.append((f"local_{kind}_{name}", sql))
for name, dividend, divisor in [
    ("bigint_decimal_zero", "CAST(raise_error('dead') AS BIGINT)", "CAST(0 AS DECIMAL(8,2))"),
    ("bigint_integer_zero", "CAST(raise_error('dead') AS BIGINT)", "0"),
    ("decimal_function_zero", "decimal(raise_error('dead'))", "CAST(0 AS DECIMAL(8,2))"),
    ("decimal_cast_zero", "CAST(raise_error('dead') AS DECIMAL(8,2))", "CAST(0 AS DECIMAL(8,2))"),
    ("try_cast_zero", "CAST(raise_error('dead') AS BIGINT)", "TRY_CAST(0 AS DECIMAL(8,2))"),
    ("decimal_zero", "CAST(raise_error('dead') AS BIGINT)", "decimal(0)"),
    ("implicit_decimal_zero", "decimal(raise_error('dead'))", "0"),
    ("float_decimal_zero", "CAST(raise_error('dead') AS BIGINT)", "CAST(0D AS DECIMAL(8,2))"),
    ("decimal_nonzero", "CAST(raise_error('dead') AS BIGINT)", "CAST(1 AS DECIMAL(8,2))"),
    ("decimal_function_nonzero", "CAST(raise_error('dead') AS BIGINT)", "decimal(1)"),
]:
    queries.append((f"fallible_dividend_{name}", f"SELECT {dividend} % {divisor} AS r"))
assert len(queries) == len({name for name, _ in queries})

spark = (SparkSession.builder.master("local[1]").appName("dar-arithmetic-nullability")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    records = [{"spark_version": spark.version, "timezone": "UTC"}]
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
                        return "NaN" if math.isnan(value) else struct.pack("!f" if isinstance(data_type, types.FloatType) else "!d", value).hex()
                    return str(value).lower() if isinstance(value, bool) else str(value)
                rows = [[encode(value, field.dataType) for value, field in zip(row, frame.schema)]
                        for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                expected = {"schema": frame.schema.jsonValue(), "rows": rows}
            except Exception as error:
                message = str(error)
                conditions = [condition for condition in ["DIVIDE_BY_ZERO", "REMAINDER_BY_ZERO", "NUMERIC_VALUE_OUT_OF_RANGE", "USER_RAISED_EXCEPTION"] if condition in message]
                assert len(conditions) == 1, message
                expected = {"condition": conditions[0]}
            records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
    Path(__file__).with_name("arithmetic-nullability-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1}))
finally:
    spark.stop()
