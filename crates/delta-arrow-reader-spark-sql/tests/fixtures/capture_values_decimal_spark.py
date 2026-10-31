"""Capture scalar Decimal VALUES types and values with Spark 4.2.0/Java 21."""

import json
import os
import sys
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable

queries = []


def format_value(value, field):
    if value is None:
        return None
    if isinstance(value, Decimal):
        text = format(value, f".{field.dataType.scale}f")
        assert Decimal(text) == value
        return text
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def add_values(name, expressions):
    rows = ", ".join(f"({i}, {expr})" for i, expr in enumerate(expressions))
    queries.append({"id": name, "sql": f"SELECT v AS amount FROM VALUES {rows} t(id,v) ORDER BY id"})


fraction = "CAST('0.12345678901234567890123456789012345678' AS DECIMAL(38,38))"
for name, other in [
    ("tinyint", "CAST(0 AS TINYINT)"),
    ("smallint", "CAST(0 AS SMALLINT)"),
    ("int", "0"),
    ("bigint", "0L"),
    ("decimal", "CAST(0 AS DECIMAL(10,0))"),
    ("float", "CAST(2 AS FLOAT)"),
    ("double", "CAST(2 AS DOUBLE)"),
    ("null", "NULL"),
    ("typed_null_int", "CAST(NULL AS INT)"),
    ("typed_null_decimal", "CAST(NULL AS DECIMAL(10,0))"),
    ("same_type", "CAST(0 AS DECIMAL(38,38))"),
]:
    add_values(name, [fraction, other])
    add_values(name + "_reversed", [other, fraction])

for name, number in [
    ("positive_below", "0.000000000000000000000000000049"),
    ("positive_half", "0.000000000000000000000000000050"),
    ("positive_above", "0.000000000000000000000000000051"),
    ("negative_below", "-0.000000000000000000000000000049"),
    ("negative_half", "-0.000000000000000000000000000050"),
    ("negative_above", "-0.000000000000000000000000000051"),
    ("carry", "0.99999999999999999999999999999999999999"),
]:
    add_values("round_" + name, [f"CAST('{number}' AS DECIMAL(38,38))", "0"])

for name, expressions in [
    ("ordinary_precision", ["CAST(1.23 AS DECIMAL(4,2))", "2"]),
    ("same_precision", ["CAST(1.23 AS DECIMAL(4,2))", "CAST(2.45 AS DECIMAL(4,2))"]),
    ("different_precision", ["CAST(1.23 AS DECIMAL(4,2))", "CAST(2.456 AS DECIMAL(7,3))"]),
    ("zero_scale", ["CAST(999 AS DECIMAL(3,0))", "CAST(32767 AS SMALLINT)"]),
    ("integer_min", [fraction, "CAST('-9223372036854775808' AS BIGINT)"]),
    ("integer_max", [fraction, "9223372036854775807L"]),
    ("maximum_integral", [fraction, "CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))"]),
    ("maximum_integral_reversed", ["CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))", fraction]),
    ("three_inputs", [fraction, "0", "0L"]),
    ("three_inputs_reversed", ["0L", "0", fraction]),
    ("three_decimals", [fraction, "CAST(1 AS DECIMAL(10,0))", "CAST(2.125 AS DECIMAL(25,3))"]),
    ("three_decimals_reversed", ["CAST(2.125 AS DECIMAL(25,3))", "CAST(1 AS DECIMAL(10,0))", fraction]),
    ("null_before", ["NULL", fraction, "0"]),
    ("null_after", [fraction, "0", "NULL"]),
    ("only_typed_nulls", ["CAST(NULL AS DECIMAL(38,38))", "CAST(NULL AS INT)"]),
    ("all_null_control", ["NULL", "NULL"]),
    ("integer_control", ["1", "2L"]),
    ("string_control", ["'first'", "'second'"]),
    ("boolean_control", ["true", "false"]),
    ("float_control", ["CAST(1.25 AS FLOAT)", "CAST(2.5 AS FLOAT)"]),
    ("double_control", ["CAST(1.25 AS DOUBLE)", "CAST(2.5 AS DOUBLE)"]),
    ("invalid_string", ["CAST(1.23 AS DECIMAL(4,2))", "'2'"]),
    ("invalid_boolean", ["CAST(1.23 AS DECIMAL(4,2))", "true"]),
    ("invalid_date", ["CAST(1.23 AS DECIMAL(4,2))", "DATE'2024-01-01'"]),
    ("invalid_list", ["CAST(1.23 AS DECIMAL(4,2))", "ARRAY(1)"]),
    ("invalid_struct", ["CAST(1.23 AS DECIMAL(4,2))", "STRUCT(1 AS n)"]),
    ("invalid_map", ["CAST(1.23 AS DECIMAL(4,2))", "MAP('k',1)"]),
    ("invalid_map_reversed", ["MAP('k',1)", "CAST(1.23 AS DECIMAL(4,2))"]),
    ("bad_decimal_cast", ["CAST('bad' AS DECIMAL(4,2))", "1"]),
    ("bad_integer_cast", [fraction, "CAST('bad' AS INT)"]),
]:
    add_values(name, expressions)

queries += [
    {"id": "ragged_short", "sql": f"VALUES (1, {fraction}), (2)"},
    {"id": "ragged_long", "sql": f"VALUES ({fraction}), (2, 3)"},
    {"id": "bare_names", "sql": f"VALUES ({fraction}), (0)"},
    {"id": "multiple_decimal_columns", "sql": f"SELECT a AS amount, b AS whole FROM VALUES (0, {fraction}, CAST(1.23 AS DECIMAL(4,2))), (1, 0, 2L) t(id,a,b) ORDER BY id"},
    {"id": "fraction_remainder", "sql": f"SELECT v % CAST('0.1' AS DECIMAL(2,1)) AS amount FROM VALUES (0, {fraction}), (1, 0) t(id,v) ORDER BY id"},
    {"id": "explicit_remainder_control", "sql": f"SELECT v % CAST('0.1' AS DECIMAL(2,1)) AS amount FROM VALUES (0, {fraction}), (1, CAST(0 AS DECIMAL(38,38))) t(id,v) ORDER BY id"},
    {"id": "zero_remainder", "sql": f"SELECT v % 0 AS amount FROM VALUES (0, {fraction}), (1, 0) t(id,v) ORDER BY id"},
    {"id": "explicit_zero_remainder_control", "sql": f"SELECT v % 0 AS amount FROM VALUES (0, {fraction}), (1, CAST(0 AS DECIMAL(38,38))) t(id,v) ORDER BY id"},
    {"id": "historical_zero_remainder", "sql": "SELECT a % b AS r FROM VALUES (0,CAST(NULL AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))),(1,7,0) t(id,a,b) ORDER BY id"},
    {"id": "historical_zero_input_type", "sql": "SELECT b AS r FROM VALUES (0,CAST(NULL AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))),(1,7,0) t(id,a,b) ORDER BY id"},
    {"id": "historical_explicit_zero_control", "sql": "SELECT a % b AS r FROM VALUES (0,CAST(NULL AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))),(1,CAST(7 AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))) t(id,a,b) ORDER BY id"},
    {"id": "historical_divide_control", "ordered": False, "sql": "SELECT a/b AS r FROM VALUES (CAST(NULL AS DOUBLE),CAST(0 AS DOUBLE)),(5.0,2.0) t(a,b)"},
]

assert len({q["id"] for q in queries}) == len(queries)
spark = SparkSession.builder.master("local[1]").appName("dar-values-decimal-reference").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").config("spark.sql.session.timeZone", "UTC").getOrCreate()
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    records = [{
        "spark_version": spark.version,
        "timezone": spark.conf.get("spark.sql.session.timeZone"),
        "allow_precision_loss": spark.conf.get("spark.sql.decimalOperations.allowPrecisionLoss"),
        "retain_fraction_digits_first": spark.conf.get("spark.sql.legacy.decimal.retainFractionDigitsOnTruncate"),
    }]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for query in queries:
            stage = "planning"
            try:
                frame = spark.sql(query["sql"])
                schema = frame.schema.jsonValue()
                stage = "execution"
                rows = [[format_value(value, field) for value, field in zip(row, frame.schema.fields)] for row in frame.collect()]
                expected = {"status": "ok", "schema": schema, "rows": rows}
            except Exception as error:
                structured = error if hasattr(error, "getCondition") else error.java_exception
                condition = structured.getCondition()
                if condition not in {
                    "INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE",
                    "INVALID_INLINE_TABLE.NUM_COLUMNS_MISMATCH",
                    "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION",
                    "CAST_INVALID_INPUT", "REMAINDER_BY_ZERO",
                }:
                    raise
                expected = {"status": "error", "stage": stage, "condition": condition}
            records.append({**query, "ansi": ansi, "expected": expected})
    Path(__file__).with_name("values-decimal-spark.jsonl").write_text("\n".join(json.dumps(r) for r in records) + "\n")
    print(json.dumps({"observations": len(records)-1, "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
finally:
    spark.stop()
