"""Capture numeric conditional result types and exact values with Spark 4.2.0."""

import itertools
import json
import math
import os
from pathlib import Path
import struct
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


PAIRS = [
    ("same_decimal", ("DECIMAL(10,2)", "1.23"), ("DECIMAL(10,2)", "2.45")),
    ("mixed_decimal", ("DECIMAL(4,2)", "1.23"), ("DECIMAL(7,3)", "2.456")),
    ("decimal_byte", ("DECIMAL(4,2)", "1.23"), ("TINYINT", "127")),
    ("decimal_short", ("DECIMAL(4,2)", "1.23"), ("SMALLINT", "32767")),
    ("decimal_int", ("DECIMAL(4,2)", "1.23"), ("INT", "2147483647")),
    ("scale_cap", ("DECIMAL(38,20)", "1.23456789012345678901"), ("BIGINT", "9223372036854775807")),
    ("full_precision", ("DECIMAL(38,38)", "0.12345678901234567890123456789012345678"), ("DECIMAL(38,0)", "99999999999999999999999999999999999999")),
    ("rounding", ("DECIMAL(38,20)", "-1.23456789012345678955"), ("DECIMAL(38,0)", "0")),
    ("decimal_float", ("DECIMAL(20,0)", "9007199254740993"), ("FLOAT", "1.25")),
    ("decimal_double", ("DECIMAL(10,2)", "1.23"), ("DOUBLE", "2.5")),
    ("float_byte", ("FLOAT", "1.25"), ("TINYINT", "127")),
    ("float_short", ("FLOAT", "1.25"), ("SMALLINT", "32767")),
    ("float_int", ("FLOAT", "1.25"), ("INT", "16777217")),
    ("float_long", ("FLOAT", "1.25"), ("BIGINT", "9007199254740993")),
    ("float_double", ("FLOAT", "-0.0"), ("DOUBLE", "NaN")),
    ("byte_short", ("TINYINT", "-128"), ("SMALLINT", "32767")),
    ("short_int", ("SMALLINT", "-32768"), ("INT", "2147483647")),
    ("int_long", ("INT", "-2147483648"), ("BIGINT", "9223372036854775807")),
]


def conditional_expression(function, left, right, condition="k % 2 = 0"):
    if function == "IF":
        return f"IF({condition},{left},{right})"
    if function == "CASE":
        return f"CASE WHEN {condition} THEN {left} ELSE {right} END"
    return f"{function}({left},{right})"


def queries():
    for name, first, second in PAIRS:
        for reverse in [False, True]:
            left, right = (second, first) if reverse else (first, second)
            source = (f"(SELECT k,CAST(l AS {left[0]}) AS a,CAST(r AS {right[0]}) AS b "
                      f"FROM VALUES (0,NULL,'{right[1]}'),(1,'{left[1]}',NULL),"
                      f"(2,NULL,NULL),(3,'{left[1]}','{right[1]}') t(k,l,r)) inputs")
            for function in ["COALESCE", "IFNULL", "NVL", "IF", "CASE"]:
                expression = conditional_expression(function, "a", "b")
                for registered in [False, True]:
                    query_source = f"registered_{name}_{int(reverse)}" if registered else source
                    yield {
                        "id": f"{name}_{int(reverse)}_{function.lower()}_{registered}",
                        "sql": f"SELECT {expression} AS r FROM {query_source} ORDER BY k",
                    }
                    if function in {"IF", "CASE"}:
                        selected = conditional_expression(function, "a", "b", "k % 2 = 1")
                        yield {
                            "id": f"{name}_{int(reverse)}_{function.lower()}_{registered}_nonnull_branches",
                            "sql": f"SELECT {selected} AS r FROM {query_source} ORDER BY k",
                        }
                if name in {"scale_cap", "float_int"}:
                    yield {
                        "id": f"{name}_{int(reverse)}_{function.lower()}_empty",
                        "sql": f"SELECT {expression} AS r FROM {source} WHERE false",
                    }
                if name in {"same_decimal", "scale_cap", "float_int"}:
                    for select_left in [False, True]:
                        first_arg = f"CAST('{left[1]}' AS {left[0]})" if select_left else f"CAST(NULL AS {left[0]})"
                        second_arg = f"CAST('{right[1]}' AS {right[0]})"
                        expression = conditional_expression(function, first_arg, second_arg, str(select_left).lower())
                        yield {
                            "id": f"{name}_{int(reverse)}_{function.lower()}_literal_{select_left}",
                            "sql": f"SELECT {expression} AS r",
                        }
    for index, types in enumerate(itertools.permutations(["DECIMAL(38,20)", "DECIMAL(38,10)", "BIGINT"])):
        arguments = [f"CAST(NULL AS {t})" for t in types[:-1]] + [f"CAST('1' AS {types[-1]})"]
        yield {"id": f"variadic_{index}", "sql": f"SELECT COALESCE({','.join(arguments)}) AS r"}
    for function in ["COALESCE", "IFNULL", "NVL", "IF", "CASE"]:
        for name, left, right in [
            ("untyped_null", "NULL", "CAST(1.23 AS DECIMAL(10,2))"),
            ("typed_null", "CAST(NULL AS INT)", "CAST(NULL AS DECIMAL(10,2))"),
            ("both_null", "NULL", "NULL"),
            ("unused_error", "CAST(1.23 AS DECIMAL(10,2))", "CAST('bad' AS INT)"),
            ("required_error", "CAST(NULL AS DECIMAL(10,2))", "CAST('bad' AS INT)"),
        ]:
            expression = conditional_expression(function, left, right, str(name != "required_error").lower())
            yield {"id": f"{function.lower()}_{name}", "sql": f"SELECT {expression} AS r"}
    yield {
        "id": "replay_decimal_division_after_case",
        "sql": "SELECT CAST('bad' AS DECIMAL(10,2)) / n AS r FROM (SELECT CASE WHEN id%2=0 THEN CAST(NULL AS DECIMAL(10,2)) ELSE 2 END AS n FROM range(1)) t",
    }


def encode_value(value, field):
    if value is None:
        return None
    if field.dataType.typeName() == "decimal":
        return format(value, f".{field.dataType.scale}f")
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        return struct.pack(">f" if field.dataType.typeName() == "float" else ">d", value).hex()
    return str(value)


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-conditional-numeric")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        registrations = {}
        for name, first, second in PAIRS:
            for reverse in [False, True]:
                left, right = (second, first) if reverse else (first, second)
                table = f"registered_{name}_{int(reverse)}"
                rows = [(0, None, right[1]), (1, left[1], None), (2, None, None), (3, left[1], right[1])]
                frame = spark.createDataFrame(rows, StructType([
                    StructField("k", IntegerType(), False), StructField("l", StringType()), StructField("r", StringType()),
                ]))
                frame.selectExpr("k", f"CAST(l AS {left[0]}) AS a", f"CAST(r AS {right[0]}) AS b").createOrReplaceTempView(table)
                registrations[table] = {"left_type": left[0], "right_type": right[0], "rows": rows}
        records = []
        for case in queries():
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                try:
                    frame = spark.sql(case["sql"])
                    expected = {"status": "ok", "schema": frame.schema.jsonValue(),
                                "rows": [[encode_value(value, field) for value, field in zip(row, frame.schema.fields)] for row in frame.collect()]}
                except Exception as error:
                    condition = error.getCondition()
                    if condition not in {"CAST_INVALID_INPUT", "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION"}:
                        raise
                    expected = {"status": "error", "condition": condition}
                records.append({**case, "ansi": ansi, "timezone": "UTC", "float_encoding": "ieee754_bits", "expected": expected})
        assert len({(r['id'], r['ansi']) for r in records}) == len(records)
        metadata = {"spark_version": spark.version, "registered_inputs": registrations,
                    "retain_fraction_digits_first": spark.conf.get("spark.sql.legacy.decimal.retainFractionDigitsOnTruncate")}
        Path(__file__).with_name("conditional-numeric-spark.jsonl").write_text(
            "\n".join(json.dumps(r, separators=(",", ":")) for r in [metadata, *records]) + "\n")
        print(json.dumps({"observations": len(records), "errors": sum(r["expected"]["status"] == "error" for r in records)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
