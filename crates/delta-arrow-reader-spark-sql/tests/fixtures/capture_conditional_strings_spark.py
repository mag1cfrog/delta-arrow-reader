"""Capture STRING alternatives in conditional expressions with Spark 4.2.0."""

import itertools
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from capture_conditional_numeric_spark import conditional_expression, encode_value


PAIRS = [
    ("byte", "TINYINT", "127", "2147483648"),
    ("short", "SMALLINT", "32767", "2147483648"),
    ("int", "INT", "2147483647", "2147483648"),
    ("long", "BIGINT", "9223372036854775807", " -2147483649 "),
    ("decimal", "DECIMAL(10,2)", "1.23", "2.500"),
    ("float", "FLOAT", "1.25", "2.500"),
    ("double", "DOUBLE", "-0.0", "NaN"),
    ("boolean", "BOOLEAN", "false", "yes"),
    ("binary", "BINARY", "ascii", "Aé"),
]
FUNCTIONS = ["COALESCE", "IFNULL", "NVL", "IF", "CASE"]


def inputs():
    for name, data_type, value, text in PAIRS:
        for reverse in [False, True]:
            left_type, right_type = ("STRING", data_type) if reverse else (data_type, "STRING")
            left, right = (text, value) if reverse else (value, text)
            yield f"strings_{name}_{int(reverse)}", {
                "left_type": left_type, "right_type": right_type,
                "rows": [(0, None, right), (1, left, None), (2, None, None), (3, left, right)],
            }


def queries():
    for table, definition in inputs():
        literals = ",".join("(" + ",".join(
            "NULL" if value is None else str(value) if isinstance(value, int) else f"'{value}'"
            for value in row) + ")" for row in definition["rows"])
        local = (f"(SELECT k,CAST(l AS {definition['left_type']}) AS a,"
                 f"CAST(r AS {definition['right_type']}) AS b FROM VALUES {literals} t(k,l,r)) inputs")
        for function in FUNCTIONS:
            for registered in [False, True]:
                source = table if registered else local
                conditions = ["k % 2 = 0", "k % 2 = 1"] if function in {"IF", "CASE"} else ["true"]
                for selection, condition in enumerate(conditions):
                    expression = conditional_expression(function, "a", "b", condition)
                    yield {"id": f"{table}_{function.lower()}_{registered}_{selection}",
                           "sql": f"SELECT {expression} AS r FROM {source} ORDER BY k"}
            expression = conditional_expression(function, "a", "b", "true")
            yield {"id": f"{table}_{function.lower()}_empty", "sql": f"SELECT {expression} AS r FROM {table} WHERE false"}
    for function in FUNCTIONS:
        for data_type, value in [("INT", "1"), ("DECIMAL(10,2)", "1.23"), ("DOUBLE", "2.5"), ("BOOLEAN", "true")]:
            for name, left, right, condition in [
                ("required_error", f"CAST(NULL AS {data_type})", "'bad'", "false"),
                ("unused_error", f"CAST('{value}' AS {data_type})", "'bad'", "true"),
                ("both_null", f"CAST(NULL AS {data_type})", "CAST(NULL AS STRING)", "true"),
            ]:
                expression = conditional_expression(function, left, right, condition)
                yield {"id": f"{function.lower()}_{data_type}_{name}", "sql": f"SELECT {expression} AS r"}
        guarded = conditional_expression(function, "a", "b", "a IS NOT NULL")
        yield {"id": f"{function.lower()}_rowwise_unused_error",
               "sql": f"SELECT {guarded} AS r FROM VALUES (0,1,'bad'),(1,NULL,'2') t(k,a,b) ORDER BY k"}
        for sql_type, text in [("DOUBLE", "1e10"), ("DOUBLE", "1e-8"), ("DOUBLE", "-Infinity"), ("FLOAT", "-0.0"), ("DECIMAL(8,3)", "1.230")]:
            expression = conditional_expression(function, f"CAST('{text}' AS {sql_type})", "'unused'", "true")
            yield {"id": f"{function.lower()}_format_{sql_type}_{text}", "sql": f"SELECT {expression} AS r"}
        for text in ["1.2", "9223372036854775808", "  +42  "]:
            expression = conditional_expression(function, "CAST(NULL AS INT)", f"'{text}'", "false")
            yield {"id": f"{function.lower()}_integer_text_{text}", "sql": f"SELECT {expression} AS r"}
        for expression, label in [("NULL", "null"), ("CAST(NULL AS STRING)", "string_null")]:
            result = conditional_expression(function, expression, "'value'", "false")
            yield {"id": f"{function.lower()}_{label}", "sql": f"SELECT {result} AS r"}
    for group, arguments in enumerate([
        ["CAST(NULL AS INT)", "CAST(NULL AS DOUBLE)", "'2.5'"],
        ["CAST(NULL AS INT)", "CAST(NULL AS BOOLEAN)", "'true'"],
        ["CAST(NULL AS INT)", "CAST(NULL AS BINARY)", "'xy'"],
        ["CAST(NULL AS INT)", "DATE '2026-01-01'", "'text'"],
    ]):
        for order, values in enumerate(itertools.permutations(arguments)):
            yield {"id": f"variadic_{group}_{order}", "sql": f"SELECT COALESCE({','.join(values)}) AS r"}


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-conditional-strings")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        registrations = dict(inputs())
        for name, definition in registrations.items():
            frame = spark.createDataFrame(definition["rows"], StructType([
                StructField("k", IntegerType(), False), StructField("l", StringType()), StructField("r", StringType()),
            ]))
            frame.selectExpr("k", f"CAST(l AS {definition['left_type']}) AS a",
                             f"CAST(r AS {definition['right_type']}) AS b").createOrReplaceTempView(name)
        records = []
        for case in queries():
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                try:
                    frame = spark.sql(case["sql"])
                    rows = [[str(v).lower() if isinstance(v, bool) else bytes(v).hex() if isinstance(v, (bytes, bytearray))
                             else encode_value(v, field) for v, field in zip(row, frame.schema.fields)] for row in frame.collect()]
                    expected = {"status": "ok", "schema": frame.schema.jsonValue(), "rows": rows}
                except Exception as error:
                    condition = error.getCondition()
                    if condition not in {"CAST_INVALID_INPUT", "DATATYPE_MISMATCH.DATA_DIFF_TYPES"}:
                        raise
                    expected = {"status": "error", "condition": condition}
                records.append({**case, "ansi": ansi, "timezone": "UTC", "float_encoding": "ieee754_bits", "expected": expected})
        assert len({(r["id"], r["ansi"]) for r in records}) == len(records)
        metadata = {"spark_version": spark.version, "registered_inputs": registrations}
        Path(__file__).with_name("conditional-strings-spark.jsonl").write_text(
            "\n".join(json.dumps(r, separators=(",", ":")) for r in [metadata, *records]) + "\n")
        print(json.dumps({"observations": len(records), "errors": sum(r["expected"]["status"] == "error" for r in records)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
