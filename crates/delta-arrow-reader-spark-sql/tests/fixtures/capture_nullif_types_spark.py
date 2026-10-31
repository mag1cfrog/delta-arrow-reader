"""Capture NULLIF result preservation against independent Spark 4.2.0.

Use the Python/Java environment described in README.md. Float output uses IEEE
bits so a wider comparison cannot hide changes to the original value.
"""

import json
import math
import os
from pathlib import Path
import struct
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, FloatType


def result_queries():
    pairs = [
        ("int_double", "INT", "DOUBLE", "1", "2"),
        ("double_int", "DOUBLE", "INT", "1", "2"),
        ("large_long_double", "BIGINT", "DOUBLE", "9007199254740993", "0"),
        ("rounded_long_double", "BIGINT", "DOUBLE", "9007199254740993", "9007199254740992"),
        ("float_double", "FLOAT", "DOUBLE", "1.25", "2.5"),
        ("wide_float_double", "FLOAT", "DOUBLE", "16777217", "16777217"),
        ("negative_zero_float_double", "FLOAT", "DOUBLE", "'-0.0'", "1"),
        ("int_decimal", "INT", "DECIMAL(10,2)", "1", "2.01"),
        ("decimal_int", "DECIMAL(10,2)", "INT", "1.25", "2"),
        ("decimal_decimal", "DECIMAL(10,2)", "DECIMAL(15,3)", "1.25", "2.125"),
        ("wide_decimal_double", "DECIMAL(38,18)", "DOUBLE", "12345678901234567890.123456789012345678", "0"),
        ("string_int", "STRING", "INT", "'001'", "2"),
        ("int_string", "INT", "STRING", "1", "'2'"),
        ("boolean", "BOOLEAN", "BOOLEAN", "true", "false"),
        ("date", "DATE", "DATE", "'2026-01-02'", "'2026-01-03'"),
    ]
    for name, left_type, right_type, left, right in pairs:
        a, b = f"CAST({left} AS {left_type})", f"CAST({right} AS {right_type})"
        null_left, null_right = f"CAST(NULL AS {left_type})", f"CAST(NULL AS {right_type})"
        for case, args in [
            ("values", f"{a},{b}"),
            ("same_literal", f"{a},CAST({left} AS {right_type})"),
            ("null_left", f"{null_left},{b}"),
            ("null_right", f"{a},{null_right}"),
            ("both_null", f"{null_left},{null_right}"),
        ]:
            yield f"{name}_{case}", f"SELECT NULLIF({args}) AS r"
        source = f" FROM VALUES (0,{a},{b}),(1,{null_left},{b}),(2,{a},{null_right}) t(k,a,b)"
        yield f"{name}_columns", "SELECT NULLIF(a,b) AS r" + source + " ORDER BY k"
        yield f"{name}_empty", "SELECT NULLIF(a,b) AS r" + source + " WHERE false"
    for name, arguments, source in [
        ("implicit_column_conversion", "a,b", " FROM VALUES (0,CAST(NULL AS INT),'bad'),(1,8,'7') t(k,a,b) ORDER BY k"),
        ("implicit_all_null_columns", "a,b", " FROM VALUES (0,CAST(NULL AS INT),'bad'),(1,NULL,'bad') t(k,a,b) ORDER BY k"),
        ("implicit_literal_conversion", "a,'bad'", " FROM VALUES (CAST(NULL AS INT)),(NULL) t(a)"),
        ("implicit_empty_columns", "a,b", " FROM VALUES (0,CAST(NULL AS INT),'bad'),(1,8,'7') t(k,a,b) WHERE false"),
        ("incomparable_scalar", "1,ARRAY(1)", ""),
        ("incomparable_typed_null", "CAST(NULL AS INT),ARRAY(1)", ""),
        ("untyped_nulls", "NULL,NULL", ""),
        ("untyped_left", "NULL,1", ""),
        ("untyped_right", "1,NULL", ""),
        ("untyped_columns", "a,b", " FROM VALUES (NULL,NULL),(NULL,NULL) t(a,b)"),
        ("untyped_empty", "a,b", " FROM VALUES (NULL,NULL) t(a,b) WHERE false"),
        ("null_left_bad_right", "CAST(NULL AS INT),CAST('bad' AS DOUBLE)", ""),
        ("live_bad_right", "1,CAST('bad' AS DOUBLE)", ""),
        ("bad_left_null_right", "CAST('bad' AS INT),CAST(NULL AS DOUBLE)", ""),
        ("rowwise_bad_right", "a,CAST(b AS DOUBLE)", " FROM VALUES (0,CAST(NULL AS INT),'bad'),(1,7,'7'),(2,8,'7'),(3,NULL,'bad') t(k,a,b) ORDER BY k"),
    ]:
        yield name, f"SELECT NULLIF({arguments}) AS r{source}"


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, (FloatType, DoubleType)):
        return "nan" if math.isnan(value) else struct.pack("!f" if isinstance(data_type, FloatType) else "!d", value).hex()
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-nullif-result-types")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    observations = []
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        queries = list(result_queries())
        assert len(dict(queries)) == len(queries)
        for name, sql in queries:
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                try:
                    frame = spark.sql(sql)
                    schema = frame.schema.jsonValue()
                    data_type = frame.schema.fields[0].dataType
                    expected = {"status": "ok", "schema": schema,
                                "rows": [[encode(row[0], data_type)] for row in frame.collect()]}
                except Exception as error:
                    condition = getattr(error, "getCondition", lambda: None)()
                    if condition not in {"CAST_INVALID_INPUT", "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES"}:
                        raise
                    expected = {"status": "error", "condition": condition}
                assert (expected["status"] == "error") == (name.startswith("incomparable_") or (ansi and name in {"live_bad_right", "bad_left_null_right"})), (name, ansi, expected)
                observation = {"id": name, "sql": sql, "ansi": ansi, "expected": expected}
                if name == "wide_decimal_double_same_literal":
                    observation["retained_difference"] = "nullif_comparison_coercion"
                observations.append(observation)
        metadata = {"spark_version": spark.version, "timezone": "UTC",
                    "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version")}
        Path(__file__).with_name("nullif-types-spark.jsonl").write_text(
            "\n".join(json.dumps(record, separators=(",", ":")) for record in [metadata, *observations]) + "\n")
        print(json.dumps({"observations": len(observations), "errors": sum(x["expected"]["status"] == "error" for x in observations)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
