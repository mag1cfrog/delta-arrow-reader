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
    comparison_pairs = [
        ("wide_double_decimal", "DOUBLE", "DECIMAL(38,18)", "12345678901234567890.123456789012345678", "0"),
        ("decimal_float_precision", "DECIMAL(20,9)", "FLOAT", "16777217", "16777216"),
        ("float_decimal_precision", "FLOAT", "DECIMAL(20,9)", "16777217", "16777217"),
        ("int_float_precision", "INT", "FLOAT", "16777217", "16777217"),
        ("float_int_precision", "FLOAT", "INT", "16777217", "16777217"),
        ("long_float_precision", "BIGINT", "FLOAT", "9007199254740993", "9007199254740992"),
        ("float_long_precision", "FLOAT", "BIGINT", "9007199254740993", "9007199254740992"),
        ("tiny_float", "TINYINT", "FLOAT", "127", "127"),
        ("float_tiny", "FLOAT", "TINYINT", "127", "127"),
        ("small_float", "SMALLINT", "FLOAT", "32767", "32767"),
        ("float_small", "FLOAT", "SMALLINT", "32767", "32767"),
        ("negative_int_float", "INT", "FLOAT", "-16777217", "-16777217"),
        ("negative_float_int", "FLOAT", "INT", "-16777217", "-16777217"),
        ("negative_decimal_double", "DECIMAL(38,18)", "DOUBLE", "-12345678901234567890.123456789012345678", "-12345678901234567890.123456789012345678"),
        ("fractional_decimal_float", "DECIMAL(10,2)", "FLOAT", "1.25", "1.25"),
        ("fractional_float_decimal", "FLOAT", "DECIMAL(10,2)", "1.25", "1.25"),
    ]
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
    for name, left_type, right_type, left, right in pairs + comparison_pairs:
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
        if (name, left_type, right_type, left, right) in comparison_pairs:
            yield f"{name}_equality", f"SELECT {a}={b} AS r"
            yield f"{name}_equality_columns", "SELECT a=b AS r" + source + " ORDER BY k"
    for float_type in ["FLOAT", "DOUBLE"]:
        for token in ["NaN", "Infinity", "-Infinity"]:
            floating = f"CAST('{token}' AS {float_type})"
            decimal = "CAST(0 AS DECIMAL(10,2))"
            for order, a, b in [("float_left", floating, decimal), ("decimal_left", decimal, floating)]:
                name = f"nonfinite_{float_type.lower()}_{token}_{order}"
                source = f" FROM VALUES (0,{a},{b}),(1,NULL,{b}),(2,{a},NULL) t(k,a,b)"
                yield f"{name}_values", f"SELECT NULLIF({a},{b}) AS r"
                yield f"{name}_columns", "SELECT NULLIF(a,b) AS r" + source + " ORDER BY k"
                yield f"{name}_empty", "SELECT NULLIF(a,b) AS r" + source + " WHERE false"
                yield f"{name}_equality", f"SELECT {a}={b} AS r"
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


def known_null_right_queries():
    source = " FROM VALUES (0,'bad'),(1,CAST(NULL AS STRING)),(2,'001') t(k,a)"
    for data_type in ["INT", "BIGINT", "DOUBLE", "DECIMAL(10,2)", "BOOLEAN",
                      "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "STRING"]:
        right = f"CAST(NULL AS {data_type})"
        for name, sql in [
            ("literal", f"SELECT NULLIF('bad',{right}) AS r"),
            ("column", f"SELECT NULLIF(a,{right}) AS r{source} ORDER BY k"),
            ("empty", f"SELECT NULLIF(a,{right}) AS r{source} WHERE false"),
        ]:
            yield f"known_right_null_{data_type}_{name}", sql
    for name, right in [
        ("untyped", "NULL"),
        ("try_cast", "TRY_CAST('bad' AS INT)"),
        ("cast_null", "CAST(CAST(NULL AS INT) AS BIGINT)"),
        ("nullif", "NULLIF(1,1)"),
        ("case", "CASE WHEN true THEN CAST(NULL AS INT) ELSE 1 END"),
        ("if", "IF(false,1,CAST(NULL AS INT))"),
        ("coalesce", "COALESCE(CAST(NULL AS INT),CAST(NULL AS INT))"),
    ]:
        yield f"known_right_null_{name}", f"SELECT NULLIF(a,{right}) AS r{source} ORDER BY k"
    for name, sql in [
        ("projection", "SELECT NULLIF(a,n) AS r FROM (SELECT id,'bad' AS a,CAST(NULL AS INT) AS n FROM range(2)) s ORDER BY id"),
        ("nested_projection", "SELECT NULLIF(a,n) AS r FROM (SELECT k AS j,a,n FROM (SELECT id AS k,'bad' AS a,CAST(NULL AS INT) AS n FROM range(2)) x) y ORDER BY j"),
        ("bad_left", "SELECT NULLIF(CAST('bad' AS INT),CAST(NULL AS DOUBLE)) AS r"),
        ("bad_left_column", "SELECT NULLIF(CAST(a AS INT),CAST(NULL AS DOUBLE)) AS r FROM VALUES ('bad') t(a)"),
        ("bad_left_empty", "SELECT NULLIF(CAST(a AS INT),CAST(NULL AS DOUBLE)) AS r FROM VALUES ('bad') t(a) WHERE false"),
        ("bad_local_values", "SELECT NULLIF(a,CAST(NULL AS DOUBLE)) AS r FROM VALUES (CAST('bad' AS INT)) t(a) WHERE false"),
        ("incomparable", "SELECT NULLIF(1,CAST(NULL AS ARRAY<INT>)) AS r"),
        ("incomparable_left", "SELECT NULLIF(ARRAY(1),CAST(NULL AS INT)) AS r"),
        ("dynamic_values", "SELECT NULLIF(a,b) AS r FROM VALUES (0,'bad',CAST(NULL AS INT)),(1,'001',CAST(NULL AS INT)) t(k,a,b) ORDER BY k"),
        ("dynamic_registered", "SELECT NULLIF(a,b) AS r FROM nullif_input ORDER BY k"),
        ("dynamic_registered_literal", "SELECT NULLIF('bad',b) AS r FROM nullif_input ORDER BY k"),
        ("dynamic_registered_empty", "SELECT NULLIF(a,b) AS r FROM nullif_input WHERE false"),
        ("known_registered", "SELECT NULLIF(a,CAST(NULL AS INT)) AS r FROM nullif_input ORDER BY k"),
        ("volatile_left", "SELECT r>=0 AND r<1 AS r FROM (SELECT NULLIF(rand(42),CAST(NULL AS DOUBLE)) AS r FROM range(3)) s"),
    ]:
        yield "known_right_null_" + name, sql


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-nullif-result-types")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    observations = []
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        spark.createDataFrame([(0, "bad", None), (1, "001", None), (2, None, None)],
                              "k INT, a STRING, b INT").createOrReplaceTempView("nullif_input")
        queries = [*result_queries(), *known_null_right_queries()]
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
                    if condition not in {"CAST_INVALID_INPUT", "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES",
                                         "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION"}:
                        raise
                    expected = {"status": "error", "condition": condition}
                observation = {"id": name, "sql": sql, "ansi": ansi, "expected": expected}
                if not ansi and name in {"known_right_null_DATE_column", "known_right_null_TIMESTAMP_column",
                                         "known_right_null_TIMESTAMP_NTZ_column"}:
                    observation["retained_difference"] = "string_temporal_ansi_629"
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
