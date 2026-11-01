"""Capture Spark STRING comparison and NULLIF results.

Use the Python/Java environment described in README.md. Hex-encoded strings
preserve control bytes without embedding them in SQL source files.
"""

import json
import math
import os
from pathlib import Path
import struct
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, DoubleType


def string_comparison_queries():
    pairs = [
        ("int_large", "2147483648", "INT", "1"),
        ("long_max", "9223372036854775807", "BIGINT", "9223372036854775807"),
        ("long_overflow", "9223372036854775808", "BIGINT", "1"),
        ("tiny_overflow", "128", "TINYINT", "1"),
        ("fraction", "1.5", "INT", "1"),
        ("bad", "bad", "INT", "1"),
        ("scientific", "1e2", "INT", "100"),
        ("leading_zero", " +001 ", "INT", "2"),
        ("controls", "\x00\x7f1\x1f", "INT", "1"),
        ("unicode_space", "\u00a01\u00a0", "INT", "1"),
        ("float_precision", "16777217", "FLOAT", "16777217"),
        ("double_suffix", "1.25f", "DOUBLE", "1.25"),
        ("double_hex", "0x1.4p2", "DOUBLE", "5"),
        ("float_nan", "NaN", "FLOAT", "'NaN'"),
        ("double_nan", "-NaN", "DOUBLE", "'NaN'"),
        ("double_inf", "+INF", "DOUBLE", "'Infinity'"),
        ("decimal_scale", "1.125", "DECIMAL(10,2)", "1.12"),
        ("decimal_integer", "1.5", "DECIMAL(10,0)", "2"),
        ("decimal_wide", "12345678901234567890.123456789012345678", "DECIMAL(38,18)",
         "12345678901234567890.123456789012345678"),
        ("boolean_true", "true", "BOOLEAN", "true"),
        ("boolean_false", "f", "BOOLEAN", "false"),
        ("boolean_on", "on", "BOOLEAN", "true"),
        ("boolean_controls", "\x7fTRUE\x00", "BOOLEAN", "true"),
        ("boolean_unicode", "\u00a0true\u00a0", "BOOLEAN", "true"),
    ]
    for name, text, data_type, value in pairs:
        encoded = text.encode().hex()
        left = f"decode(unhex('{encoded}'),'UTF-8')"
        right = f"CAST({value} AS {data_type})"
        source = (f" FROM (SELECT k,decode(unhex(h),'UTF-8') AS a,b FROM VALUES "
                  f"(0,'{encoded}',{right}),(1,CAST(NULL AS STRING),{right}),"
                  f"(2,'{encoded}',CAST(NULL AS {data_type})) t(k,h,b)) s")
        for label, expression in [("equal", f"{left}={right}"),
                                  ("equal_reverse", f"{right}={left}"),
                                  ("left", f"NULLIF({left},{right})"),
                                  ("right", f"NULLIF({right},{left})")]:
            yield name + "_" + label, f"SELECT {expression} AS r"
        for label, expression in [("equal", "a=b"), ("equal_reverse", "b=a"), ("left", "NULLIF(a,b)"),
                                  ("right", "NULLIF(b,a)")]:
            yield name + "_" + label + "_columns", f"SELECT {expression} AS r" + source + " ORDER BY k"
            yield name + "_" + label + "_empty", f"SELECT {expression} AS r" + source + " WHERE false"
    for label, data_type, right in [("integer", "INT", "1"), ("floating", "DOUBLE", "1.25"),
                                    ("boolean", "BOOLEAN", "false")]:
        for operator in ["=", "!=", "<", "<=", ">", ">=", "<=>"]:
            for reverse in [False, True]:
                left_column, right_column = ("b", "a") if reverse else ("a", "b")
                yield label + "_" + operator + "_" + str(reverse), (
                    f"SELECT {left_column}{operator}{right_column} AS r FROM VALUES "
                    f"(0,'1',CAST({right} AS {data_type})),(1,'0',CAST({right} AS {data_type})),"
                    f"(2,NULL,CAST({right} AS {data_type})),(3,'1',CAST(NULL AS {data_type})) t(k,a,b) ORDER BY k")
    for name, sql in [
        ("null_right_known_bad", "SELECT NULLIF('bad',CAST(NULL AS INT)) AS r"),
        ("null_left_bad_string", "SELECT NULLIF(CAST(NULL AS INT),'bad') AS r"),
        ("null_left_bad_boolean", "SELECT NULLIF(CAST(NULL AS BOOLEAN),'on') AS r"),
        ("null_right_string", "SELECT NULLIF(1,CAST(NULL AS STRING)) AS r"),
        ("equal_null_bad", "SELECT CAST(NULL AS INT)='bad' AS r"),
        ("literal_bad_empty", "SELECT 'bad'=1 AS r WHERE false"),
        ("case_unused", "SELECT CASE WHEN false THEN 'bad'=1 ELSE true END AS r"),
        ("incomparable", "SELECT NULLIF('1',ARRAY(1)) AS r"),
        ("filter_integer", "SELECT k AS r FROM VALUES (0,'1',1),(1,'1.5',1),(2,NULL,1) t(k,a,b) WHERE a=b ORDER BY k"),
        ("join_integer", "SELECT s.k AS r FROM VALUES (0,'1'),(1,'1.5'),(2,CAST(NULL AS STRING)) s(k,a) JOIN VALUES (1),(2) n(b) ON s.a=n.b ORDER BY s.k"),
        ("filter_boolean", "SELECT k AS r FROM VALUES (0,'true',true),(1,'f',false),(2,'on',true),(3,NULL,true) t(k,a,b) WHERE a=b ORDER BY k"),
        ("join_boolean", "SELECT s.k AS r FROM VALUES (0,'true'),(1,'f'),(2,'on'),(3,CAST(NULL AS STRING)) s(k,a) JOIN VALUES (true),(false) n(b) ON n.b=s.a ORDER BY s.k"),
    ]:
        yield name, sql


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-string-comparisons")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    observations = []
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        queries = list(string_comparison_queries())
        assert len(dict(queries)) == len(queries)
        for name, sql in queries:
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                try:
                    frame = spark.sql(sql)
                    rows = []
                    data_type = frame.schema.fields[0].dataType
                    for row in frame.collect():
                        value = row[0]
                        if value is not None:
                            if isinstance(data_type, (FloatType, DoubleType)):
                                value = ("nan" if math.isnan(value) else
                                         struct.pack("!f" if isinstance(data_type, FloatType) else "!d", value).hex())
                            elif isinstance(value, bool):
                                value = str(value).lower()
                            else:
                                value = str(value)
                        rows.append([value])
                    expected = {"status": "ok", "schema": frame.schema.jsonValue(), "rows": rows}
                except Exception as error:
                    condition = getattr(error, "getCondition", lambda: None)()
                    if condition not in ["CAST_INVALID_INPUT", "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES"]:
                        raise
                    expected = {"status": "error", "condition": condition}
                record = {"id": name, "sql": sql, "ansi": ansi, "expected": expected}
                observations.append(record)
        metadata = {"spark_version": spark.version, "timezone": "UTC",
                    "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version")}
        Path(__file__).with_name("string-comparison-spark.jsonl").write_text(
            "\n".join(json.dumps(record, separators=(",", ":")) for record in [metadata, *observations]) + "\n")
        print(json.dumps({"observations": len(observations), "errors": sum(x["expected"]["status"] == "error" for x in observations)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
