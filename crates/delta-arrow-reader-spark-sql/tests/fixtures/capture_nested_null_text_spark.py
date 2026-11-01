"""Capture Spark nested NULL markers separately from SQL NULL validity."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


INPUTS = [(0, None), (1, "NULL"), (2, "null"), (3, "a,b"), (4, "")]


def queries():
    expressions = [
        ("untyped_array", "ARRAY(NULL)"),
        ("integer_array", "ARRAY(1,CAST(NULL AS INT),2)"),
        ("float_array", "ARRAY(CAST(NULL AS FLOAT))"),
        ("double_array", "ARRAY(CAST(NULL AS DOUBLE))"),
        ("string_array", "ARRAY('NULL',CAST(NULL AS STRING),'null')"),
        ("struct", "NAMED_STRUCT('a',1,'b',CAST(NULL AS STRING))"),
        ("map", "MAP('key',CAST(NULL AS STRING))"),
        ("nested_array", "ARRAY(ARRAY(1,NULL),CAST(NULL AS ARRAY<INT>))"),
        ("nested_struct", "NAMED_STRUCT('a',ARRAY(NULL),'b',CAST(NULL AS STRUCT<v:INT>))"),
        ("nested_map", "MAP('key',ARRAY(CAST(NULL AS INT)))"),
        ("null_array", "CAST(NULL AS ARRAY<INT>)"),
        ("null_struct", "CAST(NULL AS STRUCT<v:INT>)"),
        ("null_map", "CAST(NULL AS MAP<STRING,INT>)"),
    ]
    for name, expression in expressions:
        yield name, f"SELECT CAST({expression} AS STRING) AS r"
    for name, expression in [
        ("array", "ARRAY(v)"), ("struct", "NAMED_STRUCT('v',v)"), ("map", "MAP('key',v)"),
    ]:
        yield f"column_{name}", f"SELECT CAST({expression} AS STRING) AS r FROM nested_inputs ORDER BY k"
        yield f"column_{name}_empty", f"SELECT CAST({expression} AS STRING) AS r FROM nested_inputs WHERE false"
        yield f"csv_{name}", f"SELECT TO_CSV(NAMED_STRUCT('nested',{expression})) AS r FROM nested_inputs ORDER BY k"
    yield "column_top_level_null", "SELECT CAST(CASE WHEN k=0 THEN NULL ELSE ARRAY(v) END AS STRING) AS r FROM nested_inputs ORDER BY k"
    yield "pivot_null_name", "SELECT * FROM nested_inputs PIVOT(SUM(k) FOR (v) IN (NULL,'NULL' AS present))"


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-nested-null-text")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        spark.createDataFrame(INPUTS, StructType([
            StructField("k", IntegerType(), False), StructField("v", StringType()),
        ])).createOrReplaceTempView("nested_inputs")
        records = []
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in queries():
                frame = spark.sql(sql)
                records.append({"id": name, "ansi": ansi, "sql": sql,
                                "rows": [[None if value is None else str(value) for value in row] for row in frame.collect()],
                                "schema": frame.schema.jsonValue()})
        metadata = {"spark_version": spark.version, "inputs": INPUTS,
                    "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version")}
        Path(__file__).with_name("nested-null-text-spark.jsonl").write_text(
            "\n".join(json.dumps(r, separators=(",", ":")) for r in [metadata, *records]) + "\n")
        print(json.dumps({"observations": len(records)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
