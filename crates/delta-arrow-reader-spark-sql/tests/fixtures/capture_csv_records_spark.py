"""Capture Spark 4.2 CSV record validity and option boundaries for csv_records.rs."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


INPUTS = [(0, None), (1, ""), (2, " "), (3, "value"), (4, "a,b")]


def queries():
    for data_type in ["BOOLEAN", "INT", "FLOAT", "DECIMAL(10,2)", "DATE", "STRING"]:
        yield f"scalar_null_{data_type}", f"SELECT TO_CSV(NAMED_STRUCT('v',CAST(NULL AS {data_type}))) AS r"
    for name, expression in [
        ("two_nulls", "NAMED_STRUCT('a',CAST(NULL AS INT),'b',CAST(NULL AS STRING))"),
        ("null_struct", "CAST(NULL AS STRUCT<v:STRING>)"),
        ("empty_struct", "STRUCT()"),
        ("nonempty", "NAMED_STRUCT('a',1,'b','value')"),
    ]:
        yield f"scalar_{name}", f"SELECT TO_CSV({expression}) AS r"
    for name, options in [
        ("default", ""),
        ("quote_all", ",MAP('quoteAll','true')"),
        ("null_marker", ",MAP('nullValue','NULL')"),
        ("null_empty", ",MAP('nullValue','')"),
        ("null_space", ",MAP('nullValue',' ')"),
        ("empty_empty", ",MAP('emptyValue','')"),
        ("empty_marker", ",MAP('emptyValue','_')"),
        ("empty_comma", ",MAP('emptyValue',',')"),
        ("no_quote", ",MAP('quote','')"),
        ("keep_spaces", ",MAP('ignoreLeadingWhiteSpace','false','ignoreTrailingWhiteSpace','false')"),
        ("separator", ",MAP('sep',';')"),
    ]:
        for value in ["NULL", "''"]:
            yield f"scalar_{name}_{value}", f"SELECT TO_CSV(NAMED_STRUCT('v',CAST({value} AS STRING)){options}) AS r"
        yield f"column_{name}", f"SELECT TO_CSV(NAMED_STRUCT('v',v){options}) AS r FROM csv_inputs ORDER BY k"
    yield "column_two_fields", "SELECT TO_CSV(NAMED_STRUCT('v',v,'n',CAST(NULL AS INT))) AS r FROM csv_inputs ORDER BY k"
    yield "column_null_struct", "SELECT TO_CSV(CASE WHEN k=0 THEN NULL ELSE NAMED_STRUCT('v',v) END) AS r FROM csv_inputs ORDER BY k"
    yield "column_empty", "SELECT TO_CSV(NAMED_STRUCT('v',v)) AS r FROM csv_inputs WHERE false"
    for name, option in [("invalid_quote", "MAP('quote','xx')"), ("invalid_bool", "MAP('quoteAll','invalid')")]:
        yield name, f"SELECT TO_CSV(NAMED_STRUCT('v',CAST(NULL AS STRING)),{option}) AS r"
        yield f"{name}_null_struct", f"SELECT TO_CSV(CAST(NULL AS STRUCT<v:STRING>),{option}) AS r"


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    # Spark's generated column path throws for a NULL CSV result; capture its evaluator.
    spark = (SparkSession.builder.master("local[1]").appName("dar-csv-records")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.codegen.wholeStage", "false")
             .config("spark.sql.codegen.factoryMode", "NO_CODEGEN").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        spark.createDataFrame(INPUTS, StructType([
            StructField("k", IntegerType(), False), StructField("v", StringType()),
        ])).createOrReplaceTempView("csv_inputs")
        records = []
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in queries():
                try:
                    frame = spark.sql(sql)
                    expected = {"status": "ok", "schema": frame.schema.jsonValue(),
                                "rows": [list(row) for row in frame.collect()]}
                except Exception as error:
                    if not name.startswith("invalid_"):
                        raise
                    if hasattr(error, "getCondition"):
                        expected = {"status": "error", "condition": error.getCondition()}
                    else:
                        cause = error.java_exception
                        while cause.getCause() is not None:
                            cause = cause.getCause()
                        expected = {"status": "error", "class": cause.getClass().getName(),
                                    "message": cause.getMessage()}
                records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
        metadata = {"spark_version": spark.version, "inputs": INPUTS,
                    "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
                    "codegen_factory_mode": "NO_CODEGEN", "whole_stage_codegen": False}
        Path(__file__).with_name("csv-records-spark.jsonl").write_text(
            "\n".join(json.dumps(r, separators=(",", ":")) for r in [metadata, *records]) + "\n")
        print(json.dumps({"observations": len(records), "errors": sum(r["expected"]["status"] == "error" for r in records)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
