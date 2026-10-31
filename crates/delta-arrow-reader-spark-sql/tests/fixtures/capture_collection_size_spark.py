"""Capture collection-size values and admission with Spark 4.2.0 and Java 21."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable


def queries():
    arguments = [
        ("untyped_null", "NULL"),
        ("null_array", "CAST(NULL AS ARRAY<INT>)"),
        ("empty_array", "CAST(ARRAY() AS ARRAY<INT>)"),
        ("null_elements", "ARRAY(NULL, NULL)"),
        ("array", "ARRAY(1, NULL, 3)"),
        ("nested_array", "ARRAY(ARRAY(1, 2), ARRAY(3))"),
        ("null_map", "CAST(NULL AS MAP<STRING, INT>)"),
        ("empty_map", "MAP()"),
        ("map", "MAP('a', 1, 'b', NULL)"),
        ("nested_map", "MAP('a', ARRAY(1, 2), 'b', ARRAY(3))"),
        ("invalid_integer", "1"),
        ("invalid_null_integer", "CAST(NULL AS INT)"),
        ("invalid_string", "'abc'"),
        ("invalid_struct", "STRUCT(1, 2)"),
        ("live_bad_cast", "ARRAY(CAST('bad' AS INT))"),
    ]
    for function in ["SIZE", "CARDINALITY", "ARRAY_SIZE"]:
        for name, argument in arguments:
            yield f"{function.lower()}_{name}", f"SELECT {function}({argument}) AS r"
        for name, source in [
            ("array_column", "VALUES (0, ARRAY(1, NULL)), (1, CAST(NULL AS ARRAY<INT>)), (2, ARRAY()) t(k,v)"),
            ("map_column", "VALUES (0, MAP('a', 1)), (1, CAST(NULL AS MAP<STRING, INT>)), (2, MAP()) t(k,v)"),
            ("null_column", "VALUES (0, NULL), (1, NULL), (2, NULL) t(k,v)"),
        ]:
            yield f"{function.lower()}_{name}", f"SELECT {function}(v) AS r FROM {source} ORDER BY k"
            yield f"{function.lower()}_{name}_empty", f"SELECT {function}(v) AS r FROM {source} WHERE false"
        yield f"{function.lower()}_empty_scalar", f"SELECT {function}(NULL) AS r FROM range(0)"
        yield f"{function.lower()}_arity_zero", f"SELECT {function}() AS r"
        yield f"{function.lower()}_arity_two", f"SELECT {function}(ARRAY(1), ARRAY(2)) AS r"


spark = (
    SparkSession.builder.master("local[1]")
    .appName("dar-collection-size-reference")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)
records = []
try:
    assert spark.version == "4.2.0"
    spark.sparkContext.setLogLevel("ERROR")
    metadata = {
        "spark_version": spark.version,
        "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
        "default_legacy_size_of_null": spark.conf.get("spark.sql.legacy.sizeOfNull"),
    }
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for legacy in [True, False]:
            spark.conf.set("spark.sql.legacy.sizeOfNull", str(legacy).lower())
            for name, sql in queries():
                try:
                    frame = spark.sql(sql)
                    rows = [list(row) for row in frame.collect()]
                    expected = {"status": "ok", "schema": frame.schema.jsonValue(), "rows": rows}
                except Exception as error:
                    expected = {"status": "error", "condition": error.getCondition()}
                records.append({"id": name, "sql": sql, "ansi": ansi, "legacy_size_of_null": legacy, "expected": expected})
    Path(__file__).with_name("collection-size-spark.jsonl").write_text(
        "\n".join(json.dumps(record) for record in [metadata, *records]) + "\n"
    )
    print(json.dumps({"observations": len(records), **metadata}))
finally:
    spark.stop()
