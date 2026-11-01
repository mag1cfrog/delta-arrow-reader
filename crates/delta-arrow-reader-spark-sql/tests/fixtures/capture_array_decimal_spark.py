"""Capture Decimal array membership results with Spark 4.2.0 and Java 21."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession


PAIRS = [
    ("scale", "DECIMAL(10,2)", "DECIMAL(8,1)", "1.20", "1.2"),
    ("precision38", "DECIMAL(38,20)", "DECIMAL(38,0)", "1.49999999999999999999", "1"),
    ("fraction38", "DECIMAL(38,38)", "INT", "0.12345678901234567890123456789012345678", "0"),
    ("byte", "DECIMAL(10,2)", "TINYINT", "127", "127"),
    ("short", "DECIMAL(10,2)", "SMALLINT", "32767", "32767"),
    ("integer", "DECIMAL(10,2)", "INT", "2147483", "2147483"),
    ("long", "DECIMAL(38,20)", "BIGINT", "1.00000000000000000001", "1"),
    ("long_max", "DECIMAL(38,0)", "BIGINT", "9223372036854775807", "9223372036854775807"),
    ("float", "DECIMAL(10,2)", "FLOAT", "16777217", "16777217"),
    ("double", "DECIMAL(38,0)", "DOUBLE", "9007199254740993", "9007199254740992"),
    ("zero", "DECIMAL(5,2)", "DOUBLE", "0.00", "-0.0"),
    ("nan", "DECIMAL(5,2)", "DOUBLE", "1", "NaN"),
    ("infinity", "DECIMAL(5,2)", "FLOAT", "1", "Infinity"),
]


def inputs():
    for name, left_type, right_type, left, right in PAIRS:
        for reverse in [False, True]:
            element_type, lookup_type = (right_type, left_type) if reverse else (left_type, right_type)
            element, lookup = (right, left) if reverse else (left, right)
            yield f"decimal_membership_{name}_{int(reverse)}", {
                "element_type": element_type, "lookup_type": lookup_type,
                "rows": [(0, [element, None], lookup), (1, [element], "0"),
                         (2, None, lookup), (3, [], lookup), (4, [None], lookup),
                         (5, [element], None), (6, [], None), (7, [element, element], lookup)],
            }


def queries():
    for table, definition in inputs():
        left_type, right_type = definition["element_type"], definition["lookup_type"]
        element, lookup = definition["rows"][0][1][0], definition["rows"][0][2]
        array = f"ARRAY(CAST('{element}' AS {left_type}),CAST(NULL AS {left_type}))"
        needle = f"CAST('{lookup}' AS {right_type})"
        yield {"id": table + "_literal", "sql": f"SELECT ARRAY_CONTAINS({array},{needle}) AS r"}
        yield {"id": table + "_local", "sql": f"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT {array} AS a,{needle} AS n FROM range(2)) t"}
        yield {"id": table + "_registered", "sql": f"SELECT ARRAY_CONTAINS(a,n) AS r FROM {table} ORDER BY id"}
        yield {"id": table + "_empty", "sql": f"SELECT ARRAY_CONTAINS(a,n) AS r FROM {table} WHERE false"}
        yield {"id": table + "_scalar_lookup", "sql": f"SELECT ARRAY_CONTAINS(a,{needle}) AS r FROM {table} ORDER BY id"}
        yield {"id": table + "_scalar_array", "sql": f"SELECT ARRAY_CONTAINS({array},n) AS r FROM {table} ORDER BY id"}
    for name, array, lookup in [
        ("required_integer_error", "ARRAY(CAST('bad' AS INT))", "CAST(1 AS DECIMAL(10,2))"),
        ("unused_integer_error", "ARRAY(CAST('bad' AS INT))", "CAST(NULL AS DECIMAL(10,2))"),
        ("required_lookup_error", "ARRAY(CAST(1 AS DECIMAL(10,2)))", "CAST('bad' AS INT)"),
        ("unused_lookup_error", "CAST(NULL AS ARRAY<DECIMAL(10,2)>)", "CAST('bad' AS INT)"),
        ("invalid_array_type", "CAST(1 AS DECIMAL(10,2))", "CAST(1 AS DECIMAL(8,1))"),
        ("invalid_array_type_null_lookup", "CAST(1 AS DECIMAL(10,2))", "CAST(NULL AS DECIMAL(8,1))"),
    ]:
        yield {"id": name, "sql": f"SELECT ARRAY_CONTAINS({array},{lookup}) AS r"}


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-array-decimal-reference")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        registered = dict(inputs())
        for table, definition in registered.items():
            raw = table + "_strings"
            spark.createDataFrame(definition["rows"], "id INT NOT NULL,a ARRAY<STRING>,n STRING").createOrReplaceTempView(raw)
            spark.sql(f"SELECT id,CAST(a AS ARRAY<{definition['element_type']}>) AS a,"
                      f"CAST(n AS {definition['lookup_type']}) AS n FROM {raw}").createOrReplaceTempView(table)
        records = []
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for case in queries():
                try:
                    frame = spark.sql(case["sql"])
                    expected = {"status": "ok", "schema": frame.schema.jsonValue(),
                                "rows": [list(row) for row in frame.collect()]}
                except Exception as error:
                    condition = error.getCondition()
                    if condition not in {"CAST_INVALID_INPUT", "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"}:
                        raise
                    expected = {"status": "error", "condition": condition}
                records.append({**case, "ansi": ansi, "expected": expected})
        assert len({(r["id"], r["ansi"]) for r in records}) == len(records)
        metadata = {"spark_version": spark.version, "registered_inputs": registered,
                    "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version")}
        Path(__file__).with_name("array-decimal-spark.jsonl").write_text(
            "\n".join(json.dumps(r, separators=(",", ":")) for r in [metadata, *records]) + "\n")
        print(json.dumps({"observations": len(records), "errors": sum(r["expected"]["status"] == "error" for r in records)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
