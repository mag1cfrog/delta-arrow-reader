"""Capture floating membership using inputs that preserve signed zero and NaN."""

import json
import os
from pathlib import Path
import struct
import sys

from pyspark.sql import SparkSession


def inputs():
    for sql_type, patterns in [
        ("FLOAT", ["00000000", "80000000", "3fc00000", "7fc00001", "ffc12345", "7f800000", "ff800000"]),
        ("DOUBLE", ["0000000000000000", "8000000000000000", "3ff8000000000000", "7ff8000000000001", "fff8123456789abc", "7ff0000000000000", "fff0000000000000"]),
    ]:
        zero, negative_zero, number, nan, other_nan, infinity, negative_infinity = patterns
        rows = [
            ([zero], negative_zero), ([negative_zero], zero),
            ([zero, None], negative_zero), ([negative_zero, None], zero),
            ([number, None], negative_zero), ([number], negative_zero),
            ([nan, None], other_nan), ([other_nan, None], nan),
            ([infinity, None], infinity), ([infinity], negative_infinity),
            (None, zero), ([], zero), ([zero], None), ([], None),
            ([nan], zero), ([zero, nan], negative_zero), ([None, None], zero),
        ]
        for fixed in [False, True]:
            selected = [r for r in rows if not fixed or r[0] is None or len(r[0]) == 2]
            name = sql_type.lower() + ("_fixed" if fixed else "_inputs")
            yield name, {"sql_type": sql_type, "fixed": fixed,
                         "rows": [(i, array, lookup) for i, (array, lookup) in enumerate(selected)]}


def queries():
    for name, definition in inputs():
        sql_type = definition["sql_type"]
        expressions = [
            ("registered", "ARRAY_CONTAINS(a,n)"),
            ("scalar_zero", f"ARRAY_CONTAINS(a,CAST('-0.0' AS {sql_type}))"),
            ("scalar_nan", f"ARRAY_CONTAINS(a,CAST('NaN' AS {sql_type}))"),
            ("constant_zero_array", f"ARRAY_CONTAINS(ARRAY(CAST('0.0' AS {sql_type}),CAST(NULL AS {sql_type})),n)"),
            ("constant_negative_array", f"ARRAY_CONTAINS(ARRAY(CAST('-0.0' AS {sql_type}),CAST(NULL AS {sql_type})),n)"),
            ("constant_nan_array", f"ARRAY_CONTAINS(ARRAY(CAST('NaN' AS {sql_type}),CAST(NULL AS {sql_type})),n)"),
            ("empty", "ARRAY_CONTAINS(a,n)"),
        ]
        for label, expression in expressions:
            suffix = " WHERE false" if label == "empty" else ""
            yield {"id": name + "_" + label, "input": name,
                   "sql": f"SELECT {expression} AS r FROM {name}{suffix} ORDER BY id"}
    for sql_type in ["FLOAT", "DOUBLE"]:
        for left, right in [("0.0", "-0.0"), ("-0.0", "0.0"), ("NaN", "NaN"), ("Infinity", "-Infinity")]:
            expression = f"ARRAY_CONTAINS(ARRAY(CAST('{left}' AS {sql_type}),CAST(NULL AS {sql_type})),CAST('{right}' AS {sql_type}))"
            for local in [False, True]:
                suffix = " FROM range(2)" if local else ""
                yield {"id": f"{sql_type}_{left}_{right}_{local}", "sql": f"SELECT {expression} AS r{suffix}"}
    for name, array, lookup in [
        ("required_array_error", "ARRAY(CAST(CAST('bad' AS INT) AS DOUBLE))", "CAST('0.0' AS DOUBLE)"),
        ("unused_array_error", "ARRAY(CAST(CAST('bad' AS INT) AS DOUBLE))", "CAST(NULL AS DOUBLE)"),
        ("required_lookup_error", "ARRAY(CAST('0.0' AS DOUBLE))", "CAST(CAST('bad' AS INT) AS DOUBLE)"),
        ("unused_lookup_error", "CAST(NULL AS ARRAY<DOUBLE>)", "CAST(CAST('bad' AS INT) AS DOUBLE)"),
    ]:
        yield {"id": name, "sql": f"SELECT ARRAY_CONTAINS({array},{lookup}) AS r"}


def decode(bits):
    if bits is None:
        return None
    return struct.unpack(">f" if len(bits) == 8 else ">d", bytes.fromhex(bits))[0]


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-floating-membership")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        registered = dict(inputs())
        for name, definition in registered.items():
            rows = [(i, None if array is None else [decode(v) for v in array], decode(lookup))
                    for i, array, lookup in definition["rows"]]
            kind = definition["sql_type"]
            spark.createDataFrame(rows, f"id INT NOT NULL,a ARRAY<{kind}>,n {kind}").createOrReplaceTempView(name)
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
                    if condition != "CAST_INVALID_INPUT":
                        raise
                    expected = {"status": "error", "condition": condition}
                records.append({**case, "ansi": ansi, "expected": expected})
        assert len({(r["id"], r["ansi"]) for r in records}) == len(records)
        header = {"spark_version": spark.version, "inputs": registered,
                  "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version")}
        Path(__file__).with_name("array-floating-spark.jsonl").write_text(
            "\n".join(json.dumps(r, separators=(",", ":")) for r in [header, *records]) + "\n")
        print(json.dumps({"observations": len(records), "errors": sum(r["expected"]["status"] == "error" for r in records)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
