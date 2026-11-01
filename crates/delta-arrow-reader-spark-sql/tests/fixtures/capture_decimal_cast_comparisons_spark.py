"""Capture comparisons after Decimal casts using Spark 4.2.0 and Java 21."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession


INPUTS = {
    "fractional": {
        "sql_type": "DECIMAL(8,2)",
        "values": ["1.49", "1.50", "1.51", "-1.49", "-1.50", "-1.51", "1.01", "1.04", "1.05", "1.06", "-1.01", "-1.04", "-1.05", "-1.06", "0.01", "-0.01", "0.00", "2.00", None],
        "targets": ["DECIMAL(8,0)", "DECIMAL(8,1)", "DECIMAL(8,2)", "DECIMAL(10,3)", "INT", "BIGINT"],
    },
    "precision38": {
        "sql_type": "DECIMAL(38,20)",
        "values": ["1.49999999999999999999", "1.50000000000000000000", "-1.49999999999999999999", "-1.50000000000000000000", "1.00000000000000000001", "-1.00000000000000000001", "0.00000000000000000001", "0.00000000000000000000", None],
        "targets": ["DECIMAL(38,0)", "DECIMAL(38,19)", "DECIMAL(38,20)"],
    },
    "integral": {
        "sql_type": "DECIMAL(2,0)",
        "values": ["1", "-1", "0", "99", "-99", None],
        "targets": ["TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL(3,0)"],
    },
    "overflow": {
        "sql_type": "DECIMAL(6,0)",
        "values": ["1", "128", "-129", "100000", None],
        "targets": ["TINYINT", "SMALLINT", "DECIMAL(2,0)"],
    },
    "int32": {
        "sql_type": "INT",
        "values": ["1", "2147483647", "-2147483648", None],
        "targets": ["DECIMAL(10,2)", "DECIMAL(12,2)"],
    },
}


def queries():
    for table, definition in INPUTS.items():
        for target in definition["targets"]:
            for cast in ["CAST", "TRY_CAST"]:
                converted = f"{cast}(v AS {target})"
                literal = f"CAST(1 AS {target})"
                negative = f"CAST(-1 AS {target})"
                expressions = [
                    ("eq", f"{converted}={literal}"),
                    ("ne", f"{converted}<>{literal}"),
                    ("lt", f"{converted}<{literal}"),
                    ("le", f"{converted}<={literal}"),
                    ("gt", f"{converted}>{literal}"),
                    ("ge", f"{converted}>={literal}"),
                    ("reversed", f"{literal}={converted}"),
                    ("negative_eq", f"{converted}={negative}"),
                    ("negative_lt", f"{converted}<{negative}"),
                    ("negative_gt", f"{converted}>{negative}"),
                    ("negative_member", f"{converted} IN ({negative},CAST(NULL AS {target}))"),
                    ("member", f"{converted} IN ({literal},CAST(2 AS {target}))"),
                    ("missing", f"{converted} NOT IN ({literal},CAST(2 AS {target}))"),
                    ("nullable_member", f"{converted} IN ({literal},CAST(NULL AS {target}))"),
                    ("nullable_missing", f"{converted} NOT IN ({literal},CAST(NULL AS {target}))"),
                ]
                projection = ",".join(f"{sql} AS {name}" for name, sql in expressions)
                for empty in [False, True]:
                    suffix = " WHERE false" if empty else ""
                    yield {"id": f"{table}_{target}_{cast}_{empty}", "table": table,
                           "target": target, "cast": cast,
                           "sql": f"SELECT {projection} FROM {table}{suffix} ORDER BY id"}


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-decimal-cast-reference")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for table, definition in INPUTS.items():
            raw = table + "_strings"
            spark.createDataFrame(list(enumerate(definition["values"])), "id INT NOT NULL,v STRING").createOrReplaceTempView(raw)
            spark.sql(f"SELECT id,CAST(v AS {definition['sql_type']}) AS v FROM {raw}").createOrReplaceTempView(table)
        observations = []
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for case in queries():
                try:
                    frame = spark.sql(case["sql"])
                    expected = {"status": "ok", "schema": frame.schema.jsonValue(),
                                "rows": [list(row) for row in frame.collect()]}
                except Exception as error:
                    expected = {"status": "error", "condition": error.getCondition()}
                observations.append({**case, "ansi": ansi, "expected": expected})
        metadata = {"spark_version": spark.version, "inputs": INPUTS,
                    "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version")}
        Path(__file__).with_name("decimal-cast-comparisons-spark.jsonl").write_text(
            "\n".join(json.dumps(r, separators=(",", ":")) for r in [metadata, *observations]) + "\n")
        print(json.dumps({"observations": len(observations), "errors": [r["expected"]["condition"] for r in observations if r["expected"]["status"] == "error"]}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
