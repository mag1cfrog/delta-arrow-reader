"""Capture Boolean short-circuit results with PySpark 4.2.0 and Java 21.

The JSONL includes registered inputs and both ANSI modes. Rust replay splits
those inputs into different batch sizes without requiring a Spark installation.
"""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
spark = (SparkSession.builder.master("local[1]").appName("dar-boolean-evaluation")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    inputs = [
        {"table": "and_inputs", "rows": [[0, False, "bad"], [1, True, "1"], [2, None, "1"]]},
        {"table": "or_inputs", "rows": [[0, True, "bad"], [1, False, "1"], [2, None, "1"]]},
    ]
    for item in inputs:
        spark.createDataFrame(item["rows"], "k INT NOT NULL,v BOOLEAN,s STRING").createOrReplaceTempView(item["table"])
    truth_rows = [[i, a, b] for i, (a, b) in enumerate(
        (a, b) for a in [True, False, None] for b in [True, False, None])]
    spark.createDataFrame(truth_rows, "k INT NOT NULL,a BOOLEAN,b BOOLEAN").createOrReplaceTempView("truth_inputs")
    queries = []
    for op in ["AND", "OR"]:
        table = op.lower() + "_inputs"
        for name, left, suffix in [
            ("skip", "v", ""), ("required", "NOT v", ""),
            ("nonnull_skip", "v", "WHERE k<2"), ("empty", "v", "WHERE k<0"),
            ("null_required", "CAST(NULL AS BOOLEAN)", ""),
        ]:
            queries.append((op.lower() + "_" + name,
                            f"SELECT {left} {op} CAST(s AS INT)>0 AS r FROM {table} {suffix} ORDER BY k"))
        queries.append((op.lower() + "_truth", f"SELECT a {op} b AS r FROM truth_inputs ORDER BY k"))
        for literal in ["true", "false", "CAST(NULL AS BOOLEAN)"]:
            for left, right in [("a", literal), (literal, "b")]:
                queries.append((f"{op.lower()}_{left}_{right}",
                                f"SELECT {left} {op} {right} AS r FROM truth_inputs ORDER BY k"))
    records = [{"spark_version": spark.version, "timezone": "UTC",
                "inputs": inputs, "truth_rows": truth_rows}]
    assert len(dict(queries)) == len(queries)
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            try:
                frame = spark.sql(sql)
                expected = {"status": "ok", "rows": [list(row) for row in frame.collect()],
                            "schema": frame.schema.jsonValue()}
            except Exception as error:
                condition = error.getCondition()
                assert condition == "CAST_INVALID_INPUT", (name, condition)
                expected = {"status": "error", "condition": condition}
            records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
    Path(__file__).with_name("boolean-evaluation-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"cases": len(records) - 1,
                      "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
finally:
    spark.stop()
