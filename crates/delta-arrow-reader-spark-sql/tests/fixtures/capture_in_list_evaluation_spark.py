"""Capture IN-list short-circuit results with PySpark 4.2.0 and Java 21.

Registered inputs keep the malformed casts at execution time. Long lists and
computed lookups also exercise DAR's native membership path during replay.
"""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
spark = (SparkSession.builder.master("local[1]").appName("dar-in-list-evaluation")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    rows = [[i, a, b, [None, 1, 2][i % 3]] for i, (a, b) in enumerate(
        (a, b) for a in [1, 0, None] for b in [1, 0, None])]
    rows += [[9, -1, -1, None], [10, 1, 2, 3], [11, None, None, None]]
    inputs = spark.createDataFrame(rows, "k INT NOT NULL,a INT,b INT,c INT")
    inputs.createOrReplaceTempView("inputs")
    match_error = "CAST(CASE WHEN a=b THEN 'bad' ELSE '1' END AS INT)"
    null_error = "CAST(CASE WHEN a IS NULL THEN 'bad' ELSE '1' END AS INT)"
    queries = []
    for op in ["IN", "NOT IN"]:
        for name, lookup, items in [
            ("long_skip", "a", f"b,c,99,{match_error}"),
            ("long_required", "a", f"{match_error},b,c,99"),
            ("computed_skip", "COALESCE(a,-2)", f"b,{match_error}"),
            ("computed_required", "COALESCE(a,-2)", f"{match_error},b"),
            ("null_lookup_first", "a", f"{null_error},b,c,99"),
            ("null_lookup_last", "a", f"b,c,99,{null_error}"),
            ("nonnull_lookup_required", "COALESCE(a,-2)", f"{null_error},b,c,99"),
        ]:
            queries.append((op.lower().replace(" ", "_") + "_" + name,
                            f"SELECT {lookup} {op} ({items}) AS r FROM inputs ORDER BY k"))
    records = [{"spark_version": spark.version, "timezone": "UTC",
                "input_schema": inputs.schema.jsonValue(), "input_rows": rows}]
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
    Path(__file__).with_name("in-list-evaluation-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"cases": len(records) - 1,
                      "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
finally:
    spark.stop()
