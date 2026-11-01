"""Capture NULL comparisons, list membership and conditional schema stages.

Use PySpark 4.2.0 and Java 21. Registered integer columns keep runtime NULL
behavior separate from the preparation of local SQL VALUES expressions.
"""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
spark = (SparkSession.builder.master("local[1]").appName("dar-null-logic")
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
    expressions = [
        ("not_column", "NOT (a=1)"),
        ("not_untyped_null", "NOT NULL"),
        ("not_typed_null", "NOT CAST(NULL AS BOOLEAN)"),
        ("nullsafe_columns", "a<=>b"),
        ("nullsafe_null_rhs", "a<=>CAST(NULL AS INT)"),
        ("nullsafe_null_lhs", "CAST(NULL AS INT)<=>a"),
        ("nullsafe_both_null", "CAST(NULL AS INT)<=>CAST(NULL AS INT)"),
        ("equal_columns", "a=b"),
        ("not_equal_columns", "a<>b"),
        ("in_columns", "a IN (b,c)"),
        ("not_in_columns", "a NOT IN (b,c)"),
        ("in_literal_null", "a IN (1,NULL)"),
        ("not_in_literal_null", "a NOT IN (1,NULL)"),
        ("in_null_first", "a IN (NULL,1)"),
        ("not_in_null_first", "a NOT IN (NULL,1)"),
        ("in_null_lookup", "CAST(NULL AS INT) IN (a,b)"),
        ("not_in_null_lookup", "CAST(NULL AS INT) NOT IN (a,b)"),
        ("in_only_null", "a IN (CAST(NULL AS INT))"),
        ("not_in_only_null", "a NOT IN (CAST(NULL AS INT))"),
    ]
    bad_cast = "CAST(CASE WHEN a=b THEN 'bad' ELSE '1' END AS INT)"
    for op in ["IN", "NOT IN"]:
        for name, items in [("skip", f"b,{bad_cast}"), ("required", f"{bad_cast},b")]:
            expressions.append((op.lower().replace(" ", "_") + "_cast_" + name, f"a {op} ({items})"))
        for name, lookup, items in [
            ("long_skip", "a", f"b,c,99,{bad_cast}"),
            ("long_required", "a", f"{bad_cast},b,c,99"),
            ("computed_skip", "COALESCE(a,-2)", f"b,{bad_cast}"),
            ("computed_required", "COALESCE(a,-2)", f"{bad_cast},b"),
        ]:
            expressions.append((op.lower().replace(" ", "_") + "_cast_" + name,
                                f"{lookup} {op} ({items})"))
    queries = [(name, f"SELECT {expression} AS r FROM inputs ORDER BY k")
               for name, expression in expressions]
    queries += [
        ("in_empty", "SELECT a IN (b,c) AS r FROM inputs WHERE k<0"),
        ("schema_case_constant", "SELECT CASE WHEN true THEN 1 ELSE CAST(NULL AS INT) END AS r"),
        ("schema_if_constant", "SELECT IF(true,1,CAST(NULL AS INT)) AS r"),
        ("schema_if_column", "SELECT IF(a=1,1,CAST(NULL AS INT)) AS r FROM inputs ORDER BY k"),
    ]
    assert len(dict(queries)) == len(queries)
    records = [{"spark_version": spark.version, "timezone": "UTC",
                "input_schema": inputs.schema.jsonValue(), "input_rows": rows}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            try:
                frame = spark.sql(sql)
                analyzed = frame.schema.jsonValue()
                optimized = frame._jdf.queryExecution().optimizedPlan().output()
                nullable = [optimized.apply(i).nullable() for i in range(optimized.size())]
                expected = {"status": "ok", "rows": [list(row) for row in frame.collect()],
                            "schema": analyzed, "optimized_nullable": nullable}
            except Exception as error:
                condition = error.getCondition()
                assert condition == "CAST_INVALID_INPUT", (name, condition)
                expected = {"status": "error", "condition": condition}
            records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
    Path(__file__).with_name("null-logic-spark.jsonl").write_text(
        "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records))
    print(json.dumps({"cases": len(records) - 1,
                      "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
finally:
    spark.stop()
