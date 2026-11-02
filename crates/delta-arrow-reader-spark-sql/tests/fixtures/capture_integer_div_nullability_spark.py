"""Capture integer DIV schemas, errors and unused expressions with Spark 4.2.0."""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
queries = []
widths = [("TINYINT", -128), ("SMALLINT", -32768), ("INT", -2147483648),
          ("BIGINT", -9223372036854775808)]
for kind, minimum in widths:
    left = f"CAST(id-1 AS {kind})"
    for name, expression in [
        ("positive", f"{left} DIV CAST(2 AS {kind})"),
        ("negative", f"{left} DIV CAST(-2 AS {kind})"),
        ("minimum", f"CAST(CASE WHEN id=0 THEN {minimum} ELSE 7 END AS {kind}) DIV CAST(-1 AS {kind})"),
        ("zero", f"{left} DIV CAST(0 AS {kind})"),
        ("nullable_left", f"CAST(CASE WHEN id=0 THEN NULL ELSE id END AS {kind}) DIV CAST(2 AS {kind})"),
        ("nullable_right", f"{left} DIV CAST(CASE WHEN id=0 THEN NULL ELSE 2 END AS {kind})"),
        ("masked_zero", f"CAST(CASE WHEN id=0 THEN NULL ELSE 7 END AS {kind}) DIV CAST(id AS {kind})"),
        ("null_left", f"CAST(NULL AS {kind}) DIV CAST(id AS {kind})"),
        ("null_right", f"{left} DIV CAST(NULL AS {kind})"),
    ]:
        queries.append((f"{kind.lower()}_{name}", f"SELECT {expression} AS r FROM range(3)"))
for left, _ in widths:
    for right, _ in widths:
        queries.append((f"mixed_{left.lower()}_{right.lower()}",
                        f"SELECT CAST(id+7 AS {left}) DIV CAST(id+1 AS {right}) AS r FROM range(3)"))
for name, expression in [
    ("bigint_zero", "id DIV 0"),
    ("int_zero", "CAST(id AS INT) DIV 0"),
    ("overflow", "(CASE WHEN id=0 THEN CAST(-9223372036854775808 AS BIGINT) ELSE 7 END) DIV -1"),
    ("valid", "id DIV 2"),
]:
    grouped = (f"SELECT {expression} AS v FROM range(3) GROUP BY {expression} "
               f"HAVING {expression} IS NULL")
    for consumer, sql in [
        ("where", f"SELECT id AS r FROM range(3) WHERE {expression} IS NULL"),
        ("having", grouped),
        ("exists", f"SELECT EXISTS({grouped}) AS r"),
        ("limited_exists", f"SELECT EXISTS({grouped} LIMIT 1) AS r"),
        ("limited_count", f"SELECT count(*) AS r FROM ({grouped} LIMIT 1) t"),
        ("in", f"SELECT 1 AS r WHERE (7 IN(SELECT {expression} AS v FROM range(3) GROUP BY {expression} ORDER BY v NULLS FIRST LIMIT 1)) IS NULL"),
        ("not_in", f"SELECT 1 AS r WHERE (7 NOT IN(SELECT {expression} AS v FROM range(3) GROUP BY {expression} ORDER BY v NULLS FIRST LIMIT 1)) IS NULL"),
        ("empty", f"SELECT {expression} AS r FROM range(0)"),
        ("limit_zero", f"SELECT {expression} AS r FROM range(3) LIMIT 0"),
        ("unreachable", f"SELECT CASE WHEN false THEN {expression} ELSE 2 END AS r FROM range(3)"),
        ("unused_projection", f"SELECT 1 AS r FROM(SELECT {expression} AS v FROM range(3)) t"),
        ("unused_group_exists", f"SELECT EXISTS(SELECT {expression} AS v FROM range(3) GROUP BY {expression} LIMIT 1) AS r"),
        ("unused_group_limit", f"SELECT 1 AS r FROM(SELECT {expression} AS v FROM range(3) GROUP BY {expression} LIMIT 1)t"),
    ]:
        queries.append((f"{name}_{consumer}", sql))
for name, sql in [
    ("literal", "SELECT 7 DIV 2 AS r"),
    ("null_literal", "SELECT NULL DIV 2 AS r"),
    ("null_divisor", "SELECT 7 DIV NULL AS r"),
    ("both_null", "SELECT NULL DIV NULL AS r"),
    ("constant_zero", "SELECT 7 DIV 0 AS r"),
    ("constant_overflow", "SELECT CAST(-9223372036854775808 AS BIGINT) DIV -1 AS r"),
    ("local_unused_group", "SELECT EXISTS(SELECT id DIV 0 AS v FROM VALUES(1),(2) t(id) GROUP BY id DIV 0 LIMIT 1) AS r"),
]:
    queries.append((name, sql))
assert len({name for name, _ in queries}) == len(queries)

spark = (SparkSession.builder.master("local[1]").appName("dar-integer-div-nullability")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    records = [{"spark_version": spark.version, "timezone": "UTC"}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            # Legacy projected-IN NULL policy is tracked separately in #142.
            if not ansi and name in {"bigint_zero_in", "bigint_zero_not_in",
                                     "int_zero_in", "int_zero_not_in"}:
                continue
            try:
                frame = spark.sql(sql)
                rows = [[None if value is None else str(value).lower()
                         if isinstance(value, bool) else str(value) for value in row]
                        for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                expected = {"schema": frame.schema.jsonValue(), "rows": rows}
            except Exception as error:
                conditions = [condition for condition in ["DIVIDE_BY_ZERO", "ARITHMETIC_OVERFLOW"]
                              if f"[{condition}]" in str(error)]
                assert len(conditions) == 1, str(error)
                expected = {"condition": conditions[0]}
            records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
    Path(__file__).with_name("integer-div-nullability-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1}))
finally:
    spark.stop()
