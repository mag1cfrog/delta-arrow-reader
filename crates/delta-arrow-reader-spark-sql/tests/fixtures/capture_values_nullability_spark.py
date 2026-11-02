"""Capture VALUES nullability and its observable expression consumers with Spark 4.2.0."""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
queries = []
for name, values in [
    ("integers", "(0),(1)"),
    ("nullable", "(0),(NULL)"),
    ("all_null", "(CAST(NULL AS INT)),(CAST(NULL AS INT))"),
    ("void", "(NULL),(NULL)"),
    ("mixed_width", "(CAST(1 AS SMALLINT)),(CAST(2 AS BIGINT))"),
    ("decimal", "(CAST(1 AS DECIMAL(10,2))),(CAST(2 AS DECIMAL(10,2)))"),
    ("string_cast", "(CAST('1' AS INT)),(CAST('2' AS INT))"),
    ("try_cast", "(TRY_CAST('bad' AS INT)),(TRY_CAST('2' AS INT))"),
    ("conditional", "(CASE WHEN true THEN 1 ELSE NULL END),(2)"),
    ("strings", "('alpha'),('beta')"),
    ("booleans", "(true),(false)"),
]:
    for consumer, sql in [
        ("projection", f"SELECT v AS r FROM VALUES {values} t(v)"),
        ("null_predicate", f"SELECT v IS NULL AS r FROM VALUES {values} t(v)"),
        ("empty", f"SELECT v AS r FROM VALUES {values} t(v) LIMIT 0"),
    ]:
        queries.append((f"{name}_{consumer}", sql))
queries.extend([
    ("coalesce_div_dead", "SELECT COALESCE(k,7 DIV 0) AS r FROM VALUES(0),(1)t(k) LIMIT 0"),
    ("coalesce_div_unused", "SELECT COALESCE(k,7 DIV 0) AS r FROM VALUES(0),(1)t(k)"),
    ("coalesce_div_required", "SELECT COALESCE(k,7 DIV 0) AS r FROM VALUES(0),(NULL)t(k)"),
    ("local_div_having_exists", "SELECT EXISTS(SELECT id DIV 0 AS v FROM VALUES(0),(1)t(id) GROUP BY id DIV 0 HAVING id DIV 0 IS NULL) AS r"),
    ("local_div_having_count", "SELECT COUNT(*) AS r FROM(SELECT id DIV 0 AS v FROM VALUES(0),(1)t(id) GROUP BY id DIV 0 HAVING id DIV 0 IS NULL LIMIT 1)t"),
    ("cast_required", "SELECT v AS r FROM VALUES(CAST('bad' AS INT)),(1)t(v)"),
    ("cast_unused", "SELECT 1 AS r FROM VALUES(CAST('bad' AS INT)),(1)t(v) LIMIT 0"),
    ("two_columns", "SELECT a,b FROM VALUES(1,NULL),(2,3)t(a,b)"),
])
for name, left, source in [
    ("nonnull", "7", "SELECT v FROM VALUES(0),(1)t(v)"),
    ("match", "1", "SELECT v FROM VALUES(0),(1)t(v)"),
    ("nullable_right", "7", "SELECT v FROM VALUES(0),(NULL)t(v)"),
    ("null_left", "CAST(NULL AS INT)", "SELECT v FROM VALUES(0),(1)t(v)"),
    ("empty", "7", "SELECT v FROM VALUES(0),(1)t(v) WHERE false"),
    ("null_left_empty", "CAST(NULL AS INT)", "SELECT v FROM VALUES(0),(1)t(v) WHERE false"),
    ("limited_group", "7", "SELECT v FROM VALUES(0),(1)t(v) GROUP BY v ORDER BY v LIMIT 1"),
    ("required_error", "7", "SELECT id+9223372036854775807L FROM range(2)"),
]:
    for negated in [False, True]:
        queries.append((f"in_{name}_{negated}", f"SELECT {left} {'NOT IN' if negated else 'IN'}({source}) AS r"))
for nullable in [False, True]:
    outer = "(0),(7),(NULL)" if nullable else "(0),(7)"
    for negated in [False, True]:
        queries.append((f"correlated_in_{nullable}_{negated}",
                        f"SELECT x, x {'NOT IN' if negated else 'IN'}(SELECT y FROM VALUES(0),(1)t(y) WHERE y=o.x) AS r FROM VALUES{outer}o(x)"))
assert len({name for name, _ in queries}) == len(queries)

spark = (SparkSession.builder.master("local[1]").appName("dar-values-nullability")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    records = [{"spark_version": spark.version, "timezone": "UTC"}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            try:
                frame = spark.sql(sql)
                rows = [[None if value is None else str(value).lower()
                         if isinstance(value, bool) else str(value) for value in row]
                        for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                expected = {"schema": frame.schema.jsonValue(), "rows": rows}
            except Exception as error:
                conditions = [condition for condition in ["DIVIDE_BY_ZERO", "ARITHMETIC_OVERFLOW", "CAST_INVALID_INPUT", "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION"]
                              if f"[{condition}]" in str(error)]
                assert len(conditions) == 1, str(error)
                expected = {"condition": conditions[0]}
            records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
    Path(__file__).with_name("values-nullability-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1}))
finally:
    spark.stop()
