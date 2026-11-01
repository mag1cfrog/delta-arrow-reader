"""Capture relational results over explicit nullable, duplicate-preserving inputs.

Use PySpark 4.2.0 and Java 21. Rust replays these inputs without Spark installed.
The four IN truth tables also retain the approved SQL three-valued reference.
"""
import json
import math
from pathlib import Path

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, types


inputs = [
    {"name": "left_inputs", "types": ["integer", "integer", "string"],
     "columns": ["k", "n", "label"],
     "rows": [[None, 10, "null-left"], [1, 20, "a"], [2, 30, "b"],
              [2, 31, "b2"], [4, None, None]]},
    {"name": "right_inputs", "types": ["long", "integer", "string"],
     "columns": ["k", "w", "label"],
     "rows": [[None, 100, "null-right"], [2, 200, "x"],
              [2, 201, "x2"], [3, None, "y"]]},
    {"name": "empty_inputs", "types": ["integer", "integer", "string"],
     "columns": ["k", "n", "label"], "rows": []},
    {"name": "float_left", "types": ["double"], "columns": ["k"],
     "rows": [["nan"], ["0.0"], ["-0.0"], ["1.0"], [None], ["nan"]]},
    {"name": "float_right", "types": ["double"], "columns": ["k"],
     "rows": [["nan"], ["0.0"], ["2.0"], [None]]},
    {"name": "zero_positive", "types": ["double"], "columns": ["k"],
     "rows": [["0.0"], ["0.0"]]},
    {"name": "zero_negative", "types": ["double"], "columns": ["k"],
     "rows": [["-0.0"]]},
]
queries = [
    ("projection", "SELECT k,n,label FROM left_inputs ORDER BY k NULLS FIRST,n"),
    ("projection_alias", "SELECT n+1 AS next_n,label AS text FROM left_inputs ORDER BY k NULLS FIRST,n"),
    ("projection_empty", "SELECT k,n,label FROM empty_inputs"),
    ("filter", "SELECT k,n FROM left_inputs WHERE n>=20 AND k<>4 ORDER BY k,n"),
    ("filter_null", "SELECT k,n FROM left_inputs WHERE k IS NULL OR n IS NULL ORDER BY k NULLS FIRST,n"),
    ("filter_empty", "SELECT k,n FROM left_inputs WHERE k>99"),
    ("cross_join", "SELECT l.k AS lk,r.k AS rk FROM left_inputs l CROSS JOIN right_inputs r WHERE l.k=1 AND r.k=3"),
    ("cte_chain", "WITH a AS (SELECT k,n FROM left_inputs), b AS (SELECT k,n+1 AS n FROM a WHERE k=2) SELECT k,n FROM b ORDER BY k,n"),
    ("cte_reuse", "WITH a AS (SELECT k,n FROM left_inputs WHERE k=2) SELECT l.n AS ln,r.n AS rn FROM a l JOIN a r ON l.k=r.k ORDER BY ln,rn"),
    ("cte_nested", "WITH a AS (WITH b AS (SELECT k,n FROM left_inputs) SELECT k,n FROM b WHERE k=1) SELECT k,n FROM a"),
    ("cte_shadow", "WITH a AS (SELECT k,n FROM left_inputs WHERE k=1) SELECT k,n FROM (WITH a AS (SELECT k,n FROM left_inputs WHERE k=2) SELECT k,n FROM a) nested ORDER BY k,n"),
    ("scalar_one", "SELECT (SELECT n FROM left_inputs WHERE k=1) AS r"),
    ("scalar_empty", "SELECT (SELECT n FROM left_inputs WHERE k=99) AS r"),
    ("scalar_many", "SELECT (SELECT k FROM left_inputs WHERE k=2) AS r"),
    ("correlated_sum", "SELECT l.k,(SELECT SUM(r.w) FROM right_inputs r WHERE r.k=l.k) AS total FROM left_inputs l ORDER BY l.k NULLS FIRST,l.n"),
    ("correlated_count", "SELECT l.k,(SELECT COUNT(*) FROM right_inputs r WHERE r.k=l.k) AS total FROM left_inputs l ORDER BY l.k NULLS FIRST,l.n"),
    ("values_decimal", "SELECT v FROM VALUES (1.25),(2),(CAST(NULL AS DECIMAL(5,2))) t(v) ORDER BY v NULLS FIRST"),
    ("values_decimal_explicit", "SELECT v FROM VALUES (CAST(1.25 AS DECIMAL(12,2))),(CAST(2 AS DECIMAL(12,2))) t(v) ORDER BY v"),
    ("intersect_all_unbalanced", "SELECT v FROM (SELECT v FROM VALUES(1),(1),(1) l(v) INTERSECT ALL SELECT v FROM VALUES(1) r(v)) result"),
    ("intersect_all_unbalanced_reversed", "SELECT v FROM (SELECT v FROM VALUES(1) l(v) INTERSECT ALL SELECT v FROM VALUES(1),(1),(1) r(v)) result"),
]
for join in ["INNER", "LEFT", "RIGHT", "FULL", "LEFT SEMI", "LEFT ANTI"]:
    selected = "l.k AS lk,l.n" if join in ["LEFT SEMI", "LEFT ANTI"] else "l.k AS lk,r.k AS rk,l.n,r.w"
    order = "lk NULLS FIRST,n" if join in ["LEFT SEMI", "LEFT ANTI"] else "lk NULLS FIRST,rk NULLS FIRST,n,w"
    for name, comparison in [("equal", "="), ("null_safe", "<=>")]:
        queries.append((f"join_{join.lower().replace(' ', '_')}_{name}",
                        f"SELECT {selected} FROM left_inputs l {join} JOIN right_inputs r "
                        f"ON l.k {comparison} r.k ORDER BY {order}"))
for join in ["INNER", "LEFT", "RIGHT", "FULL"]:
    queries.append((f"using_{join.lower()}",
                    f"SELECT k,l.n,r.w FROM left_inputs l {join} JOIN right_inputs r "
                    "USING(k) ORDER BY k NULLS FIRST,n,w"))
    queries.append((f"empty_join_{join.lower()}",
                    f"SELECT l.k AS lk,r.k AS rk FROM empty_inputs l {join} JOIN right_inputs r "
                    "ON l.k=r.k ORDER BY lk NULLS FIRST,rk NULLS FIRST"))
for operation in ["UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]:
    for name, left, right in [("integer", "left_inputs", "right_inputs"),
                              ("float", "float_left", "float_right"),
                              ("zero", "zero_positive", "zero_negative")]:
        expression = "CAST(k AS BIGINT)" if name == "integer" else "k"
        queries.append((f"set_{name}_{operation.lower().replace(' ', '_')}",
                        f"SELECT k FROM (SELECT {expression} AS k FROM {left} {operation} "
                        f"SELECT {expression} AS k FROM {right}) result ORDER BY k NULLS FIRST"))
for negated in [False, True]:
    op = "NOT EXISTS" if negated else "EXISTS"
    queries.append((op.lower().replace(" ", "_"),
                    f"SELECT l.k,{op}(SELECT r.k FROM right_inputs r WHERE r.k=l.k) AS found "
                    "FROM left_inputs l ORDER BY l.k NULLS FIRST,l.n"))
    for empty in [False, True]:
        name = ("not_in" if negated else "in") + ("_empty" if empty else "")
        condition = " WHERE false" if empty else ""
        queries.append((name, f"SELECT k,k {'NOT IN' if negated else 'IN'} "
                        f"(SELECT k FROM right_inputs{condition}) AS found "
                        "FROM left_inputs ORDER BY k NULLS FIRST,n"))
assert len(dict(queries)) == len(queries)


def display_value(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        if value == 0:
            return "0"
        return format(value, ".17g")
    return str(value)


spark = (SparkSession.builder.master("local[1]").appName("dar-relational-queries")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    for source in inputs:
        schema = types.StructType.fromJson({"type": "struct", "fields": [
            {"name": name, "type": kind, "nullable": True, "metadata": {}}
            for name, kind in zip(source["columns"], source["types"])]})
        rows = [[float(value) if kind == "double" and value is not None else value
                 for kind, value in zip(source["types"], row)] for row in source["rows"]]
        spark.createDataFrame(rows, schema).createOrReplaceTempView(source["name"])
    records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": inputs}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            try:
                frame = spark.sql(sql)
                schema = frame.schema.jsonValue()
                rows = [[display_value(value) for value in row] for row in frame.collect()]
                expected = {"status": "ok", "schema": schema, "rows": rows}
            except Exception as error:
                condition = getattr(error, "getCondition", lambda: None)()
                cause = getattr(error, "java_exception", None)
                while condition is None and cause is not None:
                    try:
                        condition = cause.getCondition()
                    except Py4JError:
                        pass
                    cause = cause.getCause()
                assert condition is not None, (name, str(error))
                expected = {"status": "error", "condition": condition}
            record = {"id": name, "ansi": ansi, "sql": sql, "expected": expected}
            if name in ["in", "not_in", "in_empty", "not_in_empty"]:
                found = {"in": [None, None, "true", "true", None],
                         "not_in": [None, None, "false", "false", None],
                         "in_empty": ["false"] * 5, "not_in_empty": ["true"] * 5}[name]
                record["sql_null_rows"] = list(map(list, zip([None, "1", "2", "2", "4"], found)))
            records.append(record)
    Path(__file__).with_name("relational-queries-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1,
                      "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
finally:
    spark.stop()
