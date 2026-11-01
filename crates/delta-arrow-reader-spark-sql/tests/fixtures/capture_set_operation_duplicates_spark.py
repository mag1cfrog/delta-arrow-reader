"""Capture duplicate-preserving set operations with PySpark 4.2.0 and Java 21."""
import json
import math
from pathlib import Path

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, types


def display(value):
    if value is None:
        return None
    if isinstance(value, float):
        return "NaN" if math.isnan(value) else format(value, ".17g")
    return str(value)


inputs = [
    {"name": f"{side}_{kind}", "type": kind,
     "rows": ["nan", "nan", "1", "1", None, None] if side == "left"
     else ["nan", "1", None]}
    for kind in ["float", "double"] for side in ["left", "right"]
]
pairs = [
    ("unbalanced", "SELECT v FROM VALUES(1),(1),(1) l(v)", "SELECT v FROM VALUES(1) r(v)"),
    ("balanced", "SELECT v FROM VALUES(1),(1) l(v)", "SELECT v FROM VALUES(1),(1) r(v)"),
    ("disjoint", "SELECT v FROM VALUES(1),(1) l(v)", "SELECT v FROM VALUES(2),(2) r(v)"),
    ("mixed_counts", "SELECT v FROM VALUES(1),(1),(1),(2),(2),(NULL),(NULL) l(v)",
     "SELECT v FROM VALUES(1),(1),(2),(NULL),(NULL),(NULL) r(v)"),
    ("all_null", "SELECT v FROM VALUES(CAST(NULL AS INT)),(CAST(NULL AS INT)) l(v)",
     "SELECT CAST(NULL AS INT) AS v"),
    ("untyped_null", "SELECT v FROM VALUES(NULL),(NULL) l(v)", "SELECT NULL AS v"),
    ("empty", "SELECT CAST(id AS INT) AS v FROM range(0)", "SELECT v FROM VALUES(1),(1) r(v)"),
    ("both_empty", "SELECT id AS v FROM range(0)", "SELECT id AS v FROM range(0)"),
    ("multiple_columns", "SELECT v,label FROM VALUES(1,'a'),(1,'a'),(1,'b'),(NULL,'a') l(v,label)",
     "SELECT v,label FROM VALUES(1,'a'),(1,'b'),(1,'b'),(NULL,'a'),(NULL,'a') r(v,label)"),
    ("second_column_counts", "SELECT v,label FROM VALUES(1,'a'),(1,'a'),(1,'b') l(v,label)",
     "SELECT 1 AS v,'b' AS label"),
    ("coerced_duplicates", "SELECT v FROM VALUES(1),(1),(1) l(v)",
     "SELECT v FROM VALUES('1'),('01') r(v)"),
    ("integer_width", "SELECT v FROM VALUES(1),(1),(1) l(v)",
     "SELECT CAST(v AS BIGINT) AS v FROM VALUES(1),(1) r(v)"),
    ("decimal_scale", "SELECT CAST(v AS DECIMAL(5,2)) AS v FROM VALUES(1),(1),(1) l(v)",
     "SELECT CAST(v AS DECIMAL(6,3)) AS v FROM VALUES(1),(1) r(v)"),
]
for kind in ["float", "double"]:
    pairs.append((f"nan_{kind}", f"SELECT v FROM left_{kind}", f"SELECT v FROM right_{kind}"))
queries = []
for op in ["UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]:
    for name, left, right in pairs:
        for reverse in [False, True]:
            first, second = (right, left) if reverse else (left, right)
            queries.append((f"{name}_{op.lower().replace(' ', '_')}_{reverse}", f"{first} {op} {second}"))
queries += [
    ("cte_reuse", "WITH q AS (SELECT v FROM VALUES(1),(1),(1),(2) t(v)) "
     "SELECT v AS result FROM q INTERSECT ALL SELECT v FROM q WHERE v=1 LIMIT 2"),
    ("column_count", "SELECT 1 AS v INTERSECT ALL SELECT 1,2"),
    ("required_cast", "SELECT 'bad' AS v INTERSECT ALL SELECT 1"),
]
assert len(dict(queries)) == len(queries)

spark = (SparkSession.builder.master("local[1]").appName("dar-set-operation-duplicates")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    for source in inputs:
        data_type = types.FloatType() if source["type"] == "float" else types.DoubleType()
        schema = types.StructType([types.StructField("v", data_type, nullable=True)])
        rows = [(None if value is None else float(value),) for value in source["rows"]]
        spark.createDataFrame(rows, schema).createOrReplaceTempView(source["name"])
    records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": inputs}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            try:
                frame = spark.sql(sql)
                schema = frame.schema.jsonValue()
                rows = [[display(value) for value in row] for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
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
            records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
    Path(__file__).with_name("set-operation-duplicates-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1,
                      "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
finally:
    spark.stop()
