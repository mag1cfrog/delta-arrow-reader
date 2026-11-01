"""Capture scalar set-operation coercion with PySpark 4.2.0 and Java 21."""
import datetime
import json
import os
import time
from pathlib import Path

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession


def display(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, datetime.datetime):
        return str((value - datetime.datetime(1970, 1, 1)) // datetime.timedelta(microseconds=1))
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return str(value)


os.environ["TZ"] = "UTC"
time.tzset()

spark = (SparkSession.builder.master("local[1]").appName("dar-set-operation-coercion")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    pairs = [
        ("integer_width", "CAST(1 AS INT)", "CAST(1 AS BIGINT)"),
        ("small_integer", "CAST(1 AS TINYINT)", "CAST(1 AS SMALLINT)"),
        ("decimal_scale", "CAST(1 AS DECIMAL(5,2))", "CAST(1 AS DECIMAL(6,3))"),
        ("decimal_integer", "CAST(1 AS DECIMAL(5,2))", "CAST(1 AS BIGINT)"),
        ("decimal_float", "CAST(1 AS DECIMAL(5,2))", "CAST(1 AS FLOAT)"),
        ("float_integer", "CAST(1 AS FLOAT)", "CAST(1 AS INT)"),
        ("string_integer", "'01'", "CAST(1 AS INT)"),
        ("string_float", "'1.0'", "CAST(1 AS FLOAT)"),
        ("string_boolean", "'true'", "true"),
        ("string_binary", "'abc'", "X'616263'"),
        ("untyped_null", "NULL", "CAST(NULL AS BIGINT)"),
        ("typed_null", "CAST(NULL AS INT)", "CAST(NULL AS BIGINT)"),
        ("date_timestamp", "DATE'2020-01-02'", "TIMESTAMP'2020-01-02 00:00:00'"),
        ("date_ntz", "DATE'2020-01-02'", "TIMESTAMP_NTZ'2020-01-02 00:00:00'"),
        ("ntz_ltz", "TIMESTAMP_NTZ'2020-01-02 00:00:00'", "TIMESTAMP'2020-01-02 00:00:00'"),
        ("string_date", "'2020-01-02'", "DATE'2020-01-02'"),
        ("string_timestamp", "'2020-01-02 00:00:00'", "TIMESTAMP'2020-01-02 00:00:00'"),
        ("invalid_integer", "'bad'", "CAST(1 AS INT)"),
        ("incompatible_boolean_integer", "true", "1"),
    ]
    queries = []
    for op in ["UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]:
        for name, left, right in pairs:
            for reverse in [False, True]:
                first, second = (right, left) if reverse else (left, right)
                queries.append((f"{name}_{op.lower().replace(' ', '_')}_{reverse}",
                                f"SELECT {first} AS result {op} SELECT {second} AS other"))
    queries += [
        ("multi_column", "SELECT CAST(1 AS INT) AS id,CAST(2 AS DECIMAL(4,1)) AS amount "
         "UNION ALL SELECT CAST(2 AS BIGINT),CAST(3 AS DECIMAL(5,2))"),
        ("union_by_name", "SELECT CAST(1 AS INT) AS id,CAST(2 AS DECIMAL(4,1)) AS amount "
         "UNION ALL BY NAME SELECT CAST(3 AS DECIMAL(5,2)) AS amount,CAST(2 AS BIGINT) AS id"),
        ("empty_left", "SELECT CAST(id AS INT) AS result FROM range(0) "
         "UNION ALL SELECT CAST(1 AS BIGINT)"),
        ("empty_right", "SELECT CAST(1 AS INT) AS result EXCEPT "
         "SELECT CAST(id AS BIGINT) FROM range(0)"),
        ("empty_both", "SELECT CAST(id AS INT) AS result FROM range(0) INTERSECT "
         "SELECT CAST(id AS BIGINT) FROM range(0)"),
        ("cte_reuse", "WITH q AS (SELECT 1 AS value) SELECT value AS result FROM q "
         "UNION ALL SELECT CAST(value AS BIGINT) FROM q"),
        ("coerced_duplicates", "SELECT CAST(v AS INT) AS result FROM VALUES(1),(1) l(v) "
         "EXCEPT ALL SELECT v FROM VALUES('1'),('01') r(v)"),
        ("distinct_after_coercion", "SELECT v AS result FROM VALUES('1'),('01') l(v) "
         "INTERSECT SELECT 1"),
        ("both_untyped_null", "SELECT NULL AS result UNION SELECT NULL"),
        ("column_count", "SELECT 1 AS result UNION SELECT 1,2"),
    ]
    queries += [
        ("invalid_float", "SELECT 'bad' AS result UNION ALL SELECT CAST(1 AS FLOAT)"),
        ("invalid_boolean", "SELECT 'bad' AS result UNION ALL SELECT true"),
        ("invalid_date", "SELECT 'bad' AS result UNION ALL SELECT DATE'2020-01-02'"),
        ("invalid_timestamp", "SELECT 'bad' AS result UNION ALL SELECT TIMESTAMP'2020-01-02 00:00:00'"),
        ("integer_overflow", "SELECT '9223372036854775808' AS result UNION ALL SELECT CAST(1 AS BIGINT)"),
        ("decimal_precision", "SELECT CAST(1.1 AS DECIMAL(38,20)) AS result UNION ALL SELECT CAST(1 AS BIGINT)"),
    ]
    for op in ["UNION", "UNION ALL"]:
        for index, expressions in enumerate([
            ["'1.0'", "1", "CAST(1 AS FLOAT)"],
            ["1", "'1.0'", "CAST(1 AS FLOAT)"],
            ["CAST(1 AS FLOAT)", "1", "'1.0'"],
        ]):
            queries.append((f"three_inputs_{op.lower().replace(' ', '_')}_{index}",
                            f" {op} ".join(f"SELECT {value} AS result" for value in expressions)))
    queries = [(name, sql, "UTC") for name, sql in queries]
    for timezone in ["America/Phoenix", "America/New_York"]:
        queries += [
            ("zoned_date", "SELECT DATE'2020-03-08' AS result UNION ALL SELECT TIMESTAMP'2020-03-08 00:00:00'", timezone),
            ("zoned_ntz", "SELECT TIMESTAMP_NTZ'2020-03-08 02:30:00' AS result UNION ALL SELECT TIMESTAMP'2020-03-08 03:30:00'", timezone),
            ("zoned_string", "SELECT '2020-03-08 03:30:00' AS result INTERSECT SELECT TIMESTAMP'2020-03-08 03:30:00'", timezone),
        ]
    assert len({(name, timezone) for name, _, timezone in queries}) == len(queries)
    records = [{"spark_version": spark.version, "timezone": "UTC"}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql, timezone in queries:
            spark.conf.set("spark.sql.session.timeZone", timezone)
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
            records.append({"id": name, "ansi": ansi, "timezone": timezone, "sql": sql, "expected": expected})
    by_name = []
    for missing in [False, True]:
        right = ("SELECT CAST(2 AS BIGINT) AS id,CAST(3 AS SMALLINT) AS extra" if missing
                 else "SELECT 'second' AS label,CAST(2 AS BIGINT) AS id")
        frame = spark.sql("SELECT 1 AS id,'first' AS label").unionByName(
            spark.sql(right), allowMissingColumns=missing)
        by_name.append({"allow_missing": missing, "schema": frame.schema.jsonValue(),
                        "rows": [[display(value) for value in row] for row in frame.collect()]})
    records[0]["by_name"] = by_name
    Path(__file__).with_name("set-operation-coercion-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1,
                      "errors": sum(r.get("expected", {}).get("status") == "error" for r in records)}))
finally:
    spark.stop()
