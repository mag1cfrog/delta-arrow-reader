"""Capture STRING comparisons with Spark DATE, TIMESTAMP and TIMESTAMP_NTZ.

Use the Python/Java reference environment in README.md. Registered inputs keep
UTC instant annotations while the session timezone changes.
"""
import datetime as dt
import json
import os
from pathlib import Path
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, TimestampNTZType
from capture_temporal_comparison_spark import encode_reference_value

TYPES = [("DATE", "d"), ("TIMESTAMP_NTZ", "ntz"), ("TIMESTAMP", "ltz")]
WALL = dt.datetime(2026, 1, 2)
EPOCH = dt.datetime(1970, 1, 1)
WALL_US = (WALL - EPOCH).days * 86400_000_000
INPUT = {"strings": ["2026-01-02 00:00:00", "bad", None, "2026-01-02 00:00:00"],
         "date_days": [(WALL - EPOCH).days] * 2 + [None, None],
         "ntz_us": [WALL_US] * 2 + [None, None],
         "ltz_utc_us": [WALL_US + 7 * 3600_000_000] * 2 + [None, None]}


def queries():
    for data_type, column in TYPES:
        target = f"CAST('2026-01-02 00:00:00' AS {data_type})"
        for name, text in [("valid", "2026-01-02 00:00:00"), ("year", "2026"),
                           ("month", "2026-01"), ("fraction", "2026-01-02 00:00:00.123456789"),
                           ("offset", "2026-01-02T00:00:00-07:00"), ("bad", "bad"),
                           ("empty_string", ""), ("bad_date", "2026-02-30")]:
            string = f"'{text}'"
            for name_suffix, expression in [("equal", f"{string}={target}"),
                                           ("equal_reverse", f"{target}={string}"),
                                           ("nullif_string", f"NULLIF({string},{target})"),
                                           ("nullif_temporal", f"NULLIF({target},{string})")]:
                yield "UTC", f"{column}_{name}_{name_suffix}", f"SELECT {expression} AS r", False
        for timezone, text in [("America/Phoenix", "2026-01-02 00:00:00"),
                               ("+05:45", "2026-01-02 00:00:00"),
                               ("America/New_York", "2026-03-08 02:30:00.123456"),
                               ("America/New_York", "2026-11-01 01:30:00.123456")]:
            target = f"CAST('{text}' AS {data_type})"
            for reverse in [False, True]:
                left, right = (target, f"'{text}'") if reverse else (f"'{text}'", target)
                for name, expression in [("equal", f"{left}={right}"), ("nullif", f"NULLIF({left},{right})")]:
                    yield timezone, f"{column}_{text}_{reverse}_{name}", f"SELECT {expression} AS r", False
        for timezone in ["UTC", "America/Phoenix", "America/New_York", "+05:45"]:
            for name, expression in [("equal", f"s={column}"), ("reverse", f"{column}=s"),
                                     ("less", f"s<{column}"), ("nullsafe", f"s<=>{column}"),
                                     ("nullif_string", f"NULLIF(s,{column})"),
                                     ("nullif_temporal", f"NULLIF({column},s)")]:
                for subset in ["", " WHERE k<>1", " WHERE false"]:
                    yield timezone, f"external_{column}_{name}_{subset}", f"SELECT {expression} AS r FROM external_string_temporal{subset} ORDER BY k", column == "ltz" and name == "nullif_temporal"
        source = " FROM VALUES (0,'bad'),(1,CAST(NULL AS STRING)),(2,'2026-01-02') t(k,s)"
        for name, expression in [("known_null_right", f"NULLIF(s,CAST(NULL AS {data_type}))"),
                                 ("known_null_left", f"NULLIF(CAST(NULL AS {data_type}),s)"),
                                 ("equal_null", f"s=CAST(NULL AS {data_type})")]:
            for label, table in [("local", source), ("registered", " FROM external_string_temporal")]:
                yield "UTC", f"{column}_{name}_{label}", f"SELECT {expression} AS r{table} ORDER BY k", column == "ltz" and name == "known_null_left" and label == "registered"
        for name, expression in [("equal", f"s=CAST('2026-01-02' AS {data_type})"),
                                 ("nullif_string", f"NULLIF(s,CAST('2026-01-02' AS {data_type}))"),
                                 ("nullif_temporal", f"NULLIF(CAST('2026-01-02' AS {data_type}),s)")]:
            for suffix in [" ORDER BY k", " WHERE false", " LIMIT 0"]:
                yield "UTC", f"{column}_local_{name}_{suffix}", f"SELECT {expression} AS r{source}{suffix}", False
        for name, sql in [
            ("unused_case", f"SELECT CASE WHEN false THEN 'bad'={target} ELSE true END AS r"),
            ("empty_literal", f"SELECT 'bad'={target} AS r WHERE false"),
            ("filter", f"SELECT k AS r FROM external_string_temporal WHERE s={column} ORDER BY k"),
            ("join", f"SELECT x.k AS r FROM external_string_temporal x JOIN external_string_temporal y ON x.s=y.{column} ORDER BY x.k"),
            ("between", f"SELECT k AS r FROM external_string_temporal WHERE s BETWEEN {column} AND {column} ORDER BY k"),
        ]:
            yield "America/Phoenix", f"{column}_{name}", sql, False
        for reverse in [False, True]:
            target = f"CAST('2026-01-02' AS {data_type})"
            left, right = (target, "'bad'") if reverse else ("'bad'", target)
            expression = f"NULLIF({left},{right})"
            for name, sql in [
                ("empty", f"SELECT {expression} AS r WHERE false"),
                ("limit", f"SELECT {expression} AS r LIMIT 0"),
                ("unused", f"SELECT CASE WHEN false THEN {expression} END AS r"),
            ]:
                yield "UTC", f"{column}_nullif_literal_{reverse}_{name}", sql, False
    for data_type in ["INT", "DOUBLE", "DECIMAL(10,2)", "BOOLEAN"]:
        for reverse in [False, True]:
            target = f"CAST(1 AS {data_type})"
            left, right = (target, "'bad'") if reverse else ("'bad'", target)
            expression = f"NULLIF({left},{right})"
            for name, sql in [
                ("empty", f"SELECT {expression} AS r WHERE false"),
                ("limit", f"SELECT {expression} AS r LIMIT 0"),
                ("unused", f"SELECT CASE WHEN false THEN {expression} END AS r"),
            ]:
                yield "UTC", f"{data_type}_nullif_literal_{reverse}_{name}", sql, False
    for reverse in [False, True]:
        left, right = ("DATE '2026-01-02'", "'bad'") if reverse else ("'bad'", "DATE '2026-01-02'")
        expression = f"NULLIF({left},{right})"
        for name, sql in [
            ("empty", f"SELECT {expression} AS r WHERE false"),
            ("limit", f"SELECT {expression} AS r LIMIT 0"),
            ("unused", f"SELECT CASE WHEN false THEN {expression} END AS r"),
            ("registered_unused", f"SELECT CASE WHEN k<0 THEN {expression} END AS r FROM external_string_temporal ORDER BY k"),
            ("registered_used", f"SELECT CASE WHEN k=0 THEN {expression} END AS r FROM external_string_temporal ORDER BY k"),
            ("nested_limit", f"SELECT {expression} AS r FROM (SELECT k FROM external_string_temporal LIMIT 0) t"),
            ("nested_local_limit", f"SELECT {expression} AS r FROM (SELECT k FROM VALUES (0) t(k) LIMIT 0) t"),
            ("nested_empty", f"SELECT {expression} AS r FROM (SELECT k FROM external_string_temporal WHERE false) t"),
        ]:
            yield "UTC", f"d_nullif_typed_literal_{reverse}_{name}", sql, False


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    spark = (SparkSession.builder.master("local[1]").appName("dar-string-temporal")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    observations = []
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        rows = [(k, text, None if day is None else EPOCH.date() + dt.timedelta(days=day),
                 None if ntz is None else EPOCH + dt.timedelta(microseconds=ntz),
                 None if ltz is None else (EPOCH + dt.timedelta(microseconds=ltz)).replace(tzinfo=dt.timezone.utc))
                for k, (text, day, ntz, ltz) in enumerate(zip(INPUT["strings"], INPUT["date_days"], INPUT["ntz_us"], INPUT["ltz_utc_us"]))]
        schema = StructType([StructField("k", IntegerType(), False), StructField("s", StringType()),
                             StructField("d", DateType()), StructField("ntz", TimestampNTZType()), StructField("ltz", TimestampType())])
        spark.createDataFrame(rows, schema).createOrReplaceTempView("external_string_temporal")
        for timezone, name, sql, original_ltz in queries():
            spark.conf.set("spark.sql.session.timeZone", timezone)
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                try:
                    frame = spark.sql(sql)
                    schema = frame.schema.jsonValue()
                    rows = [[encode_reference_value(row[0], schema["fields"][0]["type"])] for row in frame.collect()]
                    expected = {"status": "ok", "schema": schema, "rows": rows}
                except Exception as error:
                    condition = getattr(error, "getCondition", lambda: None)()
                    if condition not in ["CAST_INVALID_INPUT", "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION"]:
                        raise
                    expected = {"status": "error", "condition": condition}
                record = {"id": name, "sql": sql, "timezone": timezone, "ansi": ansi, "expected": expected}
                if original_ltz:
                    record["result_timezone"] = "UTC"
                if expected["status"] == "error" and name.startswith(("ntz_", "ltz_", "external_ntz_", "external_ltz_")):
                    record["error_representation"] = "timestamp_parser"
                if ansi and name.endswith("_equal_null_local"):
                    record["retained_difference"] = "local_null_comparison_633"
                observations.append(record)
        assert len({(r["id"], r["ansi"], r["timezone"]) for r in observations}) == len(observations)
        metadata = {"spark_version": spark.version, "external_input": INPUT,
                    "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version")}
        Path(__file__).with_name("string-temporal-spark.jsonl").write_text(
            "\n".join(json.dumps(record, separators=(",", ":")) for record in [metadata, *observations]) + "\n")
        print(json.dumps({"observations": len(observations), "errors": sum(r["expected"]["status"] == "error" for r in observations)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
