"""Capture interval qualifiers and resolved names in an existing Spark 4.2.0 env.

Pass a fresh output path. Existing captures are never overwritten.
"""
import json
import os
from pathlib import Path
import sys

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, functions, types

from capture_floating_group_keys_spark import encode


INTERVALS = [
    ("year", "INTERVAL '2' YEAR", "INTERVAL '0' YEAR"),
    ("year_month", "INTERVAL '2-3' YEAR TO MONTH", "INTERVAL '0-0' YEAR TO MONTH"),
    ("month", "INTERVAL '27' MONTH", "INTERVAL '0' MONTH"),
    ("day", "INTERVAL '2' DAY", "INTERVAL '0' DAY"),
    ("day_hour", "INTERVAL '2 03' DAY TO HOUR", "INTERVAL '0 00' DAY TO HOUR"),
    ("day_minute", "INTERVAL '2 03:04' DAY TO MINUTE", "INTERVAL '0 00:00' DAY TO MINUTE"),
    ("day_second", "INTERVAL '2 03:04:05.123456' DAY TO SECOND",
     "INTERVAL '0 00:00:00' DAY TO SECOND"),
    ("hour", "INTERVAL '51' HOUR", "INTERVAL '0' HOUR"),
    ("hour_minute", "INTERVAL '51:04' HOUR TO MINUTE", "INTERVAL '0:00' HOUR TO MINUTE"),
    ("hour_second", "INTERVAL '51:04:05.123456' HOUR TO SECOND",
     "INTERVAL '0:00:00' HOUR TO SECOND"),
    ("minute", "INTERVAL '3064' MINUTE", "INTERVAL '0' MINUTE"),
    ("minute_second", "INTERVAL '3064:05.123456' MINUTE TO SECOND",
     "INTERVAL '0:00' MINUTE TO SECOND"),
    ("second", "INTERVAL '183845.123456' SECOND", "INTERVAL '0' SECOND"),
]


def queries():
    timestamp = "TIMESTAMP '2020-01-01 00:00:00.123456'"
    for name, interval, zero in INTERVALS:
        for kind, expression in [("literal", interval), ("zero", zero),
                                 ("negative", f"-({interval})")]:
            yield f"{name}_{kind}", "literal", f"SELECT {expression}"
        for operation, expression in [
            ("left_multiply", f"id*({interval})"),
            ("right_multiply", f"({interval})*id"),
            ("divide", f"({interval})/2"),
            ("add", f"({interval})+({zero})"),
        ]:
            yield f"{name}_{operation}", "arithmetic", f"SELECT {expression} FROM inputs"
        for operation, expression in [
            ("add", f"{timestamp}+({interval})"),
            ("reverse_add", f"({interval})+{timestamp}"),
            ("subtract", f"{timestamp}-({interval})"),
            ("computed", f"{timestamp}+id*({interval})"),
        ]:
            yield f"timestamp_{name}_{operation}", "timestamp", f"SELECT {expression} FROM inputs"
    for name, interval in [
        ("years", "INTERVAL 2 YEARS"), ("months", "INTERVAL 27 MONTHS"),
        ("days", "INTERVAL 2 DAYS"), ("hours", "INTERVAL 51 HOURS"),
        ("minutes", "INTERVAL 3064 MINUTES"), ("seconds", "INTERVAL 1.123456 SECONDS"),
        ("week", "INTERVAL 1 WEEK"), ("millisecond", "INTERVAL 1 MILLISECOND"),
        ("microsecond", "INTERVAL 1 MICROSECOND"),
        ("multi_ym", "INTERVAL 2 YEARS 3 MONTHS"),
        ("multi_zero_ym", "INTERVAL 0 YEARS 0 MONTHS"),
        ("multi_dt", "INTERVAL 2 DAYS 3 HOURS 4 MINUTES 5.123456 SECONDS"),
        ("multi_hour_minute", "INTERVAL 0 HOURS 0 MINUTES"),
        ("multi_zero_day_hour", "INTERVAL 0 DAYS 0 HOURS"),
        ("multi_cancel_ym", "INTERVAL 1 YEAR -12 MONTHS"),
        ("multi_cancel_dt", "INTERVAL 1 DAY -24 HOURS"),
        ("string", "INTERVAL '2 days 3 hours'"),
        ("mixed", "INTERVAL 1 MONTH 1 DAY"),
        ("mixed_zero", "INTERVAL 0 MONTHS 0 DAYS"),
        ("precision", "INTERVAL '0.123456789' SECOND"),
    ]:
        yield f"syntax_{name}", "syntax", f"SELECT {interval}"
    for name, expression in [
        ("day_hour", "INTERVAL 1 DAY + INTERVAL 1 HOUR"),
        ("hour_day", "INTERVAL 1 HOUR + INTERVAL 1 DAY"),
        ("year_month", "INTERVAL 1 YEAR + INTERVAL 1 MONTH"),
        ("month_year", "INTERVAL 1 MONTH - INTERVAL 1 YEAR"),
        ("fractional_multiply", "INTERVAL 1 DAY * 0.5"),
        ("fractional_divide", "INTERVAL 1 DAY / 3"),
        ("null_multiply", "INTERVAL 1 DAY * CAST(NULL AS INT)"),
    ]:
        yield f"arithmetic_{name}", "arithmetic", f"SELECT {expression}"
    for name, _, _ in INTERVALS:
        qualifier = name.replace("_", " TO ").upper()
        family = "YEAR TO MONTH" if name in ["year", "year_month", "month"] else "DAY TO SECOND"
        source = "INTERVAL '2-3' YEAR TO MONTH" if family == "YEAR TO MONTH" else (
            "INTERVAL '2 03:04:05.123456' DAY TO SECOND")
        yield f"cast_{name}", "cast", f"SELECT CAST({source} AS INTERVAL {qualifier})"
        yield f"cast_null_{name}", "cast", f"SELECT CAST(NULL AS INTERVAL {qualifier})"
    key = "TIMESTAMP '2020-01-01 00:00:00'+id*INTERVAL 1 DAY"
    for name, bound in [
        ("day", "INTERVAL 1 DAY"), ("hour", "INTERVAL 24 HOURS"),
        ("day_hour", "INTERVAL '1 00' DAY TO HOUR"),
        ("day_second", "INTERVAL '1 00:00:00' DAY TO SECOND"),
        ("year", "INTERVAL 1 YEAR"), ("month", "INTERVAL 1 MONTH"),
        ("numeric", "1"), ("mixed", "INTERVAL 1 MONTH 1 DAY"),
    ]:
        expression = f"SUM(v) OVER (ORDER BY {key} RANGE BETWEEN {bound} PRECEDING AND CURRENT ROW)"
        yield f"range_{name}", "window", f"SELECT id,{expression} FROM inputs"
        yield f"range_{name}_aliased", "window_alias", f"SELECT id,{expression} AS r FROM inputs"
    for name, bound in [("day", "INTERVAL 1 DAY"), ("hour", "INTERVAL 24 HOURS"),
                        ("day_hour", "INTERVAL '1 00' DAY TO HOUR"),
                        ("zero_year", "INTERVAL 0 YEAR")]:
        expression = ("SUM(v) OVER (ORDER BY DATE_ADD(DATE '2020-01-01',id) "
                      f"RANGE BETWEEN {bound} PRECEDING AND CURRENT ROW)")
        yield f"date_range_{name}", "date_window", f"SELECT id,{expression} AS r FROM inputs"


def capture_query(spark, sql):
    try:
        frame = spark.sql(sql)
        schema = frame.schema.jsonValue()
        # Keep interval support and timestamp formatting independent of Python's host timezone.
        columns = [functions.col("`" + field.name.replace("`", "``") + "`")
                   for field in frame.schema]
        values = frame.select(*[
            (column.cast("string") if isinstance(field.dataType,
             (types.YearMonthIntervalType, types.DayTimeIntervalType,
              types.TimestampType, types.TimestampNTZType)) else column).alias(str(index))
            for index, (column, field) in enumerate(zip(columns, frame.schema))])
        rows = [encode(row, values.schema) for row in values.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        return {"schema": schema, "rows": rows}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        cause = getattr(error, "java_exception", None)
        while condition is None and cause is not None:
            try:
                condition = cause.getCondition()
            except Py4JError:
                pass
            cause = cause.getCause()
        assert condition is not None, (sql, str(error))
        return {"condition": condition}


def main():
    output = Path(sys.argv[1])
    assert not output.exists(), output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-interval-qualifiers")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([types.StructField("id", types.IntegerType(), False),
                                   types.StructField("v", types.IntegerType(), True)])
        rows = [(0, 2), (1, 3), (2, None), (3, 5)]
        spark.createDataFrame(rows, schema).createOrReplaceTempView("inputs")
        records = [{"spark_version": spark.version, "timezone": "UTC",
                    "interval_encoding": "cast_string", "timestamp_encoding": "cast_string", "inputs": [
                        {"name": "inputs", "schema": schema.jsonValue(),
                         "rows": [encode(row, schema) for row in rows]}]}]
        definitions = list(queries())
        assert len({name for name, _, _ in definitions}) == len(definitions)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, context, sql in definitions:
                records.append({"id": name, "context": context, "ansi": ansi,
                                "sql": sql, "expected": capture_query(spark, sql)})
        with output.open("x") as destination:
            destination.writelines(json.dumps(record, separators=(",", ":")) + "\n"
                                   for record in records)
        print(json.dumps({"observations": len(records) - 1, "output": str(output),
                          "errors": sum("condition" in r["expected"] for r in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
