"""Capture Spark arithmetic of a DATE and a day-time or year-month interval (#952).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for `+`, `-`, TRY_ADD and TRY_SUBTRACT
of a DATE and an interval. Spark adds a DAY interval to a DATE with `date_add` and returns a
DATE, and adds a day-time interval with time units to the DATE cast to TIMESTAMP and returns a
TIMESTAMP. The intervals are literals of every day-time qualifier, casts, negations, sums,
products, MAKE_DT_INTERVAL, DATE, TIMESTAMP and TIMESTAMP_NTZ differences, conditionals, ABS,
aggregates and a window over intervals, typed and untyped NULL, and subquery columns; the
DATE values include month ends, a leap day, the Los Angeles DST change days, years 1 and 9999
and NULL. Every query runs in both ANSI modes in UTC and America/Los_Angeles. Run from the
crate root in the reference environment described in README.md; the output path must not
exist.
"""
import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles"]

# DATE values: month ends, a leap day, the Los Angeles DST change days, the DATE limits Spark
# reads and NULL.
DATES = ["2020-01-01", "2020-01-31", "2020-02-29", "2020-03-08", "2020-11-01", "0001-01-01",
         "9999-12-31", None]


def days(text):
    return (date.fromisoformat(text) - date(1970, 1, 1)).days


INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "d", "type": "date", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), None if day is None else str(days(day))]
             for key, day in enumerate(DATES)]}]

# A day-time interval with time units: Spark returns a TIMESTAMP.
TIME_UNITS = [
    ("plus_hour", "d + INTERVAL '2' HOUR"),
    ("minus_hour", "d - INTERVAL '2' HOUR"),
    ("hour_plus", "INTERVAL '2' HOUR + d"),
    ("plus_negative_minute", "d + INTERVAL '-30' MINUTE"),
    ("plus_second", "d + INTERVAL '45' SECOND"),
    ("plus_fraction", "d + INTERVAL '1.5' SECOND"),
    ("plus_day_hour", "d + INTERVAL '1 02' DAY TO HOUR"),
    ("minus_day_second", "d - INTERVAL '1 02:03:04.5' DAY TO SECOND"),
    ("plus_hour_minute", "d + INTERVAL '02:30' HOUR TO MINUTE"),
    ("plus_24_hours", "d + INTERVAL '24' HOUR"),
    ("plus_multi_unit", "d + INTERVAL 2 HOURS 30 MINUTES"),
    ("plus_cast_hour", "d + CAST(3 AS INTERVAL HOUR)"),
    ("plus_null_hour", "d + CAST(NULL AS INTERVAL HOUR)"),
    ("plus_make_dt", "d + make_dt_interval(1, 2, 3, 4.5)"),
    ("plus_timestamp_difference",
     "d + (TIMESTAMP '2020-01-02 03:00:00' - TIMESTAMP '2020-01-01 00:00:00')"),
    ("plus_negated_hour", "d + -INTERVAL '2' HOUR"),
    ("plus_day_plus_hour", "d + (INTERVAL '1' DAY + INTERVAL '2' HOUR)"),
    ("plus_day_times_two", "d + INTERVAL '1' DAY * 2"),
    ("plus_hours_halved", "d + INTERVAL '3' HOUR / 2"),
    ("try_add_hour", "try_add(d, INTERVAL '2' HOUR)"),
    ("try_add_hour_first", "try_add(INTERVAL '2' HOUR, d)"),
    ("try_subtract_hour", "try_subtract(d, INTERVAL '2' HOUR)"),
    ("plus_ntz_difference",
     "d + (TIMESTAMP_NTZ '2020-01-01 03:00:00' - TIMESTAMP_NTZ '2020-01-01 00:00:00')"),
    ("plus_ntz_minus_null", "d + (TIMESTAMP_NTZ '2020-01-01 03:00:00' - NULL)"),
    ("plus_negated_timestamp_difference",
     "d + -(TIMESTAMP '2020-01-02 03:00:00' - TIMESTAMP '2020-01-01 00:00:00')"),
    ("plus_timestamp_difference_plus_day",
     "d + ((TIMESTAMP '2020-01-02 03:00:00' - TIMESTAMP '2020-01-01 00:00:00') "
     "+ INTERVAL '1' DAY)"),
    ("plus_if_hour", "d + if(k > 3, INTERVAL '2' HOUR, INTERVAL '3' HOUR)"),
    ("plus_if_product", "d + if(k > 3, INTERVAL '1' DAY * 2, INTERVAL '1' DAY)"),
    ("plus_case_day_or_hour",
     "d + CASE WHEN k > 3 THEN INTERVAL '2' HOUR ELSE INTERVAL '1' DAY END"),
    ("plus_coalesce_hour", "d + coalesce(INTERVAL '2' HOUR, INTERVAL '3' HOUR)"),
    ("plus_nullif_hour", "d + nullif(INTERVAL '2' HOUR, INTERVAL '3' HOUR)"),
    ("plus_greatest_day_hour", "d + greatest(INTERVAL '2' HOUR, INTERVAL '1' DAY)"),
    ("plus_abs_hour", "d + abs(INTERVAL '-2' HOUR)"),
    ("plus_window_max_hour", "d + max(INTERVAL '2' HOUR) OVER ()"),
    # Spark reads an untyped NULL beside a DATE as a DAY TO SECOND interval.
    ("plus_null", "d + NULL"),
    ("null_plus", "NULL + d"),
    ("try_add_null", "try_add(d, NULL)"),
    ("try_add_null_first", "try_add(NULL, d)"),
]

# A DAY interval: Spark returns a DATE.
DAYS = [
    ("plus_day", "d + INTERVAL '2' DAY"),
    ("minus_day", "d - INTERVAL '2' DAY"),
    ("day_plus", "INTERVAL '2' DAY + d"),
    ("plus_cast_day", "d + CAST(3 AS INTERVAL DAY)"),
    ("plus_date_difference", "d + (DATE '2020-01-05' - DATE '2020-01-01')"),
    ("plus_negated_day", "d + -INTERVAL '1' DAY"),
    ("plus_day_plus_day", "d + (INTERVAL '1' DAY + INTERVAL '2' DAY)"),
    ("plus_null_day", "d + CAST(NULL AS INTERVAL DAY)"),
    ("try_add_day", "try_add(d, INTERVAL '1' DAY)"),
    ("try_add_day_first", "try_add(INTERVAL '1' DAY, d)"),
    ("try_subtract_day", "try_subtract(d, INTERVAL '1' DAY)"),
    ("plus_far_day", "d + (DATE '9999-12-31' - d)"),
    ("minus_far_day", "d - (d - DATE '0001-01-01')"),
    ("plus_null_minus_day", "d + (NULL - INTERVAL '1' DAY)"),
    ("plus_if_day", "d + if(k > 3, INTERVAL '2' DAY, INTERVAL '3' DAY)"),
    ("plus_case_day", "d + CASE WHEN k > 3 THEN INTERVAL '2' DAY END"),
]

# Year-month intervals, which keep `DATE` arithmetic.
YEAR_MONTH = [
    ("plus_month", "d + INTERVAL '1' MONTH"),
    ("minus_year", "d - INTERVAL '1' YEAR"),
]


def definitions():
    for name, expression in TIME_UNITS + DAYS + YEAR_MONTH:
        yield f"column_{name}", f"SELECT k, {expression} AS r FROM inputs"
    # An interval column of a subquery keeps its qualifier.
    for name, interval in [("hour", "INTERVAL '2' HOUR"), ("day", "INTERVAL '2' DAY")]:
        yield (f"subquery_{name}_column",
               f"SELECT k, d + i AS r FROM (SELECT k, d, {interval} AS i FROM inputs)")
    # Aggregates of intervals: MAX and SUM keep the qualifier, AVG is `DAY TO SECOND`.
    yield ("grouped_plus_max_hour",
           "SELECT k, d + max(INTERVAL '2' HOUR) AS r FROM inputs GROUP BY k, d")
    for name, aggregate in [("avg", "avg"), ("sum", "sum")]:
        yield (f"aggregate_plus_{name}_day",
               f"SELECT DATE '2020-01-01' + {aggregate}(INTERVAL '1' DAY) AS r FROM inputs")
    yield "literal_plus_hour", "SELECT DATE '2020-03-08' + INTERVAL '2' HOUR AS r"
    yield "literal_plus_day", "SELECT DATE '2020-01-31' + INTERVAL '1' DAY AS r"
    yield "literal_minus_hour", "SELECT DATE '2020-11-01' - INTERVAL '2' HOUR AS r"


def python_value(value, kind):
    if value is None:
        return None
    if kind in ("integer", "long"):
        return int(value)
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    if kind == "timestamp_ntz":
        return datetime(1970, 1, 1) + timedelta(microseconds=int(value))
    if kind == "date":
        return date(1970, 1, 1) + timedelta(days=int(value))
    if kind == "interval month":
        # Registered as INT months and cast in SQL; PySpark has no YEAR TO MONTH values.
        return int(value)
    if kind == "interval day to second":
        return timedelta(microseconds=int(value))
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row or array value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType, types.YearMonthIntervalType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.TimestampType, types.TimestampNTZType,
                              types.DayTimeIntervalType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.StringType):
        return row.getUTF8String(index).toString()
    if isinstance(data_type, types.ArrayType):
        array = row.getArray(index)
        return [encode_value(array, item, data_type.elementType, types)
                for item in range(array.numElements())]
    raise AssertionError(f"unexpected result type {data_type}")


def error_details(error, Py4JError):
    """Return the outermost error condition and the innermost Java exception class and
    message."""
    condition = getattr(error, "getCondition", lambda: None)()
    cause = getattr(error, "java_exception", None)
    if cause is None:
        cause = getattr(error, "_origin", None)
    exception = message = None
    while cause is not None:
        try:
            exception = cause.getClass().getName()
            message = cause.getMessage()
            if condition is None:
                condition = cause.getCondition()
        except Py4JError:
            pass
        cause = cause.getCause()
    return condition, exception, message


def capture(spark, sql, types, Py4JError):
    record = {"schema_stages": {}}
    phase = "sql"
    try:
        frame = spark.sql(sql)
        execution = frame._jdf.queryExecution()
        record["schema_stages"]["analyzed"] = json.loads(execution.analyzed().schema().json())
        phase = "optimized"
        record["schema_stages"]["optimized"] = json.loads(execution.optimizedPlan().schema().json())
        phase = "collect"
        executed = execution.executedPlan()
        internal_rows = executed.executeCollect()
        record["schema_stages"]["executed"] = json.loads(executed.schema().json())
        fields = frame.schema.fields
        rows = [[encode_value(row, index, field.dataType, types)
                 for index, field in enumerate(fields)] for row in internal_rows]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        record["expected"] = {"status": "ok", "rows": rows}
    except Exception as error:
        condition, exception, java_message = error_details(error, Py4JError)
        record["expected"] = {"status": "error", "condition": condition,
                              "exception": exception, "java_message": java_message,
                              "message": str(error).splitlines()[0]}
        record["spark_error_phase"] = phase
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh JSONL output path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output

    import pyspark
    from py4j.protocol import Py4JError
    from pyspark.sql import SparkSession, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={SPARK_REVISION}" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-date-interval-arithmetic")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for recorded in INPUTS:
            fields = recorded["schema"]["fields"]
            kinds = [field["type"] for field in fields]
            months = [field["name"] for field in fields if field["type"] == "interval month"]
            schema = types.StructType.fromJson({"type": "struct", "fields": [
                dict(field, type="integer") if field["name"] in months else field
                for field in fields]})
            spark.createDataFrame([[python_value(value, kind) for value, kind in zip(row, kinds)]
                                   for row in recorded["rows"]], schema) \
                .selectExpr(*[f"CAST({field['name']} AS INTERVAL MONTH) AS {field['name']}"
                              if field["name"] in months else field["name"]
                              for field in fields]) \
                .createOrReplaceTempView(recorded["name"])
            # The registered values must be exactly the recorded encodings.
            frame = spark.table(recorded["name"])
            stored = [[encode_value(row, index, field.dataType, types)
                       for index, field in enumerate(frame.schema.fields)]
                      for row in frame._jdf.queryExecution().executedPlan().executeCollect()]
            assert stored == recorded["rows"], stored
        header = {
            "spark_version": spark.version, "spark_build": build,
            "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
            "python_version": sys.version.split()[0],
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "value_encoding": ("INT as int; DATE days since the epoch as int; TIMESTAMP "
                               "UTC-epoch microseconds as long; DAY TO SECOND microseconds as "
                               "long"),
            "settings": {
                "spark.sql.timestampType": spark.conf.get("spark.sql.timestampType"),
                "spark.sql.optimizer.excludedRules":
                    spark.conf.get("spark.sql.optimizer.excludedRules")},
            "inputs": INPUTS,
        }
        records = [header]
        cases = list(definitions())
        assert len({name for name, _ in cases}) == len(cases)
        for zone in ZONES:
            spark.conf.set("spark.sql.session.timeZone", zone)
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for name, sql in cases:
                    records.append({"id": name, "timezone": zone, "ansi": ansi, "sql": sql,
                                    **capture(spark, sql, types, Py4JError)})
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({
            "observations": len(records) - 1,
            "errors": sum(r["expected"]["status"] == "error" for r in records[1:]),
            "output": str(args.output),
            "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
