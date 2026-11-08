"""Capture Spark TRY_ADD and TRY_SUBTRACT of TIMESTAMP values and intervals (#1129).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for TRY_ADD and TRY_SUBTRACT of
TIMESTAMP and TIMESTAMP_NTZ values with DAY TO SECOND, YEAR TO MONTH and calendar intervals in
both argument orders, TRY_SUBTRACT of TIMESTAMP, TIMESTAMP_NTZ and DATE values, untyped NULL
operands and wrong argument counts. Inputs are a registered table of TIMESTAMP and
TIMESTAMP_NTZ values with DAY TO SECOND and YEAR TO MONTH intervals, literals, and BIGINT
microsecond edges. The instants sit a day or a month before a skipped or repeated Los Angeles
local time in 2024, in local mean time, and at the start of year 1 and the end of year 9999;
the TIMESTAMP_NTZ values sit in the skipped and repeated hours, at a month end, in local mean
time and at the same year bounds; one row is NULL. Every query runs in both ANSI modes in
UTC and America/Los_Angeles. Run from the crate root in the reference environment described
in README.md; the output path must not exist.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles"]

DAY = 86_400_000_000
HOUR = 3_600_000_000

# (UTC instant, TIMESTAMP_NTZ, DAY TO SECOND microseconds, YEAR TO MONTH months); Los Angeles
# local times noted.
ROWS = [
    ("2024-03-09 10:30:00", "2024-03-10 02:30:00", DAY, 1),  # 02:30 PST; skipped a day later.
    ("2024-11-02 08:30:00", "2024-11-03 01:30:00", DAY + HOUR, -1),  # 01:30 PDT; repeats.
    ("2024-02-10 10:30:00", "2024-01-31 12:00:00", -DAY, 1),  # 02:30 PST; skipped a month later.
    ("1883-11-17 20:02:58", "1883-11-17 12:10:00", DAY, 12),  # 12:10 local mean time.
    ("9999-12-31 23:59:59.999999", "9999-12-31 23:59:59.999999", 1, 1),
    ("0001-01-01 00:00:00", "0001-01-01 00:00:00", -1, -1),
    (None, None, None, None),
]

# BIGINT microseconds beyond the years Spark's literals accept, and the epoch.
EDGES = [9_223_372_036_854_775_807, -9_223_372_036_854_775_808, 0]


def micros(text):
    value = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def text(value):
    return None if value is None else str(value)


INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "l", "type": "timestamp_ntz", "nullable": True, "metadata": {}},
        {"name": "d", "type": "interval day to second", "nullable": True, "metadata": {}},
        {"name": "m", "type": "interval month", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), None if instant is None else str(micros(instant)),
              None if local is None else str(micros(local)), text(day_time), text(months)]
             for key, (instant, local, day_time, months) in enumerate(ROWS)]}, {
    "name": "edges",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "m", "type": "long", "nullable": False, "metadata": {}}]},
    "rows": [[str(key), str(value)] for key, value in enumerate(EDGES)]}]

CALENDAR = "make_interval(0, 1, 0, 1, 1)"
COLUMNS = [
    ("try_add_day", "try_add(ts, INTERVAL '1' DAY)"),
    ("try_add_day_first", "try_add(INTERVAL '1' DAY, ts)"),
    ("try_add_day_to_second", "try_add(ts, INTERVAL '1 01:00:00' DAY TO SECOND)"),
    ("try_add_column_day_time", "try_add(ts, d)"),
    ("try_add_month", "try_add(ts, INTERVAL '1' MONTH)"),
    ("try_add_month_first", "try_add(INTERVAL '1' MONTH, ts)"),
    ("try_add_column_months", "try_add(ts, m)"),
    ("try_add_calendar", f"try_add(ts, {CALENDAR})"),
    ("try_add_calendar_first", f"try_add({CALENDAR}, ts)"),
    ("try_subtract_day", "try_subtract(ts, INTERVAL '1' DAY)"),
    ("try_subtract_column_day_time", "try_subtract(ts, d)"),
    ("try_subtract_month", "try_subtract(ts, INTERVAL '1' MONTH)"),
    ("try_subtract_column_months", "try_subtract(ts, m)"),
    ("try_subtract_calendar", f"try_subtract(ts, {CALENDAR})"),
    ("ntz_try_add_day", "try_add(l, INTERVAL '1' DAY)"),
    ("ntz_try_add_day_first", "try_add(INTERVAL '1' DAY, l)"),
    ("ntz_try_add_column_day_time", "try_add(l, d)"),
    ("ntz_try_add_month", "try_add(l, INTERVAL '1' MONTH)"),
    ("ntz_try_add_column_months", "try_add(l, m)"),
    ("ntz_try_add_calendar", f"try_add(l, {CALENDAR})"),
    ("ntz_try_subtract_day", "try_subtract(l, INTERVAL '1' DAY)"),
    ("ntz_try_subtract_month", "try_subtract(l, INTERVAL '1' MONTH)"),
    ("ntz_try_subtract_calendar", f"try_subtract(l, {CALENDAR})"),
    # Negating the smallest day count overflows; Spark runs TRY_SUBTRACT under ANSI.
    ("ntz_try_subtract_smallest_calendar_days",
     "try_subtract(l, make_interval(0, -70555328, 0, -2147483648))"),
    ("ntz_try_add_untyped_null", "try_add(l, NULL)"),
    ("ntz_try_subtract_untyped_null", "try_subtract(l, NULL)"),
    ("ntz_try_subtract_from_untyped_null", "try_subtract(NULL, l)"),
    ("try_subtract_timestamp", "try_subtract(ts, TIMESTAMP '2024-01-01 00:00:00')"),
    ("try_subtract_timestamp_ntz", "try_subtract(ts, l)"),
    ("try_subtract_from_timestamp_ntz", "try_subtract(l, ts)"),
    ("try_subtract_timestamp_ntz_values", "try_subtract(l, TIMESTAMP_NTZ '2024-01-01 00:00:00')"),
    ("try_subtract_date", "try_subtract(ts, DATE '2024-03-10')"),
    ("lambda_try_add_day", "transform(array(ts), x -> try_add(x, INTERVAL '1' DAY))"),
]

LITERALS = [
    ("try_add_day", "try_add(TIMESTAMP '2024-03-09 02:30:00', INTERVAL '1' DAY)"),
    ("try_subtract_day", "try_subtract(TIMESTAMP '2024-03-11 02:30:00', INTERVAL '1' DAY)"),
    ("try_add_month", "try_add(TIMESTAMP '2024-02-10 02:30:00', INTERVAL '1' MONTH)"),
    ("try_add_null_timestamp", "try_add(CAST(NULL AS TIMESTAMP), INTERVAL '1' DAY)"),
    ("try_add_null_interval",
     "try_add(TIMESTAMP '2024-03-09 02:30:00', CAST(NULL AS INTERVAL DAY))"),
    ("try_subtract_smallest_year_month",
     "try_subtract(TIMESTAMP '2024-01-01 00:00:00', INTERVAL '-178956970-8' YEAR TO MONTH)"),
    ("try_subtract_smallest_calendar_months",
     "try_subtract(TIMESTAMP '2024-01-01 00:00:00', make_interval(-178956970, -8))"),
    ("try_subtract_smallest_calendar_days",
     "try_subtract(TIMESTAMP '2024-01-01 00:00:00', make_interval(0, -70555328, 0, -2147483648))"),
    ("try_add_untyped_null", "try_add(TIMESTAMP '2024-03-09 02:30:00', NULL)"),
    ("try_add_untyped_null_first", "try_add(NULL, TIMESTAMP '2024-03-09 02:30:00')"),
    ("try_subtract_untyped_null", "try_subtract(TIMESTAMP '2024-03-09 02:30:00', NULL)"),
    ("try_subtract_from_untyped_null", "try_subtract(NULL, TIMESTAMP '2024-03-09 02:30:00')"),
    ("try_add_one_argument", "try_add(TIMESTAMP '2024-03-09 02:30:00')"),
    ("try_subtract_three_arguments", "try_subtract(TIMESTAMP '2024-03-09 02:30:00', "
                                     "INTERVAL '1' DAY, INTERVAL '1' DAY)"),
    ("try_add_overflow", "try_add(timestamp_micros(9223372036854775807), INTERVAL '1' DAY)"),
    ("try_subtract_timestamps", "try_subtract(TIMESTAMP '2024-03-10 03:00:00', "
                                "TIMESTAMP '2024-01-01 00:00:00')"),
    ("try_subtract_timestamp_ntz_in_gap", "try_subtract(TIMESTAMP '2024-03-10 04:00:00', "
                                          "TIMESTAMP_NTZ '2024-03-10 02:30:00')"),
]

EDGE_CASES = [
    ("try_add_day", "try_add(ts, INTERVAL '1' DAY)"),
    ("try_subtract_day", "try_subtract(ts, INTERVAL '1' DAY)"),
    ("try_add_month", "try_add(ts, INTERVAL '1' MONTH)"),
    ("try_add_hour", "try_add(ts, INTERVAL '1' HOUR)"),
    ("try_subtract_epoch", "try_subtract(ts, TIMESTAMP '1970-01-01 00:00:00')"),
]


def definitions():
    for name, expression in COLUMNS:
        yield f"column_{name}", f"SELECT k, {expression} AS r FROM inputs"
    for name, expression in LITERALS:
        yield f"literal_{name}", f"SELECT {expression} AS r"
    for name, expression in EDGE_CASES:
        yield (f"edge_{name}",
               f"SELECT k, {expression} AS r FROM (SELECT k, timestamp_micros(m) AS ts FROM edges)")
    yield ("edge_try_subtract_max_min",
           "SELECT try_subtract(timestamp_micros(a.m), timestamp_micros(b.m)) AS r "
           "FROM edges a CROSS JOIN edges b WHERE a.k = 0 AND b.k = 1")


def python_value(value, kind):
    if value is None:
        return None
    if kind in ("integer", "long"):
        return int(value)
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    if kind == "timestamp_ntz":
        return datetime(1970, 1, 1) + timedelta(microseconds=int(value))
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-try-timestamp-intervals")
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
            "value_encoding": ("INT as int; BIGINT as long; TIMESTAMP and TIMESTAMP_NTZ "
                               "UTC-epoch microseconds as long; DAY TO SECOND microseconds as "
                               "long; YEAR TO MONTH months as int; STRING as text; ARRAY as a "
                               "list of its elements"),
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
