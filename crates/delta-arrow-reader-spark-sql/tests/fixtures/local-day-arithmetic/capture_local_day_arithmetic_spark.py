"""Capture Spark TIMESTAMP day arithmetic in the session time zone (#1093).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for TIMESTAMP plus and minus DAY,
DAY TO SECOND, HOUR, MINUTE, SECOND, YEAR-MONTH and calendar intervals, TIMESTAMPADD and
DATEADD in every unit, SEQUENCE with interval steps and TIMESTAMP subtraction. Inputs are a
registered TIMESTAMP column with BIGINT quantities and DAY TO SECOND intervals, a
TIMESTAMP_NTZ column, literals, and BIGINT microsecond edges. The instants sit around the
2024 Los Angeles DST changes (local results in the skipped hour or the repeated hour),
around Los Angeles leaving local mean time (-07:52:58) on 1883-11-18, in 1800, before the
epoch, in years 1 and 9999, and NULL. Every query runs in both ANSI modes in UTC,
America/Los_Angeles and +05:30. Run from the crate root in the reference environment
described in README.md; the output path must not exist.
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
ZONES = ["UTC", "America/Los_Angeles", "+05:30"]

DAY = 86_400_000_000
HOUR = 3_600_000_000

# (UTC instant, BIGINT quantity, DAY TO SECOND microseconds); Los Angeles local times noted.
INSTANTS = [
    ("2024-03-09 10:30:00", 1, DAY),  # 02:30 PST; a day later, 02:30 is skipped.
    ("2024-03-10 09:59:59.999999", 1, DAY),  # 01:59:59.999999 PST, before the skip.
    ("2024-03-10 10:00:00", -1, -DAY),  # 03:00 PDT, after the skip.
    ("2024-03-11 09:30:00", -1, -DAY),  # 02:30 PDT; a day earlier, 02:30 is skipped.
    ("2024-11-02 08:30:00", 1, DAY),  # 01:30 PDT; a day later, 01:30 repeats.
    ("2024-11-03 08:30:00", 0, DAY + HOUR),  # 01:30 PDT, the first repeated 01:30.
    ("2024-11-03 09:30:00", 1, -(DAY + HOUR)),  # 01:30 PST, the second repeated 01:30.
    ("2024-11-04 09:30:00", -1, -DAY),  # 01:30 PST; a day earlier, 01:30 repeats.
    ("2024-02-10 10:30:00", 1, 23 * HOUR),  # 02:30 PST; a month later, 02:30 is skipped.
    ("2024-10-03 08:30:00", 1, DAY),  # 01:30 PDT; a month later, 01:30 repeats.
    # 12:10:00 local mean time; a day later, after 12:07:02 local mean time became 12:00 PST.
    ("1883-11-17 20:02:58", 1, DAY),
    # 12:03:00 local mean time; a day later, 12:03 repeats, first in local mean time.
    ("1883-11-17 19:55:58", 1, DAY),
    ("1883-11-19 20:03:00", -1, -DAY),  # 12:03:00 PST; a day earlier, 12:03 repeats.
    ("1800-06-15 12:34:56.789123", 2, 2 * DAY + 3 * HOUR + HOUR // 2),  # 04:41:58 LMT.
    ("1969-12-31 23:59:59.999999", 3, 0),
    ("0001-01-01 00:00:00", -2, -(DAY + 1)),
    ("9999-12-31 23:59:59.999999", 1, 1),
    ("2024-06-15 12:00:00", None, None),
    (None, 1, DAY),
]

# TIMESTAMP_NTZ values: a Los Angeles local time that is skipped, one that repeats, a plain
# one and NULL.
LOCAL_TIMES = ["2024-03-10 02:30:00", "2024-11-03 01:30:00", "2024-01-01 00:00:00", None]

# BIGINT microseconds beyond the years Spark's literals accept.
EDGES = [9_223_372_036_854_775_807, -9_223_372_036_854_775_808]


def micros(text):
    value = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def text(value):
    return None if value is None else str(value)


INPUTS = [{
    "name": "instants",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "n", "type": "long", "nullable": True, "metadata": {}},
        {"name": "d", "type": "interval day to second", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), None if instant is None else str(micros(instant)), text(quantity),
              text(interval)]
             for key, (instant, quantity, interval) in enumerate(INSTANTS)]}, {
    "name": "local_times",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "l", "type": "timestamp_ntz", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), None if local is None else str(micros(local))]
             for key, local in enumerate(LOCAL_TIMES)]}, {
    "name": "edges",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "m", "type": "long", "nullable": False, "metadata": {}}]},
    "rows": [[str(key), str(value)] for key, value in enumerate(EDGES)]}]

UNITS = ["MICROSECOND", "MILLISECOND", "SECOND", "MINUTE", "HOUR", "DAY", "DAYOFYEAR", "WEEK",
         "MONTH", "QUARTER", "YEAR"]

COLUMNS = [
    ("plus_day", "ts + INTERVAL '1' DAY"),
    ("minus_day", "ts - INTERVAL '1' DAY"),
    ("plus_negative_day", "ts + INTERVAL '-1' DAY"),
    ("interval_plus", "INTERVAL '1' DAY + ts"),
    ("plus_day_to_second", "ts + INTERVAL '1 01:00:00.000001' DAY TO SECOND"),
    ("minus_day_to_second", "ts - INTERVAL '1 01:00:00' DAY TO SECOND"),
    ("plus_negative_day_to_second", "ts + INTERVAL '-1 01:00:00' DAY TO SECOND"),
    ("plus_hour", "ts + INTERVAL '1' HOUR"),
    ("plus_23_hours", "ts + INTERVAL '23' HOUR"),
    ("plus_25_hours", "ts + INTERVAL '25' HOUR"),
    ("minus_25_hours", "ts - INTERVAL '25' HOUR"),
    ("plus_1440_minutes", "ts + INTERVAL '1440' MINUTE"),
    ("plus_86400_seconds", "ts + INTERVAL '86400' SECOND"),
    ("plus_column_interval", "ts + d"),
    ("minus_column_interval", "ts - d"),
    ("plus_month", "ts + INTERVAL '1' MONTH"),
    ("minus_month", "ts - INTERVAL '1' MONTH"),
    ("plus_year_to_month", "ts + INTERVAL '1-1' YEAR TO MONTH"),
    # Calendar intervals of a month, a day and an hour.
    ("plus_calendar_interval", "ts + make_interval(0, 1, 0, 1, 1)"),
    ("minus_calendar_interval", "ts - make_interval(0, 1, 0, 1, 1)"),
] + [(f"timestampadd_{unit.lower()}", f"timestampadd({unit}, 1, ts)") for unit in UNITS] + [
    (f"timestampadd_{unit.lower()}_column", f"timestampadd({unit}, n, ts)") for unit in UNITS
] + [(f"dateadd_{unit.lower()}", f"dateadd({unit}, n, ts)") for unit in UNITS] + [
    ("date_add_day", "date_add(DAY, n, ts)"),
    ("timestampadd_24_hours", "timestampadd(HOUR, 24, ts)"),
    ("timestampadd_86400_seconds", "timestampadd(SECOND, 86400, ts)"),
    ("timestampadd_negative_day", "timestampadd(DAY, -1, ts)"),
    ("sequence_hours", "sequence(ts, ts + INTERVAL '1' DAY, INTERVAL '1' HOUR)"),
    ("sequence_days", "sequence(ts, ts + INTERVAL '2' DAY, INTERVAL '1' DAY)"),
    ("sequence_day_hours", "sequence(ts, ts + INTERVAL '3' DAY, INTERVAL '1 01' DAY TO HOUR)"),
    ("sequence_default_step", "sequence(ts, ts + INTERVAL '2' DAY)"),
    ("sequence_negative_days", "sequence(ts, ts - INTERVAL '2' DAY, INTERVAL '-1' DAY)"),
    ("sequence_months", "sequence(ts, ts + INTERVAL '2' MONTH, INTERVAL '1' MONTH)"),
    ("sequence_calendar_days", "sequence(ts, ts + INTERVAL '2' DAY, make_interval(0, 0, 0, 1))"),
    ("minus_timestamp", "ts - TIMESTAMP '2024-01-01 00:00:00'"),
    ("timestamp_minus", "TIMESTAMP '2024-03-10 03:00:00' - ts"),
    ("minus_date", "ts - DATE '2024-03-10'"),
    ("minus_timestamp_ntz", "ts - TIMESTAMP_NTZ '2024-03-10 00:00:00'"),
    ("lambda_plus_day", "transform(array(ts), x -> x + INTERVAL '1' DAY)"),
    ("lambda_minus_timestamp", "transform(array(ts), x -> x - TIMESTAMP '2024-01-01 00:00:00')"),
]

LITERALS = [
    ("plus_day", "TIMESTAMP '2024-03-09 02:30:00' + INTERVAL '1' DAY"),
    ("minus_day", "TIMESTAMP '2024-03-11 02:30:00' - INTERVAL '1' DAY"),
    ("plus_day_to_second", "TIMESTAMP '2024-03-09 02:30:00' + INTERVAL '1 01:00:00' DAY TO SECOND"),
    ("plus_25_hours", "TIMESTAMP '2024-03-09 02:30:00' + INTERVAL '25' HOUR"),
    ("plus_month", "TIMESTAMP '2024-02-10 02:30:00' + INTERVAL '1' MONTH"),
    ("null_plus_day", "CAST(NULL AS TIMESTAMP) + INTERVAL '1' DAY"),
    ("plus_null_interval", "TIMESTAMP '2024-03-09 02:30:00' + CAST(NULL AS INTERVAL DAY)"),
    ("timestamp_ntz_plus_day", "TIMESTAMP_NTZ '2024-03-09 02:30:00' + INTERVAL '1' DAY"),
    ("string_plus_day", "'2024-03-09 02:30:00' + INTERVAL '1' DAY"),
    ("date_plus_hour", "DATE '2024-03-10' + INTERVAL '1' HOUR"),
    ("date_plus_day_to_hour", "DATE '2024-03-09' + INTERVAL '1 02' DAY TO HOUR"),
    ("minus_timestamp", "TIMESTAMP '2024-03-10 03:00:00' - TIMESTAMP '2024-01-01 00:00:00'"),
    ("minus_date", "TIMESTAMP '2024-03-10 03:00:00' - DATE '2024-01-01'"),
    ("date_minus_timestamp", "DATE '2024-03-11' - TIMESTAMP '2024-03-10 03:00:00'"),
    ("timestamp_ntz_minus_timestamp",
     "TIMESTAMP_NTZ '2024-03-11 00:00:00' - TIMESTAMP '2024-03-10 03:00:00'"),
    ("null_minus_timestamp", "CAST(NULL AS TIMESTAMP) - TIMESTAMP '2024-01-01 00:00:00'"),
    # Spark subtracts the TIMESTAMP's local date-time from the skipped TIMESTAMP_NTZ 02:30.
    ("minus_timestamp_ntz_in_gap",
     "TIMESTAMP '2024-03-10 04:00:00' - TIMESTAMP_NTZ '2024-03-10 02:30:00'"),
    ("timestamp_ntz_in_gap_minus_timestamp",
     "TIMESTAMP_NTZ '2024-03-10 02:30:00' - TIMESTAMP '2024-03-10 01:00:00'"),
    # Negating the smallest intervals overflows.
    ("minus_smallest_year_month",
     "TIMESTAMP '2024-01-01 00:00:00' - INTERVAL '-178956970-8' YEAR TO MONTH"),
    ("minus_smallest_calendar_months",
     "TIMESTAMP '2024-01-01 00:00:00' - make_interval(-178956970, -8)"),
    ("minus_smallest_calendar_days",
     "TIMESTAMP '2024-01-01 00:00:00' - make_interval(0, 0, 0, -2147483648)"),
    ("timestampadd_day", "timestampadd(DAY, 1, TIMESTAMP '2024-03-09 02:30:00')"),
    ("dateadd_day", "dateadd(DAY, 1, TIMESTAMP '2024-03-09 02:30:00')"),
    ("date_add_day", "date_add(DAY, 1, TIMESTAMP '2024-03-09 02:30:00')"),
    ("timestampadd_month", "timestampadd(MONTH, 1, TIMESTAMP '2024-02-10 02:30:00')"),
    ("timestampadd_exact_microseconds",
     "timestampadd(MICROSECOND, 9007199254740993, TIMESTAMP '1970-01-01 00:00:00')"),
    ("timestampadd_int_overflow", "timestampadd(DAY, 2147483648, TIMESTAMP '2024-01-01 00:00:00')"),
    # Wrapped to INT, these counts would be 1.
    ("timestampadd_wrapping_days",
     "timestampadd(DAY, 4294967297, TIMESTAMP '2024-01-01 00:00:00')"),
    ("timestampadd_wrapping_months",
     "timestampadd(MONTH, 4294967297, TIMESTAMP '2024-01-01 00:00:00')"),
    ("timestampadd_double", "timestampadd(SECOND, 1.9D, TIMESTAMP '2024-03-09 02:30:00')"),
    ("timestampadd_huge_double",
     "timestampadd(MICROSECOND, 1e30D, TIMESTAMP '2024-03-09 02:30:00')"),
    ("timestampadd_huge_decimal",
     "timestampadd(MICROSECOND, 99999999999999999999.5, TIMESTAMP '2024-03-09 02:30:00')"),
    ("timestampadd_invalid_string", "timestampadd(DAY, 'x', TIMESTAMP '2024-03-09 02:30:00')"),
    ("timestampadd_boolean", "timestampadd(DAY, true, TIMESTAMP '2024-03-09 02:30:00')"),
    ("timestampadd_decimal", "timestampadd(SECOND, 1.5, TIMESTAMP '2024-03-09 02:30:00')"),
    ("timestampadd_string_quantity", "timestampadd(DAY, '1', TIMESTAMP '2024-03-09 02:30:00')"),
    ("timestampadd_null_quantity", "timestampadd(DAY, NULL, TIMESTAMP '2024-03-09 02:30:00')"),
    ("timestampadd_null", "timestampadd(DAY, 1, CAST(NULL AS TIMESTAMP))"),
    ("timestampadd_timestamp_ntz", "timestampadd(DAY, 1, TIMESTAMP_NTZ '2024-03-09 02:30:00')"),
    ("timestampadd_date", "timestampadd(HOUR, 1, DATE '2024-03-10')"),
    ("timestampadd_string", "timestampadd(DAY, 1, '2024-03-09 02:30:00')"),
    ("sequence_days", "sequence(TIMESTAMP '2024-03-09 02:30:00', "
                      "TIMESTAMP '2024-03-11 02:30:00', INTERVAL '1' DAY)"),
]

EDGE_CASES = [
    # (id, edge row, expression over `ts`)
    ("max_minus_day", 0, "ts - INTERVAL '1' DAY"),
    ("min_plus_day", 1, "ts + INTERVAL '1' DAY"),
    ("max_plus_day", 0, "ts + INTERVAL '1' DAY"),
    ("min_minus_day", 1, "ts - INTERVAL '1' DAY"),
    ("max_plus_hour", 0, "ts + INTERVAL '1' HOUR"),
    ("max_plus_month", 0, "ts + INTERVAL '1' MONTH"),
    ("max_timestampadd_microsecond", 0, "timestampadd(MICROSECOND, 1, ts)"),
    ("max_timestampadd_day", 0, "timestampadd(DAY, 1, ts)"),
    ("max_timestampadd_month", 0, "timestampadd(MONTH, 1, ts)"),
    ("min_timestampadd_day", 1, "timestampadd(DAY, 1, ts)"),
    ("max_minus_epoch", 0, "ts - TIMESTAMP '1970-01-01 00:00:00'"),
    ("min_minus_epoch", 1, "ts - TIMESTAMP '1970-01-01 00:00:00'"),
]


def definitions():
    for name, expression in COLUMNS:
        yield f"column_{name}", f"SELECT k, {expression} AS r FROM instants"
    yield ("column_pairs_minus_timestamp",
           "SELECT a.k AS a, b.k AS b, a.ts - b.ts AS r FROM instants a CROSS JOIN instants b")
    yield ("column_pairs_minus_timestamp_ntz",
           "SELECT i.k AS a, w.k AS b, i.ts - w.l AS r FROM instants i CROSS JOIN local_times w")
    yield ("column_pairs_timestamp_ntz_minus",
           "SELECT i.k AS a, w.k AS b, w.l - i.ts AS r FROM instants i CROSS JOIN local_times w")
    for name, expression in LITERALS:
        yield f"literal_{name}", f"SELECT {expression} AS r"
    for name, key, expression in EDGE_CASES:
        yield (f"edge_{name}",
               f"SELECT k, {expression} AS r FROM "
               f"(SELECT k, timestamp_micros(m) AS ts FROM edges) WHERE k = {key}")
    yield ("edge_max_minus_min",
           "SELECT timestamp_micros(a.m) - timestamp_micros(b.m) AS r "
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
    if kind == "interval day to second":
        return timedelta(microseconds=int(value))
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row or array value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-local-day-arithmetic")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for recorded in INPUTS:
            schema = types.StructType.fromJson(recorded["schema"])
            kinds = [field["type"] for field in recorded["schema"]["fields"]]
            spark.createDataFrame([[python_value(value, kind) for value, kind in zip(row, kinds)]
                                   for row in recorded["rows"]], schema) \
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
                               "long; STRING as text; ARRAY as a list of its elements"),
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
