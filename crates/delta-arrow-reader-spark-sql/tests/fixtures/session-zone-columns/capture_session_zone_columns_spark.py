"""Capture Spark results over registered TIMESTAMP columns in a session time zone (#1012).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages for zone-dependent functions over a registered TIMESTAMP column and over the same
instants inside a struct, an array and a map: field extraction, DATE_FORMAT, casts, date
truncation, date and interval arithmetic, comparisons, grouping and set operations. Every
query runs in both ANSI modes in UTC and America/Los_Angeles; HOUR and CAST to STRING also
run in the zone IDs Z, PST and GMT+08:00 and in Asia/Kathmandu and +05:45. Run from the
crate root in the reference environment described in README.md; the output path must not
exist.
"""
import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles"]
# Zone IDs Spark accepts that Arrow cannot parse, and the zones DAR's test annotations name,
# for HOUR and CAST to STRING only.
ZONE_ID_SESSIONS = ["Z", "PST", "GMT+08:00", "Asia/Kathmandu", "+05:45"]
ZONE_ID_CASES = ["hour", "cast_string"]

# Registered instants as UTC text; `tn` holds the same text as a TIMESTAMP_NTZ wall clock.
INSTANTS = [
    # The issue's reproduction: 05:04 in Los Angeles.
    "2024-01-02 13:04:05.123456",
    # Los Angeles skips 02:00-03:00 on 2024-03-10: the last instant before and the first after.
    "2024-03-10 09:59:59.999999",
    "2024-03-10 10:00:00",
    # Los Angeles repeats 01:00-02:00 on 2024-11-03: 01:30 in daylight time, then standard.
    "2024-11-03 08:30:00",
    "2024-11-03 09:30:00",
    # 2023-12-31 23:30 in Los Angeles: a different year, quarter, week and day.
    "2024-01-01 07:30:00",
    "1969-12-31 23:59:59.999999",
    # Before 1883-11-18 20:00 UTC Los Angeles keeps local mean time, -07:52:58.
    "1883-11-18 19:00:00",
    "1800-06-15 12:00:00",
    # 0001-01-01 16:07:02 in Los Angeles.
    "0001-01-02 00:00:00",
    "9999-12-31 23:59:59.999999",
    None,
]

TIMESTAMP_EXPRESSIONS = [
    ("year", "year(ts)"), ("quarter", "quarter(ts)"), ("month", "month(ts)"),
    ("day", "day(ts)"), ("dayofweek", "dayofweek(ts)"), ("weekday", "weekday(ts)"),
    ("dayofyear", "dayofyear(ts)"), ("weekofyear", "weekofyear(ts)"), ("hour", "hour(ts)"),
    ("minute", "minute(ts)"), ("second", "second(ts)"),
    ("extract_year", "extract(YEAR FROM ts)"), ("extract_hour", "extract(HOUR FROM ts)"),
    ("extract_second", "extract(SECOND FROM ts)"),
    ("date_part_minute", "date_part('MINUTE', ts)"),
    ("date_format_full", "date_format(ts, 'yyyy-MM-dd HH:mm:ss.SSSSSS')"),
    ("date_format_offset", "date_format(ts, 'yyyy-MM-dd HH:mm XXX')"),
    ("date_format_text", "date_format(ts, 'EEE a hh')"),
    ("cast_string", "CAST(ts AS STRING)"), ("cast_date", "CAST(ts AS DATE)"),
    ("try_cast_date", "try_cast(ts AS DATE)"), ("concat", "concat('t=', ts)"),
    ("date_trunc_year", "date_trunc('YEAR', ts)"),
    ("date_trunc_quarter", "date_trunc('QUARTER', ts)"),
    ("date_trunc_month", "date_trunc('MONTH', ts)"),
    ("date_trunc_week", "date_trunc('WEEK', ts)"),
    ("date_trunc_day", "date_trunc('DAY', ts)"),
    ("date_trunc_hour", "date_trunc('HOUR', ts)"),
    ("date_trunc_minute", "date_trunc('MINUTE', ts)"),
    ("trunc_week", "trunc(ts, 'WEEK')"), ("trunc_month", "trunc(ts, 'MM')"),
    ("trunc_year", "trunc(ts, 'YEAR')"),
    ("to_date", "to_date(ts)"), ("date_add", "date_add(ts, 1)"),
    ("date_sub", "date_sub(ts, 1)"), ("add_months", "add_months(ts, 1)"),
    ("last_day", "last_day(ts)"), ("next_day", "next_day(ts, 'MON')"),
    ("datediff", "datediff(ts, DATE '2024-01-01')"),
    ("months_between", "months_between(ts, TIMESTAMP '2024-01-01 00:00:00')"),
    # Arithmetic in local time: a day across a DST change is 23 or 25 hours.
    ("plus_interval_day", "ts + INTERVAL '1' DAY"),
    ("minus_interval_day", "ts - INTERVAL '1' DAY"),
    ("plus_interval_hour", "ts + INTERVAL '1' HOUR"),
    ("plus_interval_month", "ts + INTERVAL '1' MONTH"),
    ("timestampadd_day", "timestampadd(DAY, 1, ts)"),
    ("timestampadd_hour", "timestampadd(HOUR, 1, ts)"),
    ("sequence_hours", "size(sequence(ts, ts + INTERVAL '1' DAY, INTERVAL '1' HOUR))"),
    ("minus_timestamp", "ts - TIMESTAMP '2024-01-01 00:00:00'"),
    ("to_json", "to_json(named_struct('t', ts))"),
    # Zone-independent controls.
    ("select", "ts"), ("unix_micros", "unix_micros(ts)"),
    ("unix_timestamp", "unix_timestamp(ts)"),
    ("from_utc_timestamp", "from_utc_timestamp(ts, 'Asia/Tokyo')"),
    ("to_utc_timestamp", "to_utc_timestamp(ts, 'Asia/Tokyo')"),
    ("ntz_hour", "hour(tn)"),
    # Comparisons with values read in the session zone.
    ("equals_literal", "ts = TIMESTAMP '2024-01-02 05:04:05.123456'"),
    ("at_least_string", "ts >= '2024-01-02 05:04:05'"),
    ("equals_ntz", "ts = TIMESTAMP_NTZ '2024-01-02 05:04:05.123456'"),
    ("at_least_date", "ts >= DATE '2024-01-02'"),
    ("date_equals", "CAST(ts AS DATE) = DATE '2024-01-02'"),
    # A column merged with a session-zone literal.
    ("coalesce_hour", "hour(coalesce(ts, TIMESTAMP '2000-01-01 12:00:00'))"),
    ("case_hour", "hour(CASE WHEN k % 2 = 0 THEN ts ELSE TIMESTAMP '2000-01-01 12:00:00' END)"),
    ("greatest_hour", "hour(greatest(ts, TIMESTAMP '1000-01-01 00:00:00'))"),
]

NESTED_EXPRESSIONS = [
    ("struct_hour", "hour(s.t)"), ("struct_cast_string", "CAST(s.t AS STRING)"),
    ("struct_date_trunc", "date_trunc('DAY', s.t)"),
    ("array_date_format", "date_format(a[0], 'yyyy-MM-dd HH:mm')"),
    ("array_cast_date", "CAST(a[0] AS DATE)"), ("element_at_hour", "hour(element_at(a, 1))"),
    ("map_hour", "hour(m['x'])"), ("map_cast_string", "CAST(m['x'] AS STRING)"),
]

STATEMENTS = [
    ("group_by_date",
     "SELECT CAST(ts AS DATE) AS d, count(*) AS n FROM zone_inputs GROUP BY CAST(ts AS DATE)"),
    ("filter_hour", "SELECT k FROM zone_inputs WHERE hour(ts) = 5"),
    ("filter_literal", "SELECT k FROM zone_inputs WHERE ts >= TIMESTAMP '2024-01-02 00:00:00'"),
    ("cte_hour", "WITH c AS (SELECT k, ts FROM zone_inputs) SELECT k, hour(ts) AS r FROM c"),
    ("alias_hour", "SELECT t.k, hour(t.ts) AS r FROM zone_inputs AS t"),
    ("join_hour",
     "SELECT a.k, hour(b.ts) AS r FROM zone_inputs AS a JOIN zone_inputs AS b ON a.k = b.k"),
    ("union_hour", "SELECT hour(x) AS r FROM (SELECT ts AS x FROM zone_inputs "
                   "UNION ALL SELECT TIMESTAMP '2024-06-01 12:00:00')"),
    ("union_values",
     "SELECT ts AS r FROM zone_inputs UNION ALL SELECT TIMESTAMP '2024-06-01 12:00:00'"),
    ("max_hour", "SELECT hour(max(ts)) AS r FROM zone_inputs"),
    ("empty_hour", "SELECT hour(ts) AS r FROM empty_zone_inputs"),
    ("empty_cast_string", "SELECT CAST(ts AS STRING) AS r FROM empty_zone_inputs"),
]


def definitions():
    for name, expression in TIMESTAMP_EXPRESSIONS:
        yield name, f"SELECT k, {expression} AS r FROM zone_inputs"
    for name, expression in NESTED_EXPRESSIONS:
        yield name, f"SELECT k, {expression} AS r FROM nested_inputs"
    yield from STATEMENTS


def micros(text, aware):
    """Encode a wall-clock text as epoch microseconds without float arithmetic."""
    value = datetime.fromisoformat(text)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc if aware else None)
    if aware:
        value = value.replace(tzinfo=timezone.utc)
    delta = value - epoch
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


TABLE_FIELDS = [("k", "integer", False), ("ts", "timestamp", True), ("tn", "timestamp_ntz", True)]


def table(name, instants):
    rows = [[str(key), None if text is None else str(micros(text, True)),
             None if text is None else str(micros(text, False))]
            for key, text in enumerate(instants)]
    return {"name": name,
            "schema": {"type": "struct", "fields": [
                {"name": field, "type": kind, "nullable": nullable, "metadata": {}}
                for field, kind, nullable in TABLE_FIELDS]},
            "rows": rows}


# Spark registers the nested inputs as a view of `zone_inputs`; Spark TIMESTAMP values carry
# no zone, so a view and a stored table read the same instants.
NESTED_VIEW = ("SELECT k, named_struct('t', ts) AS s, array(ts) AS a, map('x', ts) AS m "
               "FROM zone_inputs")


def python_value(value, kind):
    """Build the exact Python value Spark converts back to the recorded encoding."""
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    if kind == "timestamp_ntz":
        return datetime(1970, 1, 1) + timedelta(microseconds=int(value))
    raise AssertionError(f"unexpected input type {kind}")


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.TimestampType, types.TimestampNTZType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.BooleanType):
        return "true" if row.getBoolean(index) else "false"
    if isinstance(data_type, types.DoubleType):
        # The IEEE 754 bits, so no decimal rendering intervenes.
        return struct.pack(">d", row.getDouble(index)).hex()
    if isinstance(data_type, types.DecimalType):
        # Py4J returns the java.math.BigDecimal as a Python Decimal with the same scale.
        decimal = row.getDecimal(index, data_type.precision, data_type.scale)
        return format(decimal.toJavaBigDecimal(), "f")
    if isinstance(data_type, types.DayTimeIntervalType):
        return str(row.getLong(index))
    if isinstance(data_type, types.StringType):
        return row.getUTF8String(index).toString()
    raise AssertionError(f"unexpected result type {data_type}")


def error_details(error, Py4JError):
    """Return the outermost error condition and the innermost Java exception class."""
    condition = getattr(error, "getCondition", lambda: None)()
    cause = getattr(error, "java_exception", None)
    if cause is None:
        # PySpark keeps the Java exception of a converted error in _origin.
        cause = getattr(error, "_origin", None)
    exception = None
    while cause is not None:
        try:
            exception = cause.getClass().getName()
            if condition is None:
                condition = cause.getCondition()
        except Py4JError:
            pass
        cause = cause.getCause()
    return condition, exception


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
        condition, exception = error_details(error, Py4JError)
        record["expected"] = {"status": "error", "condition": condition,
                              "exception": exception, "message": str(error).splitlines()[0]}
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-session-zone-columns")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        inputs = [table("zone_inputs", INSTANTS), table("empty_zone_inputs", [])]
        for recorded in inputs:
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
        spark.sql(NESTED_VIEW).createOrReplaceTempView("nested_inputs")
        header = {
            "spark_version": spark.version, "spark_build": build,
            "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
            "python_version": sys.version.split()[0],
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "value_encoding": ("INT and DATE days as int; BIGINT, TIMESTAMP UTC-epoch and "
                               "TIMESTAMP_NTZ wall-clock microseconds and DAY TO SECOND "
                               "microseconds as long; BOOLEAN as true or false; DOUBLE as "
                               "big-endian IEEE 754 hex; DECIMAL as plain text; STRING as "
                               "text"),
            "settings": {
                "spark.sql.timestampType": spark.conf.get("spark.sql.timestampType"),
                "spark.sql.optimizer.excludedRules":
                    spark.conf.get("spark.sql.optimizer.excludedRules")},
            "inputs": inputs,
            "nested_inputs": {"name": "nested_inputs", "sql": NESTED_VIEW},
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
        zone_id_cases = [(name, sql) for name, sql in cases if name in ZONE_ID_CASES]
        assert len(zone_id_cases) == len(ZONE_ID_CASES)
        for zone in ZONE_ID_SESSIONS:
            spark.conf.set("spark.sql.session.timeZone", zone)
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for name, sql in zone_id_cases:
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
