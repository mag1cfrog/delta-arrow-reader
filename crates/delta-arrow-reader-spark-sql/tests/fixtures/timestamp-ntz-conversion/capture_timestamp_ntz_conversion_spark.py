"""Capture Spark conversions from TIMESTAMP to TIMESTAMP_NTZ in a session time zone (#1013).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages for explicit casts (CAST, TRY_CAST and `::`), TO_TIMESTAMP_NTZ, the implicit
conversion in CONVERT_TIMEZONE, comparisons of converted values with TIMESTAMP_NTZ
literals, and other expressions over the converted values, with registered columns and
literals. Formatted TO_TIMESTAMP_NTZ and TO_TIMESTAMP and TIMESTAMP() of a TIMESTAMP are
recorded as well. The instants include Los Angeles DST changes, local mean time before
1883, negative epochs, years 1 and 9999, instants beyond year 262143 in winter, the BIGINT
extremes and NULL. Every query runs in both ANSI modes in UTC, America/Los_Angeles and
Asia/Kathmandu; CAST also runs in the zone IDs Z, PST, GMT+08:00 and +05:45. Run from the
crate root in the reference environment described in README.md; the output path must not
exist.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles", "Asia/Kathmandu"]
# Zone IDs Spark accepts, three of which Arrow cannot parse, for CAST only.
ZONE_ID_SESSIONS = ["Z", "PST", "GMT+08:00", "+05:45"]
ZONE_ID_CASES = ["column_cast"]
MIN_MICROS = -9223372036854775808
MAX_MICROS = 9223372036854775807


def micros(text):
    """Encode a UTC text as epoch microseconds without float arithmetic."""
    value = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


# Registered instants: UTC texts, or epoch microseconds beyond Python's years.
INSTANTS = [
    # The issue's reproduction: 05:04 in Los Angeles, 18:49 in Kathmandu.
    micros("2024-01-02 13:04:05.123456"),
    # Los Angeles skips 02:00-03:00 on 2024-03-10: the last instant before and the first after.
    micros("2024-03-10 09:59:59.999999"),
    micros("2024-03-10 10:00:00"),
    # Los Angeles repeats 01:00-02:00 on 2024-11-03: 01:30 in daylight time, then standard.
    micros("2024-11-03 08:30:00"),
    micros("2024-11-03 09:30:00"),
    micros("1969-12-31 23:59:59.999999"),
    # Los Angeles keeps local mean time, -07:52:58, until 1883-11-18 20:00 UTC.
    micros("1883-11-18 19:59:59.999999"),
    micros("1883-11-18 20:00:00"),
    micros("1800-06-15 12:00:00"),
    # Kathmandu moved from +05:30 to +05:45 on 1986-01-01.
    micros("1985-12-31 18:29:59.999999"),
    micros("1985-12-31 18:30:00"),
    # 0000-12-31 16:07:02 in Los Angeles.
    micros("0001-01-01 00:00:00"),
    micros("9999-12-31 23:59:59.999999"),
    # January 15 of years -264000 and 268000 at 12:00 UTC, beyond the date library's range.
    # Instants after 2099 are in winter: DST after 2099 is #1052.
    -8_393_201_294_400_000_000,
    8_395_097_169_600_000_000,
    None,
]
# Converting the smallest instant west of UTC, or the largest east of it, overflows BIGINT.
EXTREME_INSTANTS = [MIN_MICROS, MAX_MICROS]
REQUIRED_INSTANTS = [micros("2024-01-02 13:04:05.123456"), micros("2024-11-03 09:30:00")]

OVERLAP = "TIMESTAMP_NTZ '2024-11-03 01:30:00'"

COLUMN_EXPRESSIONS = [
    ("cast", "CAST(ts AS TIMESTAMP_NTZ)"),
    ("try_cast", "try_cast(ts AS TIMESTAMP_NTZ)"),
    ("colon_cast", "ts::timestamp_ntz"),
    ("to_timestamp_ntz", "to_timestamp_ntz(ts)"),
    # CONVERT_TIMEZONE converts a TIMESTAMP argument to TIMESTAMP_NTZ in the session zone.
    ("convert_timezone_utc", "convert_timezone('UTC', 'UTC', ts)"),
    ("convert_timezone_tokyo", "convert_timezone('UTC', 'Asia/Tokyo', ts)"),
    ("convert_timezone_session", "convert_timezone('Asia/Tokyo', ts)"),
    # Expressions over the converted local date-time.
    ("hour", "hour(CAST(ts AS TIMESTAMP_NTZ))"),
    ("cast_string", "CAST(CAST(ts AS TIMESTAMP_NTZ) AS STRING)"),
    ("cast_date", "CAST(CAST(ts AS TIMESTAMP_NTZ) AS DATE)"),
    ("round_trip", "CAST(CAST(ts AS TIMESTAMP_NTZ) AS TIMESTAMP)"),
    ("equals_ntz", "CAST(ts AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2024-01-02 05:04:05.123456'"),
    # Spark's optimizer compares the instant of a TIMESTAMP_NTZ literal instead: in a repeated
    # hour, the earlier one.
    ("overlap_equals", f"CAST(ts AS TIMESTAMP_NTZ) = {OVERLAP}"),
    ("overlap_not_equals", f"CAST(ts AS TIMESTAMP_NTZ) != {OVERLAP}"),
    ("overlap_null_safe", f"CAST(ts AS TIMESTAMP_NTZ) <=> {OVERLAP}"),
    ("overlap_less", f"CAST(ts AS TIMESTAMP_NTZ) < {OVERLAP}"),
    ("overlap_at_most", f"CAST(ts AS TIMESTAMP_NTZ) <= {OVERLAP}"),
    ("overlap_greater", f"CAST(ts AS TIMESTAMP_NTZ) > {OVERLAP}"),
    ("overlap_at_least", f"CAST(ts AS TIMESTAMP_NTZ) >= {OVERLAP}"),
    ("overlap_reversed", f"{OVERLAP} = CAST(ts AS TIMESTAMP_NTZ)"),
    ("overlap_in", f"CAST(ts AS TIMESTAMP_NTZ) IN ({OVERLAP}, "
                   "TIMESTAMP_NTZ '2024-01-02 05:04:05.123456')"),
    ("overlap_between", "CAST(ts AS TIMESTAMP_NTZ) "
                        f"BETWEEN TIMESTAMP_NTZ '2024-11-03 01:00:00' AND {OVERLAP}"),
    ("overlap_try_cast", f"try_cast(ts AS TIMESTAMP_NTZ) = {OVERLAP}"),
    ("overlap_to_timestamp_ntz", f"to_timestamp_ntz(ts) = {OVERLAP}"),
    ("overlap_string", "CAST(ts AS TIMESTAMP_NTZ) = CAST('2024-11-03 01:30:00' AS TIMESTAMP_NTZ)"),
    ("overlap_null", "CAST(ts AS TIMESTAMP_NTZ) = CAST(NULL AS TIMESTAMP_NTZ)"),
    # Los Angeles skips 2024-03-10 02:30, and repeats 1883-11-18 12:00-12:07:02.
    ("gap_equals", "CAST(ts AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2024-03-10 02:30:00'"),
    ("gap_end_equals", "CAST(ts AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2024-03-10 03:00:00'"),
    ("gap_at_least", "CAST(ts AS TIMESTAMP_NTZ) >= TIMESTAMP_NTZ '2024-03-10 02:30:00'"),
    ("local_mean_time_equals", "CAST(ts AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '1883-11-18 12:00:00'"),
    ("local_mean_time_at_least",
     "CAST(ts AS TIMESTAMP_NTZ) >= TIMESTAMP_NTZ '1883-11-18 12:00:00'"),
    ("coalesce", "coalesce(CAST(ts AS TIMESTAMP_NTZ), TIMESTAMP_NTZ '2000-01-01 00:00:00')"),
    # TIMESTAMP targets keep the instant.
    ("to_timestamp", "to_timestamp(ts)"),
    ("try_to_timestamp", "try_to_timestamp(ts)"),
    ("to_timestamp_ltz", "to_timestamp_ltz(ts)"),
    # Formatted parsing and TIMESTAMP() of a TIMESTAMP, outside this conversion.
    ("to_timestamp_ntz_format", "to_timestamp_ntz(ts, 'yyyy-MM-dd HH:mm:ss')"),
    ("to_timestamp_format", "to_timestamp(ts, 'yyyy-MM-dd HH:mm:ss')"),
    ("timestamp_function", "timestamp(ts)"),
    # A nested value converted by a cast of its container.
    ("array_cast", "CAST(array(ts) AS ARRAY<TIMESTAMP_NTZ>)[0]"),
    ("struct_cast", "CAST(named_struct('t', ts) AS STRUCT<t: TIMESTAMP_NTZ>).t"),
    ("map_cast", "CAST(map('x', ts) AS MAP<STRING, TIMESTAMP_NTZ>)['x']"),
]

EXTREME_EXPRESSIONS = [
    ("cast", "CAST(ts AS TIMESTAMP_NTZ)"),
    ("try_cast", "try_cast(ts AS TIMESTAMP_NTZ)"),
    ("to_timestamp_ntz", "to_timestamp_ntz(ts)"),
    ("convert_timezone_utc", "convert_timezone('UTC', 'UTC', ts)"),
    # A comparison Spark unwraps never converts the instant.
    ("at_least", "CAST(ts AS TIMESTAMP_NTZ) >= TIMESTAMP_NTZ '2024-01-01 00:00:00'"),
    ("try_at_least", "try_cast(ts AS TIMESTAMP_NTZ) >= TIMESTAMP_NTZ '2024-01-01 00:00:00'"),
]

LITERAL_EXPRESSIONS = [
    # TIMESTAMP literals without an offset are read in the session zone.
    ("session_text", "CAST(TIMESTAMP '2024-01-02 13:04:05.123456' AS TIMESTAMP_NTZ)"),
    ("utc_text", "CAST(TIMESTAMP '2024-01-02 13:04:05.123456+00:00' AS TIMESTAMP_NTZ)"),
    ("gap_text", "CAST(TIMESTAMP '2024-03-10 02:30:00' AS TIMESTAMP_NTZ)"),
    ("overlap_text", "CAST(TIMESTAMP '2024-11-03 01:30:00' AS TIMESTAMP_NTZ)"),
    ("overlap_standard", "CAST(TIMESTAMP '2024-11-03 09:30:00+00:00' AS TIMESTAMP_NTZ)"),
    ("pre_epoch", "CAST(TIMESTAMP '1969-12-31 23:59:59.999999+00:00' AS TIMESTAMP_NTZ)"),
    ("local_mean_time", "CAST(TIMESTAMP '1800-06-15 12:00:00+00:00' AS TIMESTAMP_NTZ)"),
    ("first_year", "CAST(TIMESTAMP '0001-01-01 00:00:00+00:00' AS TIMESTAMP_NTZ)"),
    ("last_year", "CAST(TIMESTAMP '9999-12-31 23:59:59.999999+00:00' AS TIMESTAMP_NTZ)"),
    ("micros_max", f"CAST(timestamp_micros({MAX_MICROS}) AS TIMESTAMP_NTZ)"),
    ("micros_min", f"CAST(timestamp_micros({MIN_MICROS}) AS TIMESTAMP_NTZ)"),
    ("try_micros_max", f"try_cast(timestamp_micros({MAX_MICROS}) AS TIMESTAMP_NTZ)"),
    ("seconds", "CAST(timestamp_seconds(1704200645) AS TIMESTAMP_NTZ)"),
    ("null", "CAST(CAST(NULL AS TIMESTAMP) AS TIMESTAMP_NTZ)"),
    ("try_cast", "try_cast(TIMESTAMP '2024-01-02 13:04:05.123456+00:00' AS TIMESTAMP_NTZ)"),
    ("colon_cast", "TIMESTAMP '2024-01-02 13:04:05.123456+00:00'::timestamp_ntz"),
    ("to_timestamp_ntz", "to_timestamp_ntz(TIMESTAMP '2024-01-02 13:04:05.123456+00:00')"),
    ("convert_timezone",
     "convert_timezone('UTC', 'UTC', TIMESTAMP '2024-01-02 13:04:05.123456+00:00')"),
    ("hour", "hour(CAST(TIMESTAMP '2024-01-02 13:04:05.123456+00:00' AS TIMESTAMP_NTZ))"),
    # Constant conversions inside CONVERT_TIMEZONE and TO_TIMESTAMP_NTZ.
    ("convert_timezone_micros_min",
     f"convert_timezone('UTC', 'UTC', timestamp_micros({MIN_MICROS}))"),
    ("convert_timezone_micros_max",
     f"convert_timezone('UTC', 'UTC', timestamp_micros({MAX_MICROS}))"),
    ("to_timestamp_ntz_micros_min", f"to_timestamp_ntz(timestamp_micros({MIN_MICROS}))"),
    ("to_timestamp_ntz_micros_max", f"to_timestamp_ntz(timestamp_micros({MAX_MICROS}))"),
    # Formatted parsing and TIMESTAMP() of a TIMESTAMP, outside this conversion.
    ("to_timestamp_ntz_format",
     "to_timestamp_ntz(TIMESTAMP '2024-01-02 13:04:05.123456+00:00', 'yyyy-MM-dd')"),
    ("timestamp_function", "timestamp(TIMESTAMP '2024-01-02 13:04:05.123456+00:00')"),
]

STATEMENTS = [
    ("filter_at_least", "SELECT k FROM ntz_inputs "
                        "WHERE CAST(ts AS TIMESTAMP_NTZ) >= TIMESTAMP_NTZ '2024-01-02 05:04:05'"),
    # Both Los Angeles overlap instants read 01:30.
    ("filter_equals", "SELECT k FROM ntz_inputs "
                      "WHERE CAST(ts AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2024-11-03 01:30:00'"),
    ("group_by", "SELECT CAST(ts AS TIMESTAMP_NTZ) AS r, count(*) AS n FROM ntz_inputs "
                 "GROUP BY CAST(ts AS TIMESTAMP_NTZ)"),
    ("required_cast", "SELECT k, CAST(ts AS TIMESTAMP_NTZ) AS r FROM required_inputs"),
    ("required_try_cast", "SELECT k, try_cast(ts AS TIMESTAMP_NTZ) AS r FROM required_inputs"),
    ("required_to_timestamp_ntz", "SELECT k, to_timestamp_ntz(ts) AS r FROM required_inputs"),
    # A NOT NULL extreme instant whose conversion overflows.
    ("required_extreme_min_cast",
     "SELECT k, CAST(ts AS TIMESTAMP_NTZ) AS r FROM required_extreme_inputs WHERE k = 0"),
    ("required_extreme_max_cast",
     "SELECT k, CAST(ts AS TIMESTAMP_NTZ) AS r FROM required_extreme_inputs WHERE k = 1"),
    ("required_extreme_min_try_cast",
     "SELECT k, try_cast(ts AS TIMESTAMP_NTZ) AS r FROM required_extreme_inputs WHERE k = 0"),
    ("required_extreme_max_try_cast",
     "SELECT k, try_cast(ts AS TIMESTAMP_NTZ) AS r FROM required_extreme_inputs WHERE k = 1"),
    ("empty_cast", "SELECT k, CAST(ts AS TIMESTAMP_NTZ) AS r FROM empty_inputs"),
]


def definitions():
    for name, expression in COLUMN_EXPRESSIONS:
        yield f"column_{name}", f"SELECT k, {expression} AS r FROM ntz_inputs"
    for key, label in enumerate(["min", "max"]):
        for name, expression in EXTREME_EXPRESSIONS:
            yield (f"extreme_{label}_{name}",
                   f"SELECT k, {expression} AS r FROM extreme_inputs WHERE k = {key}")
    for name, expression in LITERAL_EXPRESSIONS:
        yield f"literal_{name}", f"SELECT {expression} AS r"
    yield from STATEMENTS


def table(name, instants, nullable):
    return {"name": name,
            "schema": {"type": "struct", "fields": [
                {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "ts", "type": "timestamp", "nullable": nullable, "metadata": {}}]},
            "rows": [[str(key), None if value is None else str(value)]
                     for key, value in enumerate(instants)]}


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
        return struct.pack(">d", row.getDouble(index)).hex()
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


def register(spark, recorded, types):
    """Register `recorded` as stored columns: the instants are built from BIGINT values,
    which reach beyond Python's years, and checkpointed so queries read a scan."""
    longs = types.StructType([
        types.StructField("k", types.IntegerType(), False),
        types.StructField("m", types.LongType(), recorded["schema"]["fields"][1]["nullable"])])
    rows = [[int(key), None if value is None else int(value)] for key, value in recorded["rows"]]
    frame = spark.createDataFrame(rows, longs) \
        .selectExpr("k", "timestamp_micros(m) AS ts").localCheckpoint(eager=True)
    assert json.loads(frame.schema.json()) == recorded["schema"], frame.schema.json()
    frame.createOrReplaceTempView(recorded["name"])
    # The registered values must be exactly the recorded encodings.
    frame = spark.table(recorded["name"])
    stored = [[encode_value(row, index, field.dataType, types)
               for index, field in enumerate(frame.schema.fields)]
              for row in frame._jdf.queryExecution().executedPlan().executeCollect()]
    assert stored == recorded["rows"], stored


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
    spark = (SparkSession.builder.master("local[1]").appName("dar-timestamp-ntz-conversion")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate projections over local rows during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        inputs = [table("ntz_inputs", INSTANTS, True),
                  table("extreme_inputs", EXTREME_INSTANTS, True),
                  table("required_inputs", REQUIRED_INSTANTS, False),
                  table("required_extreme_inputs", EXTREME_INSTANTS, False),
                  table("empty_inputs", [], True)]
        for recorded in inputs:
            register(spark, recorded, types)
        header = {
            "spark_version": spark.version, "spark_build": build,
            "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
            "python_version": sys.version.split()[0],
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "value_encoding": ("INT and DATE days as int; BIGINT, TIMESTAMP UTC-epoch and "
                               "TIMESTAMP_NTZ wall-clock microseconds as long; BOOLEAN as "
                               "true or false; STRING as text"),
            "settings": {
                "spark.sql.timestampType": spark.conf.get("spark.sql.timestampType"),
                "spark.sql.optimizer.excludedRules":
                    spark.conf.get("spark.sql.optimizer.excludedRules")},
            "inputs": inputs,
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
