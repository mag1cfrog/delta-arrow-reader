"""Capture Spark TIMESTAMPDIFF result fields, typed values and errors (#1006).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages for TIMESTAMPDIFF and the three-argument DATEDIFF, DATE_DIFF and TIMEDIFF forms in
every unit, with TIMESTAMP, TIMESTAMP_NTZ, DATE, STRING and NULL arguments, literal and
registered-column inputs, unit spellings and unsupported units or argument types. Every
query runs in both ANSI modes in UTC and America/Los_Angeles. Run from the crate root in the
reference environment described in README.md; the output path must not exist.
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
UNITS = ["YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND", "MILLISECOND",
         "MICROSECOND"]
MIN_MICROS = "timestamp_micros(-9223372036854775807)"
MAX_MICROS = "timestamp_micros(9223372036854775807)"

# TIMESTAMP literals: wall-clock text is read in the session zone; `+00:00` fixes an instant.
TIMESTAMP_PAIRS = [
    ("same_instant", "TIMESTAMP '2024-01-01 00:00:00'", "TIMESTAMP '2024-01-01 00:00:00'"),
    # The issue's reproduction: two hours across midnight.
    ("partial_day", "TIMESTAMP '2024-01-01 23:00:00'", "TIMESTAMP '2024-01-02 01:00:00'"),
    ("partial_day_reverse", "TIMESTAMP '2024-01-02 01:00:00'", "TIMESTAMP '2024-01-01 23:00:00'"),
    ("whole_year", "TIMESTAMP '2024-01-01 00:00:00'", "TIMESTAMP '2025-01-01 00:00:00'"),
    ("one_micro_back", "TIMESTAMP '2024-01-01 00:00:00.000001'",
     "TIMESTAMP '2024-01-01 00:00:00'"),
    ("before_epoch", "TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '1969-12-31 23:59:58.5'"),
    ("month_end", "TIMESTAMP '2024-01-31 00:00:00'", "TIMESTAMP '2024-02-29 00:00:00'"),
    ("month_end_time", "TIMESTAMP '2024-01-29 12:00:00'", "TIMESTAMP '2024-02-29 11:00:00'"),
    ("month_end_reverse", "TIMESTAMP '2024-03-31 10:00:00'", "TIMESTAMP '2024-02-29 11:00:00'"),
    ("leap_year", "TIMESTAMP '2000-02-29 00:00:00'", "TIMESTAMP '2001-02-28 00:00:00'"),
    ("leap_year_reverse", "TIMESTAMP '2001-02-28 00:00:00'", "TIMESTAMP '2000-02-29 00:00:00'"),
    ("quarter_reverse", "TIMESTAMP '2024-05-15 00:00:00'", "TIMESTAMP '2024-01-15 00:00:00'"),
    # Los Angeles skips 02:00-03:00 on 2024-03-10 and repeats 01:00-02:00 on 2024-11-03.
    ("gap_wall_clock", "TIMESTAMP '2024-03-10 01:30:00'", "TIMESTAMP '2024-03-10 03:30:00'"),
    ("gap_instants", "TIMESTAMP '2024-03-10 09:30:00+00:00'",
     "TIMESTAMP '2024-03-10 10:30:00+00:00'"),
    ("overlap_instants", "TIMESTAMP '2024-11-03 08:30:00+00:00'",
     "TIMESTAMP '2024-11-03 09:30:00+00:00'"),
    ("overlap_reverse", "TIMESTAMP '2024-11-03 09:30:00+00:00'",
     "TIMESTAMP '2024-11-03 08:30:00+00:00'"),
    ("dst_day", "TIMESTAMP '2024-03-09 12:00:00'", "TIMESTAMP '2024-03-10 12:00:00'"),
    ("dst_week", "TIMESTAMP '2024-03-04 12:00:00'", "TIMESTAMP '2024-03-11 12:00:00'"),
    ("dst_month", "TIMESTAMP '2024-02-10 02:30:00'", "TIMESTAMP '2024-03-10 03:00:00'"),
    # A transition beyond the zone database's listed years.
    ("far_future_gap", "TIMESTAMP '2100-03-14 09:30:00+00:00'",
     "TIMESTAMP '2100-03-14 10:30:00+00:00'"),
    ("calendar_range", "TIMESTAMP '0001-01-01 00:00:00'",
     "TIMESTAMP '9999-12-31 23:59:59.999999'"),
    ("micros_to_max", "timestamp_micros(0)", MAX_MICROS),
    ("micros_to_min", "timestamp_micros(0)", MIN_MICROS),
    ("micros_extremes", MIN_MICROS, MAX_MICROS),
]

# Other argument types, converted to TIMESTAMP in the session zone.
TYPE_PAIRS = [
    ("ntz_partial_day", "TIMESTAMP_NTZ '2024-01-01 23:00:00'",
     "TIMESTAMP_NTZ '2024-01-02 01:00:00'"),
    ("ntz_gap", "TIMESTAMP_NTZ '2024-03-10 02:30:00'", "TIMESTAMP_NTZ '2024-03-10 03:30:00'"),
    ("ntz_overlap", "TIMESTAMP_NTZ '2024-11-03 01:30:00'", "TIMESTAMP_NTZ '2024-11-03 02:30:00'"),
    ("date_dst_day", "DATE '2024-03-10'", "DATE '2024-03-11'"),
    ("date_month_end", "DATE '2024-01-31'", "DATE '2024-02-29'"),
    ("date_before_epoch", "DATE '1969-12-31'", "DATE '1970-01-01'"),
    ("date_timestamp", "DATE '2022-01-01'", "TIMESTAMP '2021-12-31 23:59:50'"),
    ("ntz_timestamp", "TIMESTAMP_NTZ '2024-03-10 01:30:00'",
     "TIMESTAMP '2024-03-10 10:30:00+00:00'"),
    ("date_ntz", "DATE '2024-03-10'", "TIMESTAMP_NTZ '2024-03-10 03:00:00'"),
    ("strings", "'2024-01-01'", "'2024-01-03 12:00:00'"),
    ("invalid_string", "'bad'", "TIMESTAMP '2024-01-03 00:00:00'"),
]

NULL_PAIRS = [
    ("null_start", "CAST(NULL AS TIMESTAMP)", "TIMESTAMP '2024-01-02 00:00:00'"),
    ("null_end", "TIMESTAMP '2024-01-01 00:00:00'", "CAST(NULL AS TIMESTAMP)"),
    ("untyped_null_start", "NULL", "TIMESTAMP '2024-01-02 00:00:00'"),
    ("untyped_nulls", "NULL", "NULL"),
]

UNSUPPORTED_PAIRS = [
    ("int_arguments", "1", "2"),
    ("boolean_arguments", "true", "false"),
    ("time_arguments", "TIME '01:00:00'", "TIME '02:00:00'"),
]

PARTIAL_DAY = "TIMESTAMP '2024-01-01 23:00:00', TIMESTAMP '2024-01-02 01:00:00'"
OTHER_PROBES = [
    ("lower_unit", f"SELECT timestampdiff(day, {PARTIAL_DAY}) AS r"),
    ("mixed_case_unit", f"SELECT datediff(Hour, {PARTIAL_DAY}) AS r"),
    ("string_unit", f"SELECT timestampdiff('DAY', {PARTIAL_DAY}) AS r"),
    ("string_unit_datediff", f"SELECT datediff('DAY', {PARTIAL_DAY}) AS r"),
    ("string_unit_date_diff", f"SELECT date_diff('hour', {PARTIAL_DAY}) AS r"),
    ("double_quoted_unit", f"SELECT timestampdiff(\"DAY\", {PARTIAL_DAY}) AS r"),
    ("unknown_unit", f"SELECT timestampdiff(FOO, {PARTIAL_DAY}) AS r"),
    ("plural_unit", f"SELECT timestampdiff(DAYS, {PARTIAL_DAY}) AS r"),
    ("unknown_unit_datediff", f"SELECT datediff(FOO, {PARTIAL_DAY}) AS r"),
    ("unknown_unit_date_diff", f"SELECT date_diff(DAYS, {PARTIAL_DAY}) AS r"),
    ("column_unit", "SELECT timestampdiff(k, s, e) AS r FROM diff_inputs"),
    ("column_unit_datediff", "SELECT datediff(k, s, e) AS r FROM diff_inputs"),
    ("dayofyear", f"SELECT timestampdiff(DAYOFYEAR, {PARTIAL_DAY}) AS r"),
    ("dayofyear_datediff", f"SELECT datediff(DAYOFYEAR, {PARTIAL_DAY}) AS r"),
    ("dayofyear_date_diff", f"SELECT date_diff(DAYOFYEAR, {PARTIAL_DAY}) AS r"),
    ("timestamp_diff_name", f"SELECT timestamp_diff(DAY, {PARTIAL_DAY}) AS r"),
    ("two_argument_datediff", "SELECT datediff(TIMESTAMP '2024-01-02 01:00:00', "
                              "TIMESTAMP '2024-01-01 23:00:00') AS r"),
    ("unaliased_timestampdiff", f"SELECT timestampdiff(HOUR, {PARTIAL_DAY})"),
    ("unaliased_datediff", f"SELECT datediff(day, {PARTIAL_DAY})"),
    ("unaliased_date_diff", "SELECT date_diff(Month, DATE '2024-01-31', DATE '2024-03-01')"),
    ("unaliased_timediff", f"SELECT timediff(SECOND, {PARTIAL_DAY})"),
    ("unaliased_columns", "SELECT timestampdiff(HOUR, s, e) FROM diff_inputs"),
    ("dayofyear_columns", "SELECT k, timestampdiff(DAYOFYEAR, s, e) AS r FROM diff_inputs"),
    ("dayofyear_empty", "SELECT timestampdiff(DAYOFYEAR, s, e) AS r FROM empty_diff_inputs"),
    ("dayofyear_empty_datediff",
     "SELECT datediff(DAYOFYEAR, sd, ed) AS r FROM empty_diff_inputs"),
    ("empty_hour", "SELECT timestampdiff(HOUR, s, e) AS r FROM empty_diff_inputs"),
    ("empty_month", "SELECT timestampdiff(MONTH, sn, en) AS r FROM empty_diff_inputs"),
    ("invalid_string_column",
     "SELECT k, timestampdiff(DAY, ss, TIMESTAMP '2024-01-01 00:00:00') AS r FROM bad_strings"),
    ("date_and_timestamp_columns", "SELECT k, timestampdiff(HOUR, sd, s) AS r FROM diff_inputs"),
    ("ntz_and_timestamp_columns", "SELECT k, timestampdiff(MINUTE, sn, e) AS r FROM diff_inputs"),
    ("column_and_literal",
     "SELECT k, timestampdiff(DAY, s, TIMESTAMP '2024-06-01 00:00:00') AS r FROM diff_inputs"),
    # Constant failures fold during optimization unless a conditional branch may skip them.
    ("dayofyear_skipped_branch",
     f"SELECT CASE WHEN k > 10 THEN timestampdiff(DAYOFYEAR, {PARTIAL_DAY}) ELSE 0 END AS r "
     "FROM diff_inputs"),
    ("dayofyear_true_branch",
     f"SELECT CASE WHEN true THEN timestampdiff(DAYOFYEAR, {PARTIAL_DAY}) END AS r"),
    ("overflow_coalesce",
     f"SELECT coalesce(1, timestampdiff(MICROSECOND, {MIN_MICROS}, {MAX_MICROS})) AS r"),
    ("invalid_string_skipped_branch",
     "SELECT CASE WHEN k > 10 THEN timestampdiff(DAY, 'bad', TIMESTAMP '2024-01-01 00:00:00') "
     "ELSE 0 END AS r FROM diff_inputs"),
    ("dayofyear_limit_zero", f"SELECT timestampdiff(DAYOFYEAR, {PARTIAL_DAY}) AS r LIMIT 0"),
    ("dayofyear_empty_input",
     f"SELECT timestampdiff(DAYOFYEAR, {PARTIAL_DAY}) AS r FROM empty_diff_inputs"),
    ("dayofyear_false_filter",
     f"SELECT timestampdiff(DAYOFYEAR, {PARTIAL_DAY}) AS r FROM diff_inputs WHERE false"),
]

TABLE_FIELDS = [("k", "integer", False), ("s", "timestamp", True), ("e", "timestamp", True),
                ("sn", "timestamp_ntz", True), ("en", "timestamp_ntz", True),
                ("sd", "date", True), ("ed", "date", True),
                ("ss", "string", True), ("es", "string", True)]
# Per row: start and end as UTC instants, wall clocks, days and session-zone strings.
TABLE_ROWS = [
    ("2024-03-10 09:30:00", "2024-03-10 10:30:00", "2024-03-10 02:30:00", "2024-03-10 03:30:00",
     "2024-03-10", "2024-03-11", "2024-03-10 01:30:00", "2024-03-10 03:30:00"),
    ("2024-11-03 08:30:00", "2024-11-03 09:30:00", "2024-11-03 01:30:00", "2024-11-03 02:30:00",
     "2024-11-03", "2024-11-04", "2024-11-03 01:30:00", "2024-11-03 01:59:59.999999"),
    ("1970-01-01 00:00:00", "1969-12-31 23:59:58.500000", "1970-01-01 00:00:00",
     "1969-12-31 23:59:58.500000", "1970-01-01", "1969-12-31", "1970-01-01",
     "1969-12-31 23:59:58.5"),
    ("2024-01-31 12:00:00", "2024-02-29 11:00:00", "2024-01-31 12:00:00", "2024-02-29 11:00:00",
     "2024-01-31", "2024-02-29", "2024-01-31 12:00:00", "2024-02-29 11:00:00"),
    (None, "2024-01-01 00:00:00", "2024-01-01 00:00:00", None, None, None, None, "2024-01-01"),
]


def definitions():
    for unit in UNITS:
        for name, start, end in TIMESTAMP_PAIRS + TYPE_PAIRS:
            yield f"{unit.lower()}_{name}", f"SELECT timestampdiff({unit}, {start}, {end}) AS r"
    for unit in ["DAY", "HOUR", "MONTH"]:
        for name, start, end in NULL_PAIRS:
            yield f"{unit.lower()}_{name}", f"SELECT timestampdiff({unit}, {start}, {end}) AS r"
    for name, start, end in UNSUPPORTED_PAIRS:
        yield f"day_{name}", f"SELECT timestampdiff(DAY, {start}, {end}) AS r"
    for unit in UNITS:
        for function in ["datediff", "date_diff", "timediff"]:
            yield (f"{function}_{unit.lower()}",
                   f"SELECT {function}({unit}, {PARTIAL_DAY}) AS r")
        for columns, (start, end) in [("timestamp", ("s", "e")), ("ntz", ("sn", "en")),
                                      ("date", ("sd", "ed"))]:
            yield (f"{unit.lower()}_{columns}_columns",
                   f"SELECT k, timestampdiff({unit}, {start}, {end}) AS r FROM diff_inputs")
    for unit in ["MICROSECOND", "HOUR", "DAY", "MONTH"]:
        yield (f"{unit.lower()}_string_columns",
               f"SELECT k, timestampdiff({unit}, ss, es) AS r FROM diff_inputs")
    yield from OTHER_PROBES


def micros(text, aware):
    """Encode a wall-clock text as epoch microseconds without float arithmetic."""
    value = datetime.fromisoformat(text)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc if aware else None)
    if aware:
        value = value.replace(tzinfo=timezone.utc)
    delta = value - epoch
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def days(text):
    return (date.fromisoformat(text) - date(1970, 1, 1)).days


def table(name, rows):
    def encode(row):
        s, e, sn, en, sd, ed, ss, es = row
        values = [micros(s, True) if s else None, micros(e, True) if e else None,
                  micros(sn, False) if sn else None, micros(en, False) if en else None,
                  days(sd) if sd else None, days(ed) if ed else None]
        return [None if value is None else str(value) for value in values] + [ss, es]

    return {"name": name,
            "schema": {"type": "struct", "fields": [
                {"name": field, "type": kind, "nullable": nullable, "metadata": {}}
                for field, kind, nullable in TABLE_FIELDS]},
            "rows": [[str(key)] + encode(row) for key, row in enumerate(rows)]}


def bad_strings():
    return {"name": "bad_strings",
            "schema": {"type": "struct", "fields": [
                {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "ss", "type": "string", "nullable": True, "metadata": {}}]},
            "rows": [["0", "2024-01-02"], ["1", "bad"], ["2", None]]}


def python_value(value, kind):
    """Build the exact Python value Spark converts back to the recorded encoding."""
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "date":
        return date(1970, 1, 1) + timedelta(days=int(value))
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    if kind == "timestamp_ntz":
        return datetime(1970, 1, 1) + timedelta(microseconds=int(value))
    return value


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.TimestampType, types.TimestampNTZType)):
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-timestamp-diff")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        inputs = [table("diff_inputs", TABLE_ROWS), table("empty_diff_inputs", []),
                  bad_strings()]
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
        header = {
            "spark_version": spark.version, "spark_build": build,
            "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
            "python_version": sys.version.split()[0],
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "value_encoding": ("INT and DATE days as int; BIGINT, TIMESTAMP UTC-epoch and "
                               "TIMESTAMP_NTZ wall-clock microseconds as long; STRING as text"),
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
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({
            "observations": len(records) - 1,
            "errors": sum(r["expected"]["status"] == "error" for r in records[1:]),
            "missing_exception": sum(r["expected"]["status"] == "error"
                                     and r["expected"]["exception"] is None
                                     for r in records[1:]),
            "output": str(args.output),
            "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
