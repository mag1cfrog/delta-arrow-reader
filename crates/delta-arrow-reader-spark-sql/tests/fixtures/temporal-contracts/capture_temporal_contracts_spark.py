"""Capture Spark DATE_FORMAT tokens, date/timestamp parsing and timestamp type contracts (#153).

Records analyzed, optimized and executed schemas, typed internal-row values and
failing stages for DATE_FORMAT patterns, unformatted and formatted parsing with
CAST, TRY_CAST, TO_* and TRY_TO_TIMESTAMP, typed literals, and conversions
between DATE, TIMESTAMP and TIMESTAMP_NTZ. Every query runs in both ANSI modes
with UTC and America/Los_Angeles session zones. Run from the crate root in the
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

VALUES_TABLE = "temporal_values"
PARSE_TABLE = "parse_inputs"

# (DATE, TIMESTAMP instant in UTC, TIMESTAMP_NTZ wall clock, per-row format).
TEMPORAL_ROWS = [
    ("2024-01-02", "2024-01-02 13:04:05.123456", "2024-01-02 13:04:05.123456", "yyyy-MM-dd"),
    ("1969-12-31", "1969-12-31 23:59:59.999999", "1969-12-31 23:59:59.999999",
     "HH:mm:ss.SSSSSS"),
    ("0001-01-01", "0001-01-02 00:00:00", "0001-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss z"),
    ("9999-12-31", "9999-12-31 23:59:59.999999", "9999-12-31 23:59:59.999999",
     "EEEE, MMMM d, yyyy"),
    # 03:30 PDT just after the spring gap; the NTZ value is the skipped local time.
    ("2024-03-10", "2024-03-10 10:30:00", "2024-03-10 02:30:00", "yyyy-MM-dd HH:mm:ss XXX"),
    # 01:30 PDT, the first occurrence in the fall overlap; the NTZ value is ambiguous.
    ("2024-11-03", "2024-11-03 08:30:00", "2024-11-03 01:30:00", "hh:mm a VV"),
    # 01:30 PST, the second occurrence; the NTZ value predates standard time zones.
    ("1800-01-01", "2024-11-03 09:30:00", "1800-01-01 00:00:00", "''yyyy''"),
    (None, None, None, None),
]

FORMAT_PATTERNS = [
    # The frozen experiment's patterns.
    "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss.SSS",
    "G", "GGGG", "GGGGG", "y", "yy", "yyy", "yyyy", "yyyyy", "yyyyyy",
    "D", "DD", "DDD", "M", "MM", "MMM", "MMMM", "MMMMM", "L", "LL", "LLL", "LLLL",
    "d", "dd", "ddd", "Q", "QQ", "QQQ", "QQQQ", "q", "qq", "E", "EE", "EEE", "EEEE", "EEEEE",
    "F", "a", "aa", "h", "hh", "K", "KK", "k", "kk", "H", "HH", "HHH", "m", "mm", "s", "ss",
    "S", "SS", "SSS", "SSSS", "SSSSS", "SSSSSS", "SSSSSSS", "SSSSSSSSS", "SSSSSSSSSS",
    "V", "VV", "z", "zz", "zzz", "zzzz", "zzzzz", "O", "OO", "OOOO",
    "X", "XX", "XXX", "XXXX", "XXXXX", "XXXXXX", "x", "xx", "xxx", "xxxx", "xxxxx",
    "Z", "ZZ", "ZZZ", "ZZZZ", "ZZZZZ", "ZZZZZZ",
    "'T'", "''", "yyyy'T'HH", "'it''s' yyyy", "'unclosed", "[yyyy]", "yyyy[-MM]", "yyyy]",
    "Y", "YYYY", "w", "W", "u", "e", "c", "A", "n", "N", "p", "B", "b", "#", "{", "}", "!",
    "yyyy-MM-dd hh:mm:ss a", "EEEE, MMMM d, yyyy", "", " ",
]

# Literal inputs for DATE_FORMAT, interpreted in the session zone where applicable.
FORMAT_LITERALS = ("DATE '2024-01-02'", "TIMESTAMP '2024-01-02 13:04:05.123456'",
                   "TIMESTAMP_NTZ '2024-01-02 13:04:05.123456'")

# The kind of input formatted in each output column: a key, a DATE, a TIMESTAMP literal (in
# the session zone), the UTC-annotated TIMESTAMP column, a TIMESTAMP_NTZ or a NULL.
LITERAL_INPUTS = ["date", "timestamp_literal", "timestamp_ntz"]
COLUMN_INPUTS = ["key", "date", "timestamp_column", "timestamp_ntz"]

FORMAT_PROBES = [
    ("format_null_pattern", "SELECT DATE_FORMAT(TIMESTAMP '2024-01-02 13:04:05', "
                            "CAST(NULL AS STRING)) AS r", ["timestamp_literal"]),
    ("format_null_value", "SELECT DATE_FORMAT(CAST(NULL AS TIMESTAMP), 'yyyy') AS r", ["null"]),
    ("format_unaliased", "SELECT DATE_FORMAT(TIMESTAMP '2024-01-02 13:04:05', 'yyyy-MM-dd')",
     ["timestamp_literal"]),
    ("format_column_pattern", f"SELECT k, DATE_FORMAT(d, f) AS d, DATE_FORMAT(ts, f) AS ts, "
                              f"DATE_FORMAT(ntz, f) AS ntz FROM {VALUES_TABLE}", COLUMN_INPUTS),
    ("format_column_invalid_pattern", f"SELECT k, DATE_FORMAT(ts, CASE WHEN k = 0 THEN 'Y' "
                                      f"ELSE f END) AS r FROM {VALUES_TABLE}",
     ["key", "timestamp_column"]),
    ("format_literal_dst", "SELECT DATE_FORMAT(TIMESTAMP '2024-07-04 00:05:06.000789+00:00', "
                           "'yyyy-MM-dd HH:mm:ss.SSSSSS zzz XXX') AS r", ["timestamp_literal"]),
    # The frozen experiment's DATE_FORMAT probes, verbatim.
    ("frozen_spark_date_format", "SELECT DATE_FORMAT(DATE '2024-01-02', 'yyyy-MM-dd') AS v",
     ["date"]),
    ("frozen_dates_format_tokens", "SELECT DATE_FORMAT(TIMESTAMP '2024-01-02 13:04:05.123456', "
                                   "'yyyy-MM-dd HH:mm:ss.SSS')", ["timestamp_literal"]),
]

# The frozen experiment's parsing and precision probes, verbatim.
FROZEN_PARSES = [
    ("frozen_dates_invalid_date", "cast_date", "SELECT CAST('2024-02-30' AS DATE)"),
    ("frozen_dates_try_date", "try_cast_date", "SELECT TRY_CAST('2024-02-30' AS DATE)"),
    ("frozen_dates_timestamp_precision", "cast_timestamp_ntz",
     "SELECT CAST('2024-01-02 03:04:05.123456' AS TIMESTAMP_NTZ)"),
    ("frozen_dates_timestamp_ltz", "cast_timestamp_ltz",
     "SELECT CAST('2024-01-02 03:04:05.123456' AS TIMESTAMP_LTZ)"),
]

# Unformatted strings; None is the NULL row.
PARSE_STRINGS = [
    ("frozen_invalid_date", "2024-02-30"),
    ("leap_day", "2024-02-29"),
    ("common_leap_day", "2023-02-29"),
    ("month_13", "2024-13-01"),
    ("day_0", "2024-01-00"),
    ("day_32", "2024-01-32"),
    ("invalid_date_time", "2024-02-30 12:00:00"),
    ("hour_24", "2024-01-01 24:00:00"),
    ("minute_60", "2024-01-01 23:60:00"),
    ("second_60", "2024-01-01 23:59:60"),
    ("frozen_precision", "2024-01-02 03:04:05.123456"),
    ("fraction_0", "2024-01-02 03:04:05"),
    ("fraction_1", "2024-01-02 03:04:05.1"),
    ("fraction_3", "2024-01-02 03:04:05.123"),
    ("fraction_5", "2024-01-02 03:04:05.12345"),
    ("fraction_7", "2024-01-02 03:04:05.1234567"),
    ("fraction_9", "2024-01-02 03:04:05.123456789"),
    ("pre_epoch_fraction", "1969-12-31 23:59:59.5"),
    ("offset", "2024-01-02 03:04:05.123456+05:30"),
    ("utc_suffix", "2024-01-02 03:04:05.123456Z"),
    ("region", "2024-01-02 03:04:05 America/Los_Angeles"),
    ("min_year", "0001-01-01 00:00:00"),
    ("max_year", "9999-12-31 23:59:59.999999"),
    ("five_digit_year", "+10000-01-01 00:00:00"),
    ("timestamp_min", "-290308-12-21 19:59:05.224192"),
    ("timestamp_max", "+294247-01-10 04:00:54.775807"),
    ("timestamp_over_max", "+294247-01-10 04:00:54.775808"),
    ("la_gap", "2024-03-10 02:30:00"),
    ("la_overlap", "2024-11-03 01:30:00"),
    ("lmt", "1800-01-01 00:00:00"),
    ("empty", ""),
    ("bad", "bad"),
]

PARSE_OPERATORS = [
    ("cast_date", lambda s: f"CAST({s} AS DATE)"),
    ("cast_timestamp", lambda s: f"CAST({s} AS TIMESTAMP)"),
    ("cast_timestamp_ntz", lambda s: f"CAST({s} AS TIMESTAMP_NTZ)"),
    ("cast_timestamp_ltz", lambda s: f"CAST({s} AS TIMESTAMP_LTZ)"),
    ("try_cast_date", lambda s: f"TRY_CAST({s} AS DATE)"),
    ("try_cast_timestamp", lambda s: f"TRY_CAST({s} AS TIMESTAMP)"),
    ("try_cast_timestamp_ntz", lambda s: f"TRY_CAST({s} AS TIMESTAMP_NTZ)"),
    ("to_date", lambda s: f"TO_DATE({s})"),
    ("try_to_date", lambda s: f"TRY_TO_DATE({s})"),
    ("to_timestamp", lambda s: f"TO_TIMESTAMP({s})"),
    ("to_timestamp_ntz", lambda s: f"TO_TIMESTAMP_NTZ({s})"),
    ("try_to_timestamp", lambda s: f"TRY_TO_TIMESTAMP({s})"),
]

TYPED_LITERALS = ["DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ"]

# (name, string, pattern) for formatted parsing; None is NULL.
FORMATTED_PARSES = [
    ("date", "2024-01-02", "yyyy-MM-dd"),
    ("invalid_date", "2024-02-30", "yyyy-MM-dd"),
    ("short_fields", "2024-1-2", "yyyy-MM-dd"),
    ("single_letter_fields", "2024-1-2", "yyyy-M-d"),
    ("mismatch", "2024-01-02", "yyyy/MM/dd"),
    ("trailing_text", "2024-01-02 extra", "yyyy-MM-dd"),
    ("missing_time", "2024-01-02", "yyyy-MM-dd HH:mm:ss"),
    ("twelve_hour", "02/01/2024 01:02:03 PM", "dd/MM/yyyy hh:mm:ss a"),
    ("micros", "2024-01-02 13:04:05.123456", "yyyy-MM-dd HH:mm:ss.SSSSSS"),
    ("short_fraction", "2024-01-02 13:04:05.123", "yyyy-MM-dd HH:mm:ss.SSSSSS"),
    ("offset", "2024-01-02T13:04:05+05:30", "yyyy-MM-dd'T'HH:mm:ssXXX"),
    ("region", "2024-01-02 13:04:05 America/New_York", "yyyy-MM-dd HH:mm:ss VV"),
    ("la_gap", "2024-03-10 02:30:00", "yyyy-MM-dd HH:mm:ss"),
    ("la_overlap", "2024-11-03 01:30:00", "yyyy-MM-dd HH:mm:ss"),
    ("pre_epoch", "1969-12-31 23:59:59.999999", "yyyy-MM-dd HH:mm:ss.SSSSSS"),
    ("time_only", "13:04:05", "HH:mm:ss"),
    ("year_only", "2024", "yyyy"),
    ("month_name", "Jan 2 2024", "MMM d yyyy"),
    ("hour_24", "2024-01-01 24:00:00", "yyyy-MM-dd HH:mm:ss"),
    ("empty", "", "yyyy-MM-dd"),
    ("null_string", None, "yyyy-MM-dd"),
    ("null_pattern", "2024-01-02", None),
    ("week_based_pattern", "2024-01-02", "YYYY-MM-dd"),
    ("formatting_only_pattern", "2024-01-02 Tue", "yyyy-MM-dd E"),
]

FORMATTED_OPERATORS = [
    ("to_date", "TO_DATE"),
    ("try_to_date", "TRY_TO_DATE"),
    ("to_timestamp", "TO_TIMESTAMP"),
    ("to_timestamp_ntz", "TO_TIMESTAMP_NTZ"),
    ("to_timestamp_ltz", "TO_TIMESTAMP_LTZ"),
    ("try_to_timestamp", "TRY_TO_TIMESTAMP"),
]

CONVERSIONS = [
    ("columns", lambda d, ts, ntz: f"{d} AS d, {ts} AS ts, {ntz} AS ntz"),
    ("ntz_to_timestamp", lambda d, ts, ntz: f"CAST({ntz} AS TIMESTAMP) AS r"),
    ("ntz_to_timestamp_ltz", lambda d, ts, ntz: f"CAST({ntz} AS TIMESTAMP_LTZ) AS r"),
    ("timestamp_to_ntz", lambda d, ts, ntz: f"CAST({ts} AS TIMESTAMP_NTZ) AS r"),
    ("timestamp_to_date", lambda d, ts, ntz: f"CAST({ts} AS DATE) AS r"),
    ("date_to_timestamp", lambda d, ts, ntz: f"CAST({d} AS TIMESTAMP) AS r"),
    ("date_to_ntz", lambda d, ts, ntz: f"CAST({d} AS TIMESTAMP_NTZ) AS r"),
    ("timestamp_to_string", lambda d, ts, ntz: f"CAST({ts} AS STRING) AS r"),
    ("ntz_to_string", lambda d, ts, ntz: f"CAST({ntz} AS STRING) AS r"),
    ("timestamp_hour", lambda d, ts, ntz: f"hour({ts}) AS r"),
    ("unix_micros", lambda d, ts, ntz: f"unix_micros({ts}) AS r"),
]

# Literal rows for conversions: (name, DATE, TIMESTAMP, TIMESTAMP_NTZ) text.
CONVERSION_LITERALS = [
    ("ordinary", "2024-01-02", "2024-01-02 13:04:05.123456", "2024-01-02 13:04:05.123456"),
    ("pre_epoch", "1969-12-31", "1969-12-31 23:59:59.999999", "1969-12-31 23:59:59.999999"),
    ("la_gap", "2024-03-10", "2024-03-10 02:30:00", "2024-03-10 02:30:00"),
    ("la_overlap", "2024-11-03", "2024-11-03 01:30:00", "2024-11-03 01:30:00"),
    ("lmt", "1800-01-01", "1800-01-01 00:00:00", "1800-01-01 00:00:00"),
    ("utc_instant", "2024-07-04", "2024-07-04 00:05:06.000789+00:00", "2024-07-04 00:05:06.000789"),
]


def sql_string(value):
    """Quote a STRING literal with backslash escapes, which both engines read the same way."""
    if value is None:
        return "CAST(NULL AS STRING)"
    assert "\\" not in value, value
    return "'" + value.replace("'", "\\'") + "'"


def definitions():
    for index, pattern in enumerate(FORMAT_PATTERNS):
        quoted = sql_string(pattern)
        outputs = ", ".join(f"DATE_FORMAT({value}, {quoted}) AS {name}"
                            for name, value in zip(["d", "ts", "ntz"], FORMAT_LITERALS))
        yield {"id": f"format_{index}_literal", "section": "format", "pattern": pattern,
               "inputs": LITERAL_INPUTS, "sql": f"SELECT {outputs}"}
        yield {"id": f"format_{index}_column", "section": "format", "pattern": pattern,
               "inputs": COLUMN_INPUTS,
               "sql": f"SELECT k, DATE_FORMAT(d, {quoted}) AS d, DATE_FORMAT(ts, {quoted}) AS ts, "
                      f"DATE_FORMAT(ntz, {quoted}) AS ntz FROM {VALUES_TABLE}"}
    for name, sql, inputs in FORMAT_PROBES:
        yield {"id": name, "section": "format", "inputs": inputs, "sql": sql}
    for name, operator, sql in FROZEN_PARSES:
        yield {"id": name, "section": "parse", "operator": operator, "sql": sql}
    null_key = len(PARSE_STRINGS) + len(FORMATTED_PARSES)
    for key, (name, value) in enumerate(PARSE_STRINGS):
        for operator, expression in PARSE_OPERATORS:
            yield {"id": f"parse_{name}_{operator}_literal", "section": "parse", "input": name,
                   "operator": operator, "sql": f"SELECT {expression(sql_string(value))} AS r"}
            yield {"id": f"parse_{name}_{operator}_column", "section": "parse", "input": name,
                   "operator": operator,
                   "sql": f"SELECT k, {expression('s')} AS r FROM {PARSE_TABLE} "
                          f"WHERE k IN ({key}, {null_key})"}
        if value and "'" not in value:
            for literal in TYPED_LITERALS:
                yield {"id": f"parse_{name}_{literal.lower()}_typed_literal", "section": "parse",
                       "input": name, "operator": f"{literal.lower()}_literal",
                       "sql": f"SELECT {literal} '{value}' AS r"}
    for offset, (name, value, pattern) in enumerate(FORMATTED_PARSES):
        key = len(PARSE_STRINGS) + offset
        for operator, function in FORMATTED_OPERATORS:
            yield {"id": f"formatted_{name}_{operator}_literal", "section": "formatted",
                   "input": name, "operator": operator,
                   "sql": f"SELECT {function}({sql_string(value)}, {sql_string(pattern)}) AS r"}
            yield {"id": f"formatted_{name}_{operator}_column", "section": "formatted",
                   "input": name, "operator": operator,
                   "sql": f"SELECT k, {function}(s, f) AS r FROM {PARSE_TABLE} "
                          f"WHERE k IN ({key}, {null_key})"}
    for name, expression in CONVERSIONS:
        yield {"id": f"convert_{name}_column", "section": "convert", "operator": name,
               "sql": f"SELECT k, {expression('d', 'ts', 'ntz')} FROM {VALUES_TABLE}"}
        for literal, d, ts, ntz in CONVERSION_LITERALS:
            yield {"id": f"convert_{name}_{literal}_literal", "section": "convert",
                   "operator": name, "input": literal,
                   "sql": "SELECT " + expression(f"DATE '{d}'", f"TIMESTAMP '{ts}'",
                                                 f"TIMESTAMP_NTZ '{ntz}'")}


def micros(text, aware):
    """Encode a recorded timestamp text as epoch microseconds without float arithmetic."""
    value = datetime.fromisoformat(text)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc if aware else None)
    if aware:
        value = value.replace(tzinfo=timezone.utc)
    delta = value - epoch
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def input_tables():
    values_rows = []
    for key, (d, ts, ntz, pattern) in enumerate(TEMPORAL_ROWS):
        values_rows.append([
            str(key),
            None if d is None else str((date.fromisoformat(d) - date(1970, 1, 1)).days),
            None if ts is None else str(micros(ts, True)),
            None if ntz is None else str(micros(ntz, False)),
            pattern])
    parse_rows = [[str(key), value, None] for key, (_, value) in enumerate(PARSE_STRINGS)]
    parse_rows += [[str(len(PARSE_STRINGS) + offset), value, pattern]
                   for offset, (_, value, pattern) in enumerate(FORMATTED_PARSES)]
    parse_rows.append([str(len(parse_rows)), None, None])

    def field(name, kind, nullable=True):
        return {"name": name, "type": kind, "nullable": nullable, "metadata": {}}

    return [
        {"name": VALUES_TABLE,
         "schema": {"type": "struct", "fields": [
             field("k", "integer", False), field("d", "date"), field("ts", "timestamp"),
             field("ntz", "timestamp_ntz"), field("f", "string")]},
         "rows": values_rows},
        {"name": PARSE_TABLE,
         "schema": {"type": "struct", "fields": [
             field("k", "integer", False), field("s", "string"), field("f", "string")]},
         "rows": parse_rows},
    ]


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
    """Return the innermost error condition, or the Java exception class without one."""
    condition = getattr(error, "getCondition", lambda: None)()
    cause = getattr(error, "java_exception", None)
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-temporal-contracts")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        inputs = input_tables()
        for table in inputs:
            schema = types.StructType.fromJson(table["schema"])
            kinds = [field["type"] for field in table["schema"]["fields"]]
            rows = [[python_value(value, kind) for value, kind in zip(row, kinds)]
                    for row in table["rows"]]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table["name"])
            # The registered values must be exactly the recorded encodings.
            frame = spark.table(table["name"])
            stored = [[encode_value(row, index, field.dataType, types)
                       for index, field in enumerate(frame.schema.fields)]
                      for row in frame._jdf.queryExecution().executedPlan().executeCollect()]
            assert stored == table["rows"], (table["name"], stored)
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
        assert len({case["id"] for case in cases}) == len(cases)
        for zone in ZONES:
            spark.conf.set("spark.sql.session.timeZone", zone)
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for case in cases:
                    records.append({**case, "timezone": zone, "ansi": ansi,
                                    **capture(spark, case["sql"], types, Py4JError)})
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
