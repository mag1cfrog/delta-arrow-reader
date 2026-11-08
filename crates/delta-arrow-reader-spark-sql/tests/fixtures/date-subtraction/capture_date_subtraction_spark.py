"""Capture Spark subtraction of two DATE values (#987).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for `-` and TRY_SUBTRACT of two DATE
values and for consumers of the difference: interval scaling, negation, comparison, interval
addition, DATE addition, casts, CASE, IS NULL and a lambda. Spark gives the DAY interval between
the dates as microseconds, with exact arithmetic. Inputs are registered tables of nullable and
NOT NULL DATE values, including NULLs, signs, years 1 and 9999, dates around the epoch and Los
Angeles DST changes, and INT day numbers whose DATE values reach the limits of the DATE range
and of BIGINT microseconds. Every query runs in both ANSI modes in UTC and America/Los_Angeles.
Run from the crate root in the reference environment described in README.md; the output path
must not exist.
"""
import argparse
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles"]

# (end, start)
ROWS = [
    ("2020-01-02", "2020-01-01"),
    ("2020-01-01", "2020-01-02"),
    ("9999-12-31", "0001-01-01"),
    ("0001-01-01", "9999-12-31"),
    ("1970-01-01", "1969-12-31"),
    ("1969-12-31", "1960-01-01"),
    ("2024-03-11", "2024-03-10"),  # Los Angeles skips an hour on 2024-03-10.
    ("2024-11-04", "2024-11-03"),  # Los Angeles repeats an hour on 2024-11-03.
    ("2024-02-29", "2024-02-29"),
    (None, "2024-01-01"),
    ("2024-01-01", None),
]

# NOT NULL (end, start).
REQUIRED = [
    ("2020-01-02", "2020-01-01"),
    ("2020-01-01", "2020-01-02"),
    ("9999-12-31", "0001-01-01"),
]

# BIGINT microseconds hold 106751991 days and 4 hours, so one more whole day overflows.
DAYS_LIMIT = 106751991

# NOT NULL day numbers for `date_from_unix_date`: the DAY limits of BIGINT microseconds and one
# day past them, and the INT limits of DATE.
LIMITS = [DAYS_LIMIT, DAYS_LIMIT + 1, -DAYS_LIMIT, -DAYS_LIMIT - 1, 2147483647, -2147483648]


def days(text):
    return (date.fromisoformat(text) - date(1970, 1, 1)).days


INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "a", "type": "date", "nullable": True, "metadata": {}},
        {"name": "b", "type": "date", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), None if end is None else str(days(end)),
              None if start is None else str(days(start))]
             for key, (end, start) in enumerate(ROWS)]}, {
    "name": "required",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "a", "type": "date", "nullable": False, "metadata": {}},
        {"name": "b", "type": "date", "nullable": False, "metadata": {}}]},
    "rows": [[str(key), str(days(end)), str(days(start))]
             for key, (end, start) in enumerate(REQUIRED)]}, {
    "name": "limits",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "u", "type": "integer", "nullable": False, "metadata": {}}]},
    "rows": [[str(key), str(value)] for key, value in enumerate(LIMITS)]}]

COLUMNS = [
    ("minus", "a - b"),
    ("try_subtract", "try_subtract(a, b)"),
    ("minus_date_literal", "a - DATE '2020-01-01'"),
    ("date_literal_minus", "DATE '2020-01-01' - b"),
    ("divide", "(a - b) / 7"),
    ("divide_by_zero", "(a - b) / 0"),
    ("multiply", "(a - b) * 2"),
    ("multiply_double", "(a - b) * 1.5"),
    ("negate", "-(a - b)"),
    ("equals_day", "(a - b) = INTERVAL '1' DAY"),
    ("less_than_zero", "(a - b) < INTERVAL '0' DAY"),
    ("plus_hour", "(a - b) + INTERVAL '1' HOUR"),
    ("cast_string", "CAST(a - b AS STRING)"),
    ("cast_bigint", "CAST(a - b AS BIGINT)"),
    ("case", "CASE WHEN k > 0 THEN a - b END"),
    ("is_null", "(a - b) IS NULL"),
    ("lambda", "transform(array(a), x -> x - b)"),
]

LITERALS = [
    ("minus", "DATE '2020-01-02' - DATE '2020-01-01'"),
    ("minus_negative", "DATE '2020-01-01' - DATE '2020-01-02'"),
    ("minus_years", "DATE '9999-12-31' - DATE '0001-01-01'"),
    ("try_subtract", "try_subtract(DATE '2024-03-11', DATE '2024-03-10')"),
    ("divide", "(DATE '2020-01-02' - DATE '2020-01-01') / 7"),
    ("null_start", "DATE '2020-01-02' - CAST(NULL AS DATE)"),
    ("null_end", "CAST(NULL AS DATE) - DATE '2020-01-02'"),
    # Spark subtracts an untyped NULL from a DATE as a number of days, and an untyped NULL
    # minus a DATE as two DATE values.
    ("untyped_null_start", "DATE '2020-01-02' - NULL"),
    ("untyped_null_end", "NULL - DATE '2020-01-02'"),
    ("limit_days", f"date_from_unix_date({DAYS_LIMIT}) - DATE '1970-01-01'"),
    ("past_limit_days", f"date_from_unix_date({DAYS_LIMIT + 1}) - DATE '1970-01-01'"),
    ("negative_limit_days", f"date_from_unix_date({-DAYS_LIMIT}) - DATE '1970-01-01'"),
    ("past_negative_limit_days",
     f"date_from_unix_date({-DAYS_LIMIT - 1}) - DATE '1970-01-01'"),
    # Both dates' midnights are beyond BIGINT microseconds, but their difference is not.
    ("far_dates", "date_from_unix_date(2147483647) - date_from_unix_date(2147483646)"),
    ("widest_dates", "date_from_unix_date(2147483647) - date_from_unix_date(-2147483648)"),
    ("try_widest_dates",
     "try_subtract(date_from_unix_date(2147483647), date_from_unix_date(-2147483648))"),
]


def definitions():
    for name, expression in COLUMNS:
        yield f"column_{name}", f"SELECT k, {expression} AS r FROM inputs"
    for name, expression in LITERALS:
        yield f"literal_{name}", f"SELECT {expression} AS r"
    for name, expression in [("minus", "a - b"), ("try_subtract", "try_subtract(a, b)"),
                             ("minus_date_literal", "a - DATE '2020-01-01'"),
                             ("divide", "(a - b) / 7")]:
        yield f"required_{name}", f"SELECT k, {expression} AS r FROM required"
    # The DAR name of an unaliased difference.
    yield "column_unaliased", "SELECT k, a - b FROM inputs"
    # A DATE plus the difference, without the two rows between years 1 and 9999, and one of
    # them alone.
    yield "column_plus_date", "SELECT k, b + (a - b) AS r FROM inputs WHERE k NOT IN (2, 3)"
    yield "column_plus_date_far", "SELECT k, b + (a - b) AS r FROM inputs WHERE k = 2"
    # Day numbers at and past the BIGINT microsecond limits, one row each, and all rows.
    for key, name in enumerate(["limit", "past_limit", "negative_limit", "past_negative_limit",
                                "int_max", "int_min"]):
        yield (f"limits_{name}",
               f"SELECT k, date_from_unix_date(u) - DATE '1970-01-01' AS r "
               f"FROM limits WHERE k = {key}")
    yield ("limits_try_subtract",
           "SELECT k, try_subtract(date_from_unix_date(u), DATE '1970-01-01') AS r FROM limits")
    yield ("limits_far_neighbors",
           "SELECT k, date_from_unix_date(u) - date_from_unix_date(u - 1) AS r "
           "FROM limits WHERE k = 4")


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "date":
        return date(1970, 1, 1) + timedelta(days=int(value))
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row or array value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.DayTimeIntervalType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.StringType):
        return row.getUTF8String(index).toString()
    if isinstance(data_type, types.BooleanType):
        return row.getBoolean(index)
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
        # The Dataset API runs the query with the session's settings.
        frame._jdf.collectAsList()
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-date-subtraction")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for recorded in INPUTS:
            kinds = [field["type"] for field in recorded["schema"]["fields"]]
            schema = types.StructType.fromJson(recorded["schema"])
            spark.createDataFrame([[python_value(value, kind) for value, kind in zip(row, kinds)]
                                   for row in recorded["rows"]], schema) \
                .createOrReplaceTempView(recorded["name"])
            # The registered fields and values must be exactly the recorded ones.
            frame = spark.table(recorded["name"])
            assert json.loads(frame.schema.json()) == recorded["schema"], frame.schema
            stored = [[encode_value(row, index, field.dataType, types)
                       for index, field in enumerate(frame.schema.fields)]
                      for row in frame._jdf.queryExecution().executedPlan().executeCollect()]
            assert stored == recorded["rows"], stored
        header = {
            "spark_version": spark.version, "spark_build": build,
            "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
            "python_version": sys.version.split()[0],
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "value_encoding": ("INT and DATE days since the epoch as int text; BIGINT and "
                               "day-time interval microseconds as long text; STRING as text; "
                               "BOOLEAN as a JSON boolean; ARRAY as a list of its elements"),
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
