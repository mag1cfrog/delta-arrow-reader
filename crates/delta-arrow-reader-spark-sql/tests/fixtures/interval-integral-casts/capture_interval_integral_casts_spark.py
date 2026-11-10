"""Capture Spark casts of year-month and day-time intervals to integral and DECIMAL types (#1152).

Records analyzed, optimized and executed schemas, typed internal-row values and failing stages,
with the innermost Java exception of a failure, for CAST and TRY_CAST of intervals of every
qualifier to TINYINT, SMALLINT, INT, BIGINT and DECIMAL: values built from numbers, literals,
fractional seconds, limits and overflow, NULL, DATE and TIMESTAMP differences, MAKE_DT_INTERVAL
and MAKE_YM_INTERVAL, a DAY TO SECOND column, interval arithmetic, negation, conditionals and
aggregates, and columns of subqueries, common table expressions, VALUES and UNION, with FLOAT,
DOUBLE and BOOLEAN targets, which Spark rejects, and unaliased generated names; and DATE
arithmetic whose interval qualifier is tracked through those columns or widened beside a DAY TO
SECOND column. Every query runs in both ANSI modes in UTC and America/Los_Angeles. Run from the crate root in the reference
environment described in README.md; the output path must not exist.
"""
import argparse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles"]

YEAR_MONTH = ["YEAR", "YEAR TO MONTH", "MONTH"]
DAY_TIME = ["DAY", "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND", "HOUR", "HOUR TO MINUTE",
            "HOUR TO SECOND", "MINUTE", "MINUTE TO SECOND", "SECOND"]
QUALIFIERS = YEAR_MONTH + DAY_TIME
TARGETS = ["TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL", "DECIMAL(20, 6)", "DECIMAL(4, 1)"]
TRY_TARGETS = ["TINYINT", "SMALLINT", "INT", "DECIMAL(4, 1)"]


def text(value):
    return None if value is None else str(value)


def micros(text_value):
    value = datetime.fromisoformat(text_value).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def days(text_value):
    return (date.fromisoformat(text_value) - date(1970, 1, 1)).days


# INT counts `n`, read as each qualifier's end field: zero, small values of both signs, the
# TINYINT and SMALLINT limits and their neighbors, the largest day count of a DAY TO SECOND
# interval, and NULL. BIGINT `b` holds second counts beyond INT.
COUNTS = [0, 3, -3, 127, 128, -128, -129, 32767, 32768, 106751991, -106751991, None]
SECONDS = [9_223_372_036, -9_223_372_036, 2_147_483_647, 2_147_483_648, None]
# DECIMAL(10, 3) second counts with fractions, halves for rounding to DECIMAL(4, 1), and NULL.
FRACTIONS = ["3.700", "-3.700", "0.001", "1.250", "-1.250", "1.249", "999.950", None]
# (d1, d2, t1, t2): DATE and TIMESTAMP pairs, with the Los Angeles DST start between the third
# pair, years 1 and 9999, and NULL.
PAIRS = [
    ("2024-01-01", "2024-01-04", "2024-01-01 00:00:00", "2024-01-04 06:30:00"),
    ("2024-01-04", "2024-01-01", "2024-01-04 06:30:00", "2024-01-01 00:00:00"),
    ("2024-03-09", "2024-03-11", "2024-03-09 12:00:00", "2024-03-11 12:00:00"),
    ("0001-01-01", "9999-12-31", "0001-01-01 00:00:00", "9999-12-31 23:59:59"),
    (None, "2024-01-01", None, "2024-01-01 00:00:00"),
]
# DAY TO SECOND microseconds: 1 day 02:03:04.5, its negation, a fraction of a second, NULL.
SPANS = [93_784_500_000, -93_784_500_000, 1_500_000, None]

INPUTS = [{
    "name": "counts",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "n", "type": "integer", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), text(value)] for key, value in enumerate(COUNTS)]}, {
    "name": "seconds",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "b", "type": "long", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), text(value)] for key, value in enumerate(SECONDS)]}, {
    "name": "fractions",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "f", "type": "decimal(10,3)", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), value] for key, value in enumerate(FRACTIONS)]}, {
    "name": "pairs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "d1", "type": "date", "nullable": True, "metadata": {}},
        {"name": "d2", "type": "date", "nullable": True, "metadata": {}},
        {"name": "t1", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "t2", "type": "timestamp", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), text(d1 and days(d1)), text(d2 and days(d2)),
              text(t1 and micros(t1)), text(t2 and micros(t2))]
             for key, (d1, d2, t1, t2) in enumerate(PAIRS)]}, {
    "name": "spans",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "s", "type": "interval day to second", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), text(value)] for key, value in enumerate(SPANS)]}]


def slug(value):
    return (value.lower().replace("decimal(20, 6)", "decimal_wide")
            .replace("decimal(4, 1)", "decimal_narrow").replace(" to ", "_to_").replace(" ", "_"))


LITERALS = [
    ("day", "INTERVAL '3' DAY"), ("negative_day", "INTERVAL '-3' DAY"),
    ("day_to_hour", "INTERVAL '1 2' DAY TO HOUR"),
    ("negative_day_to_hour", "INTERVAL '-1 2' DAY TO HOUR"),
    ("day_to_minute", "INTERVAL '1 2:3' DAY TO MINUTE"),
    ("day_to_second", "INTERVAL '1 2:3:4.5' DAY TO SECOND"),
    ("hour", "INTERVAL '2' HOUR"), ("hour_to_minute", "INTERVAL '2:3' HOUR TO MINUTE"),
    ("hour_to_second", "INTERVAL '2:3:4.5' HOUR TO SECOND"),
    ("minute", "INTERVAL '3' MINUTE"), ("minute_to_second", "INTERVAL '3:4.5' MINUTE TO SECOND"),
    ("second", "INTERVAL '4.5' SECOND"), ("negative_second", "INTERVAL '-4.5' SECOND"),
    ("year", "INTERVAL '3' YEAR"), ("negative_year", "INTERVAL '-3' YEAR"),
    ("year_to_month", "INTERVAL '1-2' YEAR TO MONTH"), ("month", "INTERVAL '14' MONTH"),
    ("unit_days", "INTERVAL 3 DAYS"), ("unit_days_hours", "INTERVAL 3 DAYS 4 HOURS"),
    ("unit_hours", "INTERVAL 3 HOURS"), ("unit_minutes", "INTERVAL 90 MINUTES"),
    ("unit_day_seconds", "INTERVAL 1 DAY 30 SECONDS"), ("unit_years", "INTERVAL 3 YEARS"),
    ("unit_years_months", "INTERVAL 3 YEARS 2 MONTHS"), ("unit_months", "INTERVAL 3 MONTHS"),
    ("largest_days", "INTERVAL '106751991' DAY"), ("smallest_days", "INTERVAL '-106751991' DAY"),
    ("largest_day_to_second", "INTERVAL '106751991 04:00:54.775807' DAY TO SECOND"),
    ("smallest_day_to_second", "INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND"),
    ("largest_years", "INTERVAL '178956970' YEAR"),
    ("largest_year_to_month", "INTERVAL '178956970-7' YEAR TO MONTH"),
    ("smallest_year_to_month", "INTERVAL '-178956970-8' YEAR TO MONTH"),
    ("cast_string_day", "CAST('3' AS INTERVAL DAY)"),
    ("day_as_hour", "CAST(INTERVAL '3' DAY AS INTERVAL HOUR)"),
    ("day_to_hour_as_day", "CAST(INTERVAL '1 2' DAY TO HOUR AS INTERVAL DAY)"),
    ("year_as_month", "CAST(INTERVAL '3' YEAR AS INTERVAL MONTH)"),
    ("null_day", "CAST(NULL AS INTERVAL DAY)"), ("null_year", "CAST(NULL AS INTERVAL YEAR)"),
]

DAY = "(d2 - d1)"
EXPRESSIONS = [
    ("date_difference", DAY), ("timestamp_difference", "(t2 - t1)"),
    ("plus_day", f"{DAY} + INTERVAL '1' DAY"), ("plus_hour", f"{DAY} + INTERVAL '1' HOUR"),
    ("minus_minute", f"{DAY} - INTERVAL '30' MINUTE"),
    ("try_add_day", f"try_add({DAY}, INTERVAL '1' DAY)"),
    ("try_subtract_hour", f"try_subtract({DAY}, INTERVAL '1' HOUR)"),
    ("negation", f"-{DAY}"), ("negative", f"negative({DAY})"), ("abs", f"abs({DAY})"),
    ("times_two", f"{DAY} * 2"), ("divided_by_two", f"{DAY} / 2"),
    ("coalesce_day", f"coalesce({DAY}, INTERVAL '1' DAY)"),
    ("coalesce_hour", f"coalesce({DAY}, INTERVAL '1' HOUR)"),
    ("if_day", f"IF(k > 0, {DAY}, INTERVAL '1' DAY)"),
    ("if_hour", f"IF(k > 0, {DAY}, INTERVAL '1' HOUR)"),
    ("case_day", f"CASE WHEN k > 1 THEN {DAY} WHEN k > 0 THEN INTERVAL '1' DAY END"),
    ("nullif", f"nullif({DAY}, INTERVAL '3' DAY)"),
    ("greatest", f"greatest({DAY}, INTERVAL '1' DAY)"),
    ("least_hour", f"least({DAY}, INTERVAL '1' HOUR)"),
    ("make_dt_interval_days", "make_dt_interval(k)"),
    ("make_dt_interval_hours", "make_dt_interval(0, k)"),
    ("make_ym_interval_years", "make_ym_interval(k)"),
    ("make_ym_interval_months", "make_ym_interval(0, k)"),
    ("year_plus_month", "CAST(k AS INTERVAL YEAR) + INTERVAL '1' MONTH"),
    ("year_plus_year", "CAST(k AS INTERVAL YEAR) + INTERVAL '1' YEAR"),
    ("year_times_two", "CAST(k AS INTERVAL YEAR) * 2"),
    ("negated_year", "-CAST(k AS INTERVAL YEAR)"),
]


def definitions():
    # Each qualifier, built from a count of its end field, to each target.
    for qualifier in QUALIFIERS:
        built = f"CAST(n AS INTERVAL {qualifier})"
        for target in TARGETS:
            yield (f"count_{slug(qualifier)}_{slug(target)}",
                   f"SELECT k, CAST({built} AS {target}) AS r FROM counts")
        for target in TRY_TARGETS:
            yield (f"try_count_{slug(qualifier)}_{slug(target)}",
                   f"SELECT k, TRY_CAST({built} AS {target}) AS r FROM counts")
    # Second counts beyond INT, and fractional seconds.
    for qualifier in ["SECOND", "MINUTE TO SECOND", "DAY TO SECOND"]:
        built = f"CAST(b AS INTERVAL {qualifier})"
        for target in ["INT", "BIGINT", "DECIMAL(20, 6)"]:
            yield (f"seconds_{slug(qualifier)}_{slug(target)}",
                   f"SELECT k, CAST({built} AS {target}) AS r FROM seconds")
        yield (f"try_seconds_{slug(qualifier)}_int",
               f"SELECT k, TRY_CAST({built} AS INT) AS r FROM seconds")
    for qualifier in ["SECOND", "MINUTE TO SECOND", "HOUR TO SECOND", "DAY TO SECOND"]:
        built = f"CAST(f AS INTERVAL {qualifier})"
        for target in TARGETS:
            yield (f"fraction_{slug(qualifier)}_{slug(target)}",
                   f"SELECT k, CAST({built} AS {target}) AS r FROM fractions")
    yield ("try_fraction_second_decimal_narrow",
           "SELECT k, TRY_CAST(CAST(f AS INTERVAL SECOND) AS DECIMAL(4, 1)) AS r FROM fractions")
    yield ("fraction_second_decimal_scale_two",
           "SELECT k, CAST(CAST(f AS INTERVAL SECOND) AS DECIMAL(10, 2)) AS r FROM fractions")
    # Literals, folded to constants.
    for name, literal in LITERALS:
        for target in ["INT", "BIGINT", "DECIMAL(20, 6)"]:
            yield f"literal_{name}_{slug(target)}", f"SELECT CAST({literal} AS {target}) AS r"
    for name, literal in [("day", "INTERVAL '200' DAY"), ("year", "INTERVAL '200' YEAR"),
                          ("second", "INTERVAL '9223372036' SECOND")]:
        for target in ["TINYINT", "INT"]:
            yield (f"literal_overflow_{name}_{slug(target)}",
                   f"SELECT CAST({literal} AS {target}) AS r")
            yield (f"literal_try_overflow_{name}_{slug(target)}",
                   f"SELECT TRY_CAST({literal} AS {target}) AS r")
    # DATE and TIMESTAMP differences, functions and arithmetic over registered rows.
    for name, expression in EXPRESSIONS:
        source = "counts" if name.startswith(("make_", "year_", "negated_year")) else "pairs"
        for target in ["INT", "BIGINT", "DECIMAL(20, 6)"]:
            yield (f"expression_{name}_{slug(target)}",
                   f"SELECT k, CAST({expression} AS {target}) AS r FROM {source}")
    for target in ["TINYINT", "SMALLINT", "DECIMAL", "DECIMAL(4, 1)"]:
        yield (f"expression_date_difference_{slug(target)}",
               f"SELECT k, CAST({DAY} AS {target}) AS r FROM pairs")
    yield ("expression_try_date_difference_smallint",
           f"SELECT k, TRY_CAST({DAY} AS SMALLINT) AS r FROM pairs")
    # A DAY TO SECOND column.
    for target in ["INT", "BIGINT", "DECIMAL(20, 6)"]:
        yield f"column_span_{slug(target)}", f"SELECT k, CAST(s AS {target}) AS r FROM spans"
    # Columns of subqueries, common table expressions, VALUES and UNION, and aggregates.
    columns = [
        ("subquery_date_difference",
         f"SELECT k, CAST(x AS INT) AS r FROM (SELECT k, {DAY} AS x FROM pairs)"),
        ("cte_date_difference",
         f"WITH t AS (SELECT k, {DAY} AS x FROM pairs) SELECT k, CAST(x AS INT) AS r FROM t"),
        ("subquery_literal_day", "SELECT CAST(x AS INT) AS r FROM (SELECT INTERVAL '3' DAY AS x)"),
        ("subquery_hour_plus_day",
         "SELECT k, CAST(x + INTERVAL '1' DAY AS INT) AS r "
         "FROM (SELECT k, CAST(n AS INTERVAL HOUR) AS x FROM counts)"),
        ("nested_subquery_date_difference",
         f"SELECT k, CAST(y AS BIGINT) AS r FROM (SELECT k, x AS y FROM "
         f"(SELECT k, {DAY} AS x FROM pairs) AS a) AS b"),
        ("subquery_sum", f"SELECT CAST(x AS INT) AS r FROM (SELECT sum({DAY}) AS x FROM pairs)"),
        ("subquery_min", f"SELECT CAST(x AS INT) AS r FROM (SELECT min({DAY}) AS x FROM pairs)"),
        ("subquery_max", f"SELECT CAST(x AS INT) AS r FROM (SELECT max({DAY}) AS x FROM pairs)"),
        ("subquery_avg", f"SELECT CAST(x AS BIGINT) AS r FROM (SELECT avg({DAY}) AS x FROM pairs)"),
        ("grouped_max",
         f"SELECT k, CAST(x AS INT) AS r FROM (SELECT k, max({DAY}) AS x FROM pairs GROUP BY k)"),
        ("sum", f"SELECT CAST(sum({DAY}) AS INT) AS r FROM pairs"),
        ("avg", f"SELECT CAST(avg({DAY}) AS BIGINT) AS r FROM pairs"),
        ("window_lag", f"SELECT k, CAST(lag({DAY}) OVER (ORDER BY k) AS INT) AS r FROM pairs"),
        ("values_day",
         "SELECT CAST(v AS INT) AS r FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY) AS t(v)"),
        ("values_day_hour",
         "SELECT CAST(v AS INT) AS r FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' HOUR) AS t(v)"),
        ("union_day",
         "SELECT CAST(x AS INT) AS r FROM "
         "(SELECT INTERVAL '1' DAY AS x UNION ALL SELECT INTERVAL '2' DAY)"),
        ("union_day_hour",
         "SELECT CAST(x AS INT) AS r FROM "
         "(SELECT INTERVAL '1' DAY AS x UNION ALL SELECT INTERVAL '2' HOUR)"),
        ("union_date_difference",
         f"SELECT CAST(x AS INT) AS r FROM (SELECT {DAY} AS x FROM pairs "
         "UNION ALL SELECT INTERVAL '2' DAY)"),
        ("array_element", f"SELECT k, CAST(array({DAY})[0] AS INT) AS r FROM pairs"),
        ("explode",
         "SELECT CAST(x AS INT) AS r FROM "
         "(SELECT explode(array(INTERVAL '1' DAY, INTERVAL '2' DAY)) AS x)"),
    ]
    for name, sql in columns:
        yield f"column_{name}", sql
    # Qualifiers through stars, unaliased casts, untyped NULLs, scalar subqueries, MAX_BY and a
    # common table expression read by two set operations, and DATE arithmetic that reads them.
    tracking = [
        ("star_cte",
         f"WITH a AS (SELECT k, {DAY} AS x FROM pairs), b AS (SELECT * FROM a) "
         "SELECT k, CAST(x AS INT) AS r FROM b"),
        ("qualified_star",
         f"SELECT k, CAST(x AS INT) AS r FROM (SELECT a.* FROM (SELECT k, {DAY} AS x FROM pairs) "
         "AS a)"),
        ("unaliased_cast_column",
         "SELECT k, CAST(c AS INT) AS r FROM "
         "(SELECT k, CAST(n AS INTERVAL HOUR) FROM counts) AS s(k, c)"),
        ("union_null_day",
         "SELECT CAST(x AS INT) AS r FROM (SELECT NULL AS x UNION ALL SELECT INTERVAL '2' DAY)"),
        ("scalar_subquery_max", f"SELECT CAST((SELECT max({DAY}) FROM pairs) AS INT) AS r"),
        ("max_by", f"SELECT CAST(max_by({DAY}, -k) AS INT) AS r FROM pairs"),
        ("shared_cte_set_operations",
         "WITH c AS (SELECT INTERVAL '3' DAY AS i) SELECT CAST(i AS INT) AS r FROM "
         "(TABLE c UNION ALL SELECT INTERVAL '1' HOUR) UNION ALL SELECT CAST(i AS INT) FROM c"),
        ("date_plus_span_plus_hour",
         "SELECT k, DATE '2024-01-01' + (s + INTERVAL '1' HOUR) AS r FROM spans"),
        ("date_plus_coalesce_span_hour",
         "SELECT k, DATE '2024-01-01' + coalesce(s, INTERVAL '1' HOUR) AS r FROM spans"),
        ("date_plus_subquery_hour",
         "SELECT k, d1 + x AS r FROM (SELECT k, d1, CAST(k AS INTERVAL HOUR) AS x FROM pairs)"),
        ("date_plus_subquery_day",
         "SELECT k, d1 + x AS r FROM (SELECT k, d1, CAST(k AS INTERVAL DAY) AS x FROM pairs)"),
        ("date_plus_star_difference",
         f"SELECT k, d1 + x AS r FROM (SELECT * FROM (SELECT k, d1, {DAY} AS x FROM pairs))"),
        ("date_plus_star_difference_plus_hour",
         f"SELECT k, d1 + (x + INTERVAL '1' HOUR) AS r FROM "
         f"(SELECT *, {DAY} AS x FROM pairs)"),
        ("date_plus_span", "SELECT k, DATE '2024-01-01' + s AS r FROM spans"),
        ("cast_array_span_plus_hour",
         "SELECT k, CAST(array(s)[0] + INTERVAL '1' HOUR AS INT) AS r FROM spans"),
        ("date_plus_array_span_plus_hour",
         "SELECT k, DATE '2024-01-01' + (array(s)[0] + INTERVAL '1' HOUR) AS r FROM spans"),
        ("date_plus_array_difference", f"SELECT k, d1 + array({DAY})[0] AS r FROM pairs"),
        ("union_chain_timestamp_difference",
         f"SELECT k, CAST(x AS BIGINT) AS r FROM (SELECT k, {DAY} AS x FROM pairs "
         f"UNION ALL SELECT k + 10, {DAY} FROM pairs UNION ALL SELECT k + 20, t2 - t1 FROM pairs)"),
        ("union_chain_hour",
         "SELECT CAST(x AS INT) AS r FROM (SELECT INTERVAL '1' DAY AS x "
         "UNION ALL SELECT INTERVAL '2' DAY UNION ALL SELECT INTERVAL '3' HOUR)"),
        ("union_except_chain_hour",
         "SELECT CAST(x AS INT) AS r FROM (SELECT INTERVAL '1' DAY AS x "
         "UNION ALL SELECT INTERVAL '2' DAY EXCEPT SELECT INTERVAL '3' HOUR)"),
        ("date_plus_union_chain_hour",
         "SELECT k, d1 + x AS r FROM (SELECT k, d1, CAST(k AS INTERVAL DAY) AS x FROM pairs "
         "UNION ALL SELECT k, d1, CAST(k AS INTERVAL DAY) FROM pairs "
         "UNION ALL SELECT k, d1, CAST(k AS INTERVAL HOUR) FROM pairs)"),
    ]
    for name, sql in tracking:
        yield f"tracking_{name}", sql
    # Spark rejects FLOAT, DOUBLE and BOOLEAN targets.
    for name, source in [("day", "CAST(n AS INTERVAL DAY)"), ("year", "CAST(n AS INTERVAL YEAR)")]:
        for target in ["DOUBLE", "FLOAT", "BOOLEAN"]:
            yield (f"rejected_{name}_{slug(target)}",
                   f"SELECT k, CAST({source} AS {target}) AS r FROM counts")
    yield ("rejected_try_day_double",
           "SELECT k, TRY_CAST(CAST(n AS INTERVAL DAY) AS DOUBLE) AS r FROM counts")
    # Unaliased casts name the interval.
    yield "name_literal_day", "SELECT CAST(INTERVAL '3' DAY AS INT)"
    yield "name_date_difference", f"SELECT CAST({DAY} AS INT) FROM pairs"


def python_value(value, kind):
    if value is None:
        return None
    if kind in ("integer", "long"):
        return int(value)
    if kind == "date":
        return date(1970, 1, 1) + timedelta(days=int(value))
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    if kind.startswith("decimal"):
        return Decimal(value)
    if kind == "interval day to second":
        return timedelta(microseconds=int(value))
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, types.ByteType):
        return str(row.getByte(index))
    if isinstance(data_type, types.ShortType):
        return str(row.getShort(index))
    if isinstance(data_type, (types.IntegerType, types.DateType, types.YearMonthIntervalType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.TimestampType, types.DayTimeIntervalType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.DecimalType):
        return row.getDecimal(index, data_type.precision, data_type.scale).toString()
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
        rows_out = [[encode_value(row, index, field.dataType, types)
                     for index, field in enumerate(fields)] for row in internal_rows]
        rows_out.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        record["expected"] = {"status": "ok", "rows": rows_out}
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-interval-integral-casts")
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
            "value_encoding": ("TINYINT, SMALLINT, INT, BIGINT, DATE days, TIMESTAMP and DAY "
                               "TO SECOND microseconds and DECIMAL as text"),
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
