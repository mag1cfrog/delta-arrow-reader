"""Capture Spark MAKE_YM_INTERVAL values, errors and nullability (#1159, #1165).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for MAKE_YM_INTERVAL with literal,
nullable-column, NOT NULL-column, empty-input and NULL arguments, none, one and two arguments,
signs, the YEAR TO MONTH limits and one month past them, INT, TINYINT, SMALLINT, BIGINT,
DECIMAL, DOUBLE and FLOAT arguments with fractions, signs, values beyond INT, NaN and
infinities, BOOLEAN, DATE, TIMESTAMP, BINARY, ARRAY and interval arguments, STRING, malformed
and empty STRING arguments, a NULL beside an overflowing argument or a failing argument cast in
either order, STRING arguments beside a NULL literal, a cast NULL, a computed NULL, NULLIF or
IF, failing STRING arguments in row order, INT() of a STRING, unaliased generated names, and
for comparisons, negation, IS NULL and TRY arithmetic over its result. Every query runs in both
ANSI modes in UTC and America/Los_Angeles. Run from the crate root in the reference environment
described in README.md; the output path must not exist.
"""
import argparse
from decimal import Decimal
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


def text(value):
    return None if value is None else str(value)


# (years y INT, months mo INT, b BIGINT, t STRING) per row. Row 4 overflows the YEAR TO MONTH
# range, row 5 is its lower limit, b is beyond INT in rows 3 and 5, and t is malformed in row
# 3, empty in row 4 and beyond INT in row 6.
NULLABLE_ROWS = [
    (1, 2, 5, "3"),
    (-1, -2, -5, "-3"),
    (None, 2, None, None),
    (2, None, 4294967297, "x"),
    (178956970, 8, 0, ""),
    (-178956970, -8, -4294967297, " 7 "),
    (0, 0, 2147483647, "2147483648"),
]

# The same columns, all NOT NULL. Row 2 is the upper limit; row 3 overflows the YEAR TO MONTH
# range, b is beyond INT and t is malformed; row 4 holds the lower limits.
REQUIRED_ROWS = [
    (1, 2, 5, "3"),
    (-1, -2, -5, "-3"),
    (178956970, 7, 2147483647, " 7 "),
    (178956970, 8, 4294967297, "x"),
    (-178956970, -8, -2147483648, "-2147483648"),
]


def fields(nullable):
    return [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "y", "type": "integer", "nullable": nullable, "metadata": {}},
        {"name": "mo", "type": "integer", "nullable": nullable, "metadata": {}},
        {"name": "b", "type": "long", "nullable": nullable, "metadata": {}},
        {"name": "t", "type": "string", "nullable": nullable, "metadata": {}}]


def rows(values):
    return [[str(key)] + [text(value) for value in row] for key, row in enumerate(values)]


def double_bits(value):
    """A DOUBLE as the hex of its IEEE 754 bits."""
    return struct.pack(">d", value).hex()


def float_bits(value):
    """A FLOAT as the hex of its IEEE 754 bits."""
    return struct.pack(">f", value).hex()


def number_fields(nullable):
    return [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "dc", "type": "decimal(12,2)", "nullable": nullable, "metadata": {}},
        {"name": "f", "type": "double", "nullable": nullable, "metadata": {}},
        {"name": "fl", "type": "float", "nullable": nullable, "metadata": {}}]


def number_rows(values):
    return [[str(key), text(dc), None if f is None else double_bits(f),
             None if fl is None else float_bits(fl)] for key, (dc, f, fl) in enumerate(values)]


INF = float("inf")
NAN = float("nan")

# (DECIMAL(12, 2) dc, DOUBLE f, FLOAT fl) per row. Row 3 is beyond INT, row 4 holds DECIMAL
# years that overflow the interval, a NaN and an infinity, and row 6 a NULL DECIMAL beside a
# DOUBLE beyond INT.
NULLABLE_NUMBERS = [
    ("1.50", 1.5, 1.5),
    ("-1.50", -1.5, -1.5),
    (None, None, None),
    ("2147483648.00", 2147483648.0, 3e9),
    ("178956971.00", NAN, INF),
    ("0.99", -0.99, 0.5),
    (None, 1e10, -INF),
]

REQUIRED_NUMBERS = [
    ("1.25", 1.75, 2.5),
    ("-2.50", -2.5, -0.5),
]


INPUTS = [
    {"name": "nullable", "schema": {"type": "struct", "fields": fields(True)},
     "rows": rows(NULLABLE_ROWS)},
    {"name": "required", "schema": {"type": "struct", "fields": fields(False)},
     "rows": rows(REQUIRED_ROWS)},
    {"name": "empty", "schema": {"type": "struct", "fields": fields(False)}, "rows": []},
    {"name": "numbers", "schema": {"type": "struct", "fields": number_fields(True)},
     "rows": number_rows(NULLABLE_NUMBERS)},
    {"name": "numbers_required", "schema": {"type": "struct", "fields": number_fields(False)},
     "rows": number_rows(REQUIRED_NUMBERS)}]

COLUMNS = [
    ("nullable_both", "make_ym_interval(y, mo)", "nullable", "k <> 4"),
    ("nullable_overflow", "make_ym_interval(y, mo)", "nullable", "k = 4"),
    ("nullable_year", "make_ym_interval(y)", "nullable", "k <> 4"),
    ("nullable_month", "make_ym_interval(0, mo)", "nullable", "true"),
    ("nullable_bigint", "make_ym_interval(0, b)", "nullable", "k NOT IN (3, 5)"),
    ("nullable_bigint_beyond_int", "make_ym_interval(0, b)", "nullable", "k = 3"),
    ("nullable_bigint_below_int", "make_ym_interval(0, b)", "nullable", "k = 5"),
    ("nullable_string", "make_ym_interval(0, t)", "nullable", "k IN (0, 1, 2, 5)"),
    ("nullable_string_malformed", "make_ym_interval(0, t)", "nullable", "k = 3"),
    ("nullable_string_empty", "make_ym_interval(0, t)", "nullable", "k = 4"),
    ("nullable_string_beyond_int", "make_ym_interval(0, t)", "nullable", "k = 6"),
    ("nullable_null_year_overflowing_month", "make_ym_interval(y, 2147483647)", "nullable",
     "k = 2"),
    ("nullable_null_year_malformed_month", "make_ym_interval(mo, t)", "nullable", "k = 3"),
    ("nullable_malformed_year_null_month", "make_ym_interval(t, mo)", "nullable", "k = 3"),
    ("nullable_null_year_bigint_beyond_int", "make_ym_interval(mo, b)", "nullable", "k = 3"),
    ("nullable_bigint_beyond_int_null_month", "make_ym_interval(b, mo)", "nullable", "k = 3"),
    ("required_both", "make_ym_interval(y, mo)", "required", "k <> 3"),
    ("required_year", "make_ym_interval(y)", "required", "k <> 3"),
    ("required_month", "make_ym_interval(0, mo)", "required", "true"),
    ("required_overflow", "make_ym_interval(y, mo)", "required", "k = 3"),
    ("required_literal_month_overflow", "make_ym_interval(y, 2147483637)", "required", "k = 0"),
    ("required_bigint", "make_ym_interval(0, b)", "required", "k NOT IN (2, 3)"),
    ("required_bigint_years_overflow", "make_ym_interval(b)", "required", "k = 2"),
    ("required_bigint_beyond_int", "make_ym_interval(0, b)", "required", "k = 3"),
    ("required_string", "make_ym_interval(0, t)", "required", "k <> 3"),
    ("required_string_malformed", "make_ym_interval(0, t)", "required", "k = 3"),
    ("required_string_is_null", "make_ym_interval(t) IS NULL", "required", "k IN (0, 2, 3)"),
    ("required_string_null_month", "make_ym_interval(t, NULL)", "required", "true"),
    ("required_string_string_null_month", "make_ym_interval(t, CAST(NULL AS STRING))",
     "required", "true"),
    ("required_string_computed_null_month", "make_ym_interval(t, NULL + 1)", "required", "true"),
    ("required_string_nullif_month", "make_ym_interval(t, nullif(1, 1))", "required", "true"),
    ("nullable_null_year_string", "make_ym_interval(NULL, t)", "nullable", "true"),
    ("required_string_failures_in_row_order",
     "make_ym_interval(t, CASE WHEN k = 0 THEN 'bad' ELSE '1' END)", "required", "true"),
    ("required_int_function", "make_ym_interval(int(t))", "required", "k IN (0, 1)"),
    ("required_int_function_is_null", "make_ym_interval(int(t)) IS NULL", "required", "k = 3"),
    ("required_small_types", "make_ym_interval(CAST(k AS TINYINT), CAST(mo AS SMALLINT))",
     "required", "true"),
    ("required_null_month", "make_ym_interval(y, NULL)", "required", "true"),
    ("required_typed_null_year", "make_ym_interval(CAST(NULL AS INT), mo)", "required", "true"),
    ("required_comparison", "make_ym_interval(y) = INTERVAL '1' YEAR", "required", "k <> 3"),
    ("required_negation", "-make_ym_interval(y, mo)", "required", "k IN (0, 1, 2)"),
    ("required_is_null", "make_ym_interval(y, mo) IS NULL", "required", "k <> 3"),
    ("required_try_add_overflow", "try_add(make_ym_interval(y, mo), INTERVAL '1' MONTH)",
     "required", "k = 3"),
    ("empty_both", "make_ym_interval(y, mo)", "empty", "true"),
    ("numbers_decimal", "make_ym_interval(dc)", "numbers", "k IN (0, 1, 2, 5)"),
    ("numbers_decimal_beyond_int", "make_ym_interval(0, dc)", "numbers", "k = 3"),
    ("numbers_decimal_years_overflow", "make_ym_interval(dc)", "numbers", "k = 4"),
    ("numbers_double", "make_ym_interval(0, f)", "numbers", "k IN (0, 1, 2, 5)"),
    ("numbers_double_beyond_int", "make_ym_interval(0, f)", "numbers", "k = 3"),
    ("numbers_double_nan", "make_ym_interval(0, f)", "numbers", "k = 4"),
    ("numbers_float", "make_ym_interval(fl)", "numbers", "k IN (0, 1, 2, 5)"),
    ("numbers_float_beyond_int", "make_ym_interval(0, fl)", "numbers", "k = 3"),
    ("numbers_float_infinity", "make_ym_interval(0, fl)", "numbers", "k = 4"),
    ("numbers_float_negative_infinity", "make_ym_interval(0, fl)", "numbers", "k = 6"),
    ("numbers_null_year_failing_month", "make_ym_interval(dc, f)", "numbers", "k = 6"),
    ("numbers_failing_year_null_month", "make_ym_interval(f, dc)", "numbers", "k = 6"),
    ("numbers_required", "make_ym_interval(dc, f)", "numbers_required", "true"),
    ("numbers_required_float", "make_ym_interval(fl)", "numbers_required", "true"),
    ("numbers_required_comparison", "make_ym_interval(dc) = INTERVAL '1' YEAR",
     "numbers_required", "true"),
]

LITERALS = [
    ("no_arguments", "make_ym_interval()"),
    ("year", "make_ym_interval(1)"),
    ("year_month", "make_ym_interval(1, 2)"),
    ("negative", "make_ym_interval(-1, -2)"),
    ("upper_limit", "make_ym_interval(178956970, 7)"),
    ("lower_limit", "make_ym_interval(-178956970, -8)"),
    ("upper_overflow", "make_ym_interval(178956970, 8)"),
    ("lower_overflow", "make_ym_interval(-178956970, -9)"),
    ("year_overflow", "make_ym_interval(178956971)"),
    ("null_year", "make_ym_interval(NULL, 1)"),
    ("null_month", "make_ym_interval(1, NULL)"),
    ("null_only", "make_ym_interval(NULL)"),
    ("overflowing_year_null_month", "make_ym_interval(178956971, NULL)"),
    ("strings", "make_ym_interval('1', '2')"),
    ("string_malformed", "make_ym_interval('x')"),
    ("string_empty", "make_ym_interval('')"),
    ("string_malformed_null_month", "make_ym_interval('x', NULL)"),
    ("string_malformed_string_null_month", "make_ym_interval('x', CAST(NULL AS STRING))"),
    ("string_malformed_cast_null_month",
     "make_ym_interval('x', CAST(CAST(NULL AS STRING) AS INT))"),
    ("string_malformed_computed_null_month", "make_ym_interval('x', NULL + 1)"),
    ("string_malformed_nullif_month", "make_ym_interval('x', nullif(1, 1))"),
    ("string_malformed_if_null_month", "make_ym_interval('x', IF(false, 1, NULL))"),
    ("null_year_string_malformed", "make_ym_interval(NULL, 'x')"),
    ("bigints", "make_ym_interval(1L, 2L)"),
    ("bigint_beyond_int", "make_ym_interval(4294967297L)"),
    ("tinyint_smallint", "make_ym_interval(1Y, 2S)"),
    ("decimal", "make_ym_interval(1.5)"),
    ("double", "make_ym_interval(1.5D)"),
    ("float", "make_ym_interval(CAST(1.5 AS FLOAT))"),
    ("negative_decimals", "make_ym_interval(-1.99, 2.99)"),
    ("decimal_beyond_int", "make_ym_interval(2147483648.5)"),
    ("double_beyond_int", "make_ym_interval(0, 1e10D)"),
    ("double_nan", "make_ym_interval(0, CAST('NaN' AS DOUBLE))"),
    ("boolean", "make_ym_interval(true)"),
    ("date", "make_ym_interval(DATE '2024-01-01')"),
    ("timestamp", "make_ym_interval(1, TIMESTAMP '2024-01-01 00:00:00')"),
    ("binary", "make_ym_interval(X'01')"),
    ("array", "make_ym_interval(1, array(1))"),
    ("interval", "make_ym_interval(INTERVAL '1' YEAR)"),
    ("comparison", "make_ym_interval(1) = INTERVAL '1' YEAR"),
    ("try_add_overflow", "try_add(make_ym_interval(178956970, 8), INTERVAL '1' MONTH)"),
    ("try_subtract_overflow",
     "try_subtract(make_ym_interval(178956970, 8), INTERVAL '1' MONTH)"),
    ("try_multiply_overflow", "try_multiply(make_ym_interval(178956970, 8), 2)"),
    ("try_divide_overflow", "try_divide(make_ym_interval(178956970, 8), 2)"),
    ("try_add_date_overflow", "try_add(DATE '2024-01-01', make_ym_interval(178956970, 8))"),
]

# Unaliased queries, whose generated names Spark derives from both arguments.
NAMES = [
    ("name_year", "SELECT make_ym_interval(1)"),
    ("name_column", "SELECT k, make_ym_interval(y, mo) FROM required WHERE k <> 3"),
]


def definitions():
    """The record names and queries, in capture order."""
    for name, expression, table, condition in COLUMNS:
        yield f"column_{name}", f"SELECT k, {expression} AS r FROM {table} WHERE {condition}"
    for name, expression in LITERALS:
        yield f"literal_{name}", f"SELECT {expression} AS r"
    yield from NAMES


def python_value(value, kind):
    if value is None:
        return None
    if kind in ("integer", "long"):
        return int(value)
    if kind == "string":
        return value
    if kind == "double":
        return struct.unpack(">d", bytes.fromhex(value))[0]
    if kind == "float":
        return struct.unpack(">f", bytes.fromhex(value))[0]
    if kind.startswith("decimal"):
        return Decimal(value)
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.YearMonthIntervalType)):
        return str(row.getInt(index))
    if isinstance(data_type, types.LongType):
        return str(row.getLong(index))
    if isinstance(data_type, types.StringType):
        return row.getUTF8String(index).toString()
    if isinstance(data_type, types.BooleanType):
        return row.getBoolean(index)
    if isinstance(data_type, types.DoubleType):
        return double_bits(row.getDouble(index))
    if isinstance(data_type, types.FloatType):
        return float_bits(row.getFloat(index))
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-make-ym-interval")
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
            "value_encoding": ("INT, BIGINT, DECIMAL, STRING and YEAR TO MONTH months as "
                               "text; DOUBLE and FLOAT as the hex of their IEEE 754 bits; "
                               "BOOLEAN as a JSON boolean"),
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
