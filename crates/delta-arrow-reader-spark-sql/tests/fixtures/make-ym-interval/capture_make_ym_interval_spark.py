"""Capture Spark MAKE_YM_INTERVAL values, errors and nullability (#1159).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for MAKE_YM_INTERVAL with literal,
nullable-column, NOT NULL-column, empty-input and NULL arguments, none, one and two arguments,
signs, the YEAR TO MONTH limits and one month past them, INT, TINYINT, SMALLINT, BIGINT,
DECIMAL, DOUBLE, BOOLEAN, STRING, malformed and empty STRING arguments, a NULL beside an
overflowing argument or a failing argument cast in either order, unaliased generated names, and for comparisons, negation and IS NULL
over its result. Every query runs in both ANSI modes in UTC and America/Los_Angeles. Run from
the crate root in the reference environment described in README.md; the output path must not
exist.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
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


INPUTS = [
    {"name": "nullable", "schema": {"type": "struct", "fields": fields(True)},
     "rows": rows(NULLABLE_ROWS)},
    {"name": "required", "schema": {"type": "struct", "fields": fields(False)},
     "rows": rows(REQUIRED_ROWS)},
    {"name": "empty", "schema": {"type": "struct", "fields": fields(False)}, "rows": []}]

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
    ("required_small_types", "make_ym_interval(CAST(k AS TINYINT), CAST(mo AS SMALLINT))",
     "required", "true"),
    ("required_null_month", "make_ym_interval(y, NULL)", "required", "true"),
    ("required_typed_null_year", "make_ym_interval(CAST(NULL AS INT), mo)", "required", "true"),
    ("required_comparison", "make_ym_interval(y) = INTERVAL '1' YEAR", "required", "k <> 3"),
    ("required_negation", "-make_ym_interval(y, mo)", "required", "k IN (0, 1, 2)"),
    ("required_is_null", "make_ym_interval(y, mo) IS NULL", "required", "k <> 3"),
    ("empty_both", "make_ym_interval(y, mo)", "empty", "true"),
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
    ("bigints", "make_ym_interval(1L, 2L)"),
    ("bigint_beyond_int", "make_ym_interval(4294967297L)"),
    ("tinyint_smallint", "make_ym_interval(1Y, 2S)"),
    ("decimal", "make_ym_interval(1.5)"),
    ("double", "make_ym_interval(1.5D)"),
    ("boolean", "make_ym_interval(true)"),
    ("comparison", "make_ym_interval(1) = INTERVAL '1' YEAR"),
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
            "value_encoding": ("INT, BIGINT, STRING and YEAR TO MONTH months as text; "
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
