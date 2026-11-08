"""Capture Spark MAKE_INTERVAL values, errors and nullability (#1131).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for MAKE_INTERVAL with literal,
column and NULL arguments, every argument count, fractional and negative values, values
that overflow its months, days or microseconds, and TINYINT, SMALLINT, BIGINT, DECIMAL,
FLOAT, DOUBLE, STRING, BOOLEAN and DATE arguments, and for a TIMESTAMP minus a
MAKE_INTERVAL.
Every query runs in both ANSI modes in UTC and America/Los_Angeles. Run from the crate root in
the reference environment described in README.md; the output path must not exist.
"""
import argparse
from datetime import timedelta
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

# (years, months, weeks, days, hours, minutes, seconds as text) per row; row 3 overflows.
ROWS = [
    (1, 2, 3, 4, 5, 6, "7.000008"),
    (-1, -2, -3, -4, -5, -6, "-7.000008"),
    (None, 2, 3, 4, 5, 6, "7.500000"),
    (178956970, 8, 0, 0, 0, 0, "0.000000"),
    (0, 0, 0, 0, 0, 0, None),
]


def text(value):
    return None if value is None else str(value)


def double_bits(value):
    """A DOUBLE as the hex of its IEEE 754 bits."""
    return struct.pack(">d", value).hex()


# (b BIGINT, f DOUBLE, t STRING, dc DECIMAL(12, 2)) per row; row 1 overflows or is invalid
# as an INT part, and its DOUBLE is beyond DECIMAL(18, 6) seconds.
OTHER_ROWS = [
    ("5", double_bits(1.5), "7", "1.25"),
    ("4294967297", double_bits(1e20), "abc", "3000000000.50"),
    (None, None, None, None),
    ("-3", double_bits(-2.5), " -4 ", "-0.50"),
]


INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "y", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "mo", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "w", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "d", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "h", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "mi", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "s", "type": "decimal(18,6)", "nullable": True, "metadata": {}}]},
    "rows": [[str(key)] + [text(value) for value in row] for key, row in enumerate(ROWS)]}, {
    "name": "required",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "d", "type": "integer", "nullable": False, "metadata": {}}]},
    "rows": [["0", "1"], ["1", "-2"]]}, {
    "name": "others",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "b", "type": "long", "nullable": True, "metadata": {}},
        {"name": "f", "type": "double", "nullable": True, "metadata": {}},
        {"name": "t", "type": "string", "nullable": True, "metadata": {}},
        {"name": "dc", "type": "decimal(12,2)", "nullable": True, "metadata": {}}]},
    "rows": [[str(key)] + list(row) for key, row in enumerate(OTHER_ROWS)]}]

COLUMNS = [
    ("all_arguments", "make_interval(y, mo, w, d, h, mi, s)", "inputs", "k <> 3"),
    ("days", "make_interval(0, 0, 0, d)", "inputs", "k <> 3"),
    ("overflow", "make_interval(y, mo, w, d, h, mi, s)", "inputs", "k = 3"),
    ("required_days", "make_interval(0, 0, 0, d)", "required", "true"),
    ("required_years", "make_interval(d)", "required", "true"),
    ("timestamp_minus", "TIMESTAMP '2024-01-01 00:00:00' - make_interval(0, 0, 0, d)",
     "required", "true"),
    ("bigint_hours", "make_interval(0, 0, 0, 0, b)", "others", "k <> 1"),
    ("bigint_hours_overflow", "make_interval(0, 0, 0, 0, b)", "others", "k = 1"),
    ("bigint_seconds", "make_interval(0, 0, 0, 0, 0, 0, b)", "others", "true"),
    ("double_days", "make_interval(0, 0, 0, f)", "others", "k <> 1"),
    ("double_days_large", "make_interval(0, 0, 0, f)", "others", "k = 1"),
    ("double_seconds", "make_interval(0, 0, 0, 0, 0, 0, f)", "others", "k <> 1"),
    ("double_seconds_large", "make_interval(0, 0, 0, 0, 0, 0, f)", "others", "k = 1"),
    ("string_days", "make_interval(0, 0, 0, t)", "others", "k <> 1"),
    ("string_days_invalid", "make_interval(0, 0, 0, t)", "others", "k = 1"),
    ("string_seconds", "make_interval(0, 0, 0, 0, 0, 0, t)", "others", "k <> 1"),
    ("decimal_days", "make_interval(0, 0, 0, dc)", "others", "k <> 1"),
    ("decimal_days_overflow", "make_interval(0, 0, 0, dc)", "others", "k = 1"),
    ("decimal_seconds", "make_interval(0, 0, 0, 0, 0, 0, dc)", "others", "true"),
]

LITERALS = [
    ("all_arguments", "make_interval(1, 2, 3, 4, 5, 6, 7.000008)"),
    ("no_arguments", "make_interval()"),
    ("years", "make_interval(1)"),
    ("years_months", "make_interval(1, 2)"),
    ("to_days", "make_interval(1, 2, 3, 4)"),
    ("to_minutes", "make_interval(1, 2, 3, 4, 5, 6)"),
    ("negative", "make_interval(-1, -2, -3, -4, -5, -6, -7.000008)"),
    ("fractional_seconds", "make_interval(0, 0, 0, 0, 0, 0, 59.999999)"),
    ("smallest_second_fraction", "make_interval(0, 0, 0, 0, 0, 0, 0.000001)"),
    ("large_seconds", "make_interval(0, 0, 0, 0, 0, 0, 9223372036854.775807)"),
    ("null_year", "make_interval(NULL, 1)"),
    ("null_seconds", "make_interval(1, 2, 3, 4, 5, 6, NULL)"),
    ("months_overflow", "make_interval(178956970, 8)"),
    ("years_overflow", "make_interval(178956971)"),
    ("days_overflow", "make_interval(0, 0, 306783378, 7)"),
    ("hours_overflow", "make_interval(0, 0, 0, 0, 2562047789)"),
    ("seconds_overflow", "make_interval(0, 0, 0, 0, 2562047788, 0, 54.775808)"),
    ("timestamp_minus", "TIMESTAMP '2024-01-01 00:00:00' - make_interval(0, 1, 0, 1, 1)"),
    ("timestamp_minus_smallest_months",
     "TIMESTAMP '2024-01-01 00:00:00' - make_interval(-178956970, -8)"),
    ("too_many_arguments", "make_interval(1, 2, 3, 4, 5, 6, 7, 8)"),
    ("smallint_tinyint", "make_interval(1S, 2Y)"),
    ("bigint_wraps", "make_interval(4294967297L)"),
    ("decimal_part", "make_interval(2.5)"),
    ("negative_decimal_part", "make_interval(-2.5)"),
    ("decimal_part_overflow", "make_interval(3000000000.5)"),
    ("double_part", "make_interval(1.5D)"),
    ("double_part_nan", "make_interval(CAST('NaN' AS DOUBLE))"),
    ("string_parts", "make_interval('1', '2', '3', '4', '5', '6', '7.000008')"),
    ("empty_string", "make_interval('')"),
    ("invalid_string", "make_interval('x')"),
    ("boolean_part", "make_interval(true)"),
    ("date_part", "make_interval(DATE '2024-01-01')"),
    ("int_seconds", "make_interval(0, 0, 0, 0, 0, 0, 61)"),
    ("bigint_seconds_large", "make_interval(0, 0, 0, 0, 0, 0, 9223372036854775807L)"),
    ("double_seconds", "make_interval(0, 0, 0, 0, 0, 0, 1.5D)"),
    ("double_seconds_half_microsecond", "make_interval(0, 0, 0, 0, 0, 0, 5.0E-7D)"),
    ("double_seconds_large", "make_interval(0, 0, 0, 0, 0, 0, 1.0E20D)"),
    ("double_seconds_nan", "make_interval(0, 0, 0, 0, 0, 0, CAST('NaN' AS DOUBLE))"),
    ("float_seconds", "make_interval(0, 0, 0, 0, 0, 0, CAST(1.5 AS FLOAT))"),
    ("decimal_seconds_rounding", "make_interval(0, 0, 0, 0, 0, 0, 1.0000005)"),
    ("negative_decimal_seconds_rounding", "make_interval(0, 0, 0, 0, 0, 0, -1.0000005)"),
    ("wide_decimal_seconds", "make_interval(0, 0, 0, 0, 0, 0, CAST(1.25 AS DECIMAL(38, 10)))"),
    ("string_seconds", "make_interval(0, 0, 0, 0, 0, 0, '1.5')"),
    ("invalid_string_seconds", "make_interval(0, 0, 0, 0, 0, 0, 'x')"),
    ("boolean_seconds", "make_interval(0, 0, 0, 0, 0, 0, true)"),
]


def definitions():
    """The record names and queries, in capture order."""
    for name, expression, table, condition in COLUMNS:
        yield f"column_{name}", f"SELECT k, {expression} AS r FROM {table} WHERE {condition}"
    for name, expression in LITERALS:
        yield f"literal_{name}", f"SELECT {expression} AS r"


def python_value(value, kind):
    if value is None:
        return None
    if kind in ("integer", "long"):
        return int(value)
    if kind == "double":
        return struct.unpack(">d", bytes.fromhex(value))[0]
    if kind == "string":
        return value
    if kind.startswith("decimal"):
        return Decimal(value)
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
    if isinstance(data_type, types.DoubleType):
        return double_bits(row.getDouble(index))
    if isinstance(data_type, types.CalendarIntervalType):
        from py4j.java_gateway import get_field
        interval = row.getInterval(index)
        return [str(get_field(interval, field)) for field in ("months", "days", "microseconds")]
    if isinstance(data_type, types.DecimalType):
        return row.getDecimal(index, data_type.precision, data_type.scale).toString()
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-make-interval")
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
            "value_encoding": ("INT and BIGINT as text; DOUBLE as the hex of its IEEE 754 "
                               "bits; DECIMAL and STRING as text; TIMESTAMP UTC-epoch "
                               "microseconds as text; CALENDAR INTERVAL as [months, days, "
                               "microseconds]"),
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
