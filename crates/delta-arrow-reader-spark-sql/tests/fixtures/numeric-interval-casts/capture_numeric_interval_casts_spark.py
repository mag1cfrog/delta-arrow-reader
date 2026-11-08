"""Capture Spark casts of numbers to year-month and day-time intervals (#997).

Records analyzed, optimized and executed schemas, typed internal-row values and failing stages,
with the innermost Java exception of a failure, for CAST and TRY_CAST of TINYINT, SMALLINT, INT,
BIGINT and DECIMAL values to every year-month and day-time qualifier: positive, negative and
zero values, NULL, fractional DECIMALs and their rounding, DECIMAL(38, s) values whose products
exceed 128 bits or 34 significant digits, each type's limits, BIGINT values inside and outside
INT whose interval overflows, values whose interval overflows, numbers nested in ARRAY, STRUCT
and MAP casts, literal and column inputs, unaliased generated names, and FLOAT, DOUBLE and
BOOLEAN sources, which Spark rejects. Every query runs in both ANSI modes in UTC and
America/Los_Angeles. Run from the crate root in the reference environment described in
README.md; the output path must not exist.
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

YEAR_MONTH = ["YEAR", "YEAR TO MONTH", "MONTH"]
DAY_TIME = ["DAY", "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND", "HOUR", "HOUR TO MINUTE",
            "HOUR TO SECOND", "MINUTE", "MINUTE TO SECOND", "SECOND"]
QUALIFIERS = YEAR_MONTH + DAY_TIME
# One qualifier per end field.
END_FIELDS = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"]


def text(value):
    return None if value is None else str(value)


def double_bits(value):
    """A DOUBLE as the hex of its IEEE 754 bits."""
    return struct.pack(">d", value).hex()


# (TINYINT ti, SMALLINT si, INT i, BIGINT b, DECIMAL(10, 2) dc, DECIMAL(20, 6) dw) per row.
# Rows 0 to 3 are small, rows 4 and 5 the limits, row 6 holds sub-unit and half fractions.
NUMBERS = [
    (3, 3, 3, 3, "3.25", "3.250000"),
    (-3, -3, -3, -3, "-3.75", "-3.750000"),
    (0, 0, 0, 0, "0.00", "0.000000"),
    (None, None, None, None, None, None),
    (127, 32767, 2147483647, 9223372036854775807, "99999999.99", "99999999999999.999999"),
    (-128, -32768, -2147483648, -9223372036854775808, "-99999999.99",
     "-99999999999999.999999"),
    (1, 1, 1, 1, "0.50", "0.000001"),
]

# DECIMAL(38, 36), DECIMAL(38, 37) and DECIMAL(38, 38) values whose products by a unit length
# exceed 128-bit integers or 34 significant digits.
WIDE = [
    ("1.500000000000000000000000000000000000", "0.9583333333333333333333333333333333333",
     "0.25312872227430555555555555555555555555"),
    ("-8.458333333333333333333333333333333333", "-0.9583333333333333333333333333333333333",
     "-0.25312872227430555555555555555555555555"),
    (None, None, None),
]

REQUIRED = [
    (5, 5, 5, 5, "5.50", "0.500000"),
    (-5, -5, -5, -5, "-5.50", "-0.500000"),
]


def number_fields(nullable):
    names = [("ti", "byte"), ("si", "short"), ("i", "integer"), ("b", "long"),
             ("dc", "decimal(10,2)"), ("dw", "decimal(20,6)")]
    return [{"name": "k", "type": "integer", "nullable": False, "metadata": {}}] + [
        {"name": name, "type": kind, "nullable": nullable, "metadata": {}}
        for name, kind in names]


def rows(values):
    return [[str(key)] + [text(value) for value in row] for key, row in enumerate(values)]


INPUTS = [
    {"name": "numbers", "schema": {"type": "struct", "fields": number_fields(True)},
     "rows": rows(NUMBERS)},
    {"name": "required", "schema": {"type": "struct", "fields": number_fields(False)},
     "rows": rows(REQUIRED)},
    {"name": "wide", "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "w36", "type": "decimal(38,36)", "nullable": True, "metadata": {}},
        {"name": "w37", "type": "decimal(38,37)", "nullable": True, "metadata": {}},
        {"name": "w38", "type": "decimal(38,38)", "nullable": True, "metadata": {}}]},
     "rows": rows(WIDE)},
    {"name": "floats", "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "f", "type": "double", "nullable": True, "metadata": {}}]},
     "rows": [["0", double_bits(1.5)]]},
]


def slug(qualifier):
    return qualifier.lower().replace(" ", "_")


def definitions():
    """The record names and queries, in capture order."""
    column = "SELECT k, {cast}({source} AS INTERVAL {qualifier}) AS r FROM {table} WHERE {where}"
    for qualifier in QUALIFIERS:
        name = slug(qualifier)
        yield (f"int_{name}", column.format(cast="CAST", source="i", qualifier=qualifier,
                                            table="numbers", where="k IN (0, 1, 2, 3, 6)"))
        yield (f"int_max_{name}", column.format(cast="CAST", source="i", qualifier=qualifier,
                                                table="numbers", where="k = 4"))
        yield (f"int_min_{name}", column.format(cast="CAST", source="i", qualifier=qualifier,
                                                table="numbers", where="k = 5"))
        yield (f"bigint_max_{name}", column.format(cast="CAST", source="b", qualifier=qualifier,
                                                   table="numbers", where="k = 4"))
        yield (f"try_bigint_max_{name}", column.format(cast="TRY_CAST", source="b",
                                                       qualifier=qualifier, table="numbers",
                                                       where="k = 4"))
        yield (f"decimal_{name}", column.format(cast="CAST", source="dc", qualifier=qualifier,
                                                table="numbers", where="k IN (0, 1, 2, 3, 6)"))
        yield (f"required_int_{name}", column.format(cast="CAST", source="i",
                                                     qualifier=qualifier, table="required",
                                                     where="true"))
    for qualifier in END_FIELDS:
        name = slug(qualifier)
        for source in ["ti", "si", "b", "dw"]:
            yield (f"{source}_{name}", column.format(cast="CAST", source=source,
                                                     qualifier=qualifier, table="numbers",
                                                     where="k IN (0, 1, 2, 3, 6)"))
        for source in ["ti", "si", "dc"]:
            yield (f"{source}_limits_{name}", column.format(cast="CAST", source=source,
                                                            qualifier=qualifier,
                                                            table="numbers",
                                                            where="k IN (4, 5)"))
        # Each DECIMAL(20, 6) limit overflows, so each failing query holds one failing row.
        for limit, key in [("max", 4), ("min", 5)]:
            yield (f"dw_{limit}_{name}", column.format(cast="CAST", source="dw",
                                                       qualifier=qualifier, table="numbers",
                                                       where=f"k = {key}"))
        yield (f"required_decimal_{name}", column.format(cast="CAST", source="dc",
                                                         qualifier=qualifier, table="required",
                                                         where="true"))
        yield (f"try_decimal_limits_{name}", column.format(cast="TRY_CAST", source="dw",
                                                           qualifier=qualifier,
                                                           table="numbers", where="k IN (4, 5)"))
        for source in ["w36", "w37"]:
            yield (f"{source}_{name}", column.format(cast="CAST", source=source,
                                                     qualifier=qualifier, table="wide",
                                                     where="true"))
        yield (f"try_w38_{name}", column.format(cast="TRY_CAST", source="w38",
                                                qualifier=qualifier, table="wide", where="true"))
    for name, source, qualifier, where in [
        ("int_sized_bigint_max_year", "CAST(i AS BIGINT)", "YEAR", "k = 4"),
        ("int_sized_bigint_min_year", "CAST(i AS BIGINT)", "YEAR", "k = 5"),
        ("int_sized_bigint_max_day", "CAST(i AS BIGINT)", "DAY", "k = 4"),
        ("bigint_min_month", "b", "MONTH", "k = 5"),
        ("bigint_min_second", "b", "SECOND", "k = 5"),
    ]:
        yield name, column.format(cast="CAST", source=source, qualifier=qualifier,
                                  table="numbers", where=where)
    yield ("double_column_day", "SELECT k, CAST(f AS INTERVAL DAY) AS r FROM floats")
    literals = [
        ("negative_year", "CAST(-7 AS INTERVAL YEAR)"),
        ("negative_year_to_month", "CAST(-7 AS INTERVAL YEAR TO MONTH)"),
        ("negative_month", "CAST(-7 AS INTERVAL MONTH)"),
        ("decimal_year", "CAST(1.5 AS INTERVAL YEAR)"),
        ("decimal_month_half", "CAST(2.5 AS INTERVAL MONTH)"),
        ("decimal_month_negative_half", "CAST(-2.5 AS INTERVAL MONTH)"),
        ("decimal_day", "CAST(1.5 AS INTERVAL DAY)"),
        ("decimal_second", "CAST(1.5 AS INTERVAL SECOND)"),
        ("decimal_sub_microsecond", "CAST(0.0000005 AS INTERVAL SECOND)"),
        ("decimal_negative_sub_microsecond", "CAST(-0.0000005 AS INTERVAL SECOND)"),
        ("decimal_sub_microsecond_below_half", "CAST(0.0000004 AS INTERVAL SECOND)"),
        ("decimal_day_to_second", "CAST(1.000001 AS INTERVAL DAY TO SECOND)"),
        ("year_overflow", "CAST(178956971 AS INTERVAL YEAR)"),
        ("try_year_overflow", "TRY_CAST(178956971 AS INTERVAL YEAR)"),
        ("day_overflow", "CAST(106751992 AS INTERVAL DAY)"),
        ("try_day_overflow", "TRY_CAST(106751992 AS INTERVAL DAY)"),
        ("day_limit", "CAST(106751991 AS INTERVAL DAY)"),
        ("null_year", "CAST(NULL AS INTERVAL YEAR)"),
        ("typed_null_day", "CAST(CAST(NULL AS INT) AS INTERVAL DAY)"),
        ("wide_one_day", "CAST(CAST(1 AS DECIMAL(38,30)) AS INTERVAL DAY)"),
        ("wide_near_one_second",
         "CAST(0.99999999999999999999999999999999999999 AS INTERVAL SECOND)"),
        ("wide_rounded_year", "CAST(8.458333333333333333333333333333333333 AS INTERVAL YEAR)"),
        ("wide_rounded_month", "CAST(2.4999999999999999999999999999999999999 AS INTERVAL MONTH)"),
        ("wide_rounded_second", "CAST(1.4999994999999999999999999999999999999 AS INTERVAL SECOND)"),
        ("int_sized_bigint_year_overflow", "CAST(178956971L AS INTERVAL YEAR)"),
        ("bigint_month_overflow", "CAST(2147483648L AS INTERVAL MONTH)"),
        ("nested_array_year", "CAST(ARRAY(1, -2) AS ARRAY<INTERVAL YEAR>)[0]"),
        ("nested_struct_year", "CAST(NAMED_STRUCT('a', 3) AS STRUCT<a: INTERVAL YEAR>).a"),
        ("nested_map_hour", "CAST(MAP('k', 2) AS MAP<STRING, INTERVAL HOUR>)['k']"),
        ("double_year", "CAST(1.5D AS INTERVAL YEAR)"),
        ("float_second", "CAST(CAST(1.5 AS FLOAT) AS INTERVAL SECOND)"),
        ("boolean_day", "CAST(true AS INTERVAL DAY)"),
    ]
    for name, expression in literals:
        yield f"literal_{name}", f"SELECT {expression} AS r"
    yield "name_year", "SELECT CAST(-7 AS INTERVAL YEAR)"
    yield "name_day_to_second", "SELECT CAST(1 AS INTERVAL DAY TO SECOND)"


def python_value(value, kind):
    if value is None:
        return None
    if kind in ("byte", "short", "integer", "long"):
        return int(value)
    if kind == "double":
        return struct.unpack(">d", bytes.fromhex(value))[0]
    if kind.startswith("decimal"):
        return Decimal(value)
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, types.ByteType):
        return str(row.getByte(index))
    if isinstance(data_type, types.ShortType):
        return str(row.getShort(index))
    if isinstance(data_type, (types.IntegerType, types.YearMonthIntervalType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.DayTimeIntervalType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.DoubleType):
        return double_bits(row.getDouble(index))
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-numeric-interval-casts")
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
            "value_encoding": ("TINYINT, SMALLINT, INT, BIGINT and DECIMAL as text; DOUBLE as "
                               "the hex of its IEEE 754 bits; YEAR TO MONTH intervals as months "
                               "and DAY TO SECOND intervals as microseconds, as text"),
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
