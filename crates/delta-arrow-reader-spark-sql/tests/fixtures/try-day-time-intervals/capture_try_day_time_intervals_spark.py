"""Capture Spark TRY_ADD and TRY_SUBTRACT of DAY TO SECOND intervals (#1136).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for TRY_ADD and TRY_SUBTRACT of
DAY TO SECOND intervals of every qualifier pair, with YEAR TO MONTH pairs for comparison and
Spark's default column names. Inputs are a registered table of DAY TO SECOND intervals,
including the smallest and largest, and NULLs, and literals, including each qualifier's
largest value beside a small one. Spark adds them exactly and returns NULL on overflow. Every
query runs in both ANSI modes in UTC and America/Los_Angeles. Run from the crate root in the
reference environment described in README.md; the output path must not exist.
"""
import argparse
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles"]

DAY = 86_400_000_000
HOUR = 3_600_000_000
LARGEST = 9_223_372_036_854_775_807
SMALLEST = -9_223_372_036_854_775_808

# (a, b) DAY TO SECOND microseconds.
ROWS = [
    (DAY, 2 * HOUR),
    (LARGEST, 1),  # a + b overflows.
    (SMALLEST, -1),  # a + b overflows.
    (SMALLEST, 1),  # a - b overflows.
    (-DAY, DAY),
    (None, DAY),
    (DAY, None),
]


def text(value):
    return None if value is None else str(value)


INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "a", "type": "interval day to second", "nullable": True, "metadata": {}},
        {"name": "b", "type": "interval day to second", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), text(a), text(b)] for key, (a, b) in enumerate(ROWS)]}]

COLUMNS = [
    ("try_add", "try_add(a, b)"),
    ("try_subtract", "try_subtract(a, b)"),
    ("try_add_hour", "try_add(a, INTERVAL '1' HOUR)"),
    ("try_subtract_from_day", "try_subtract(INTERVAL '1' DAY, a)"),
    ("try_add_untyped_null", "try_add(a, NULL)"),
    ("lambda_try_add", "transform(array(a), x -> try_add(x, b))"),
]

LITERALS = [
    ("try_add_days", "try_add(INTERVAL '1' DAY, INTERVAL '2' DAY)"),
    ("try_add_day_hour", "try_add(INTERVAL '1' DAY, INTERVAL '1' HOUR)"),
    ("try_subtract_day_minutes", "try_subtract(INTERVAL '1' DAY, INTERVAL '30' MINUTE)"),
    ("try_add_seconds", "try_add(INTERVAL '1.5' SECOND, INTERVAL '0.000001' SECOND)"),
    ("try_add_overflow", "try_add(INTERVAL '106751991 04:00:54.775807' DAY TO SECOND, "
                         "INTERVAL '0.000001' SECOND)"),
    ("try_subtract_overflow", "try_subtract(INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND, "
                              "INTERVAL '0.000001' SECOND)"),
    ("try_add_null", "try_add(CAST(NULL AS INTERVAL DAY), INTERVAL '1' DAY)"),
    ("try_add_untyped_null", "try_add(INTERVAL '1' DAY, NULL)"),
    ("try_subtract_untyped_null_first", "try_subtract(NULL, INTERVAL '1' DAY)"),
    ("try_add_year_month", "try_add(INTERVAL '1' YEAR, INTERVAL '2' MONTH)"),
    ("try_add_year_month_overflow",
     "try_add(INTERVAL '178956970-7' YEAR TO MONTH, INTERVAL '1' MONTH)"),
    ("try_subtract_year_month", "try_subtract(INTERVAL '1' YEAR, INTERVAL '2' MONTH)"),
]


# One literal of every DAY TO SECOND qualifier.
QUALIFIERS = [
    ("day", "INTERVAL '1' DAY"),
    ("day_to_hour", "INTERVAL '1 02' DAY TO HOUR"),
    ("day_to_minute", "INTERVAL '1 02:03' DAY TO MINUTE"),
    ("day_to_second", "INTERVAL '1 02:03:04.000005' DAY TO SECOND"),
    ("hour", "INTERVAL '2' HOUR"),
    ("hour_to_minute", "INTERVAL '2:03' HOUR TO MINUTE"),
    ("hour_to_second", "INTERVAL '2:03:04.000005' HOUR TO SECOND"),
    ("minute", "INTERVAL '3' MINUTE"),
    ("minute_to_second", "INTERVAL '3:04.000005' MINUTE TO SECOND"),
    ("second", "INTERVAL '4.000005' SECOND"),
]

# The largest or smallest value of a qualifier beside a small one that overflows it.
QUALIFIER_OVERFLOWS = [
    ("largest_days_plus_hours", "try_add(INTERVAL '106751991' DAY, INTERVAL '5' HOUR)"),
    ("largest_hours_plus_minute", "try_add(INTERVAL '2562047788' HOUR, INTERVAL '1' MINUTE)"),
    ("largest_minutes_plus_second",
     "try_add(INTERVAL '153722867280' MINUTE, INTERVAL '55' SECOND)"),
    ("smallest_days_minus_hours", "try_subtract(INTERVAL '-106751991' DAY, INTERVAL '5' HOUR)"),
    ("smallest_hours_minus_minute",
     "try_subtract(INTERVAL '-2562047788' HOUR, INTERVAL '1' MINUTE)"),
]


def definitions():
    """The record names and queries, in capture order."""
    for name, expression in COLUMNS:
        yield f"column_{name}", f"SELECT k, {expression} AS r FROM inputs"
    for name, expression in LITERALS:
        yield f"literal_{name}", f"SELECT {expression} AS r"
    for function in ["try_add", "try_subtract"]:
        # Every ordered qualifier pair, as one column each.
        pairs = ", ".join(f"{function}({left}, {right}) AS {left_name}__{right_name}"
                          for left_name, left in QUALIFIERS
                          for right_name, right in QUALIFIERS)
        yield f"literal_{function}_qualifier_pairs", f"SELECT {pairs}"
        # A column beside every qualifier, in both orders.
        columns = ", ".join(f"{function}(a, {literal}) AS a__{name}, "
                            f"{function}({literal}, a) AS {name}__a"
                            for name, literal in QUALIFIERS)
        yield f"column_{function}_qualifiers", f"SELECT k, {columns} FROM inputs"
    for name, expression in QUALIFIER_OVERFLOWS:
        yield f"literal_{name}", f"SELECT {expression} AS r"
    # Spark's default column names.
    yield ("literal_unaliased_try_add",
           "SELECT try_add(INTERVAL '1' DAY, INTERVAL '2' HOUR), "
           "try_subtract(INTERVAL '1 02:03' DAY TO MINUTE, INTERVAL '4.000005' SECOND)")
    yield "column_unaliased_try_add", "SELECT k, try_add(a, b), try_subtract(b, a) FROM inputs"
    yield "literal_try_add_nulls", "SELECT try_add(NULL, NULL) AS r"
    yield "literal_try_subtract_nulls", "SELECT try_subtract(NULL, NULL) AS r"


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-try-day-time-intervals")
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
            "value_encoding": ("INT as int; DAY TO SECOND microseconds as long; YEAR TO MONTH "
                               "months as int; ARRAY as a list of its elements"),
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
