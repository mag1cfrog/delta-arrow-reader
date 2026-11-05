"""Capture Spark interval qualifiers, generated names and typed interval values.

The query corpus is the original #749 capture (candidate commit c54d99c3,
script SHA256 12cc8fd6...) plus its eight NULL-coercion review probes. Values
are recorded from Spark's internal rows instead of string casts. Run from the
crate root in the reference environment described in README.md; the output
path must not exist.
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
INPUT_ROWS = [(0, 2), (1, 3), (2, None), (3, 5)]

INTERVALS = [
    ("year", "INTERVAL '2' YEAR", "INTERVAL '0' YEAR"),
    ("year_month", "INTERVAL '2-3' YEAR TO MONTH", "INTERVAL '0-0' YEAR TO MONTH"),
    ("month", "INTERVAL '27' MONTH", "INTERVAL '0' MONTH"),
    ("day", "INTERVAL '2' DAY", "INTERVAL '0' DAY"),
    ("day_hour", "INTERVAL '2 03' DAY TO HOUR", "INTERVAL '0 00' DAY TO HOUR"),
    ("day_minute", "INTERVAL '2 03:04' DAY TO MINUTE", "INTERVAL '0 00:00' DAY TO MINUTE"),
    ("day_second", "INTERVAL '2 03:04:05.123456' DAY TO SECOND",
     "INTERVAL '0 00:00:00' DAY TO SECOND"),
    ("hour", "INTERVAL '51' HOUR", "INTERVAL '0' HOUR"),
    ("hour_minute", "INTERVAL '51:04' HOUR TO MINUTE", "INTERVAL '0:00' HOUR TO MINUTE"),
    ("hour_second", "INTERVAL '51:04:05.123456' HOUR TO SECOND",
     "INTERVAL '0:00:00' HOUR TO SECOND"),
    ("minute", "INTERVAL '3064' MINUTE", "INTERVAL '0' MINUTE"),
    ("minute_second", "INTERVAL '3064:05.123456' MINUTE TO SECOND",
     "INTERVAL '0:00' MINUTE TO SECOND"),
    ("second", "INTERVAL '183845.123456' SECOND", "INTERVAL '0' SECOND"),
]


def queries():
    timestamp = "TIMESTAMP '2020-01-01 00:00:00.123456'"
    for name, interval, zero in INTERVALS:
        for kind, expression in [("literal", interval), ("zero", zero),
                                 ("negative", f"-({interval})")]:
            yield f"{name}_{kind}", "literal", f"SELECT {expression}"
        for operation, expression in [
            ("left_multiply", f"id*({interval})"),
            ("right_multiply", f"({interval})*id"),
            ("divide", f"({interval})/2"),
            ("add", f"({interval})+({zero})"),
        ]:
            yield f"{name}_{operation}", "arithmetic", f"SELECT {expression} FROM inputs"
        for operation, expression in [
            ("add", f"{timestamp}+({interval})"),
            ("reverse_add", f"({interval})+{timestamp}"),
            ("subtract", f"{timestamp}-({interval})"),
            ("computed", f"{timestamp}+id*({interval})"),
        ]:
            yield f"timestamp_{name}_{operation}", "timestamp", f"SELECT {expression} FROM inputs"
    for name, interval in [
        ("years", "INTERVAL 2 YEARS"), ("months", "INTERVAL 27 MONTHS"),
        ("days", "INTERVAL 2 DAYS"), ("hours", "INTERVAL 51 HOURS"),
        ("minutes", "INTERVAL 3064 MINUTES"), ("seconds", "INTERVAL 1.123456 SECONDS"),
        ("week", "INTERVAL 1 WEEK"), ("millisecond", "INTERVAL 1 MILLISECOND"),
        ("microsecond", "INTERVAL 1 MICROSECOND"),
        ("multi_ym", "INTERVAL 2 YEARS 3 MONTHS"),
        ("multi_zero_ym", "INTERVAL 0 YEARS 0 MONTHS"),
        ("multi_dt", "INTERVAL 2 DAYS 3 HOURS 4 MINUTES 5.123456 SECONDS"),
        ("multi_hour_minute", "INTERVAL 0 HOURS 0 MINUTES"),
        ("multi_zero_day_hour", "INTERVAL 0 DAYS 0 HOURS"),
        ("multi_cancel_ym", "INTERVAL 1 YEAR -12 MONTHS"),
        ("multi_cancel_dt", "INTERVAL 1 DAY -24 HOURS"),
        ("string", "INTERVAL '2 days 3 hours'"),
        ("mixed", "INTERVAL 1 MONTH 1 DAY"),
        ("mixed_zero", "INTERVAL 0 MONTHS 0 DAYS"),
        ("precision", "INTERVAL '0.123456789' SECOND"),
    ]:
        yield f"syntax_{name}", "syntax", f"SELECT {interval}"
    for name, expression in [
        ("day_hour", "INTERVAL 1 DAY + INTERVAL 1 HOUR"),
        ("hour_day", "INTERVAL 1 HOUR + INTERVAL 1 DAY"),
        ("year_month", "INTERVAL 1 YEAR + INTERVAL 1 MONTH"),
        ("month_year", "INTERVAL 1 MONTH - INTERVAL 1 YEAR"),
        ("fractional_multiply", "INTERVAL 1 DAY * 0.5"),
        ("fractional_divide", "INTERVAL 1 DAY / 3"),
        ("null_multiply", "INTERVAL 1 DAY * CAST(NULL AS INT)"),
    ]:
        yield f"arithmetic_{name}", "arithmetic", f"SELECT {expression}"
    for name, _, _ in INTERVALS:
        qualifier = name.replace("_", " TO ").upper()
        family = "YEAR TO MONTH" if name in ["year", "year_month", "month"] else "DAY TO SECOND"
        source = "INTERVAL '2-3' YEAR TO MONTH" if family == "YEAR TO MONTH" else (
            "INTERVAL '2 03:04:05.123456' DAY TO SECOND")
        yield f"cast_{name}", "cast", f"SELECT CAST({source} AS INTERVAL {qualifier})"
        yield f"cast_null_{name}", "cast", f"SELECT CAST(NULL AS INTERVAL {qualifier})"
    key = "TIMESTAMP '2020-01-01 00:00:00'+id*INTERVAL 1 DAY"
    for name, bound in [
        ("day", "INTERVAL 1 DAY"), ("hour", "INTERVAL 24 HOURS"),
        ("day_hour", "INTERVAL '1 00' DAY TO HOUR"),
        ("day_second", "INTERVAL '1 00:00:00' DAY TO SECOND"),
        ("year", "INTERVAL 1 YEAR"), ("month", "INTERVAL 1 MONTH"),
        ("numeric", "1"), ("mixed", "INTERVAL 1 MONTH 1 DAY"),
    ]:
        expression = f"SUM(v) OVER (ORDER BY {key} RANGE BETWEEN {bound} PRECEDING AND CURRENT ROW)"
        yield f"range_{name}", "window", f"SELECT id,{expression} FROM inputs"
        yield f"range_{name}_aliased", "window_alias", f"SELECT id,{expression} AS r FROM inputs"
    for name, bound in [("day", "INTERVAL 1 DAY"), ("hour", "INTERVAL 24 HOURS"),
                        ("day_hour", "INTERVAL '1 00' DAY TO HOUR"),
                        ("zero_year", "INTERVAL 0 YEAR")]:
        expression = ("SUM(v) OVER (ORDER BY DATE_ADD(DATE '2020-01-01',id) "
                      f"RANGE BETWEEN {bound} PRECEDING AND CURRENT ROW)")
        yield f"date_range_{name}", "date_window", f"SELECT id,{expression} AS r FROM inputs"


NULL_COERCION_PROBES = [
    ("null_left_multiply", "SELECT NULL * INTERVAL 1 DAY"),
    ("null_right_multiply", "SELECT INTERVAL 1 DAY * NULL"),
    ("typed_null_left_multiply", "SELECT CAST(NULL AS INT) * INTERVAL 1 DAY"),
    ("typed_null_right_multiply", "SELECT INTERVAL 1 DAY * CAST(NULL AS INT)"),
    ("timestamp_null_add", "SELECT TIMESTAMP '2020-01-01 00:00:00' + NULL"),
    ("null_timestamp_add", "SELECT NULL + TIMESTAMP '2020-01-01 00:00:00'"),
    ("timestamp_null_subtract", "SELECT TIMESTAMP '2020-01-01 00:00:00' - NULL"),
    ("timestamp_typed_interval_null_add",
     "SELECT TIMESTAMP '2020-01-01 00:00:00' + CAST(NULL AS INTERVAL DAY)"),
]


def definitions():
    yield from queries()
    for name, sql in NULL_COERCION_PROBES:
        yield f"null_coercion_{name}", "null_coercion", sql


def encode_value(row, index, data_type, types, get_field):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.YearMonthIntervalType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.DayTimeIntervalType,
                              types.TimestampType, types.TimestampNTZType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.CalendarIntervalType):
        interval = row.getInterval(index)
        return [str(get_field(interval, field)) for field in ("months", "days", "microseconds")]
    raise AssertionError(f"unexpected result type {data_type}")


def error_condition(error, Py4JError):
    condition = getattr(error, "getCondition", lambda: None)()
    cause = getattr(error, "java_exception", None)
    while condition is None and cause is not None:
        try:
            condition = cause.getCondition()
        except Py4JError:
            pass
        cause = cause.getCause()
    return condition


def capture(spark, sql, types, get_field, Py4JError):
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
        rows = [[encode_value(row, index, field.dataType, types, get_field)
                 for index, field in enumerate(fields)] for row in internal_rows]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        record["expected"] = {"status": "ok", "schema": frame.schema.jsonValue(), "rows": rows}
    except Exception as error:
        condition = error_condition(error, Py4JError)
        assert condition is not None, (sql, str(error))
        record["expected"] = {"status": "error", "condition": condition,
                              "message": str(error).splitlines()[0]}
        record["spark_error_phase"] = phase
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh JSONL output path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output

    import pyspark
    from py4j.java_gateway import get_field
    from py4j.protocol import Py4JError
    from pyspark.sql import SparkSession, types

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={SPARK_REVISION}" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-interval-qualifiers")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([types.StructField("id", types.IntegerType(), False),
                                   types.StructField("v", types.IntegerType(), True)])
        spark.createDataFrame(INPUT_ROWS, schema).createOrReplaceTempView("inputs")
        header = {
            "spark_version": spark.version, "spark_build": build,
            "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
            "python_version": sys.version.split()[0], "timezone": "UTC",
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "value_encoding": ("integers, DATE days and YEAR-MONTH months as int; BIGINT, "
                               "DAY-TIME microseconds and TIMESTAMP UTC microseconds as long; "
                               "CALENDAR intervals as [months, days, microseconds]"),
            "settings": {"spark.sql.optimizer.excludedRules":
                         spark.conf.get("spark.sql.optimizer.excludedRules")},
            # Recorded inputs encode integers as decimal strings.
            "inputs": [{"name": "inputs", "schema": schema.jsonValue(),
                        "rows": [[str(i), None if v is None else str(v)] for i, v in INPUT_ROWS]}],
        }
        records = [header]
        cases = list(definitions())
        assert len({name for name, _, _ in cases}) == len(cases)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, context, sql in cases:
                records.append({"id": name, "context": context, "ansi": ansi, "sql": sql,
                                **capture(spark, sql, types, get_field, Py4JError)})
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
