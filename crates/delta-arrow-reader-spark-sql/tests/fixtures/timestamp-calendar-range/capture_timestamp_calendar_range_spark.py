"""Capture Spark's DATE_TRUNC, MONTHS_BETWEEN and WEEKOFYEAR across the TIMESTAMP years (#1087).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for DATE_TRUNC to every unit and
spelling, also of a struct field, MONTHS_BETWEEN with and without rounding and in both
argument orders, and WEEKOFYEAR of TIMESTAMP and DATE values. The values reach years 1 and
9999 and both sides of the nanosecond range's limits, and include Los Angeles local mean time,
both DST changes of 2024, with each pass of the repeated hour, and NULL. Every query runs in
both ANSI modes in UTC and America/Los_Angeles. Run from the crate root in the reference
environment described in README.md; the output path must not exist.
"""
import argparse
from datetime import date, datetime, timedelta, timezone
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

# (TIMESTAMP as a UTC instant, a second TIMESTAMP, DATE)
ROWS = [
    ("0001-01-02 00:00:00", "9999-12-31 23:59:59.999999", "0001-01-01"),
    ("9999-12-31 23:59:59.999999", "0001-01-02 00:00:00", "9999-12-31"),
    # Just outside and just inside Arrow's nanosecond range, 1677-09-21 00:12:43.145224192 to
    # 2262-04-11 23:47:16.854775807.
    ("1677-09-21 00:12:43", "2262-04-11 23:47:17", "1677-09-21"),
    ("1677-09-21 00:12:44", "2262-04-11 23:47:16", "2262-04-11"),
    # Los Angeles local mean time, -07:52:58.
    ("1800-06-15 12:34:56.789123", "1883-11-18 20:00:30", "1800-06-15"),
    # 03:30 PDT on the spring change day, and 01:30 on the autumn one in PDT and in PST.
    ("2024-03-10 10:30:00", "2024-11-03 08:30:00", "2024-03-10"),
    ("2024-11-03 09:30:00", "2024-02-29 12:00:00", "2024-11-03"),
    (None, None, None),
]


def micros(text):
    value = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def days(text):
    return (date.fromisoformat(text) - date(1970, 1, 1)).days


def optional(value, convert):
    return None if value is None else str(convert(value))


INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "ts2", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "d", "type": "date", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), optional(first, micros), optional(second, micros), optional(day, days)]
             for key, (first, second, day) in enumerate(ROWS)]}]

UNITS = ["YEAR", "YYYY", "YY", "QUARTER", "MONTH", "MM", "MON", "WEEK", "DAY", "DD", "HOUR",
         "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND"]


def definitions():
    for unit in UNITS:
        yield (f"date_trunc_{unit.lower()}",
               f"SELECT k, date_trunc('{unit}', ts) AS r FROM inputs")
    yield ("struct_date_trunc_day",
           "SELECT k, date_trunc('DAY', named_struct('t', ts).t) AS r FROM inputs")
    for name, expression in [
            ("months_between", "months_between(ts, ts2)"),
            ("months_between_unrounded", "months_between(ts, ts2, false)"),
            ("months_between_literal", "months_between(ts, TIMESTAMP '2000-02-29 12:00:00')"),
            ("months_between_date", "months_between(d, ts)"),
            ("weekofyear", "weekofyear(ts)"),
            ("weekofyear_date", "weekofyear(d)")]:
        yield name, f"SELECT k, {expression} AS r FROM inputs"
    for name, expression in [
            ("date_trunc_year", "date_trunc('YEAR', TIMESTAMP '0001-01-02 00:00:00')"),
            ("date_trunc_hour", "date_trunc('HOUR', TIMESTAMP '9999-12-31 23:59:59.999999')"),
            ("months_between",
             "months_between(TIMESTAMP '0001-01-31 00:00:00', TIMESTAMP '9999-12-31 00:00:00')"),
            ("weekofyear", "weekofyear(TIMESTAMP '9999-12-31 23:59:59')")]:
        yield f"literal_{name}", f"SELECT {expression} AS r"


def double_bits(value):
    """The IEEE 754 bits of a DOUBLE, as 16 hex digits, so values compare exactly."""
    return struct.pack(">d", value).hex()


def python_value(value, kind):
    if value is None:
        return None
    if kind in ("integer", "long"):
        return int(value)
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    if kind == "timestamp_ntz":
        return datetime(1970, 1, 1) + timedelta(microseconds=int(value))
    if kind == "date":
        return date(1970, 1, 1) + timedelta(days=int(value))
    if kind == "string":
        return value
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
    if isinstance(data_type, types.DoubleType):
        return double_bits(row.getDouble(index))
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-timestamp-calendar-range")
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
            "value_encoding": ("INT as int; DATE days since the epoch as int; TIMESTAMP "
                               "UTC-epoch and TIMESTAMP_NTZ wall-clock microseconds as long; "
                               "DOUBLE as the 16 hex digits of its IEEE 754 bits; STRING as "
                               "text"),
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
