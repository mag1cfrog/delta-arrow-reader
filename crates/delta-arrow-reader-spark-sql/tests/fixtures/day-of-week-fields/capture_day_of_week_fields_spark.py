"""Capture Spark's day-of-week and week-year fields of EXTRACT, DATE_PART and DATEPART (#1116).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for the DAYOFWEEK, DOW, DAYOFWEEK_ISO,
DOW_ISO and YEAROFWEEK fields, with DAY, WEEK, DOY, DAYOFWEEK() and WEEKDAY() as controls. The
sources are nullable DATE, TIMESTAMP, TIMESTAMP_NTZ and STRING columns and literals: Sundays
and Mondays at ISO year boundaries, a Los Angeles DST change day, years 1 and 9999, the epoch,
a TIMESTAMP whose local day differs between UTC and Los Angeles, and NULL, and STRING values
that are not dates. Every query runs in
both ANSI modes in UTC and America/Los_Angeles. Run from the crate root in the reference
environment described in README.md; the output path must not exist.
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

# (DATE, TIMESTAMP as a UTC instant, TIMESTAMP_NTZ, STRING)
ROWS = [
    ("2024-03-10", "2024-03-10 07:30:00", "2024-03-10 02:30:00", "2024-03-10"),  # a Sunday
    ("2024-03-11", "2024-03-11 12:00:00", "2024-03-11 00:00:00", "2024-03-11"),  # a Monday
    # Monday 2024-12-30 is in ISO year 2025; Sunday 2021-01-03 is in ISO year 2020.
    ("2024-12-30", "2024-12-30 05:00:00", "2024-12-30 23:59:59.999999", "2024-12-30"),
    ("2021-01-03", "2021-01-03 23:00:00", "2021-01-03 12:00:00", "2021-01-03"),
    ("0001-01-01", "0001-01-01 12:00:00", "0001-01-01 00:00:00", "0001-01-01"),
    ("9999-12-31", "9999-12-31 12:00:00", "9999-12-31 23:59:59.999999", "9999-12-31"),
    ("1970-01-01", "1970-01-01 00:00:00", "1970-01-01 00:00:00", "1970-01-01 10:00:00"),
    (None, None, None, None),
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
        {"name": "d", "type": "date", "nullable": True, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "tn", "type": "timestamp_ntz", "nullable": True, "metadata": {}},
        {"name": "s", "type": "string", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), optional(day, days), optional(instant, micros), optional(local, micros),
              text]
             for key, (day, instant, local, text) in enumerate(ROWS)]}]

FIELDS = ["DAYOFWEEK", "DOW", "DAYOFWEEK_ISO", "DOW_ISO", "YEAROFWEEK"]
CONTROLS = ["DAY", "WEEK", "DOY"]
SOURCES = ["d", "ts", "tn", "s"]
LITERALS = [("date", "DATE '2024-03-10'"), ("timestamp", "TIMESTAMP '2024-03-10 23:30:00'"),
            ("timestamp_ntz", "TIMESTAMP_NTZ '2024-12-30 01:00:00'"), ("string", "'2021-01-03'")]


def definitions():
    for source in SOURCES:
        for field in FIELDS + CONTROLS:
            yield (f"extract_{field.lower()}_{source}",
                   f"SELECT k, extract({field} FROM {source}) AS r FROM inputs")
        for field in FIELDS:
            yield (f"date_part_{field.lower()}_{source}",
                   f"SELECT k, date_part('{field.lower()}', {source}) AS r FROM inputs")
        yield (f"datepart_dow_{source}", f"SELECT k, datepart('DOW', {source}) AS r FROM inputs")
        yield (f"dayofweek_{source}", f"SELECT k, dayofweek({source}) AS r FROM inputs")
        yield (f"weekday_{source}", f"SELECT k, weekday({source}) AS r FROM inputs")
    for name, literal in LITERALS:
        for field in FIELDS:
            yield f"literal_{field.lower()}_{name}", f"SELECT extract({field} FROM {literal}) AS r"
    # A STRING that is not a date fails under ANSI and is NULL otherwise.
    yield "literal_dow_invalid_string", "SELECT extract(DOW FROM 'not a date') AS r"
    yield ("extract_dow_invalid_string_column",
           "SELECT k, extract(DOW FROM concat(s, 'x')) AS r FROM inputs")
    # A padded, mixed-case field name.
    yield "date_part_padded_dow", "SELECT k, date_part(' Dow ', d) AS r FROM inputs"


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
    spark = (SparkSession.builder.master("local[1]").appName("dar-day-of-week-fields")
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
                               "STRING as text"),
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
