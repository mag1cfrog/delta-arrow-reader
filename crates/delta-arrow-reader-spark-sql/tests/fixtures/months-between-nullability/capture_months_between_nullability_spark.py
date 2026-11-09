"""Capture Spark's MONTHS_BETWEEN values and nullability (#1199).

Records analyzed, optimized and executed schemas, typed internal-row values and failing stages,
with the innermost Java exception of a failure, for MONTHS_BETWEEN with and without `roundOff`
over NOT NULL and nullable DATE, TIMESTAMP, TIMESTAMP_NTZ and STRING columns and literals, with
literal, NOT NULL column, nullable column, NULL, volatile, Boolean-expression, STRING, INT and
DATE `roundOff` values, beside a failing argument, and of TIMESTAMP_NTZ times on the Los Angeles
change days. Spark casts both arguments to TIMESTAMP, and declares the result nullable when an
argument or `roundOff` is. Every query runs in both ANSI modes in UTC and America/Los_Angeles.
Run from the crate root in the reference environment described in README.md; the output path
must not exist.
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

# (DATE, TIMESTAMP instant in UTC, TIMESTAMP_NTZ, STRING, roundOff, STRING roundOff); the
# nullable copies are NULL in the last row. The rows hold month ends, a leap day, the Los Angeles
# spring and autumn change days and the epoch.
ROWS = [
    ("2024-01-31", "2024-03-31 12:00:00", "2024-02-29 23:30:00", "2024-02-29 06:00:00", True,
     "true"),
    ("2024-03-10", "2023-11-05 09:30:00", "2023-11-05 01:30:00", "2024-03-10 03:00:00", False,
     "no"),
    ("1969-12-31", "1970-01-31 00:00:00", "1960-02-29 23:59:59", "1970-01-01", True, "Y"),
]


def micros(text):
    value = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def days(text):
    return (date.fromisoformat(text) - date(1970, 1, 1)).days


def double_bits(value):
    """A DOUBLE as the hex of its IEEE 754 bits."""
    return struct.pack(">d", value).hex()


LAST = len(ROWS) - 1
INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "dt", "type": "date", "nullable": False, "metadata": {}},
        {"name": "dtn", "type": "date", "nullable": True, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": False, "metadata": {}},
        {"name": "tsn", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "tn", "type": "timestamp_ntz", "nullable": False, "metadata": {}},
        {"name": "tnn", "type": "timestamp_ntz", "nullable": True, "metadata": {}},
        {"name": "s", "type": "string", "nullable": False, "metadata": {}},
        {"name": "sn", "type": "string", "nullable": True, "metadata": {}},
        {"name": "ro", "type": "boolean", "nullable": False, "metadata": {}},
        {"name": "ron", "type": "boolean", "nullable": True, "metadata": {}},
        {"name": "rs", "type": "string", "nullable": False, "metadata": {}}]},
    "rows": [[str(key), str(days(day)), None if key == LAST else str(days(day)),
              str(micros(instant)), None if key == LAST else str(micros(instant)),
              str(micros(local)), None if key == LAST else str(micros(local)),
              text, None if key == LAST else text,
              str(round_off).lower(), None if key == LAST else str(round_off).lower(), spelled]
             for key, (day, instant, local, text, round_off, spelled) in enumerate(ROWS)]}]

# (name, NOT NULL column, nullable column)
COLUMNS = [("date", "dt", "dtn"), ("timestamp", "ts", "tsn"), ("timestamp_ntz", "tn", "tnn"),
           ("string", "s", "sn")]
LITERALS = [("date", "DATE '2024-01-02'"), ("timestamp", "TIMESTAMP '2024-01-02 00:30:00'"),
            ("timestamp_ntz", "TIMESTAMP_NTZ '2024-01-02 00:30:00'"),
            ("string", "'2024-01-02 00:30:00'"), ("null", "NULL")]
ROUND_OFFS = [("", ""), ("_round_off_false", ", false"), ("_round_off_column", ", ro"),
              ("_round_off_nullable", ", ron"), ("_round_off_null", ", NULL")]


def definitions():
    for name, required, nullable in COLUMNS:
        for suffix, round_off in ROUND_OFFS:
            yield (f"column_{name}{suffix}",
                   f"SELECT k, months_between({required}, dt{round_off}) AS r FROM inputs")
        yield (f"column_nullable_{name}",
               f"SELECT k, months_between({nullable}, dt) AS r FROM inputs")
        yield (f"column_{name}_by_nullable",
               f"SELECT k, months_between(ts, {nullable}) AS r FROM inputs")
    for name, value in LITERALS:
        for suffix, round_off in ROUND_OFFS[:2] + ROUND_OFFS[4:]:
            yield (f"literal_{name}{suffix}",
                   f"SELECT months_between({value}, DATE '2024-03-31'{round_off}) AS r")
        yield (f"literal_{name}_by_column",
               f"SELECT k, months_between({value}, dt) AS r FROM inputs")
    yield ("literal_timestamp_ntz_date",
           "SELECT months_between(TIMESTAMP_NTZ '2024-01-02 00:30:00', DATE '2024-01-02') AS r")
    # The arguments are evaluated before roundOff, so a failing argument fails beside a NULL one.
    yield ("column_failing_date_nullable_round_off",
           "SELECT k, months_between(CAST(CASE WHEN k = 2 THEN 'garbage' ELSE '2024-01-01' END "
           "AS DATE), dt, ron) AS r FROM inputs")
    # A volatile roundOff is read once per row, so each row is rounded or NULL.
    yield ("volatile_round_off",
           "SELECT count(DISTINCT r) AS n FROM (SELECT months_between("
           "TIMESTAMP '2024-03-10 12:00:00', DATE '2024-02-01', "
           "CASE WHEN rand() > 0.5 THEN true END) AS r FROM range(2000))")
    yield ("column_round_off_or_true",
           "SELECT k, months_between(ts, dt, ron OR true) AS r FROM inputs")
    # Spark casts a STRING roundOff to BOOLEAN only in ANSI mode, and rejects other types.
    for name, round_off in [("string", "rs"), ("string_literal", "'yes'"), ("int", "1"),
                            ("null_int", "CAST(NULL AS INT)"), ("date", "dt")]:
        yield (f"column_{name}_typed_round_off",
               f"SELECT k, months_between(ts, dt, {round_off}) AS r FROM inputs")
    # TIMESTAMP_NTZ times after the Los Angeles spring and autumn changes.
    for name, value, day in [("spring", "2024-03-10 12:00:00", "2024-02-29"),
                             ("autumn", "2023-11-05 12:00:00", "2023-04-30")]:
        yield (f"literal_timestamp_ntz_{name}_change_day",
               f"SELECT months_between(TIMESTAMP_NTZ '{value}', DATE '{day}') AS r")


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "date":
        return date(1970, 1, 1) + timedelta(days=int(value))
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    if kind == "timestamp_ntz":
        return datetime(1970, 1, 1) + timedelta(microseconds=int(value))
    if kind == "string":
        return value
    if kind == "boolean":
        return {"true": True, "false": False}[value]
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row or array value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.TimestampType, types.TimestampNTZType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.StringType):
        return row.getUTF8String(index).toString()
    if isinstance(data_type, types.DoubleType):
        return double_bits(row.getDouble(index))
    if isinstance(data_type, types.BooleanType):
        return str(row.getBoolean(index)).lower()
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-months-between-nullability")
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
                               "TIMESTAMP UTC-epoch microseconds and TIMESTAMP_NTZ wall-clock "
                               "microseconds as long text; DOUBLE as the hex of its IEEE 754 "
                               "bits; STRING as text; BOOLEAN as true or false text; ARRAY as a "
                               "list of its elements"),
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
