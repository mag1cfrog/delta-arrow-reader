"""Capture Spark's argument checks of UNIX_DATE (#1188).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for UNIX_DATE over NOT NULL and nullable
DATE columns, DATE literals across the DATE range, and NULL, which Spark accepts, and over
TIMESTAMP, TIMESTAMP_NTZ, STRING, TIME, INT, BIGINT, DOUBLE, DECIMAL, BOOLEAN, BINARY,
interval and ARRAY arguments, as literals, typed NULLs and columns, in projections, filters and
an unreached filter, which Spark rejects during analysis without an implicit cast. Every query
runs in both ANSI modes in UTC and America/Los_Angeles. Run from the crate root in the reference
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

# (DATE, TIMESTAMP as a UTC instant, TIMESTAMP_NTZ, STRING, INT); the nullable copies are NULL
# in the last row.
ROWS = [
    ("1970-01-01", "1970-01-01 00:00:00", "1970-01-01 00:00:00", "1970-01-01", 0),
    ("1969-12-31", "1969-12-31 23:59:59.999999", "1969-12-31 23:59:59.999999", "1969-12-31", -1),
    ("0001-01-01", "0001-01-01 00:00:00", "0001-01-01 00:00:00", "0001-01-01", -719162),
    ("9999-12-31", "9999-12-31 23:59:59.999999", "9999-12-31 23:59:59.999999", "9999-12-31",
     2932896),
    ("2024-03-10", "2024-03-10 10:30:00", "2024-03-10 02:30:00", "2024-03-10", 19792),
]


def micros(text):
    value = datetime.fromisoformat(text)
    delta = value - datetime(1970, 1, 1)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def days(text):
    return (date.fromisoformat(text) - date(1970, 1, 1)).days


LAST = len(ROWS) - 1
INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "dr", "type": "date", "nullable": False, "metadata": {}},
        {"name": "d", "type": "date", "nullable": True, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": False, "metadata": {}},
        {"name": "tn", "type": "timestamp_ntz", "nullable": False, "metadata": {}},
        {"name": "t", "type": "string", "nullable": True, "metadata": {}},
        {"name": "i", "type": "integer", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), str(days(day)), None if key == LAST else str(days(day)),
              str(micros(instant)), str(micros(local)), None if key == LAST else text,
              None if key == LAST else str(number)]
             for key, (day, instant, local, text, number) in enumerate(ROWS)]}]

# Arguments Spark rejects, as literals.
REJECTED = [
    ("timestamp", "TIMESTAMP '2024-01-02 03:04:05'"),
    ("timestamp_ntz", "TIMESTAMP_NTZ '2024-01-02 03:04:05'"),
    ("string", "'2024-01-02'"),
    ("time", "TIME '01:02:03'"),
    ("int", "19724"),
    ("bigint", "19724L"),
    ("double", "1.5D"),
    ("decimal", "1.5"),
    ("boolean", "true"),
    ("binary", "X'01'"),
    ("day_interval", "INTERVAL '1' DAY"),
    ("month_interval", "INTERVAL '1' MONTH"),
    ("array", "array(DATE '2024-01-02')"),
    ("null_string", "CAST(NULL AS STRING)"),
    ("null_timestamp", "CAST(NULL AS TIMESTAMP)"),
    ("null_int", "CAST(NULL AS INT)"),
]


def definitions():
    # Accepted: DATE and NULL.
    yield "column_required_date", "SELECT k, unix_date(dr) AS r FROM inputs"
    yield "column_nullable_date", "SELECT k, unix_date(d) AS r FROM inputs"
    yield "column_date_filter", "SELECT k FROM inputs WHERE unix_date(dr) < 0"
    for name, value in [("date", "2024-01-02"), ("first_date", "0001-01-01"),
                        ("last_date", "9999-12-31"), ("date_before_epoch", "1969-12-31")]:
        yield f"literal_{name}", f"SELECT unix_date(DATE '{value}') AS r"
    yield "literal_null", "SELECT unix_date(NULL) AS r"
    yield "literal_null_date", "SELECT unix_date(CAST(NULL AS DATE)) AS r"
    # Rejected during analysis.
    for name, value in REJECTED:
        yield f"literal_{name}", f"SELECT unix_date({value}) AS r"
    for name, column in [("timestamp", "ts"), ("timestamp_ntz", "tn"), ("string", "t"),
                         ("int", "i")]:
        yield f"column_{name}", f"SELECT k, unix_date({column}) AS r FROM inputs"
    yield "column_string_filter", "SELECT k FROM inputs WHERE unix_date(t) > 0"
    yield "column_string_unreached", "SELECT k, unix_date(t) AS r FROM inputs WHERE false"


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
    spark = (SparkSession.builder.master("local[1]").appName("dar-unix-date-arguments")
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
            "value_encoding": ("INT and DATE days since the epoch as int text; TIMESTAMP "
                               "microseconds since the UTC epoch and TIMESTAMP_NTZ wall-clock "
                               "microseconds as long text; STRING as text"),
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
