"""Capture Spark TIMESTAMPADD over full-width BIGINT quantities (#981).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for TIMESTAMPADD in every unit over
BIGINT quantities at the signed INT and BIGINT boundaries and beyond INT where a wrap would
fit, as columns and literals, in winter and summer, with NULL quantities and timestamps, a
NULL quantity beside a malformed STRING timestamp, an empty input, TIMESTAMP_NTZ and DATE
timestamps, units written in lower and mixed case, the DATEADD and DATE_ADD names, and
explicit INT casts of the quantity. Every query runs in both ANSI modes in UTC and
America/Los_Angeles. Rows are collected through the Dataset API, which runs with the
session's settings, before their typed values are read from the executed plan. Run from the
crate root in the reference environment described in README.md; the output path must not
exist.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles"]

UNITS = ["MICROSECOND", "MILLISECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH",
         "QUARTER", "YEAR"]

# 2024-01-01 and 2024-07-01 00:00:00 as UTC-epoch microseconds, and as TIMESTAMP_NTZ
# wall-clock times.
START = "1704067200000000"
SUMMER = "1719792000000000"
VALID = "2024-01-01 00:00:00"

# (q BIGINT, ts TIMESTAMP, ntz TIMESTAMP_NTZ, s STRING) per row: INT and BIGINT boundaries,
# counts beyond INT that wrap to 0 or 1, a NULL quantity beside a malformed STRING timestamp,
# NULL timestamps, and a summer row.
QUANTITIES = ["1", "-1", "2147483647", "2147483648", "-2147483649", "4294967296", "4294967297",
              "9223372036854775807", "-9223372036854775808"]
ROWS = [[str(key), q, START, START, VALID] for key, q in enumerate(QUANTITIES)] + [
    ["9", None, START, START, "not a timestamp"],
    ["10", "1", None, None, None],
    ["11", "4294967297", SUMMER, SUMMER, VALID]]


def text(value):
    return None if value is None else str(value)


SCHEMA = {"type": "struct", "fields": [
    {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
    {"name": "q", "type": "long", "nullable": True, "metadata": {}},
    {"name": "ts", "type": "timestamp", "nullable": True, "metadata": {}},
    {"name": "ntz", "type": "timestamp_ntz", "nullable": True, "metadata": {}},
    {"name": "s", "type": "string", "nullable": True, "metadata": {}}]}

INPUTS = [{"name": "quantities", "schema": SCHEMA, "rows": ROWS},
          {"name": "empty", "schema": SCHEMA, "rows": []}]

LITERAL_QUANTITIES = [("beyond_int_zero", "4294967296L"), ("largest", "9223372036854775807L"),
                      ("smallest", "-9223372036854775808L")]

OTHERS = [
    ("column_empty_day", "SELECT k, timestampadd(DAY, q, ts) AS r FROM empty"),
    ("column_empty_month", "SELECT k, timestampadd(MONTH, q, ts) AS r FROM empty"),
    ("literal_null_bigint_quantity",
     "SELECT timestampadd(DAY, CAST(NULL AS BIGINT), TIMESTAMP '2024-01-01 00:00:00') AS r"),
    ("literal_null_quantity",
     "SELECT timestampadd(DAY, NULL, TIMESTAMP '2024-01-01 00:00:00') AS r"),
    ("literal_null_timestamp",
     "SELECT timestampadd(DAY, 4294967297L, CAST(NULL AS TIMESTAMP)) AS r"),
    ("literal_explicit_int_cast",
     "SELECT timestampadd(MONTH, CAST(4294967296L AS INT), TIMESTAMP '2020-01-01 00:00:00') AS r"),
    ("column_explicit_int_cast",
     "SELECT k, timestampadd(MONTH, CAST(q AS INT), ts) AS r FROM quantities WHERE k = 5"),
    ("literal_issue_example",
     "SELECT CAST(timestampadd(MONTH, 4294967296L, TIMESTAMP '2020-01-01 00:00:00') AS STRING) "
     "AS r"),
    ("column_ntz_wrapping_days",
     "SELECT k, timestampadd(DAY, q, ntz) AS r FROM quantities WHERE k = 6"),
    ("column_ntz_wrapping_months",
     "SELECT k, timestampadd(MONTH, q, ntz) AS r FROM quantities WHERE k = 6"),
    ("column_ntz_largest_microseconds",
     "SELECT k, timestampadd(MICROSECOND, q, ntz) AS r FROM quantities WHERE k = 7"),
    ("literal_ntz_wrapping_days",
     "SELECT timestampadd(DAY, 4294967297L, TIMESTAMP_NTZ '2024-01-01 00:00:00') AS r"),
    ("literal_date_wrapping_days", "SELECT timestampadd(DAY, 4294967297L, DATE '2024-01-01') AS r"),
    ("literal_dateadd_wrapping_months",
     "SELECT dateadd(MONTH, 4294967297L, TIMESTAMP '2024-01-01 00:00:00') AS r"),
    ("literal_date_add_wrapping_months",
     "SELECT date_add(MONTH, 4294967297L, TIMESTAMP '2024-01-01 00:00:00') AS r"),
    ("column_ntz_summer_wrapping_days",
     "SELECT k, timestampadd(DAY, q, ntz) AS r FROM quantities WHERE k = 11"),
    ("literal_lowercase_day",
     "SELECT timestampadd(day, 4294967297L, TIMESTAMP '2024-01-01 00:00:00') AS r"),
    ("literal_mixed_case_month",
     "SELECT timestampadd(Month, 4294967297L, TIMESTAMP '2024-01-01 00:00:00') AS r"),
    ("column_lowercase_week",
     "SELECT k, timestampadd(week, q, ts) AS r FROM quantities WHERE k = 7"),
    ("literal_dateadd_lowercase_quarter",
     "SELECT dateadd(quarter, 9223372036854775807L, TIMESTAMP '2024-01-01 00:00:00') AS r"),
    ("column_null_quantity_invalid_string",
     "SELECT k, timestampadd(DAY, q, s) AS r FROM quantities WHERE k = 9"),
    ("column_string_timestamp",
     "SELECT k, timestampadd(DAY, q, s) AS r FROM quantities WHERE k = 6"),
    ("literal_null_quantity_invalid_string",
     "SELECT timestampadd(DAY, CAST(NULL AS BIGINT), 'not a timestamp') AS r"),
    ("literal_invalid_quantity_null_timestamp",
     "SELECT timestampadd(DAY, 'x', CAST(NULL AS TIMESTAMP)) AS r"),
]


def definitions():
    """The record names and queries, in capture order."""
    for unit in UNITS:
        for key in range(len(ROWS)):
            yield (f"column_{unit.lower()}_row{key}",
                   f"SELECT k, timestampadd({unit}, q, ts) AS r FROM quantities WHERE k = {key}")
        for name, quantity in LITERAL_QUANTITIES:
            yield (f"literal_{unit.lower()}_{name}",
                   f"SELECT timestampadd({unit}, {quantity}, TIMESTAMP '2024-01-01 00:00:00') AS r")
    yield from OTHERS


def python_value(value, kind):
    if value is None:
        return None
    if kind in ("integer", "long"):
        return int(value)
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    if kind == "timestamp_ntz":
        return datetime(1970, 1, 1) + timedelta(microseconds=int(value))
    if kind == "string":
        return value
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, types.IntegerType):
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
        # The Dataset API runs the query with the session's settings, so a failing row's
        # message renders its timestamp in the session zone; executeCollect alone does not.
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-timestampadd-quantity")
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
            schema = types.StructType.fromJson(recorded["schema"])
            spark.createDataFrame([[python_value(value, kind) for value, kind in zip(row, kinds)]
                                   for row in recorded["rows"]], schema) \
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
            "value_encoding": ("INT, BIGINT and STRING as text; TIMESTAMP UTC-epoch and "
                               "TIMESTAMP_NTZ wall-clock microseconds as text"),
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
