"""Capture Spark WEEKDAY and DAYOFWEEK (#1088).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages for WEEKDAY and DAYOFWEEK over registered DATE, TIMESTAMP, TIMESTAMP_NTZ and STRING
columns, nullable and NOT NULL, and over literals: every day of the week, years 1 and 9999, a
pre-epoch day, the day Los Angeles left local mean time, NULL, and STRING values with times,
spaces and invalid text. The TIMESTAMP instants fall at 03:30 UTC, the previous day in Los Angeles. Every query
runs in both ANSI modes in UTC and America/Los_Angeles. Run from the crate root in the
reference environment described in README.md; the output path must not exist.
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
EPOCH_DAY = date(1970, 1, 1)

DAYS = [
    # Monday 2024-03-04 through Sunday 2024-03-10, the Los Angeles spring change.
    "2024-03-04", "2024-03-05", "2024-03-06", "2024-03-07", "2024-03-08", "2024-03-09",
    "2024-03-10",
    "0001-01-01",  # A Monday.
    "9999-12-31",  # A Friday.
    "1969-12-31",  # A Wednesday.
    "1883-11-18",  # A Sunday, when Los Angeles left local mean time.
    None,
]
# Two days in NOT NULL columns.
REQUIRED_DAYS = ["2024-03-10", "2024-03-11"]
# STRING values Spark reads as dates, and ones it cannot read, each queried alone.
STRINGS = ["2024-03-10 23:59:59", " 2024-03-10 ", "2024-03-10T01:02:03Z", "2024-3-9",
           "bad", ""]


def micros(value):
    delta = value - datetime(1970, 1, 1, tzinfo=value.tzinfo)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def day_rows(days):
    rows = []
    for key, text in enumerate(days):
        if text is None:
            rows.append([str(key), None, None, None, None])
            continue
        day = date.fromisoformat(text)
        at = datetime(day.year, day.month, day.day, 3, 30)
        rows.append([str(key), str((day - EPOCH_DAY).days),
                     str(micros(at.replace(tzinfo=timezone.utc))), str(micros(at)), text])
    return rows


INPUTS = [
    {"name": "day_inputs",
     "schema": {"type": "struct", "fields": [
         {"name": name, "type": kind, "nullable": name != "k", "metadata": {}}
         for name, kind in [("k", "integer"), ("d", "date"), ("ts", "timestamp"),
                            ("tn", "timestamp_ntz"), ("s", "string")]]},
     "rows": day_rows(DAYS)},
    {"name": "required_inputs",
     "schema": {"type": "struct", "fields": [
         {"name": name, "type": kind, "nullable": False, "metadata": {}}
         for name, kind in [("k", "integer"), ("d", "date"), ("ts", "timestamp"),
                            ("tn", "timestamp_ntz"), ("s", "string")]]},
     "rows": day_rows(REQUIRED_DAYS)},
    {"name": "string_inputs",
     "schema": {"type": "struct", "fields": [
         {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
         {"name": "s", "type": "string", "nullable": True, "metadata": {}}]},
     "rows": [[str(key), text] for key, text in enumerate(STRINGS)]},
]


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "date":
        return EPOCH_DAY + timedelta(days=int(value))
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    if kind == "timestamp_ntz":
        return datetime(1970, 1, 1) + timedelta(microseconds=int(value))
    if kind == "string":
        return value
    raise AssertionError(kind)


FUNCTIONS = ["weekday", "dayofweek"]
COLUMNS = ["d", "ts", "tn", "s"]
LITERALS = [
    ("date", "DATE '2024-03-10'"),
    ("timestamp", "TIMESTAMP '2024-03-10 23:30:00'"),
    ("timestamp_utc", "TIMESTAMP '2024-03-11 03:30:00+00:00'"),
    ("timestamp_ntz", "TIMESTAMP_NTZ '2024-03-10 23:30:00'"),
    ("string", "'2024-03-10'"),
    ("first_day", "DATE '0001-01-01'"),
    ("null", "NULL"),
    # Strings Spark cannot read; under ANSI they fail while folding constants.
    ("bad", "'bad'"),
    ("empty", "''"),
    ("lowercase_t", "'2024-03-10t01:02:03'"),
]


def definitions():
    for function in FUNCTIONS:
        for column in COLUMNS:
            yield (f"{function}_{column}",
                   f"SELECT k, {function}({column}) AS r FROM day_inputs")
        for column in COLUMNS:
            yield (f"{function}_required_{column}",
                   f"SELECT k, {function}({column}) AS r FROM required_inputs")
        for key in range(len(STRINGS)):
            yield (f"{function}_string_{key}",
                   f"SELECT k, {function}(s) AS r FROM string_inputs WHERE k = {key}")
        for name, literal in LITERALS:
            yield f"{function}_literal_{name}", f"SELECT {function}({literal}) AS r"
    yield ("filter_sunday",
           "SELECT k FROM day_inputs WHERE weekday(d) = 6 AND dayofweek(d) = 1")
    yield ("group_weekday",
           "SELECT weekday(ts) AS w, count(*) AS n FROM day_inputs GROUP BY weekday(ts)")


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
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
    """Return the outermost error condition and the innermost Java exception class."""
    condition = getattr(error, "getCondition", lambda: None)()
    cause = getattr(error, "java_exception", None)
    if cause is None:
        cause = getattr(error, "_origin", None)
    exception = None
    while cause is not None:
        try:
            exception = cause.getClass().getName()
            if condition is None:
                condition = cause.getCondition()
        except Py4JError:
            pass
        cause = cause.getCause()
    return condition, exception


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
        condition, exception = error_details(error, Py4JError)
        record["expected"] = {"status": "error", "condition": condition,
                              "exception": exception, "message": str(error).splitlines()[0]}
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-weekday")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for recorded in INPUTS:
            schema = types.StructType.fromJson(recorded["schema"])
            kinds = [field["type"] for field in recorded["schema"]["fields"]]
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
            "value_encoding": ("INT and DATE days as int; TIMESTAMP UTC-epoch and TIMESTAMP_NTZ "
                               "wall-clock microseconds as long; STRING as text"),
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
