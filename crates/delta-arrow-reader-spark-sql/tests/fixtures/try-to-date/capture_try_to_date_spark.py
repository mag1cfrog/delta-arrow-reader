"""Capture Spark's TRY_TO_DATE over TIMESTAMP, DATE, TIMESTAMP_NTZ and STRING arguments (#1017).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for TRY_TO_DATE with and without a
format over NOT NULL TIMESTAMP, DATE, TIMESTAMP_NTZ and STRING columns and literals, for
TRY_TO_DATE and TO_DATE over a nullable STRING column, and for TRY_TO_DATE and TO_DATE with the
wrong number of arguments. The temporal contract references (#153) record TRY_TO_DATE and
TO_DATE over STRING values; these add the other argument types and the nullability of NOT NULL
inputs. A few cases owned by other issues are recorded too. Every query runs in both ANSI modes
in UTC and America/Los_Angeles. Run from the crate root in the reference environment described
in README.md; the output path must not exist.
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

# (TIMESTAMP instant in UTC, DATE, NOT NULL STRING, TIMESTAMP_NTZ, nullable STRING)
ROWS = [
    ("2024-01-02 23:30:00", "2024-01-02", "2024-01-02", "2024-01-02 23:30:00", "2024-01-02"),
    ("2024-03-10 10:30:00", "2024-03-10", "not a date", "2024-03-10 02:30:00", "not a date"),
    ("1969-12-31 23:59:59.999999", "1969-12-31", "1969-12-31", "1969-12-31 23:59:59.999999",
     "2024-02-30"),
    # Los Angeles is still on 2024-01-02 at this instant.
    ("2024-01-03 03:00:00", "2024-01-03", "2024-01-03", "2024-01-03 03:00:00", None),
]


def micros(text):
    value = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def days(text):
    return (date.fromisoformat(text) - date(1970, 1, 1)).days


INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": False, "metadata": {}},
        {"name": "dt", "type": "date", "nullable": False, "metadata": {}},
        {"name": "t", "type": "string", "nullable": False, "metadata": {}},
        {"name": "tn", "type": "timestamp_ntz", "nullable": False, "metadata": {}},
        {"name": "s", "type": "string", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), str(micros(instant)), str(days(day)), text, str(micros(local)), other]
             for key, (instant, day, text, local, other) in enumerate(ROWS)]}]

LITERALS = {"ts": "TIMESTAMP '2024-01-03 03:00:00'", "dt": "DATE '2024-01-02'",
            "t": "'2024-01-02'", "tn": "TIMESTAMP_NTZ '2024-01-03 03:00:00'"}
FORMATS = [("", ""), ("_format", ", 'yyyy-MM-dd'")]


def definitions():
    for column, name in [("ts", "timestamp"), ("dt", "date"), ("t", "string"),
                         ("tn", "timestamp_ntz")]:
        for format_name, format_argument in FORMATS:
            yield (f"column_try_to_date_{name}{format_name}",
                   f"SELECT k, try_to_date({column}{format_argument}) AS r FROM inputs")
            yield (f"literal_try_to_date_{name}{format_name}",
                   f"SELECT try_to_date({LITERALS[column]}{format_argument}) AS r")
    # A nullable STRING column, also for TO_DATE: DAR's sources give STRING columns other
    # encodings.
    for function in ["try_to_date", "to_date"]:
        for format_name, format_argument in FORMATS:
            yield (f"nullable_{function}_string{format_name}",
                   f"SELECT k, {function}(s{format_argument}) AS r FROM inputs")
    # The wrong number of arguments.
    for function in ["try_to_date", "to_date"]:
        yield f"{function}_no_arguments", f"SELECT {function}() AS r"
        yield (f"{function}_three_arguments",
               f"SELECT {function}('2024-01-02', 'yyyy-MM-dd', 'x') AS r")
    # Owned elsewhere: arguments that are not STRING, DATE or TIMESTAMP values, and TO_DATE's
    # formatted nullability over a DATE without ANSI mode.
    yield "literal_try_to_date_int", "SELECT try_to_date(1) AS r"
    yield "literal_try_to_date_boolean", "SELECT try_to_date(true) AS r"
    yield "literal_try_to_date_null_format", "SELECT try_to_date(NULL, 'yyyy-MM-dd') AS r"
    yield "literal_try_to_date_array", "SELECT try_to_date(array('2024-01-02')) AS r"
    yield "column_to_date_date_format", "SELECT k, to_date(dt, 'yyyy-MM-dd') AS r FROM inputs"


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
    spark = (SparkSession.builder.master("local[1]").appName("dar-try-to-date")
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
                               "microseconds as long text; STRING as text; BOOLEAN as true or "
                               "false text; ARRAY as a list of its elements"),
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
