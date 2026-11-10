"""Capture Spark's casts of STRING to DATE, TIMESTAMP and TIMESTAMP_NTZ (#1218, #1237).

Records analyzed, optimized and executed schemas, typed internal-row values and failing stages,
with the innermost Java exception of a failure, for CAST and TRY_CAST of STRING columns to DATE,
TIMESTAMP and TIMESTAMP_NTZ, for DATE_ADD and DATE_SUB of STRING dates, for DATE_FORMAT,
TO_TIMESTAMP with and without a format, TRY_TO_TIMESTAMP, TO_TIMESTAMP_NTZ, UNIX_TIMESTAMP and
formatted TO_UNIX_TIMESTAMP of STRING arguments, and for the implicit conversions of STRING
beside a DATE or TIMESTAMP in comparisons, NULLIF, DAYOFWEEK, WEEKDAY, WEEKOFYEAR, COALESCE, IF
and CASE: over a NOT NULL column of values Spark reads, padded, with a time, with a `T`
separator and at a Los Angeles DST gap, a NOT NULL column with malformed values and a nullable
column with NULLs. DAR's harness runs the same queries over dictionary-encoded, viewed and large
copies of the columns. Every query runs in both ANSI modes in UTC and America/Los_Angeles. Run
from the crate root in the reference environment described in README.md; the output path must
not exist.
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

# (STRING Spark reads as a DATE and a TIMESTAMP, STRING malformed after the first row, nullable
# STRING)
ROWS = [
    ("2024-05-17", "2024-05-17", "2024-05-17"),
    (" 2024-01-02 ", "garbage", None),
    ("2024-03-10 02:30:00", "2024-13-01", "2024-01-02T10:00:00"),
    ("2024-02-29T10:00:00", "", None),
]
INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "sv", "type": "string", "nullable": False, "metadata": {}},
        {"name": "sb", "type": "string", "nullable": False, "metadata": {}},
        {"name": "sn", "type": "string", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), valid, malformed, nullable]
             for key, (valid, malformed, nullable) in enumerate(ROWS)]}]

COLUMNS = [("valid", "sv"), ("malformed", "sb"), ("nullable", "sn")]
TARGETS = [("date", "DATE"), ("timestamp", "TIMESTAMP"), ("timestamp_ntz", "TIMESTAMP_NTZ")]


def definitions():
    for name, column in COLUMNS:
        for target_name, target in TARGETS:
            yield (f"cast_{name}_{target_name}",
                   f"SELECT k, CAST({column} AS {target}) AS r FROM inputs")
            yield (f"try_cast_{name}_{target_name}",
                   f"SELECT k, TRY_CAST({column} AS {target}) AS r FROM inputs")
        for function in ["date_add", "date_sub"]:
            yield (f"{function}_{name}", f"SELECT k, {function}({column}, 1) AS r FROM inputs")
        # Spark converts the STRING to DATE or TIMESTAMP implicitly beside one.
        for comparison_name, comparison in [
                ("equal_date", f"{column} = DATE '2024-05-17'"),
                ("less_date", f"{column} < DATE '2024-03-01'"),
                ("between_dates", f"{column} BETWEEN DATE '2024-01-01' AND DATE '2024-03-31'"),
                ("null_safe_equal_date", f"{column} <=> DATE '2024-05-17'"),
                ("date_equal", f"DATE '2024-05-17' = {column}"),
                ("equal_timestamp", f"{column} = TIMESTAMP '2024-05-17 00:00:00'"),
                ("nullif_date", f"NULLIF({column}, DATE '2024-05-17')"),
                ("nullif_timestamp", f"NULLIF({column}, TIMESTAMP '2024-05-17 00:00:00')"),
                ("nullif_timestamp_ntz",
                 f"NULLIF({column}, TIMESTAMP_NTZ '2024-05-17 00:00:00')")]:
            yield (f"{comparison_name}_{name}", f"SELECT k, {comparison} AS r FROM inputs")
        yield (f"filter_equal_date_{name}",
               f"SELECT k FROM inputs WHERE {column} = DATE '2024-05-17'")
        for function in ["dayofweek", "weekday", "weekofyear"]:
            yield (f"{function}_{name}", f"SELECT k, {function}({column}) AS r FROM inputs")
        # Spark reads the STRING as a TIMESTAMP in these functions (#1237).
        for function_name, call in [
                ("date_format", f"date_format({column}, 'yyyy-MM-dd HH:mm')"),
                ("to_timestamp", f"to_timestamp({column})"),
                ("to_timestamp_format", f"to_timestamp({column}, 'yyyy-MM-dd')"),
                ("try_to_timestamp", f"try_to_timestamp({column})"),
                ("to_timestamp_ntz", f"to_timestamp_ntz({column})"),
                ("unix_timestamp", f"unix_timestamp({column})"),
                ("to_unix_timestamp_format", f"to_unix_timestamp({column}, 'yyyy-MM-dd')")]:
            yield (f"{function_name}_{name}", f"SELECT k, {call} AS r FROM inputs")
        for result_name, result in [
                ("coalesce_date", f"coalesce({column}, DATE '2000-01-01')"),
                ("if_date", f"IF(k > 1, {column}, DATE '2000-01-01')"),
                ("case_date", f"CASE WHEN k > 1 THEN DATE '2000-01-01' ELSE {column} END")]:
            yield (f"{result_name}_{name}", f"SELECT k, {result} AS r FROM inputs")


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
    spark = (SparkSession.builder.master("local[1]").appName("dar-encoded-string-temporal-casts")
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
