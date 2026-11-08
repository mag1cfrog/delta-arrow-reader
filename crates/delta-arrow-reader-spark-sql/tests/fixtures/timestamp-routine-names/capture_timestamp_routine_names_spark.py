"""Capture Spark resolution of the TIMESTAMP_DIFF and TIMESTAMP_ADD names (#1051).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for `timestamp_diff` and
`timestamp_add` in lower, upper and mixed case with keyword, string and unknown units, two
arguments and column arguments, beside TIMESTAMPDIFF, TIMESTAMPADD and DATEADD controls.
Spark's grammar spells the difference and the addition only as TIMESTAMPDIFF, DATEDIFF,
DATE_DIFF, TIMEDIFF, TIMESTAMPADD, DATEADD and DATE_ADD, and its registry has no
`timestamp_diff` or `timestamp_add`. Every query runs in both ANSI modes in UTC and
America/Los_Angeles. Run from the crate root in the reference environment described in
README.md; the output path must not exist.
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

# (start, end) UTC instants.
ROWS = [
    ("2024-01-01 23:00:00", "2024-01-02 01:00:00"),
    ("2024-03-10 09:30:00", "2024-03-10 11:30:00"),
    ("2024-11-03 08:30:00", "2024-11-03 09:30:00"),
]


def micros(text):
    value = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "s", "type": "timestamp", "nullable": False, "metadata": {}},
        {"name": "e", "type": "timestamp", "nullable": False, "metadata": {}}]},
    "rows": [[str(key), str(micros(start)), str(micros(end))]
             for key, (start, end) in enumerate(ROWS)]}]

PARTIAL_DAY = "TIMESTAMP '2024-01-01 23:00:00', TIMESTAMP '2024-01-02 01:00:00'"
START = "TIMESTAMP '2024-01-01 00:00:00'"


def definitions():
    for name, spelling in [("timestamp_diff", "lower"), ("TIMESTAMP_DIFF", "upper")]:
        for case, arguments in [("day", f"DAY, {PARTIAL_DAY}"), ("hour", f"HOUR, {PARTIAL_DAY}"),
                                ("string_unit", f"'DAY', {PARTIAL_DAY}"),
                                ("unknown_unit", f"FOO, {PARTIAL_DAY}"),
                                ("two_arguments", PARTIAL_DAY)]:
            yield f"literal_{spelling}_timestamp_diff_{case}", f"SELECT {name}({arguments}) AS r"
        yield (f"column_{spelling}_timestamp_diff",
               f"SELECT k, {name}(HOUR, s, e) AS r FROM inputs")
    for name, spelling in [("timestamp_add", "lower"), ("Timestamp_Add", "mixed")]:
        for case, arguments in [("day", f"DAY, 1, {START}"),
                                ("string_unit", f"'DAY', 1, {START}"),
                                ("unknown_unit", f"FOO, 1, {START}")]:
            yield f"literal_{spelling}_timestamp_add_{case}", f"SELECT {name}({arguments}) AS r"
        yield (f"column_{spelling}_timestamp_add",
               f"SELECT k, {name}(HOUR, k, s) AS r FROM inputs")
    # Named arguments.
    yield ("literal_named_arguments_timestamp_diff",
           "SELECT timestamp_diff(unit => 'DAY', "
           "startTimestamp => TIMESTAMP '2024-01-01 23:00:00', "
           "endTimestamp => TIMESTAMP '2024-01-02 01:00:00') AS r")
    yield ("literal_named_arguments_timestamp_add",
           f"SELECT timestamp_add(unit => 'DAY', quantity => 1, ts => {START}) AS r")
    # Other call positions, and an unresolved column or a qualified name beside the call (#1185).
    yield ("column_window_timestamp_diff",
           "SELECT k, timestamp_diff(DAY, s, e) OVER (ORDER BY k) AS r FROM inputs")
    yield ("column_window_timestamp_add",
           "SELECT k, timestamp_add('DAY', 1, s) OVER (PARTITION BY k) AS r FROM inputs")
    yield ("column_order_by_timestamp_diff",
           "SELECT k FROM inputs ORDER BY timestamp_diff(HOUR, s, e)")
    yield "column_sort_by_timestamp_add", "SELECT k FROM inputs SORT BY timestamp_add(DAY, 1, s)"
    yield ("column_lateral_view_timestamp_diff",
           "SELECT k, x FROM inputs LATERAL VIEW timestamp_diff(DAY, s, e) t AS x")
    yield ("column_lateral_view_timestamp_add",
           "SELECT k, x FROM inputs LATERAL VIEW timestamp_add('DAY', 1, s) t AS x")
    yield ("literal_unresolved_column_beside_timestamp_diff",
           f"SELECT nosuch, timestamp_diff(DAY, {PARTIAL_DAY}) AS r")
    yield "literal_qualified_timestamp_add", f"SELECT builtin.timestamp_add(DAY, 1, {START}) AS r"
    # Spellings Spark accepts.
    yield "literal_timestampdiff", f"SELECT timestampdiff(DAY, {PARTIAL_DAY}) AS r"
    yield "literal_timestampadd", f"SELECT timestampadd(DAY, 1, {START}) AS r"
    yield "column_timestampdiff", "SELECT k, timestampdiff(HOUR, s, e) AS r FROM inputs"
    yield "column_dateadd", "SELECT k, dateadd(HOUR, k, s) AS r FROM inputs"


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row or array value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, types.IntegerType):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.TimestampType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.StringType):
        return row.getUTF8String(index).toString()
    if isinstance(data_type, types.BooleanType):
        return row.getBoolean(index)
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-timestamp-routine-names")
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
            "value_encoding": ("INT as int text; BIGINT and TIMESTAMP UTC-epoch microseconds "
                               "as long text; STRING as text; BOOLEAN as a JSON boolean; ARRAY "
                               "as a list of its elements"),
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
