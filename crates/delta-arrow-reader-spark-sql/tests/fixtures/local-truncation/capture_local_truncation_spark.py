"""Capture Spark DATE_TRUNC and field extraction in zones whose offsets have seconds (#1089).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages for DATE_TRUNC to every unit, the MINUTE and SECOND fields and DATE_FORMAT with
seconds, over a registered TIMESTAMP column and literals. The instants fall in offsets with
seconds: Monrovia's -00:44:30 in 1970, Dublin's -00:25:21 in 1910, Kathmandu's +05:41:16 in
1900 and Los Angeles' local mean time, -07:52:58, in 1800; two instants whose minutes repeat
when Los Angeles left local mean time on 1883-11-18, one on each side; two whose minutes
were skipped, in Berlin in 1893 and Monrovia in 1972; a 2024 instant and NULL. Every query
runs in both ANSI modes in UTC, America/Los_Angeles, Africa/Monrovia, Europe/Dublin,
Asia/Kathmandu and Europe/Berlin; MINUTE also runs in EST, which Java reads as -05:00. Run
from the crate root in the reference environment described in README.md; the output path
must not exist.
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
ZONES = ["UTC", "America/Los_Angeles", "Africa/Monrovia", "Europe/Dublin", "Asia/Kathmandu",
         "Europe/Berlin"]
# Java's fixed -05:00, which the date library links to Panama, for MINUTE only.
ZONE_ID_SESSIONS = ["EST"]
ZONE_ID_CASES = ["column_trunc_minute"]

INSTANTS = [
    "1970-06-15 12:34:56.789123",  # Monrovia -00:44:30.
    "1910-06-15 12:34:56.789123",  # Dublin -00:25:21.
    "1900-06-15 12:34:56.789123",  # Kathmandu +05:41:16.
    "1800-06-15 12:34:56.789123",  # Los Angeles -07:52:58.
    # 12:00:30 PST: its local minute 12:00 repeats, first in local mean time.
    "1883-11-18 20:00:30",
    # 12:06:32 local mean time, the earlier side of the repeated minute 12:06.
    "1883-11-18 19:59:30",
    # 00:06:45 CET: Berlin skipped 00:00:00-00:06:32 on 1893-04-01, leaving local mean time,
    # so the start of the minute, 00:06, was skipped.
    "1893-03-31 23:06:45",
    # 00:44:45 GMT: Monrovia skipped 00:00:00-00:44:30 on 1972-01-07, including 00:44.
    "1972-01-07 00:44:45",
    "2024-01-02 13:04:05.123456",
    None,
]


def micros(text):
    value = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


INPUTS = [{
    "name": "instants",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), None if text is None else str(micros(text))]
             for key, text in enumerate(INSTANTS)]}]

UNITS = ["YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND", "MILLISECOND",
         "MICROSECOND"]
EXPRESSIONS = [(f"trunc_{unit.lower()}", f"date_trunc('{unit}', ts)") for unit in UNITS] + [
    ("minute", "minute(ts)"),
    ("second", "second(ts)"),
    ("date_format", "date_format(ts, 'yyyy-MM-dd HH:mm:ss.SSS')"),
]
LITERALS = [
    # 04:41:58.789 local mean time in Los Angeles.
    ("minute_local_mean_time", "date_trunc('MINUTE', TIMESTAMP '1800-06-15 04:41:58.789')"),
    ("minute_utc_text", "date_trunc('MINUTE', TIMESTAMP '1970-06-15 12:34:56.789+00:00')"),
    ("hour_utc_text", "date_trunc('HOUR', TIMESTAMP '1970-06-15 12:34:56.789+00:00')"),
    ("minute_null", "date_trunc('MINUTE', CAST(NULL AS TIMESTAMP))"),
]


def definitions():
    for name, expression in EXPRESSIONS:
        yield f"column_{name}", f"SELECT k, {expression} AS r FROM instants"
    for name, expression in LITERALS:
        yield f"literal_{name}", f"SELECT {expression} AS r"


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.TimestampType, types.TimestampNTZType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.BooleanType):
        return "true" if row.getBoolean(index) else "false"
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-local-truncation")
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
            "value_encoding": ("INT as int; TIMESTAMP UTC-epoch microseconds as long; "
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
        zone_id_cases = [(name, sql) for name, sql in cases if name in ZONE_ID_CASES]
        assert len(zone_id_cases) == len(ZONE_ID_CASES)
        for zone in ZONE_ID_SESSIONS:
            spark.conf.set("spark.sql.session.timeZone", zone)
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for name, sql in zone_id_cases:
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
