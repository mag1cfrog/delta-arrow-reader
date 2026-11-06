"""Capture Spark TO_UTC_TIMESTAMP and FROM_UTC_TIMESTAMP around DST transitions (#154).

Records analyzed, optimized and executed schemas, typed internal-row values and
failing stages for both functions with TIMESTAMP_NTZ, TIMESTAMP, STRING and DATE
inputs, literal and registered-column values and zones, around the 2024 Los Angeles
spring gap and fall overlap and the Havana midnight changes, with UTC and
fixed-offset controls, NULL and invalid zones. Every query runs in both ANSI modes
with UTC, America/Los_Angeles and America/Havana session zones. Run from the crate root in the reference environment described in README.md;
the output path must not exist.
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
HAVANA = "America/Havana"
LOS_ANGELES = "America/Los_Angeles"
SESSION_ZONES = ["UTC", LOS_ANGELES, HAVANA]
TABLE = "wall_clock_times"

# (name, wall-clock text, per-row zone).
WALL_CLOCK_TIMES = [
    ("before_gap", "2024-03-10 01:59:59.999999", LOS_ANGELES),
    ("gap_start", "2024-03-10 02:00:00", LOS_ANGELES),
    ("gap", "2024-03-10 02:30:00", LOS_ANGELES),
    ("gap_end", "2024-03-10 02:59:59.999999", LOS_ANGELES),
    ("after_gap", "2024-03-10 03:00:00", LOS_ANGELES),
    ("before_overlap", "2024-11-03 00:59:59.999999", LOS_ANGELES),
    ("overlap_start", "2024-11-03 01:00:00", LOS_ANGELES),
    ("overlap", "2024-11-03 01:30:00", LOS_ANGELES),
    ("overlap_end", "2024-11-03 01:59:59.999999", LOS_ANGELES),
    ("after_overlap", "2024-11-03 02:00:00", LOS_ANGELES),
    # UTC wall-clock times of the transitions, for FROM_UTC_TIMESTAMP.
    ("utc_before_spring", "2024-03-10 09:59:59.999999", LOS_ANGELES),
    ("utc_spring", "2024-03-10 10:00:00", LOS_ANGELES),
    ("utc_first_overlap", "2024-11-03 08:30:00", LOS_ANGELES),
    ("utc_before_fall", "2024-11-03 08:59:59.999999", LOS_ANGELES),
    ("utc_fall", "2024-11-03 09:00:00", LOS_ANGELES),
    ("utc_second_overlap", "2024-11-03 09:30:00", LOS_ANGELES),
    # Havana skips midnight on 2024-03-10 and repeats it on 2024-11-03.
    ("havana_gap", "2024-03-10 00:30:00", HAVANA),
    ("havana_overlap", "2024-11-03 00:30:00", HAVANA),
    ("ordinary", "2024-01-02 13:04:05.123456", "UTC"),
    ("pre_epoch", "1969-12-31 23:59:59.999999", "+05:30"),
    ("local_mean_time", "1800-01-01 00:00:00", LOS_ANGELES),
]
ZONES = [LOS_ANGELES, HAVANA, "UTC", "+05:30"]
FUNCTIONS = ["TO_UTC_TIMESTAMP", "FROM_UTC_TIMESTAMP"]


def literal_forms(text):
    """Literal inputs for one wall-clock text, keyed by input kind."""
    return [("ntz", f"TIMESTAMP_NTZ '{text}'"), ("timestamp", f"TIMESTAMP '{text}'"),
            ("string", f"'{text}'"), ("date", f"DATE '{text[:10]}'")]


def definitions():
    null_key = len(WALL_CLOCK_TIMES)
    for function in FUNCTIONS:
        name = function.lower()
        for local, text, _ in WALL_CLOCK_TIMES:
            for kind, value in literal_forms(text):
                for zone in ZONES:
                    yield {"id": f"{name}_{local}_{kind}_{zone}", "function": name, "input": local,
                           "kind": kind, "zone": zone,
                           "sql": f"SELECT {function}({value}, '{zone}') AS r"}
        for kind, column in [("ntz", "ntz"), ("timestamp_column", "ts"), ("string", "s"),
                             ("date", "d")]:
            for zone in ZONES:
                yield {"id": f"{name}_column_{kind}_{zone}", "function": name, "kind": kind,
                       "zone": zone,
                       "sql": f"SELECT k, {function}({column}, '{zone}') AS r FROM {TABLE}"}
        yield {"id": f"{name}_column_zone", "function": name, "kind": "ntz",
               "sql": f"SELECT k, {function}(ntz, z) AS r FROM {TABLE}"}
        yield {"id": f"{name}_column_invalid_zone", "function": name, "kind": "ntz",
               "sql": f"SELECT k, {function}(ntz, CASE WHEN k = 2 THEN 'Not/AZone' ELSE z END) "
                      f"AS r FROM {TABLE}"}
        yield {"id": f"{name}_column_null_rows", "function": name, "kind": "ntz",
               "sql": f"SELECT k, {function}(ntz, z) AS r FROM {TABLE} WHERE k = {null_key}"}
        for probe, sql in [
            ("date", f"{function}(DATE '2024-03-10', '{LOS_ANGELES}')"),
            ("null_zone", f"{function}(TIMESTAMP_NTZ '2024-03-10 02:30:00', CAST(NULL AS STRING))"),
            ("null_value", f"{function}(CAST(NULL AS TIMESTAMP_NTZ), '{LOS_ANGELES}')"),
            ("invalid_zone", f"{function}(TIMESTAMP_NTZ '2024-03-10 02:30:00', 'Not/AZone')"),
            ("empty_zone", f"{function}(TIMESTAMP_NTZ '2024-03-10 02:30:00', '')"),
            ("legacy_zone", f"{function}(TIMESTAMP_NTZ '2024-03-10 02:30:00', 'PST')"),
            ("unaliased", f"{function}(TIMESTAMP_NTZ '2024-11-03 01:30:00', '{LOS_ANGELES}')"),
        ]:
            alias = "" if probe == "unaliased" else " AS r"
            yield {"id": f"{name}_{probe}", "function": name, "sql": f"SELECT {sql}{alias}"}
    # The frozen experiment's two queries, verbatim.
    yield {"id": "frozen_dates_dst_gap", "function": "to_utc_timestamp",
           "sql": "SELECT TO_UTC_TIMESTAMP(CAST('2024-03-10 02:30:00' AS TIMESTAMP_NTZ), "
                  "'America/Los_Angeles')"}
    yield {"id": "frozen_dates_dst_overlap", "function": "to_utc_timestamp",
           "sql": "SELECT TO_UTC_TIMESTAMP(CAST('2024-11-03 01:30:00' AS TIMESTAMP_NTZ), "
                  "'America/Los_Angeles')"}


def micros(text, aware):
    """Encode a wall-clock text as epoch microseconds without float arithmetic."""
    value = datetime.fromisoformat(text)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc if aware else None)
    if aware:
        value = value.replace(tzinfo=timezone.utc)
    delta = value - epoch
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def input_table():
    """The registered rows: the wall-clock text as TIMESTAMP_NTZ, as a UTC instant and as
    STRING, with a per-row zone and the text's DATE, then an all-NULL row."""
    epoch = date(1970, 1, 1)
    rows = [[str(key), str(micros(text, False)), str(micros(text, True)), text, zone,
             str((date.fromisoformat(text[:10]) - epoch).days)]
            for key, (_, text, zone) in enumerate(WALL_CLOCK_TIMES)]
    rows.append([str(len(WALL_CLOCK_TIMES)), None, None, None, None, None])

    def field(name, kind, nullable=True):
        return {"name": name, "type": kind, "nullable": nullable, "metadata": {}}

    return {"name": TABLE,
            "schema": {"type": "struct", "fields": [
                field("k", "integer", False), field("ntz", "timestamp_ntz"),
                field("ts", "timestamp"), field("s", "string"), field("z", "string"),
                field("d", "date")]},
            "rows": rows}


def python_value(value, kind):
    """Build the exact Python value Spark converts back to the recorded encoding."""
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
    return value


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.TimestampType, types.TimestampNTZType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.StringType):
        return row.getUTF8String(index).toString()
    raise AssertionError(f"unexpected result type {data_type}")


def error_details(error, Py4JError):
    """Return the innermost error condition, or the Java exception class without one."""
    condition = getattr(error, "getCondition", lambda: None)()
    cause = getattr(error, "java_exception", None)
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-utc-timestamp-conversions")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        table = input_table()
        schema = types.StructType.fromJson(table["schema"])
        kinds = [field["type"] for field in table["schema"]["fields"]]
        spark.createDataFrame([[python_value(value, kind) for value, kind in zip(row, kinds)]
                               for row in table["rows"]], schema).createOrReplaceTempView(TABLE)
        # The registered values must be exactly the recorded encodings.
        frame = spark.table(TABLE)
        stored = [[encode_value(row, index, field.dataType, types)
                   for index, field in enumerate(frame.schema.fields)]
                  for row in frame._jdf.queryExecution().executedPlan().executeCollect()]
        assert stored == table["rows"], stored
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
            "inputs": [table],
        }
        records = [header]
        cases = list(definitions())
        assert len({case["id"] for case in cases}) == len(cases)
        for zone in SESSION_ZONES:
            spark.conf.set("spark.sql.session.timeZone", zone)
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for case in cases:
                    records.append({**case, "timezone": zone, "ansi": ansi,
                                    **capture(spark, case["sql"], types, Py4JError)})
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
