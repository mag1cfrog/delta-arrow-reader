"""Capture Spark TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS in a session time zone
(#1095).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages for the three functions over registered INT, BIGINT, DOUBLE and DECIMAL columns,
DOUBLE and DECIMAL seconds cast to BIGINT, and literals, and for zone-dependent expressions over their results: HOUR, MINUTE, DATE_FORMAT,
CAST to STRING, DATE and TIMESTAMP_NTZ, and a comparison with a TIMESTAMP literal, with
UNIX_MICROS as a zone-independent control. The instants include Los Angeles DST changes,
local mean time before 1883, the Kathmandu offset change, a negative epoch, years 1 and 9999,
the largest values that do not overflow, overflowing values and NULL. Every query runs in both
ANSI modes in UTC, America/Los_Angeles and Asia/Kathmandu; HOUR also runs in the zone IDs Z,
PST and +05:45. Run from the crate root in the reference environment described in README.md;
the output path must not exist.
"""
import argparse
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles", "Asia/Kathmandu"]
# Zone IDs Spark accepts, two of which Arrow cannot parse, for HOUR only.
ZONE_ID_SESSIONS = ["Z", "PST", "+05:45"]
ZONE_ID_CASES = ["seconds_hour", "millis_hour", "micros_hour"]

# Seconds since the epoch, with the UTC instant each names.
SECONDS = [
    1704200645,  # 2024-01-02 13:04:05: 05:04 in Los Angeles, 18:49 in Kathmandu.
    1710064799,  # 2024-03-10 09:59:59, before the Los Angeles spring gap.
    1710064800,  # 2024-03-10 10:00:00, after it.
    1730622600,  # 2024-11-03 08:30:00, 01:30 PDT.
    1730626200,  # 2024-11-03 09:30:00, 01:30 PST.
    -1,  # 1969-12-31 23:59:59.
    -2717640001,  # 1883-11-18 19:59:59, Los Angeles local mean time (-07:52:58).
    -2717640000,  # 1883-11-18 20:00:00, the first instant in PST.
    -5350363200,  # 1800-06-15 12:00:00.
    504901799,  # 1985-12-31 18:29:59, Kathmandu's last instant at +05:30.
    -62135596800,  # 0001-01-01 00:00:00.
    253402300799,  # 9999-12-31 23:59:59.
    None,
]
# Fractions added to each instant: milliseconds for TIMESTAMP_MILLIS, microseconds for
# TIMESTAMP_MICROS, and the same microseconds as a DOUBLE and DECIMAL fraction of a second.
MILLIS_FRACTION = 123
MICROS_FRACTION = 123456


def inputs_table():
    rows = []
    for key, seconds in enumerate(SECONDS):
        if seconds is None:
            rows.append([str(key), None, None, None, None, None, None])
            continue
        micros = seconds * 1_000_000 + MICROS_FRACTION
        rows.append([
            str(key),
            str(seconds) if -2**31 <= seconds < 2**31 else None,
            str(seconds),
            str(seconds * 1_000 + MILLIS_FRACTION),
            str(micros),
            # The DOUBLE nearest to the fractional seconds, as its IEEE 754 bits.
            struct.pack(">d", micros / 1_000_000).hex(),
            format(Decimal(micros).scaleb(-6), "f"),
        ])
    return {"name": "epoch_inputs",
            "schema": {"type": "struct", "fields": [
                {"name": name, "type": kind, "nullable": name != "k", "metadata": {}}
                for name, kind in [("k", "integer"), ("si", "integer"), ("s", "long"),
                                   ("ms", "long"), ("us", "long"), ("d", "double"),
                                   ("dec", "decimal(20,6)")]]},
            "rows": rows}


def python_value(value, kind):
    if value is None:
        return None
    if kind in ("integer", "long"):
        return int(value)
    if kind == "double":
        return struct.unpack(">d", bytes.fromhex(value))[0]
    if kind.startswith("decimal"):
        return Decimal(value)
    raise AssertionError(kind)


FUNCTIONS = [
    ("seconds", "timestamp_seconds(s)"),
    ("seconds_int", "timestamp_seconds(si)"),
    ("seconds_double", "timestamp_seconds(d)"),
    ("seconds_decimal", "timestamp_seconds(dec)"),
    ("millis", "timestamp_millis(ms)"),
    ("micros", "timestamp_micros(us)"),
    # The DOUBLE and DECIMAL seconds truncated to whole seconds first.
    ("whole_double", "timestamp_seconds(CAST(d AS BIGINT))"),
    ("whole_decimal", "timestamp_seconds(CAST(dec AS BIGINT))"),
]

EXPRESSIONS = [
    ("value", "{t}"),
    ("hour", "hour({t})"),
    ("minute", "minute({t})"),
    ("date_format", "date_format({t}, 'yyyy-MM-dd HH:mm:ss.SSSSSS XXX')"),
    ("cast_string", "CAST({t} AS STRING)"),
    ("cast_date", "CAST({t} AS DATE)"),
    ("cast_ntz", "CAST({t} AS TIMESTAMP_NTZ)"),
    ("equals_literal", "{t} = TIMESTAMP '2024-01-02 05:04:05'"),
    ("unix_micros", "unix_micros({t})"),
]

LITERALS = [
    ("seconds_hour", "hour(timestamp_seconds(1704200645))"),
    ("seconds_cast_string", "CAST(timestamp_seconds(1704200645) AS STRING)"),
    ("seconds_date_format",
     "date_format(timestamp_seconds(-2717640001), 'yyyy-MM-dd HH:mm:ss XXX')"),
    ("seconds_fraction", "timestamp_seconds(1704200645.5)"),
    ("seconds_double", "timestamp_seconds(CAST(1704200645.25 AS DOUBLE))"),
    ("seconds_null", "timestamp_seconds(NULL)"),
    ("millis_hour", "hour(timestamp_millis(1704200645123))"),
    ("micros_hour", "hour(timestamp_micros(1704200645123456))"),
    ("micros_cast_string", "CAST(timestamp_micros(-1) AS STRING)"),
    # 2023-12-31 23:59:59 UTC, which is 2023-12-31 in Los Angeles and 2024-01-01 in Kathmandu.
    ("seconds_cast_date", "CAST(timestamp_seconds(1704067199) AS DATE)"),
    ("millis_cast_date", "CAST(timestamp_millis(1704067199000) AS DATE)"),
    ("micros_cast_date", "CAST(timestamp_micros(1704067199000000) AS DATE)"),
    # The largest values that do not overflow, and the next ones.
    ("seconds_max", "unix_micros(timestamp_seconds(9223372036854))"),
    ("seconds_overflow", "timestamp_seconds(9223372036855)"),
    ("millis_max", "unix_micros(timestamp_millis(9223372036854775))"),
    ("millis_overflow", "timestamp_millis(9223372036854776)"),
    ("micros_max", "unix_micros(timestamp_micros(9223372036854775807))"),
]


def definitions():
    for prefix, function in FUNCTIONS:
        for name, expression in EXPRESSIONS:
            yield (f"{prefix}_{name}",
                   f"SELECT k, {expression.format(t=function)} AS r FROM epoch_inputs")
    for name, expression in LITERALS:
        yield f"literal_{name}", f"SELECT {expression} AS r"


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
    if isinstance(data_type, types.DoubleType):
        return struct.pack(">d", row.getDouble(index)).hex()
    if isinstance(data_type, types.DecimalType):
        decimal = row.getDecimal(index, data_type.precision, data_type.scale)
        return format(decimal.toJavaBigDecimal(), "f")
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-timestamp-from-epoch")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        recorded = inputs_table()
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
            "value_encoding": ("INT and DATE days as int; BIGINT, TIMESTAMP UTC-epoch and "
                               "TIMESTAMP_NTZ wall-clock microseconds as long; BOOLEAN as "
                               "true or false; DOUBLE as big-endian IEEE 754 hex; DECIMAL as "
                               "plain text; STRING as text"),
            "settings": {
                "spark.sql.timestampType": spark.conf.get("spark.sql.timestampType"),
                "spark.sql.optimizer.excludedRules":
                    spark.conf.get("spark.sql.optimizer.excludedRules")},
            "inputs": [recorded],
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
