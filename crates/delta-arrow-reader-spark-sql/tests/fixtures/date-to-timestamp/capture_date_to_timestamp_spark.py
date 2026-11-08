"""Capture Spark conversions from DATE to TIMESTAMP at midnight DST changes (#1027).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages for CAST, TRY_CAST and `::` from DATE to TIMESTAMP, and for implicit conversions in
comparisons, CASE, COALESCE, GREATEST, LEAST, UNION, IN lists and subqueries, VALUES,
arrays, DATE_TRUNC, HOUR, DATE_FORMAT and lambda functions, over a registered DATE column
and literals. The days include Havana's skipped midnight on 2024-03-10 and repeated midnight
on 2024-11-03, Santiago's skipped midnight on 2024-09-08 and the end of its DST on
2024-04-06, Toronto's 1919-03-31, which a gap from 23:30 to 00:30 starts at 00:30, their
neighbors, a pre-epoch day, years 1 and 9999, days beyond the date library's years and NULL.
Every query runs in both ANSI modes in UTC, America/Havana, America/Santiago,
America/Toronto and +05:30. Run from the crate root in the reference environment described
in README.md; the output path must not exist.
"""
import argparse
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Havana", "America/Santiago", "America/Toronto", "+05:30"]
EPOCH_DAY = date(1970, 1, 1)

DAYS = [
    # Havana skips 00:00-01:00 on 2024-03-10 and repeats 00:00-01:00 on 2024-11-03.
    "2024-03-09", "2024-03-10", "2024-03-11", "2024-11-02", "2024-11-03", "2024-11-04",
    # Santiago repeats 23:00-24:00 on 2024-04-06 and skips 00:00-01:00 on 2024-09-08.
    "2024-04-06", "2024-04-07", "2024-09-07", "2024-09-08", "2024-09-09",
    # Toronto skipped 23:30-00:30 on 1919-03-30, so 1919-03-31 starts at 00:30.
    "1919-03-30", "1919-03-31", "1919-04-01",
    "1969-12-31", "0001-01-01", "9999-12-31",
    None,
]
REQUIRED_DAYS = ["2024-03-10", "2024-09-08"]


def table(name, days, nullable):
    return {"name": name,
            "schema": {"type": "struct", "fields": [
                {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "d", "type": "date", "nullable": nullable, "metadata": {}}]},
            "rows": [[str(key), None if text is None
                      else str((date.fromisoformat(text) - EPOCH_DAY).days)]
                     for key, text in enumerate(days)]}


INPUTS = [table("date_inputs", DAYS, True), table("required_dates", REQUIRED_DAYS, False)]

EXPRESSIONS = [
    ("cast", "CAST(d AS TIMESTAMP)"),
    ("try_cast", "try_cast(d AS TIMESTAMP)"),
    ("colon_cast", "d::timestamp"),
    ("cast_ltz", "CAST(d AS TIMESTAMP_LTZ)"),
    ("cast_unix_micros", "unix_micros(CAST(d AS TIMESTAMP))"),
    ("cast_hour", "hour(CAST(d AS TIMESTAMP))"),
    ("cast_string", "CAST(CAST(d AS TIMESTAMP) AS STRING)"),
    # Implicit conversions to TIMESTAMP.
    ("equals_timestamp", "d = TIMESTAMP '2024-03-10 01:00:00'"),
    ("before_timestamp", "d < TIMESTAMP '2024-09-08 00:30:00'"),
    ("case", "CASE WHEN k % 2 = 0 THEN d ELSE TIMESTAMP '2000-01-01 12:00:00' END"),
    ("coalesce", "coalesce(d, TIMESTAMP '2000-01-01 12:00:00')"),
    ("greatest", "greatest(d, TIMESTAMP '1900-01-01 00:00:00')"),
    ("least", "least(d, TIMESTAMP '2100-01-01 00:00:00')"),
    # DATE and TIMESTAMP_NTZ widen to TIMESTAMP_NTZ, without a zone.
    ("greatest_ntz", "greatest(d, TIMESTAMP_NTZ '1900-01-01 00:00:00')"),
    ("greatest_mixed",
     "greatest(d, TIMESTAMP '1900-01-01 00:00:00', TIMESTAMP_NTZ '1900-01-01 00:00:00')"),
    # Implicit conversions this capture records outside the fix.
    ("in_list", "d IN (TIMESTAMP '2024-03-10 01:00:00', TIMESTAMP '2024-09-08 01:00:00')"),
    ("array", "array(d, TIMESTAMP '2000-01-01 12:00:00')[0]"),
    ("date_trunc_hour", "date_trunc('HOUR', d)"),
    ("hour", "hour(d)"),
    ("date_format", "date_format(d, 'yyyy-MM-dd HH:mm')"),
    # Conversions of a lambda variable.
    ("lambda_cast", "transform(array(d), x -> CAST(x AS TIMESTAMP))[0]"),
    ("lambda_try_cast", "transform(array(d), x -> try_cast(x AS TIMESTAMP))[0]"),
    ("lambda_filter",
     "size(filter(array(d, DATE '2024-01-15'), x -> x < TIMESTAMP '2024-02-01 00:00:00'))"),
]

LITERALS = [
    ("havana_skipped", "CAST(DATE '2024-03-10' AS TIMESTAMP)"),
    ("havana_repeated", "CAST(DATE '2024-11-03' AS TIMESTAMP)"),
    ("santiago_skipped", "CAST(DATE '2024-09-08' AS TIMESTAMP)"),
    ("santiago_neighbor", "CAST(DATE '2024-09-09' AS TIMESTAMP)"),
    ("try_cast", "try_cast(DATE '2024-03-10' AS TIMESTAMP)"),
    ("equals", "DATE '2024-03-10' = TIMESTAMP '2024-03-10 01:00:00'"),
    ("null", "CAST(CAST(NULL AS DATE) AS TIMESTAMP)"),
    ("toronto_straddled", "CAST(DATE '1919-03-31' AS TIMESTAMP)"),
    # January 15 of years 270000 and -264000, beyond the date library's years, in winter:
    # DST after 2099 is #1052. Then the first day whose start overflows BIGINT.
    ("far_future", "CAST(date_add(DATE '9999-12-31', 94963065) AS TIMESTAMP)"),
    ("far_past", "CAST(date_add(DATE '0001-01-01', -96424372) AS TIMESTAMP)"),
    ("overflow", "CAST(date_add(DATE '9999-12-31', 103819451) AS TIMESTAMP)"),
    ("try_overflow", "try_cast(date_add(DATE '9999-12-31', 103819451) AS TIMESTAMP)"),
    # Other kernels that raise constant folding errors, over a lambda variable.
    ("lambda_ntz",
     "transform(array(TIMESTAMP '2024-01-02 13:04:05'), x -> CAST(x AS TIMESTAMP_NTZ))[0]"),
    ("lambda_timestampdiff", "transform(array(TIMESTAMP '2024-01-02 13:04:05'), "
                             "x -> timestampdiff(HOUR, x, TIMESTAMP '2024-01-03 00:00:00'))[0]"),
]


def definitions():
    for name, expression in EXPRESSIONS:
        yield f"column_{name}", f"SELECT k, {expression} AS r FROM date_inputs"
    yield "required_cast", "SELECT k, CAST(d AS TIMESTAMP) AS r FROM required_dates"
    yield "required_try_cast", "SELECT k, try_cast(d AS TIMESTAMP) AS r FROM required_dates"
    for name, expression in LITERALS:
        yield f"literal_{name}", f"SELECT {expression} AS r"
    yield ("union", "SELECT d AS r FROM date_inputs "
                    "UNION ALL SELECT TIMESTAMP '2024-03-10 01:30:00'")
    yield ("filter", "SELECT k FROM date_inputs "
                     "WHERE CAST(d AS TIMESTAMP) >= TIMESTAMP '2024-03-10 00:30:00'")
    yield ("in_subquery", "SELECT k FROM date_inputs "
                          "WHERE d IN (SELECT TIMESTAMP '2024-03-10 01:00:00')")
    yield ("values", "SELECT r FROM VALUES (DATE '2024-03-10'), "
                     "(TIMESTAMP '2024-03-10 01:30:00') AS v(r)")


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "date":
        return EPOCH_DAY + timedelta(days=int(value))
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-date-to-timestamp")
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
            "value_encoding": ("INT and DATE days as int; TIMESTAMP UTC-epoch microseconds as "
                               "long; BOOLEAN as true or false; STRING as text"),
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
