"""Capture Spark DATEDIFF result fields, typed values and errors (#152).

Records analyzed, optimized and executed schemas, typed internal-row values and
failing stages for `datediff` and `date_diff` with DATE and TIMESTAMP arguments,
and integer-day DATE arithmetic at calendar boundaries, in both ANSI modes and in
UTC and America/Los_Angeles. Run from the crate root in the reference environment
described in README.md; the output path must not exist.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles"]
EPOCH = "DATE '1970-01-01'"
# Day counts from 1970-01-01: the DATE extremes and calendar boundaries.
DATE_ROWS = [(0, 1), (19782, 19723), (19783, 19782), (-1, 0), (2147483647, -2147483648),
             (None, 0), (19723, None)]

DIFF_PAIRS = [
    ("same_day", "DATE '2024-01-01'", "DATE '2024-01-01'"),
    ("next_day", "DATE '2024-01-01' + 1", "DATE '2024-01-01'"),
    ("previous_day", "DATE '2024-01-01'", "DATE '2024-01-02'"),
    ("leap_february", "DATE '2024-03-01'", "DATE '2024-02-28'"),
    ("common_february", "DATE '2023-03-01'", "DATE '2023-02-28'"),
    ("century_leap", "DATE '2000-03-01'", "DATE '2000-02-28'"),
    ("century_common", "DATE '1900-03-01'", "DATE '1900-02-28'"),
    ("leap_year", "DATE '2025-01-01'", "DATE '2024-01-01'"),
    ("negative_year", "DATE '2024-01-01'", "DATE '2024-12-31'"),
    ("before_epoch", "DATE '1969-12-31'", "DATE '1970-01-02'"),
    ("calendar_range", "DATE '9999-12-31'", "DATE '0001-01-01'"),
    ("extremes_wrap", f"date_add({EPOCH}, 2147483647)", f"date_add({EPOCH}, -2147483648)"),
    ("extremes_reverse_wrap", f"date_add({EPOCH}, -2147483648)", f"date_add({EPOCH}, 2147483647)"),
    ("null_end", "CAST(NULL AS DATE)", "DATE '2024-01-01'"),
    ("null_start", "DATE '2024-01-01'", "CAST(NULL AS DATE)"),
    ("timestamps_across_midnight", "TIMESTAMP '2024-01-02 01:00:00'",
     "TIMESTAMP '2024-01-01 23:00:00'"),
    ("timestamps_dst_gap_day", "TIMESTAMP '2024-03-11 00:30:00'", "TIMESTAMP '2024-03-10 01:30:00'"),
    ("timestamps_utc_offset", "TIMESTAMP '2024-01-02 03:00:00+00:00'",
     "TIMESTAMP '2024-01-01 12:00:00+00:00'"),
    # Instants that a fixed winter offset would move to the previous day in the DST zone.
    ("timestamp_summer_offset", "TIMESTAMP '2024-07-01 07:30:00+00:00'", "DATE '2024-07-01'"),
    ("timestamp_fall_back_day", "TIMESTAMP '2024-11-03 07:30:00+00:00'", "DATE '2024-11-03'"),
    ("timestamp_ntz", "TIMESTAMP_NTZ '2024-01-02 00:30:00'", "TIMESTAMP_NTZ '2024-01-01 23:30:00'"),
    ("date_and_timestamp", "DATE '2024-01-02'", "TIMESTAMP '2024-01-01 23:59:59.999999'"),
]

OTHER_PROBES = [
    # The exact SQL of the accepted runtime observations this fix corrects.
    ("history_date_add", "SELECT datediff(DATE'2024-01-01'+1,DATE'2024-01-01') AS r"),
    ("history_date_sub", "SELECT datediff(DATE'2024-01-03'-1,DATE'2024-01-01') AS r"),
    ("unaliased", "SELECT datediff(DATE '2024-01-01', DATE '2023-01-01'), "
                  "date_diff(DATE '2024-01-01', DATE '2023-01-01')"),
    ("column", "SELECT datediff(date_add(DATE '1970-01-01', e), date_add(DATE '1970-01-01', s)) "
               "AS r FROM day_pairs"),
    ("column_alias", "SELECT date_diff(date_add(DATE '1970-01-01', s), "
                     "date_add(DATE '1970-01-01', e)) AS r FROM day_pairs"),
    ("column_unaliased", "SELECT datediff(date_add(DATE '1970-01-01', e), DATE '1970-01-01') "
                         "FROM day_pairs"),
    # The DATE extremes have no TIMESTAMP, so this probe keeps ordinary days.
    ("column_timestamp", "SELECT datediff(CAST(date_add(DATE '1970-01-01', e) AS TIMESTAMP) "
                         "+ INTERVAL 23 HOURS, DATE '1970-01-01') AS r FROM day_pairs "
                         "WHERE abs(e) < 1000000"),
    ("unit_day_bigint", "SELECT datediff(DAY, DATE '2024-01-01', DATE '2024-01-03') AS r"),
    ("leap_add", "SELECT DATE '2024-02-28' + 1 AS a, date_add(DATE '2024-02-28', 2) AS b, "
                 "date_sub(DATE '2024-03-01', 1) AS c, DATE '2023-03-01' - 1 AS d"),
    ("month_year_add", "SELECT date_add(DATE '2024-01-31', 30) AS a, "
                       "date_sub(DATE '2025-01-01', 366) AS b, DATE '2024-12-31' + 1 AS c"),
]


def definitions():
    for name, end, start in DIFF_PAIRS:
        yield f"datediff_{name}", f"SELECT datediff({end}, {start}) AS r"
        yield f"date_diff_{name}", f"SELECT date_diff({end}, {start}) AS r"
    yield from OTHER_PROBES


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.TimestampType, types.TimestampNTZType)):
        return str(row.getLong(index))
    raise AssertionError(f"unexpected result type {data_type}")


def error_condition(error, Py4JError):
    condition = getattr(error, "getCondition", lambda: None)()
    cause = getattr(error, "java_exception", None)
    while condition is None and cause is not None:
        try:
            condition = cause.getCondition()
        except Py4JError:
            pass
        cause = cause.getCause()
    return condition


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
        condition = error_condition(error, Py4JError)
        record["expected"] = {"status": "error", "condition": condition,
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-date-diff")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([types.StructField("e", types.IntegerType(), True),
                                   types.StructField("s", types.IntegerType(), True)])
        spark.createDataFrame(DATE_ROWS, schema).createOrReplaceTempView("day_pairs")
        header = {
            "spark_version": spark.version, "spark_build": build,
            "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
            "python_version": sys.version.split()[0],
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "value_encoding": ("INT and DATE days as int; BIGINT and TIMESTAMP microseconds "
                               "as long"),
            "settings": {"spark.sql.optimizer.excludedRules":
                         spark.conf.get("spark.sql.optimizer.excludedRules")},
            "inputs": [{"name": "day_pairs", "schema": schema.jsonValue(),
                        "rows": [[None if v is None else str(v) for v in row]
                                 for row in DATE_ROWS]}],
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
