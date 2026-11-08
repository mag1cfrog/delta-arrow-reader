"""Capture Spark's rejection of ordering and comparing calendar intervals (#1149).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for ORDER BY, window ORDER BY and
comparisons over calendar intervals built by MAKE_INTERVAL, as expressions, aliases,
ordinals and constants, and for the same over DAY TO SECOND and YEAR TO MONTH intervals,
which Spark orders. Every query runs in both ANSI modes in UTC and America/Los_Angeles. Run
from the crate root in the reference environment described in README.md; the output path
must not exist.
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

def text(value):
    return None if value is None else str(value)


INPUTS = [{
    "name": "required",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "d", "type": "integer", "nullable": False, "metadata": {}}]},
    "rows": [["0", "1"], ["1", "-2"], ["2", "3"]]}]

CALENDAR = "make_interval(0, 0, 0, d)"
DAY_TIME = "make_dt_interval(d)"
YEAR_MONTH = "make_ym_interval(d)"

QUERIES = [
    ("order_by", f"SELECT k FROM required ORDER BY {CALENDAR}"),
    ("order_by_desc_nulls_last", f"SELECT k FROM required ORDER BY {CALENDAR} DESC NULLS LAST"),
    ("order_by_alias", f"SELECT k, {CALENDAR} AS i FROM required ORDER BY i"),
    ("order_by_ordinal", f"SELECT {CALENDAR} AS i FROM required ORDER BY 1"),
    ("order_by_constant", "SELECT k FROM required ORDER BY make_interval(1)"),
    ("order_by_second_key", f"SELECT k FROM required ORDER BY k, {CALENDAR}"),
    ("sort_by", f"SELECT k FROM required SORT BY {CALENDAR}"),
    ("window_order_by", f"SELECT k, row_number() OVER (ORDER BY {CALENDAR}) AS r FROM required"),
    ("window_order_by_partitioned",
     f"SELECT k, rank() OVER (PARTITION BY k ORDER BY {CALENDAR}) AS r FROM required"),
    ("less_than", f"SELECT k, {CALENDAR} < make_interval(1) AS r FROM required"),
    ("greater_or_equal", f"SELECT k, {CALENDAR} >= make_interval(1) AS r FROM required"),
    ("equal", f"SELECT k, {CALENDAR} = make_interval(0, 0, 0, 1) AS r FROM required"),
    ("not_equal", f"SELECT k, {CALENDAR} <> make_interval(0, 0, 0, 1) AS r FROM required"),
    ("null_safe_equal", f"SELECT k, {CALENDAR} <=> make_interval(0, 0, 0, 1) AS r FROM required"),
    ("between", f"SELECT k, {CALENDAR} BETWEEN make_interval(0) AND make_interval(1) AS r "
                "FROM required"),
    ("in_list", f"SELECT k, {CALENDAR} IN (make_interval(0, 0, 0, 1)) AS r FROM required"),
    ("where_less_than", f"SELECT k FROM required WHERE {CALENDAR} < make_interval(1)"),
    ("constant_less_than", "SELECT make_interval(1) < make_interval(2) AS r"),
    ("constant_equal", "SELECT make_interval(1) = make_interval(1) AS r"),
    ("greatest", f"SELECT k, greatest({CALENDAR}, make_interval(1)) AS r FROM required"),
    ("max", f"SELECT max({CALENDAR}) AS r FROM required"),
    ("min", f"SELECT min({CALENDAR}) AS r FROM required"),
    ("least", f"SELECT k, least({CALENDAR}, make_interval(1)) AS r FROM required"),
    ("not_in_list", f"SELECT k, {CALENDAR} NOT IN (make_interval(0, 0, 0, 1)) AS r FROM required"),
    ("order_by_array", f"SELECT k FROM required ORDER BY array({CALENDAR})"),
    ("order_by_struct", f"SELECT k FROM required ORDER BY named_struct('i', {CALENDAR})"),
    ("sort_array", f"SELECT k, sort_array(array({CALENDAR})) AS r FROM required"),
    ("array_sort", f"SELECT k, array_sort(array({CALENDAR})) AS r FROM required"),
    ("array_max", f"SELECT k, array_max(array({CALENDAR})) AS r FROM required"),
    ("max_by", f"SELECT max_by(k, {CALENDAR}) AS r FROM required"),
    ("group_by", f"SELECT {CALENDAR} AS i, count(*) AS c FROM required GROUP BY {CALENDAR}"),
    ("distinct", f"SELECT DISTINCT {CALENDAR} AS i FROM required"),
    ("day_time_order_by", f"SELECT k FROM required ORDER BY {DAY_TIME}"),
    ("year_month_order_by", f"SELECT k FROM required ORDER BY {YEAR_MONTH} DESC"),
    ("day_time_order_by_alias", f"SELECT k, {DAY_TIME} AS i FROM required ORDER BY i DESC"),
    ("day_time_window_order_by",
     f"SELECT k, row_number() OVER (ORDER BY {DAY_TIME}) AS r FROM required"),
    ("day_time_less_than", f"SELECT k, {DAY_TIME} < INTERVAL '2' DAY AS r FROM required"),
    ("year_month_equal", f"SELECT k, {YEAR_MONTH} = INTERVAL '1' YEAR AS r FROM required"),
]


# Queries whose rows are recorded in Spark's order, which must be kept.
ORDERED = {"day_time_order_by", "year_month_order_by", "day_time_order_by_alias"}


def definitions():
    """The record names and queries, in capture order."""
    yield from QUERIES


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.YearMonthIntervalType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.DayTimeIntervalType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.BooleanType):
        return "true" if row.getBoolean(index) else "false"
    if isinstance(data_type, types.CalendarIntervalType):
        from py4j.java_gateway import get_field
        interval = row.getInterval(index)
        return [str(get_field(interval, field)) for field in ("months", "days", "microseconds")]
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


def capture(spark, sql, types, Py4JError, ordered):
    record = {"schema_stages": {}, "ordered": ordered}
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
        if not ordered:
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-calendar-interval-ordering")
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
            spark.createDataFrame([[python_value(value, kind) for value, kind in zip(row, kinds)]
                                   for row in recorded["rows"]],
                                  types.StructType.fromJson(recorded["schema"])) \
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
            "value_encoding": ("INT, BOOLEAN and interval counts as text; CALENDAR INTERVAL "
                               "as [months, days, microseconds]"),
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
                                    **capture(spark, sql, types, Py4JError, name in ORDERED)})
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
