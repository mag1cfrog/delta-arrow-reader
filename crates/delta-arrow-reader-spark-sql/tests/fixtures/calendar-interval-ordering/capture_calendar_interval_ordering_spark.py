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
    ("less_or_equal", f"SELECT k, {CALENDAR} <= make_interval(1) AS r FROM required"),
    ("greater_than", f"SELECT k, {CALENDAR} > make_interval(1) AS r FROM required"),
    ("null_less_than", f"SELECT k, NULL < {CALENDAR} AS r FROM required"),
    ("is_distinct_from",
     f"SELECT k, {CALENDAR} IS DISTINCT FROM make_interval(0, 0, 0, 1) AS r FROM required"),
    ("is_not_distinct_from",
     f"SELECT k, {CALENDAR} IS NOT DISTINCT FROM make_interval(0, 0, 0, 1) AS r FROM required"),
    ("equal_null",
     f"SELECT k, equal_null({CALENDAR}, make_interval(0, 0, 0, 1)) AS r FROM required"),
    ("nullif", f"SELECT k, nullif({CALENDAR}, make_interval(0, 0, 0, 1)) AS r FROM required"),
    ("simple_case", f"SELECT k, CASE {CALENDAR} WHEN make_interval(0, 0, 0, 1) THEN 1 ELSE 0 END "
                    "AS r FROM required"),
    ("null_in_list", f"SELECT k, NULL IN ({CALENDAR}) AS r FROM required"),
    ("null_not_in_list", f"SELECT k, NULL NOT IN ({CALENDAR}) AS r FROM required"),
    ("in_subquery", f"SELECT k, {CALENDAR} IN (SELECT make_interval(0, 0, 0, 1)) AS r "
                    "FROM required"),
    ("null_in_subquery", "SELECT k, NULL IN (SELECT make_interval(0, 0, 0, 1)) AS r FROM required"),
    ("multi_column_in_subquery",
     f"SELECT k, (k, {CALENDAR}) IN (SELECT 0, make_interval(0, 0, 0, 1)) AS r FROM required"),
    ("join_on", "SELECT a.k FROM required a JOIN required b "
                "ON make_interval(0, 0, 0, a.d) = make_interval(0, 0, 0, b.d)"),
    ("join_using", f"SELECT k FROM (SELECT k, {CALENDAR} AS i FROM required) a "
                   f"JOIN (SELECT {CALENDAR} AS i FROM required) b USING (i)"),
    ("natural_join", f"SELECT k FROM (SELECT k, {CALENDAR} AS i FROM required) a "
                     f"NATURAL JOIN (SELECT {CALENDAR} AS i FROM required) b"),
    ("pivot", f"SELECT * FROM (SELECT k, {CALENDAR} AS i FROM required) "
              "PIVOT (count(k) FOR i IN (make_interval(0, 0, 0, 1)))"),
    ("window_partition_by",
     f"SELECT k, count(*) OVER (PARTITION BY {CALENDAR}) AS c FROM required"),
    ("within_group", f"SELECT listagg(CAST(k AS STRING)) WITHIN GROUP (ORDER BY {CALENDAR}) AS r "
                     "FROM required"),
    ("order_by_limit", f"SELECT k FROM required ORDER BY {CALENDAR} LIMIT 2"),
    ("rollup", f"SELECT {CALENDAR} AS i, count(*) AS c FROM required GROUP BY ROLLUP({CALENDAR})"),
    ("equal_day_time", f"SELECT k, {CALENDAR} = make_dt_interval(1) AS r FROM required"),
    ("less_than_year_month", f"SELECT k, {CALENDAR} < make_ym_interval(1) AS r FROM required"),
    ("equal_integer", f"SELECT k, {CALENDAR} = 1 AS r FROM required"),
    ("equal_string", f"SELECT k, {CALENDAR} = '1 days' AS r FROM required"),
    ("in_integer_list", f"SELECT k, {CALENDAR} IN (1) AS r FROM required"),
    ("greatest_integer", f"SELECT k, greatest({CALENDAR}, 1) AS r FROM required"),
    # Precedence and shape cases from the #1149 review.
    ("array_eq_scalar",
     "SELECT k, array(make_interval(0, 0, 0, d)) = make_interval(0, 0, 0, d) AS r FROM required"),
    ("in_array_scalar",
     "SELECT k, make_interval(0, 0, 0, d) IN (array(make_interval(0, 0, 0, d))) AS r FROM "
     "required"),
    ("greatest_array_scalar",
     "SELECT k, greatest(array(make_interval(0, 0, 0, d)), make_interval(0, 0, 0, d)) AS r "
     "FROM required"),
    ("struct_eq_scalar",
     "SELECT k, named_struct('i', make_interval(0, 0, 0, d)) = make_interval(0, 0, 0, d) "
     "AS r FROM required"),
    ("array_eq_null_array",
     "SELECT k, array(make_interval(0, 0, 0, d)) = array(NULL) AS r FROM required"),
    ("array_in_null_array",
     "SELECT k, array(make_interval(0, 0, 0, d)) IN (array(NULL)) AS r FROM required"),
    ("struct_eq_null_struct",
     "SELECT k, named_struct('i', make_interval(0, 0, 0, d)) = named_struct('i', NULL) AS "
     "r FROM required"),
    ("array_eq_empty_array",
     "SELECT k, array(make_interval(0, 0, 0, d)) = array() AS r FROM required"),
    ("array_eq_dt_array",
     "SELECT k, array(make_interval(0, 0, 0, d)) = array(make_dt_interval(1)) AS r FROM required"),
    ("in_null_only", "SELECT k, make_interval(0, 0, 0, d) IN (NULL) AS r FROM required"),
    ("in_string_list", "SELECT k, make_interval(0, 0, 0, d) IN ('1 days') AS r FROM required"),
    ("in_subquery_int", "SELECT k FROM required WHERE make_interval(0, 0, 0, d) IN (SELECT 1)"),
    ("int_in_subquery_interval",
     "SELECT k FROM required WHERE k IN (SELECT make_interval(0, 0, 0, 1))"),
    ("multi_in_subquery_mismatch",
     "SELECT k FROM required WHERE (k, make_interval(0, 0, 0, d)) IN (SELECT 0, 1)"),
    ("multi_in_subquery_array",
     "SELECT k FROM required WHERE (k, array(make_interval(0, 0, 0, d))) IN (SELECT 0, "
     "array(make_interval(0, 0, 0, 1)))"),
    ("nullif_int", "SELECT k, nullif(make_interval(0, 0, 0, d), 1) AS r FROM required"),
    ("nullif_null_left", "SELECT k, nullif(NULL, make_interval(0, 0, 0, d)) AS r FROM required"),
    ("nullif_null_right", "SELECT k, nullif(make_interval(0, 0, 0, d), NULL) AS r FROM required"),
    ("greatest_single", "SELECT k, greatest(make_interval(0, 0, 0, d)) AS r FROM required"),
    ("least_null", "SELECT k, least(make_interval(0, 0, 0, d), NULL) AS r FROM required"),
    ("greatest_day_time",
     "SELECT k, greatest(make_interval(0, 0, 0, d), make_dt_interval(1)) AS r FROM required"),
    ("between_int", "SELECT k, make_interval(0, 0, 0, d) BETWEEN 1 AND 2 AS r FROM required"),
    ("between_null_bounds",
     "SELECT k, make_interval(0, 0, 0, d) BETWEEN NULL AND NULL AS r FROM required"),
    ("simple_case_int",
     "SELECT k, CASE make_interval(0, 0, 0, d) WHEN 1 THEN 1 ELSE 0 END AS r FROM required"),
    ("equal_null_int", "SELECT k, equal_null(make_interval(0, 0, 0, d), 1) AS r FROM required"),
    ("is_distinct_from_int",
     "SELECT k, make_interval(0, 0, 0, d) IS DISTINCT FROM 1 AS r FROM required"),
    ("join_on_mismatch",
     "SELECT a.k FROM required a JOIN required b ON make_interval(0, 0, 0, a.d) = b.d"),
    ("join_using_mismatch",
     "SELECT * FROM (SELECT k, make_interval(0, 0, 0, d) AS i FROM required) a JOIN "
     "(SELECT k, d AS i FROM required) b USING (i)"),
    ("pivot_multi_column",
     "SELECT * FROM (SELECT k, make_interval(0, 0, 0, d) AS i FROM required) PIVOT "
     "(count(*) FOR (k, i) IN ((0, make_interval(0, 0, 0, 1)) AS one))"),
    ("pivot_array",
     "SELECT * FROM (SELECT k, array(make_interval(0, 0, 0, d)) AS i FROM required) PIVOT "
     "(count(k) FOR i IN (array(make_interval(0, 0, 0, 1)) AS one))"),
    ("pivot_day_time_control",
     "SELECT * FROM (SELECT k, make_dt_interval(d) AS i FROM required) PIVOT (count(k) FOR "
     "i IN (INTERVAL '1' DAY AS one))"),
    ("pivot_interval_payload_control",
     "SELECT * FROM (SELECT k, make_interval(0, 0, 0, d) AS i FROM required) PIVOT "
     "(count(i) FOR k IN (0 AS z, 1 AS o))"),
    ("partition_by_array",
     "SELECT k, count(*) OVER (PARTITION BY array(make_interval(0, 0, 0, d))) AS c FROM required"),
    ("partition_by_struct",
     "SELECT k, count(*) OVER (PARTITION BY named_struct('i', make_interval(0, 0, 0, d))) "
     "AS c FROM required"),
    ("partition_by_two",
     "SELECT k, count(*) OVER (PARTITION BY k, make_interval(0, 0, 0, d)) AS c FROM required"),
    ("partition_by_named_window",
     "SELECT k, count(*) OVER w AS c FROM required WINDOW w AS (PARTITION BY "
     "make_interval(0, 0, 0, d))"),
    ("partition_and_order_interval",
     "SELECT k, rank() OVER (PARTITION BY make_interval(0, 0, 0, d) ORDER BY "
     "make_interval(0, 0, 0, d)) AS c FROM required"),
    ("partition_interval_order_k",
     "SELECT k, rank() OVER (PARTITION BY make_interval(0, 0, 0, d) ORDER BY k) AS c FROM "
     "required"),
    ("partition_by_column_alias",
     "SELECT k, count(*) OVER (PARTITION BY i) AS c FROM (SELECT k, make_interval(0, 0, 0, "
     "d) AS i FROM required)"),
    ("partition_by_day_time_control",
     "SELECT k, count(*) OVER (PARTITION BY make_dt_interval(d)) AS c FROM required"),
    ("partition_by_year_month_control",
     "SELECT k, count(*) OVER (PARTITION BY make_ym_interval(d)) AS c FROM required"),
    ("partition_by_k_payload_control",
     "SELECT k, first_value(make_interval(0, 0, 0, d)) OVER (PARTITION BY k) AS c FROM required"),
    ("nullif_int_control", "SELECT k, nullif(k, 1) AS r FROM required"),
    ("nullif_day_time_control",
     "SELECT k, nullif(make_dt_interval(d), make_dt_interval(1)) AS r FROM required"),
    ("multi_in_subquery_control", "SELECT k FROM required WHERE (k, d) IN (SELECT 0, 1)"),
    ("in_subquery_payload_control",
     "SELECT k, i FROM (SELECT k, make_interval(0, 0, 0, d) AS i FROM required) WHERE k IN "
     "(SELECT k FROM required WHERE d > 0)"),
    ("greatest_day_time_null_control",
     "SELECT k, greatest(make_dt_interval(d), NULL) AS r FROM required"),
    ("in_list_null_control", "SELECT k, k IN (NULL, 1) AS r, NULL IN (k) AS s FROM required"),
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
