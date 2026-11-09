"""Capture Spark's nullability of formatted UNIX_TIMESTAMP over DATE and TIMESTAMP arguments
(#1203).

Records analyzed, optimized and executed schemas, typed internal-row values and failing stages,
with the innermost Java exception of a failure, for UNIX_TIMESTAMP and TO_UNIX_TIMESTAMP with a
format over NOT NULL and nullable DATE, TIMESTAMP and TIMESTAMP_NTZ columns and literals, with
literal, NOT NULL column, nullable column and NULL formats; over a DATE column with a format
that fails if evaluated; over a DATE expression; as a grouping key; with aggregates and window
functions in the format; under grouping sets; in ORDER BY; and with derived format columns whose
expressions fail if evaluated. Spark declares the result nullable outside ANSI mode, and in ANSI
mode when an argument is nullable. It never evaluates the format of these arguments, but
computes its aggregates and window functions, and a format column keeps a literal argument's
result from being constant. Every query runs in both ANSI modes in UTC and America/Los_Angeles.
Run from the crate root in the reference environment described in README.md; the output path
must not exist.
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

# (DATE, TIMESTAMP instant in UTC and TIMESTAMP_NTZ, format); the nullable copies are NULL in
# the last row. Noon keeps each date the same in UTC and Los Angeles.
ROWS = [
    ("2024-01-02", "2024-01-02 12:00:00", "yyyy-MM-dd"),
    ("2024-03-10", "2024-03-10 12:00:00", "dd/MM/yyyy"),
    ("1969-12-31", "1969-12-31 12:00:00", None),
]


def micros(text):
    value = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def days(text):
    return (date.fromisoformat(text) - date(1970, 1, 1)).days


LAST = len(ROWS) - 1
INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "dt", "type": "date", "nullable": False, "metadata": {}},
        {"name": "dtn", "type": "date", "nullable": True, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": False, "metadata": {}},
        {"name": "tsn", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "tn", "type": "timestamp_ntz", "nullable": False, "metadata": {}},
        {"name": "tnn", "type": "timestamp_ntz", "nullable": True, "metadata": {}},
        {"name": "g", "type": "string", "nullable": False, "metadata": {}},
        {"name": "f", "type": "string", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), str(days(day)), None if key == LAST else str(days(day)),
              str(micros(instant)), None if key == LAST else str(micros(instant)),
              str(micros(instant)), None if key == LAST else str(micros(instant)),
              "yyyy-MM-dd", pattern]
             for key, (day, instant, pattern) in enumerate(ROWS)]}]

COLUMNS = [("date", "dt"), ("nullable_date", "dtn"), ("timestamp", "ts"),
           ("nullable_timestamp", "tsn"), ("timestamp_ntz", "tn"),
           ("nullable_timestamp_ntz", "tnn")]
LITERALS = [("date", "DATE '2024-01-02'"), ("timestamp", "TIMESTAMP '2024-01-02 12:00:00'"),
            ("timestamp_ntz", "TIMESTAMP_NTZ '2024-01-02 12:00:00'"),
            ("null_date", "CAST(NULL AS DATE)")]
FORMATS = [("format", "'yyyy-MM-dd'"), ("required_format", "g"), ("nullable_format", "f"),
           ("null_format", "NULL"), ("null_string_format", "CAST(NULL AS STRING)")]


def definitions():
    for function in ["unix_timestamp", "to_unix_timestamp"]:
        for name, column in COLUMNS:
            for format_name, pattern in FORMATS:
                yield (f"column_{function}_{name}_{format_name}",
                       f"SELECT k, {function}({column}, {pattern}) AS r FROM inputs")
        for name, value in LITERALS:
            for format_name, pattern in FORMATS[:1] + FORMATS[3:4]:
                yield (f"literal_{function}_{name}_{format_name}",
                       f"SELECT {function}({value}, {pattern}) AS r")
            for format_name, pattern in FORMATS[1:3]:
                yield (f"literal_{function}_{name}_{format_name}",
                       f"SELECT k, {function}({value}, {pattern}) AS r FROM inputs")
        yield (f"column_{function}_date_failing_format",
               f"SELECT k, {function}(dt, raise_error('format evaluated')) AS r FROM inputs")
        yield (f"expression_{function}_date_nullable_format",
               f"SELECT k, {function}(date_add(dt, 1), f) AS r FROM inputs")
        yield (f"grouped_{function}_date_required_format",
               f"SELECT {function}(dt, g) AS r, count(*) AS n FROM inputs GROUP BY 1")
        # Spark computes the aggregates and window functions of a format it does not evaluate.
        yield (f"aggregate_{function}_nullable_format",
               f"SELECT {function}(min(dt), max(f)) AS r FROM inputs")
        yield (f"grouped_aggregate_{function}_required_format",
               f"SELECT k, {function}(min(dt), max(g)) AS r FROM inputs GROUP BY k")
        yield (f"literal_aggregate_{function}_nullable_format",
               f"SELECT {function}(DATE '2024-01-02', max(f)) AS r FROM inputs")
        yield (f"literal_aggregate_{function}_required_format",
               f"SELECT {function}(DATE '2024-01-02', CAST(count(*) AS STRING)) AS r "
               "FROM inputs")
        yield (f"window_{function}_required_format",
               f"SELECT k, {function}(dt, CAST(row_number() OVER (ORDER BY k) AS STRING)) AS r "
               "FROM inputs")
        # With ANSI enabled the CAST in a computed aggregate or window function fails, also once
        # a subquery renames it, derives the format from it or reads it in a union's second input.
        failing = "max(CAST(g AS INT))"
        yield (f"failing_aggregate_{function}_format",
               f"SELECT {function}(DATE '2024-01-02', CAST({failing} AS STRING)) AS r "
               "FROM inputs")
        yield (f"failing_window_{function}_format",
               f"SELECT k, {function}(dt, CAST({failing} OVER () AS STRING)) AS r FROM inputs")
        yield (f"renamed_failing_aggregate_{function}_format",
               f"SELECT k, {function}(DATE '2024-01-02', CAST(m AS STRING)) AS r "
               f"FROM (SELECT k, {failing} AS m FROM inputs GROUP BY k) t")
        yield (f"renamed_failing_window_{function}_format",
               f"SELECT k, {function}(dt, CAST(m AS STRING)) AS r "
               f"FROM (SELECT k, dt, {failing} OVER () AS m FROM inputs) t")
        yield (f"derived_failing_aggregate_{function}_format",
               f"SELECT k, {function}(DATE '2024-01-02', x) AS r "
               f"FROM (SELECT k, CAST({failing} AS STRING) AS x FROM inputs GROUP BY k) t")
        yield (f"derived_failing_window_{function}_format",
               f"SELECT k, {function}(dt, x) AS r "
               f"FROM (SELECT k, dt, CAST({failing} OVER () AS STRING) AS x FROM inputs) t")
        yield (f"union_failing_aggregate_{function}_format",
               f"SELECT k, {function}(DATE '2024-01-02', x) AS r FROM (SELECT k, "
               f"CAST(max(k) AS STRING) AS x FROM inputs GROUP BY k UNION ALL SELECT k, "
               f"CAST({failing} AS STRING) AS x FROM inputs GROUP BY k) t")
        # A grouping set makes a NOT NULL grouping key NULL in its subtotal rows.
        for name, argument, groups in [
                ("rollup", "dt", "ROLLUP(dt, g)"), ("cube", "dt", "CUBE(dt, g)"),
                ("grouping_sets", "dt", "GROUPING SETS ((dt, g), ())"),
                ("rollup_timestamp", "ts", "ROLLUP(ts, g)")]:
            yield (f"{name}_{function}_required_format",
                   f"SELECT {argument}, {function}({argument}, g) AS r FROM inputs "
                   f"GROUP BY {groups}")
        yield (f"rollup_{function}_count_format",
               f"SELECT dt, {function}(dt, CAST(count(*) AS STRING)) AS r FROM inputs "
               "GROUP BY ROLLUP(dt)")
        yield (f"order_by_aggregate_{function}_nullable_format",
               f"SELECT k FROM inputs GROUP BY k "
               f"ORDER BY {function}(DATE '2024-01-02', max(f)), k")
        # Spark never evaluates the format, also once it inlines a derived column into the call.
        for name, derived in [("raise", "raise_error('derived format evaluated')"),
                              ("division", "CAST(k / 0 AS STRING)"),
                              ("cast", "CAST(CAST(g AS INT) AS STRING)")]:
            yield (f"derived_{name}_{function}_format",
                   f"SELECT k, {function}(dt, x) AS r FROM "
                   f"(SELECT k, dt, {derived} AS x FROM inputs)")
        yield (f"derived_literal_{function}_format",
               f"SELECT k, {function}(DATE '2024-01-02', x) AS r FROM "
               f"(SELECT k, CAST(CAST(g AS INT) AS STRING) AS x FROM inputs)")
        yield (f"derived_cte_{function}_format",
               f"WITH d AS (SELECT k, dt, raise_error('derived format evaluated') AS x "
               f"FROM inputs) SELECT k, {function}(dt, x) AS r FROM d")
        yield (f"derived_union_{function}_format",
               f"SELECT k, {function}(dt, x) AS r FROM (SELECT k, dt, CAST(k / 0 AS STRING) "
               f"AS x FROM inputs UNION ALL SELECT k, dt, CAST(k / 0 AS STRING) AS x FROM inputs)")
        yield (f"derived_literal_cte_{function}_format",
               f"WITH d AS (SELECT k, CAST(CAST(g AS INT) AS STRING) AS x FROM inputs) "
               f"SELECT k, {function}(DATE '2024-01-02', x) AS r FROM d")
        yield (f"derived_literal_union_{function}_format",
               f"SELECT k, {function}(DATE '2024-01-02', x) AS r FROM (SELECT k, "
               f"CAST(k / 0 AS STRING) AS x FROM inputs UNION ALL SELECT k, "
               f"CAST(k / 0 AS STRING) AS x FROM inputs)")
        # Spark collapses these shapes too, so it never evaluates the derived format.
        cast = "CAST(CAST(g AS INT) AS STRING) AS x"
        for name, source in [
                ("alias", f"(SELECT k, {cast} FROM inputs) t"),
                ("filter", f"(SELECT k, {cast} FROM inputs WHERE 1 = 1) t"),
                ("limit", f"(SELECT k, {cast} FROM inputs LIMIT 10) t"),
                ("nested", f"(SELECT * FROM (SELECT k, {cast} FROM inputs) u) t"),
                ("window", "(SELECT k, CAST(CAST(max(g) OVER () AS INT) AS STRING) AS x "
                           "FROM inputs) t")]:
            yield (f"derived_literal_{name}_{function}_format",
                   f"SELECT k, {function}(DATE '2024-01-02', x) AS r FROM {source}")
        yield (f"derived_literal_distinct_{function}_format",
               f"SELECT DISTINCT k, {function}(DATE '2024-01-02', x) AS r "
               f"FROM (SELECT k, {cast} FROM inputs) t")
        # Outside a projection, Spark never evaluates the derived format either.
        derived = f"(SELECT k, dt, {cast} FROM inputs) t"
        yield (f"derived_filter_{function}_format",
               f"SELECT k FROM {derived} WHERE {function}(dt, x) IS NOT NULL")
        yield (f"derived_group_key_{function}_format",
               f"SELECT {function}(DATE '2024-01-02', x) AS r, count(*) AS n FROM {derived} "
               f"GROUP BY 1")
        yield (f"derived_window_key_{function}_format",
               f"SELECT k, count(*) OVER (PARTITION BY {function}(dt, x)) AS n FROM {derived}")
        yield (f"derived_join_condition_{function}_format",
               f"SELECT a.k FROM inputs a JOIN {derived} "
               f"ON a.k = t.k AND {function}(DATE '2024-01-02', t.x) IS NOT NULL")
        yield (f"derived_union_limit_{function}_format",
               f"SELECT k, {function}(dt, x) AS r FROM (SELECT * FROM (SELECT k, dt, {cast} "
               f"FROM inputs UNION ALL SELECT k, dt, {cast} FROM inputs) u LIMIT 10) t")
        # A NOT NULL derived format keeps the result NOT NULL with ANSI enabled, and Spark never
        # evaluates it either.
        required = "coalesce(CAST(CAST(g AS INT) AS STRING), 'yyyy-MM-dd') AS x"
        for name, argument in [("date", "dt"), ("timestamp", "ts"), ("timestamp_ntz", "tn"),
                               ("literal", "DATE '2024-01-02'")]:
            yield (f"derived_required_{name}_{function}_format",
                   f"SELECT k, {function}({argument}, x) AS r "
                   f"FROM (SELECT k, dt, ts, tn, {required} FROM inputs) t")
        # Spark computes a derived column below a join, so there it fails with ANSI enabled.
        yield (f"derived_joined_{function}_format",
               f"SELECT t.k, {function}(t.dt, t.x) AS r FROM inputs a JOIN {derived} "
               f"ON a.k = t.k")
        # Spark computes a derived column below a sort, so there it fails with ANSI enabled.
        yield (f"derived_sorted_{function}_format",
               f"SELECT k, {function}(dt, x) AS r FROM (SELECT k, dt, {cast} FROM inputs "
               f"ORDER BY k) t")
        yield (f"derived_grouped_{function}_format",
               f"SELECT k, {function}(d, x) AS r FROM (SELECT k, max(dt) AS d, "
               f"CAST(CAST(max(g) AS INT) AS STRING) AS x FROM inputs GROUP BY k)")


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
    spark = (SparkSession.builder.master("local[1]").appName("dar-formatted-unix-timestamp-nullability")
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
