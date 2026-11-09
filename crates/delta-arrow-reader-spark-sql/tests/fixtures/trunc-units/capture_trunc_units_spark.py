"""Capture Spark's TRUNC units, values and nullability (#1192).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for TRUNC of NOT NULL and nullable DATE
columns, DATE literals and TIMESTAMP, TIMESTAMP_NTZ, STRING and INT arguments, with every unit
spelling Spark supports in mixed case, unsupported, empty, padded and NULL units, constant unit
expressions, INT, BINARY and ARRAY units, a volatile unit, a constant unit that fails to
evaluate, and columns of units; constant unsupported units, units the optimizer makes constant
and unit columns beside malformed STRING dates; aggregates in the date; dates beyond year 200000
and near the earliest DATE. Spark supports YEAR, YYYY, YY, QUARTER, MON, MONTH,
MM and WEEK, in any letter case, and gives NULL for any other unit; the result is nullable.
Every query runs in both ANSI modes in UTC and America/Los_Angeles. Run from the crate root in
the reference environment described in README.md; the output path must not exist.
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

# (DATE, TIMESTAMP instant in UTC, TIMESTAMP_NTZ, STRING, STRING in other forms Spark reads as
# a DATE, STRING malformed in the rows of an unsupported or NULL unit, unit); the nullable copies
# are NULL in the last row. Noon keeps each date the same in UTC and Los Angeles.
ROWS = [
    ("2024-05-17", "2024-05-17 12:00:00", "2024-05-17", " 2024-03-10 ", "2024-05-17", "MM"),
    ("2024-03-10", "2024-03-10 12:00:00", "2024-03-10", "2024-05", "2024-03-10", "week"),
    ("1969-12-31", "1969-12-31 12:00:00", "1969-12-31", "2024", "garbage", "DD"),
    ("2024-01-01", "2024-01-01 12:00:00", "2024-01-01", "2024-02-29T10:00:00", "garbage", None),
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
        {"name": "dr", "type": "date", "nullable": False, "metadata": {}},
        {"name": "d", "type": "date", "nullable": True, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": False, "metadata": {}},
        {"name": "tn", "type": "timestamp_ntz", "nullable": False, "metadata": {}},
        {"name": "s", "type": "string", "nullable": False, "metadata": {}},
        {"name": "sv", "type": "string", "nullable": False, "metadata": {}},
        {"name": "sb", "type": "string", "nullable": False, "metadata": {}},
        {"name": "u", "type": "string", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), str(days(day)), None if key == LAST else str(days(day)),
              str(micros(instant)), str(micros(instant)), text, other, malformed, unit]
             for key, (day, instant, text, other, malformed, unit) in enumerate(ROWS)]}]

# (name, unit argument)
UNITS = [
    ("year", "'YEAR'"), ("yyyy", "'yyyy'"), ("yy", "'Yy'"), ("quarter", "'Quarter'"),
    ("mon", "'MON'"), ("month", "'month'"), ("mm", "'MM'"), ("week", "'WEEK'"),
    ("dd", "'DD'"), ("day", "'DAY'"), ("hour", "'HOUR'"), ("q", "'Q'"),
    ("invalid", "'invalid'"), ("empty", "''"), ("padded", "' MM'"), ("null", "NULL"),
    ("null_string", "CAST(NULL AS STRING)"),
]
DATE = "DATE '2024-05-17'"


def definitions():
    for name, unit in UNITS:
        yield f"column_date_{name}", f"SELECT k, trunc(dr, {unit}) AS r FROM inputs"
        yield f"literal_date_{name}", f"SELECT trunc({DATE}, {unit}) AS r"
    for name, unit in [("mm", "'MM'"), ("week", "'WEEK'"), ("dd", "'DD'")]:
        yield f"column_nullable_date_{name}", f"SELECT k, trunc(d, {unit}) AS r FROM inputs"
        yield f"column_timestamp_{name}", f"SELECT k, trunc(ts, {unit}) AS r FROM inputs"
        yield f"column_timestamp_ntz_{name}", f"SELECT k, trunc(tn, {unit}) AS r FROM inputs"
        yield f"column_string_{name}", f"SELECT k, trunc(s, {unit}) AS r FROM inputs"
    yield "column_date_unit_column", "SELECT k, trunc(dr, u) AS r FROM inputs"
    yield "literal_null_date_mm", "SELECT trunc(CAST(NULL AS DATE), 'MM') AS r"
    for name, unit in [("mm", "'MM'"), ("week", "'WEEK'")]:
        yield f"column_string_forms_{name}", f"SELECT k, trunc(sv, {unit}) AS r FROM inputs"
    yield "column_malformed_string_mm", "SELECT k, trunc(sb, 'MM') AS r FROM inputs"
    yield "column_malformed_string_unit_column", "SELECT k, trunc(sb, u) AS r FROM inputs"
    for name, unit in [("concat", "concat('M', 'M')"), ("upper", "upper('mm')"),
                       ("nullif", "nullif('MM', '')"), ("int", "1"), ("binary", "X'4D4D'")]:
        yield f"column_date_{name}_unit", f"SELECT k, trunc(dr, {unit}) AS r FROM inputs"
    yield "column_timestamp_concat_unit", \
        "SELECT k, trunc(ts, concat('M', 'M')) AS r FROM inputs"
    yield "column_date_int_unit_column", "SELECT k, trunc(dr, k) AS r FROM inputs"
    yield "column_int_mm", "SELECT k, trunc(k, 'MM') AS r FROM inputs"
    # A constant unsupported unit gives NULL without reading the date.
    for name, unit in [("dd", "'DD'"), ("null_unit", "NULL"),
                       ("concat_unit", "concat('D', 'D')")]:
        yield f"column_malformed_string_{name}", f"SELECT k, trunc(sb, {unit}) AS r FROM inputs"
    yield "column_cast_malformed_string_invalid", \
        "SELECT k, trunc(CAST(sb AS DATE), 'invalid') AS r FROM inputs"
    yield "literal_malformed_string_dd", "SELECT trunc('garbage', 'DD') AS r"
    yield "column_date_array_unit", "SELECT k, trunc(dr, array('MM')) AS r FROM inputs"
    yield "column_date_non_utf8_binary_unit", "SELECT k, trunc(dr, X'FF') AS r FROM inputs"
    yield "column_date_failing_unit", \
        "SELECT k, trunc(dr, CAST(CAST('x' AS INT) AS STRING)) AS r FROM inputs"
    for name, unit in [("mm", "'MM'"), ("week", "'WEEK'")]:
        yield f"column_far_date_{name}", \
            f"SELECT k, trunc(date_add(dr, 100000000), {unit}) AS r FROM inputs"
    # Spark's INT arithmetic wraps before the earliest DATE. Its Dataset API cannot decode a DATE
    # near the earliest one, which WEEK gives for k above 0.
    for name, unit, where in [("week", "'WEEK'", " WHERE k = 0"), ("mm", "'MM'", ""),
                              ("year", "'YEAR'", "")]:
        yield f"column_earliest_date_{name}", \
            f"SELECT k, trunc(date_sub(DATE '1970-01-01', 2147483647 - k), {unit}) AS r " \
            f"FROM inputs{where}"
    # The optimizer makes these units constant, so Spark does not read the date.
    for name, unit in [("coalesce", "coalesce('DD', u)"),
                       ("case", "CASE WHEN k IS NULL THEN 'MM' ELSE 'DD' END"),
                       ("current_date", "CAST(current_date() AS STRING)")]:
        yield f"column_malformed_string_{name}_unit", \
            f"SELECT k, trunc(sb, {unit}) AS r FROM inputs"
    # An aggregate in the date is computed whatever the unit.
    for name, date, unit, where in [
            ("dd", "max(dr)", "'DD'", ""), ("dd_where_false", "max(dr)", "'DD'", " WHERE false"),
            ("null_unit", "max(sb)", "NULL", ""), ("mm", "max(dr)", "'MM'", ""),
            ("coalesce_unit", "max(dr)", "coalesce('DD', max(u))", "")]:
        yield f"aggregate_date_{name}", f"SELECT trunc({date}, {unit}) AS r FROM inputs{where}"
    # Each of the 256 rows reads its unit once, so none is NULL. Spark rejects a volatile
    # aggregate argument, so the subquery computes TRUNC.
    yield "volatile_unit", ("SELECT count(r) AS n FROM (SELECT trunc(a.dr, IF(rand() < 0.5, "
                            "'MM', 'YEAR')) AS r FROM inputs a, inputs b, inputs c, inputs d)")


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
    spark = (SparkSession.builder.master("local[1]").appName("dar-trunc-units")
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
