"""Capture Spark's nullability of casts between TIMESTAMP_NTZ and DATE (#1003).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for explicit CAST and TRY_CAST between
TIMESTAMP_NTZ and DATE in both directions, and for the implicit casts of TO_DATE, DATE_ADD,
DATE_SUB, DATEDIFF, LAST_DAY, YEAR, MONTH, DAY, DAYOFMONTH, DAYOFYEAR, QUARTER, WEEKOFYEAR,
DAYNAME, MONTHNAME, ADD_MONTHS, NEXT_DAY, TRUNC, UNIX_DATE, WEEKDAY and DAYOFWEEK arguments, of
DATE(), TO_TIMESTAMP_NTZ, the date fields of EXTRACT, DATE_PART and DATEPART, also by their
aliases, of a DATE compared with a TIMESTAMP_NTZ, and of a DATE beside a TIMESTAMP_NTZ in CASE,
IF, COALESCE and UNION ALL, over NOT NULL and nullable columns and literals, and of TO_TIMESTAMP
and TRY_TO_TIMESTAMP of a DATE with the session timestamp type TIMESTAMP_NTZ. Five cases owned
elsewhere are recorded too. Spark declares a cast between the two types nullable whatever its input, so a NOT NULL
column's cast is nullable at every stage, and a literal's cast is nullable until constant
folding. Every query runs in both ANSI modes in UTC and America/Los_Angeles. Run from the crate
root in the reference environment described in README.md; the output path must not exist.
"""
import argparse
from datetime import date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles"]

# (TIMESTAMP_NTZ, DATE); the nullable copies are NULL in the last row.
ROWS = [
    ("2024-03-10 02:30:00", "2024-03-10"),  # Los Angeles skips 02:30 on this day.
    ("2024-11-03 01:30:00", "2024-11-03"),  # Los Angeles repeats 01:30 on this day.
    ("1969-12-31 23:59:59.999999", "1969-12-31"),
    ("0001-01-01 00:00:00", "9999-12-31"),
]


def micros(text):
    value = datetime.fromisoformat(text)
    delta = value - datetime(1970, 1, 1)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def days(text):
    return (date.fromisoformat(text) - date(1970, 1, 1)).days


LAST = len(ROWS) - 1
INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "tn", "type": "timestamp_ntz", "nullable": False, "metadata": {}},
        {"name": "n", "type": "timestamp_ntz", "nullable": True, "metadata": {}},
        {"name": "dr", "type": "date", "nullable": False, "metadata": {}},
        {"name": "d", "type": "date", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), str(micros(local)), None if key == LAST else str(micros(local)),
              str(days(day)), None if key == LAST else str(days(day))]
             for key, (local, day) in enumerate(ROWS)]}]

NTZ = "TIMESTAMP_NTZ '2024-01-02 00:30:00'"
DATE = "DATE '2024-01-02'"

# (name, expression over a TIMESTAMP_NTZ x and a DATE y)
CASES = [
    ("cast_to_date", "CAST({x} AS DATE)"),
    ("try_cast_to_date", "TRY_CAST({x} AS DATE)"),
    ("to_date", "to_date({x})"),
    ("date_add", "date_add({x}, 1)"),
    ("date_sub", "date_sub({x}, 1)"),
    ("datediff_end", "datediff({x}, {y})"),
    ("datediff_start", "datediff({y}, {x})"),
    ("last_day", "last_day({x})"),
    ("year", "year({x})"),
    ("month", "month({x})"),
    ("dayofmonth", "dayofmonth({x})"),
    ("weekday", "weekday({x})"),
    ("dayofweek", "dayofweek({x})"),
    ("day", "day({x})"),
    ("dayofyear", "dayofyear({x})"),
    ("quarter", "quarter({x})"),
    ("weekofyear", "weekofyear({x})"),
    ("dayname", "dayname({x})"),
    ("monthname", "monthname({x})"),
    ("add_months", "add_months({x}, 1)"),
    ("next_day", "next_day({x}, 'MO')"),
    ("trunc", "trunc({x}, 'MM')"),
    ("unix_date", "unix_date({x})"),
    ("cast_to_timestamp_ntz", "CAST({y} AS TIMESTAMP_NTZ)"),
    ("try_cast_to_timestamp_ntz", "TRY_CAST({y} AS TIMESTAMP_NTZ)"),
    ("date_less_than_timestamp_ntz", "{y} < {x}"),
    ("date_equals_timestamp_ntz", "{y} = {x}"),
    ("date_alias", "date({x})"),
    ("extract_year", "extract(YEAR FROM {x})"),
    ("extract_quarter", "extract(QUARTER FROM {x})"),
    ("extract_month", "extract(MONTH FROM {x})"),
    ("extract_week", "extract(WEEK FROM {x})"),
    ("extract_day", "extract(DAY FROM {x})"),
    ("extract_dayofweek", "extract(DAYOFWEEK FROM {x})"),
    ("extract_dow_iso", "extract(DOW_ISO FROM {x})"),
    ("extract_doy", "extract(DOY FROM {x})"),
    ("extract_yearofweek", "extract(YEAROFWEEK FROM {x})"),
    ("extract_hour", "extract(HOUR FROM {x})"),
    ("extract_y", "extract(Y FROM {x})"),
    ("extract_qtr", "extract(QTR FROM {x})"),
    ("extract_mons", "extract(MONS FROM {x})"),
    ("extract_w", "extract(W FROM {x})"),
    ("extract_d", "extract(D FROM {x})"),
    ("extract_days", "extract(DAYS FROM {x})"),
    ("date_part_day", "date_part('day', {x})"),
    ("datepart_month", "datepart('MONTH', {x})"),
    ("to_timestamp_ntz", "to_timestamp_ntz({y})"),
    ("coalesce_date_timestamp_ntz", "coalesce({y}, {x})"),
]


# Cases run with the session timestamp type TIMESTAMP_NTZ: (name, expression over a DATE y).
NTZ_SESSION_CASES = [
    ("to_timestamp", "to_timestamp({y})"),
    ("try_to_timestamp", "try_to_timestamp({y})"),
]

# Column cases with a row condition: (name, expression over a TIMESTAMP_NTZ x and a DATE y).
COLUMN_CASES = [
    ("case_date_or_timestamp_ntz", "CASE WHEN k > 1 THEN {y} ELSE {x} END"),
    ("if_date_or_timestamp_ntz", "if(k > 1, {y}, {x})"),
]


def definitions():
    """Yield (name, SQL, session timestamp type or None)."""
    for name, sql in plain_definitions():
        yield name, sql, None
    for name, expression in NTZ_SESSION_CASES:
        for prefix, y in [("required", "dr"), ("nullable", "d")]:
            yield (f"ntz_session_{prefix}_{name}",
                   f"SELECT k, {expression.format(y=y)} AS r FROM inputs", "TIMESTAMP_NTZ")
        yield (f"ntz_session_literal_{name}", f"SELECT {expression.format(y=DATE)} AS r",
               "TIMESTAMP_NTZ")


def plain_definitions():
    for name, expression in CASES:
        yield f"required_{name}", f"SELECT k, {expression.format(x='tn', y='dr')} AS r FROM inputs"
        yield f"nullable_{name}", f"SELECT k, {expression.format(x='n', y='d')} AS r FROM inputs"
        yield f"literal_{name}", f"SELECT {expression.format(x=NTZ, y=DATE)} AS r"
    for name, expression in COLUMN_CASES:
        yield f"required_{name}", f"SELECT k, {expression.format(x='tn', y='dr')} AS r FROM inputs"
        yield f"nullable_{name}", f"SELECT k, {expression.format(x='n', y='d')} AS r FROM inputs"
    yield ("required_union_date_timestamp_ntz",
           "SELECT k, dr AS r FROM inputs UNION ALL SELECT k, tn AS r FROM inputs")
    yield ("nullable_union_date_timestamp_ntz",
           "SELECT k, d AS r FROM inputs UNION ALL SELECT k, n AS r FROM inputs")
    # Owned elsewhere: a constant over an input proven empty (#1190), an IN list (#1191),
    # TRUNC to an unsupported unit (#1192) and comparisons with a literal, which Spark's
    # optimizer unwraps (#1197).
    yield ("literal_cast_to_date_where_false",
           f"SELECT CAST({NTZ} AS DATE) AS r FROM inputs WHERE false")
    yield ("required_date_in_timestamp_ntz",
           "SELECT k, dr IN (TIMESTAMP_NTZ '2024-03-10 00:00:00', tn) AS r FROM inputs")
    yield ("required_trunc_hour", "SELECT k, trunc(tn, 'HOUR') AS r FROM inputs WHERE k < 3")
    yield ("required_date_less_than_timestamp_ntz_literal",
           "SELECT k, dr < TIMESTAMP_NTZ '2024-03-10 00:30:00' AS r FROM inputs")
    yield ("required_cast_to_date_equals_date_literal",
           "SELECT k, CAST(tn AS DATE) = DATE '2024-03-10' AS r FROM inputs")


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "date":
        return date(1970, 1, 1) + timedelta(days=int(value))
    if kind == "timestamp_ntz":
        return datetime(1970, 1, 1) + timedelta(microseconds=int(value))
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row or array value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.TimestampNTZType)):
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-date-ntz-casts")
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
                               "TIMESTAMP_NTZ wall-clock microseconds as long text; STRING as "
                               "text; BOOLEAN as true or false text; ARRAY as a list of its "
                               "elements"),
            "settings": {
                "spark.sql.timestampType": spark.conf.get("spark.sql.timestampType"),
                "spark.sql.optimizer.excludedRules":
                    spark.conf.get("spark.sql.optimizer.excludedRules")},
            "inputs": INPUTS,
        }
        records = [header]
        cases = list(definitions())
        assert len({name for name, _, _ in cases}) == len(cases)
        default_timestamp_type = spark.conf.get("spark.sql.timestampType")
        for zone in ZONES:
            spark.conf.set("spark.sql.session.timeZone", zone)
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for name, sql, timestamp_type in cases:
                    record = {"id": name, "timezone": zone, "ansi": ansi, "sql": sql}
                    if timestamp_type is not None:
                        record["timestamp_type"] = timestamp_type
                    spark.conf.set("spark.sql.timestampType",
                                   timestamp_type or default_timestamp_type)
                    records.append({**record, **capture(spark, sql, types, Py4JError)})
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
