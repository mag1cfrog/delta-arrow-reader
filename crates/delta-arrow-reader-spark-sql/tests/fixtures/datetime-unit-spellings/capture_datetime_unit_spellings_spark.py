"""Capture Spark's datetime unit spellings in TIMESTAMPDIFF and TIMESTAMPADD calls (#1056).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for TIMESTAMPDIFF, DATEDIFF,
DATE_DIFF, TIMEDIFF, TIMESTAMPADD, DATEADD and three-argument DATE_ADD with keyword, lower-case,
backquoted, string, double-quoted, parenthesized and unknown units, two-argument calls and
upper-case names, over literals and columns, also beside a column named like a unit, and for
backquoted names, DISTINCT, ALL, FILTER, IGNORE NULLS and OVER calls, adjacent, hex and
lower-case string units, fewer than two and four arguments, and named arguments. Spark's
grammar takes the unit only as a bare keyword in a plain call of a bare name; any other
spelling makes an ordinary function call. Every query runs in both ANSI modes in UTC and
America/Los_Angeles. Run from the crate root in the reference environment described in
README.md; the output path must not exist.
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
ZONES = ["UTC", "America/Los_Angeles"]

# (start, end) UTC instants.
ROWS = [
    ("2024-01-01 23:00:00", "2024-01-02 01:00:00"),
    ("2024-03-10 09:30:00", "2024-03-10 11:30:00"),
    ("2024-11-03 08:30:00", "2024-11-03 09:30:00"),
]


def micros(text):
    value = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "s", "type": "timestamp", "nullable": False, "metadata": {}},
        {"name": "e", "type": "timestamp", "nullable": False, "metadata": {}},
        {"name": "day", "type": "timestamp", "nullable": False, "metadata": {}}]},
    "rows": [[str(key), str(micros(start)), str(micros(end)), str(micros(start))]
             for key, (start, end) in enumerate(ROWS)]}]

PARTIAL_DAY = "TIMESTAMP '2024-01-01 23:00:00', TIMESTAMP '2024-01-02 01:00:00'"
START = "TIMESTAMP '2024-01-01 00:00:00'"


# (name, unit argument as written)
UNITS = [
    ("keyword", "DAY"),
    ("lower_keyword", "day"),
    ("backquoted", "`DAY`"),
    ("backquoted_lower", "`day`"),
    ("string", "'DAY'"),
    ("double_quoted", '"DAY"'),
    ("parenthesized_string", "('DAY')"),
    ("parenthesized_keyword", "(DAY)"),
    ("unknown_keyword", "FOO"),
]
DIFF_FUNCTIONS = ["timestampdiff", "datediff", "date_diff", "timediff"]
ADD_FUNCTIONS = ["timestampadd", "dateadd", "date_add"]


def definitions():
    for function in DIFF_FUNCTIONS:
        for name, unit in UNITS:
            yield f"literal_{function}_{name}", f"SELECT {function}({unit}, {PARTIAL_DAY}) AS r"
        yield (f"literal_{function}_two_arguments",
               f"SELECT {function}(DAY, TIMESTAMP '2024-01-01 23:00:00') AS r")
        yield (f"literal_upper_{function}_unknown_keyword",
               f"SELECT {function.upper()}(FOO, {PARTIAL_DAY}) AS r")
        yield (f"column_{function}_backquoted_lower",
               f"SELECT k, {function}(`day`, s, e) AS r FROM inputs")
        yield f"column_{function}_keyword", f"SELECT k, {function}(DAY, s, e) AS r FROM inputs"
    for function in ADD_FUNCTIONS:
        for name, unit in UNITS:
            yield f"literal_{function}_{name}", f"SELECT {function}({unit}, 1, {START}) AS r"
        yield (f"literal_upper_{function}_unknown_keyword",
               f"SELECT {function.upper()}(FOO, 1, {START}) AS r")
        yield (f"column_{function}_backquoted_lower",
               f"SELECT k, {function}(`day`, 1, s) AS r FROM inputs")
        yield f"column_{function}_keyword", f"SELECT k, {function}(DAY, k, s) AS r FROM inputs"
    # A quantity beyond INT with string and backquoted units (#981 review).
    for name, unit in [("string", "'day'"), ("backquoted", "`day`")]:
        yield (f"literal_timestampadd_{name}_bigint_quantity",
               f"SELECT timestampadd({unit}, 4294967297L, {START}) AS r")
    # Ordinary calls named in upper case, which Spark's registry names in lower case.
    yield "column_upper_datediff_backquoted_lower", "SELECT k, DATEDIFF(`day`, s, e) AS r FROM inputs"
    yield "column_upper_dateadd_backquoted_lower", "SELECT k, DATEADD(`day`, 1, s) AS r FROM inputs"
    # Calls outside the grammar: a backquoted name, DISTINCT, ALL, FILTER, IGNORE NULLS and OVER.
    yield "column_backquoted_name_datediff", "SELECT k, `datediff`(DAY, s, e) AS r FROM inputs"
    yield ("column_backquoted_name_timestampdiff",
           "SELECT k, `timestampdiff`(DAY, s, e) AS r FROM inputs")
    yield "column_distinct_datediff", "SELECT k, datediff(DISTINCT DAY, s, e) AS r FROM inputs"
    yield "column_all_timestampadd", "SELECT k, timestampadd(ALL DAY, k, s) AS r FROM inputs"
    yield ("column_filter_datediff",
           "SELECT k, datediff(DAY, s, e) FILTER (WHERE k > 0) AS r FROM inputs")
    yield ("column_ignore_nulls_timestampdiff",
           "SELECT k, timestampdiff(DAY, s, e) IGNORE NULLS AS r FROM inputs")
    yield ("column_window_timestampdiff",
           "SELECT k, timestampdiff(DAY, s, e) OVER (ORDER BY k) AS r FROM inputs")
    # Other unit spellings.
    yield ("literal_datediff_adjacent_strings",
           f"SELECT datediff('DA' 'Y', {PARTIAL_DAY}) AS r")
    yield "literal_timestampadd_hex_unit", f"SELECT timestampadd(X'01', 1, {START}) AS r"
    yield ("literal_timestampdiff_lower_string",
           f"SELECT timestampdiff('day', {PARTIAL_DAY}) AS r")
    yield ("literal_datediff_four_arguments",
           f"SELECT datediff(DAY, {PARTIAL_DAY}, {START}) AS r")
    # Fewer than two arguments, and named arguments.
    yield "column_datediff_one_argument", "SELECT k, datediff(s) AS r FROM inputs"
    yield "literal_date_diff_no_arguments", "SELECT date_diff() AS r"
    yield "column_dateadd_one_argument", "SELECT k, dateadd(s) AS r FROM inputs"
    yield "column_date_add_one_argument", "SELECT k, date_add(s) AS r FROM inputs"
    yield ("column_named_timestampdiff",
           "SELECT k, timestampdiff(DAY, s, endTimestamp => e) AS r FROM inputs")
    yield ("column_named_timestampadd",
           "SELECT k, timestampadd(DAY, k, timestamp => s) AS r FROM inputs")


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "timestamp":
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row or array value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, types.IntegerType):
        return str(row.getInt(index))
    if isinstance(data_type, (types.LongType, types.TimestampType)):
        return str(row.getLong(index))
    if isinstance(data_type, types.StringType):
        return row.getUTF8String(index).toString()
    if isinstance(data_type, types.BooleanType):
        return row.getBoolean(index)
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-datetime-unit-spellings")
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
            "value_encoding": ("INT as int text; BIGINT and TIMESTAMP UTC-epoch microseconds "
                               "as long text; STRING as text; BOOLEAN as a JSON boolean; ARRAY "
                               "as a list of its elements"),
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
