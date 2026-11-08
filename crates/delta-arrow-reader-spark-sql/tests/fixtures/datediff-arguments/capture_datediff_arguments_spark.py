"""Capture Spark's argument checks of two-argument DATEDIFF and DATE_DIFF (#1005).

Records analyzed, optimized and executed schemas, typed internal-row values and failing
stages, with the innermost Java exception of a failure, for DATEDIFF and DATE_DIFF with INT,
BIGINT, DOUBLE, DECIMAL, BOOLEAN, interval, BINARY, ARRAY and MAP arguments in either
position, as literals and columns, beside NULL, in an unreached filter, and beside DATE,
TIMESTAMP and TIMESTAMP_NTZ controls. Spark casts only DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING
and NULL arguments to DATE, and rejects the others during analysis. Every query runs in both
ANSI modes in UTC and America/Los_Angeles. Run from the crate root in the reference environment
described in README.md; the output path must not exist.
"""
import argparse
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"
ZONES = ["UTC", "America/Los_Angeles"]

# (DATE, INT, DOUBLE, BOOLEAN)
ROWS = [
    ("2024-01-02", 1, 1.5, True),
    ("2024-03-11", -1, -0.5, False),
    (None, None, None, None),
]


def days(text):
    return (date.fromisoformat(text) - date(1970, 1, 1)).days


def double_bits(value):
    """A DOUBLE as the hex of its IEEE 754 bits."""
    return struct.pack(">d", value).hex()


INPUTS = [{
    "name": "inputs",
    "schema": {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "d", "type": "date", "nullable": True, "metadata": {}},
        {"name": "i", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "f", "type": "double", "nullable": True, "metadata": {}},
        {"name": "b", "type": "boolean", "nullable": True, "metadata": {}}]},
    "rows": [[str(key), None if day is None else str(days(day)),
              None if number is None else str(number),
              None if real is None else double_bits(real),
              None if flag is None else str(flag).lower()]
             for key, (day, number, real, flag) in enumerate(ROWS)]}]

DATE = "DATE '2024-01-01'"

# Arguments Spark rejects, as literals.
REJECTED = [
    ("int", "1"),
    ("bigint", "1L"),
    ("double", "1.5D"),
    ("decimal", "1.5"),
    ("boolean", "true"),
    ("day_interval", "INTERVAL '1' DAY"),
    ("day_to_second_interval", "INTERVAL '1 02:03:04' DAY TO SECOND"),
    ("month_interval", "INTERVAL '1' MONTH"),
    ("calendar_interval", "make_interval(0, 0, 0, 1)"),
    ("binary", "X'01'"),
    ("array", f"array({DATE})"),
    ("map", f"map(1, {DATE})"),
]


def definitions():
    for function in ["datediff", "date_diff"]:
        for name, value in REJECTED:
            yield f"literal_{function}_{name}_end", f"SELECT {function}({value}, {DATE}) AS r"
            yield f"literal_{function}_{name}_start", f"SELECT {function}({DATE}, {value}) AS r"
    yield "literal_datediff_both_int", "SELECT datediff(1, 2) AS r"
    yield "literal_datediff_int_null", "SELECT datediff(1, NULL) AS r"
    yield "literal_datediff_null_boolean", "SELECT datediff(NULL, true) AS r"
    for name, argument in [("int", "i"), ("double", "f"), ("boolean", "b")]:
        yield f"column_datediff_{name}_end", f"SELECT k, datediff({argument}, d) AS r FROM inputs"
        yield f"column_datediff_{name}_start", f"SELECT k, datediff(d, {argument}) AS r FROM inputs"
        yield f"column_date_diff_{name}_start", f"SELECT k, date_diff(d, {argument}) AS r FROM inputs"
    yield ("column_datediff_int_unreached",
           "SELECT k, datediff(d, i) AS r FROM inputs WHERE false")
    # Arguments Spark casts to DATE.
    yield "column_datediff_date", f"SELECT k, datediff(d, {DATE}) AS r FROM inputs"
    yield ("literal_datediff_timestamp",
           f"SELECT datediff(TIMESTAMP '2024-01-02 23:00:00', {DATE}) AS r")
    yield ("literal_date_diff_timestamp_ntz",
           f"SELECT date_diff({DATE}, TIMESTAMP_NTZ '2024-01-03 01:00:00') AS r")
    yield "literal_datediff_null", f"SELECT datediff(NULL, {DATE}) AS r"


def python_value(value, kind):
    if value is None:
        return None
    if kind == "integer":
        return int(value)
    if kind == "date":
        return date(1970, 1, 1) + timedelta(days=int(value))
    if kind == "double":
        return struct.unpack(">d", bytes.fromhex(value))[0]
    if kind == "boolean":
        return {"true": True, "false": False}[value]
    raise AssertionError(kind)


def encode_value(row, index, data_type, types):
    """Encode one internal-row or array value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, types.LongType):
        return str(row.getLong(index))
    if isinstance(data_type, types.DoubleType):
        return double_bits(row.getDouble(index))
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-datediff-arguments")
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
            "value_encoding": ("INT and DATE days since the epoch as int text; BIGINT as long "
                               "text; DOUBLE as the hex of its IEEE 754 bits; STRING as text; "
                               "BOOLEAN as true or false text; ARRAY as a list of its elements"),
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
