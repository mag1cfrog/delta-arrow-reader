"""Capture Spark DATE arithmetic fields with closed higher-order day operands (#831).

Records analyzed, optimized and executed schemas, typed internal-row values and
failing stages for DATE_ADD, DATEADD and DATE_SUB whose day operand is a closed
higher-order expression (a lambda over constant arrays), with literal, NULL,
registered-column, empty-table and near-maximum starts, literal-day controls and
higher-order results outside DATE arithmetic. Every query runs in both ANSI modes. Run
from the crate root in the reference environment described in README.md; the
output path must not exist.
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

# Day operands: closed higher-order expressions, then literal controls.
DAYS = [
    ("closed_string", "element_at(transform(array('1'), x -> x), 1)"),
    ("closed_int_cast", "CAST(element_at(transform(array(1), x -> x), 1) AS STRING)"),
    ("closed_null_string", "element_at(transform(array(CAST(NULL AS STRING)), x -> x), 1)"),
    ("closed_bad_string", "element_at(transform(array('bad'), x -> x), 1)"),
    ("closed_filter", "element_at(filter(array('1', '2'), x -> x > '1'), 1)"),
    ("closed_int", "element_at(transform(array(1), x -> x + 1), 1)"),
    ("closed_aggregate", "aggregate(array(1, 2), 0, (acc, x) -> acc + x)"),
    ("closed_sort", "element_at(array_sort(array('2', '1'), "
                    "(l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END), 1)"),
    ("closed_missing_index", "element_at(transform(array('1'), x -> x), 2)"),
    ("literal_string", "'1'"),
    ("literal_int", "1"),
]

STARTS = [
    ("literal", "DATE '1970-01-01'", ""),
    ("null", "CAST(NULL AS DATE)", ""),
    ("column", "d", " FROM day_inputs"),
    ("empty", "d", " FROM empty_days"),
    # The day before the DATE maximum, reached without parsing a six-digit year (#1011).
    ("near_max", "date_add(DATE '1970-01-01', 2147483646)", ""),
]

FUNCTIONS = ["date_add", "dateadd", "date_sub"]

# Closed higher-order results outside DATE arithmetic.
CONTROLS = [
    ("element_at_string", "SELECT element_at(transform(array('1'), x -> x), 1) AS r"),
    ("transform_array", "SELECT transform(array(1), x -> x + 1) AS r"),
    ("filter_size", "SELECT size(filter(array(1, 2), x -> x > 1)) AS r"),
    ("aggregate_int", "SELECT aggregate(array(1, 2), 0, (acc, x) -> acc + x) AS r"),
    # A lambda whose body ignores its parameter.
    ("constant_body", "SELECT element_at(transform(array('1'), x -> '2'), 1) AS r"),
    ("constant_body_day", "SELECT date_add(DATE '1970-01-01', "
                          "element_at(transform(array('1'), x -> '2'), 1)) AS r"),
    # The issue's reproduction, verbatim.
    ("issue_reproduction", "SELECT date_add(DATE '1970-01-01',\n"
                           "                element_at(transform(array('1'), x -> x), 1)) AS r"),
    ("exists_nullable", "SELECT exists(array(1, NULL), x -> x > 1) AS r"),
    ("forall_nullable", "SELECT forall(array(1, NULL), x -> x > 0) AS r"),
    # Definite results over a NULL element keep the nullable field.
    ("exists_true", "SELECT exists(array(1, NULL), x -> x > 0) AS r"),
    ("forall_false", "SELECT forall(array(1, NULL), x -> x > 1) AS r"),
    ("sort_comparator", "SELECT array_sort(array(2, 1), "
                        "(l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END) AS r"),
    # Spark plans ARRAY_SORT without a comparator as a lambda over a default comparator.
    ("sort_default", "SELECT element_at(array_sort(array('1')), 1) AS r"),
    ("sort_default_day", "SELECT date_add(DATE '1970-01-01', "
                         "element_at(array_sort(array('1')), 1)) AS r"),
    ("slice_transform", "SELECT slice(transform(array(1, 2), x -> x), 0, 1) AS r"),
    # Constant subexpressions inside a lambda body still fold during optimization.
    ("constant_body_error", "SELECT transform(array(1), x -> CAST('bad' AS INT)) AS r"),
    ("constant_body_error_empty",
     "SELECT transform(CAST(array() AS ARRAY<INT>), x -> CAST('bad' AS INT)) AS r"),
    # LIMIT requires a foldable expression.
    ("limit_lambda", "SELECT k FROM day_inputs LIMIT size(transform(array(1), x -> x))"),
]

SCHEMA = {"type": "struct", "fields": [
    {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
    {"name": "d", "type": "date", "nullable": True, "metadata": {}}]}
ROWS = [["0", "0"], ["1", None], ["2", "19781"]]


def definitions():
    for function in FUNCTIONS:
        for day_name, day in DAYS:
            for start_name, start, source in STARTS:
                key = "k, " if source == " FROM day_inputs" else ""
                yield {"id": f"{function}_{day_name}_{start_name}", "function": function,
                       "day": day_name, "start": start_name,
                       "sql": f"SELECT {key}{function}({start}, {day}) AS r{source}"}
    for name, sql in CONTROLS:
        yield {"id": f"control_{name}", "sql": sql}


def encode_value(row, index, data_type, types):
    """Encode one internal-row value by its Spark type, without string casts."""
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.IntegerType, types.DateType)):
        return str(row.getInt(index))
    if isinstance(data_type, types.BooleanType):
        return row.getBoolean(index)
    if isinstance(data_type, types.StringType):
        return row.getUTF8String(index).toString()
    if isinstance(data_type, types.ArrayType):
        values = row.getArray(index)
        return [encode_value(values, item, data_type.elementType, types)
                for item in range(values.numElements())]
    raise AssertionError(f"unexpected result type {data_type}")


def error_details(error, Py4JError):
    """Return the outermost error condition and the innermost Java exception class."""
    condition = getattr(error, "getCondition", lambda: None)()
    cause = getattr(error, "java_exception", None)
    if cause is None:
        # PySpark keeps the Java exception of a converted error in _origin.
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
    spark = (SparkSession.builder.master("local[1]").appName("dar-date-day-closed-lambda")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType.fromJson(SCHEMA)
        values = [[int(k), None if d is None else date(1970, 1, 1) + timedelta(days=int(d))]
                  for k, d in ROWS]
        spark.createDataFrame(values, schema).createOrReplaceTempView("day_inputs")
        spark.createDataFrame([], schema).createOrReplaceTempView("empty_days")
        stored = [[encode_value(row, index, field.dataType, types)
                   for index, field in enumerate(schema.fields)]
                  for row in spark.table("day_inputs")._jdf.queryExecution()
                  .executedPlan().executeCollect()]
        assert stored == ROWS, stored
        header = {
            "spark_version": spark.version, "spark_build": build,
            "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
            "python_version": sys.version.split()[0],
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "value_encoding": "INT and DATE days as int; BOOLEAN as true or false; "
                              "STRING as text; ARRAY as a list",
            "settings": {
                "spark.sql.session.timeZone": spark.conf.get("spark.sql.session.timeZone"),
                "spark.sql.optimizer.excludedRules":
                    spark.conf.get("spark.sql.optimizer.excludedRules")},
            "inputs": [{"name": "day_inputs", "schema": SCHEMA, "rows": ROWS},
                       {"name": "empty_days", "schema": SCHEMA, "rows": []}],
        }
        records = [header]
        cases = list(definitions())
        assert len({case["id"] for case in cases}) == len(cases)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for case in cases:
                records.append({**case, "ansi": ansi,
                                **capture(spark, case["sql"], types, Py4JError)})
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
