"""Capture Spark date/time separators in timestamp strings and typed literals.

Run from the crate root in the reference environment described in
tests/fixtures/timestamp-separators/README.md. The output path must not exist.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

SPARK_REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
TIMEZONES = ["UTC", "America/New_York"]
CONVERT_TO_LOCAL_RELATION = "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation"

# Names describe the separator between the date and time, or the control kept unchanged.
VALUES = [
    ("upper_t", "2026-01-02T01:02:03.123456Z"),
    ("lower_t", "2026-01-02t01:02:03.123456Z"),
    ("lower_t_plain", "2026-01-02t01:02:03"),
    ("lower_t_offset", "2026-01-02t01:02:03-07:00"),
    ("lower_t_region", "2026-01-02t01:02:03 America/Los_Angeles"),
    ("lower_t_hour", "2026-01-02t01"),
    ("lower_t_only", "2026-01-02t"),
    ("upper_t_only", "2026-01-02T"),
    ("space", "2026-01-02 01:02:03"),
    ("space_fraction", "2026-01-02 01:02:03.1"),
    ("space_nanos", "2026-01-02 01:02:03.123456789"),
    ("upper_t_offset", "2026-01-02T01:02:03+05:30"),
    ("upper_t_utc_prefix", "2026-01-02T01:02:03UTC+01:00"),
    ("space_region", "2026-01-02 01:02:03 UTC"),
    ("upper_t_hour", "2026-01-02T01"),
    ("upper_t_minute", "2026-01-02T01:02"),
    ("double_space", "2026-01-02  01:02:03"),
    ("space_upper_t", "2026-01-02 T01:02:03"),
    ("upper_t_space", "2026-01-02T 01:02:03"),
    ("padded_upper_t", "2026-01-02 T 01:02:03"),
    ("space_lower_t", "2026-01-02 t01:02:03"),
    ("tab", "2026-01-02\t01:02:03"),
    ("other_letter", "2026-01-02X01:02:03"),
    ("month_space", "2026-01 01:02:03"),
    ("month_upper_t", "2026-01T01:02:03"),
    ("year_upper_t", "2026T01:02:03"),
    ("month", "2026-01"),
    ("year", "2026"),
    ("date", "2026-01-02"),
    ("date_trailing_space", "2026-01-02 "),
    ("negative_year_lower_t", "-0001-01-02t01:02:03"),
    ("negative_year_upper_t", "-0001-01-02T01:02:03"),
    ("pre_epoch_upper_t", "1969-12-31T23:59:59.999999"),
    ("pre_epoch_lower_t", "1969-12-31t23:59:59.999999"),
    ("new_york_gap_upper_t", "2026-03-08T02:30:00"),
    ("new_york_gap_lower_t", "2026-03-08t02:30:00"),
    ("new_york_overlap_upper_t", "2026-11-01T01:30:00"),
    ("new_york_overlap_lower_t", "2026-11-01t01:30:00"),
    ("empty", ""),
    ("bad", "bad"),
]

# Each callable receives a STRING expression.
OPERATORS = [
    ("cast_timestamp", lambda e: f"CAST({e} AS TIMESTAMP)"),
    ("cast_timestamp_ntz", lambda e: f"CAST({e} AS TIMESTAMP_NTZ)"),
    ("try_cast_timestamp", lambda e: f"TRY_CAST({e} AS TIMESTAMP)"),
    ("try_cast_timestamp_ntz", lambda e: f"TRY_CAST({e} AS TIMESTAMP_NTZ)"),
    ("to_timestamp", lambda e: f"TO_TIMESTAMP({e})"),
    ("to_timestamp_ntz", lambda e: f"TO_TIMESTAMP_NTZ({e})"),
    ("date_format", lambda e: f"DATE_FORMAT({e},'yyyy-MM-dd HH:mm:ss.SSSSSS')"),
    ("equals_timestamp", lambda e: f"({e} = TIMESTAMP '2026-01-02 01:02:03.123456')"),
    ("cast_date", lambda e: f"CAST({e} AS DATE)"),
]

LITERALS = ["TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ"]


INPUT_TABLE = "separator_inputs"
NULL_KEY = len(VALUES)


def input_provider():
    schema = {"type": "struct", "fields": [
        {"name": "k", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "s", "type": "string", "nullable": True, "metadata": {}}]}
    # Recorded inputs encode integers as decimal strings.
    rows = [[str(key), value] for key, (_, value) in enumerate(VALUES)] + [[str(NULL_KEY), None]]
    return {"name": INPUT_TABLE, "schema": schema, "rows": rows}


def queries():
    for key, (name, value) in enumerate(VALUES):
        text = f"decode(unhex('{value.encode().hex()}'),'UTF-8')"
        for operator, expression in OPERATORS:
            yield name, operator, "literal", f"SELECT {expression(text)} AS r"
            yield name, operator, "column", (
                f"SELECT {expression('s')} AS r FROM {INPUT_TABLE} "
                f"WHERE k IN ({key},{NULL_KEY}) ORDER BY k")
        if value.isprintable() and "'" not in value:
            for literal in LITERALS:
                yield name, literal.lower() + "_literal", "typed_literal", \
                    f"SELECT {literal} '{value}' AS r"


def encode_rows(internal_rows, data_type, types):
    rows = []
    for row in internal_rows:
        if row.isNullAt(0):
            value = None
        elif isinstance(data_type, (types.TimestampType, types.TimestampNTZType)):
            value = str(row.getLong(0))
        elif isinstance(data_type, types.DateType):
            value = str(row.getInt(0))
        elif isinstance(data_type, types.BooleanType):
            value = "true" if row.getBoolean(0) else "false"
        elif isinstance(data_type, types.StringType):
            value = row.getUTF8String(0).toString()
        else:
            raise AssertionError(f"unexpected result type {data_type}")
        rows.append([value])
    return rows


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
    spark = (SparkSession.builder.master("local[1]").appName("dar-timestamp-separators")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             # Evaluate registered-input projections during execution, not optimization.
             .config("spark.sql.optimizer.excludedRules", CONVERT_TO_LOCAL_RELATION)
             .getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        jvm = spark.sparkContext._jvm
        header = {
            "spark_version": spark.version,
            "spark_build": build,
            "java_version": jvm.System.getProperty("java.version"),
            "python_version": sys.version.split()[0],
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "timestamp_encoding": ("TIMESTAMP rows are microseconds since the UTC epoch; "
                                   "TIMESTAMP_NTZ rows are wall-clock microseconds; "
                                   "DATE rows are days since 1970-01-01"),
            "settings": {
                "spark.sql.timestampType": spark.conf.get("spark.sql.timestampType"),
                "spark.sql.optimizer.excludedRules": spark.conf.get("spark.sql.optimizer.excludedRules"),
            },
            "inputs": [input_provider()],
        }
        provider = header["inputs"][0]
        spark.createDataFrame([[int(key), value] for key, value in provider["rows"]],
                              types.StructType.fromJson(provider["schema"])) \
            .createOrReplaceTempView(INPUT_TABLE)
        records = [header]
        for timezone in TIMEZONES:
            spark.conf.set("spark.sql.session.timeZone", timezone)
            for name, operator, form, sql in queries():
                for ansi in [True, False]:
                    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                    record = {"id": f"{name}_{operator}_{form}", "value": name,
                              "operator": operator, "form": form, "sql": sql,
                              "ansi": ansi, "timezone": timezone}
                    stages = {}
                    phase = "sql"
                    try:
                        frame = spark.sql(sql)
                        assert frame.columns == ["r"], frame.columns
                        execution = frame._jdf.queryExecution()
                        stages["analyzed"] = json.loads(execution.analyzed().schema().json())
                        phase = "optimized"
                        stages["optimized"] = json.loads(execution.optimizedPlan().schema().json())
                        phase = "collect"
                        executed = execution.executedPlan()
                        internal_rows = executed.executeCollect()
                        stages["executed"] = json.loads(executed.schema().json())
                        data_type = frame.schema[0].dataType
                        record["expected"] = {
                            "status": "ok", "type": data_type.typeName(),
                            "rows": encode_rows(internal_rows, data_type, types)}
                    except Exception as error:
                        condition = error_condition(error, Py4JError)
                        if condition is None:
                            raise
                        record["expected"] = {"status": "error", "condition": condition,
                                              "message": str(error).splitlines()[0]}
                        record["spark_error_phase"] = phase
                    record["schema_stages"] = stages
                    records.append(record)
        keys = {(r["id"], r["ansi"], r["timezone"]) for r in records[1:]}
        assert len(keys) == len(records) - 1
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
