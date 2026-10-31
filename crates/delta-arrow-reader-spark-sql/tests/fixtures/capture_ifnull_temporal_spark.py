"""Capture IFNULL/NVL temporal coercion and COALESCE controls with Spark 4.2.0."""

import datetime
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable


def reference_cases():
    for sql_type, text in [
        ("DATE", "2026-01-02"),
        ("TIMESTAMP", "2026-01-02 03:04:05.123456"),
        ("TIMESTAMP_NTZ", "2026-01-02 03:04:05.123456"),
    ]:
        temporal = f"CAST('{text}' AS {sql_type})"
        missing = f"CAST(NULL AS {sql_type})"
        for name, arguments, source in [
            ("null_left", f"{missing}, '{text}'", ""),
            ("string_left", f"'{text}', {missing}", ""),
            ("null_string", f"CAST(NULL AS STRING), {temporal}", ""),
            ("both_null", f"{missing}, CAST(NULL AS STRING)", ""),
            ("dead_invalid", f"{temporal}, 'bad'", ""),
            ("live_invalid", f"{missing}, 'bad'", ""),
            ("column", f"CAST(v AS {sql_type}), s", f" FROM VALUES (0, '{text}', 'bad'), (1, NULL, '{text}'), (2, NULL, NULL) t(k,v,s) ORDER BY k"),
            ("empty", f"CAST(v AS {sql_type}), s", f" FROM VALUES ('{text}', 'bad') t(v,s) WHERE false"),
        ]:
            yield f"{sql_type.lower()}_{name}", arguments, source
    yield "integer", "v, 9", " FROM VALUES (0, 3), (1, NULL) t(k,v) ORDER BY k"
    yield "string", "v, 'fallback'", " FROM VALUES (0, 'value'), (1, NULL) t(k,v) ORDER BY k"
    yield "same_date", "CAST(NULL AS DATE), DATE '2026-01-02'", ""
    yield "numeric_promotion", "CAST(NULL AS INT), 2D", ""


def encode_reference_value(value, data_type):
    if isinstance(value, datetime.datetime):
        # PySpark collects TIMESTAMP in the process timezone; NTZ is wall time.
        if data_type == "timestamp":
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        elapsed = value - datetime.datetime(1970, 1, 1)
        return str((elapsed.days * 86400 + elapsed.seconds) * 1_000_000 + elapsed.microseconds)
    return None if value is None else str(value)


spark = (
    SparkSession.builder.master("local[1]")
    .appName("dar-ifnull-temporal-reference")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)
records = []
try:
    assert spark.version == "4.2.0"
    spark.sparkContext.setLogLevel("ERROR")
    for timezone in ["UTC", "America/Phoenix"]:
        spark.conf.set("spark.sql.session.timeZone", timezone)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for function in ["IFNULL", "NVL", "COALESCE"]:
                queries = list(reference_cases())
                if function != "COALESCE":
                    queries += [(f"arity_{n}", ",".join(["NULL"] * n), "") for n in [0, 1, 3]]
                    if not ansi:
                        queries += [
                            ("int_string", "CAST(NULL AS INT), '9'", ""),
                            ("int_bad_string", "CAST(NULL AS INT), 'bad'", ""),
                        ]
                else:
                    queries += [
                        ("one_argument", "DATE '2026-01-02'", ""),
                        ("three_arguments", "CAST(NULL AS DATE), CAST(NULL AS STRING), '2026-01-02'", ""),
                        ("same_decimal", "CAST(NULL AS DECIMAL(10,2)), CAST(3.25 AS DECIMAL(10,2))", ""),
                    ]
                for name, arguments, source in queries:
                    sql = f"SELECT {function}({arguments}) AS r{source}"
                    try:
                        frame = spark.sql(sql)
                        schema = frame.schema.jsonValue()
                        rows = [[encode_reference_value(row[0], schema["fields"][0]["type"])] for row in frame.collect()]
                        expected = dict(status="ok", schema=schema, rows=rows)
                    except Exception as error:
                        condition = getattr(error, "getCondition", lambda: None)()
                        if condition not in {"CAST_INVALID_INPUT", "WRONG_NUM_ARGS.WITHOUT_SUGGESTION"}:
                            raise
                        expected = dict(status="error", condition=condition)
                    records.append(dict(id=f"{function.lower()}_{name}", sql=sql, ansi=ansi, timezone=timezone, expected=expected))
    output = Path(__file__).with_name("ifnull-temporal-spark.jsonl")
    metadata = dict(spark_version=spark.version, timestamp_encoding="microseconds since 1970-01-01; TIMESTAMP in UTC, TIMESTAMP_NTZ in wall time")
    output.write_text("\n".join(json.dumps(row, separators=(",", ":")) for row in [metadata, *records]) + "\n")
    print(json.dumps(dict(observations=len(records), errors=sum(row["expected"]["status"] == "error" for row in records))))
finally:
    spark.stop()
