"""Capture Spark integer-day DATE arithmetic without Python datetime conversion.

Keep the original result schema and collect DATE values through UNIX_DATE.
Run in the existing Spark 4.2.0 reference environment.
"""
import json
import os
from pathlib import Path
import sys

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import encode


def capture_query(spark, sql):
    try:
        frame = spark.sql(sql)
        schema = frame.schema.jsonValue()
        assert frame.columns == ["r"], frame.columns
        if isinstance(frame.schema[0].dataType, types.DateType):
            frame = frame.selectExpr("unix_date(r) AS r")
        rows = [encode(row, frame.schema) for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        return {"schema": schema, "rows": rows}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        cause = getattr(error, "java_exception", None)
        while condition is None and cause is not None:
            try:
                condition = cause.getCondition()
            except Py4JError:
                pass
            cause = cause.getCause()
        assert condition is not None, (sql, str(error))
        return {"condition": condition}


def inputs():
    limits = [-2147483648, -2147483647, -1, 0, 1, 2147483646, 2147483647]
    pairs = [(base, days) for base in limits for days in limits]
    for name, nullable, values in [
        ("date_day_required", False, pairs),
        ("date_day_nullable", True, pairs + [(None, 1), (1, None), (None, None)]),
        ("date_day_empty", True, []),
    ]:
        schema = types.StructType([
            types.StructField("base", types.IntegerType(), nullable),
            types.StructField("days", types.IntegerType(), nullable),
        ])
        yield name, schema, values


def queries():
    functions = ["date_add", "dateadd", "date_sub"]
    for function in functions:
        for base in ["DATE '1970-01-01'", "DATE '1969-12-31'", "DATE '1970-01-02'"]:
            for days in ["0", "1", "-1", "2147483646", "2147483647", "-2147483647", "-2147483648"]:
                yield f"{function}_{base}_{days}", f"SELECT {function}({base},{days}) AS r"
        for table, _, _ in inputs():
            yield f"{function}_{table}", (
                f"SELECT {function}(date_from_unix_date(base),days) AS r FROM {table}")
            yield f"{function}_{table}_aliases", (
                f"SELECT {function}(d,n) AS r FROM "
                f"(SELECT date_from_unix_date(base) AS d,days AS n FROM {table}) aliases")
        for label, base, days in [
            ("null_date", "CAST(NULL AS DATE)", "1"),
            ("null_days", "DATE '1970-01-01'", "CAST(NULL AS INT)"),
            ("untyped_nulls", "NULL", "NULL"),
            ("byte_days", "DATE '1970-01-01'", "CAST(1 AS TINYINT)"),
            ("short_days", "DATE '1970-01-01'", "CAST(-1 AS SMALLINT)"),
            ("literal_string_days", "DATE '1970-01-01'", "'1'"),
            ("invalid_literal_days", "DATE '1970-01-01'", "'bad'"),
            ("wide_literal_days", "DATE '1970-01-01'", "'2147483648'"),
            ("long_days", "DATE '1970-01-01'", "CAST(1 AS BIGINT)"),
            ("decimal_days", "DATE '1970-01-01'", "1.0"),
            ("bool_days", "DATE '1970-01-01'", "true"),
            ("interval_days", "DATE '1970-01-01'", "INTERVAL 1 DAY"),
            ("integer_date", "1", "1"),
            ("invalid_date", "'bad'", "1"),
            ("string_date", "'2020-02-28'", "1"),
            ("timestamp_date", "TIMESTAMP '2020-02-28 23:59:59'", "1"),
        ]:
            yield f"{function}_{label}", f"SELECT {function}({base},{days}) AS r"
        yield f"{function}_string_column", (
            f"SELECT {function}(DATE '1970-01-01',CAST(days AS STRING)) AS r "
            "FROM date_day_required")
        for count, args in [(0, ""), (1, "DATE '1970-01-01'"),
                            (4, "DAY,1,DATE '1970-01-01',1")]:
            yield f"{function}_arity_{count}", f"SELECT {function}({args}) AS r"
    for name, expression in [
        ("month_end", "add_months(DATE '2020-01-31',1)"),
        ("year_interval", "DATE '2020-02-29' + INTERVAL 1 YEAR"),
        ("day_interval", "DATE '2020-02-28' + INTERVAL 1 DAY"),
        ("timestamp_day", "timestampadd(DAY,1,TIMESTAMP '2020-02-28 23:59:59')"),
        ("timestamp_month", "timestampadd(MONTH,1,TIMESTAMP '2020-01-31 23:59:59')"),
        ("dateadd_three", "dateadd(DAY,1,TIMESTAMP '2020-02-28 23:59:59')"),
        ("date_add_three", "date_add(DAY,1,TIMESTAMP '2020-02-28 23:59:59')"),
    ]:
        yield name, f"SELECT CAST({expression} AS STRING) AS r"


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-date-day-arithmetic")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "date_encoding": "unix_date",
                    "inputs": []}
        for name, schema, rows in inputs():
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
            metadata["inputs"].append({"name": name, "schema": schema.jsonValue(),
                                       "rows": [encode(row, schema) for row in rows]})
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in queries():
                records.append({"id": name, "ansi": ansi, "sql": sql,
                                "expected": capture_query(spark, sql)})
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records) - 1
        output = Path(__file__).with_name("date-day-arithmetic-spark.jsonl")
        output.write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records))
        print(json.dumps({"observations": len(records) - 1,
                          "errors": sum("condition" in r["expected"] for r in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
