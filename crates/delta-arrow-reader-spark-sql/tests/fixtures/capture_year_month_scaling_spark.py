"""Capture Spark 4.2.0 references to a fresh output path passed as the argument.

The committed year-month-scaling-spark.jsonl beside this script supplies only
the original consumer SQL and ANSI settings. Spark recomputes every result.
"""
import decimal
import hashlib
import json
import os
from pathlib import Path
import re
import struct
import sys

os.environ["PYSPARK_YM_INTERVAL_LEGACY"] = "1"
from pyspark.sql import SparkSession, functions, types

ORIGINAL = Path(__file__).with_name("year-month-scaling-spark.jsonl")
SOURCE_SHA256 = "474f6eb7b1f41f75e77dd9d218c69c2362e0a65b7dc99f94fce7b471addda061"
ORIGINAL_SHA256 = "a69f5483344ac3a986eb3cbb35a4fe2dafb71cbd70301a32961ea840410386bf"


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, (types.FloatType, types.DoubleType)):
        return struct.pack("!f" if isinstance(data_type, types.FloatType) else "!d", value).hex()
    return str(value)


def definitions():
    for name, interval in [("year", "INTERVAL '2' YEAR"),
                           ("year_month", "INTERVAL '2-3' YEAR TO MONTH"),
                           ("month", "INTERVAL '27' MONTH")]:
        for operation, expression in [("left_multiply", f"id*({interval})"),
                                      ("right_multiply", f"({interval})*id"),
                                      ("divide", f"({interval})/2"),
                                      ("timestamp_computed", "TIMESTAMP '2020-01-01 00:00:00.123456'+"
                                       f"id*({interval})")]:
            yield f"aliased_{name}_{operation}", "aliased_control", f"SELECT {expression} AS r FROM inputs"
    for name, months, number in [
        ("positive_tie", 27, "2"), ("negative_interval", -27, "2"),
        ("negative_number", 27, "-2"), ("negative_both", -27, "-2"),
        ("fractional_decimal", 3, "0.5"), ("fractional_double", -3, "CAST(0.5 AS DOUBLE)"),
        ("fractional_float", 3, "CAST(0.5 AS FLOAT)"),
        ("int_max", 2147483647, "1"), ("int_min", -2147483648, "1"),
        ("multiply_overflow", 2147483647, "2"),
        ("min_negative_one", -2147483648, "-1"),
        ("min_negative_one_long", -2147483648, "CAST(-1 AS BIGINT)"),
        ("min_negative_one_decimal", -2147483648, "CAST(-1 AS DECIMAL(2,0))"),
        ("min_negative_one_double", -2147483648, "CAST(-1 AS DOUBLE)"),
        ("long_max", 2, "CAST(9223372036854775807 AS BIGINT)"),
        ("zero", 27, "0"), ("zero_decimal", 27, "CAST(0 AS DECIMAL(2,1))"),
        ("negative_zero_double", 27, "CAST('-0.0' AS DOUBLE)"),
        ("null_number", 27, "CAST(NULL AS INT)"),
        ("decimal_above_tie", 1, "CAST(0.50000000000000000000000000000000000001 AS DECIMAL(38,38))"),
        ("decimal_below_tie", 1, "CAST(0.49999999999999999999999999999999999999 AS DECIMAL(38,38))"),
        ("decimal_division_rounding", 1, "CAST(2.0000000000000000000000000000000000001 AS DECIMAL(38,37))"),
        ("decimal_tiny", 1, "CAST(0.00000000000000000000000000000000000001 AS DECIMAL(38,38))"),
        ("nan", 1, "CAST('NaN' AS DOUBLE)"),
        ("infinity", 1, "CAST('Infinity' AS DOUBLE)"),
    ]:
        interval = f"INTERVAL '{months}' MONTH"
        for operation, expression in [("left_multiply", f"({number})*({interval})"),
                                      ("right_multiply", f"({interval})*({number})"),
                                      ("divide", f"({interval})/({number})")]:
            yield f"literal_{name}_{operation}", "literal", f"SELECT {expression} AS r"
    for name, expression in [
        ("null_interval_multiply", "CAST(NULL AS INTERVAL YEAR TO MONTH)*2"),
        ("null_interval_divide_zero", "CAST(NULL AS INTERVAL YEAR TO MONTH)/0"),
        ("untyped_null_multiply", "NULL*INTERVAL 1 MONTH"),
        ("untyped_null_divide", "INTERVAL 1 MONTH/NULL"),
        ("day_time_multiply", "INTERVAL '1.123456' SECOND*2"),
        ("day_time_divide", "INTERVAL '1.123456' SECOND/2"),
    ]:
        yield name, "control", f"SELECT {expression} AS r"
    for table in ["scale_integer_required", "scale_integer_nullable", "scale_long", "scale_decimal",
                  "scale_float", "scale_double", "scale_overflow", "scale_zero", "scale_min_divide"]:
        for operation, expression in [("left_multiply", "n*m"), ("right_multiply", "m*n"),
                                      ("divide", "m/n")]:
            yield f"{table}_{operation}", "column", f"SELECT {expression} AS r FROM {table}"
    yield "column_timestamp", "column", ("SELECT TIMESTAMP '2020-01-01 00:00:00.123456'+"
                                         "m*n AS r FROM scale_integer_nullable")


def capture(spark, sql):
    result = {}
    phase = "public_schema"
    try:
        frame = spark.sql(sql)
        result["schema"] = frame.schema.jsonValue()
        phase = "analyzed_schema"
        result["analyzed_schema"] = json.loads(frame._jdf.queryExecution().analyzed().schema().json())
        phase = "optimized_schema"
        result["optimized_schema"] = json.loads(frame._jdf.queryExecution().optimizedPlan().schema().json())
        phase = "collect"
        columns = [functions.col("`" + field.name.replace("`", "``") + "`")
                   for field in frame.schema]
        values = frame.select(*[column.cast("string").alias(str(index))
                                for index, column in enumerate(columns)])
        result["rows"] = sorted([[value for value in row] for row in values.collect()],
                                key=lambda row: json.dumps(row, separators=(",", ":")))
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        message = str(error)
        if condition is None:
            match = re.search(r"\[([A-Z][A-Z_0-9.]+)\]", message)
            condition = match.group(1) if match else None
        result.update(condition=condition, error_phase=phase,
                      error_class=type(error).__name__, error_message=message)
        assert condition is not None or "ArithmeticException" in type(error).__name__ + message, result
    return result


def main():
    output = Path(sys.argv[1])
    assert not output.exists(), output
    assert hashlib.sha256(ORIGINAL.read_bytes()).hexdigest() == SOURCE_SHA256
    original = [json.loads(line) for line in ORIGINAL.read_text().splitlines()]
    original_cases = [case for case in original[1:] if case["id"] in {
        f"{name}_{operation}" for name in ["year", "year_month", "month"]
        for operation in ["left_multiply", "right_multiply", "divide"]
    } or case["id"] in {f"timestamp_{name}_computed" for name in ["year", "year_month", "month"]}]
    assert len(original_cases) == 24
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-year-month-scaling")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        inputs = []
        def register(name, schema, rows):
            if isinstance(schema[0].dataType, types.YearMonthIntervalType):
                source_schema = types.StructType([
                    types.StructField("m", types.IntegerType(), schema[0].nullable), schema[1]])
                frame = spark.createDataFrame(rows, source_schema).select(
                    functions.make_ym_interval(functions.lit(0), functions.col("m")).alias("m"),
                    functions.col("n"))
                schema = frame.schema
            else:
                frame = spark.createDataFrame(rows, schema)
            frame.createOrReplaceTempView(name)
            inputs.append({"name": name, "schema": schema.jsonValue(), "rows": [
                [encode(value, field.dataType) for value, field in zip(row, schema)] for row in rows]})
        register("inputs", types.StructType([types.StructField("id", types.IntegerType(), False),
                                             types.StructField("v", types.IntegerType(), True)]),
                 [(0, 2), (1, 3), (2, None), (3, 5)])
        for name, numeric_type, nullable, rows in [
            ("scale_integer_required", types.IntegerType(), False, [(27, 2), (-27, 2), (27, -2), (-27, -2)]),
            ("scale_integer_nullable", types.IntegerType(), True, [(27, 2), (-27, 2), (None, 0), (27, None)]),
            ("scale_long", types.LongType(), True, [(27, 2), (-27, -2), (None, 0), (27, None)]),
            ("scale_decimal", types.DecimalType(38, 37), True,
             [(1, decimal.Decimal("2.0000000000000000000000000000000000001")),
              (-3, decimal.Decimal("0.5")), (None, decimal.Decimal("0")), (27, None)]),
            ("scale_float", types.FloatType(), True, [(3, 0.5), (-3, 0.5), (None, 0.0), (27, None)]),
            ("scale_double", types.DoubleType(), True, [(3, 0.5), (-3, 0.5), (None, 0.0), (27, None)]),
            ("scale_overflow", types.LongType(), False, [(2147483647, 2)]),
            ("scale_zero", types.IntegerType(), False, [(27, 0)]),
            ("scale_min_divide", types.IntegerType(), False, [(-2147483648, -1)]),
        ]:
            register(name, types.StructType([types.StructField("m", types.YearMonthIntervalType(), nullable),
                                             types.StructField("n", numeric_type, nullable)]), rows)
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": inputs,
                    "interval_encoding": "cast_string", "timestamp_encoding": "cast_string",
                    "original_sha256": ORIGINAL_SHA256,
                    "original_consumer_source_sha256": SOURCE_SHA256}]
        queries = list(definitions())
        assert len({name for name, _, _ in queries}) == len(queries)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for case in original_cases:
                if case["ansi"] == ansi:
                    records.append({**case, "context": "original_unaliased", "expected": capture(spark, case["sql"])})
            for name, context, sql in queries:
                records.append({"id": name, "context": context, "ansi": ansi,
                                "sql": sql, "expected": capture(spark, sql)})
        with output.open("x") as destination:
            destination.writelines(json.dumps(record, separators=(",", ":")) + "\n" for record in records)
        print(json.dumps({"output": str(output), "observations": len(records) - 1,
                          "errors": sum("error_phase" in case["expected"] for case in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
