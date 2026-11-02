"""Independent Spark 4.2.0 error-reachability references for review #781."""
import hashlib
import json
import os
from pathlib import Path
import re
import sys


def definitions():
    for name, expr in [
        ("divide_zero", "INTERVAL '1' MONTH/0"),
        ("multiply_overflow", "INTERVAL '2147483647' MONTH*2"),
        ("divide_min", "INTERVAL '-2147483648' MONTH/(-1)"),
    ]:
        for context, sql in [
            ("direct", f"SELECT {expr} AS r"),
            ("local_false", f"SELECT {expr} AS r WHERE false"),
            ("table_false", f"SELECT {expr} AS r FROM inputs WHERE false"),
            ("local_unused", f"SELECT 1 AS r FROM (SELECT {expr} AS x) t"),
            ("table_unused", f"SELECT 1 AS r FROM (SELECT {expr} AS x FROM inputs) t"),
            ("local_limit_zero", f"SELECT {expr} AS r LIMIT 0"),
            ("table_limit_zero", f"SELECT {expr} AS r FROM inputs LIMIT 0"),
            ("values", f"SELECT x AS r FROM VALUES ({expr}) AS t(x)"),
            ("values_false", f"SELECT x AS r FROM VALUES ({expr}) AS t(x) WHERE false"),
            ("values_unused", f"SELECT 1 AS r FROM VALUES ({expr}) AS t(x)"),
            ("values_limit_zero", f"SELECT x AS r FROM VALUES ({expr}) AS t(x) LIMIT 0"),
            ("constant_case", f"SELECT CASE WHEN false THEN {expr} ELSE INTERVAL '1' MONTH END AS r"),
            ("column_case", f"SELECT CASE WHEN id<0 THEN {expr} ELSE INTERVAL '1' MONTH END AS r FROM inputs"),
        ]:
            yield name + "_" + context, context, sql
    for context, sql in [
        ("column_live", "SELECT m/n AS r FROM scale_inputs"),
        ("column_false", "SELECT m/n AS r FROM scale_inputs WHERE false"),
        ("column_unused", "SELECT 1 AS r FROM (SELECT m/n AS x FROM scale_inputs) t"),
        ("column_limit_zero", "SELECT m/n AS r FROM scale_inputs LIMIT 0"),
        ("null_interval_divisor_error", "SELECT CAST(NULL AS INTERVAL YEAR TO MONTH)/(1/0) AS r"),
        ("null_interval_number_error", "SELECT CAST(NULL AS INTERVAL YEAR TO MONTH)*(1/0) AS r"),
    ]:
        yield context, "control", sql


def capture(spark, sql):
    from pyspark.sql import functions
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
        values = frame.select(*[functions.col("`" + f.name.replace("`", "``") + "`").cast("string").alias(str(i))
                                for i, f in enumerate(frame.schema)])
        result["rows"] = sorted([list(row) for row in values.collect()], key=json.dumps)
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
    os.environ["PYSPARK_YM_INTERVAL_LEGACY"] = "1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession, functions, types
    spark = (SparkSession.builder.master("local[1]").appName("dar-year-month-connected-review-781")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        input_schema = types.StructType([types.StructField("id", types.IntegerType(), False)])
        spark.createDataFrame([(0,), (1,), (2,), (3,)], input_schema).createOrReplaceTempView("inputs")
        scale_schema = types.StructType([types.StructField("m", types.IntegerType(), False),
                                         types.StructField("n", types.IntegerType(), False)])
        scale = spark.createDataFrame([(1, 0)], scale_schema).select(
            functions.make_ym_interval(functions.lit(0), functions.col("m")).alias("m"), functions.col("n"))
        scale.createOrReplaceTempView("scale_inputs")
        records = [{"spark_version": spark.version, "timezone": "UTC", "interval_encoding": "cast_string",
                    "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "inputs": [{"name": "inputs", "schema": input_schema.jsonValue(),
                                "rows": [[str(i)] for i in range(4)]},
                               {"name": "scale_inputs", "schema": scale.schema.jsonValue(),
                                "rows": [["1", "0"]]}]}]
        cases = list(definitions())
        assert len(cases) == 45 and len({name for name, _, _ in cases}) == 45
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, context, sql in cases:
                records.append({"id": name, "context": context, "ansi": ansi, "sql": sql,
                                "expected": capture(spark, sql)})
        with output.open("x") as destination:
            destination.writelines(json.dumps(record, separators=(",", ":")) + "\n" for record in records)
        print(json.dumps({"output": str(output), "observations": len(records)-1,
                          "errors": sum("error_phase" in case["expected"] for case in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
