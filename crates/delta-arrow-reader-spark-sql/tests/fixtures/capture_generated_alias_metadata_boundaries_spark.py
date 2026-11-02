"""Record Spark alias-classification and computed grouping-key boundaries."""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(data_type, types.StructType):
        return [encode(item, field.dataType) for item, field in zip(value, data_type)]
    return str(value)


def queries(table):
    for label, expression in [
        ("column_control", "k"),
        ("direct_cast_control", "CAST(k AS BIGINT)"),
        ("unary_plus", "+k"),
        ("positive", "POSITIVE(k)"),
        ("identifier", "IDENTIFIER('k')"),
        ("identifier_cast", "CAST(IDENTIFIER('k') AS BIGINT)"),
    ]:
        yield label, f"SELECT {expression} FROM {table}"
    for label, expression in [("add", "k+1"), ("abs", "ABS(k)")]:
        for function in ["GROUPING", "GROUPING_ID"]:
            for grouping_label, grouping in [("alias", "computed_key"),
                                              ("direct", expression)]:
                name = f"{label}_{function.lower()}_{grouping_label}"
                yield name, (
                    f"SELECT {expression} AS computed_key,"
                    f"{function}({expression}) AS grouping_value,COUNT(*) AS n "
                    f"FROM {table} GROUP BY ROLLUP({grouping})"
                )


def capture(spark, sql):
    result = {}
    stage = "analysis"
    try:
        frame = spark.sql(sql)
        result["public_schema"] = frame.schema.jsonValue()
        execution = frame._jdf.queryExecution()
        result["analyzed_schema"] = json.loads(execution.analyzed().schema().json())
        stage = "optimization"
        result["optimized_schema"] = json.loads(execution.optimizedPlan().schema().json())
        stage = "execution"
        rows = [encode(row, frame.schema) for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        result["expected"] = {"schema": frame.schema.jsonValue(), "rows": rows}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        cause = getattr(error, "java_exception", None)
        while condition is None and cause is not None:
            condition = cause.getCondition()
            cause = cause.getCause()
        assert condition is not None, (sql, stage, str(error))
        result["expected"] = {"condition": condition}
        result["error"] = {"condition": condition, "stage": stage, "message": str(error)}
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), f"Refusing to overwrite {args.output}"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-generated-alias-metadata-boundaries")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        table = "alias_boundary_inputs"
        schema = types.StructType([
            types.StructField("k", types.IntegerType(), True, {"source": "plain"}),
        ])
        inputs = [(2,), (None,), (-3,), (4,)]
        spark.createDataFrame(inputs, schema).createOrReplaceTempView(table)
        records = [{
            "spark_version": spark.version,
            "timezone": "UTC",
            "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "inputs": [{"name": table, "schema": schema.jsonValue(),
                        "rows": [encode(row, schema) for row in inputs]}],
        }]
        definitions = list(queries(table))
        assert len(definitions) == 14
        assert len({name for name, _ in definitions}) == len(definitions)
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                records.append({"id": name, "ansi": ansi, "sql": sql,
                                **capture(spark, sql)})
        assert len({(record["id"], record["ansi"]) for record in records[1:]}) == 28
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1, "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
