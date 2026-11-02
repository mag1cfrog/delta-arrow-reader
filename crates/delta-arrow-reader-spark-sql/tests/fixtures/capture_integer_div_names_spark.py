"""Capture integer DIV names and declarations independently with Spark 4.2.0."""

import hashlib
import json
import os
from pathlib import Path
import platform
import sys


def queries():
    for table in ["inputs", "null_inputs", "empty_inputs"]:
        for name, expression in [
            ("column", "a DIV b"),
            ("function", "DIV(a,b)"),
            ("qualified", "q.a DIV q.b"),
            ("nested_left", "(a DIV b) DIV 2"),
            ("nested_right", "a DIV (b DIV 2)"),
            ("addition", "(a DIV b)+1"),
            ("absolute", "ABS(a DIV b)"),
            ("coalesce", "COALESCE(a DIV b,9L)"),
            ("cast", "CAST(a DIV b AS BIGINT)"),
            ("null_predicate", "(a DIV b) IS NULL"),
            ("explicit", "a DIV b AS quotient"),
            ("explicit_nested", "ABS(a DIV b) AS quotient"),
            ("quoted_alias", "a DIV b AS `div(a, b)`"),
            ("zero", "a DIV 0"),
            ("overflow", "a DIV -1"),
            ("aggregate", "SUM(a DIV b)"),
        ]:
            yield name, table, f"SELECT {expression} FROM {table} q"
        yield "outer_generated", table, (
            f"SELECT `(a div b)` FROM (SELECT a DIV b FROM {table}) q")
    for left in ["t", "s", "i", "l"]:
        for right in ["t", "s", "i", "l"]:
            yield f"mixed_{left}_{right}", "inputs", (
                f"SELECT {left} DIV {right} FROM inputs WHERE {right} IS NULL OR {right}<>0")
    for name, sql in [
        ("literal", "SELECT 7 DIV 2"),
        ("literal_function", "SELECT div(7,2)"),
        ("literal_nested", "SELECT (7 DIV 2) DIV (8 DIV 3)"),
        ("literal_explicit", "SELECT 7 DIV 2 AS quotient"),
        ("null_literal", "SELECT NULL DIV 2"),
        ("null_divisor", "SELECT 7 DIV NULL"),
        ("both_null", "SELECT NULL DIV NULL"),
        ("literal_zero", "SELECT 7 DIV 0"),
        ("literal_overflow", "SELECT CAST(-9223372036854775808 AS BIGINT) DIV -1"),
        ("null_cast_div_bad_cast_generated", "SELECT CAST(NULL AS INT) DIV CAST('bad' AS INT)"),
        ("null_cast_div_bad_cast_explicit", "SELECT CAST(NULL AS INT) DIV CAST('bad' AS INT) AS value"),
        ("bad_cast_div_null_cast_generated", "SELECT CAST('bad' AS INT) DIV CAST(NULL AS INT)"),
        ("bad_cast_div_null_cast_explicit", "SELECT CAST('bad' AS INT) DIV CAST(NULL AS INT) AS value"),
        ("range", "SELECT id DIV 2 FROM range(3)"),
        ("range_empty", "SELECT id DIV 2 FROM range(0)"),
        ("range_explicit", "SELECT id DIV 2 AS quotient FROM range(3)"),
    ]:
        yield name, "sql", sql


def capture(spark, sql):
    stages = {}
    try:
        frame = spark.sql(sql)
        stages["public"] = frame.schema.jsonValue()
        execution = frame._jdf.queryExecution()
        stages["analyzed"] = json.loads(execution.analyzed().schema().json())
        stages["optimized"] = json.loads(execution.optimizedPlan().schema().json())
        rows = [[None if value is None else str(value).lower()
                 if isinstance(value, bool) else str(value) for value in row]
                for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        stages["executed"] = json.loads(execution.executedPlan().schema().json())
        expected = {"schema": stages["public"], "rows": rows}
    except Exception as error:
        condition = error.getCondition() if hasattr(error, "getCondition") else None
        assert condition in {"DIVIDE_BY_ZERO", "ARITHMETIC_OVERFLOW"}, str(error)
        expected = {"condition": condition}
    return {"expected": expected, "schema_stages": stages}


def main():
    definitions = list(queries())
    assert len(definitions) == 83, len(definitions)
    assert len({(name, table) for name, table, _ in definitions}) == len(definitions)
    if sys.argv[1:] == ["--check"]:
        print(json.dumps({"queries": len(definitions), "observations": 2 * len(definitions)}))
        return
    destination = (Path(sys.argv[1]) if len(sys.argv) == 2 else
                   Path(__file__).with_name("integer-div-names-spark.jsonl"))
    if destination.exists():
        raise FileExistsError(f"capture destination already exists: {destination}")

    from pyspark.sql import SparkSession, types

    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-integer-div-names")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([
            types.StructField(name, kind, True)
            for name, kind in [("t", types.ByteType()), ("s", types.ShortType()),
                               ("i", types.IntegerType()), ("l", types.LongType()),
                               ("a", types.LongType()), ("b", types.IntegerType())]
        ])
        rows = [(7, 7, 7, 7, 7, 2), (-7, -7, -7, -7, -7, 2),
                (8, 8, 8, 8, 8, -2), (None, None, None, None, None, 2),
                (7, 7, 7, 7, 7, None), (0, 0, 0, 0, 0, 2),
                (-128, -32768, -2147483648, -9223372036854775808, -9223372036854775808, 2)]
        records = [{"spark_version": spark.version, "timezone": "UTC",
                    "python_version": platform.python_version(),
                    "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "settings": {"spark.sql.shuffle.partitions": "2"}, "inputs": []}]
        for table, values in [("inputs", rows), ("null_inputs", [(None,) * 6]),
                              ("empty_inputs", [])]:
            spark.createDataFrame(values, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({
                "name": table, "schema": schema.jsonValue(),
                "rows": [[None if value is None else str(value) for value in row]
                         for row in values]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, table, sql in definitions:
                records.append({"id": name, "input": table, "ansi": ansi, "sql": sql,
                                **capture(spark, sql)})
        assert len(records) == 167, len(records)
        with destination.open("x") as output:
            output.writelines(json.dumps(record, separators=(",", ":")) + "\n"
                              for record in records)
        print(json.dumps({"observations": len(records) - 1,
                          "errors": sum("condition" in record["expected"]
                                        for record in records[1:]),
                          "output": str(destination)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
