"""Capture the bounded #815 NullType extraction contract on pinned Spark."""

import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


CONSTANT_CASES = [
    ("null_local_base", "SELECT s.missing AS result FROM (SELECT NULL AS s) q"),
    ("null_computed_base", "SELECT (NULL).missing AS result"),
    ("null_literal_bracket", "SELECT NULL['missing'] AS result"),
    ("null_literal_deep", "SELECT (NULL).missing.deep AS result"),
    ("null_literal_unaliased", "SELECT (NULL).missing"),
    ("null_local_unaliased", "SELECT s.missing FROM (SELECT NULL AS s) q"),
    ("null_case_constant", "SELECT (CASE WHEN TRUE THEN NULL ELSE NULL END).missing AS result"),
    ("null_raise_constant", "SELECT (RAISE_ERROR('null-base-required')).missing AS result"),
    ("null_raise_dead_constant", "SELECT (CASE WHEN FALSE THEN RAISE_ERROR('null-base-dead') ELSE NULL END).missing AS result"),
    ("null_lambda_base", "SELECT TRANSFORM(ARRAY(NULL),x -> x.missing) AS result"),
    ("null_lambda_bracket", "SELECT TRANSFORM(ARRAY(NULL),x -> x['missing']) AS result"),
    ("null_lambda_parenthesized", "SELECT TRANSFORM(ARRAY(NULL),x -> (x).missing) AS result"),
    ("null_lambda_case", "SELECT TRANSFORM(ARRAY(NULL),x -> (CASE WHEN TRUE THEN x ELSE NULL END).missing) AS result"),
    ("null_lambda_cast", "SELECT TRANSFORM(ARRAY(NULL),x -> (CAST(x AS VOID)).missing) AS result"),
    ("null_literal_in_null_lambda", "SELECT TRANSFORM(ARRAY(NULL),x -> (NULL).missing) AS result"),
    ("null_literal_in_integer_lambda", "SELECT TRANSFORM(ARRAY(1,NULL),x -> (NULL).missing) AS result"),
    ("null_nested_lambda_constant", "SELECT TRANSFORM(ARRAY(NAMED_STRUCT('n',NULL)),x -> x.n.missing) AS result"),
    ("null_raise_in_lambda", "SELECT TRANSFORM(ARRAY(1),x -> (RAISE_ERROR('null-base-required')).missing) AS result"),
    ("null_shadow_outer", "SELECT (SELECT s.missing FROM (SELECT NULL AS s) q) AS result FROM (SELECT NAMED_STRUCT('missing',99) AS s) p"),
    ("scalar_shadow_outer", "SELECT (SELECT s.missing FROM (SELECT 1 AS s) q) AS result FROM (SELECT NULL AS s) p"),
    ("missing_struct", "SELECT (NAMED_STRUCT('other',1)).missing AS result"),
    ("duplicate_struct", "SELECT (NAMED_STRUCT('item',1,'ITEM',2)).item AS result"),
    ("scalar_base", "SELECT (1).missing AS result"),
    ("scalar_array_base", "SELECT ARRAY(1,NULL).missing AS result"),
]

PROVIDER_CASES = [
    ("null_column_dot", "s.missing"),
    ("null_column_qualified", "q.s.missing"),
    ("null_column_deep", "s.missing.deep"),
    ("null_column_bracket", "s['missing']"),
    ("null_column_parenthesized", "(s).missing"),
    ("null_struct_child", "r.n.missing"),
    ("null_case_provider", "(CASE WHEN id=0 THEN s ELSE NULL END).missing"),
    ("null_raise_live_provider", "(CASE WHEN id=0 THEN RAISE_ERROR('null-base-required') ELSE NULL END).missing"),
    ("null_raise_dead_provider", "(CASE WHEN id<0 THEN RAISE_ERROR('null-base-dead') ELSE NULL END).missing"),
    ("null_literal_in_provider_lambda", "TRANSFORM(numbers,x -> (NULL).missing)"),
    ("null_column_in_provider_lambda", "TRANSFORM(numbers,x -> s.missing)"),
    ("null_nested_provider_lambda", "TRANSFORM(records,x -> x.n.missing)"),
    ("null_provider_lambda", "TRANSFORM(nulls,x -> x.missing)"),
]

MIXED_CASES = [
    ("null_column_unaliased", "s.missing"),
    ("null_struct_child_unaliased", "r.n.missing"),
]


def cases():
    for name, sql in CONSTANT_CASES:
        yield {"id": name, "input": "constant", "sql": sql}
    for table in ["null_mixed", "null_nulls", "null_empty"]:
        for name, expression in PROVIDER_CASES:
            yield {"id": name, "input": table,
                   "sql": f"SELECT {expression} AS result FROM {table} q"}
    for name, expression in MIXED_CASES:
        yield {"id": name, "input": "null_mixed",
               "sql": f"SELECT {expression} FROM null_mixed q"}


def error_details(error):
    from py4j.protocol import Py4JError

    condition = getattr(error, "getCondition", lambda: None)()
    details = {"python_type": type(error).__name__, "message": str(error),
               "parameters": getattr(error, "getMessageParameters", lambda: None)(),
               "sqlstate": getattr(error, "getSqlState", lambda: None)(), "causes": []}
    cause = getattr(error, "java_exception", None)
    while cause is not None:
        entry = {"type": cause.getClass().getName(), "message": cause.getMessage()}
        try:
            entry["condition"] = cause.getCondition()
        except Py4JError:
            entry["condition"] = None
        if condition is None:
            condition = entry["condition"]
        details["causes"].append(entry)
        cause = cause.getCause()
    assert condition is not None, details
    return condition, details


def capture(spark, sql):
    from capture_floating_group_keys_spark import encode

    stages = {}
    stage = "analyzed"
    try:
        frame = spark.sql(sql)
        public_schema = frame.schema.jsonValue()
        execution = frame._jdf.queryExecution()
        stages["analyzed"] = json.loads(execution.analyzed().schema().json())
        assert public_schema == stages["analyzed"], sql
        stage = "optimized"
        stages["optimized"] = json.loads(execution.optimizedPlan().schema().json())
        stage = "executed"
        stages["executed"] = json.loads(execution.executedPlan().schema().json())
        stage = "collect"
        rows = [encode(row, frame.schema) for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        return {"expected": {"schema": public_schema, "rows": rows},
                "phase": "success", "schema_stages": stages}
    except Exception as error:
        condition, details = error_details(error)
        return {"expected": {"condition": condition},
                "phase": "execution" if stage == "collect" else "planning",
                "failure_stage": stage, "schema_stages": stages, "error": details}


def main():
    inventory = list(cases())
    assert len(inventory) == 65, len(inventory)
    assert len({(case["id"], case["input"]) for case in inventory}) == len(inventory)
    if sys.argv[1:] == ["--inventory"]:
        print(json.dumps({"queries_per_ansi": len(inventory), "observations": 130,
                          "cases": inventory}, indent=2))
        return

    import pyspark
    from pyspark.sql import SparkSession, types
    from capture_floating_group_keys_spark import encode

    output = Path(sys.argv[1])
    assert not output.exists(), output
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=32f7299601108917fb01920a54e084595b7b3bf8" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-null-base-extraction-815")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "false").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        null_child = types.StructType([
            types.StructField("n", types.NullType(), True, {"source": "null_child"})])
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False),
            types.StructField("s", types.NullType(), True, {"source": "null_column"}),
            types.StructField("r", null_child, True),
            types.StructField("numbers", types.ArrayType(types.IntegerType(), True), True),
            types.StructField("records", types.ArrayType(null_child, True), True),
            types.StructField("nulls", types.ArrayType(types.NullType(), True), True),
        ])
        mixed = [(0, None, (None,), [1, None], [(None,), None], [None, None]),
                 (1, None, None, None, None, None),
                 (2, None, (None,), [], [], [])]
        nulls = [(3, None, None, None, None, None)]
        records = [{"spark_version": spark.version, "spark_build": build,
                    "timezone": "UTC", "case_sensitive": False, "inputs": []}]
        for name, rows in [("null_mixed", mixed), ("null_nulls", nulls), ("null_empty", [])]:
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
            records[0]["inputs"].append({"name": name, "schema": schema.jsonValue(),
                                         "rows": [encode(row, schema, input_value=True) for row in rows]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for case in inventory:
                records.append({**case, "ansi": ansi, **capture(spark, case["sql"])})
        assert len(records) == 131, len(records)
        assert len({(case["id"], case["input"], case["ansi"])
                    for case in records[1:]}) == 130
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                      for record in records))
        print(json.dumps({"observations": 130, "inputs": 3, "output": str(output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
