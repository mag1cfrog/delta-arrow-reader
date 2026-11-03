"""Capture STRUCT resolution boundaries with the pinned Spark 4.2.0 build."""

import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode


def queries(table):
    for name, expression in [
        ("valid_direct", "s.item"),
        ("valid_deep", "s.inner.leaf"),
        ("valid_case", "S.ITEM"),
        ("valid_quoted", "s.`a.b`"),
        ("valid_bracket", "s['item']"),
        ("valid_computed", "(NAMED_STRUCT('item',1)).item"),
        ("valid_array_field", "items.item"),
        ("valid_array_element", "items[0].item"),
        ("valid_scalar_index", "numbers[0]"),
        ("missing_direct", "s.missing"),
        ("missing_deep", "s.inner.missing"),
        ("missing_identifier", "IDENTIFIER('s.missing')"),
        ("missing_bracket", "s['missing']"),
        ("missing_computed", "(NAMED_STRUCT('item',1)).missing"),
        ("missing_qualified", "q.s.missing"),
        ("duplicate_direct", "duplicate.item"),
        ("duplicate_case", "case_duplicate.item"),
        ("duplicate_bracket", "duplicate['item']"),
        ("duplicate_computed", "(NAMED_STRUCT('item',1,'item',2)).item"),
        ("scalar_tail", "k.missing"),
        ("missing_array_field", "items.missing"),
        ("duplicate_array_field", "duplicate_items.item"),
        ("scalar_array_field", "numbers.missing"),
        ("scalar_array_bracket", "numbers['missing']"),
        ("scalar_index_bounds", "numbers[9]"),
    ]:
        yield name, f"SELECT {expression} AS result FROM {table} q"
    yield "missing_multipart", (
        f"SELECT global_temp.{table}.s.missing AS result FROM global_temp.{table}"
    )
    yield "missing_before_lower_priority", (
        "SELECT p.x.id AS result FROM "
        f"(SELECT NAMED_STRUCT('other',k) AS x,"
        f"NAMED_STRUCT('x',NAMED_STRUCT('id',99)) AS p FROM {table}) p"
    )
    for name, local in [
        ("outer_absent_local_base", "1 AS other"),
        ("outer_shadow_missing", "NAMED_STRUCT('other',1) AS p"),
        ("outer_shadow_valid", "NAMED_STRUCT('id',99) AS p"),
        ("outer_shadow_duplicate", "NAMED_STRUCT('id',1,'id',2) AS p"),
    ]:
        yield name, (
            f"SELECT id,(SELECT p.id FROM (SELECT {local}) q) AS result FROM {table} p"
        )


def capture(spark, sql):
    try:
        frame = spark.sql(sql)
        public_schema = frame.schema.jsonValue()
    except Exception:
        result = capture_query(spark, sql)
        assert "condition" in result, (sql, result)
        return {"expected": result, "phase": "planning"}
    result = capture_query(spark, sql)
    if "condition" in result:
        return {"expected": result, "phase": "execution"}
    execution = frame._jdf.queryExecution()
    stages = {
        "analyzed": json.loads(execution.analyzed().schema().json()),
        "optimized": json.loads(execution.optimizedPlan().schema().json()),
        "executed": json.loads(execution.executedPlan().schema().json()),
    }
    assert result["schema"] == public_schema == stages["analyzed"], sql
    return {"expected": result, "phase": "success", "schema_stages": stages}


def main():
    output = Path(sys.argv[1])
    assert not output.exists(), output
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=32f7299601108917fb01920a54e084595b7b3bf8" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-struct-field-resolution")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "false").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        item = types.StructField("item", types.IntegerType(), True, {"source": "item"})
        record = types.StructType([
            item,
            types.StructField("inner", types.StructType([
                types.StructField("leaf", types.IntegerType(), True, {"source": "leaf"})]), True),
            types.StructField("a.b", types.IntegerType(), True, {"source": "quoted"}),
        ])
        duplicate = types.StructType([item, item])
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False),
            types.StructField("k", types.IntegerType(), True, {"source": "scalar"}),
            types.StructField("s", record, True),
            types.StructField("items", types.ArrayType(types.StructType([item]), True), True),
            types.StructField("numbers", types.ArrayType(types.IntegerType(), True), True),
            types.StructField("duplicate", duplicate, True),
            types.StructField("case_duplicate", types.StructType([
                item, types.StructField("ITEM", types.IntegerType(), True)]), True),
            types.StructField("duplicate_items", types.ArrayType(duplicate, True), True),
        ])
        rows = [(0, 3, (4, (5,), 6), [(7,), None], [8, None], (9, 10), (11, 12), [(13, 14)]),
                (1, None, (None, None, None), [(None,)], [None], (None, None), (None, None), [(None, None)]),
                (2, None, None, None, None, None, None, None)]
        records = [{"spark_version": spark.version, "spark_build": build,
                    "timezone": "UTC", "case_sensitive": False, "inputs": []}]
        tables = [("field_mixed", rows), ("field_nulls", [rows[-1]]), ("field_empty", [])]
        for table, values in tables:
            frame = spark.createDataFrame(values, schema)
            frame.createOrReplaceTempView(table)
            frame.createOrReplaceGlobalTempView(table)
            for name in [table, f"global_temp.{table}"]:
                records[0]["inputs"].append({"name": name, "schema": schema.jsonValue(),
                                             "rows": [encode(row, schema, input_value=True)
                                                      for row in values]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for table, _ in tables:
                for name, sql in queries(table):
                    records.append({"id": name, "input": table, "ansi": ansi,
                                    "sql": sql, **capture(spark, sql)})
        count = len(list(queries(tables[0][0]))) * len(tables) * 2
        assert len(records) - 1 == count
        assert len({(case["id"], case["input"], case["ansi"])
                    for case in records[1:]}) == count
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                      for record in records))
        print(json.dumps({"observations": count, "output": str(output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
