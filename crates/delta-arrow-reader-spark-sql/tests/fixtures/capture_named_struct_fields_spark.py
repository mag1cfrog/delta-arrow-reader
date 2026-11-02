"""Capture Spark 4.2.0 named_struct declarations before backend comparison."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode


def queries(table):
    for name, expression in [
        ("literal", "NAMED_STRUCT('id',99)"),
        ("nonnull_child", "NAMED_STRUCT('renamed',id)"),
        ("nullable_child", "NAMED_STRUCT('renamed',v)"),
        ("null_child", "NAMED_STRUCT('untyped',NULL,'typed',CAST(NULL AS INT))"),
        ("mixed_children", "NAMED_STRUCT('required',id,'optional',v,'literal',99)"),
        ("nested_constructor", "NAMED_STRUCT('inner',NAMED_STRUCT('required',id,'optional',v))"),
        ("nested_provider", "NAMED_STRUCT('required',required,'optional',optional)"),
        ("extension_metadata", "NAMED_STRUCT('renamed',payload)"),
        ("lambda_values", "TRANSFORM(ARRAY(id,v), x -> NAMED_STRUCT('value',x))"),
        ("lambda_structs", "TRANSFORM(ARRAY(required,optional), x -> NAMED_STRUCT('record',x))"),
        ("lambda_nested", "TRANSFORM(ARRAY(v), x -> NAMED_STRUCT('inner',NAMED_STRUCT('value',x)))"),
        ("field_names", "NAMED_STRUCT('a.b',id,'sp ace',v,'back`tick',99,'duplicate',id,'duplicate',v)"),
    ]:
        yield name, f"SELECT {expression} AS result FROM {table}"
    yield "extracted_literal", f"SELECT NAMED_STRUCT('id',99).id AS result FROM {table}"
    yield "extracted_nullable", f"SELECT NAMED_STRUCT('value',v).value AS result FROM {table}"
    yield "nested_extraction", f"SELECT NAMED_STRUCT('inner',NAMED_STRUCT('value',v)).inner.value AS result FROM {table}"
    yield "generated_label", f"SELECT ABS(NAMED_STRUCT('value',v).value) FROM {table}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error(f"output already exists: {args.output}")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-named-struct-fields")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "false").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        nested = types.StructType([
            types.StructField("required", types.IntegerType(), False, {"source": "nested_required"}),
            types.StructField("optional", types.IntegerType(), True, {"source": "nested_optional"}),
        ])
        schema = types.StructType([
            types.StructField("id", types.IntegerType(), False, {"source": "id"}),
            types.StructField("v", types.IntegerType(), True, {"source": "nullable"}),
            types.StructField("required", nested, False, {"source": "required_struct"}),
            types.StructField("optional", nested, True, {"source": "optional_struct"}),
            types.StructField("payload", types.StringType(), True, {
                "source": "extension", "ARROW:extension:name": "arrow.json",
                "ARROW:extension:metadata": ""}),
        ])
        inputs = [
            ("inputs", [(0, -2, (3, None), (4, 5), '{"id":0}'),
                        (1, None, (6, 7), None, None),
                        (2, 8, (9, None), (10, None), '{"id":2}')]),
            ("null_inputs", [(0, None, (1, None), None, None),
                             (1, None, (2, None), None, None)]),
            ("empty_inputs", []),
        ]
        records = [{"spark_version": spark.version, "timezone": "UTC", "case_sensitive": False,
                    "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "inputs": []}]
        for table, rows in inputs:
            spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                         "rows": [encode(row, schema, input_value=True) for row in rows]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            definitions = [(name, table, sql) for table, _ in inputs for name, sql in queries(table)]
            definitions.extend([
                ("literal_no_input", "none", "SELECT NAMED_STRUCT('id',99) AS result"),
                ("empty_constructor", "none", "SELECT NAMED_STRUCT() AS result"),
                ("empty_name", "none", "SELECT NAMED_STRUCT('',1) AS result"),
                ("odd_arity", "none", "SELECT NAMED_STRUCT('value') AS result"),
                ("numeric_name", "none", "SELECT NAMED_STRUCT(1,2) AS result"),
                ("null_name", "none", "SELECT NAMED_STRUCT(NULL,2) AS result"),
                ("column_name", "inputs", "SELECT NAMED_STRUCT(payload,id) AS result FROM inputs"),
            ])
            for name, table, sql in definitions:
                expected = capture_query(spark, sql)
                record = {"id": name, "input": table, "ansi": ansi, "sql": sql,
                          "expected": expected}
                if "schema" in expected:
                    execution = spark.sql(sql)._jdf.queryExecution()
                    record["schema_stages"] = {
                        stage: json.loads(plan.schema().json()) for stage, plan in [
                            ("analyzed", execution.analyzed()),
                            ("optimized", execution.optimizedPlan()),
                            ("executed", execution.executedPlan()),
                        ]}
                    assert expected["schema"] == record["schema_stages"]["analyzed"]
                records.append(record)
        assert len(records) == 111
        assert len({(record["id"], record["input"], record["ansi"])
                    for record in records[1:]}) == 110
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1,
                          "errors": sum("condition" in record["expected"] for record in records[1:]),
                          "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
