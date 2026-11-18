"""Capture ELEMENT_AT values and executed types with Spark 4.2.0 in both ANSI modes."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


def encode(value):
    if value is None:
        return None
    if isinstance(value, (tuple, list)):
        return [encode(item) for item in value]
    return str(value)


def cases():
    literals = [
        ("first", "array(7, 9)", "1"),
        ("last", "array(7, 9)", "-1"),
        ("positive_oob", "array(7, 9)", "3"),
        ("negative_oob", "array(7, 9)", "-3"),
        ("int_max", "array(7, 9)", "2147483647"),
        ("empty_positive", "CAST(array() AS ARRAY<INT>)", "1"),
        ("empty_negative", "CAST(array() AS ARRAY<INT>)", "-1"),
        ("null_array", "CAST(NULL AS ARRAY<INT>)", "3"),
        ("null_index", "array(7, 9)", "CAST(NULL AS INT)"),
        ("null_element", "array(7, CAST(NULL AS INT))", "-1"),
        ("nested_positive", "array(array(1, 2), array(3))", "3"),
        ("nested_negative", "array(array(1, 2), array(3))", "-3"),
        ("nested_hit", "array(array(1, 2), array(3))", "-1"),
        ("struct_positive", "array(named_struct('v', 1, 'label', 'a'))", "2"),
        ("struct_negative", "array(named_struct('v', 1, 'label', 'a'))", "-2"),
        ("struct_hit", "array(named_struct('v', 1, 'label', 'a'))", "1"),
        ("bigint_literal_cast", "array(7, 9)", "CAST(3L AS INT)"),
        ("negative_bigint_literal_cast", "array(7, 9)", "CAST(-3L AS INT)"),
    ]
    for name, array, index in literals:
        yield name, "element_at", f"SELECT element_at({array}, {index}) AS r"
    columns = [
        ("column_index", "element_at(a, idx)", ""),
        ("column_positive", "element_at(a, 3)", ""),
        ("column_negative", "element_at(a, -3)", ""),
        ("column_null_index", "element_at(a, idx)", " WHERE idx IS NULL"),
        ("column_null_array", "element_at(a, idx)", " WHERE a IS NULL"),
        ("column_nested", "element_at(nested, idx)", ""),
        ("column_struct", "element_at(records, idx)", ""),
        ("bigint_column_cast", "element_at(a, CAST(big_idx AS INT))", ""),
        ("case", "CASE WHEN idx < 0 THEN element_at(a, -3) ELSE element_at(a, 3) END", ""),
        ("coalesce", "coalesce(element_at(a, 3), -1)", ""),
        ("nested_coalesce", "coalesce(element_at(nested, 3), array(-1))", ""),
        ("struct_coalesce", "coalesce(element_at(records, 3), named_struct('v', -1, 'label', 'none'))", ""),
        ("lambda", "transform(array(1, 3, -3), i -> element_at(a, i))", ""),
        ("lambda_aggregate", "aggregate(array(1, 3, -3), 0, (acc, i) -> acc + coalesce(element_at(a, i), 0))", ""),
    ]
    for name, expression, where in columns:
        yield name, "element_at", f"SELECT id, {expression} AS r FROM element_inputs{where} ORDER BY id"
    yield "aggregate", "element_at", "SELECT sum(coalesce(element_at(a, 3), 0)) AS total, count(element_at(a, 3)) AS present FROM element_inputs"
    yield "closed_lambda", "element_at", "SELECT transform(array(1, 3, -3), i -> element_at(array(7, 9), i)) AS r"
    yield "case_unused_oob", "element_at", "SELECT CASE WHEN true THEN element_at(array(7, 9), 1) ELSE element_at(array(7, 9), 3) END AS r"
    controls = [
        ("zero", "SELECT element_at(array(7, 9), 0) AS r"),
        ("column_zero", "SELECT element_at(a, id - id) AS r FROM element_inputs WHERE id <= 2"),
        ("try_zero", "SELECT try_element_at(array(7, 9), 0) AS r"),
        ("try_positive", "SELECT try_element_at(array(7, 9), 3) AS r"),
        ("try_negative", "SELECT try_element_at(array(7, 9), -3) AS r"),
        ("try_column", "SELECT id, try_element_at(a, idx) AS r FROM element_inputs ORDER BY id"),
        ("try_nested", "SELECT id, try_element_at(nested, 3) AS r FROM element_inputs ORDER BY id"),
        ("try_struct", "SELECT id, try_element_at(records, -3) AS r FROM element_inputs ORDER BY id"),
        ("map_known", "SELECT element_at(map('known', 7), 'known') AS r"),
        ("map_missing", "SELECT element_at(map('known', 7), 'missing') AS r"),
        ("map_null", "SELECT element_at(map('known', CAST(NULL AS INT)), 'known') AS r"),
        ("map_column_key", "SELECT id, element_at(map('known', 7), CASE WHEN id % 2 = 0 THEN 'missing' ELSE 'known' END) AS r FROM element_inputs ORDER BY id"),
        ("subscript", "SELECT id, a[0] AS r FROM element_inputs ORDER BY id"),
        ("subscript_in_range", "SELECT id, a[0] AS r FROM element_inputs WHERE size(a) > 0 ORDER BY id"),
        ("subscript_negative", "SELECT array(7, 9)[-1] AS r"),
    ]
    for name, sql in controls:
        yield name, "control", sql
    for name, index in [("positive", "3L"), ("negative", "-3L"), ("null", "CAST(NULL AS BIGINT)")]:
        yield f"raw_bigint_{name}", "spark_rejected", f"SELECT element_at(array(7, 9), {index}) AS r"
    yield "raw_bigint_column", "spark_rejected", "SELECT id, element_at(a, big_idx) AS r FROM element_inputs ORDER BY id"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    import pyspark
    from pyspark.sql import SparkSession, types

    spark = (SparkSession.builder.master("local[1]").appName("dar-element-at-ansi")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0"
        spark.sparkContext.setLogLevel("ERROR")
        integer = types.IntegerType()
        array = types.ArrayType(integer)
        struct = types.StructType([types.StructField("v", integer), types.StructField("label", types.StringType())])
        schema = types.StructType([
            types.StructField("id", integer, False), types.StructField("a", array),
            types.StructField("idx", integer), types.StructField("big_idx", types.LongType()),
            types.StructField("nested", types.ArrayType(array)),
            types.StructField("records", types.ArrayType(struct)),
        ])
        rows = [
            (1, [7, 9], 1, 1, [[1, 2], [3]], [(1, "a"), (2, None)]),
            (2, [7, None], -1, -1, [[4], None], [(3, "b"), None]),
            (3, [7], 3, 3, [[5]], [(4, "c")]),
            (4, [], -3, -3, [], []),
            (5, None, 2, 2, None, None),
            (6, [None], None, None, [[]], [(None, "d")]),
        ]
        frame = spark.createDataFrame(rows, schema).cache()
        frame.createOrReplaceTempView("element_inputs")
        inputs = [{"name": "element_inputs", "schema": schema.jsonValue(),
                   "rows": [encode(row) for row in frame.orderBy("id").collect()]}]
        definitions = [{"id": name, "scope": scope, "sql": sql} for name, scope, sql in cases()]
        assert len({case["id"] for case in definitions}) == len(definitions)
        jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
        with ZipFile(jar) as archive:
            build = archive.read("spark-version-info.properties").decode()
        records = [{"spark_version": spark.version, "spark_build": build, "inputs": inputs,
                    "cases": definitions, "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}]
        for ansi in [False, True]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for definition in definitions:
                record = {**definition, "ansi": ansi, "ordered": True}
                try:
                    result = spark.sql(record["sql"])
                    record["rows"] = [encode(row) for row in result.collect()]
                    record["schema"] = json.loads(result._jdf.queryExecution().executedPlan().schema().json())
                    record["status"] = "ok"
                except Exception as error:
                    record.update(status="error", condition=getattr(error, "getCondition", lambda: None)(),
                                  message=str(error).splitlines()[0])
                if record["scope"] == "spark_rejected":
                    assert record["status"] == "error" and record["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE", record
                records.append(record)
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"cases": len(definitions), "observations": len(records) - 1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
