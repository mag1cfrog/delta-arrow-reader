"""Capture qualified-column precedence with independent Spark 4.2.0 queries."""
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

from capture_floating_group_keys_spark import capture_query, encode


def queries(table):
    source = f"(SELECT id,named_struct('id',99) AS p,g,v FROM {table}) p"
    for name, expression in [
        ("collision", "p.id AS selected_id"),
        ("case", "P.ID AS selected_id"),
        ("quoted", "`p`.`id` AS selected_id"),
        ("identifier", "IDENTIFIER('p.id') AS selected_id"),
        ("qualified_struct", "p.p.id AS selected_id"),
        ("qualified_struct_case", "P.P.ID AS selected_id"),
        ("qualified_struct_identifier", "IDENTIFIER('p.p.id') AS selected_id"),
        ("plain_name", "p.id"),
        ("nested_name", "p.p.id"),
        ("aggregate_argument", "SUM(p.id) AS total"),
        ("window_argument", "p.id AS selected_id,SUM(p.id) OVER (ORDER BY p.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total"),
        ("window_partition", "p.id AS selected_id,COUNT(*) OVER (PARTITION BY p.id) AS n"),
        ("window_order", "p.id AS selected_id,ROW_NUMBER() OVER (ORDER BY p.id) AS position"),
        ("missing", "p.missing AS selected_id"),
        ("missing_qualifier", "missing.id AS selected_id"),
        ("qualified_star", "p.*"),
    ]:
        yield name, f"SELECT {expression} FROM {source}"
    yield "filter", f"SELECT p.id AS selected_id FROM {source} WHERE p.id<3"
    yield "group", f"SELECT p.id AS selected_id,COUNT(*) AS n FROM {source} GROUP BY p.id"
    yield "order", f"SELECT p.id AS selected_id FROM {source} ORDER BY p.id DESC LIMIT 3"
    yield "quoted_alias", f"SELECT `p.q`.id AS selected_id FROM (SELECT id,named_struct('id',99) AS `p.q` FROM {table}) `p.q`"
    yield "struct_fallback", f"SELECT p.id AS selected_id FROM (SELECT named_struct('id',id) AS p FROM {table}) q"
    yield "struct_star", f"SELECT p.* FROM (SELECT named_struct('id',id) AS p FROM {table}) q"
    yield "nested_collision", f"SELECT p.p.id AS selected_id FROM (SELECT named_struct('id',id,'p',named_struct('id',99)) AS p FROM {table}) p"
    yield "qualified_duplicate", f"SELECT p.id AS selected_id FROM (SELECT id,id+1 AS id,named_struct('id',99) AS p FROM {table}) p"
    yield "qualified_struct_duplicate", f"SELECT p.p.id AS selected_id FROM (SELECT named_struct('id',id) AS p,named_struct('id',99) AS p FROM {table}) p"
    yield "struct_duplicate", f"SELECT p.id AS selected_id FROM (SELECT named_struct('id',id) AS p,named_struct('id',99) AS p FROM {table}) q"
    yield "invalid_qualified_nested", f"SELECT p.id.item AS selected_id FROM (SELECT id,named_struct('id',named_struct('item',99)) AS p FROM {table}) p"
    yield "missing_qualified_nested", f"SELECT p.x.id AS selected_id FROM (SELECT named_struct('other',id) AS x,named_struct('x',named_struct('id',99)) AS p FROM {table}) p"
    right = f"(SELECT id,v AS right_v FROM {table}) q"
    yield "using_hidden", f"SELECT p.id AS left_id,q.id AS right_id FROM {source} LEFT JOIN {right} USING(id)"
    yield "using_stars", f"SELECT p.*,q.* FROM {source} LEFT JOIN {right} USING(id)"
    yield "using_unqualified_star", f"SELECT * FROM {source} LEFT JOIN {right} USING(id)"


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-qualified-column-precedence")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "false").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([
            types.StructField(name, types.IntegerType(), True) for name in ["id", "g", "k", "v"]])
        rows = [(0, 0, None, None), (1, 0, 1, 10), (2, 0, 1, 20),
                (3, 0, 3, None), (4, 0, 5, 10), (5, 1, None, None),
                (6, 1, 2, None), (7, None, 1, -4), (8, None, 1, 6),
                (9, None, 4, 6), (10, 0, 1, 20)]
        records = [{"spark_version": spark.version, "timezone": "UTC",
                    "case_sensitive": False, "inputs": []}]
        for table, values in [("inputs", rows), ("empty_inputs", []),
                              ("null_inputs", [(0, None, None, None), (1, None, None, None)])]:
            spark.createDataFrame(values, schema).createOrReplaceTempView(table)
            records[0]["inputs"].append({"name": table, "schema": schema.jsonValue(),
                                         "rows": [encode(row, schema, input_value=True)
                                                  for row in values]})
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for table in ["inputs", "empty_inputs", "null_inputs"]:
                for name, sql in queries(table):
                    records.append({"id": name, "input": table, "ansi": ansi, "sql": sql,
                                    "expected": capture_query(spark, sql)})
        assert len(records) == 187
        assert len({(record["id"], record["input"], record["ansi"])
                    for record in records[1:]}) == 186
        Path(__file__).with_name("qualified-column-precedence-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1,
                          "errors": sum("condition" in record["expected"] for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
