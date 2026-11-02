"""Capture Spark 4.2.0 USING key metadata without rewriting prior evidence."""
import argparse
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

from capture_window_admission_spark import capture_query


JOINS = [("inner", "INNER JOIN"), ("left", "LEFT JOIN"), ("right", "RIGHT JOIN"),
         ("full", "FULL JOIN"), ("semi", "LEFT SEMI JOIN"), ("anti", "LEFT ANTI JOIN")]


def queries():
    for shape in ["mixed", "nulls", "empty"]:
        for label, join in JOINS:
            projections = [("aliases", "l.id AS left_id,r.id AS right_id"),
                           ("plain_star", "*")]
            if shape == "mixed":
                projections += [("qualified", "l.id,r.id"), ("left_star", "l.*"),
                                ("right_star", "r.*"), ("both_stars", "l.*,r.*"),
                                ("merged", "id AS merged_id")]
            for projection, columns in projections:
                yield f"{shape}_{label}_{projection}", (
                    f"SELECT {columns} FROM left_{shape} l {join} right_{shape} r USING(id)"
                )
    source = "left_mixed l LEFT JOIN right_mixed r USING(id)"
    for label, columns in [("keys", "id AS merged_id,l.id AS left_id,r.id AS right_id,t.id AS third_id"),
                           ("stars", "l.*,r.*,t.*"), ("plain_star", "*")]:
        yield f"chained_{label}", f"SELECT {columns} FROM {source} FULL JOIN third_mixed t USING(id)"
    yield "subquery_alias", (
        f"SELECT q.left_id,q.right_id FROM (SELECT l.id AS left_id,r.id AS right_id FROM {source}) q"
    )
    yield "renamed_key", (
        "SELECT l.key_id AS left_id,r.key_id AS right_id FROM "
        "(SELECT id AS key_id,lv FROM left_mixed) l LEFT JOIN "
        "(SELECT id AS key_id,rv FROM right_mixed) r USING(key_id)"
    )
    yield "quoted_key", (
        "SELECT l.`ID Key` AS left_id,r.`ID Key` AS right_id FROM "
        "(SELECT id AS `ID Key`,lv FROM left_mixed) l RIGHT JOIN "
        "(SELECT id AS `ID Key`,rv FROM right_mixed) r USING(`ID Key`)"
    )
    yield "composite_keys", (
        "SELECT l.*,r.* FROM left_mixed l LEFT JOIN right_mixed r USING(k,id)"
    )
    yield "coerced_keys", (
        "SELECT id AS merged_id,l.id AS left_id,r.id AS right_id FROM left_mixed l LEFT JOIN "
        "(SELECT CAST(id AS BIGINT) AS id,rv FROM right_mixed) r USING(id)"
    )
    yield "coerced_full_keys", (
        "SELECT id AS merged_id,l.id AS left_id,r.id AS right_id FROM left_mixed l FULL JOIN "
        "(SELECT CAST(id AS BIGINT) AS id,rv FROM right_mixed) r USING(id)"
    )
    yield "on_control", (
        "SELECT l.id AS left_id,r.id AS right_id FROM left_mixed l LEFT JOIN right_mixed r ON l.id=r.id"
    )
    yield "natural_control", "SELECT l.id AS left_id,r.id AS right_id FROM left_mixed l NATURAL LEFT JOIN right_mixed r"
    yield "missing_key", "SELECT * FROM left_mixed l LEFT JOIN right_mixed r USING(missing)"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new JSONL file; existing files are rejected")
    output = parser.parse_args().output
    assert not output.exists(), f"refusing to replace {output}"
    definitions = list(queries())
    assert len(definitions) == 78, len(definitions)
    assert len({name for name, _ in definitions}) == len(definitions)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-using-key-metadata")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "inputs": []}
        for side, payload, values in [
            ("left", "lv", [(1, 7, 10), (1, 7, 11), (2, 8, 20), (None, None, 30)]),
            ("right", "rv", [(1, 7, 40), (1, 7, 40), (3, 9, 50), (None, None, 60)]),
            ("third", "tv", [(1, 7, 70), (4, 10, 80), (None, None, 90)]),
        ]:
            schema = types.StructType([
                types.StructField("id", types.IntegerType(), True, {"source": side}),
                types.StructField("k", types.IntegerType(), True, {"unit": "key"}),
                types.StructField(payload, types.IntegerType(), True, {"unit": "value"}),
            ])
            for shape, rows in [("mixed", values), ("nulls", [(None, None, None)]), ("empty", [])]:
                name = f"{side}_{shape}"
                spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
                metadata["inputs"].append({"name": name, "schema": schema.jsonValue(),
                                          "rows": [[None if value is None else str(value) for value in row]
                                                   for row in rows]})
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                expected = capture_query(spark, sql)
                record = {"id": name, "ansi": ansi, "sql": sql, "expected": expected}
                if "schema" in expected:
                    query = spark.sql(sql)._jdf.queryExecution()
                    record["schema_stages"] = {
                        stage: json.loads(plan.schema().json())
                        for stage, plan in [("analyzed", query.analyzed()),
                                            ("optimized", query.optimizedPlan()),
                                            ("executed", query.executedPlan())]
                    }
                    assert record["schema_stages"]["analyzed"] == expected["schema"], name
                records.append(record)
        assert len(records) == 157, len(records)
        with output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                      for record in records))
        print(json.dumps({"output": str(output), "observations": len(records) - 1,
                          "errors": sum("condition" in record["expected"] for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
