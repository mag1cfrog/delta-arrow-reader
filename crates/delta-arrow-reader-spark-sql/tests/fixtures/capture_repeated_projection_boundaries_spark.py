"""Capture independent Spark evidence for the ordinary projection identity boundaries."""

import argparse
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types


def queries():
    repeated = "(SELECT id,id,k,lv FROM left_mixed) l"
    yield "projection_repeated_unqualified", f"SELECT id FROM {repeated}"
    yield "projection_repeated_wildcards", f"SELECT l.*,l.* FROM {repeated}"
    yield "projection_repeated_using_wildcards", (
        f"SELECT *,l.*,r.* FROM {repeated} LEFT JOIN right_mixed r USING(id)"
    )
    yield "projection_repeated_on_wildcards", (
        f"SELECT l.*,r.* FROM {repeated} LEFT JOIN right_mixed r ON l.id=r.id"
    )
    yield "projection_explicit_alias_collision", (
        "SELECT q.id FROM (SELECT id,id AS id FROM left_mixed) q"
    )
    yield "projection_two_explicit_aliases", (
        "SELECT q.id FROM (SELECT id AS id,id AS id FROM left_mixed) q"
    )
    yield "projection_repeat_alias_value", (
        "WITH shared AS (SELECT id AS renamed FROM left_mixed) "
        "SELECT q.renamed FROM (SELECT renamed,renamed FROM shared) q"
    )
    shared = "WITH shared AS (SELECT id,lv FROM left_mixed) "
    yield "projection_shared_cte_aliases", (
        shared + "SELECT q.id FROM "
        "(SELECT a.id,b.id FROM shared a JOIN shared b ON a.lv=b.lv) q"
    )
    yield "projection_shared_cte_aliases_wildcards", (
        shared + "SELECT q.* FROM "
        "(SELECT a.id,b.id FROM shared a JOIN shared b ON a.lv=b.lv) q"
    )
    yield "projection_same_cte_alias_repeated", (
        shared + "SELECT q.id FROM "
        "(SELECT a.id,a.id FROM shared a JOIN shared b ON a.lv=b.lv) q"
    )
    yield "projection_independent_ctes", (
        "WITH one AS (SELECT id,lv FROM left_mixed), "
        "two AS (SELECT id,lv FROM left_mixed) "
        "SELECT q.id FROM (SELECT a.id,b.id FROM one a JOIN two b ON a.lv=b.lv) q"
    )
    yield "projection_repeated_outer_scalar", (
        f"SELECT (SELECT l.id) AS outer_id FROM {repeated}"
    )
    yield "projection_repeated_outer_exists", (
        f"SELECT l.id FROM {repeated} WHERE EXISTS (SELECT 1 WHERE l.id=1)"
    )
    yield "projection_repeated_nested_paths", (
        "WITH shared AS (SELECT named_struct('a',id,'b',k) AS s FROM left_mixed) "
        "SELECT q.s.a,q.s.b FROM (SELECT s,s FROM shared) q"
    )
    yield "projection_distinct_nested_paths_alias", (
        "WITH shared AS (SELECT named_struct('a',id,'b',k) AS s FROM left_mixed) "
        "SELECT q.value FROM (SELECT s.a AS value,s.b AS value FROM shared) q"
    )
    yield "projection_repeated_chain", (
        f"SELECT l.id,r.id,t.id FROM {repeated} LEFT JOIN right_mixed r USING(id) "
        "LEFT JOIN third_mixed t USING(id)"
    )
    yield "projection_table_column_aliases", (
        "SELECT q.id FROM (SELECT id,id FROM left_mixed) q(id,id)"
    )
    yield "projection_cte_column_aliases", (
        "WITH shared(id,id) AS (SELECT id,id FROM left_mixed) SELECT id FROM shared"
    )


def error_details(error):
    condition = getattr(error, "getCondition", lambda: None)()
    cause = getattr(error, "java_exception", None)
    while condition is None and cause is not None:
        try:
            condition = cause.getCondition()
        except Exception:
            pass
        cause = cause.getCause()
    assert condition is not None, str(error)
    return {"condition": condition, "error_type": type(error).__name__, "message": str(error)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path, help="existing provider/header JSONL")
    parser.add_argument("output", type=Path, help="fresh output file; overwrite is rejected")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    metadata = json.loads(args.reference.read_text().splitlines()[0])
    assert metadata["spark_version"] == "4.2.0"
    assert metadata["timezone"] == "UTC"
    definitions = list(queries())
    assert len(definitions) == 18, len(definitions)
    assert len({name for name, _ in definitions}) == len(definitions)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-repeated-projection-boundaries")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "false").getOrCreate())
    try:
        assert spark.version == metadata["spark_version"], spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for provider in metadata["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            assert all(isinstance(field.dataType, types.IntegerType) for field in schema)
            rows = [[None if value is None else int(value) for value in row]
                    for row in provider["rows"]]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(provider["name"])
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                record = {"id": name, "ansi": ansi, "sql": sql}
                phase = "planning"
                try:
                    frame = spark.sql(sql)
                    execution = frame._jdf.queryExecution()
                    record["public_schema"] = frame.schema.jsonValue()
                    record["schema_stages"] = {
                        stage: json.loads(plan.schema().json())
                        for stage, plan in [("analyzed", execution.analyzed()),
                                            ("optimized", execution.optimizedPlan()),
                                            ("executed", execution.executedPlan())]
                    }
                    phase = "execution"
                    rows = [[None if value is None else str(value) for value in row]
                            for row in frame.collect()]
                    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                    record["expected"] = {"schema": record["public_schema"], "rows": rows}
                    assert record["expected"]["schema"] == record["schema_stages"]["analyzed"]
                except Exception as error:
                    record["expected"] = error_details(error)
                    record["error_phase"] = phase
                records.append(record)
        assert len(records) == 37, len(records)
        with args.output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                      for record in records))
        print(json.dumps({"observations": len(records) - 1,
                          "errors": sum("condition" in record["expected"] for record in records[1:]),
                          "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
