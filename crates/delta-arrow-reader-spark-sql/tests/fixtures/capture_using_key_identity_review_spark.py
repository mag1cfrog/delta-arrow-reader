"""Capture independent Spark evidence for the repaired USING identity boundary."""

import argparse
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types


def queries():
    repeated = "left_mixed l LEFT JOIN right_mixed r USING(id,id)"
    chain = ("left_mixed l LEFT JOIN (SELECT id,rv FROM right_mixed) r USING(id,id) "
             "LEFT JOIN (SELECT id,tv FROM third_mixed) t USING(id)")
    yield "duplicate_using_unqualified_key", f"SELECT id FROM {repeated}"
    yield "duplicate_using_chained_keys", (
        f"SELECT id AS merged_id,l.id AS left_id,r.id AS right_id,t.id AS third_id FROM {chain}"
    )
    yield "duplicate_using_chained_stars", f"SELECT l.*,r.*,t.* FROM {chain}"
    yield "duplicate_using_outer_visible_scalar", f"SELECT (SELECT l.id) AS outer_id FROM {repeated}"
    yield "duplicate_using_outer_hidden_scalar", f"SELECT (SELECT r.id) AS outer_id FROM {repeated}"
    yield "duplicate_using_outer_visible_exists", (
        f"SELECT l.id AS left_id FROM {repeated} WHERE EXISTS (SELECT 1 WHERE l.id=1)"
    )
    yield "duplicate_using_outer_hidden_exists", (
        f"SELECT l.id AS left_id FROM {repeated} WHERE EXISTS (SELECT 1 WHERE r.id=1)"
    )
    cte = ("WITH shared AS (SELECT id,lv FROM left_mixed l "
           "LEFT JOIN right_mixed r USING(id)) ")
    yield "using_cte_distinct_aliases_qualified", (
        cte + "SELECT a.id AS left_id,b.id AS right_id FROM shared a JOIN shared b ON a.id=b.id"
    )
    yield "using_cte_distinct_aliases_unqualified", (
        cte + "SELECT id FROM shared a JOIN shared b ON a.id=b.id"
    )
    yield "duplicate_using_cte_distinct_aliases", (
        "WITH shared AS (SELECT * FROM left_mixed l LEFT JOIN "
        "(SELECT id,rv FROM right_mixed) r USING(id,id)) "
        "SELECT a.id AS left_id,b.id AS right_id FROM shared a JOIN shared b ON a.lv=b.lv"
    )
    yield "full_merged_and_source_keys_chained", (
        "SELECT id AS merged_id,l.id AS left_id,r.id AS right_id,t.id AS third_id "
        "FROM left_mixed l FULL JOIN (SELECT id,rv FROM right_mixed) r USING(id) "
        "LEFT JOIN (SELECT id,tv FROM third_mixed) t USING(id)"
    )
    yield "duplicate_full_unqualified_key", (
        "SELECT id FROM left_mixed l FULL JOIN right_mixed r USING(id,id)"
    )
    yield "ordinary_duplicate_projection_using", (
        "SELECT l.id FROM (SELECT id,id FROM left_mixed) l "
        "LEFT JOIN right_mixed r USING(id)"
    )
    yield "ordinary_duplicate_projection_on", (
        "SELECT l.id FROM (SELECT id,id FROM left_mixed) l "
        "LEFT JOIN right_mixed r ON l.id=r.id"
    )
    yield "ordinary_reused_cte_unqualified", (
        "WITH shared AS (SELECT id FROM left_mixed) "
        "SELECT id FROM shared l JOIN shared r ON l.id=r.id"
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
    assert len(definitions) == 15, len(definitions)
    assert len({name for name, _ in definitions}) == len(definitions)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-using-key-identity-review")
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
        assert len(records) == 31, len(records)
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
