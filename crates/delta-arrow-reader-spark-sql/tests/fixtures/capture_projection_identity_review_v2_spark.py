"""Capture independent Spark evidence for the outer-reference, grouping and set-operation projection identity boundaries."""

import argparse
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types


def queries():
    yield "outer_projection_local_same", (
        "SELECT q.id FROM (SELECT l.id,l.id FROM left_mixed l) q"
    )
    yield "outer_projection_local_distinct", (
        "SELECT q.id FROM (SELECT l.id,r.id FROM left_mixed l "
        "CROSS JOIN right_mixed r) q"
    )
    yield "outer_projection_same", (
        "SELECT (SELECT q.id FROM (SELECT l.id,l.id) q) AS selected_id "
        "FROM left_mixed l"
    )
    yield "outer_projection_distinct", (
        "SELECT (SELECT q.id FROM (SELECT l.id,r.id) q) AS selected_id "
        "FROM left_mixed l CROSS JOIN right_mixed r"
    )
    yield "outer_projection_explicit_alias", (
        "SELECT (SELECT q.id FROM (SELECT l.id,l.id AS id) q) AS selected_id "
        "FROM left_mixed l"
    )
    yield "outer_projection_materialized_then_repeated", (
        "SELECT (SELECT q.id FROM (SELECT s.id,s.id FROM (SELECT l.id) s) q) "
        "AS selected_id FROM left_mixed l"
    )
    yield "outer_projection_exists", (
        "SELECT l.id FROM left_mixed l WHERE EXISTS "
        "(SELECT 1 FROM (SELECT l.id,l.id) q WHERE q.id=1)"
    )
    yield "grouped_projection_same", (
        "SELECT q.id FROM (SELECT id,id FROM left_mixed GROUP BY id) q"
    )
    yield "grouped_projection_distinct", (
        "SELECT q.id FROM (SELECT id,k AS id FROM left_mixed GROUP BY id,k) q"
    )
    yield "grouped_projection_explicit_alias", (
        "SELECT q.id FROM (SELECT id,id AS id FROM left_mixed GROUP BY id) q"
    )
    yield "union_projection_left_same_right_distinct", (
        "SELECT q.id FROM (SELECT id,id FROM left_mixed UNION ALL "
        "SELECT id,k FROM left_mixed) q"
    )
    yield "union_projection_both_same", (
        "SELECT q.id FROM (SELECT id,id FROM left_mixed UNION ALL "
        "SELECT id,id FROM right_mixed) q"
    )
    yield "union_projection_left_distinct_right_same", (
        "SELECT q.id FROM (SELECT id,k AS id FROM left_mixed UNION ALL "
        "SELECT id,id FROM right_mixed) q"
    )
    yield "union_projection_distinct_slots_wildcard", (
        "SELECT q.* FROM (SELECT id,id FROM left_mixed UNION ALL "
        "SELECT id,k FROM left_mixed) q"
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
    assert len(definitions) == 14, len(definitions)
    assert len({name for name, _ in definitions}) == len(definitions)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-outer-projection-identity-review")
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
        assert len(records) == 29, len(records)
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
