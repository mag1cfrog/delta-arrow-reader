"""Capture independent Spark evidence for the explicit grouped aliases that resemble internal projection IDs."""

import argparse
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types


def queries():
    yield "grouped_internal_id_alias_collision", (
        "SELECT q.`#0` FROM (SELECT id AS `#0`,id AS `#0` "
        "FROM left_mixed GROUP BY 1) q"
    )
    yield "grouped_ordinary_alias_control", (
        "SELECT q.value FROM (SELECT id AS value,id AS value "
        "FROM left_mixed GROUP BY 1) q"
    )
    yield "grouped_internal_id_distinct_sources", (
        "SELECT q.`#0` FROM (SELECT id AS `#0`,k AS `#0` "
        "FROM left_mixed GROUP BY id,k) q"
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
    assert len(definitions) == 3, len(definitions)
    assert len({name for name, _ in definitions}) == len(definitions)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-grouped-alias-identity-collision-review")
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
        assert len(records) == 7, len(records)
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
