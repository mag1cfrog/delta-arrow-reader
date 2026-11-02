"""Capture duplicate USING keys and chained preserved/hidden key order."""
import argparse
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types


def queries():
    for label, join, preserved in [
        ("inner", "INNER", "l"), ("left", "LEFT", "l"),
        ("right", "RIGHT", "r"), ("full", "FULL", "l"),
        ("semi", "LEFT SEMI", "l"), ("anti", "LEFT ANTI", "l"),
    ]:
        for projection, columns in [
            ("qualified_key", f"{preserved}.id"),
            ("qualified_star", f"{preserved}.*"),
            ("plain_star", "*"),
        ]:
            yield f"duplicate_using_{label}_{projection}", (
                f"SELECT {columns} FROM left_mixed l {join} JOIN right_mixed r USING(id,id)"
            )
    for join in ["RIGHT", "FULL"]:
        for projection, columns in [
            ("left_star", "l.*"), ("all_stars", "l.*,r.*,t.*"), ("plain_star", "*"),
        ]:
            yield f"chained_{join.lower()}_then_left_{projection}", (
                f"SELECT {columns} FROM left_mixed l {join} JOIN "
                "(SELECT id,rv FROM right_mixed) r USING(id) LEFT JOIN "
                "(SELECT k,tv FROM third_mixed) t USING(k)"
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
    parser.add_argument("reference", type=Path, help="original provider/header JSONL")
    parser.add_argument("output", type=Path, help="fresh output file; overwrite is rejected")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    metadata = json.loads(args.reference.read_text().splitlines()[0])
    definitions = list(queries())
    assert len(definitions) == 24, len(definitions)
    assert len({name for name, _ in definitions}) == len(definitions)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-using-key-boundaries")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "false").getOrCreate())
    try:
        assert spark.version == metadata["spark_version"] == "4.2.0"
        spark.sparkContext.setLogLevel("ERROR")
        for provider in metadata["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            rows = [[None if value is None else int(value) for value in row]
                    for row in provider["rows"]]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(provider["name"])
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                record = {"id": name, "ansi": ansi, "sql": sql}
                try:
                    frame = spark.sql(sql)
                    execution = frame._jdf.queryExecution()
                    record["schema_stages"] = {
                        stage: json.loads(plan.schema().json())
                        for stage, plan in [("analyzed", execution.analyzed()),
                                            ("optimized", execution.optimizedPlan()),
                                            ("executed", execution.executedPlan())]
                    }
                    rows = [[None if value is None else str(value) for value in row]
                            for row in frame.collect()]
                    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                    record["expected"] = {"schema": frame.schema.jsonValue(), "rows": rows}
                    assert record["expected"]["schema"] == record["schema_stages"]["analyzed"]
                except Exception as error:
                    record["expected"] = error_details(error)
                records.append(record)
        assert len(records) == 49, len(records)
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
