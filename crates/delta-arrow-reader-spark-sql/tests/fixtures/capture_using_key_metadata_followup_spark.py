"""Capture original key spelling and chained USING visibility independently."""
import argparse
import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types


def queries():
    for join in ["LEFT", "RIGHT", "FULL", "LEFT SEMI", "LEFT ANTI"]:
        for projection, columns in [("plain", "*"), ("left", "l.*")]:
            yield f"uppercase_using_{join.replace(' ', '_').lower()}_{projection}", (
                f"SELECT {columns} FROM left_mixed l {join} JOIN right_mixed r USING(ID)"
            )
    yield "mixed_key_spelling_left", (
        "SELECT l.*,r.* FROM (SELECT id AS `Id`,lv FROM left_mixed) l LEFT JOIN "
        "(SELECT id AS `ID`,rv FROM right_mixed) r USING(id)"
    )
    yield "mixed_key_spelling_right", (
        "SELECT l.*,r.* FROM (SELECT id AS `Id`,lv FROM left_mixed) l RIGHT JOIN "
        "(SELECT id AS `ID`,rv FROM right_mixed) r USING(id)"
    )
    yield "chained_left_star_order", (
        "SELECT l.*,r.*,t.* FROM left_mixed l LEFT JOIN "
        "(SELECT id,rv FROM right_mixed) r USING(id) LEFT JOIN "
        "(SELECT k,tv FROM third_mixed) t USING(k)"
    )
    yield "chained_original_key_left", (
        "SELECT l.*,r.*,t.* FROM left_mixed l LEFT JOIN "
        "(SELECT k,rv FROM right_mixed) r USING(k) LEFT JOIN "
        "(SELECT id,tv FROM third_mixed) t USING(id)"
    )
    for join in ["INNER", "RIGHT"]:
        yield f"chained_original_key_{join.lower()}", (
            "SELECT id AS merged_id,l.id AS left_id,r.id AS right_id,t.id AS third_id "
            f"FROM left_mixed l LEFT JOIN right_mixed r USING(id) {join} JOIN third_mixed t USING(id)"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    metadata = json.loads(args.reference.read_text().splitlines()[0])
    definitions = list(queries())
    assert len(definitions) == 16
    assert len({name for name, _ in definitions}) == len(definitions)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-using-key-metadata-followup")
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
                frame = spark.sql(sql)
                rows = [[None if value is None else str(value) for value in row]
                        for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                execution = frame._jdf.queryExecution()
                stages = {stage: json.loads(plan.schema().json()) for stage, plan in [
                    ("analyzed", execution.analyzed()), ("optimized", execution.optimizedPlan()),
                    ("executed", execution.executedPlan()),
                ]}
                expected = {"schema": frame.schema.jsonValue(), "rows": rows}
                assert expected["schema"] == stages["analyzed"]
                records.append({"id": name, "ansi": ansi, "sql": sql,
                                "expected": expected, "schema_stages": stages})
        assert len(records) == 33
        with args.output.open("x") as destination:
            destination.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                      for record in records))
        print(json.dumps({"observations": len(records) - 1, "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
