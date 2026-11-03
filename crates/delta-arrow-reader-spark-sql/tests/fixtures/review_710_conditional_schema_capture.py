"""Capture exact new-unit and accepted dynamic conditional schemas for review 710."""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


QUERIES = [
    ("unit_if", "SELECT IF(CAST(NULL AS BOOLEAN),(SELECT CAST(7 AS INT) DIV CAST(0 AS INT)),CAST(7 AS BIGINT)) AS r"),
    ("unit_case", "SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN (SELECT CAST(7 AS INT) DIV CAST(0 AS INT)) ELSE CAST(7 AS BIGINT) END AS r"),
    ("dynamic_case", "SELECT k,CASE WHEN b THEN (SELECT 3L) ELSE 7L END AS r FROM VALUES (0,CAST(NULL AS BOOLEAN)),(1,false),(2,true) t(k,b) ORDER BY k"),
    ("dynamic_if", "SELECT k,IF(b,(SELECT 3L),7L) AS r FROM VALUES (0,CAST(NULL AS BOOLEAN)),(1,false),(2,true) t(k,b) ORDER BY k"),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-review-710-conditional-schemas")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": [],
                    "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for label, sql in QUERIES:
                record = {"id": label, "ansi": ansi, "sql": sql}
                try:
                    frame = spark.sql(sql)
                    record["public_schema"] = frame.schema.jsonValue()
                    execution = frame._jdf.queryExecution()
                    record["analyzed_schema"] = json.loads(execution.analyzed().schema().json())
                    record["optimized_schema"] = json.loads(execution.optimizedPlan().schema().json())
                    record["executed_schema_before_collect"] = json.loads(execution.executedPlan().schema().json())
                    record["analyzed_plan"] = execution.analyzed().toString()
                    record["optimized_plan"] = execution.optimizedPlan().toString()
                    collected = frame.collect()
                    record["executed_schema_after_collect"] = json.loads(execution.executedPlan().schema().json())
                    record["physical_plan"] = execution.executedPlan().toString()
                    record["expected"] = {"schema": frame.schema.jsonValue(),
                                          "rows": [[None if v is None else str(v) for v in row]
                                                   for row in collected]}
                except Exception as error:
                    record["error"] = {"message": str(error),
                                       "condition": getattr(error, "getCondition", lambda: None)()}
                records.append(record)
        assert len(records) == 9
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations":len(records)-1,"output":str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
