"""Capture all sixteen exact historical IF declarations for review 710."""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


QUERIES = [('replay/cast-scalar-cast-cases/10/tinyint_if', 'SELECT IF(CAST(NULL AS BOOLEAN),(SELECT CAST(7 AS TINYINT) DIV CAST(0 AS TINYINT)),CAST(7 AS BIGINT)) AS r'), ('replay/cast-scalar-cast-cases/19/smallint_if', 'SELECT IF(CAST(NULL AS BOOLEAN),(SELECT CAST(7 AS SMALLINT) DIV CAST(0 AS SMALLINT)),CAST(7 AS BIGINT)) AS r'), ('replay/cast-scalar-cast-cases/28/int_if', 'SELECT IF(CAST(NULL AS BOOLEAN),(SELECT CAST(7 AS INT) DIV CAST(0 AS INT)),CAST(7 AS BIGINT)) AS r'), ('replay/cast-scalar-cast-cases/37/bigint_if', 'SELECT IF(CAST(NULL AS BOOLEAN),(SELECT CAST(7 AS BIGINT) DIV CAST(0 AS BIGINT)),CAST(7 AS BIGINT)) AS r'), ('replay/cast-scalar-cast-cases/46/overflow_if', 'SELECT IF(CAST(NULL AS BOOLEAN),(SELECT CAST(2147483647 AS INT) + CAST(1 AS INT)),CAST(7 AS INT)) AS r'), ('replay/cast-scalar-cast-cases/55/modulo_if', 'SELECT IF(CAST(NULL AS BOOLEAN),(SELECT CAST(7 AS INT) % CAST(0 AS INT)),CAST(7 AS INT)) AS r'), ('replay/cast-scalar-cast-cases/64/double_if', 'SELECT IF(CAST(NULL AS BOOLEAN),(SELECT 7D / 0D),CAST(7 AS DOUBLE)) AS r'), ('replay/cast-scalar-cast-cases/73/decimal_if', 'SELECT IF(CAST(NULL AS BOOLEAN),(SELECT CAST(7 AS DECIMAL(10,2)) / CAST(0 AS DECIMAL(10,2))),CAST(7 AS DECIMAL(23,13))) AS r')]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-review-710-accepted-if-schemas")
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
        assert len(records) == 17
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations":len(records)-1,"output":str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
