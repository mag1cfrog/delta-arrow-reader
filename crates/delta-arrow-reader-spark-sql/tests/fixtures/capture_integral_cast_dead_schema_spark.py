"""Independent schema evidence for the two #805 replay changes."""

import json
import os
import sys

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
queries = [
    ("replay/cast-dead-cases/52/focused_int_column_case_dead_true",
     "SELECT CASE WHEN k=0 THEN CAST(7 AS INT) ELSE CAST('bad' AS INT) END AS r FROM VALUES (0),(0) t(k)"),
    ("replay/cast-scalar-cast-cases/365/literal_scalar_dynamic_true",
     "SELECT k,CASE WHEN k=0 THEN 7 ELSE (SELECT CAST('bad' AS INT)) END AS r FROM VALUES (0),(0) t(k)"),
]
assert len(queries) == len({name for name, _ in queries}) == 2
spark = (SparkSession.builder.master("local[1]").appName("dar-integral-cast-dead-branch-schema")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.ansi.enabled", "true")
         .getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    for name, sql in queries:
        frame = spark.sql(sql)
        execution = frame._jdf.queryExecution()
        collected = frame.collect()
        executed = execution.executedPlan()
        record = {
            "spark_version": spark.version,
            "id": name,
            "sql": sql,
            "ansi": True,
            "public_schema": frame.schema.jsonValue(),
            "analyzed_schema": json.loads(execution.analyzed().schema().json()),
            "optimized_schema": json.loads(execution.optimizedPlan().schema().json()),
            "executed_schema": json.loads(executed.schema().json()),
            "executed_plan": executed.toString(),
            "rows": [[None if value is None else str(value) for value in row]
                     for row in collected],
        }
        print(json.dumps(record, separators=(",", ":")), flush=True)
finally:
    spark.stop()
