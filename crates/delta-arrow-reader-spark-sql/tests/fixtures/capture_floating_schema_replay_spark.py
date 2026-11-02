"""Recheck the recorded historical IDs against Spark, using the frozen corpus SQL."""
import gzip
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
directory = Path(__file__).parent
output = directory / "floating-schema-replay-spark.jsonl"
# Only IDs select the queries; expected outcomes are recomputed by Spark.
ids = [json.loads(line)["id"] for line in output.read_text().splitlines()[1:]]
selected_ids = set(ids)
assert len(ids) == len(selected_ids) == 252
queries = {}
with gzip.open(directory / "accepted-runtime.jsonl.gz", "rt") as source:
    for line in source:
        case = json.loads(line)
        if case["id"] in selected_ids:
            queries[case["id"]] = case
assert set(queries) == selected_ids

spark = (SparkSession.builder.master("local[1]").appName("dar-floating-schema-replay")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    records = [{"spark_version": spark.version, "timezone": "UTC"}]
    for identity in ids:
        case = queries[identity]
        spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
        try:
            frame = spark.sql(case["sql"])
            rows = [[None if value is None else str(value).lower()
                     if isinstance(value, bool) else str(value) for value in row]
                    for row in frame.collect()]
            rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
            expected = {"schema": frame.schema.jsonValue(), "rows": rows}
        except Exception as error:
            assert "[CAST_INVALID_INPUT]" in str(error), str(error)
            expected = {"condition": "CAST_INVALID_INPUT"}
        records.append({"id": identity, "ansi": case["ansi"], "expected": expected})
    output.write_text("".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1}))
finally:
    spark.stop()
