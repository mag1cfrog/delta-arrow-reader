"""Recheck affected historical queries independently, using the frozen corpus SQL."""
import gzip
import json
import os
import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
directory = Path(__file__).parent
output = directory / "expression-nullability-replay-spark.jsonl"
# IDs select queries; Spark recomputes every expected outcome.
ids = [json.loads(line)["id"] for line in output.read_text().splitlines()[1:]]
selected_ids = set(ids)
assert len(ids) == len(selected_ids) == 2297
queries = {}
with gzip.open(directory / "accepted-runtime.jsonl.gz", "rt") as source:
    for line in source:
        case = json.loads(line)
        if case["id"] in selected_ids:
            queries[case["id"]] = case
assert set(queries) == selected_ids

spark = (SparkSession.builder.master("local[1]").appName("dar-expression-nullability-replay")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    records = [{"spark_version": spark.version, "timezone": "UTC"}]
    observed = {}
    for identity in ids:
        case = queries[identity]
        key = (case["sql"], case["ansi"])
        if key not in observed:
            spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
            try:
                frame = spark.sql(case["sql"])
                rows = [[None if value is None else str(value).lower()
                         if isinstance(value, bool) else str(value) for value in row]
                        for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                expected = {"schema": frame.schema.jsonValue(), "rows": rows}
            except Exception as error:
                message = str(error)
                condition = error.getCondition() if hasattr(error, "getCondition") else None
                if condition is None:
                    match = re.search(r"\[(DIVIDE_BY_ZERO|REMAINDER_BY_ZERO|NUMERIC_VALUE_OUT_OF_RANGE|CAST_INVALID_INPUT|DATATYPE_MISMATCH)(?:\.[A-Z_]+)?\]", message)
                    condition = match.group(1) if match else None
                if condition is None:
                    assert message == "Underflow", message
                    expected = {"error": message}
                else:
                    expected = {"condition": condition.split(".")[0]}
            observed[key] = expected
        records.append({"id": identity, "ansi": case["ansi"], "expected": observed[key]})
    output.write_text("".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1, "unique_queries": len(observed)}))
finally:
    spark.stop()
