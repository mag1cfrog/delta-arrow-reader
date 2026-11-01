"""Record grouping and set multiplicities after outer joins add NULL rows."""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types

os.environ["PYSPARK_PYTHON"] = sys.executable
inputs = {
    "probe": ["match", "match", "other", "match", "match", "match", "match", "match", None, "match", None],
    "lookup": ["match", "lookup_only"],
    "empty_lookup": [],
}
kinds = [
    ("float", "CAST('-0.0' AS FLOAT)"),
    ("double", "CAST('-0.0' AS DOUBLE)"),
    ("long", "CAST(7 AS BIGINT)"),
    ("string", "'fixed'"),
    ("null_double", "CAST(NULL AS DOUBLE)"),
]
queries = []
for kind, value in kinds:
    probe = f"(SELECT {value} AS k,label FROM probe) p"
    lookup = f"(SELECT {value} AS k,label FROM lookup) q"
    sources = [
        ("left_lookup", f"SELECT q.k FROM probe p LEFT JOIN {lookup} ON p.label=q.label"),
        ("right_lookup", f"SELECT q.k FROM {lookup} RIGHT JOIN probe p ON p.label=q.label"),
        ("full_lookup", f"SELECT q.k FROM probe p FULL JOIN {lookup} ON p.label=q.label"),
        ("left_probe", f"SELECT p.k FROM {probe} LEFT JOIN lookup q ON p.label=q.label"),
        ("right_probe", f"SELECT p.k FROM lookup q RIGHT JOIN {probe} ON p.label=q.label"),
        ("full_probe", f"SELECT p.k FROM {probe} FULL JOIN lookup q ON p.label=q.label"),
        ("inner_lookup", f"SELECT q.k FROM probe p INNER JOIN {lookup} ON p.label=q.label"),
        ("semi_probe", f"SELECT p.k FROM {probe} LEFT SEMI JOIN lookup q ON p.label=q.label"),
        ("anti_probe", f"SELECT p.k FROM {probe} LEFT ANTI JOIN lookup q ON p.label=q.label"),
        ("empty_lookup", f"SELECT q.k FROM probe p LEFT JOIN (SELECT {value} AS k,label FROM empty_lookup) q ON p.label=q.label"),
    ]
    for name, source in sources:
        queries.append((f"{kind}_{name}_group", f"SELECT k,COUNT(*) AS n FROM ({source}) j GROUP BY k", kind))
        for operation in ["INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]:
            for reverse in [False, True]:
                left, right = (f"SELECT {value} AS k", source) if reverse else (source, f"SELECT {value} AS k")
                sql = f"SELECT k FROM ({left} {operation} {right}) j"
                queries.append((f"{kind}_{name}_{operation.lower().replace(' ', '_')}_{reverse}", sql, kind))
assert len(queries) == 450
assert len({name for name, _, _ in queries}) == len(queries)
spark = (SparkSession.builder.master("local[1]").appName("dar-outer-join-constants")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.adaptive.enabled", "false").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ui.retainedExecutions", "1").config("spark.ui.retainedJobs", "1")
         .config("spark.ui.retainedStages", "1").getOrCreate())
assert spark.version == "4.2.0", spark.version
spark.sparkContext.setLogLevel("ERROR")
try:
    schema = types.StructType([types.StructField("label", types.StringType(), True)])
    for name, values in inputs.items():
        spark.createDataFrame(spark.sparkContext.parallelize([(value,) for value in values], 2), schema).createOrReplaceTempView(name)
    with Path(__file__).with_name("outer-join-constants-spark.jsonl").open("w") as output:
        output.write(json.dumps({"spark_version": spark.version, "timezone": "UTC", "adaptive": False, "input_partitions": 2, "inputs": inputs}) + "\n")
        count = 0
        for ansi in [False, True]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql, kind in queries:
                frame = spark.sql(sql)
                rows = [list(row) for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                output.write(json.dumps({"id": name, "sql": sql, "ansi": ansi, "value_type": kind, "schema": frame.schema.jsonValue(), "rows": rows}, separators=(",", ":")) + "\n")
                output.flush()
                count += 1
                if count % 90 == 0:
                    print(json.dumps({"observations": count}), flush=True)
        assert count == 900
finally:
    spark.stop()
