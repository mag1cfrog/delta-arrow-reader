"""Capture observable INTERSECT/EXCEPT zero results with PySpark 4.2.0."""
import json
import os
import struct
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types

os.environ["PYSPARK_PYTHON"] = sys.executable
fixture_dir = Path(__file__).parent
with (fixture_dir / "set-operation-float-keys-spark.jsonl").open() as source:
    inputs = json.loads(next(source))["inputs"]
queries = []
for kind, sql_type in [("float", "FLOAT"), ("double", "DOUBLE")]:
    negative = f"CAST('-0.0' AS {sql_type})"
    positive = f"CAST('0.0' AS {sql_type})"
    pairs = []
    for label, left, right in [
        ("zero", "negative", "positive"), ("same", "negative", "negative"),
        ("mixed", "mixed_left", "mixed_right"), ("nan", "nan_left", "nan_right"),
        ("empty", "negative", "empty"), ("both_empty", "empty", "empty"),
    ]:
        for columns in ["k", "k,label"]:
            pairs.append((f"{label}_{columns.replace(',', '_')}",
                          f"SELECT {columns} FROM {kind}_{left}",
                          f"SELECT {columns} FROM {kind}_{right}", columns))
    pairs += [
        ("literal_same", f"SELECT {negative} AS k", f"SELECT {negative} AS k", "k"),
        ("literal_opposite", f"SELECT {negative} AS k", f"SELECT {positive} AS k", "k"),
        ("literal_empty", f"SELECT {negative} AS k", f"SELECT {positive} AS k WHERE false", "k"),
        ("values", f"SELECT k FROM VALUES ({negative}),({negative}) v(k)",
         f"SELECT k FROM VALUES ({positive}) v(k)", "k"),
        ("projection", f"SELECT {negative} AS k FROM {kind}_positive",
         f"SELECT {positive} AS k FROM {kind}_negative", "k"),
        ("computed", f"SELECT {positive} * -1 AS k FROM {kind}_positive",
         f"SELECT {positive} AS k FROM {kind}_negative", "k"),
        ("filtered", f"SELECT k FROM (SELECT {negative} AS k FROM {kind}_positive) q WHERE k=0",
         f"SELECT {positive} AS k", "k"),
        ("limited", f"SELECT k FROM (SELECT {negative} AS k FROM {kind}_positive LIMIT 1) q",
         f"SELECT {positive} AS k", "k"),
        ("nested_distinct", f"SELECT k FROM (SELECT {negative} AS k INTERSECT SELECT {positive} AS k) q",
         f"SELECT {positive} AS k", "k"),
        ("nested_all", f"SELECT k FROM (SELECT {negative} AS k INTERSECT ALL SELECT {positive} AS k) q",
         f"SELECT {negative} AS k", "k"),
        ("grouped_literal", f"SELECT {negative} AS k FROM {kind}_positive GROUP BY {negative}",
         f"SELECT {positive} AS k", "k"),
        ("aggregate_value", f"SELECT first({negative}) AS k FROM {kind}_positive",
         f"SELECT {positive} AS k", "k"),
        ("nested_computed", f"SELECT k * -1 AS k FROM (SELECT {positive} AS k INTERSECT SELECT {negative} AS k) q",
         f"SELECT {positive} AS k", "k"),
        ("coerced_constant", f"SELECT {negative} AS k FROM {kind}_positive",
         f"SELECT CAST('0.0' AS {'DOUBLE' if sql_type == 'FLOAT' else 'FLOAT'}) AS k", "k"),
        ("join_preserved_constant", f"SELECT q.k FROM (SELECT {negative} AS k,label FROM {kind}_negative) q "
         f"LEFT JOIN {kind}_mixed_left t ON t.label=q.label", f"SELECT {positive} AS k", "k"),
        ("join_nullable_constant", f"SELECT q.k FROM {kind}_mixed_left t LEFT JOIN "
         f"(SELECT {negative} AS k,label FROM {kind}_negative) q ON t.label=q.label", f"SELECT {positive} AS k", "k"),
        ("inner_join_constant", f"SELECT q.k FROM (SELECT {negative} AS k,label FROM {kind}_negative) q "
         f"JOIN {kind}_mixed_left t ON t.label=q.label", f"SELECT {positive} AS k", "k"),
        ("deduplicated_constant", f"SELECT DISTINCT {negative} AS k FROM {kind}_positive",
         f"SELECT {positive} AS k", "k"),
        ("windowed_constant", f"SELECT k FROM (SELECT k,row_number() OVER (ORDER BY label) AS rn "
         f"FROM (SELECT {negative} AS k,label FROM {kind}_positive) p) q", f"SELECT {positive} AS k", "k"),
        ("ordered_constant", f"SELECT k FROM (SELECT k FROM (SELECT {negative} AS k,label "
         f"FROM {kind}_positive) p ORDER BY label LIMIT 1) q", f"SELECT {positive} AS k", "k"),
    ]
    for label, left, right, columns in pairs:
        for operation in ["INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]:
            for reverse in [False, True]:
                first, second = (right, left) if reverse else (left, right)
                extra = ",label" if columns == "k,label" else ""
                sql = (f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated{extra} "
                       f"FROM ({first} {operation} {second}) result")
                queries.append((f"{kind}_{label}_{operation.lower().replace(' ', '_')}_{reverse}", sql))
assert len({name for name, _ in queries}) == len(queries)

spark = (SparkSession.builder.master("local[1]").appName("dar-set-zero-results")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.ui.retainedExecutions", "1").config("spark.ui.retainedJobs", "1")
         .config("spark.ui.retainedStages", "1").config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
assert spark.version == "4.2.0", spark.version
spark.sparkContext.setLogLevel("ERROR")
count = 0
try:
    with (fixture_dir / "set-operation-zero-results-spark.jsonl").open("w") as output:
        output.write(json.dumps({"spark_version":spark.version,"timezone":"UTC","inputs":inputs}) + "\n")
        for partitions in [1, 2]:
            for source in inputs:
                data_type = types.FloatType() if source["type"] == "float" else types.DoubleType()
                encoding = "!f" if source["type"] == "float" else "!d"
                rows = [(None if value is None else struct.unpack(encoding, bytes.fromhex(value))[0], label)
                        for value, label in source["rows"]]
                schema = types.StructType([types.StructField("k", data_type, True),
                                           types.StructField("label", types.StringType(), True)])
                spark.createDataFrame(spark.sparkContext.parallelize(rows, partitions), schema).createOrReplaceTempView(source["name"])
            for adaptive in [False, True]:
                spark.conf.set("spark.sql.adaptive.enabled", str(adaptive).lower())
                for ansi in [False, True]:
                    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                    for name, sql in queries:
                        frame = spark.sql(sql)
                        rows = [list(row) for row in frame.collect()]
                        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                        record = {"id":name,"sql":sql,"ansi":ansi,"adaptive":adaptive,
                                  "partitions":partitions,"schema":frame.schema.jsonValue(),"rows":rows}
                        output.write(json.dumps(record, separators=(",", ":")) + "\n")
                        output.flush()
                        count += 1
                        if count % 192 == 0:
                            print(json.dumps({"observations":count}), flush=True)
    print(json.dumps({"done":count}), flush=True)
finally:
    spark.stop()
