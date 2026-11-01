"""Capture UNION result strings and negation with explicit Spark 4.2 inputs."""
import json
import os
from pathlib import Path
import struct
import sys

from pyspark.sql import SparkSession, types

os.environ["PYSPARK_PYTHON"] = sys.executable
fixture_dir = Path(__file__).parent
metadata = json.loads((fixture_dir / "set-operation-float-keys-spark.jsonl").read_text().splitlines()[0])
queries = []
for kind, sql_type in [("float", "FLOAT"), ("double", "DOUBLE")]:
    negative = f"CAST('-0.0' AS {sql_type})"
    positive = f"CAST('0.0' AS {sql_type})"
    for label, left, right in [
        ("same", "negative", "negative"),
        ("opposite", "negative", "positive"),
        ("reverse", "positive", "negative"),
        ("mixed", "mixed_left", "mixed_right"),
        ("nan", "nan_left", "nan_right"),
        ("empty", "negative", "empty"),
        ("both_empty", "empty", "empty"),
    ]:
        for columns in ["k", "k,label"]:
            extra = ",label" if columns != "k" else ""
            for operation in ["UNION", "UNION ALL"]:
                sql = f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated{extra} FROM (SELECT {columns} FROM {kind}_{left} {operation} SELECT {columns} FROM {kind}_{right}) q"
                queries.append((f"{kind}_{label}_{columns.replace(',', '_')}_{operation.replace(' ', '_').lower()}", sql))
    pairs = [
        ("literal_same", f"SELECT {negative} AS k", f"SELECT {negative} AS k"),
        ("literal_opposite", f"SELECT {negative} AS k", f"SELECT {positive} AS k"),
        ("literal_aliases", f"SELECT {negative} AS k", f"SELECT {negative} AS renamed"),
        ("literal_empty", f"SELECT {negative} AS k", f"SELECT {positive} AS k WHERE false"),
        ("projection_same", f"SELECT {negative} AS k FROM {kind}_negative", f"SELECT {negative} AS k FROM {kind}_negative"),
        ("projection_other_source", f"SELECT {negative} AS k FROM {kind}_negative", f"SELECT {negative} AS k FROM {kind}_positive"),
        ("projection_aliases", f"SELECT {negative} AS k FROM {kind}_negative a", f"SELECT {negative} AS renamed FROM {kind}_negative b"),
        ("computed_same", f"SELECT {positive} * -1 AS k", f"SELECT {positive} * -1 AS k"),
        ("computed_literal", f"SELECT {positive} * -1 AS k", f"SELECT {negative} AS k"),
        ("values_same", f"SELECT k FROM VALUES ({negative}),({negative}) v(k)", f"SELECT k FROM VALUES ({negative}),({negative}) v(k)"),
        ("values_opposite", f"SELECT k FROM VALUES ({negative}),({negative}) v(k)", f"SELECT k FROM VALUES ({positive}) v(k)"),
        ("filtered_same", f"SELECT {negative} AS k FROM {kind}_negative WHERE k IS NOT NULL", f"SELECT {negative} AS k FROM {kind}_negative WHERE k IS NOT NULL"),
        ("filtered_other", f"SELECT {negative} AS k FROM {kind}_negative WHERE k IS NOT NULL", f"SELECT {negative} AS k FROM {kind}_negative WHERE label IS NOT NULL"),
        ("grouped_same", f"SELECT {negative} AS k FROM {kind}_negative GROUP BY {negative}", f"SELECT {negative} AS k FROM {kind}_negative GROUP BY {negative}"),
        ("coerced_literal", f"SELECT {negative} AS k", f"SELECT CAST('-0.0' AS {'DOUBLE' if kind == 'float' else 'FLOAT'}) AS k"),
        ("null_literal", f"SELECT CAST(NULL AS {sql_type}) AS k", f"SELECT {negative} AS k"),
    ]
    for label, left, right in pairs:
        for operation in ["UNION", "UNION ALL"]:
            sql = f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM ({left} {operation} {right}) q"
            queries.append((f"{kind}_{label}_{operation.replace(' ', '_').lower()}", sql))
    for label, body in [
        ("nested_same", f"SELECT {negative} AS k UNION SELECT {negative} AS k UNION SELECT {negative} AS k"),
        ("nested_mixed", f"SELECT {negative} AS k UNION SELECT {negative} AS k UNION SELECT {positive} AS k"),
        ("nested_all", f"(SELECT {negative} AS k UNION ALL SELECT {negative} AS k) UNION SELECT {negative} AS k"),
        ("nested_intersect", f"(SELECT {negative} AS k INTERSECT SELECT {positive} AS k) UNION (SELECT {negative} AS k INTERSECT SELECT {positive} AS k)"),
        ("registered_outer_limit", f"SELECT k FROM {kind}_negative UNION SELECT k FROM {kind}_negative LIMIT 1"),
    ]:
        queries.append((f"{kind}_{label}", f"SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated FROM ({body}) q"))
assert len(queries) == 130
assert len({name for name, _ in queries}) == len(queries)
setting_controls = {"same_k_union", "opposite_k_union", "literal_same_union", "projection_same_union", "projection_other_source_union", "registered_outer_limit"}

spark = (SparkSession.builder.master("local[1]").appName("dar-union-zero-results")
    .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.ui.retainedExecutions", "1").config("spark.ui.retainedJobs", "1")
    .config("spark.ui.retainedStages", "1").config("spark.sql.session.timeZone", "UTC")
    .getOrCreate())
assert spark.version == "4.2.0", spark.version
spark.sparkContext.setLogLevel("ERROR")
count = 0
try:
    with (fixture_dir / "union-zero-results-spark.jsonl").open("x") as output:
        output.write(json.dumps(metadata) + "\n")
        for partitions, adaptive in [(2, False), (1, False), (1, True), (2, True)]:
            for item in metadata["inputs"]:
                is_float = item["type"] == "float"
                data_type = types.FloatType() if is_float else types.DoubleType()
                encoding = ">f" if is_float else ">d"
                rows = [(None if value is None else struct.unpack(encoding, bytes.fromhex(value))[0], label) for value, label in item["rows"]]
                schema = types.StructType([types.StructField("k", data_type, True), types.StructField("label", types.StringType(), True)])
                spark.createDataFrame(spark.sparkContext.parallelize(rows, partitions), schema).createOrReplaceTempView(item["name"])
            spark.conf.set("spark.sql.adaptive.enabled", str(adaptive).lower())
            for ansi in [False, True]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for name, sql in queries:
                    if (partitions, adaptive) != (2, False) and name.split("_", 1)[1] not in setting_controls:
                        continue
                    frame = spark.sql(sql)
                    rows = [list(row) for row in frame.collect()]
                    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                    output.write(json.dumps({"id":name,"sql":sql,"ansi":ansi,"adaptive":adaptive,"partitions":partitions,"schema":frame.schema.jsonValue(),"rows":rows,"optimized_plan":frame._jdf.queryExecution().optimizedPlan().toString()}, separators=(",", ":")) + "\n")
                    output.flush()
                    count += 1
                    if count % 32 == 0:
                        print(json.dumps({"observations":count}), flush=True)
        assert count == 332, count
finally:
    spark.stop()

