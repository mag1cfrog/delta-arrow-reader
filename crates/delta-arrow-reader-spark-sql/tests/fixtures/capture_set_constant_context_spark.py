"""Capture constant-result boundaries for INTERSECT/EXCEPT with PySpark 4.2.0."""
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
    outer = (f"SELECT q.k FROM {kind}_mixed_left p LEFT JOIN "
             f"(SELECT {negative} AS k,label FROM {kind}_negative) q ON p.label=q.label")
    nested = (f"SELECT q.k FROM {kind}_mixed_left p LEFT JOIN "
              f"(SELECT r.k,n.label FROM {kind}_negative n LEFT JOIN "
              f"(SELECT {negative} AS k,label FROM {kind}_negative) r ON n.label=r.label) q "
              "ON p.label=q.label")
    lefts = [
        ("outer", outer),
        ("filtered", f"SELECT k FROM ({outer}) j WHERE k IS NOT NULL"),
        ("limit", outer + " LIMIT 5"),
        ("nested", nested),
        ("fallback", f"SELECT COALESCE(k,{negative}) AS k FROM ({outer}) j"),
    ]
    rights = [
        ("literal", f"SELECT {positive} AS k"),
        ("nullable", f"SELECT k FROM {kind}_positive"),
        ("filtered", f"SELECT k FROM {kind}_positive WHERE k IS NOT NULL"),
        ("nonnull", f"SELECT k FROM {kind}_nonnull"),
        ("values", f"SELECT k FROM VALUES ({positive}) v(k)"),
        ("values_null", f"SELECT k FROM VALUES ({positive}),(CAST(NULL AS {sql_type})) v(k)"),
        ("empty", f"SELECT k FROM {kind}_empty"),
        ("null", f"SELECT CAST(NULL AS {sql_type}) AS k"),
    ]
    pairs = [(f"{left_name}_{right_name}", left, right)
             for left_name, left in lefts for right_name, right in rights]
    pairs += [(f"outer_{name}", outer, right) for name, right in [
        ("nested_intersect", f"SELECT k FROM (SELECT {positive} AS k INTERSECT SELECT {positive} AS k) q"),
        ("grouped", f"SELECT {positive} AS k FROM {kind}_positive GROUP BY {positive}"),
        ("projection_values", f"SELECT {positive} AS k FROM VALUES (1),(2) v(x)"),
        ("projection_table", f"SELECT {positive} AS k FROM {kind}_positive"),
    ]]
    for name, source in [
        ("values", "VALUES (1),(2) v(x)"),
        ("filtered_values", "(SELECT x FROM VALUES (1),(2) v(x) WHERE x>0) v"),
        ("limited_values", "(SELECT x FROM VALUES (1),(2) v(x) LIMIT 1) v"),
        ("sorted_values", "(SELECT x FROM VALUES (1),(2) v(x) ORDER BY x DESC) v"),
        ("distinct_values", "(SELECT DISTINCT x FROM VALUES (1),(2) v(x)) v"),
        ("grouped_values", "(SELECT x FROM VALUES (1),(2) v(x) GROUP BY x) v"),
        ("window_values", "(SELECT x,row_number() OVER (ORDER BY x) rn FROM VALUES (1),(2) v(x)) v"),
        ("joined_values", "(SELECT a.x FROM VALUES (1),(2) a(x) JOIN VALUES (1),(2) b(x) ON a.x=b.x) v"),
    ]:
        pairs.append((name + "_left", f"SELECT {negative} AS k FROM {source}", f"SELECT {positive} AS k"))
        pairs.append((name + "_right", outer, f"SELECT {positive} AS k FROM {source}"))
    for name, query in [
        ("right_set_except", f"SELECT {positive} AS k EXCEPT SELECT CAST(1 AS {sql_type}) AS k"),
        ("right_set_null", f"SELECT CAST(NULL AS {sql_type}) AS k INTERSECT SELECT CAST(NULL AS {sql_type}) AS k"),
        ("right_set_empty", f"SELECT {positive} AS k EXCEPT SELECT {positive} AS k"),
    ]:
        pairs.append((name, outer, f"SELECT k FROM ({query}) q"))
    scalar_source = (f"(SELECT x,(SELECT COUNT(*) FROM {kind}_positive) AS n "
                     "FROM VALUES (1),(2) v(x)) q WHERE n>0")
    marker_outer = (f"SELECT q.k FROM {kind}_mixed_left p LEFT JOIN "
                    f"(SELECT {negative} AS k INTERSECT SELECT {positive} AS k) q "
                    "ON p.label='same'")
    pairs += [
        ("scalar_projection_left", f"SELECT {negative} AS k FROM {scalar_source}", f"SELECT {positive} AS k"),
        ("scalar_projection_right", outer, f"SELECT {positive} AS k FROM {scalar_source}"),
        ("marker_nullable_registered", marker_outer, f"SELECT k FROM {kind}_positive"),
        ("marker_nullable_literal", marker_outer, f"SELECT {positive} AS k"),
        ("marker_nullable_null", marker_outer, f"SELECT CAST(NULL AS {sql_type}) AS k"),
        ("right_outer", outer.replace("LEFT JOIN", "RIGHT JOIN"), f"SELECT {positive} AS k"),
        ("full_outer_literal", outer.replace("LEFT JOIN", "FULL JOIN"), f"SELECT {positive} AS k"),
        ("full_outer_registered", outer.replace("LEFT JOIN", "FULL JOIN"), f"SELECT k FROM {kind}_positive"),
        ("stable_sibling", f"SELECT k,current_timestamp() AS tick FROM ({outer}) q",
         f"SELECT {positive} AS k,current_timestamp() AS tick"),
        ("sorted_partition_left", f"SELECT k FROM (SELECT {negative} AS k,label FROM {kind}_positive SORT BY label) q",
         f"SELECT {positive} AS k"),
        ("sorted_partition_right", outer,
         f"SELECT k FROM (SELECT {positive} AS k,label FROM {kind}_positive SORT BY label) q"),
    ]
    for name, left, right in pairs:
        for operation in ["INTERSECT", "EXCEPT"]:
            queries.append((f"{kind}_{name}_{operation.lower()}",
                            "SELECT CAST(k AS STRING) AS text,CAST(-k AS STRING) AS negated "
                            f"FROM (({left}) {operation} ({right})) q"))
assert len(queries) == 296
assert len({name for name, _ in queries}) == len(queries)

spark = (SparkSession.builder.master("local[1]").appName("dar-set-constant-context")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.adaptive.enabled", "false").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ui.retainedExecutions", "1").config("spark.ui.retainedJobs", "1")
         .config("spark.ui.retainedStages", "1").getOrCreate())
assert spark.version == "4.2.0", spark.version
spark.sparkContext.setLogLevel("ERROR")
try:
    for source in inputs:
        data_type = types.FloatType() if source["type"] == "float" else types.DoubleType()
        encoding = "!f" if source["type"] == "float" else "!d"
        rows = [(None if value is None else struct.unpack(encoding, bytes.fromhex(value))[0], label)
                for value, label in source["rows"]]
        schema = types.StructType([types.StructField("k", data_type, True),
                                   types.StructField("label", types.StringType(), True)])
        spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema).createOrReplaceTempView(source["name"])
    for kind in ["float", "double"]:
        data_type = types.FloatType() if kind == "float" else types.DoubleType()
        schema = types.StructType([types.StructField("k", data_type, False)])
        spark.createDataFrame([(0.0,)], schema).createOrReplaceTempView(kind + "_nonnull")
    with (fixture_dir / "set-constant-context-spark.jsonl").open("w") as output:
        output.write(json.dumps({"spark_version": spark.version, "timezone": "UTC", "inputs": inputs,
                                 "adaptive": False, "input_partitions": 2,
                                 "nonnull_tables": ["float_nonnull", "double_nonnull"]}) + "\n")
        count = 0
        for ansi in [False, True]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in queries:
                frame = spark.sql(sql)
                rows = [list(row) for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                output.write(json.dumps({"id": name, "sql": sql, "ansi": ansi,
                                         "schema": frame.schema.jsonValue(), "rows": rows,
                                         "optimized_plan": frame._jdf.queryExecution().optimizedPlan().toString()},
                                        separators=(",", ":")) + "\n")
                output.flush()
                count += 1
                if count % 40 == 0:
                    print(json.dumps({"observations": count}), flush=True)
        assert count == 592
        print(json.dumps({"done": count}), flush=True)
finally:
    spark.stop()
