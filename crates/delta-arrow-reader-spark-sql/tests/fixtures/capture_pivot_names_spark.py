"""Capture generated PIVOT names and surrounding expression names with Spark 4.2.0."""
import json
import os
import struct
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types


os.environ["PYSPARK_PYTHON"] = sys.executable
inputs = []
queries = []
for kind, encoding in [("float", "!f"), ("double", "!d")]:
    name = f"{kind}_inputs"
    inputs.append({"name": name, "type": kind, "rows": [
        [struct.pack(encoding, 0.0).hex(), "a"],
        [struct.pack(encoding, -0.0).hex(), "a"],
        [None, None],
    ]})
    aggregates = [
        ("upper_sum", "SUM(k)"), ("lower_sum", "sum(k)"), ("mixed_sum", "SuM(k)"),
        ("max", "MAX(label)"), ("nested_function", "UPPER(MAX(label))"),
        ("sorted_casts", "SORT_ARRAY(COLLECT_LIST(CAST(k AS STRING)))"),
        ("descending", "SORT_ARRAY(COLLECT_LIST(label), false)"),
        ("round_default", "ROUND(SUM(k))"), ("first", "FIRST(label)"),
        ("first_true", "FIRST(label,true)"), ("first_value", "FIRST_VALUE(label)"),
        ("count_if", "COUNT_IF(k IS NOT NULL)"), ("mean", "MEAN(k)"),
        ("distinct", "COUNT(DISTINCT label)"),
        ("count_null", "COUNT_IF(k IS NULL)"),
        ("count_cast_null", "COUNT_IF(CAST(k AS STRING) IS NULL)"),
        ("literal_case", "CONCAT(MAX(label), 'MiXeD')"),
        ("struct_cast", "SORT_ARRAY(COLLECT_LIST(TO_JSON(STRUCT(CAST(k AS STRING)))))"),
        ("udf_case", "CaSe_UdF(MAX(label))"),
    ]
    for label, aggregate in aggregates:
        queries.append((f"{kind}_projection_{label}", f"SELECT {aggregate} FROM {name}"))
        queries.append((f"{kind}_pivot_{label}", f"SELECT * FROM {name} PIVOT("
                        f"{aggregate},COUNT(*) AS n FOR (k) IN (CAST('-0.0' AS {kind})))"))
    for label, expression in [
        ("cast", "CAST(k AS STRING)"), ("colon_cast", "k::STRING"),
        ("nested_cast", "CAST(CAST(k AS STRING) AS DOUBLE)"),
        ("numeric_nested_cast", "CAST(CAST(k AS DOUBLE) AS STRING)"),
        ("try_cast", "TRY_CAST(k AS STRING)"),
        ("nested_try_cast", "LENGTH(TRY_CAST(k AS STRING))"),
        ("nested_cast_length", "LENGTH(CAST(k AS STRING))"),
        ("struct_fields", "TO_JSON(STRUCT(CAST(k AS STRING))) AS r"),
        ("nested_struct_fields", "UPPER(TO_JSON(STRUCT(CAST(k AS STRING)))) AS r"),
        ("struct_display", "TO_JSON(STRUCT(CAST(k AS STRING)))"),
        ("nested_struct_display", "UPPER(TO_JSON(STRUCT(CAST(k AS STRING))))"),
        ("udf_lower", "case_udf(label)"),
        ("udf_mixed", "CaSe_UdF(label)"),
        ("udf_upper", "CASE_UDF(label)"),
        ("alias_cast", "CAST(k AS STRING) AS explicit"),
        ("cast_scope", "LENGTH(CAST(k AS STRING)),CAST(k AS STRING),"
         "TO_JSON(STRUCT(CAST(k AS STRING))) AS s"),
    ]:
        queries.append((f"{kind}_projection_{label}", f"SELECT {expression} FROM {name}"))
    for label, aggregate in [
        ("single", "SORT_ARRAY(COLLECT_LIST(CAST(k AS STRING)))"),
        ("aliased", "SORT_ARRAY(COLLECT_LIST(CAST(k AS STRING))) AS Keys,MAX(label) AS Text"),
        ("original", "SORT_ARRAY(COLLECT_LIST(CAST(k AS STRING))), MAX(label) AS text"),
    ]:
        queries.append((f"{kind}_pivot_{label}", f"SELECT * FROM {name} PIVOT("
                        f"{aggregate} FOR (k) IN (CAST('-0.0' AS {kind}) AS Zero,NULL AS Nulls))"))
        queries.append((f"{kind}_multiple_{label}", f"SELECT * FROM {name} PIVOT("
                        f"{aggregate} FOR (k,label) IN ((CAST('-0.0' AS {kind}),'a'),"
                        "(NULL,NULL) AS Nulls))"))
    queries.append((f"{kind}_reference_generated_name",
                    f"SELECT `Zero_sort_array(collect_list(CAST(k AS STRING)), true)` "
                    f"AS keys FROM (SELECT * FROM {name} PIVOT("
                    "SORT_ARRAY(COLLECT_LIST(CAST(k AS STRING))),MAX(label) AS text "
                    f"FOR (k) IN (CAST('-0.0' AS {kind}) AS Zero)))"))
    queries.append((f"{kind}_column_case", f"SELECT * FROM (SELECT k,label AS Label FROM {name}) "
                    f"PIVOT(MAX(Label),COUNT(*) AS n FOR (k) IN (CAST('-0.0' AS {kind}) AS Zero))"))
    for label, grouping in [
        ("ordinary", "CAST(k AS STRING)"),
        ("rollup", "ROLLUP(CAST(k AS STRING))"),
        ("cube", "CUBE(CAST(k AS STRING))"),
        ("sets", "GROUPING SETS ((CAST(k AS STRING)),())"),
    ]:
        queries.append((f"{kind}_grouping_{label}",
                        f"SELECT CAST(k AS STRING),COUNT(*) AS n FROM {name} GROUP BY {grouping}"))

window_cast = ("SELECT window FROM (SELECT CAST(window AS STRING) "
               "FROM VALUES (TIMESTAMP'2024-01-01 00:00:00') t(ts) "
               "GROUP BY window(ts,'1 minute')) q")
queries.extend([
    ("deferred_projection_cast", f"SELECT COALESCE(window,'') AS r FROM ({window_cast}) result"),
    ("deferred_projection_nested_cast", f"SELECT LENGTH(({window_cast})) AS r"),
    ("deferred_projection_partial_retry", "SELECT LENGTH((SELECT label FROM "
     "(SELECT CAST(label AS STRING),CAST(window AS STRING) AS w "
     "FROM VALUES ('a',TIMESTAMP'2024-01-01 00:00:00') t(label,ts) "
     "GROUP BY label,window(ts,'1 minute')) q)) AS r"),
])

assert len({name for name, _ in queries}) == len(queries)


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, (types.FloatType, types.DoubleType)):
        return struct.pack("!f" if isinstance(data_type, types.FloatType) else "!d", value).hex()
    if isinstance(data_type, types.ArrayType):
        return [encode(item, data_type.elementType) for item in value]
    return str(value)


spark = (SparkSession.builder.master("local[1]").appName("dar-pivot-names")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    spark.udf.register("case_udf", lambda value: value, types.StringType())
    for source in inputs:
        kind = source["type"]
        encoding = "!f" if kind == "float" else "!d"
        rows = [(None if bits is None else struct.unpack(encoding, bytes.fromhex(bits))[0], label)
                for bits, label in source["rows"]]
        schema = types.StructType([
            types.StructField("k", types.FloatType() if kind == "float" else types.DoubleType(), True),
            types.StructField("label", types.StringType(), True),
        ])
        spark.createDataFrame(rows, schema).createOrReplaceTempView(source["name"])
    records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": inputs}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            frame = spark.sql(sql)
            rows = [[encode(value, field.dataType) for value, field in zip(row, frame.schema)]
                    for row in frame.collect()]
            rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
            records.append({"id": name, "ansi": ansi, "sql": sql,
                            "expected": {"schema": frame.schema.jsonValue(), "rows": rows}})
    Path(__file__).with_name("pivot-names-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1, "input_tables": len(inputs)}))
finally:
    spark.stop()
