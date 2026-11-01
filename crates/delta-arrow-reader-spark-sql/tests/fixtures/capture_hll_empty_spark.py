"""Capture HLL empty-state, configuration and schema contracts with Spark 4.2.0."""
import json
import os
from pathlib import Path
import sys

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, IntegerType, StructField, StructType


INPUTS = "inputs"
INPUT_ROWS = [(0, None), (1, 5), (2, 5), (3, 6)]
FRAMES = {
    "prefix": "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING",
    "rows": "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
    "following": "ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING",
}


def queries():
    for config in [4, 12, 18]:
        aggregate = f"HLL_SKETCH_AGG(value,{config})"
        for label, predicate in [("all", "true"), ("empty", "false"), ("nulls", "value IS NULL")]:
            yield f"group_{config}_{label}", f"SELECT {aggregate} AS sketch,HLL_SKETCH_ESTIMATE({aggregate}) AS n FROM {INPUTS} WHERE {predicate}"
        yield f"filter_{config}", f"SELECT {aggregate} FILTER (WHERE false) AS sketch FROM {INPUTS}"
        for label, frame in FRAMES.items():
            yield f"window_{config}_{label}", f"SELECT id,sketch,HLL_SKETCH_ESTIMATE(sketch) AS n FROM (SELECT id,{aggregate} OVER (ORDER BY id {frame}) AS sketch FROM {INPUTS}) ORDER BY id"
        source = f"(SELECT id,{aggregate} AS sketch FROM {INPUTS} GROUP BY id)"
        for label, predicate in [("all", "true"), ("empty", "false")]:
            yield f"union_{config}_{label}", f"SELECT HLL_UNION_AGG(sketch) AS sketch,HLL_SKETCH_ESTIMATE(HLL_UNION_AGG(sketch)) AS n FROM {source} WHERE {predicate}"
        for label in ["prefix", "following"]:
            yield f"union_window_{config}_{label}", f"SELECT id,combined,HLL_SKETCH_ESTIMATE(combined) AS n FROM (SELECT id,HLL_UNION_AGG(sketch) OVER (ORDER BY id {FRAMES[label]}) AS combined FROM {source}) ORDER BY id"
    yield "constant_config", f"SELECT HLL_SKETCH_AGG(value,4+4) AS sketch FROM {INPUTS} WHERE false"
    yield "union_nulls", "SELECT HLL_UNION_AGG(sketch) AS sketch FROM (VALUES (CAST(NULL AS BINARY))) t(sketch)"
    yield "estimate_null", "SELECT HLL_SKETCH_ESTIMATE(CAST(NULL AS BINARY)) AS n"
    for label, config in [("low", "3"), ("high", "22"), ("null", "CAST(NULL AS INT)"), ("column", "id")]:
        yield f"invalid_config_{label}", f"SELECT HLL_SKETCH_AGG(value,{config}) AS sketch FROM {INPUTS}"
    yield "invalid_sketch", "SELECT HLL_SKETCH_ESTIMATE(X'') AS n"
    for config in [4, 18]:
        for union in [False, True]:
            function = "HLL_UNION_AGG(sketch)" if union else f"HLL_SKETCH_AGG(COALESCE(value,0),{config})"
            source = f"(SELECT id,HLL_SKETCH_AGG(COALESCE(value,0),{config}) AS sketch FROM inputs GROUP BY id)" if union else INPUTS
            window = "ORDER BY CASE id WHEN 0 THEN 0 WHEN 1 THEN 1 WHEN 2 THEN 4 ELSE 5 END RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING"
            yield f"range_gap_{config}_{'union' if union else 'sketch'}", f"SELECT id,sketch,HLL_SKETCH_ESTIMATE(sketch) AS n FROM (SELECT id,{function} OVER ({window}) AS sketch FROM {source}) ORDER BY id"
        source = f"(SELECT id,HLL_SKETCH_AGG(value,{config}) OVER (ORDER BY id {FRAMES['following']}) AS sketch FROM inputs)"
        yield f"scalar_union_{config}", f"SELECT id,HLL_UNION(sketch,sketch) AS sketch,HLL_SKETCH_ESTIMATE(HLL_UNION(sketch,sketch)) AS n FROM {source} ORDER BY id"
    yield "scalar_union_left_null", "SELECT HLL_UNION(CAST(NULL AS BINARY),X'') AS sketch"
    yield "scalar_union_right_null", "SELECT HLL_UNION(X'',CAST(NULL AS BINARY)) AS sketch"
    yield "scalar_union_flag_null", "SELECT HLL_UNION(X'',X'',CAST(NULL AS BOOLEAN)) AS sketch"
    yield "invalid_union", "SELECT HLL_UNION(X'',X'') AS sketch"
    yield "invalid_union_aggregate", "SELECT HLL_UNION_AGG(sketch) AS sketch FROM invalid_sketches"
    yield "invalid_union_config", "SELECT HLL_UNION(a,b) AS sketch FROM (SELECT HLL_SKETCH_AGG(value,4) AS a,HLL_SKETCH_AGG(value,18) AS b FROM inputs)"
    yield "invalid_config_not_null", "SELECT HLL_SKETCH_AGG(value,3) IS NOT NULL AS present FROM inputs"
    yield "invalid_config_coalesce", "SELECT COALESCE(HLL_SKETCH_AGG(value,3),X'') AS sketch FROM inputs"
    yield "invalid_config_window_not_null", "SELECT HLL_SKETCH_AGG(value,3) OVER (ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) IS NOT NULL AS present FROM inputs ORDER BY id"
    yield "literal_estimate_not_null", "SELECT HLL_SKETCH_ESTIMATE(X'') IS NOT NULL AS present"
    yield "invalid_column_estimate", "SELECT HLL_SKETCH_ESTIMATE(sketch) AS n FROM invalid_sketches"
    yield "column_estimate_not_null", "SELECT HLL_SKETCH_ESTIMATE(sketch) IS NOT NULL AS present FROM invalid_sketches"
    yield "column_estimate_coalesce", "SELECT COALESCE(HLL_SKETCH_ESTIMATE(sketch),CAST(0 AS BIGINT)) AS n FROM invalid_sketches"
    yield "column_union_not_null", "SELECT HLL_UNION(sketch,sketch) IS NOT NULL AS present FROM invalid_sketches"


def encode_value(spark, value):
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        sketch = spark._jvm.org.apache.datasketches.hll.HllSketch.heapify(bytearray(value))
        return {"lg_config_k": sketch.getLgConfigK(),
                "estimate": spark._jvm.java.lang.Math.round(sketch.getEstimate())}
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-hll-empty")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = StructType([StructField("id", IntegerType(), True),
                             StructField("value", IntegerType(), True)])
        spark.createDataFrame(INPUT_ROWS, schema).createOrReplaceTempView(INPUTS)
        spark.createDataFrame([(bytearray(),)], StructType([
            StructField("sketch", BinaryType(), False)
        ])).createOrReplaceTempView("invalid_sketches")
        records = [{"spark_version": spark.version, "timezone": "UTC",
                    "input_rows": INPUT_ROWS, "input_schema": schema.jsonValue()}]
        cases = [(name, sql, "spark") for name, sql in queries()]
        # Native FILTER execution is checked before the separate Spark admission repair.
        for config in [4, 18]:
            for label in ["prefix", "following"]:
                for union in [False, True]:
                    function = "HLL_UNION_AGG(sketch)" if union else f"HLL_SKETCH_AGG(value,{config})"
                    source = f"(SELECT id,HLL_SKETCH_AGG(value,{config}) AS sketch FROM inputs GROUP BY id)" if union else INPUTS
                    sql = f"SELECT id,sketch,HLL_SKETCH_ESTIMATE(sketch) AS n FROM (SELECT id,{function} FILTER (WHERE id=2) OVER (ORDER BY id {FRAMES[label]}) AS sketch FROM {source}) ORDER BY id"
                    cases.append((f"native_filter_{config}_{label}_{'union' if union else 'sketch'}", sql, "datafusion"))
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql, frontend in cases:
                record = {"id": name, "ansi": ansi, "sql": sql, "frontend": frontend}
                try:
                    frame = spark.sql(sql)
                    rows = frame.collect()
                    record["expected"] = {"status": "ok", "schema": frame.schema.jsonValue(),
                                          "rows": [[encode_value(spark, value) for value in row] for row in rows]}
                    record["binary_hex_rows"] = [[value.hex() if isinstance(value, (bytes, bytearray)) else None for value in row] for row in rows]
                except Exception as error:
                    condition = getattr(error, "getCondition", lambda: None)()
                    cause = getattr(error, "java_exception", None)
                    while condition is None and cause is not None:
                        try:
                            condition = cause.getCondition()
                        except Py4JError:
                            pass
                        cause = cause.getCause()
                    assert condition is not None, (name, str(error))
                    record["expected"] = {"status": "error", "condition": condition}
                records.append(record)
        assert len(records) == 139
        Path(__file__).with_name("hll-empty-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records) - 1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
