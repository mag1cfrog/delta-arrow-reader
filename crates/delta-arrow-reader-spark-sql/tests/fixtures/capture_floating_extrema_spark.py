"""Capture Spark 4.2.0 extrema with JVM raw bits, including input round trips."""
import argparse
import json
import math
import os
from pathlib import Path
import struct
import sys

from pyspark.sql import SparkSession, functions as functions, types


def decode(bits, kind):
    return None if bits is None else struct.unpack("!f" if kind == "float" else "!d",
                                                 bytes.fromhex(bits))[0]


def capture(spark, sql):
    frame = spark.sql(sql)
    schema = frame.schema
    projections = []
    for field in frame.schema:
        name = "`" + field.name.replace("`", "``") + "`"
        if isinstance(field.dataType, (types.FloatType, types.DoubleType)):
            owner, method = (("Float", "floatToRawIntBits") if isinstance(field.dataType, types.FloatType)
                             else ("Double", "doubleToRawLongBits"))
            expression = f"CASE WHEN {name} IS NULL THEN NULL ELSE java_method('java.lang.{owner}','{method}',{name}) END"
            projections.append(functions.expr(expression).alias(field.name))
        else:
            projections.append(functions.col(name).cast("string").alias(field.name))
    rows = []
    encoded = frame.select(*projections)
    for row in encoded.collect():
        values = []
        for value, field in zip(row, schema):
            if value is not None and isinstance(field.dataType, (types.FloatType, types.DoubleType)):
                width = 32 if isinstance(field.dataType, types.FloatType) else 64
                value = f"{int(value) & ((1 << width) - 1):0{width // 4}x}"
            values.append(value)
        rows.append(values)
    rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
    return {"schema": schema.jsonValue(), "rows": rows}, encoded


def execution_metrics(frame):
    result = []

    def visit(node):
        if node.nodeName() == "AdaptiveSparkPlan":
            visit(node.executedPlan())
            return
        if node.nodeName() in ["ResultQueryStage", "ShuffleQueryStage", "BroadcastQueryStage",
                               "TableCacheQueryStage"]:
            visit(node.plan())
            return
        metrics = {}
        iterator = node.metrics().iterator()
        while iterator.hasNext():
            pair = iterator.next()
            metrics[pair._1()] = pair._2().value()
        result.append({"node": node.nodeName(), "metrics": metrics})
        iterator = node.children().iterator()
        while iterator.hasNext():
            visit(iterator.next())

    visit(frame._jdf.queryExecution().executedPlan())
    return result


def inputs(probe):
    for kind, value_type, values in [
        ("float", types.FloatType(), ["80000000", "00000000", "7fc01234", "ffc04321",
                                     "ff800000", "7f800000", "c0000000", "3fc00000"]),
        ("double", types.DoubleType(), ["8000000000000000", "0000000000000000",
                                       "7ff8000000001234", "fff8000000004321",
                                       "fff0000000000000", "7ff0000000000000",
                                       "c000000000000000", "3ff8000000000000"]),
    ]:
        negative_zero, positive_zero, nan_a, nan_b, negative_inf, positive_inf, low, high = values
        cases = {
            "zeros_negative": [negative_zero, positive_zero, negative_zero],
            "nans_positive": [nan_a, nan_b, nan_a],
            "nans_negative": [nan_b, nan_a, nan_b],
            "mixed": [None, nan_b, positive_inf, negative_zero, positive_zero,
                      low, negative_inf, nan_a, high, high, None],
        }
        if not probe:
            cases.update({"zeros_positive": [positive_zero, negative_zero, positive_zero],
                          "nulls": [None, None], "empty": [],
                          "positive_inf": [positive_inf], "negative_inf": [negative_inf],
                          "finite": [None, low, high, high, negative_zero, positive_zero],
                          "interleaved": [negative_zero, None, positive_zero, nan_b, None,
                                          nan_a, negative_zero, positive_zero, low, high]})
        for label, bits in cases.items():
            yield kind + "_" + label, kind, value_type, bits


def allowed_rows(expected, input_bits, kind, grouped):
    allowed = []
    fields = expected["schema"]["fields"]
    for row in expected["rows"]:
        candidates = []
        for index, (value, field) in enumerate(zip(row, fields)):
            if value is None or field["type"] not in ["float", "double"]:
                candidates.append([value])
                continue
            target = decode(value, kind)
            selected = []
            for ordinal, bits in enumerate(input_bits):
                if bits is None or (grouped and ordinal % 2 != int(row[0])):
                    continue
                if field["name"].startswith("kept_") and ordinal % 3 == 1:
                    continue
                item = decode(bits, kind)
                if item == target or (math.isnan(item) and math.isnan(target)):
                    selected.append(bits)
            assert selected, (row, field)
            candidates.append(sorted(set(selected)))
        allowed.append(candidates)
    return allowed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--output", type=Path,
                        default=Path(__file__).with_name("floating-extrema-spark.jsonl"))
    args = parser.parse_args()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-floating-extrema")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        metadata = {"spark_version": spark.version, "timezone": "UTC", "inputs": [],
                    "raw_bits": "JVM floatToRawIntBits/doubleToRawLongBits", "probe": args.probe}
        definitions = []
        for table, kind, value_type, bits in inputs(args.probe):
            schema = types.StructType([
                types.StructField("k", value_type, True, {"purpose": "extrema-payload"}),
                types.StructField("id", types.IntegerType(), False),
            ])
            spark.createDataFrame([(decode(value, kind), i) for i, value in enumerate(bits)], schema).coalesce(1).createOrReplaceTempView(table)
            recorded = {"name": table, "schema": schema.jsonValue(),
                        "rows": [[value, str(i)] for i, value in enumerate(bits)]}
            roundtrip, _ = capture(spark, f"SELECT k,id FROM {table}")
            assert roundtrip["rows"] == sorted(recorded["rows"], key=lambda row: json.dumps(row, separators=(",", ":"))), (table, roundtrip)
            recorded["jvm_roundtrip"] = roundtrip
            metadata["inputs"].append(recorded)
            extrema = "MIN(k) AS low,MAX(k) AS high,MIN(DISTINCT k) AS unique_low,MAX(DISTINCT k) AS unique_high,MIN(k) FILTER(WHERE id%3<>1) AS kept_low,MAX(k) FILTER(WHERE id%3<>1) AS kept_high"
            definitions.append((table + "_global", table, kind, bits, False,
                                f"SELECT {extrema} FROM {table}"))
            definitions.append((table + "_group", table, kind, bits, True,
                                f"SELECT id%2 AS g,{extrema} FROM {table} GROUP BY id%2"))
            frame = "ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"
            following = "ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING"
            definitions.append((table + "_window", table, kind, bits, None,
                                f"SELECT id,k,MIN(k) OVER ({frame}) AS low,MAX(k) OVER ({frame}) AS high,MIN(k) FILTER(WHERE id%3<>1) OVER ({frame}) AS kept_low,MAX(k) FILTER(WHERE id%3<>1) OVER ({frame}) AS kept_high,MIN(k) OVER ({following}) AS next_low,MAX(k) OVER ({following}) AS next_high FROM {table}"))
            if not args.probe and table.endswith(("mixed", "interleaved", "zeros_negative", "nans_positive")):
                for label, window, named in [
                    ("prefix", "ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", ""),
                    ("all", "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", ""),
                    ("range", "ORDER BY id RANGE BETWEEN 2 PRECEDING AND CURRENT ROW", ""),
                    ("named_desc", "w", "WINDOW w AS (PARTITION BY id%2 ORDER BY id DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"),
                ]:
                    over = window if label == "named_desc" else f"({window})"
                    definitions.append((table + "_" + label, table, kind, bits, None,
                                        f"SELECT id,k,MIN(k) OVER {over} AS low,MAX(k) OVER {over} AS high FROM {table} {named}"))
                partitioned = table + "_partitioned"
                spark.table(table).repartition(3).createOrReplaceTempView(partitioned)
                partitioned_roundtrip, _ = capture(spark, f"SELECT k,id FROM {partitioned}")
                assert partitioned_roundtrip["rows"] == roundtrip["rows"], partitioned
                metadata["inputs"].append({**recorded, "name": partitioned,
                                           "jvm_roundtrip": partitioned_roundtrip,
                                           "spark_partitions": 3})
                definitions.append((partitioned + "_global", partitioned, kind, bits, False,
                                    f"SELECT {extrema} FROM {partitioned}"))
                definitions.append((partitioned + "_group", partitioned, kind, bits, True,
                                    f"SELECT id%2 AS g,{extrema} FROM {partitioned} GROUP BY id%2"))
            if not args.probe and table.endswith("mixed"):
                definitions.append((table + "_default_names", table, kind, bits, False,
                                    f"SELECT MIN(k),MAX(DISTINCT k) FROM {table}"))
        records = [metadata]
        for ansi in ([True] if args.probe else [True, False]):
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, table, kind, bits, grouped, sql in definitions:
                expected, frame = capture(spark, sql)
                record = {"id": name, "table": table, "ansi": ansi, "sql": sql,
                          "expected": expected,
                          "plan": frame._jdf.queryExecution().executedPlan().toString()}
                if grouped is not None:
                    record["allowed_rows"] = allowed_rows(expected, bits, kind, grouped)
                records.append(record)
            if not args.probe:
                for setting, value in [("spark.sql.codegen.factoryMode", "NO_CODEGEN"),
                                       ("spark.sql.window.segmentTree.enabled", "true")]:
                    previous = spark.conf.get(setting)
                    spark.conf.set(setting, value)
                    threshold = spark.conf.get("spark.sql.window.segmentTree.minPartitionRows")
                    if "segmentTree" in setting:
                        spark.conf.set("spark.sql.window.segmentTree.minPartitionRows", "1")
                    try:
                        for name, table, kind, bits, grouped, sql in definitions:
                            if name.endswith("nans_positive_range"):
                                expected, frame = capture(spark, sql)
                                metrics = execution_metrics(frame)
                                if "segmentTree" in setting:
                                    assert sum(node["metrics"].get("numSegmentTreeFrames", 0)
                                               for node in metrics) > 0, metrics
                                records.append({"id": name + "_" + value, "table": table,
                                                "ansi": ansi, "sql": sql, "expected": expected,
                                                "spark_setting": [setting, value],
                                                "metrics": metrics,
                                                "plan": frame._jdf.queryExecution().executedPlan().toString()})
                    finally:
                        spark.conf.set(setting, previous)
                        spark.conf.set("spark.sql.window.segmentTree.minPartitionRows", threshold)
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records) - 1
        args.output.write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records))
        print(json.dumps({"observations": len(records) - 1, "inputs": len(metadata["inputs"]),
                          "output": str(args.output), "raw_roundtrips": "passed"}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
