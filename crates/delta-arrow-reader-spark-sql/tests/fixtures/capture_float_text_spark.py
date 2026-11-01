"""Capture Spark FLOAT/DOUBLE text from exact bits, including serialization."""

import json
import math
import os
from pathlib import Path
import random
import struct
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, FloatType, IntegerType, StructField, StructType


def bit_patterns(width):
    code = "f" if width == 32 else "d"
    random_bits = random.Random(651 + width)
    patterns = {random_bits.getrandbits(width) for _ in range(128)}
    sign = 1 << (width - 1)
    patterns.update([0, 1, 2, 3, sign, sign + 1])
    if width == 32:
        patterns.update([0x007FFFFF, 0x00800000, 0x7F7FFFFF, 0x48A1D074, 0x4999E602])
        exponents = [-45, -44, -38, -8, -4, -3, -2, 0, 6, 7, 8, 10, 20, 38]
    else:
        patterns.update([0x000FFFFFFFFFFFFF, 0x0010000000000000, 0x7FEFFFFFFFFFFFFF,
                         0x42E872355E743ED4, 0x430C6BF526340002, 0x431740188E7DEC85])
        exponents = [-323, -308, -8, -4, -3, -2, 0, 6, 7, 8, 10, 20, 23, 308]
    for exponent in exponents:
        bits = int.from_bytes(struct.pack(">" + code, 10.0 ** exponent), "big")
        for delta in [-1, 0, 1]:
            patterns.add(bits + delta)
            patterns.add((bits + delta) | sign)
    for value in [math.inf, -math.inf, math.nan]:
        patterns.add(int.from_bytes(struct.pack(">" + code, value), "big"))
    return sorted(patterns)


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-float-text")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.codegen.wholeStage", "false")
             .config("spark.sql.codegen.factoryMode", "NO_CODEGEN")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        records = []
        pivot_cases = []
        for sql_type, width, code, spark_type in [("FLOAT", 32, "f", FloatType()), ("DOUBLE", 64, "d", DoubleType())]:
            bits = [None, *bit_patterns(width)]
            values = [None if value is None else struct.unpack(">" + code, value.to_bytes(width // 8, "big"))[0] for value in bits]
            frame = spark.createDataFrame(list(enumerate(values)), StructType([
                StructField("k", IntegerType(), False), StructField("value", spark_type),
            ]))
            pivot_inputs = [1e10, 1e-8, -0.0, math.inf]
            pivot_bits = [struct.pack(">" + code, value).hex() for value in pivot_inputs]
            spark.createDataFrame([(value,) for value in pivot_inputs], StructType([
                StructField("value", spark_type),
            ])).createOrReplaceTempView("float_pivot_inputs")
            results = []
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                results.append([row.asDict() for row in frame.selectExpr(
                    "k", "CAST(value AS STRING) AS text",
                    "TO_CSV(NAMED_STRUCT('v',value)) AS csv",
                    "TO_XML(NAMED_STRUCT('v',value)) AS xml",
                    "CAST(ARRAY(value) AS STRING) AS nested",
                ).orderBy("k").collect()])
                pivot_literals = ",".join(f"CAST('{value}' AS {sql_type})" for value in ["1e10", "1e-8", "-0.0", "Infinity"])
                sql = f"SELECT * FROM float_pivot_inputs PIVOT(COUNT(*) FOR (value) IN ({pivot_literals}))"
                pivot = spark.sql(sql)
                pivot_cases.append({"sql_type": sql_type, "ansi": ansi, "sql": sql,
                                    "bits": pivot_bits, "schema": pivot.schema.jsonValue(), "rows": [list(row) for row in pivot.collect()]})
            assert results[0] == results[1], "ANSI mode changed floating text"
            for raw, output in zip(bits, results[0]):
                output.pop("k")
                records.append({"sql_type": sql_type, "bits": None if raw is None else f"{raw:0{width//4}x}", "expected": output})
        metadata = {"spark_version": spark.version, "java_version": spark._jvm.java.lang.System.getProperty("java.version"), "ansi_modes": [True, False], "codegen_factory_mode": "NO_CODEGEN", "whole_stage_codegen": False, "pivot_cases": pivot_cases}
        Path(__file__).with_name("float-text-spark.jsonl").write_text(
            "\n".join(json.dumps(row, separators=(",", ":")) for row in [metadata, *records]) + "\n")
        print(json.dumps({"observations": len(records), "metadata": metadata}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
