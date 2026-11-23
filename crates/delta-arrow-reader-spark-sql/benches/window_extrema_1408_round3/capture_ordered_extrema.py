"""Capture Spark values for extrema and frame endpoints on the same order key."""

import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parents[1] / "tests" / "fixtures"
sys.path.insert(0, str(FIXTURES))
sys.path.insert(0, str(FIXTURES / "regression-windows-1512"))
from capture_date_range_747_spark import REVISION, VERSION, cleanup
from capture_spark import capture


INPUTS = [
    (0, 0, None, 40), (1, 0, 1, 11), (2, 0, 1, 12),
    (3, 0, 2, 23), (4, 0, 3, None), (5, 0, 3, 35),
    (6, 0, None, 60), (7, 1, None, 70), (8, 1, None, 80),
]
FRAMES = {
    "sliding_rows": "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    "sliding_range": "RANGE BETWEEN 2 PRECEDING AND CURRENT ROW",
    "growing": "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    "whole_partition": "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
    "possibly_empty": "ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING",
}


def cases():
    for frame, bounds in FRAMES.items():
        for direction in ("ASC", "DESC"):
            for null_order in ("FIRST", "LAST"):
                over = f"OVER (PARTITION BY g ORDER BY k {direction} NULLS {null_order} {bounds})"
                endpoints = {
                    "max_by": "max_by(v, k)", "min_by": "min_by(v, k)",
                    "first_id": "first_value(id)", "last_id": "last_value(id)",
                    "first_v": "first_value(v)", "last_v": "last_value(v)",
                }
                expressions = ", ".join(f"{fn} {over} AS {name}" for name, fn in endpoints.items())
                yield (f"{frame}_{direction.lower()}_nulls_{null_order.lower()}",
                       f"SELECT id, {expressions} FROM input_rows")


def main(output):
    if "PYSPARK_GATEWAY_PORT" in os.environ:
        raise RuntimeError("Spark capture requires its own JVM gateway")
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    assert sys.version_info[:2] == (3, 12)
    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert not output.exists() and not output.is_symlink()
    spark = None
    try:
        spark = (SparkSession.builder.master("local[1]").appName("dar-window-extrema-1408")
                 .config("spark.ui.enabled", "false")
                 .config("spark.driver.bindAddress", "127.0.0.1")
                 .config("spark.driver.host", "127.0.0.1")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "1")
                 .config("spark.sql.adaptive.enabled", "false").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        jvm = spark.sparkContext._jvm
        build = getattr(getattr(jvm.org.apache.spark, "package$"), "MODULE$")
        java = jvm.java.lang.System.getProperty("java.specification.version")
        assert spark.version == VERSION and build.SPARK_REVISION() == REVISION and java == "21"
        frame = spark.createDataFrame(INPUTS, "id BIGINT, g BIGINT, k BIGINT, v BIGINT")
        assert frame.rdd.getNumPartitions() == 1
        frame.createOrReplaceTempView("input_rows")
        records = [{"spark_version": VERSION, "spark_revision": REVISION, "java_major": java,
                    "timezone": "UTC", "input_fields": ["id", "g", "k", "v"], "inputs": INPUTS}]
        expected_ids = {row[0] for row in INPUTS}
        for ansi in (False, True):
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in cases():
                records.append(capture(spark, name, sql, ansi, expected_ids))
    finally:
        gateway = SparkContext._gateway
        if gateway is not None and getattr(gateway, "proc", None) is None:
            raise RuntimeError("Spark capture requires its own JVM gateway")
        shutdown = cleanup(spark, gateway)
        if sys.exc_info()[0] is None:
            assert shutdown.get("reaped") and not shutdown["errors"], "Spark JVM shutdown failed"

    assert len(records) == 41
    with output.open("x") as stream:
        for record in records:
            stream.write(json.dumps(record, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
