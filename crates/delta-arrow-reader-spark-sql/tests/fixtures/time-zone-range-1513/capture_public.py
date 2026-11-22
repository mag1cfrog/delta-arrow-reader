"""Capture exact Spark outputs for DAR's real query panic reproductions."""

import hashlib
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
import observe_public
from observe_public import CASES

sys.path.insert(0, str(HERE.parent))
from capture_date_range_747_spark import REVISION, VERSION, cleanup

if "PYSPARK_GATEWAY_PORT" in os.environ:
    raise RuntimeError("Spark capture requires its own JVM gateway")

from pyspark import SparkContext
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

output = Path(sys.argv[1]).resolve()
assert sys.version_info[:2] == (3, 12)
assert SparkContext._gateway is None and SparkContext._active_spark_context is None
assert not output.exists() and not output.is_symlink()
spark = None
try:
    spark = (SparkSession.builder.master("local[1]").appName("dar-time-zone-public-1513")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
             .config("spark.sql.timeType.enabled", "true")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.adaptive.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("OFF")
    jvm = spark.sparkContext._jvm
    build = getattr(getattr(jvm.org.apache.spark, "package$"), "MODULE$")
    java_major = jvm.java.lang.System.getProperty("java.specification.version")
    assert spark.version == VERSION and build.SPARK_REVISION() == REVISION and java_major == "21"
    records = [{"spark_version": VERSION, "spark_revision": REVISION, "java_major": java_major,
                 "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "case_source_sha256": hashlib.sha256(Path(observe_public.__file__).read_bytes()).hexdigest()}]
    for name, micros, zone, sql in CASES:
        for ansi in (False, True):
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            days, tick = divmod(micros, 86_400_000_000)
            hour, tick = divmod(tick, 3_600_000_000)
            minute, tick = divmod(tick, 60_000_000)
            second, fraction = divmod(tick, 1_000_000)
            stamp = (
                f"SELECT timestamp_micros(CAST('{micros}' AS BIGINT)) AS ltz, "
                f"CAST(timestamp_micros(CAST('{micros}' AS BIGINT)) AS TIMESTAMP_NTZ) AS ntz, "
                f"date_from_unix_date({days}) AS d, "
                f"make_time({hour},{minute},{second}.{fraction:06d}) AS tod"
            )
            spark.sql(stamp).createOrReplaceTempView("probe")
            try:
                spark.conf.set("spark.sql.session.timeZone", zone)
                query = spark.sql(f"SELECT {sql} AS result FROM probe")
                plan = query._jdf.queryExecution().executedPlan()
                fields = json.loads(plan.schema().json())["fields"]
                rows = plan.executeCollect()
                assert len(rows) == 1 and len(fields) == 1
                result = {"status":"ok","type":fields[0]["type"],
                          "micros":None if rows[0].isNullAt(0) else rows[0].getLong(0)}
            except PySparkException as err:
                result = {"status":"error","condition":err.getCondition()}
            except Py4JJavaError as err:
                result = {"status":"error","class":err.java_exception.getClass().getName()}
            records.append({"id":name,"ansi":ansi,"input_micros":micros,"session":zone,
                            "sql":sql,"result":result})
            if not ansi:
                print(name, result, flush=True)
finally:
    gateway = SparkContext._gateway
    if gateway is not None and getattr(gateway, "proc", None) is None:
        raise RuntimeError("Spark capture requires its own JVM gateway")
    shutdown = cleanup(spark, gateway)
    if sys.exc_info()[0] is None:
        assert shutdown.get("reaped") and not shutdown["errors"], "Spark JVM shutdown failed"

with output.open("x") as out:
    for record in records:
        out.write(json.dumps(record,separators=(",",":"))+"\n")
