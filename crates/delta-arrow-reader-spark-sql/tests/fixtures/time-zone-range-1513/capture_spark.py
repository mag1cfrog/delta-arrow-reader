"""Capture the Spark 4.2.0 outcome for each recorded Chrono panic."""

from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from capture_date_range_747_spark import REVISION, VERSION, cleanup

INVENTORY = HERE / "dar-calendar-panics.jsonl"
SQL_BY_ROUTE = {
    "classic": "convert_timezone(source, target, cast(timestamp_micros(micros) AS TIMESTAMP_NTZ))",
    "from_utc": "from_utc_timestamp(timestamp_micros(micros), target)",
    "non_classic": "to_utc_timestamp(from_utc_timestamp(timestamp_micros(micros), target), source)",
}


def input_cases():
    limits, *inventory = map(json.loads, INVENTORY.read_text().splitlines())
    assert limits["kind"] == "limits"
    keys = sorted({(case["mode"], case["from"], case["to"], case["micros"]) for case in inventory})
    assert len(keys) > 2200
    return [
        (case_id, "from_utc" if mode == "non_classic" and source in {"UTC", "Z"} else mode,
         source, target, micros)
        for case_id, (mode, source, target, micros) in enumerate(keys)
    ]


def capture(spark, cases):
    from pyspark.errors import PySparkException
    from py4j.protocol import Py4JJavaError

    groups = defaultdict(list)
    dates = spark.sparkContext._jvm.org.apache.spark.sql.catalyst.util.DateTimeUtils
    availability = {}
    for case_id, route, source, target, micros in cases:
        for zone in (source, target):
            if zone not in availability:
                try:
                    dates.getZoneId(zone)
                except (PySparkException, Py4JJavaError):
                    availability[zone] = False
                else:
                    availability[zone] = True
        invalid = None if availability[source] and availability[target] else (source, target)
        groups[(route, invalid)].append((case_id, source, target, micros))

    results = {}
    for (route, invalid), rows in groups.items():
        spark.createDataFrame(rows, "id long, source string, target string, micros long").createOrReplaceTempView("zone_cases")
        try:
            query = spark.sql(f"SELECT id, {SQL_BY_ROUTE[route]} AS result FROM zone_cases")
            plan = query._jdf.queryExecution().executedPlan()
            fields = json.loads(plan.schema().json())["fields"]
            result_type = fields[1]["type"]
            expected_type = "timestamp_ntz" if route == "classic" else "timestamp"
            assert result_type == expected_type
            observations = plan.executeCollect()
        except (PySparkException, Py4JJavaError):
            if invalid is None:
                raise
            for case_id, *_ in rows:
                results[case_id] = {"status": "error"}
        else:
            assert len(observations) == len(rows) and invalid is None
            for row in observations:
                case_id = row.getLong(0)
                assert case_id not in results
                results[case_id] = {"status": "ok", "type": result_type,
                                    "micros": None if row.isNullAt(1) else row.getLong(1)}
    assert set(results) == {case[0] for case in cases}
    return results


def main(output):
    if "PYSPARK_GATEWAY_PORT" in os.environ:
        raise RuntimeError("Spark capture requires its own JVM gateway")
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    assert sys.version_info[:2] == (3, 12)
    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert not output.exists() and not output.is_symlink()
    cases = input_cases()
    spark = None
    try:
        spark = (SparkSession.builder.master("local[1]").appName("dar-time-zone-range-1513")
                 .config("spark.ui.enabled", "false")
                 .config("spark.driver.bindAddress", "127.0.0.1")
                 .config("spark.driver.host", "127.0.0.1")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
                 .config("spark.sql.shuffle.partitions", "1")
                 .config("spark.sql.adaptive.enabled", "false").getOrCreate())
        spark.sparkContext.setLogLevel("OFF")
        jvm = spark.sparkContext._jvm
        build = getattr(getattr(jvm.org.apache.spark, "package$"), "MODULE$")
        java_major = jvm.java.lang.System.getProperty("java.specification.version")
        assert spark.version == VERSION and build.SPARK_REVISION() == REVISION and java_major == "21"
        records = [{
            "spark_version": VERSION, "spark_revision": REVISION, "java_major": java_major,
            "time_zone": "UTC", "timestamp_type": "TIMESTAMP_LTZ", "sql": SQL_BY_ROUTE,
            "inventory_sha256": hashlib.sha256(INVENTORY.read_bytes()).hexdigest(),
            "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        }]
        for ansi in (False, True):
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            assert spark.conf.get("spark.sql.ansi.enabled") == str(ansi).lower()
            results = capture(spark, cases)
            for case_id, route, source, target, micros in cases:
                records.append({"id": case_id, "ansi": ansi, "route": route,
                                "from": source, "to": target, "input_micros": micros,
                                "result": results[case_id]})
    finally:
        gateway = SparkContext._gateway
        if gateway is not None and getattr(gateway, "proc", None) is None:
            raise RuntimeError("Spark capture requires its own JVM gateway")
        shutdown = cleanup(spark, gateway)
        if sys.exc_info()[0] is None:
            assert shutdown.get("reaped") and not shutdown["errors"], "Spark JVM shutdown failed"

    with output.open("x") as target:
        for record in records:
            target.write(json.dumps(record, separators=(",", ":")) + "\n")
    print(f"Captured {len(records) - 1} time zone outcomes across both ANSI modes", flush=True)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
