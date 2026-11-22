"""Capture #1512 regression windows and aggregate controls on Spark 4.2.0."""

import gzip
import hashlib
import json
import os
from pathlib import Path
import struct
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[2]
sys.path.insert(0, str(HERE.parent))

from capture_date_range_747_spark import REVISION, VERSION, cleanup

FUNCTIONS = [
    "regr_r2", "regr_avgy", "regr_intercept", "regr_slope", "regr_sxx",
    "regr_sxy", "regr_syy", "regr_avgx", "regr_count",
]
INPUT = WORKSPACE / "benches/functions_1406/input.json"


def sql_cases():
    reduced = json.loads((WORKSPACE / "benches/functions_1406/manifest.json").read_text())
    full = json.loads((WORKSPACE / "benches/functions/manifest.json").read_text())
    original = {case["id"]: case["spark_sql"] for case in full["functions"]
                if case["id"].startswith("window.window.regr_")}
    reduced_sql = {case["id"]: case["spark_sql"] for case in reduced["functions"]
                   if case["id"].startswith("window.window.regr_")}
    assert all(original[name] == sql for name, sql in reduced_sql.items())
    cases = [(f"original_{name}", original[f"window.window.{name}"]) for name in FUNCTIONS]

    sliding = "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW"
    frames = [
        ("two_preceding", "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW", "d", "CAST(p AS DOUBLE)"),
        ("preceding_following", "ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING",
         "d", "CAST(p AS DOUBLE)"),
        ("range", "RANGE BETWEEN 4096 PRECEDING AND CURRENT ROW", "d", "CAST(p AS DOUBLE)"),
        ("growing", "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
         "d", "CAST(p AS DOUBLE)"),
        ("whole_partition", "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
         "d", "CAST(p AS DOUBLE)"),
        ("nullable_arguments", sliding, "CASE WHEN id % 3 = 0 THEN NULL ELSE d END",
         "CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(p AS DOUBLE) END"),
        ("constant_independent", sliding, "d", "CAST(7 AS DOUBLE)"),
        ("constant_dependent", sliding, "CAST(0.7 AS DOUBLE)", "CAST(p AS DOUBLE)"),
        ("single_row", "ROWS BETWEEN CURRENT ROW AND CURRENT ROW", "d", "CAST(p AS DOUBLE)"),
    ]
    for name, frame, y, x in frames:
        over = f"OVER (PARTITION BY grp ORDER BY id {frame})"
        results = ", ".join(f"{fn}({y}, {x}) {over} AS {fn}" for fn in FUNCTIONS)
        cases.append((name, f"SELECT id, {results} FROM bench"))

    aggregates = ", ".join(f"{fn}(d, CAST(p AS DOUBLE)) AS {fn}" for fn in FUNCTIONS)
    cases.extend([
        ("plain_aggregates", f"SELECT {aggregates} FROM bench"),
        ("constant_dependent_aggregate",
         "SELECT regr_r2(CAST(0.7 AS DOUBLE), CAST(p AS DOUBLE)) AS regr_r2 FROM bench"),
        ("constant_independent_aggregate",
         "SELECT regr_r2(d, CAST(7 AS DOUBLE)) AS regr_r2 FROM bench"),
    ])
    assert len(cases) == len({name for name, _ in cases}) == 21
    return cases


def bench_input():
    source = json.loads(INPUT.read_text())
    fields = source["schema"]["fields"]
    positions = [next(i for i, field in enumerate(fields) if field["name"] == name)
                 for name in ("id", "grp", "p", "d")]
    projection = [fields[i] for i in positions]
    assert [field["type"] for field in projection] == ["long", "integer", "integer", "double"]
    schema = {"type": "struct", "fields": projection}
    raw = [[row[i] for i in positions] for row in source["rows"]]
    rows = [tuple(None if value is None else
                  struct.unpack(">d", bytes.fromhex(value))[0] if index == 3 else int(value)
                  for index, value in enumerate(row)) for row in raw]
    assert len(rows) == len({row[0] for row in rows}) == 96
    return schema, rows


def encode(value):
    if isinstance(value, float):
        return struct.pack(">d", value).hex()
    if isinstance(value, int):
        return str(value)
    assert value is None
    return None


def capture(spark, name, sql, ansi, expected_ids):
    query = spark.sql(sql)
    result = [tuple(row) for row in query.collect()]
    plan = query._jdf.queryExecution().executedPlan()
    fields = json.loads(plan.schema().json())["fields"]
    executed = [{"name": field["name"], "type": field["type"]} for field in fields]
    assert all(len(row) == len(executed) for row in result)
    if executed[0]["name"] == "id":
        assert {row[0] for row in result} == expected_ids
        assert len(result) == len(expected_ids)
        result.sort(key=lambda row: row[0])
    else:
        assert len(result) == 1
    return {"id": name, "ansi": ansi, "sql": sql, "executed": executed,
            "rows": [[encode(value) for value in row] for row in result]}


def main(output):
    if "PYSPARK_GATEWAY_PORT" in os.environ:
        raise RuntimeError("Spark capture requires its own JVM gateway")
    from pyspark import SparkContext
    from pyspark.sql import SparkSession, types

    assert sys.version_info[:2] == (3, 12)
    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert not output.exists() and not output.is_symlink()
    schema, rows = bench_input()
    spark = None
    try:
        spark = (SparkSession.builder.master("local[1]").appName("dar-regression-windows-1512")
                 .config("spark.ui.enabled", "false")
                 .config("spark.driver.bindAddress", "127.0.0.1")
                 .config("spark.driver.host", "127.0.0.1")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "1")
                 .config("spark.sql.adaptive.enabled", "false").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        jvm = spark.sparkContext._jvm
        build = getattr(getattr(jvm.org.apache.spark, "package$"), "MODULE$")
        java_major = jvm.java.lang.System.getProperty("java.specification.version")
        assert spark.version == VERSION and build.SPARK_REVISION() == REVISION and java_major == "21"
        assert spark.sparkContext.master == "local[1]"
        frame = spark.createDataFrame(rows, types.StructType.fromJson(schema))
        assert frame.schema.jsonValue() == schema and frame.rdd.getNumPartitions() == 1
        frame.createOrReplaceTempView("bench")
        observations = [{
            "spark_version": spark.version, "spark_revision": build.SPARK_REVISION(),
            "java_major": java_major, "master": spark.sparkContext.master,
            "settings": {name: spark.conf.get(name) for name in
                         ("spark.sql.session.timeZone", "spark.sql.shuffle.partitions",
                          "spark.sql.adaptive.enabled")},
            "input_sha256": hashlib.sha256(INPUT.read_bytes()).hexdigest(),
            "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        }]
        expected_ids = {row[0] for row in rows}
        for ansi in (False, True):
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            assert spark.conf.get("spark.sql.ansi.enabled") == str(ansi).lower()
            for name, sql in sql_cases():
                observations.append(capture(spark, name, sql, ansi, expected_ids))
    finally:
        gateway = SparkContext._gateway
        if gateway is not None and getattr(gateway, "proc", None) is None:
            raise RuntimeError("Spark capture requires its own JVM gateway")
        shutdown = cleanup(spark, gateway)
        if sys.exc_info()[0] is None:
            assert shutdown.get("reaped") and not shutdown["errors"], "Spark JVM shutdown failed"

    assert len(observations) == 43
    with output.open("xb") as target:
        with gzip.GzipFile(fileobj=target, filename="", mode="wb", mtime=0) as compressed:
            for record in observations:
                line = json.dumps(record, separators=(",", ":")) + "\n"
                compressed.write(line.encode())
    print("Captured 42 Spark executions across both ANSI modes", flush=True)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
