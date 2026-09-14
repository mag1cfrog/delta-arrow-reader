"""Run the fixed SQL corpus against full Sail or independent Apache Spark."""

import argparse
import importlib.metadata
import json
import hashlib
import os
import platform
import time
import sys
from datetime import date
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def load_corpus():
    inputs = json.loads((ROOT / "inputs.json").read_text())
    cases = [json.loads(line) for line in (ROOT / "queries.jsonl").read_text().splitlines()]
    ids = set()
    for case in cases:
        if not isinstance(case.get("id"), str) or case["id"] in ids:
            raise ValueError(f"Missing or duplicate case id: {case.get('id')}")
        ids.add(case["id"])
        if not isinstance(case.get("sql"), str) or not isinstance(case.get("family"), str):
            raise ValueError(f"Missing SQL/family: {case['id']}")
        if case.get("comparison", "multiset") not in {"multiset", "ordered", "partition_id", "partition_sort", "monotonic_id"}:
            raise ValueError(f"Unknown comparison: {case['id']}")
        if set(case.get("settings", {})) - set(inputs["settings"]):
            raise ValueError(f"Unknown setting override: {case['id']}")
        if case["family"] == "excluded" and not case.get("host_only"):
            raise ValueError(f"Excluded operation must not execute in reference: {case['id']}")
    return inputs, cases


def encode(value):
    """Keep nested values and duplicate columns; schema records their logical types."""
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (bytes, bytearray)):
        return {"binary_hex": value.hex()}
    if isinstance(value, dict):
        entries = [[encode(k), encode(v)] for k, v in value.items()]
        return {"map_entries": sorted(entries, key=lambda entry: json.dumps(entry[0], sort_keys=True))}
    if isinstance(value, (list, tuple)):
        return [encode(v) for v in value]
    # Decimal is never coerced through float. These are observations, not a
    # general Spark-vs-Arrow comparison format; the Rust oracle checks come later.
    return str(value)


def corpus_hash(inputs, cases):
    payload = json.dumps({"inputs": inputs, "cases": cases}, sort_keys=True).encode()
    return hashlib.sha256(payload).hexdigest()


def error_result(status, error):
    get_condition = getattr(error, "getCondition", None)
    condition = get_condition() if get_condition else None
    if condition is None and getattr(error, "java_exception", None) is not None:
        # Some execution errors cross Py4J as wrapped SparkThrowables.
        from pyspark.errors.exceptions.captured import CapturedException

        condition = CapturedException(origin=error.java_exception).getCondition()
    return {"status": status, "error": str(error),
            "condition": condition}


def observe(spark, case):
    try:
        frame = spark.sql(case["sql"])
        schema = frame.schema.jsonValue()
    except Exception as error:
        return error_result("planning_error", error)
    try:
        return {"status": "ok", "schema": schema, "rows": [encode(row) for row in frame.collect()]}
    except Exception as error:
        return {**error_result("execution_error", error), "schema": schema}


def verify_seeds(cases, observations):
    checked = 0
    for case, actual in zip(cases, observations, strict=True):
        if "seed_rows" not in case or case.get("host_only") or case.get("known_boundary"):
            continue
        if actual["status"] != "ok":
            raise AssertionError(f"Seed query now fails: {case['id']}")
        expected, observed = case["seed_rows"], actual["rows"]
        if case.get("comparison", "multiset") == "multiset":
            expected = Counter(json.dumps(row, sort_keys=True) for row in expected)
            observed = Counter(json.dumps(row, sort_keys=True) for row in observed)
        if observed != expected:
            raise AssertionError(f"Seed rows changed: {case['id']}")
        if "seed_fields" in case:
            names = [field["name"] for field in actual["schema"]["fields"]]
            if names != case["seed_fields"]:
                raise AssertionError(f"Seed field names changed: {case['id']}")
        checked += 1
    print(f"Verified {checked} existing seeds for rows and specified field names")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=["sail", "spark"], default="sail")
    parser.add_argument("--check", action="store_true", help="Validate corpus without Python packages")
    parser.add_argument("--out", type=Path, help="Write raw full-Sail observations outside source files")
    args = parser.parse_args()
    inputs, cases = load_corpus()
    print(f"{len(cases)} cases: {dict(Counter(case['family'] for case in cases))}")
    if args.check:
        return
    if args.out is None:
        parser.error("--out is required unless using --check")
    reference = inputs["reference" if args.engine == "sail" else "spark_reference"]
    version = importlib.metadata.version(reference["distribution"])
    if version != reference["version"]:
        raise ValueError(f"Expected {reference}, found {version}")
    # Classic PySpark decodes timestamp instants using the host timezone.
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

    server = None
    spark = None
    observations = []
    args.out.parent.mkdir(parents=True, exist_ok=True)
    try:
        if args.engine == "sail":
            from pysail.spark import SparkConnectServer

            server = SparkConnectServer()
            server.start()
            _, port = server.listening_address
            spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
        else:
            os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
            os.environ["PYSPARK_PYTHON"] = sys.executable
            spark = (SparkSession.builder.master("local[2]")
                     .appName("delta-reader-spark-oracle")
                     .config("spark.ui.enabled", "false")
                     .config("spark.driver.bindAddress", "127.0.0.1")
                     .config("spark.driver.host", "127.0.0.1")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.adaptive.enabled", "false")
                     .getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
        for key, value in inputs["settings"].items():
            spark.conf.set(key, value)
        for name, table in inputs["tables"].items():
            schema = StructType.fromJson(table["schema"])
            rows = [[date.fromisoformat(value) if field.dataType.typeName() == "date" and value is not None else value
                     for field, value in zip(schema, row, strict=True)] for row in table["rows"]]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
        input_schemas = {name: spark.table(name).schema.jsonValue() for name in inputs["tables"]}
        if input_schemas != {name: table["schema"] for name, table in inputs["tables"].items()}:
            raise AssertionError("engine changed an explicit input schema")
        input_rows = {name: [encode(row) for row in spark.table(name).collect()] for name in inputs["tables"]}
        environment = {"engine": args.engine, "python": platform.python_version(),
                       "client_timezone": "UTC", "spark_version": spark.version,
                       "corpus_sha256": corpus_hash(inputs, cases), "input_schemas": input_schemas, "input_rows": input_rows}
        if args.engine == "spark":
            environment["java_version"] = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
            environment["py4j_version"] = importlib.metadata.version("py4j")
        for case in cases:
            if case.get("host_only"):
                actual = {"status": "requires_host_adapter"}
            else:
                settings = inputs["settings"] | case.get("settings", {})
                for key, value in settings.items():
                    spark.conf.set(key, value)
                actual = observe(spark, case)
            observations.append({"id": case["id"], "settings": inputs["settings"] | case.get("settings", {}), **actual})
            args.out.write_text(json.dumps({"reference": reference, **environment, "inputs": inputs, "observations": observations}, indent=2) + "\n")
        print(f"Observed statuses: {dict(Counter(row['status'] for row in observations))}")
        verify_seeds(cases, observations)
        print("Observations recorded; this command does not assert Spark compatibility.")
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            if server is not None:
                server.stop()


if __name__ == "__main__":
    main()
