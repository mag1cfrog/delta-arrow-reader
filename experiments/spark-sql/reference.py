"""Observe pinned full Sail behavior before importing or trimming its Rust frontend."""

import argparse
import importlib.metadata
import json
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
    if isinstance(value, bytes):
        return {"binary_hex": value.hex()}
    if isinstance(value, dict):
        return {"map_entries": [[encode(k), encode(v)] for k, v in value.items()]}
    if isinstance(value, (list, tuple)):
        return [encode(v) for v in value]
    # Decimal is never coerced through float. These are observations, not a
    # general Spark-vs-Arrow comparison format; the Rust oracle checks come later.
    return str(value)


def observe(spark, case):
    try:
        frame = spark.sql(case["sql"])
        schema = frame.schema.jsonValue()
    except Exception as error:
        return {"status": "planning_error", "error": str(error)}
    try:
        return {"status": "ok", "schema": schema, "rows": [encode(row) for row in frame.collect()]}
    except Exception as error:
        return {"status": "execution_error", "schema": schema, "error": str(error)}


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
    parser.add_argument("--check", action="store_true", help="Validate corpus without Python packages")
    parser.add_argument("--out", type=Path, help="Write raw full-Sail observations outside source files")
    args = parser.parse_args()
    inputs, cases = load_corpus()
    print(f"{len(cases)} cases: {dict(Counter(case['family'] for case in cases))}")
    if args.check:
        return
    if args.out is None:
        parser.error("--out is required unless using --check")
    version = importlib.metadata.version(inputs["reference"]["distribution"])
    if version != inputs["reference"]["version"]:
        raise ValueError(f"Expected pysail {inputs['reference']['version']}, found {version}")

    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    server = SparkConnectServer()
    server.start()
    spark = None
    observations = []
    args.out.parent.mkdir(parents=True, exist_ok=True)
    try:
        _, port = server.listening_address
        spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
        for key, value in inputs["settings"].items():
            spark.conf.set(key, value)
        for name, sql in inputs["views"].items():
            spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {sql}").collect()
        for case in cases:
            if case.get("host_only"):
                actual = {"status": "requires_host_adapter"}
            else:
                settings = inputs["settings"] | case.get("settings", {})
                for key, value in settings.items():
                    spark.conf.set(key, value)
                actual = observe(spark, case)
            observations.append({"id": case["id"], "settings": inputs["settings"] | case.get("settings", {}), **actual})
            args.out.write_text(json.dumps({"reference": inputs["reference"], "inputs": inputs, "observations": observations}, indent=2) + "\n")
        print(f"Observed statuses: {dict(Counter(row['status'] for row in observations))}")
        verify_seeds(cases, observations)
        print("Observations recorded; this command does not assert Spark compatibility.")
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            server.stop()


if __name__ == "__main__":
    main()
