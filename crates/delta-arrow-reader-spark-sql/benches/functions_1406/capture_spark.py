#!/usr/bin/env python3
"""Capture original #1391 SQL against the typed reduced table in Spark 4.2.0."""

import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "tests/fixtures"))

from capture_date_range_747_spark import capture
from capture_spark_requests import capture_requested_queries


def capture_with_typed_table(spark, case, encode):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW bench AS
        SELECT id, grp, i, p, d, s, s2, from_json(arr, 'ARRAY<BIGINT>') AS arr,
            CAST(ts AS TIMESTAMP) AS ts, b, theta_b
        FROM bench_encoded
    """)
    return capture(spark, case, encode)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    input_path, manifest_path = HERE / "input.json", HERE / "manifest.json"
    source = json.loads(input_path.read_text())
    fields = source["schema"]["fields"]
    assert [field["name"] for field in fields] == ["id", "grp", "i", "p", "d", "s", "s2", "arr", "ts", "b", "theta_b"]
    assert fields[7]["type"] == {"type": "array", "elementType": "long", "containsNull": True}
    assert fields[8]["type"] == "timestamp"
    assert len(source["rows"]) == 96
    # The shared capture helper takes primitive source fields. The view restores the exact input types.
    fields[7]["type"] = fields[8]["type"] = "string"
    for row in source["rows"]:
        if row[7] is not None:
            row[7] = json.dumps([int(value) if value is not None else None for value in row[7]])
    manifest = json.loads(manifest_path.read_text())
    cases = [dict(corpus="reduced-bench", id=case["id"], ansi=ansi,
                  sql=case["spark_sql"], ordered=False)
             for case in manifest["functions"] if case["reference"] == "spark"
             for ansi in (False, True)]
    request = {"issue": 1406, "expected_record_count": len(cases), "corpora": [{
        "corpus": "reduced-bench",
        "source": {"path": str(input_path), "sha256": hashlib.sha256(input_path.read_bytes()).hexdigest()},
        "metadata": {"inputs": [{"name": "bench_encoded", **source}]}, "cases": cases,
    }]}
    capture_requested_queries(request, args.output, [Path(__file__), manifest_path, input_path],
                              [manifest_path, input_path], capture_query=capture_with_typed_table,
                              spark_settings={"spark.sql.adaptive.enabled": "false"})


if __name__ == "__main__":
    main()
