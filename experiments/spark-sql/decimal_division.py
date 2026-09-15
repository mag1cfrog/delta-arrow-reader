"""Capture Spark results and compare the isolated decimal-division candidate."""

import argparse
import json
import os
import re
import sys
from decimal import Decimal
from pathlib import Path

from reference import observe

ROOT = Path(__file__).resolve().parent


def load_cases(path=ROOT / "decimal-division.jsonl"):
    return [json.loads(line) for line in path.read_text().splitlines()]


def capture(out, cases_path=ROOT / "decimal-division.jsonl"):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master("local[2]").appName("decimal-division-probe")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.decimalOperations.allowPrecisionLoss", "true")
             .getOrCreate())
    results = []
    try:
        if spark.version != "4.2.0":
            raise ValueError(f"expected Spark 4.2.0, got {spark.version}")
        spark.sparkContext.setLogLevel("ERROR")
        cases = load_cases(cases_path)
        for case in cases:
            for ansi in ("true", "false"):
                spark.conf.set("spark.sql.ansi.enabled", ansi)
                actual = observe(spark, case)
                if actual["status"] == "ok":
                    actual["types"] = [f["type"] for f in actual["schema"]["fields"]]
                results.append({"id": f"{case['id']}_{ansi}", "actual": actual})
        out.write_text(json.dumps({"cases": cases, "spark_version": spark.version,
                                   "results": results}, indent=2) + "\n")
    finally:
        spark.stop()


def types(actual):
    names = {"Int8": "byte", "Int16": "short", "Int32": "integer", "Int64": "long",
             "Float32": "float", "Float64": "double", "Null": "void"}
    return [re.sub(r"Decimal128\((\d+), (-?\d+)\)", r"decimal(\1,\2)", t)
            if t not in names else names[t] for t in actual["types"]]


def rows(actual):
    # Decimal construction and equality are exact; normalize() would round to the
    # Python decimal context's default 28 digits. No float conversion or tolerance.
    return [[Decimal(v) if v is not None else None for v in row] for row in actual["rows"]]


def compare(reference, candidate, cases_path=ROOT / "decimal-division.jsonl"):
    reference_capture = json.loads(reference.read_text())
    candidate_capture = json.loads(candidate.read_text())
    cases = load_cases(cases_path)
    if reference_capture["cases"] != cases or candidate_capture["cases"] != cases:
        raise ValueError("capture SQL does not match the current cases")
    expected = reference_capture["results"]
    observed = candidate_capture["results"]
    ids = [c["id"] for c in expected]
    corpus_ids = [f"{case['id']}_{ansi}" for case in cases for ansi in ("true", "false")]
    if ids != corpus_ids or ids != [c["id"] for c in observed] or len(set(ids)) != len(ids):
        raise ValueError("missing, duplicate, reordered or unknown case IDs")
    results = []
    for left, right in zip(expected, observed, strict=True):
        a, b = left["actual"], right["actual"]
        differences = []
        if a["status"] != b["status"]:
            differences.append("status")
        elif a["status"] == "ok":
            if types(a) != types(b):
                differences.append("types")
            if rows(a) != rows(b):
                differences.append("rows")
        # Error stages are compared; error conditions and schema metadata are
        # deliberately outside this focused value/type probe's match count.
        results.append({"id": left["id"], "differences": differences})
    return {"agreement": sum(not r["differences"] for r in results),
            "total": len(results), "cases": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    spark = sub.add_parser("spark")
    spark.add_argument("out", type=Path)
    check = sub.add_parser("compare")
    check.add_argument("reference", type=Path)
    check.add_argument("candidate", type=Path)
    check.add_argument("--report", type=Path, required=True)
    for command in (spark, check):
        command.add_argument("--cases", type=Path, default=ROOT / "decimal-division.jsonl")
    args = parser.parse_args()
    if args.mode == "spark":
        capture(args.out, args.cases)
        return 0
    result = compare(args.reference, args.candidate, args.cases)
    args.report.write_text(json.dumps(result, indent=2) + "\n")
    print(f"{result['agreement']}/{result['total']} agree on values/types or error stage")
    return int(result["agreement"] != result["total"])


if __name__ == "__main__":
    sys.exit(main())
