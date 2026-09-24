"""Bounded FLOAT BROUND type-contract checks for issue 205."""

import argparse
from collections import Counter
from decimal import Decimal
import json
import math
from pathlib import Path
import struct

from decimal_division import capture, load_cases, types

ROOT = Path(__file__).resolve().parent
CASES = ROOT / "float-bround.jsonl"


def cases():
    result = []

    def add(name, sql, category="target"):
        result.append({"id": name, "sql": sql, "batch_size": 2, "category": category})

    for name, scale, values in [
        ("default", None, ["2.5", "3.5", "-2.5", "-3.5", "0", "2.4", "2.6"]),
        ("zero", "0", ["2.5", "3.5", "-2.5", "-3.5", "0", "-2.4", "-2.6"]),
        ("positive", "1", ["1.25", "1.75", "-1.25", "-1.75", "1.24", "1.26"]),
        ("negative", "-1", ["25", "35", "-25", "-35", "24", "26"]),
    ]:
        suffix = "" if scale is None else f", {scale}"
        for i, value in enumerate(values + ["NULL"]):
            add(f"{name}_literal_{i}", f"SELECT BROUND(CAST({value} AS FLOAT){suffix}) AS r")
        source = ",".join(f"(CAST({v} AS FLOAT))" for v in values + ["NULL"])
        add(f"{name}_column", f"SELECT BROUND(a{suffix}) AS r FROM VALUES {source} t(a)")
        add(f"{name}_empty", f"SELECT BROUND(a{suffix}) AS r FROM VALUES {source} t(a) WHERE false")
    for name, value in [("value", "2.5"), ("null", "NULL")]:
        add(f"null_scale_{name}", f"SELECT BROUND(a, CAST(NULL AS INT)) AS r "
            f"FROM VALUES (CAST({value} AS FLOAT)) t(a)")
    for typ in ["DOUBLE", "INT", "BIGINT", "DECIMAL(6,3)"]:
        add("control_" + typ, f"SELECT BROUND(a, 0) AS r FROM VALUES "
            f"(CAST(2.5 AS {typ})),(CAST(3.5 AS {typ})),(CAST(NULL AS {typ})) t(a)", "control")
    # Keep numerical boundaries visible without expanding a result-type fix.
    for name, value, scale in [
        ("negative_zero", "-0.5", "0"),
        ("high_scale", "2.5", "39"),
        ("low_scale", "2.5", "-46"),
        ("nan", "'NaN'", "0"),
        ("infinity", "'Infinity'", "0"),
    ]:
        add(name, f"SELECT BROUND(a, {scale}) AS r FROM VALUES (CAST({value} AS FLOAT)) t(a)", "boundary")
    return result


def values(actual):
    """Compare represented IEEE values, not decimal renderings or a tolerance."""
    def value(text, typ):
        if text is None:
            return None
        if typ in ("float", "double"):
            number = float(text)
            return "NaN" if math.isnan(number) else struct.pack("!f" if typ == "float" else "!d", number)
        return Decimal(text)
    return Counter(tuple(value(v, t) for v, t in zip(row, types(actual), strict=True))
                   for row in actual["rows"])


def compare(reference, candidate, corpus=CASES):
    left, right = (json.loads(p.read_text()) for p in (reference, candidate))
    cases = load_cases(corpus)
    assert left["cases"] == right["cases"] == cases, "changed SQL"
    ids = [f"{c['id']}_{ansi}" for c in cases for ansi in ("true", "false")]
    assert [r["id"] for r in left["results"]] == [r["id"] for r in right["results"]] == ids
    checks = []
    for case, a, b in zip([c for c in cases for _ in range(2)], left["results"], right["results"], strict=True):
        a, b = a["actual"], b["actual"]
        differences = []
        nullable = None
        if a["status"] != "ok" or b["status"] != "ok":
            differences.append("status")
        else:
            if types(a) != types(b):
                differences.append("types")
            if values(a) != values(b):
                differences.append("rows")
            nullable = [f["nullable"] for f in a["schema"]["fields"]]
            if nullable != b.get("logical_nullable"):
                differences.append("logical_nullable")
        checks.append({"id": ids[len(checks)], "category": case["category"],
                       "differences": differences,
                       "physical_nullable_difference": nullable != b.get("physical_nullable")
                       if nullable is not None else None})
    summary = {category: {"total": sum(c["category"] == category for c in checks),
                         "agreement": sum(c["category"] == category and not c["differences"] for c in checks)}
               for category in ("target", "control", "boundary")}
    return {"summary": summary, "cases": checks}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("generate")
    spark = sub.add_parser("spark")
    spark.add_argument("out", type=Path)
    check = sub.add_parser("compare")
    for name in ("reference", "candidate", "report"):
        check.add_argument(name, type=Path)
    args = parser.parse_args()
    if args.mode == "generate":
        CASES.write_text("".join(json.dumps(c) + "\n" for c in cases()))
    elif args.mode == "spark":
        capture(args.out, CASES)
    else:
        result = compare(args.reference, args.candidate)
        args.report.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result["summary"]))
        return int(any(c["differences"] for c in result["cases"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
