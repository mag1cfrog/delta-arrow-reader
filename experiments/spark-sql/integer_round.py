"""Signed-integer ROUND reference cases for issue 212."""

import argparse
import json
from pathlib import Path

from decimal_division import capture, compare as numeric_compare
from float_bround import values
from round_arguments import error_cause as argument_error_cause

ROOT = Path(__file__).resolve().parent
CASES = ROOT / "integer-round.jsonl"


def cases():
    result = []

    def add(name, sql):
        result.append({"id": name, "sql": sql, "batch_size": 2})

    for dtype, bits in [("TINYINT", 8), ("SMALLINT", 16), ("INT", 32), ("BIGINT", 64)]:
        low, high = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
        values = sorted({low, low + 1, high - 1, high, -26, -25, -24,
                         -16, -15, -14, -6, -5, -4, 0, 4, 5, 6, 14, 15, 16, 24, 25, 26})
        for value in values:
            for scale in [-2, -1, 0, 2]:
                add(f"{dtype}_{value}_{scale}", f"SELECT ROUND(CAST({value} AS {dtype}), {scale}) AS r")
        for value in [low, high, 0]:
            for scale in [-38, -20, -19, -18, -10, -5, -3, 18, 38]:
                add(f"{dtype}_boundary_{value}_{scale}", f"SELECT ROUND(CAST({value} AS {dtype}), {scale}) AS r")
        entries = ", ".join(f"(CAST({v} AS {dtype}))" for v in values) + ", (NULL)"
        for scale in [-38, -20, -19, -18, -10, -5, -3, -2, -1, 0, 2, 38]:
            add(f"{dtype}_column_{scale}", f"SELECT ROUND(a, {scale}) AS r FROM VALUES {entries} t(a)")
        for scale in ["NULL", "'2'", "-1.9"]:
            add(f"{dtype}_coercion_{scale}", f"SELECT ROUND(a, {scale}) AS r FROM VALUES (CAST(25 AS {dtype})), (NULL) t(a)")
        for scale in [-19, -1, 2]:
            add(f"{dtype}_empty_{scale}", f"SELECT ROUND(CAST(id AS {dtype}), {scale}) AS r FROM range(0)")
            add(f"{dtype}_null_{scale}", f"SELECT ROUND(a, {scale}) AS r FROM VALUES (CAST(NULL AS {dtype})), (NULL) t(a)")
    for value in [9007199254740991, 9007199254740992, 9007199254740993,
                  4999999999999999999, 5000000000000000000, 5000000000000000001]:
        for sign in [1, -1]:
            for scale in [-19, -18, -1, 0, 2]:
                add(f"big_exact_{value * sign}_{scale}", f"SELECT ROUND(CAST({value * sign} AS BIGINT), {scale}) AS r")
    for name, expr in [
        ("default", "ROUND(25L)"),
        ("decimal", "ROUND(CAST(1.255 AS DECIMAL(10,3)), '2')"),
        ("float", "ROUND(CAST(1.25 AS FLOAT), 1)"),
        ("double", "ROUND(CAST(1.25 AS DOUBLE), 1)"),
        ("string", "ROUND('1.25', '1')"),
        ("bround_decimal", "BROUND(CAST(2.345 AS DECIMAL(6,3)), '2')"),
        ("bround_float", "BROUND(CAST(-0.5 AS FLOAT), 0)"),
    ]:
        add("control_" + name, f"SELECT {expr} AS r")
    return result


def error_cause(actual):
    if actual.get("condition"):
        return actual["condition"]
    message = actual.get("error", "")
    if "[ARITHMETIC_OVERFLOW]" in message:
        return "ARITHMETIC_OVERFLOW"
    for cause in ["Underflow", "BigInteger would overflow supported range"]:
        if message.strip() == cause or f"error: {cause}" in message or f"ArithmeticException: {cause}" in message:
            return cause
    return argument_error_cause(actual)


def compare(reference, candidate, corpus=CASES):
    result = numeric_compare(reference, candidate, corpus)
    left, right = (json.loads(p.read_text())["results"] for p in [reference, candidate])
    for check, a, b in zip(result["cases"], left, right, strict=True):
        x, y = a["actual"], b["actual"]
        successful = x["status"] == y["status"] == "ok"
        if successful:
            check["differences"] = [d for d in check["differences"] if d != "rows"]
            if values(x) != values(y):
                check["differences"].append("rows")
        if x["status"] != "ok" and y["status"] != "ok":
            check["differences"] = [] if error_cause(x) is not None and error_cause(x) == error_cause(y) else ["error_cause"]
        check["phase_difference"] = [x["status"], y["status"]] if x["status"] != y["status"] else None
        nullable = [f["nullable"] for f in x.get("schema", {}).get("fields", [])]
        check["logical_nullable_difference"] = nullable != y.get("logical_nullable") if successful else None
        check["physical_nullable_difference"] = nullable != y.get("physical_nullable") if successful else None
    result["agreement"] = sum(not c["differences"] for c in result["cases"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("generate")
    spark = sub.add_parser("spark")
    spark.add_argument("out", type=Path)
    check = sub.add_parser("compare")
    for name in ["reference", "candidate", "report"]:
        check.add_argument(name, type=Path)
    args = parser.parse_args()
    if args.mode == "generate":
        CASES.write_text("".join(json.dumps(c) + "\n" for c in cases()))
    elif args.mode == "spark":
        capture(args.out, CASES)
    else:
        result = compare(args.reference, args.candidate)
        args.report.write_text(json.dumps(result, indent=2) + "\n")
        print(f"{result['agreement']}/{result['total']} agree on values/types or error cause")
        return int(result["agreement"] != result["total"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
