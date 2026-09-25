"""Decimal ROUND scale-subtraction boundaries for issue 214."""

import argparse
import json
from pathlib import Path

from decimal_division import capture
from round_arguments import compare

ROOT = Path(__file__).resolve().parent
CASES = ROOT / "decimal-round-extreme.jsonl"


def cases():
    result = []

    def add(name, sql, category="target"):
        result.append({"id": name, "sql": sql, "category": category, "batch_size": 2})

    def pair(name, value, precision, scale, target, category="target"):
        value = f"CAST('{value}' AS DECIMAL({precision},{scale}))"
        add(name + "_literal", f"SELECT ROUND({value}, {target}) AS r", category)
        add(name + "_column", f"SELECT ROUND(a, {target}) AS r FROM VALUES "
            f"(CAST(NULL AS DECIMAL({precision},{scale}))),({value}) t(a)", category)

    for precision in range(1, 39):
        scale = min(precision, 3)
        target = scale - 2147483647 - 1
        for sign in ("", "-"):
            pair(f"precision_{precision}_{sign or 'positive'}", sign + "0." + "0" * (scale - 1) + "1",
                 precision, scale, target)
    for scale in (0, 1, 2, 3, 18, 28, 37, 38):
        value = "1" if scale == 0 else "0." + "0" * (scale - 1) + "1"
        boundary = scale - 2147483647
        targets = sorted({-2147483648, boundary - 1, boundary, boundary + 1, -1000, -39, -1, 0, scale, 2147483647})
        for target in targets:
            # Adjacent INT-valid subtraction exposes BigInteger capacity errors,
            # owned by the existing reference review, separately from Underflow.
            category = "reference_review" if target in (boundary, boundary + 1) else "target"
            for name, v in [("positive", value), ("negative", "-" + value), ("zero", "0")]:
                pair(f"scale_{scale}_{target}_{name}", v, 38, scale, target, category)
        extreme = boundary - 1
        for name, source, suffix in [
            ("all_null", f"(CAST(NULL AS DECIMAL(38,{scale}))),(NULL)", ""),
            ("empty", f"(CAST('{value}' AS DECIMAL(38,{scale})))", " WHERE false"),
            ("zero_null", f"(CAST(0 AS DECIMAL(38,{scale}))),(NULL)", ""),
        ]:
            add(f"{name}_{scale}", f"SELECT ROUND(a, {extreme}) AS r FROM VALUES {source} t(a){suffix}")
        add(f"null_scale_{scale}", f"SELECT ROUND(CAST('{value}' AS DECIMAL(38,{scale})), NULL) AS r")
    for name, sql in [
        ("wrapped_long", "SELECT ROUND(CAST(1.255 AS DECIMAL(10,3)), 2147483648L) AS r"),
        ("ordinary_overflow", "SELECT ROUND(CAST('" + "9" * 38 + "' AS DECIMAL(38,0)), -1) AS r"),
        ("half_up_positive", "SELECT ROUND(CAST(2.345 AS DECIMAL(6,3)), 2) AS r"),
        ("half_up_negative", "SELECT ROUND(CAST(-2.345 AS DECIMAL(6,3)), 2) AS r"),
        ("null_skips_bad_input", "SELECT ROUND(CAST('bad' AS DECIMAL(10,2)), NULL) AS r"),
        ("control_float", "SELECT ROUND(2.675F, 2) AS r"),
        ("control_integer", "SELECT ROUND(9007199254740993L, 2) AS r"),
        ("control_double", "SELECT ROUND(CAST(2.5 AS DOUBLE), 0) AS r"),
        ("control_bround", "SELECT BROUND(CAST(2.345 AS DECIMAL(6,3)), 2) AS r"),
    ]:
        add(name, sql, "control" if name.startswith("control") else "target")
    return result


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
        result = compare(args.reference, args.candidate, CASES)
        args.report.write_text(json.dumps(result, indent=2) + "\n")
        print(result["agreement"], "/", result["total"])
        return int(result["agreement"] != result["total"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
