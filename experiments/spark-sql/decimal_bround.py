"""Bounded Decimal128 BROUND reference cases for issue 115."""

import argparse
import json
from collections import Counter
from pathlib import Path

from decimal_division import capture, rows, types

ROOT = Path(__file__).resolve().parent
CASES = ROOT / "decimal-bround.jsonl"


def cases():
    result = []

    def add(name, sql, control=False):
        result.append({"id": name, "sql": sql, "batch_size": 2,
                       "control": control})

    def pair(name, value, precision, scale, target):
        value = f"CAST('{value}' AS DECIMAL({precision},{scale}))"
        suffix = "" if target is None else f", {target}"
        add(name + "_literal", f"SELECT BROUND({value}{suffix}) AS r")
        add(name + "_column", f"SELECT BROUND(a{suffix}) AS r FROM VALUES ({value}),"
            f"(CAST(NULL AS DECIMAL({precision},{scale}))) t(a)")

    for i, value in enumerate(["2.345", "2.355", "-2.345", "-2.355", "2.344", "2.346",
                               "-2.344", "-2.346", "0", "9.995", "-9.995"]):
        pair(f"ties_{i}", value, 6, 3, "2")
    for precision in range(1, 39):
        scale = min(precision, 3)
        # Exercise precision expansion, scale reduction and the cap at 38.
        pair(f"precision_{precision}", "0." + "0" * (scale - 1) + "5",
             precision, scale, str(scale - 1))
    for scale in [0, 1, 18, 28, 37, 38]:
        value = "5" if scale == 0 else "0." + "0" * (scale - 1) + "5"
        for target in [-1, 0, scale, scale + 1, 2147483647]:
            pair(f"scale_{scale}_{target}", value, 38, scale, str(target))
    for value in ["25", "35", "-25", "-35", "99", "-99"]:
        pair("negative_" + value, value, 6, 0, "-1")
    for target in [-38, -39, -255, -256, -1000, -2147483648, -2147483647]:
        pair(f"extreme_{target}", "2.345", 6, 3, str(target))
        pair(f"extreme_zero_{target}", "0", 38, 0, str(target))
    for i, value in enumerate(["9" * 38, "-" + "9" * 38, "5" + "0" * 37]):
        for target in [-1, -38, -39]:
            pair(f"overflow_{i}_{target}", value, 38, 0, str(target))
    for name, target in [
        ("default", None), ("null", "NULL"), ("typed_null", "CAST(NULL AS INT)"),
        ("tiny", "CAST(2 AS TINYINT)"), ("small", "CAST(2 AS SMALLINT)"),
        ("big", "2L"), ("decimal", "2.9"), ("double", "CAST(2.9 AS DOUBLE)"),
        ("string", "'2'"), ("invalid_string", "'bad'"), ("bool", "true"),
        ("large_big", "2147483648L"), ("folded", "1+1"),
        ("volatile", "rand()"),
    ]:
        pair("argument_" + name, "2.345", 6, 3, target)
    add("column_scale", "SELECT BROUND(a, d) AS r FROM VALUES (CAST(2.345 AS DECIMAL(6,3)),2) t(a,d)")
    add("null_input", "SELECT BROUND(CAST(NULL AS DECIMAL(6,3)), 2) AS r")
    add("empty", "SELECT BROUND(a, 2) AS r FROM VALUES (CAST(2.345 AS DECIMAL(6,3))) t(a) WHERE false")
    add("null_scale_skips_input", "SELECT BROUND(CAST(1 AS DECIMAL(6,3))/CAST(0 AS DECIMAL(6,3)), NULL) AS r")
    for name, typ in [("integer", "INT"), ("bigint", "BIGINT"), ("double", "DOUBLE"), ("float", "FLOAT")]:
        add("control_" + name, f"SELECT BROUND(a, 0) AS r FROM VALUES (CAST(2.5 AS {typ})),"
            f"(CAST(3.5 AS {typ})),(CAST(NULL AS {typ})) t(a)", True)
    add("control_round", "SELECT ROUND(a, 2) AS r FROM VALUES (CAST(2.345 AS DECIMAL(6,3))),"
        "(CAST(-2.345 AS DECIMAL(6,3))),(CAST(NULL AS DECIMAL(6,3))) t(a)", True)
    return result


def compare(reference, candidate):
    reference, candidate = (json.loads(p.read_text()) for p in (reference, candidate))
    corpus = [json.loads(line) for line in CASES.read_text().splitlines()]
    assert reference["cases"] == candidate["cases"] == corpus, "changed SQL"
    ids = [f"{c['id']}_{ansi}" for c in corpus for ansi in ("true", "false")]
    assert [r["id"] for r in reference["results"]] == ids
    assert [r["id"] for r in candidate["results"]] == ids
    controls = {f"{c['id']}_{ansi}" for c in corpus if c.get("control")
                for ansi in ("true", "false")}
    checks = []
    for left, right in zip(reference["results"], candidate["results"], strict=True):
        expected, actual = left["actual"], right["actual"]
        diff = []
        if expected["status"] == actual["status"] == "ok":
            if types(expected) != types(actual):
                diff.append("types")
            if Counter(map(tuple, rows(expected))) != Counter(map(tuple, rows(actual))):
                diff.append("rows")
            if [f["nullable"] for f in expected["schema"]["fields"]] != actual.get("logical_nullable"):
                diff.append("logical_nullable")
        elif expected["status"] != "ok" and actual["status"] != "ok":
            condition = expected.get("condition")
            # Spark's BigDecimal scale underflow has no structured condition.
            token = f"[{condition}]" if condition else "Underflow"
            if token not in actual.get("error", "") or (
                condition is None and expected.get("error", "").strip() != "Underflow"
            ):
                diff.append("error_condition")
        else:
            diff.append("status")
        checks.append({"id": left["id"], "differences": diff, "control": left["id"] in controls,
                       "physical_nullable_difference": (
                           [f["nullable"] for f in expected["schema"]["fields"]] != actual.get("physical_nullable")
                       ) if expected["status"] == actual["status"] == "ok" else None,
                       "phase_difference": [expected["status"], actual["status"]]
                       if expected["status"] != actual["status"] else None})
    target = [c for c in checks if not c["control"]]
    return {"target_agreement": sum(not c["differences"] for c in target),
            "target_total": len(target),
            "agreement": sum(not c["differences"] for c in checks),
            "total": len(checks), "cases": checks}


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
        CASES.write_text("".join(json.dumps(case) + "\n" for case in cases()))
    elif args.mode == "spark":
        capture(args.out, CASES)
    else:
        report = compare(args.reference, args.candidate)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(f"{report['agreement']}/{report['total']} agree on values/types, logical nullability or error condition")
        return int(report["agreement"] != report["total"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
