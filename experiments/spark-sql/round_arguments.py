"""ROUND argument coercion and NULL-scale reference cases for issue 144."""

import argparse
import json
from pathlib import Path

from decimal_division import capture, load_cases, types
from float_bround import values

ROOT = Path(__file__).resolve().parent
CASES = ROOT / "round-arguments.jsonl"


def cases():
    result = []

    def add(name, sql):
        result.append({"id": name, "sql": sql, "batch_size": 2})

    scales = [
        ("default", None), ("negative", "-1"), ("zero", "0"), ("positive", "2"),
        ("tiny", "CAST(2 AS TINYINT)"), ("small", "CAST(2 AS SMALLINT)"),
        ("long", "2L"), ("decimal", "2.9"), ("float", "2.9F"),
        ("double", "CAST(2.9 AS DOUBLE)"), ("negative_decimal", "-1.9"),
        ("string", "'2'"), ("fraction_string", "'2.9'"),
        ("spaced_string", "' 2 '"), ("bad_string", "'bad'"),
        ("boolean", "true"), ("null", "NULL"), ("string_null", "CAST(NULL AS STRING)"),
        ("folded", "1+1"), ("folded_string", "concat('2','')"),
    ]
    for name, value in [("decimal", "CAST(1.255 AS DECIMAL(10,3))"),
                        ("float", "CAST(1.25 AS FLOAT)"),
                        ("double", "CAST(1.25 AS DOUBLE)"), ("string", "'1.25'")]:
        for label, scale in scales:
            suffix = "" if scale is None else ", " + scale
            add(f"{name}_{label}_literal", f"SELECT ROUND({value}{suffix}) AS r")
            add(f"{name}_{label}_column", f"SELECT ROUND(a{suffix}) AS r FROM VALUES ({value}), (NULL) t(a)")
    for name, scale in [("long_overflow", "2147483648L"), ("long_underflow", "-2147483649L"),
                        ("float_overflow", "CAST(2147483648 AS FLOAT)"),
                        ("double_overflow", "CAST(2147483648 AS DOUBLE)"),
                        ("decimal_overflow", "CAST(2147483648 AS DECIMAL(12,1))"),
                        ("string_overflow", "'2147483648'"),
                        ("string_underflow", "'-2147483649'"),
                        ("nan", "CAST('NaN' AS DOUBLE)"),
                        ("infinity", "CAST('Infinity' AS DOUBLE)")]:
        # Zero isolates the INT coercion from the separately owned Decimal
        # BigDecimal scale-underflow behavior on nonzero values.
        add(name, f"SELECT ROUND(a, {scale}) AS r FROM VALUES (CAST(0 AS DECIMAL(10,3))), (NULL) t(a)")
    for name, value in [("bad_decimal", "CAST('bad' AS DECIMAL(10,3))"),
                        ("bad_double", "CAST('bad' AS DOUBLE)"),
                        ("bad_string", "'bad'"),
                        ("division", "CAST(1 AS DECIMAL(10,3))/CAST(0 AS DECIMAL(10,3))")]:
        for suffix, scale in [("null", "CAST(NULL AS INT)"), ("live", "2")]:
            add(f"evaluation_{name}_{suffix}", f"SELECT ROUND({value}, {scale}) AS r")
    for scale in ("NULL", "'bad'", "2"):
        add("column_bad_" + scale, f"SELECT ROUND(CAST(a AS DECIMAL(10,3)), {scale}) AS r FROM VALUES ('bad'), (NULL) t(a)")
    for value in ("true", "array(1,2)"):
        add("invalid_value_" + value, f"SELECT ROUND({value}, 2) AS r")
    for label, sql in [
        ("null_input", "SELECT ROUND(NULL, '2') AS r"),
        ("empty", "SELECT ROUND(a, '2') AS r FROM VALUES (CAST(1.255 AS DECIMAL(10,3))) t(a) WHERE false"),
        ("all_null", "SELECT ROUND(a, '2') AS r FROM VALUES (CAST(NULL AS DECIMAL(10,3))), (NULL) t(a)"),
        ("column_scale", "SELECT ROUND(a,d) AS r FROM VALUES (CAST(1.255 AS DECIMAL(10,3)),2) t(a,d)"),
        ("null_dynamic_scale", "SELECT ROUND(CAST(NULL AS DECIMAL(10,3)), id) AS r FROM range(2)"),
        ("volatile_scale", "SELECT ROUND(CAST(1.255 AS DECIMAL(10,3)), rand()) AS r"),
        ("subquery_scale", "SELECT ROUND(CAST(1.255 AS DECIMAL(10,3)), (SELECT 2)) AS r"),
        ("subquery_bad_input", "SELECT ROUND((SELECT CAST(v AS DECIMAL(10,3)) FROM VALUES ('bad') t(v)), NULL) AS r"),
        ("subquery_multiple_rows", "SELECT ROUND((SELECT CAST(v AS DECIMAL(10,3)) FROM VALUES (1), (2) t(v)), NULL) AS r"),
        ("no_arguments", "SELECT ROUND() AS r"),
        ("extra_argument", "SELECT ROUND(1.2, 2, 3) AS r"),
        ("nested", "SELECT ROUND(ROUND(CAST(1.255 AS DECIMAL(10,3)), '2'), 1.9) AS r"),
    ]:
        add(label, sql)
    return result


def error_cause(actual):
    """Keep Spark condition identity separate from text and evaluation phase."""
    if actual.get("condition"):
        return actual["condition"]
    text = actual.get("error", "")
    for condition in ("CAST_INVALID_INPUT", "CAST_OVERFLOW", "DIVIDE_BY_ZERO",
                      "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE", "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT",
                      "WRONG_NUM_ARGS.WITHOUT_SUGGESTION", "SCALAR_SUBQUERY_TOO_MANY_ROWS"):
        if f"[{condition}]" in text:
            return condition
    # Existing native CAST/subquery errors omit Spark's structured condition.
    if "Cannot cast" in text:
        return "CAST_INVALID_INPUT"
    if "Scalar subquery returned more than one row" in text:
        return "SCALAR_SUBQUERY_TOO_MANY_ROWS"
    if "Arrow error: Divide by zero error" in text:
        return "DIVIDE_BY_ZERO"
    if "Decimal overflow: rounded value exceeds precision" in text:
        return "NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION"
    if "out of range integral type conversion attempted" in text:
        return "CAST_OVERFLOW"
    if text.strip() == "Underflow" or "Execution error: Underflow" in text:
        return "Underflow"
    return None


def compare(reference, candidate, corpus=CASES):
    left, right = (json.loads(p.read_text()) for p in (reference, candidate))
    corpus = load_cases(corpus)
    assert left["cases"] == right["cases"] == corpus, "changed SQL"
    ids = [f"{c['id']}_{ansi}" for c in corpus for ansi in ("true", "false")]
    assert [r["id"] for r in left["results"]] == [r["id"] for r in right["results"]] == ids
    checks = []
    for a, b in zip(left["results"], right["results"], strict=True):
        x, y = a["actual"], b["actual"]
        diff = []
        successful = x["status"] == y["status"] == "ok"
        if successful:
            if types(x) != types(y):
                diff.append("types")
            if values(x) != values(y):
                diff.append("rows")
        elif x["status"] != "ok" and y["status"] != "ok":
            if error_cause(x) is None or error_cause(x) != error_cause(y):
                diff.append("error_cause")
        else:
            diff.append("status")
        nullable = [f["nullable"] for f in x.get("schema", {}).get("fields", [])]
        checks.append({"id": a["id"], "differences": diff,
                       "phase_difference": [x["status"], y["status"]] if x["status"] != y["status"] else None,
                       "logical_nullable_difference": nullable != y.get("logical_nullable") if successful else None,
                       "physical_nullable_difference": nullable != y.get("physical_nullable") if successful else None})
    return {"agreement": sum(not c["differences"] for c in checks), "total": len(checks), "cases": checks}


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
        print(f"{result['agreement']}/{result['total']} agree on values/types or error cause")
        return int(result["agreement"] != result["total"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
