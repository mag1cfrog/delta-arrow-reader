"""Generate and verify the bounded signed-integer overflow corpus for issue 114."""

import argparse
import json
from pathlib import Path

from decimal_division import capture, rows, types

ROOT = Path(__file__).resolve().parent
CASES = ROOT / "integer-overflow.jsonl"


def cases():
    result = []

    def add(name, sql):
        result.append({"id": name, "sql": sql, "batch_size": 2})

    def cast(value, kind):
        return f"CAST({value} AS {kind})"

    for kind, bits in [("TINYINT", 8), ("SMALLINT", 16), ("INT", 32), ("BIGINT", 64)]:
        lo, hi = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
        boundaries = {
            "add": ("+", [(hi, 0), (hi, 1), (lo, 0), (lo, -1)]),
            "sub": ("-", [(hi, 0), (hi, -1), (lo, 0), (lo, 1)]),
            "mul": ("*", [(hi // 2, 2), (hi // 2 + 1, 2),
                          (lo // 2, 2), (lo // 2 - 1, 2), (lo, -1)]),
        }
        for op_name, (op, pairs) in boundaries.items():
            for i, (x, y) in enumerate(pairs):
                left, right = cast(x, kind), cast(y, kind)
                prefix = f"{kind.lower()}_{op_name}_{i}"
                add(prefix + "_constants", f"SELECT {left} {op} {right} AS r")
                add(prefix + "_scalar_array", f"SELECT {left} {op} b AS r FROM VALUES ({right}) t(b)")
                add(prefix + "_array_scalar", f"SELECT a {op} {right} AS r FROM VALUES ({left}) t(a)")
                add(prefix + "_arrays", f"SELECT a {op} b AS r FROM VALUES ({left},{right}) t(a,b)")
            for null_left in (False, True):
                x, y = ("NULL", hi) if null_left else (hi, "NULL")
                add(f"{kind.lower()}_{op_name}_null_{null_left}",
                    f"SELECT a {op} b AS r FROM VALUES ({cast(x, kind)},{cast(y, kind)}) t(a,b)")
            # A wider operand changes the operation's width before overflow checking.
            for wide in ("INT", "BIGINT"):
                add(f"{kind.lower()}_{op_name}_promote_{wide}",
                    f"SELECT a {op} b AS r FROM VALUES ({cast(hi, kind)},{cast(2, wide)}) t(a,b)")

    add("arithmetic_overflow_ansi", "SELECT CAST(2147483647 AS INT) + id AS r FROM VALUES (1) AS t(id)")
    for name, op, bound, other in [("add", "+", 2147483647, 1),
                                  ("sub", "-", -2147483648, 1),
                                  ("mul", "*", 1073741824, 2)]:
        expr = f"a {op} {other}"
        source = f"FROM VALUES ({bound},false),(0,true) t(a,keep)"
        add(name + "_dead_case", f"SELECT CASE WHEN keep THEN {expr} ELSE 0 END AS r {source}")
        add(name + "_live_case", f"SELECT CASE WHEN NOT keep THEN {expr} ELSE 0 END AS r {source}")
        add(name + "_filtered", f"SELECT {expr} AS r {source} WHERE keep")
        add(name + "_live_filter", f"SELECT a AS r {source} WHERE {expr} = 0")
        add(name + "_empty", f"SELECT {expr} AS r {source} WHERE false")
        add(name + "_constant_dead_case", f"SELECT CASE WHEN false THEN {bound} {op} {other} ELSE 0 END AS r")
        add(name + "_untyped_null", f"SELECT a {op} NULL AS r {source}")
        add(name + "_nested_error_null", f"SELECT ({expr}) {op} CAST(NULL AS INT) AS r {source}")
        for left in ["CAST(NULL AS INT)", "n"]:
            add(name + "_left_null_" + str(left == "n"),
                f"SELECT {left} {op} (a {op} {other}) AS r FROM VALUES ({bound},CAST(NULL AS INT)),(0,1) t(a,n)")
        for source_name, source in [("range", "range(2)"), ("values", "(VALUES(0),(1)) t(id)")]:
            add(name + "_derived_null_" + source_name,
                f"SELECT n {op} r AS r FROM (SELECT CAST(NULL AS INT) AS n,CAST({bound} AS INT) {op} CAST(id+{other} AS INT) AS r FROM {source}) q")
        add(name + "_constant_error_right_null", f"SELECT ({bound} {op} {other}) {op} CAST(NULL AS INT) AS r")
        add(name + "_constant_error_left_null", f"SELECT CAST(NULL AS INT) {op} ({bound} {op} {other}) AS r")
        add(name + "_float_control", f"SELECT CAST(1.5 AS FLOAT) {op} CAST(2 AS FLOAT) AS r")
        add(name + "_double_control", f"SELECT CAST(1.5 AS DOUBLE) {op} CAST(2 AS BIGINT) AS r")
        add(name + "_decimal_control", f"SELECT CAST(1.5 AS DECIMAL(8,2)) {op} CAST(2 AS DECIMAL(8,2)) AS r")
    return result


def compare(reference, candidate):
    reference, candidate = (json.loads(p.read_text()) for p in (reference, candidate))
    corpus = [json.loads(line) for line in CASES.read_text().splitlines()]
    assert reference["cases"] == candidate["cases"] == corpus, "changed SQL"
    ids = [f"{case['id']}_{ansi}" for case in corpus for ansi in ("true", "false")]
    assert [r["id"] for r in reference["results"]] == ids
    assert [r["id"] for r in candidate["results"]] == ids
    checked = []
    for a, b in zip(reference["results"], candidate["results"], strict=True):
        expected, actual = a["actual"], b["actual"]
        diff = []
        phase = None
        if expected["status"] == actual["status"] == "ok":
            if types(expected) != types(actual):
                diff.append("types")
            if sorted(map(str, rows(expected))) != sorted(map(str, rows(actual))):
                diff.append("rows")
        elif expected["status"] != "ok" and actual["status"] != "ok":
            # Arbitrary errors are never counted as compatible overflow.
            condition = expected.get("condition")
            if condition not in {"ARITHMETIC_OVERFLOW", "BINARY_ARITHMETIC_OVERFLOW"} or f"[{condition}]" not in actual["error"]:
                diff.append("error_condition")
            if expected["status"] != actual["status"]:
                phase = [expected["status"], actual["status"]]
        else:
            diff.append("status")
        checked.append({"id": a["id"], "differences": diff, "phase_difference": phase})
    return {"agreement": sum(not row["differences"] for row in checked),
            "total": len(checked), "cases": checked}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("generate")
    spark = sub.add_parser("spark")
    spark.add_argument("out", type=Path)
    check = sub.add_parser("compare")
    check.add_argument("reference", type=Path)
    check.add_argument("candidate", type=Path)
    check.add_argument("report", type=Path)
    args = parser.parse_args()
    if args.mode == "generate":
        CASES.write_text("".join(json.dumps(case) + "\n" for case in cases()))
    elif args.mode == "spark":
        capture(args.out, CASES)
    else:
        result = compare(args.reference, args.candidate)
        args.report.write_text(json.dumps(result, indent=2) + "\n")
        print(f"{result['agreement']}/{result['total']} agree on values/types or overflow condition")
        return int(result["agreement"] != result["total"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
