"""FLOAT ROUND represented-value and scale boundaries for issue 213."""

import argparse
import json
from pathlib import Path
import struct

from decimal_division import capture
from float_bround import compare
from float_bround_boundaries import f32

ROOT = Path(__file__).resolve().parent
CASES = ROOT / "float-round.jsonl"


def cases():
    result = []

    def add(name, expression, values=None, category="target", empty=False):
        sql = f"SELECT {expression} AS r"
        if values is not None:
            sql += " FROM VALUES " + ",".join(
                f"(CAST('{v}' AS FLOAT))" if v is not None else "(CAST(NULL AS FLOAT))"
                for v in values) + " t(a)"
        if empty:
            sql += " WHERE false"
        result.append({"id": name, "sql": sql, "batch_size": 3, "category": category})

    base = {0, 1, 2, 3, 16, 0x007FFFFF, 0x00800000, 0x00800001,
            0x3EFFFFFF, 0x3F000000, 0x3F000001, 0x3FA00000, 0x40200000,
            0x40600000, 0x4B7FFFFF, 0x4B800000, 0x7F7FFFFE, 0x7F7FFFFF,
            0x7F800000, 0x7FC00000}
    for value in (1.005, 1.015, 2.675, 2.685, 25.0, 35.0):
        center = struct.unpack("!I", struct.pack("!f", value))[0]
        base.update((center - 1, center, center + 1))
    scales = [-1000, -47, -46, -45, -40, -39, -38, -37, -20, -2, -1,
              0, 1, 2, 3, 6, 8, 16, 37, 38, 39, 44, 45, 46, 47, 1000]
    for scale in scales:
        bits = set(base)
        if -38 <= scale <= 45:
            for tie in (0.5, 1.5, 2.5, 9.5):
                value = tie * 10.0 ** -scale
                if f32(1) <= value <= f32(0x7F7FFFFF):
                    center = struct.unpack("!I", struct.pack("!f", value))[0]
                    bits.update((center - 1, center, center + 1))
        values = [repr(f32(b | sign)) for b in sorted(bits) for sign in (0, 0x80000000)]
        add(f"column_scale_{scale}", f"ROUND(a, {scale})", values + [None, None])
        for name, b in [("zero", 0), ("negative_zero", 0x80000000), ("tiny", 1),
                        ("negative_tiny", 0x80000001), ("max", 0x7F7FFFFF),
                        ("negative_max", 0xFF7FFFFF)]:
            add(f"literal_{name}_{scale}", f"ROUND(CAST('{f32(b)!r}' AS FLOAT), {scale})")
    for value in ("2.675", "-2.675", "1.25", "-1.25"):
        for scale in (1, 2):
            add(f"literal_{value}_{scale}", f"ROUND(CAST('{value}' AS FLOAT), {scale})")
    add("default_scale", "ROUND(a)", ["-0.5", "-0.0", "0.0", "0.5", "2.5", None])
    add("empty", "ROUND(a, 2)", ["2.675", None], empty=True)
    add("all_null", "ROUND(a, -1)", [None, None])
    add("null_scale", "ROUND(a, CAST(NULL AS INT))", ["2.675", "NaN", None])
    add("null_scale_bad_input", "ROUND(CAST('bad' AS FLOAT), NULL)")
    for scale in ("'2'", "2L", "1+1", "2.9"):
        add("normalized_" + scale, f"ROUND(a, {scale})", ["2.675", "-2.675", None])
    for typ in ("DOUBLE", "INT", "BIGINT", "DECIMAL(6,3)"):
        for func in ("ROUND", "BROUND"):
            add(f"control_{func}_{typ}", f"{func}(CAST(2.5 AS {typ}), 0)", category="control")
    add("control_float_bround", "BROUND(a, 0)", ["2.5", "-2.5", "-0.5", None], "control")
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
        print(json.dumps(result["summary"]))
        return int(any(c["differences"] for c in result["cases"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
