"""FLOAT BROUND zero and scale boundaries for issue 208."""

import argparse
import json
from pathlib import Path
import struct

from decimal_division import capture
from float_bround import compare

ROOT = Path(__file__).resolve().parent
CASES = ROOT / "float-bround-boundaries.jsonl"


def f32(bits):
    return struct.unpack("!f", struct.pack("!I", bits))[0]


def cases():
    result = []

    def add(name, expression, values=None, category="target"):
        sql = f"SELECT {expression} AS r"
        if values is not None:
            sql += " FROM VALUES " + ",".join(
                f"(CAST('{v}' AS FLOAT))" if v is not None else "(CAST(NULL AS FLOAT))"
                for v in values) + " t(a)"
        result.append({"id": name, "sql": sql, "batch_size": 3, "category": category})

    base_bits = [0, 1, 2, 3, 16, 0x007FFFFF, 0x00800000, 0x00800001,
                 0x3EFFFFFF, 0x3F000000, 0x3F000001, 0x40200000, 0x40600000,
                 0x7F7FFFFE, 0x7F7FFFFF, 0x7F800000, 0x7FC00000]
    scales = [-47, -46, -45, -40, -39, -38, -37, -1, 0, 1, 37, 38, 39, 44, 45, 46, 47]
    for scale in scales:
        bits = set(base_bits)
        threshold = 0.5 * 10.0 ** -scale
        if f32(1) <= threshold <= f32(0x7F7FFFFF):
            center = struct.unpack("!I", struct.pack("!f", threshold))[0]
            bits.update([center - 1, center, center + 1])
        values = [repr(f32(b | sign)) for b in sorted(bits) for sign in (0, 0x80000000)]
        add(f"column_scale_{scale}", f"BROUND(a, {scale})", values + [None, None])
        for name, b in [("zero", 0), ("negative_zero", 0x80000000), ("tiny", 1),
                        ("negative_tiny", 0x80000001), ("max", 0x7F7FFFFF),
                        ("negative_max", 0xFF7FFFFF)]:
            add(f"literal_{name}_{scale}", f"BROUND(CAST('{f32(b)!r}' AS FLOAT), {scale})")
    for name, value, scale in [("original_zero", "-0.5", 0), ("original_high", "2.5", 39),
                               ("original_low", "2.5", -46)]:
        add(name, f"BROUND(a, {scale})", [value])
    add("default_scale", "BROUND(a)", ["-0.5", "-0.0", "0.0", "0.5", "2.5", None])
    for typ in ("DOUBLE", "INT", "BIGINT", "DECIMAL(6,3)"):
        add("control_" + typ, f"BROUND(CAST(2.5 AS {typ}), 0)", category="control")
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
