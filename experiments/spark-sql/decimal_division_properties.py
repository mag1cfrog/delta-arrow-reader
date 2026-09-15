"""Check Decimal division through SQL against exact Python integer arithmetic."""

import argparse
import json
import random
import subprocess
from pathlib import Path

from decimal_division import compare


def result_type(p1, s1, p2, s2):
    # Spark default allowPrecisionLoss=true, nonnegative input scales.
    scale = max(6, s1 + p2 + 1)
    precision = p1 - s1 + s2 + scale
    if precision > 38:
        scale = max(38 - (precision - scale), min(scale, 6))
        precision = 38
    return precision, scale


def decimal_text(value, scale):
    if value is None:
        return "NULL"
    sign = "-" if value < 0 else ""
    digits = str(abs(value)).rjust(scale + 1, "0")
    return sign + (digits[:-scale] + "." + digits[-scale:] if scale else digits)


def generate():
    rng = random.Random(380076)
    pairs = set()
    types = [(1, 0), (3, 0), (10, 2), (18, 4), (20, 0), (28, 18),
             (38, 0), (38, 2), (38, 6), (38, 18), (38, 34), (38, 38)]
    pairs.update((*a, *b) for a in types for b in types)
    # Every valid Decimal128 type pair whose guard-digit expression exceeds 76 digits.
    for p1 in range(1, 39):
        for s1 in range(p1 + 1):
            for p2 in range(1, 39):
                for s2 in range(p2 + 1):
                    _, scale = result_type(p1, s1, p2, s2)
                    if p1 - s1 + max(s1, scale - 3) + 4 + s2 > 76:
                        pairs.add((p1, s1, p2, s2))
    cases, expected = [], []
    for p1, s1, p2, s2 in sorted(pairs):
        precision, scale = result_type(p1, s1, p2, s2)
        exponent = scale + s2 - s1
        assert 0 <= exponent <= 44
        factor = 10 ** exponent
        amax, bmax, limit = 10 ** p1 - 1, 10 ** p2 - 1, 10 ** precision - 1
        rows = [(0, 1), (1, 1), (amax, bmax), (amax, 1), (1, bmax),
                (amax // 2, max(1, bmax // 2))]
        # Cross both native intermediate widths and the final result boundary.
        for bound in [(2 ** 127 - 1) // factor, (2 ** 255 - 1) // factor,
                      limit * bmax // factor]:
            rows.extend((a, bmax) for a in [bound - 1, bound, bound + 1] if 0 <= a <= amax)
        rows += [(rng.randrange(1, amax + 1), rng.randrange(1, bmax + 1)) for _ in range(8)]
        rows = list(dict.fromkeys((a * sa, b * sb) for a, b in rows
                                  for sa, sb in [(1, 1), (-1, 1), (1, -1), (-1, -1)]))
        rows += [(None, 0), (1, None), (None, None)]

        def evaluate(a, b):
            if a is None or b is None:
                return None, False
            if b == 0:
                return None, True
            # Exact rational division and HALF_UP, with unbounded integers.
            q, r = divmod(abs(a) * factor, abs(b))
            q += 2 * r >= abs(b)
            if (a < 0) != (b < 0):
                q = -q
            overflow = abs(q) > limit
            return (None if overflow else decimal_text(q, scale)), overflow

        evaluated = [evaluate(a, b) for a, b in rows]
        for subset in ["all", "representable"]:
            selected = [(row, value) for row, value in zip(rows, evaluated, strict=True)
                        if subset == "all" or not value[1]]
            values = ",".join(
                f"({i}," + ",".join("NULL" if x is None else "'" + decimal_text(x, s) + "'"
                                      for x, s in zip(row, [s1, s2], strict=True)) + ")"
                for i, (row, _) in enumerate(selected))
            case = {"id": f"p{p1}s{s1}_p{p2}s{s2}_{subset}", "batch_size": 8,
                    "sql": f"SELECT CAST(a AS DECIMAL({p1},{s1})) / CAST(b AS DECIMAL({p2},{s2})) AS r "
                           f"FROM VALUES {values} AS t(id,a,b) ORDER BY id"}
            cases.append(case)
            for ansi in [True, False]:
                if ansi and any(overflow for _, (_, overflow) in selected):
                    actual = {"status": "execution_error"}
                else:
                    actual = {"status": "ok", "types": [f"decimal({precision},{scale})"],
                              "rows": [[value] for _, (value, _) in selected]}
                expected.append({"id": case["id"] + "_" + str(ansi).lower(), "actual": actual})
    return cases, expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    args.run_dir.mkdir(parents=True, exist_ok=False)
    cases, expected = generate()
    cases_file, reference, candidate, report = [args.run_dir / name for name in
                                               ["cases.jsonl", "reference.json", "candidate.json", "check.json"]]
    cases_file.write_text("".join(json.dumps(case) + "\n" for case in cases))
    reference.write_text(json.dumps({"reference": "exact Python integers, not a Spark capture",
                                     "cases": cases, "results": expected}))
    subprocess.run([str(args.binary.resolve()), str(cases_file), str(candidate)], check=True)
    result = compare(reference, candidate, cases_file)
    result["exact_rows_checked"] = sum(len(r["actual"].get("rows", [])) for r in expected)
    report.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k != "cases"}))
    return int(result["agreement"] != result["total"])


if __name__ == "__main__":
    raise SystemExit(main())
