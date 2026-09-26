"""Compare early arithmetic errors without equating unrelated error causes."""
import argparse
import json
from pathlib import Path
import re

from decimal_division import capture
from string_modulo_coercion import compare as base_compare
from division_cast_classification import cause as arithmetic_cause
from modulo_null_guard import cause as modulo_cause

CASES = Path(__file__).with_name('early-arithmetic.jsonl')


def cause(actual):
    known = modulo_cause(actual, False) or arithmetic_cause(actual)
    if known is not None:
        return known
    # Arrow's numeric narrowing error has no Spark condition tag. This does
    # not match string parsing, decimal precision errors, or generic CAST errors.
    number = r'(?:NaN|[-+]?(?:inf|Infinity|\d+(?:\.\d*)?(?:[eE][-+]?\d+)?))'
    if re.search(r"Cast error: Can't cast value " + number + r' to type Int(?:8|16|32|64)\b',
                 actual.get('error', '')):
        return 'CAST_OVERFLOW'
    return None


def compare(reference, candidate, corpus=CASES):
    result = base_compare(reference, candidate, corpus)
    a, b = (json.loads(path.read_text())['results'] for path in [reference, candidate])
    for left, right, row in zip(a, b, result['cases'], strict=True):
        if left['actual']['status'] != 'ok' and right['actual']['status'] != 'ok':
            causes = [cause(x['actual']) for x in [left, right]]
            row['error_causes'] = causes
            row['differences'] = [] if causes[0] is not None and causes[0] == causes[1] else ['error_cause']
    result['agreement'] = sum(not row['differences'] for row in result['cases'])
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    sub.add_parser('spark').add_argument('out', type=Path)
    check = sub.add_parser('compare')
    for name in ['reference', 'candidate', 'out']:
        check.add_argument(name, type=Path)
    args = parser.parse_args()
    if args.mode == 'spark':
        capture(args.out, CASES)
    else:
        result = compare(args.reference, args.candidate)
        args.out.write_text(json.dumps(result, indent=2) + '\n')
        print(result['agreement'], '/', result['total'])
