"""BIGINT DIV overflow and mode-isolation evidence for issue 146."""
import argparse
import json
from pathlib import Path

from decimal_division import capture, load_cases
from round_arguments import compare as base_compare, error_cause

ROOT = Path(__file__).resolve().parent
CASES = ROOT / 'bigint-div-overflow.jsonl'
MIN = -9223372036854775808
MAX = 9223372036854775807


def cases():
    result = []
    def add(id, sql, category='target'):
        result.append({'id': id, 'sql': sql, 'category': category,
                       'integer_divide': category != 'control', 'batch_size': 2})
    def long(value):
        return f'CAST({"NULL" if value is None else value} AS BIGINT)'
    for a in [MIN, MIN + 1, MAX, -7, 0, 7, None]:
        for b in [-3, -2, -1, 0, 1, 2, 3, MIN, MAX, None]:
            add(f'literal_{a}_{b}', f'SELECT {long(a)} DIV {long(b)} AS q')
    patterns = {
        'ordinary': [(MIN, 2), (MAX, -1), (-7, 2), (7, -2)],
        'overflow': [(MIN, -1), (MIN + 1, -1), (None, 0), (7, 2)],
        'null_masks': [(None, -1), (MIN, None), (None, 0), (7, -2)],
        'live_zero': [(None, 0), (7, 0)],
        'all_null': [(None, 0), (None, None)],
    }
    for name, pairs in patterns.items():
        source = ','.join(f'({i},{long(a)},{long(b)})' for i, (a, b) in enumerate(pairs))
        for alias, expression in [('operator', 'a DIV b'), ('function', 'div(a,b)')]:
            add(f'{name}_{alias}', f'SELECT {expression} AS q FROM VALUES {source} t(id,a,b) ORDER BY id')
    source = ','.join(f'({i},{long(a)})' for i, a in enumerate([MIN, MIN + 1, MAX, -7, 0, 7, None]))
    for b in [-3, -1, 0, 1, 2, None]:
        add(f'array_scalar_{b}', f'SELECT a DIV {long(b)} AS q FROM VALUES {source} t(id,a) ORDER BY id')
    source = ','.join(f'({i},{long(b)})' for i, b in enumerate([-3, -1, 0, 1, 2, None]))
    for a in [MIN, MIN + 1, MAX, None]:
        add(f'scalar_array_{a}', f'SELECT {long(a)} DIV b AS q FROM VALUES {source} t(id,b) ORDER BY id')
    for typ, minimum in [('TINYINT', -128), ('SMALLINT', -32768), ('INT', -2147483648)]:
        for left, right in [(long(MIN), f'CAST(-1 AS {typ})'),
                            (f'CAST({minimum} AS {typ})', long(-1)),
                            (f'CAST({minimum} AS {typ})', f'CAST(-1 AS {typ})')]:
            suffix = len(result)
            add(f'mixed_{typ}_{suffix}', f'SELECT a DIV b AS q FROM VALUES ({left},{right}),(NULL,NULL) t(a,b)')
    for id, sql in [
        ('literal_alias', f'SELECT div({long(MIN)}, {long(-1)}) AS q'),
        ('empty', f'SELECT a DIV b AS q FROM VALUES ({long(MIN)},{long(-1)}) t(a,b) WHERE false'),
        ('dead_literal', f'SELECT CASE WHEN false THEN {long(MIN)} DIV {long(-1)} ELSE 7L END AS q'),
        ('dead_column', f'SELECT CASE WHEN a=7 THEN a DIV b ELSE 7L END AS q FROM VALUES ({long(MIN)},{long(-1)}) t(a,b)'),
        ('live_column', f'SELECT CASE WHEN a={long(MIN)} THEN a DIV b ELSE 7L END AS q FROM VALUES ({long(MIN)},{long(-1)}) t(a,b)'),
        ('filtered', f'SELECT a DIV b AS q FROM VALUES ({long(MIN)},{long(-1)}),(7L,2L) t(a,b) WHERE a=7'),
        ('range_column', f'SELECT ({long(MIN)}+id) DIV -1L AS q FROM range(2)'),
        ('null_left', f'SELECT NULL DIV {long(-1)} AS q'),
        ('null_right', f'SELECT {long(MIN)} DIV NULL AS q'),
    ]:
        add(id, sql)
    for id, sql in [
        ('bad_left_zero', "SELECT CAST('bad' AS BIGINT) DIV 0L AS q"),
        ('bad_left_null', "SELECT CAST('bad' AS BIGINT) DIV CAST(NULL AS BIGINT) AS q"),
        ('null_left_bad_right', "SELECT CAST(NULL AS BIGINT) DIV CAST('bad' AS BIGINT) AS q"),
        ('column_null_bad_right', "SELECT CAST(NULL AS BIGINT) DIV CAST(b AS BIGINT) AS q FROM VALUES ('bad') t(b)"),
        ('column_bad_left_zero', "SELECT CAST(a AS BIGINT) DIV 0L AS q FROM VALUES ('bad') t(a)"),
    ]:
        add(id, sql, 'evaluation_reference')
    for id, sql in [
        ('remainder', f'SELECT {long(MIN)} % {long(-1)} AS q'),
        ('divide', 'SELECT 7L / 2L AS q'),
        ('decimal_div', 'SELECT CAST(7 AS DECIMAL(8,2)) DIV CAST(2 AS DECIMAL(8,2)) AS q'),
        ('round', 'SELECT ROUND(25L,-1) AS q'),
    ]:
        add(id, sql, 'control')
    return result


def cause(actual, direct):
    if actual.get('condition'):
        return actual['condition']
    text = actual.get('error', '')
    if direct and ('[ARITHMETIC_OVERFLOW]' in text or
                   'Arithmetic overflow: Overflow happened on: -9223372036854775808 / -1' in text):
        return 'ARITHMETIC_OVERFLOW'
    return error_cause(actual)


def compare(reference, candidate, corpus=CASES):
    check = base_compare(reference, candidate, corpus)
    a, b = (json.loads(p.read_text())['results'] for p in [reference, candidate])
    for case, x, y, row in zip([c for c in load_cases(corpus) for _ in range(2)], a, b, check['cases'], strict=True):
        x, y = x['actual'], y['actual']
        if x['status'] != 'ok' and y['status'] != 'ok':
            cx, cy = (cause(v, case['integer_divide']) for v in [x, y])
            row['differences'] = [] if cx is not None and cx == cy else ['error_cause']
            row['error_causes'] = [cx, cy]
    check['agreement'] = sum(not r['differences'] for r in check['cases'])
    return check


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    sub.add_parser('generate')
    sub.add_parser('spark').add_argument('out', type=Path)
    check = sub.add_parser('compare')
    for name in ['reference', 'candidate', 'report']:
        check.add_argument(name, type=Path)
    args = parser.parse_args()
    if args.mode == 'generate':
        CASES.write_text(''.join(json.dumps(c) + '\n' for c in cases()))
    elif args.mode == 'spark':
        capture(args.out, CASES)
    else:
        result = compare(args.reference, args.candidate)
        args.report.write_text(json.dumps(result, indent=2) + '\n')
        print(result['agreement'], '/', result['total'])
        return int(result['agreement'] != result['total'])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
