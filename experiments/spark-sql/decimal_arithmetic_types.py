"""Decimal arithmetic result types and precision boundaries for issue 143."""
import argparse
import json
from pathlib import Path

from decimal_division import capture, load_cases
from modulo_null_guard import cause as modulo_cause
from round_arguments import compare as base_compare

ROOT = Path(__file__).resolve().parent
CASES = ROOT / 'decimal-arithmetic-types.jsonl'


def cases():
    result = []

    def add(id, sql, category='target', batch_size=2):
        result.append(dict(id=id, sql=sql, category=category, batch_size=batch_size))

    def decimal(value, precision, scale):
        return f"CAST('{value}' AS DECIMAL({precision},{scale}))"

    operators = [('+', 'add'), ('-', 'sub'), ('*', 'mul'), ('%', 'mod')]
    # Each pair exercises a different result-type bound. Negative rows check
    # HALF_UP ties on both sides of zero; NULL rows check validity propagation.
    pairs = [
        ('small', ('2.25', 8, 2), ('1.5', 3, 1)),
        ('precision37', ('0.123456789012345675', 37, 18), ('1', 1, 0)),
        ('precision38', ('0.123456789012345675', 38, 18), ('1', 1, 0)),
        ('scale38', ('0.' + '9' * 38, 38, 38), ('0.' + '9' * 38, 38, 38)),
        ('aligned76', ('9' * 38, 38, 0), ('0.12345678901234567890123456789012345678', 38, 38)),
        ('scale40', ('1.123456789012345678901234567895', 38, 30), ('1.0000000005', 20, 10)),
        ('minimum6', ('1.12345675', 38, 8), ('2.00000005', 38, 8)),
        ('preserve6', ('1.123455', 38, 6), ('1.000001', 38, 6)),
        ('integer38', ('9' * 38, 38, 0), ('2', 38, 0)),
        ('multiply39', ('1.1234567895', 20, 10), ('1.0000000001', 20, 10)),
        ('native37', ('123456789', 18, 0), ('987654321', 18, 0)),
        ('low_scale', ('9' * 37 + '.9', 38, 1), ('0.5', 1, 1)),
    ]
    for name, x, y in pairs:
        for side, (a, b) in [('left', (x, y)), ('right', (y, x))]:
            left, right = decimal(*a), decimal(*b)
            negative = decimal('-' + a[0], *a[1:])
            for op, label in operators:
                add(f'{name}_{side}_{label}_literal', f'SELECT {left} {op} {right} AS r')
                add(f'{name}_{side}_{label}_column',
                    f'SELECT a {op} b AS r FROM VALUES (0,{left},{right}),'
                    f'(1,{negative},{right}),(2,NULL,{right}),(3,{left},NULL),(4,NULL,NULL) t(id,a,b) ORDER BY id')
    for name, peer in [('one', '1'), ('negative', '-12'), ('long', '9223372036854775807L'),
                       ('byte', 'CAST(2 AS TINYINT)'), ('short', 'CAST(2 AS SMALLINT)')]:
        for op, label in operators:
            for side, (left, right) in [('left', ('d', peer)), ('right', (peer, 'd'))]:
                add(f'peer_{name}_{side}_{label}',
                    f'SELECT {left} {op} {right} AS r FROM VALUES (0,CAST(2.25 AS DECIMAL(38,18))),'
                    '(1,CAST(-3.5 AS DECIMAL(38,18))),(2,CAST(NULL AS DECIMAL(38,18))) t(id,d) ORDER BY id')
    for op, label in operators:
        for side, (left, right) in [('left', ('a', 'b')), ('right', ('b', 'a'))]:
            for name, peer in [('null', 'NULL'), ('typed_null', 'CAST(NULL AS DECIMAL(2,1))'),
                               ('byte_column', 'CAST(2 AS TINYINT)'), ('int_column', '2')]:
                add(f'{name}_{side}_{label}', f'SELECT {left} {op} {right} AS r FROM VALUES '
                    f'(0,CAST(2.25 AS DECIMAL(8,2)),{peer}),(1,NULL,NULL) t(id,a,b) ORDER BY id')
        add(f'empty_{label}', f'SELECT a {op} b AS r FROM VALUES '
            "(CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)),CAST(0.125 AS DECIMAL(38,38))) t(a,b) WHERE false")
    for batch in [1, 2, 64]:
        for name, rows in [
            ('masked_zero', "(0,CAST(NULL AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))),(1,7,0.2),(2,9,NULL)"),
            ('live_zero', "(0,CAST(NULL AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))),(1,7,0)"),
            ('all_null', "(0,CAST(NULL AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))),(1,NULL,NULL)"),
        ]:
            add(f'{name}_batch{batch}', f'SELECT a % b AS r FROM VALUES {rows} t(id,a,b) ORDER BY id', batch_size=batch)
    add('literal_zero', 'SELECT d % 0 AS r FROM VALUES (CAST(2.25 AS DECIMAL(8,2))),(NULL) t(d)')
    add('mod_alias', 'SELECT mod(d,1) AS r FROM VALUES (CAST(2.25 AS DECIMAL(8,2))),(NULL) t(d)')
    add('wide_negative_divisor', "SELECT a % b AS r FROM VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)),CAST(-0.12345678901234567890123456789012345678 AS DECIMAL(38,38))) t(a,b)")
    for name, sql in [
        ('native_decimal', 'SELECT CAST(2.25 AS DECIMAL(8,2)) + 1 AS r'),
        ('integer', 'SELECT 7 + 2 AS r'),
        ('integer_overflow', 'SELECT 2147483647 + 1 AS r'),
        ('float', 'SELECT CAST(1.5 AS FLOAT) + CAST(2 AS FLOAT) AS r'),
        ('decimal_float', 'SELECT CAST(2.25 AS DECIMAL(8,2)) * CAST(1.5 AS FLOAT) AS r'),
        ('division', 'SELECT CAST(2.25 AS DECIMAL(38,18)) / 3 AS r'),
        ('integral_division', 'SELECT CAST(2.25 AS DECIMAL(38,18)) DIV 2 AS r'),
        ('bround', 'SELECT BROUND(CAST(2.25 AS DECIMAL(38,18)),1) AS r'),
        ('round', 'SELECT ROUND(CAST(2.25 AS DECIMAL(38,18)),1) AS r'),
    ]:
        add(name, sql, 'control')
    assert len({c['id'] for c in result}) == len(result)
    return result


def cause(actual, direct_modulo=False):
    if actual.get('condition'):
        return actual['condition']
    text = actual.get('error', '')
    if ('Cannot cast to Decimal128(' in text and 'Overflowing on' in text
            or ('too large to store in a Decimal128' in text or 'too small to store in a Decimal128' in text)):
        return 'NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION'
    if 'Arithmetic overflow' in text and '2147483647 + 1' in text:
        return 'ARITHMETIC_OVERFLOW'
    if '[ARITHMETIC_OVERFLOW]' in text:
        return 'ARITHMETIC_OVERFLOW'
    return modulo_cause(actual, direct_modulo)


def compare(reference, candidate, corpus=CASES):
    result = base_compare(reference, candidate, corpus)
    a, b = (json.loads(path.read_text())['results'] for path in [reference, candidate])
    for case, x, y, row in zip([c for c in load_cases(corpus) for _ in range(2)], a, b, result['cases'], strict=True):
        x, y = x['actual'], y['actual']
        if x['status'] != 'ok' and y['status'] != 'ok':
            direct_modulo = ' % ' in case['sql'] or 'mod(' in case['sql']
            cx, cy = cause(x, direct_modulo), cause(y, direct_modulo)
            row['differences'] = [] if cx is not None and cx == cy else ['error_cause']
            row['error_causes'] = [cx, cy]
    result['agreement'] = sum(not c['differences'] for c in result['cases'])
    return result


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
