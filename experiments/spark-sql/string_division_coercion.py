"""Pinned string-peer division coercion checks for issue 145."""
import argparse
import json
from pathlib import Path

from decimal_division import capture
from round_arguments import compare as base_compare, error_cause

ROOT = Path(__file__).resolve().parent
CASES = ROOT / 'string-division-coercion.jsonl'


def cases():
    result = []
    def add(id, sql, category='target'):
        result.append({'id': id, 'sql': sql, 'category': category, 'batch_size': 2})
    peers = [('byte', 'CAST(2 AS TINYINT)'), ('short', 'CAST(2 AS SMALLINT)'),
             ('int', '2'), ('long', '2L'), ('decimal', 'CAST(2 AS DECIMAL(10,2))'),
             ('float', 'CAST(2 AS FLOAT)'), ('double', 'CAST(2 AS DOUBLE)'),
             ('string', "'2'"), ('null', 'NULL'), ('typed_null', 'CAST(NULL AS INT)')]
    for name, peer in peers:
        for side, (a, b) in [('left', ("'7'", peer)), ('right', (peer, "'7'"))]:
            for op, token in [('divide', '/'), ('div', 'DIV')]:
                add(f'{name}_{side}_{op}_literal', f'SELECT {a} {token} {b} AS r')
                add(f'{name}_{side}_{op}_column',
                    f'SELECT a {token} b AS r FROM VALUES (0,{a},{b}),(1,NULL,NULL) t(id,a,b) ORDER BY id')
    strings = [('fraction', '7.5'), ('negative', '-7'), ('spaces', '  +7  '),
               ('tabs', '\t-7\n'), ('control', '\x017\x1f'), ('del', '\x7f7\x7f'),
               ('exponent', '7e1'), ('empty', ''), ('space_only', ' '), ('bad', 'bad'),
               ('too_large', '9223372036854775808'), ('too_small', '-9223372036854775809'),
               ('min', '-9223372036854775808'), ('max', '9223372036854775807'),
               ('above_double_integer', '9007199254740993'), ('float_overflow', '1e309'),
               ('nan', 'NaN'), ('infinity', '-Infinity')]
    for name, value in strings:
        string = "'" + value + "'"
        for side, (a, b) in [('left', (string, '2')), ('right', ('7', string))]:
            for op, token in [('divide', '/'), ('div', 'DIV')]:
                add(f'{name}_{side}_{op}', f'SELECT a {token} b AS r FROM VALUES ({a},{b}) t(a,b)')
        # A fractional peer selects DOUBLE even under ANSI; it must not borrow
        # the integral peer's BIGINT conversion.
        add(f'{name}_decimal_literal', f'SELECT CAST(2 AS DECIMAL(10,2)) / {string} AS r')
        add(f'{name}_decimal_column', f'SELECT a / CAST(2 AS DECIMAL(10,2)) AS r FROM VALUES ({string}) t(a)')
    for op, token in [('divide', '/'), ('div', 'DIV')]:
        for label, a, b in [('zero', "'7'", '0'), ('string_zero', '7', "'0'"),
                            ('bad_left_zero', "'bad'", '0'), ('both_bad', "'bad'", "'worse'"),
                            ('bad_left_null', "'bad'", 'CAST(NULL AS BIGINT)'),
                            ('null_left_bad', 'CAST(NULL AS BIGINT)', "'bad'")]:
            category = 'evaluation_reference' if label in ['bad_left_null', 'null_left_bad'] else 'target'
            add(f'{label}_{op}_literal', f'SELECT {a} {token} {b} AS r', category)
            add(f'{label}_{op}_column', f'SELECT a {token} b AS r FROM VALUES ({a},{b}) t(a,b)', category)
        for name, source in [('null_masks', "(0,CAST(NULL AS STRING),0),(1,'7',NULL),(2,'8',2)"),
                             ('live_zero', "(0,CAST(NULL AS STRING),0),(1,'7',0)"),
                             ('all_null', "(0,CAST(NULL AS STRING),0),(1,NULL,NULL)")]:
            add(f'{name}_{op}', f'SELECT a {token} b AS r FROM VALUES {source} t(id,a,b) ORDER BY id')
        add(f'empty_{op}', f"SELECT a {token} b AS r FROM VALUES ('7',2) t(a,b) WHERE false")
        add(f'dead_branch_{op}', f"SELECT CASE WHEN false THEN 'bad' {token} 2 ELSE 7 END AS r")
        add(f'array_scalar_{op}', f"SELECT a {token} 2 AS r FROM VALUES (0,'7'),(1,'-9'),(2,NULL) t(id,a) ORDER BY id")
        add(f'scalar_array_{op}', f"SELECT 7 {token} b AS r FROM VALUES (0,'2'),(1,'-3'),(2,NULL) t(id,b) ORDER BY id")
    for name, sql in [
        ('div_alias', "SELECT div(a,b) AS r FROM VALUES ('7',2),('-9',2),(NULL,NULL) t(a,b)"),
        ('divide_alias', "SELECT `/`(a,b) AS r FROM VALUES ('7',2),('-9',2),(NULL,NULL) t(a,b)"),
        ('decimal_seed', "SELECT CAST(2 AS DECIMAL(10,2)) / '3' AS r"),
        ('div_seed', "SELECT '7' DIV 2 AS r"),
        ('range_column', "SELECT CAST(id+7 AS STRING) DIV 2 AS r FROM range(4)"),
        ('decimal_range', "SELECT CAST(id+7 AS STRING) / CAST(2 AS DECIMAL(10,2)) AS r FROM range(4)"),
    ]:
        add(name, sql)
    for name, sql in [
        ('numeric_divide', 'SELECT 7 / 2 AS r'),
        ('numeric_div', 'SELECT 7L DIV 2L AS r'),
        ('overflow_div', 'SELECT CAST(-9223372036854775808 AS BIGINT) DIV -1L AS r'),
        ('decimal_cast', "SELECT CAST('7.5' AS DECIMAL(10,2)) / CAST(2 AS DECIMAL(10,2)) AS r"),
        ('explicit_double', "SELECT CAST('7.5' AS DOUBLE) / 2 AS r"),
        ('decimal_div', 'SELECT CAST(7 AS DECIMAL(10,2)) DIV CAST(2 AS DECIMAL(10,2)) AS r'),
        ('remainder', 'SELECT 7 % 2 AS r'),
        ('round', 'SELECT ROUND(CAST(1.255 AS DECIMAL(10,3)),2) AS r'),
    ]:
        add(name, sql, 'control')
    assert len({c['id'] for c in result}) == len(result)
    return result


def cause(actual):
    if actual.get('condition'):
        return actual['condition']
    text = actual.get('error', '')
    for condition in ['DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE',
                      'DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES']:
        if f'[{condition}]' in text:
            return condition
    if 'Arithmetic overflow: Overflow happened on: -9223372036854775808 / -1' in text:
        return 'ARITHMETIC_OVERFLOW'
    return error_cause(actual)


def compare(reference, candidate, corpus=CASES):
    check = base_compare(reference, candidate, corpus)
    a, b = (json.loads(p.read_text())['results'] for p in [reference, candidate])
    for x, y, row in zip(a, b, check['cases'], strict=True):
        x, y = x['actual'], y['actual']
        if x['status'] != 'ok' and y['status'] != 'ok':
            cx, cy = cause(x), cause(y)
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
