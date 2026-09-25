"""Strict/TRY integer-string CAST whitespace and grammar boundaries."""
import argparse
import json
from pathlib import Path

from decimal_division import capture
from division_cast_classification import compare

CASES = Path(__file__).with_name('strict-integer-cast.jsonl')


def quoted(value):
    return 'CAST(NULL AS STRING)' if value is None else "'" + value.replace("'", "''") + "'"


def cases():
    result = []
    for typ, width in [('TINYINT', 8), ('SMALLINT', 16), ('INT', 32), ('BIGINT', 64)]:
        lo, hi = -(1 << (width - 1)), (1 << (width - 1)) - 1
        valid = [None, '7', '-0', '+7', str(lo), str(hi)]
        valid += [chr(byte) + '7' + chr(byte) for byte in [*range(33), 127]]
        valid += [' \t' + str(lo) + '\x7f', '\x00+' + str(hi) + '\n']
        invalid = ['', ' \t\x00\x7f', 'bad', '+', '-', '+ 7', '1 2', '1\x7f2',
                   '1.25', '-1.25', '.', '1e2', '1_000', str(lo - 1), str(hi + 1),
                   ' ' + str(lo - 1) + ' ', ' ' + str(hi + 1) + ' ',
                   '\u00a07\u00a0', '\u00857\u0085', '\u20037\u2003', '\ufeff7\ufeff',
                   '\u0667', '\uff17', '!7!', '"7"']
        for keyword in ['CAST', 'TRY_CAST']:
            for batch in [1, 2, 64]:
                rows = ','.join(f'({i},{quoted(v)})' for i, v in enumerate(valid))
                result.append(dict(id=f'{typ.lower()}_{keyword.lower()}_valid_batch{batch}',
                    category='allowed_trim', target=typ, batch_size=batch,
                    sql=f'SELECT {keyword}(v AS {typ}) AS r FROM VALUES {rows} t(id,v) ORDER BY id'))
            for i, value in enumerate(invalid):
                result.append(dict(id=f'{typ.lower()}_{keyword.lower()}_invalid{i}',
                    category='grammar_control', target=typ, batch_size=2,
                    sql=f'SELECT {keyword}({quoted(value)} AS {typ}) AS r'))
            for name, sql in [
                ('empty', f"SELECT {keyword}(v AS {typ}) AS r FROM VALUES (' 7 ') t(v) WHERE false"),
                ('all_null', f'SELECT {keyword}(v AS {typ}) AS r FROM VALUES (CAST(NULL AS STRING)),(NULL) t(v)'),
                ('runtime_column', f"SELECT {keyword}(CASE WHEN id % 2 = 0 THEN ' 7 ' ELSE '\t-7\x7f' END AS {typ}) AS r FROM range(5) ORDER BY id"),
            ]:
                result.append(dict(id=f'{typ.lower()}_{keyword.lower()}_{name}',
                    category='shape_control', target=typ, batch_size=2, sql=sql))
    assert len({c['id'] for c in result}) == len(result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    sub.add_parser('generate')
    sub.add_parser('spark').add_argument('out', type=Path)
    check = sub.add_parser('compare')
    for name in ['reference', 'candidate', 'out']:
        check.add_argument(name, type=Path)
    args = parser.parse_args()
    if args.mode == 'generate':
        CASES.write_text(''.join(json.dumps(c) + '\n' for c in cases()))
        print(len(cases()), 'queries')
    elif args.mode == 'spark':
        capture(args.out, CASES)
    else:
        result = compare(args.reference, args.candidate, CASES)
        args.out.write_text(json.dumps(result, indent=2) + '\n')
        print(result['agreement'], '/', result['total'])
