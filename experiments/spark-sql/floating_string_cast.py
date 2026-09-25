"""Explicit FLOAT/DOUBLE string grammar, precision and conversion-mode checks."""
import argparse
from decimal import Decimal, localcontext
import json
from pathlib import Path
import struct

from decimal_division import capture
from division_cast_classification import compare

CASES = Path(__file__).with_name('floating-string-cast.jsonl')


def quoted(value):
    # Use Spark's backslash form so a quote byte reaches the numeric parser.
    # The separate doubled-quote SQL-literal discrepancy stays recorded.
    return 'CAST(NULL AS STRING)' if value is None else "'" + value.replace('\\', '\\\\').replace("'", "\\'") + "'"


def inputs():
    valid = ['0', '-0', '+0.0', '1', '1.', '.5', '-.0', '+1e2', '1E-3',
             'NaN', '+NaN', '-NaN', 'nan', 'NAN', 'Infinity', '-Infinity', '+iNf', '-iNfInItY',
             '0x1p0', '0X1.Ap+2', '0x.8p0', '-0X1.P1', '0x1f.p-4',
             '0e999999', '-0e-999999', '1e999999', '-1e-999999',
             '0x0p999999', '-0x0p-999999', '0x1p999999', '-0x1p-999999',
             '1.000000059604644775390626', '1.000000059604644775390624',
             '0x1.' + '0'*80 + '1p0', '0x1.' + 'f'*80 + 'p0']
    valid += [sign + base + suffix for sign in ['', '+', '-']
              for base in ['1.25', '.5', '1e2', '0x1.8p2', '0X1.P-1']
              for suffix in ['f', 'F', 'd', 'D']]
    valid += [chr(b) + '1.25f' + chr(b) for b in range(33)]
    boundaries = ['0x1.fffffep127', '0x1.ffffffp127', '0x1.fffffeffffffp127',
                  '0x1p-149', '0x1p-150', '0x1.000001p-150', '0x0.fffffep-126', '0x1p-126',
                  '0x1.fffffffffffffp1023', '0x1.fffffffffffff8p1023',
                  '0x1p-1074', '0x1p-1075', '0x1.0000000000001p-1075',
                  '0x0.fffffffffffffp-1022', '0x1p-1022']
    with localcontext() as ctx:
        ctx.prec = 1200
        for fmt, width, bits in [('f', 4, [0, 0x007fffff, 0x3f800000, 0x4b800000]),
                                  ('d', 8, [0, 0x000fffffffffffff, 0x3ff0000000000000, 0x4340000000000000])]:
            for word in bits:
                a, b = [Decimal.from_float(struct.unpack('!'+fmt, n.to_bytes(width, 'big'))[0]) for n in [word, word+1]]
                midpoint = (a+b)/2
                epsilon = (b-a)/(1 << 70)
                boundaries += [str(midpoint+offset) for offset in [-epsilon, Decimal(0), epsilon]]
    valid += [sign + text for text in boundaries for sign in ['', '-']]
    invalid = ['', ' ', '+', '-', '.', 'e1', '1e', '1e+', '1 2', '1_000',
               '1fF', 'F', 'NaNf', 'Infinityd', 'nanD', '+nan', '-NAN', '+nAn',
               '0x', '0x1', '0x1.', '0x.p0', '0x1p', '0x1p+', '0x1p1.0',
               '0x1p1e2', '0x1p0_1', '0xx1p0', '00x1p0', '0x1g.p0', '0x1p0dD',
               '0x1e2', '0xNaNp1', '0xInfp1', '0x1_p0', '1\x002', '1\n2',
               '\x7f1.25\x7f', '\u00a01.25\u00a0', '\u00851.25\u0085',
               '\u20031.25\u2003', '\ufeff1.25\ufeff', '\u0661.\u0662', '\uff11.\uff12', "1'", '1,25']
    return list(dict.fromkeys(valid)), invalid


def cases():
    valid, invalid = inputs()
    result = []
    for typ in ['FLOAT', 'DOUBLE']:
        for keyword in ['CAST', 'TRY_CAST']:
            prefix = f'{typ.lower()}_{keyword.lower()}'
            for batch in [1, 2, 64]:
                rows = ','.join(f'({i},{quoted(v)})' for i,v in enumerate([None, *valid]))
                result.append(dict(id=f'{prefix}_valid_batch{batch}', category='valid_column', batch_size=batch,
                    sql=f'SELECT {keyword}(v AS {typ}) AS r FROM VALUES {rows} t(id,v) ORDER BY id'))
            for category, values in [('invalid', invalid), ('literal', ['1.25f','0X1.Ap2','-0x1p-1075',
                    '1.000000059604644775390626','NaN','-NaN','+iNf',' \t1.5D\n'])]:
                for i, value in enumerate(values):
                    result.append(dict(id=f'{prefix}_{category}{i}', category=category, batch_size=2,
                        sql=f'SELECT {keyword}({quoted(value)} AS {typ}) AS r'))
            for name, source in [('empty', "FROM VALUES ('1.5f') t(v) WHERE false"),
                                 ('all_null', 'FROM VALUES (CAST(NULL AS STRING)),(NULL) t(v)'),
                                 ('runtime', "FROM (SELECT id,CASE WHEN id%2=0 THEN ' 0x1.8p2f ' ELSE '-0.0D' END AS v FROM range(5)) t ORDER BY id")]:
                result.append(dict(id=f'{prefix}_{name}',category='shape_control',batch_size=2,
                    sql=f'SELECT {keyword}(v AS {typ}) AS r {source}'))
            rows = ','.join(f'({i},{quoted(v)})' for i,v in enumerate([None,'1.25f',*invalid,'-0x1p-149']))
            result.append(dict(id=f'{prefix}_mixed',category='mixed_column',batch_size=2,
                sql=f'SELECT {keyword}(v AS {typ}) AS r FROM VALUES {rows} t(id,v) ORDER BY id'))
    assert len(result) == len({c['id'] for c in result})
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode',required=True)
    sub.add_parser('generate')
    sub.add_parser('spark').add_argument('out',type=Path)
    check = sub.add_parser('compare')
    for name in ['reference','candidate','out']: check.add_argument(name,type=Path)
    args = parser.parse_args()
    if args.mode == 'generate':
        CASES.write_text(''.join(json.dumps(c)+'\n' for c in cases()))
        print(len(cases()),'queries')
    elif args.mode == 'spark': capture(args.out,CASES)
    else:
        result = compare(args.reference,args.candidate,CASES)
        args.out.write_text(json.dumps(result,indent=2)+'\n')
        print(result['agreement'],'/',result['total'])
