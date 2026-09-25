"""Numeric string CAST boundaries for the legacy-mode repair."""
import argparse
import json
from pathlib import Path

from decimal_division import capture
from division_cast_classification import compare

CASES = Path(__file__).with_name('legacy-numeric-cast.jsonl')


def quoted(value):
    return "CAST(NULL AS STRING)" if value is None else "'" + value.replace("'", "''") + "'"


cases = []
for typ, width in [('TINYINT',8), ('SMALLINT',16), ('INT',32), ('BIGINT',64),
                   ('FLOAT',32), ('DOUBLE',64)]:
    values = [None, '', 'bad', '0', '-0', '+7', ' 7 ', '\t-7\n', '\x1f7\x1f',
              '\x7f7\x7f', '1.25', '-1.25', '.9', '.', '+.', '1.', '1e2',
              '1.2.3', 'NaN', '+NaN', 'nAn', 'Infinity', '-inf', '1e400']
    if typ in ['FLOAT','DOUBLE']:
        values += ['1.25f', '1.25D', '0x1.8p2', '0x1p128', '-0.0', '1e-400',
                   '3.4028235e38', '1.7976931348623157e308', '9007199254740993']
    else:
        lo, hi = -(1 << (width-1)), (1 << (width-1))-1
        values += [str(lo), str(hi), str(lo-1), str(hi+1), str(lo)+'.9', str(hi)+'.9',
                   '9223372036854775808', '999999999999999999999999999999999999']
    values = list(dict.fromkeys(values))
    for i, value in enumerate(values):
        cases.append(dict(id=f'{typ.lower()}_{i}', target=typ, value=value, form='literal',
                          sql=f'SELECT CAST({quoted(value)} AS {typ}) AS r', batch_size=2))
    rows = ','.join(f'({i},{quoted(value)})' for i,value in enumerate(values))
    for cast in ['CAST', 'TRY_CAST']:
        cases.append(dict(id=f'{typ.lower()}_{cast.lower()}_column',target=typ,form='column',
                          sql=f'SELECT {cast}(v AS {typ}) AS r FROM VALUES {rows} t(id,v) ORDER BY id', batch_size=2))
        for value, name in [('bad','bad'), ('1.25','fraction'), (' 7 ','space'), (None,'null')]:
            cases.append(dict(id=f'{typ.lower()}_{cast.lower()}_{name}', target=typ, form='column_one',
                              sql=f'SELECT {cast}(v AS {typ}) AS r FROM VALUES ({quoted(value)}) t(v)',batch_size=1))

for typ in ['TINYINT','SMALLINT','INT','BIGINT','FLOAT','DOUBLE']:
    for name, sql in [
        ('empty', f"SELECT CAST(v AS {typ}) AS r FROM VALUES ('7') t(v) WHERE false"),
        ('all_null', f"SELECT CAST(v AS {typ}) AS r FROM VALUES (CAST(NULL AS STRING)),(NULL) t(v)"),
        ('range_column', f"SELECT CAST(CAST(id AS STRING) AS {typ}) AS r FROM range(5) ORDER BY id"),
        ('numeric_peer', f"SELECT CAST(v AS {typ}) AS r FROM VALUES (12.5),(NULL) t(v)"),
    ]:
        cases.append(dict(id=typ.lower()+'_'+name,target=typ,form=name,sql=sql,batch_size=2))
for name, sql in [
    ('decimal', "SELECT CAST(v AS DECIMAL(18,4)) AS r FROM VALUES (' +7.125 '),('bad'),(NULL) t(v)"),
    ('decimal_try', "SELECT TRY_CAST(v AS DECIMAL(18,4)) AS r FROM VALUES (' +7.125 '),('bad'),(NULL) t(v)"),
    ('integer_null_guard', "SELECT CAST(v AS BIGINT) DIV CAST(NULL AS BIGINT) AS r FROM VALUES ('bad'),('1.25'),(NULL) t(v)"),
    ('float_null_guard', "SELECT CAST(v AS DOUBLE) / CAST(NULL AS DOUBLE) AS r FROM VALUES ('bad'),('1.25'),(NULL) t(v)"),
]:
    cases.append(dict(id=name,target='control',form=name,sql=sql,batch_size=2))
assert len({c['id'] for c in cases}) == len(cases)

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='mode',required=True)
    sub.add_parser('generate')
    sub.add_parser('spark').add_argument('out',type=Path)
    check=sub.add_parser('compare')
    for name in ['reference','candidate','out']: check.add_argument(name,type=Path)
    args=parser.parse_args()
    if args.mode=='generate':
        CASES.write_text(''.join(json.dumps(c)+'\n' for c in cases))
        print(len(cases),'queries')
    elif args.mode=='spark': capture(args.out,CASES)
    else:
        result=compare(args.reference,args.candidate,CASES)
        args.out.write_text(json.dumps(result,indent=2)+'\n')
        print(result['agreement'],'/',result['total'])
