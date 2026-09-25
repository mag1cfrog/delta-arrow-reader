"""NULL-aware remainder zero guards for issue 147."""
import argparse
import json
from pathlib import Path
from decimal_division import capture, load_cases
from round_arguments import compare as base_compare, error_cause

ROOT=Path(__file__).resolve().parent
CASES=ROOT/'modulo-null-guard.jsonl'


def cases():
    result=[]
    def add(id,sql,category='target',direct=True):
        result.append({'id':id,'sql':sql,'category':category,'direct_modulo':direct,'batch_size':2})
    pairs=[('INT','INT'),('BIGINT','BIGINT'),('FLOAT','FLOAT'),('DOUBLE','DOUBLE'),
           ('DECIMAL(8,2)','DECIMAL(8,2)'),('DECIMAL(38,18)','DECIMAL(38,18)'),
           ('FLOAT','INT'),('INT','FLOAT'),('FLOAT','DECIMAL(8,2)'),
           ('DECIMAL(8,2)','FLOAT'),('DOUBLE','DECIMAL(8,2)')]
    patterns={'null_zero':[('NULL','0'),('NULL','-0.0'),('5','2'),('NULL','NULL')],
              'null_divisor':[('5','NULL'),('NULL','2'),('-5','2'),('5','-2')],
              'live_zero':[('NULL','0'),('5','-0.0')],
              'all_null':[('NULL','0'),('NULL','NULL')],
              'ordinary':[('5','2'),('-5','2'),('0','2'),('5','-2')]}
    for left,right in pairs:
        key=left+'_'+right
        for label,rows in patterns.items():
            source=','.join(f'({i},CAST({a} AS {left}),CAST({b} AS {right}))' for i,(a,b) in enumerate(rows))
            for alias,expr in [('operator','a % b'),('function','mod(a,b)')]:
                add(f'{key}_{label}_{alias}',f'SELECT {expr} AS r FROM VALUES {source} t(id,a,b) ORDER BY id')
        for label,a,b in [('null_zero','NULL','0'),('null_divisor','5','NULL'),('live_zero','5','0'),('ordinary','5','2')]:
            # Keep literal and narrow-width type observations separate from mask targets.
            category='coercion_reference' if {left,right}=={'FLOAT','INT'} else 'target'
            add(f'{key}_literal_{label}',f'SELECT CAST({a} AS {left}) % CAST({b} AS {right}) AS r',category)
        add(f'{key}_array_scalar_zero',f'SELECT a % CAST(0 AS {right}) AS r FROM VALUES (CAST(NULL AS {left})),(NULL) t(a)')
        add(f'{key}_scalar_array_zero',f'SELECT CAST(NULL AS {left}) % b AS r FROM VALUES (CAST(0 AS {right})),(CAST(-0.0 AS {right})) t(b)')
        add(f'{key}_empty',f'SELECT a % b AS r FROM VALUES (CAST(5 AS {left}),CAST(0 AS {right})) t(a,b) WHERE false')
    for typ in ['FLOAT','DOUBLE']:
        for label,a,b in [('negative_zero',"'-0.0'",'2'),('nan',"'NaN'",'2'),('infinity',"'Infinity'",'2'),('zero_nan','0',"'NaN'"),('finite_infinity','5',"'Infinity'"),('nan_zero',"'NaN'",'0')]:
            add(f'{typ}_{label}',f'SELECT CAST({a} AS {typ}) % CAST({b} AS {typ}) AS r')
    for typ in ['TINYINT','SMALLINT']:
        for label,rows in patterns.items():
            source=','.join(f'(CAST({a} AS {typ}),CAST({b} AS {typ}))' for a,b in rows)
            add(f'{typ}_{label}',f'SELECT a % b AS r FROM VALUES {source} t(a,b)','coercion_reference')
    for id,sql in [
        ('dead_branch','SELECT CASE WHEN false THEN CAST(5 AS DOUBLE) % 0 ELSE CAST(2 AS DOUBLE) END AS r'),
        ('dead_column_branch','SELECT CASE WHEN id=0 THEN a % b ELSE CAST(2 AS DOUBLE) END AS r FROM VALUES (1,CAST(5 AS DOUBLE),CAST(0 AS DOUBLE)) t(id,a,b)'),
        ('untyped_null','SELECT NULL % 0 AS r'),
        ('left_cast_zero',"SELECT CAST('bad' AS DOUBLE) % CAST(0 AS DOUBLE) AS r"),
        ('column_cast_zero',"SELECT CAST(a AS DOUBLE) % CAST(0 AS DOUBLE) AS r FROM VALUES ('bad') t(a)"),
    ]:add(id,sql)
    for id,sql in [
        ('dead_left_null_right',"SELECT CAST('bad' AS DOUBLE) % CAST(NULL AS DOUBLE) AS r"),
        ('null_left_bad_right',"SELECT CAST(NULL AS DOUBLE) % CAST('bad' AS DOUBLE) AS r"),
        ('column_null_left_bad_right',"SELECT CAST(NULL AS DOUBLE) % CAST(b AS DOUBLE) AS r FROM VALUES ('bad') t(b)"),
        ('nested_division','SELECT (1 / 0) % 2 AS r'),
    ]:add(id,sql,'evaluation_reference',False)
    for id,sql in [('divide','SELECT a/b AS r FROM VALUES (CAST(NULL AS DOUBLE),CAST(0 AS DOUBLE)),(5.0,2.0) t(a,b)'),
                   ('round','SELECT ROUND(CAST(2.675 AS FLOAT),2) AS r'),
                   ('div','SELECT 7 DIV 2 AS r')]:add('control_'+id,sql,'control',False)
    return result


def cause(actual,direct_modulo):
    if actual.get('condition'):
        return actual['condition']
    text=actual.get('error','')
    if '[REMAINDER_BY_ZERO]' in text or 'Remainder by zero' in text:
        return 'REMAINDER_BY_ZERO'
    # Arrow's integer/Decimal remainder uses DivideByZero. Only direct
    # remainder cases may normalize it; nested division keeps its own cause.
    if direct_modulo and 'Arrow error: Divide by zero error' in text:
        return 'REMAINDER_BY_ZERO'
    return error_cause(actual)


def compare(reference,candidate,corpus=CASES):
    check=base_compare(reference,candidate,corpus)
    a,b=(json.loads(p.read_text())['results'] for p in [reference,candidate])
    for case,x,y,row in zip([c for c in load_cases(corpus) for _ in range(2)],a,b,check['cases'],strict=True):
        x,y=x['actual'],y['actual']
        if x['status']!='ok' and y['status']!='ok':
            cx,cy=cause(x,case.get('direct_modulo',True)),cause(y,case.get('direct_modulo',True))
            row['differences']=[] if cx is not None and cx==cy else ['error_cause']
            row['error_causes']=[cx,cy]
    check['agreement']=sum(not r['differences'] for r in check['cases'])
    return check


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='mode',required=True)
    sub.add_parser('generate')
    sub.add_parser('spark').add_argument('out',type=Path)
    check=sub.add_parser('compare')
    for name in ['reference','candidate','report']:check.add_argument(name,type=Path)
    args=parser.parse_args()
    if args.mode=='generate':CASES.write_text(''.join(json.dumps(c)+'\n' for c in cases()))
    elif args.mode=='spark':capture(args.out,CASES)
    else:
        result=compare(args.reference,args.candidate)
        args.report.write_text(json.dumps(result,indent=2)+'\n')
        print(result['agreement'],'/',result['total'])
        return int(result['agreement']!=result['total'])
    return 0


if __name__=='__main__':raise SystemExit(main())
