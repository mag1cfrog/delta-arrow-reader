"""Check scalar CAST reachability against pinned Spark observations."""
import argparse
import json
from pathlib import Path
from decimal_division import capture
from division_cast_classification import compare

CASES = Path(__file__).with_name('dead-cast.jsonl')

def focused_cases():
    result=[]
    def add(id,sql,category):result.append(dict(id=id,sql=sql,category=category,batch_size=2))
    for typ in ['INT','DOUBLE','DECIMAL(10,2)']:
     tag=typ.split('(')[0].lower();cast=f"CAST('bad' AS {typ})"
     for source,sql in [('scalar',''),('values',' FROM VALUES (0) t(k)'),('range',' FROM range(3)')]:
      for wrapper,suffix in [('live',''),('limit0',' LIMIT 0'),('false',' WHERE false'),('false_limit0',' WHERE false LIMIT 0')]:
       add(f'{tag}_{source}_{wrapper}',f'SELECT {cast} AS r{sql}{suffix}','cast_reachability')
      add(f'{tag}_{source}_unused',f'SELECT 7 AS r FROM (SELECT {cast} AS unused{sql}) t','cast_reachability')
     for branch,expr in [('dead',f'CASE WHEN false THEN {cast} ELSE 7 END'),('live',f'CASE WHEN true THEN {cast} ELSE 7 END'),('coalesce_dead',f'COALESCE(CAST(7 AS {typ}),{cast})'),('coalesce_live',f'COALESCE(CAST(NULL AS {typ}),{cast})')]:
      add(f'{tag}_{branch}',f'SELECT {expr} AS r','conditional')
     for name,rows in [('dead','(0),(0)'),('mixed','(0),(1)'),('live','(1),(1)')]:
      add(f'{tag}_column_case_{name}',f'SELECT CASE WHEN k=0 THEN CAST(7 AS {typ}) ELSE {cast} END AS r FROM VALUES {rows} t(k)','conditional')
    for op in ['/','DIV','%']:
     for shape,a,b in [('bad_null',"'bad'",'CAST(NULL AS BIGINT)'),('null_bad','CAST(NULL AS BIGINT)',"'bad'"),('null_zero','CAST(NULL AS BIGINT)','0L'),('bad_zero',"'bad'",'0L'),('valid_null',"'7'",'CAST(NULL AS BIGINT)')]:
      for form in ['literal','column']:
       sql=f'SELECT {a} {op} {b} AS r' if form=='literal' else f'SELECT a {op} b AS r FROM VALUES ({a},{b}) t(a,b)'
       add(f'{op}_{shape}_{form}',sql,'operand_order')
     for name,rows in [('all_null',"('bad',CAST(NULL AS BIGINT)),('worse',NULL)"),('mixed_safe',"('bad',CAST(NULL AS BIGINT)),('7',2L)"),('mixed_live',"('bad',CAST(NULL AS BIGINT)),('bad',2L)"),('reverse_safe',"(CAST(NULL AS BIGINT),'bad'),(7L,'2')")]:
      add(f'{op}_{name}',f'SELECT a {op} b AS r FROM VALUES {rows} t(a,b)','operand_order')
    for name,sql in [
     ('live_zero','SELECT 7 DIV 0 AS r'),('unused_local_zero','SELECT 7 AS r FROM (SELECT 7 DIV 0 AS q FROM VALUES (0) t(k)) t'),
     ('local_zero_limit0','SELECT 7 DIV 0 AS r FROM VALUES (0) t(k) LIMIT 0'),
     ('range_zero_limit0','SELECT 7 DIV 0 AS r FROM range(3) LIMIT 0'),
     ('range_zero_false','SELECT 7 DIV 0 AS r FROM range(3) WHERE false'),
     ('local_null_bad_divisor',"SELECT CAST(NULL AS BIGINT) DIV CAST(v AS BIGINT) AS r FROM VALUES ('bad') t(v)"),
     ('local_bad_null_divisor',"SELECT CAST(v AS BIGINT) DIV n AS r FROM VALUES ('bad',CAST(NULL AS BIGINT)) t(v,n)"),
     ('bad_type_dead','SELECT CASE WHEN false THEN 1D DIV 2D ELSE 7 END AS r'),
     ('bad_type_limit0','SELECT 1D DIV 2D AS r FROM range(3) LIMIT 0'),
     ('bad_column_dead','SELECT CASE WHEN false THEN missing ELSE 7 END AS r'),
     ('nonthrowing_native','SELECT (id+1)*2 AS r FROM range(3)'),
     ('try_live',"SELECT TRY_CAST('bad' AS BIGINT) AS r"),
     ('local_empty_input',"SELECT CAST('bad' AS DOUBLE) AS r FROM (SELECT 1 AS k WHERE false) t"),
     ('constant_null_guard',"SELECT CAST(NULL AS BIGINT) DIV CAST('bad' AS BIGINT) AS r"),
     ('range_case_dead',"SELECT CASE WHEN id>=0 THEN 7D ELSE CAST('bad' AS DOUBLE) END AS r FROM range(3)"),
     ('range_case_live',"SELECT CASE WHEN id=0 THEN 7D ELSE CAST('bad' AS DOUBLE) END AS r FROM range(3)"),
    ]:add(name,sql,'retained_control')
    return result

def guard_cases():
    queries=[
     ('and_bad_left',"SELECT CAST('bad' AS DOUBLE)>0D AND false AS r"),
     ('and_bad_right',"SELECT false AND CAST('bad' AS DOUBLE)>0D AS r"),
     ('or_bad_left',"SELECT CAST('bad' AS DOUBLE)>0D OR true AS r"),
     ('or_bad_right',"SELECT true OR CAST('bad' AS DOUBLE)>0D AS r"),
     ('int_and_bad_left',"SELECT CAST('bad' AS INT)>0 AND false AS r"),
     ('int_and_bad_right',"SELECT false AND CAST('bad' AS INT)>0 AS r"),
     ('int_or_bad_left',"SELECT CAST('bad' AS INT)>0 OR true AS r"),
     ('int_or_bad_right',"SELECT true OR CAST('bad' AS INT)>0 AS r"),
     ('literal_bad_div_null_column',"SELECT CAST('bad' AS BIGINT) DIV (CASE WHEN id%2=0 THEN CAST(NULL AS BIGINT) ELSE 2L END) AS r FROM range(1)"),
     ('literal_bad_float_null_column',"SELECT CAST('bad' AS DOUBLE) / (CASE WHEN id%2=0 THEN CAST(NULL AS BIGINT) ELSE 2L END) AS r FROM range(1)"),
     ('literal_bad_div_null_alias',"SELECT CAST('bad' AS BIGINT) DIV n AS r FROM (SELECT CASE WHEN id%2=0 THEN CAST(NULL AS BIGINT) ELSE 2L END AS n FROM range(1)) t"),
     ('literal_bad_float_null_alias',"SELECT CAST('bad' AS DOUBLE) / n AS r FROM (SELECT CASE WHEN id%2=0 THEN CAST(NULL AS BIGINT) ELSE 2L END AS n FROM range(1)) t"),
     ('range_guarded_int',"SELECT CAST(v AS BIGINT) DIV n AS r FROM (SELECT id,CASE WHEN id%2=0 THEN 'bad' ELSE '7' END AS v,CASE WHEN id%2=0 THEN CAST(NULL AS BIGINT) ELSE 2L END AS n FROM range(5)) t ORDER BY id"),
     ('range_guarded_float',"SELECT CAST(v AS DOUBLE) / n AS r FROM (SELECT id,CASE WHEN id%2=0 THEN 'bad' ELSE '7' END AS v,CASE WHEN id%2=0 THEN CAST(NULL AS BIGINT) ELSE 2L END AS n FROM range(5)) t ORDER BY id"),
     ('string_peer_nulls',"SELECT a/b AS r FROM VALUES ('bad',CAST(NULL AS STRING)),('7','2') t(a,b)"),
     ('string_div_peer_nulls',"SELECT a DIV b AS r FROM VALUES ('bad',CAST(NULL AS STRING)),('7','2') t(a,b)"),
    ]
    return [dict(id=id,sql=sql,batch_size=2,category='guard_order') for id,sql in queries]

def cases():
    source = json.loads(Path(__file__).with_name('division-cast-classification-results.json').read_text())
    owned = [row for row in source['observations'] if row['root'] == 'dead_cast']
    result = [dict(id=f'owned_{i}',sql=row['sql'],batch_size=row['batch_size'],category=row['kind'])
              for i,row in enumerate(owned)]
    for prefix,group in [('focused',focused_cases()),('guard',guard_cases())]:
        result += [case | {'id':prefix+'_'+case['id']} for case in group]
    assert len(owned) == 33 and len(result) == len({case['id'] for case in result}) == 173
    return result

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='mode',required=True)
    sub.add_parser('generate')
    sub.add_parser('spark').add_argument('out',type=Path)
    check=sub.add_parser('compare')
    for name in ['reference','candidate','out']:check.add_argument(name,type=Path)
    args=parser.parse_args()
    if args.mode=='generate':
        CASES.write_text(''.join(json.dumps(case)+'\n' for case in cases()))
        print(len(cases()),'queries')
    elif args.mode=='spark':capture(args.out,CASES)
    else:
        result=compare(args.reference,args.candidate,CASES)
        args.out.write_text(json.dumps(result,indent=2)+'\n')
        print(result['agreement'],'/',result['total'])
