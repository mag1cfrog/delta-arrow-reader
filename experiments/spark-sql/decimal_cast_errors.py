"""Generate and compare Decimal CAST reachability observations for issue 246."""
import argparse
import json
from pathlib import Path
from decimal_division import capture
from division_cast_classification import compare

CASES=Path(__file__).with_name('decimal-cast-errors.jsonl')

def cases():
    root=Path(__file__).resolve().parent
    record=json.loads((root/'dead-cast-results.json').read_text())
    old={c['id']:c for c in map(json.loads,(root/'dead-cast.jsonl').read_text().splitlines())}
    owned=[id.removesuffix('_true') for id,value in record['remaining'].items() if value['owner']==246]
    assert len(owned)==6
    cases=[old[id]|{'id':f'owned_{i}'} for i,id in enumerate(owned)]
    def add(id,sql,batch=2):cases.append({'id':id,'sql':sql,'batch_size':batch,'category':'decimal_cast'})
    for typ in ['DECIMAL(10,2)','DECIMAL(5,0)','DECIMAL(38,18)']:
     tag=typ.removeprefix('DECIMAL(').removesuffix(')').replace(',','_')
     for cast in ['CAST','TRY_CAST']:
      fn=lambda value:f'{cast}({value} AS {typ})'
      for name,value in [('valid',"'1.235'"),('negative',"'-1.235'"),('bad',"'bad'"),('nan',"'NaN'"),('null','CAST(NULL AS STRING)'),('overflow',"'99999999.995'"),('wide',"'1e40'"),('tiny',"'1e-100'")]:
       add(f'literal_{tag}_{cast}_{name}',f'SELECT {fn(value)} AS r')
      for batch in [1,2,64]:
       for kind,rows in [('valid',"(0,'1.235'),(1,'-1.235'),(2,NULL),(3,'0')"),('invalid',"(0,'1.235'),(1,'bad'),(2,NULL),(3,'99999999.995')")]:
        add(f'column_{tag}_{cast}_{kind}_{batch}',f'SELECT {fn("v")} AS r FROM VALUES {rows} t(k,v) ORDER BY k',batch)
    for cast in ['CAST','TRY_CAST']:
     expr=f"{cast}('bad' AS DECIMAL(10,2))"
     for source,sql in [('scalar',''),('values',' FROM VALUES (0) t(k)'),('range',' FROM range(3)')]:
      for wrapper,suffix in [('live',''),('limit0',' LIMIT 0'),('false',' WHERE false'),('false_limit0',' WHERE false LIMIT 0')]:
       add(f'context_{cast}_{source}_{wrapper}',f'SELECT {expr} AS r{sql}{suffix}')
      add(f'context_{cast}_{source}_unused',f'SELECT 7 AS r FROM (SELECT {expr} AS unused{sql}) t')
     for name,value in [('dead',f'CASE WHEN false THEN {expr} ELSE 7 END'),('live',f'CASE WHEN true THEN {expr} ELSE 7 END'),('coalesce_dead',f'COALESCE(CAST(7 AS DECIMAL(10,2)),{expr})'),('coalesce_live',f'COALESCE(CAST(NULL AS DECIMAL(10,2)),{expr})')]:
      add(f'conditional_{cast}_{name}',f'SELECT {value} AS r')
     for name,rows in [('dead','(0),(0)'),('mixed','(0),(1)'),('live','(1),(1)')]:
      add(f'conditional_{cast}_column_{name}',f'SELECT CASE WHEN k=0 THEN CAST(7 AS DECIMAL(10,2)) ELSE {expr} END AS r FROM VALUES {rows} t(k)')
     for name,predicate in [('dead','id>=0'),('live','id=0')]:
      add(f'conditional_{cast}_range_{name}',f'SELECT CASE WHEN {predicate} THEN CAST(7 AS DECIMAL(10,2)) ELSE {expr} END AS r FROM range(3)')
     for op in ['/','DIV','%','+','-','*']:
      for name,left,right in [('left_null','CAST(NULL AS DECIMAL(10,2))',expr),('right_null',expr,'CAST(NULL AS DECIMAL(10,2))')]:
       add(f'null_{cast}_{op}_{name}',f'SELECT {left} {op} {right} AS r')
      for name,left,right,rows in [('left_null','n',f'{cast}(v AS DECIMAL(10,2))',"('bad',CAST(NULL AS DECIMAL(10,2)))"),('right_null',f'{cast}(v AS DECIMAL(10,2))','n',"('bad',CAST(NULL AS DECIMAL(10,2)))"),('mixed_guard',f'{cast}(v AS DECIMAL(10,2))','n',"('bad',CAST(NULL AS DECIMAL(10,2))),('7',2)")]:
       add(f'null_column_{cast}_{op}_{name}',f'SELECT {left} {op} {right} AS r FROM VALUES {rows} t(v,n)')
    for name,sql in [
     ('implicit_literal_valid',"SELECT DECIMAL('1.25') AS r"),
     ('implicit_literal_bad',"SELECT DECIMAL('bad') AS r"),
     ('implicit_column',"SELECT DECIMAL(v) AS r FROM VALUES ('bad') t(v)"),
     ('nested_valid',"SELECT CAST(CAST('1.235' AS DECIMAL(10,2)) AS DECIMAL(5,1)) AS r"),
     ('nested_invalid',"SELECT CAST(CAST('bad' AS DECIMAL(10,2)) AS DECIMAL(5,1)) AS r"),
     ('empty_local',"SELECT CAST('bad' AS DECIMAL(10,2)) AS r FROM (SELECT 1 WHERE false) t"),
     ('early_zero_control',"SELECT 7 DIV 0 AS r FROM VALUES (0) t(k) LIMIT 0"),
     ('native_control','SELECT id+1 AS r FROM range(3)'),
    ]:add(name,sql)
    for op in ['/','DIV','%']:
     for shape,expr in [('null_zero','CAST(NULL AS DECIMAL(10,2)) OP CAST(0 AS DECIMAL(10,2))'),('bad_zero',"CAST('bad' AS DECIMAL(10,2)) OP CAST(0 AS DECIMAL(10,2))"),('valid_zero',"CAST('7' AS DECIMAL(10,2)) OP CAST(0 AS DECIMAL(10,2))")]:
      add(f'zero_literal_{op}_{shape}','SELECT '+expr.replace('OP',op)+' AS r')
     for name,rows in [('null_zero',"(CAST(NULL AS STRING),CAST(0 AS DECIMAL(10,2)))"),('bad_zero',"('bad',CAST(0 AS DECIMAL(10,2)))"),('valid_zero',"('7',CAST(0 AS DECIMAL(10,2)))")]:
      add(f'zero_column_{op}_{name}',f'SELECT CAST(v AS DECIMAL(10,2)) {op} n AS r FROM VALUES {rows} t(v,n)')
     add(f'constant_bad_nullable_{op}',f"SELECT CAST('bad' AS DECIMAL(10,2)) {op} n AS r FROM (SELECT CASE WHEN id%2=0 THEN CAST(NULL AS DECIMAL(10,2)) ELSE 2 END AS n FROM range(1)) t")
    assert len(cases)==len({c['id'] for c in cases})
    return cases

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='mode',required=True)
    sub.add_parser('generate')
    sub.add_parser('spark').add_argument('out',type=Path)
    check=sub.add_parser('compare')
    for name in ['reference','candidate','out']:check.add_argument(name,type=Path)
    args=parser.parse_args()
    if args.mode=='generate':
        CASES.write_text(''.join(json.dumps(c)+'\n' for c in cases()))
        print(len(cases()),'queries')
    elif args.mode=='spark':capture(args.out,CASES)
    else:
        result=compare(args.reference,args.candidate,CASES)
        args.out.write_text(json.dumps(result,indent=2)+'\n')
        print(result['agreement'],'/',result['total'])
