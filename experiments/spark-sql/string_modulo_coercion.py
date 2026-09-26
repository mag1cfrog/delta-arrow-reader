"""Generate and compare STRING-peer modulo observations for issue 247."""
import argparse
import json
from pathlib import Path
from decimal_division import capture
from division_cast_classification import compare as base_compare, cause as arithmetic_cause
from modulo_null_guard import cause as modulo_cause

CASES=Path(__file__).with_name('string-modulo-coercion.jsonl')

def compare(reference, candidate, corpus=CASES):
    result=base_compare(reference,candidate,corpus)
    a,b=(json.loads(path.read_text())['results'] for path in [reference,candidate])
    for left,right,row in zip(a,b,result['cases'],strict=True):
        if left['actual']['status']!='ok' and right['actual']['status']!='ok':
            # Recognize the existing explicit remainder condition. Keep Arrow's
            # generic DIVIDE_BY_ZERO distinct, without SQL-based relabeling.
            causes=[modulo_cause(x['actual'],False) or arithmetic_cause(x['actual']) for x in [left,right]]
            row['error_causes']=causes
            row['differences']=[] if causes[0] is not None and causes[0]==causes[1] else ['error_cause']
    result['agreement']=sum(not r['differences'] for r in result['cases'])
    return result

def cases():
    root=Path(__file__).resolve().parent
    record=json.loads((root/'dead-cast-results.json').read_text())
    old={c['id']:c for c in map(json.loads,(root/'dead-cast.jsonl').read_text().splitlines())}
    owned=sorted({id.rsplit('_',1)[0] for id,v in record['remaining'].items() if v['owner']==247})
    assert len(owned)==12
    cases=[old[id]|{'id':f'owned_{i}'} for i,id in enumerate(owned)]
    def add(id,sql,batch=2):cases.append({'id':id,'sql':sql,'batch_size':batch,'category':'string_modulo'})
    peers=[('byte','CAST(2 AS TINYINT)'),('short','CAST(2 AS SMALLINT)'),('int','2'),('long','2L'),('float','2F'),('double','2D'),('decimal','CAST(2 AS DECIMAL(10,2))'),('string',"'2'"),('null','NULL'),('typed_null','CAST(NULL AS BIGINT)')]
    for alias in ['percent','mod']:
     op=lambda a,b:f'{a} % {b}' if alias=='percent' else f'MOD({a},{b})'
     for name,peer in peers:
      for side,(a,b) in [('left',("'7'",peer)),('right',(peer,"'-7'"))]:
       add(f'{alias}_{name}_{side}_literal',f'SELECT {op(a,b)} AS r')
       add(f'{alias}_{name}_{side}_column',f'SELECT {op("a","b")} AS r FROM VALUES (0,{a},{b}),(1,NULL,NULL) t(k,a,b) ORDER BY k')
     for typ in ['BIGINT','DOUBLE','DECIMAL(10,2)']:
      for name,a,b in [('bad_null',"'bad'",f'CAST(NULL AS {typ})'),('null_bad',f'CAST(NULL AS {typ})',"'bad'"),('bad_zero',"'bad'",f'CAST(0 AS {typ})'),('valid_zero',"'7'",f'CAST(0 AS {typ})'),('zero_string',f'CAST(7 AS {typ})',"'-0'"),('null_zero','CAST(NULL AS STRING)',f'CAST(0 AS {typ})')]:
       add(f'{alias}_{typ}_{name}_literal',f'SELECT {op(a,b)} AS r')
       add(f'{alias}_{typ}_{name}_column',f'SELECT {op("a","b")} AS r FROM VALUES ({a},{b}) t(a,b)')
      for batch in [1,2,64]:
       for name,rows in [('safe',f"(0,'bad',CAST(NULL AS {typ})),(1,'-7',CAST(2 AS {typ})),(2,NULL,0)"),('live',f"(0,'bad',CAST(NULL AS {typ})),(1,'bad',CAST(2 AS {typ}))"),('all_null',f"(0,'bad',CAST(NULL AS {typ})),(1,'worse',NULL)")]:
        add(f'{alias}_{typ}_{name}_batch{batch}',f'SELECT {op("a","b")} AS r FROM VALUES {rows} t(k,a,b) ORDER BY k',batch)
     for name,source in [('empty',"FROM VALUES ('bad',2L) t(a,b) WHERE false"),('limit0',"FROM VALUES ('bad',2L) t(a,b) LIMIT 0")]:
      add(f'{alias}_{name}',f'SELECT {op("a","b")} AS r {source}')
     bad=op("'bad'","2L")
     add(f'{alias}_dead_case',f'SELECT CASE WHEN false THEN {bad} ELSE 7L END AS r')
     for batch in [1,2,64]:
      add(f'{alias}_range{batch}',f'SELECT {op("CAST(id-7 AS STRING)","3L")} AS r FROM range(16) ORDER BY id',batch)
      add(f'{alias}_array_scalar{batch}',f'SELECT {op("a","2L")} AS r FROM VALUES (0,\'7\'),(1,\'-9\'),(2,NULL) t(k,a) ORDER BY k',batch)
      add(f'{alias}_scalar_array{batch}',f'SELECT {op("7L","b")} AS r FROM VALUES (0,\'2\'),(1,\'-3\'),(2,NULL) t(k,b) ORDER BY k',batch)
    strings=[('fraction','7.5'),('negative','-7'),('spaces','  +7  '),('tabs','\t-7\n'),('control','\x017\x1f'),('del','\x7f7\x7f'),('exponent','7e1'),('empty',''),('spaces_only',' '),('bad','bad'),('too_large','9223372036854775808'),('too_small','-9223372036854775809'),('min','-9223372036854775808'),('max','9223372036854775807'),('above_double_integer','9007199254740993'),('float_overflow','1e309'),('nan','NaN'),('infinity','-Infinity')]
    for name,value in strings:
     for peer in ['2L','2D']:
      for side in ['left','right']:
       a,b=("'"+value+"'",peer) if side=='left' else (peer,"'"+value+"'")
       add(f'grammar_{name}_{peer}_{side}',f'SELECT a % b AS r FROM VALUES ({a},{b}) t(a,b)')
    for name,sql in [('numeric_long','SELECT a % b AS r FROM VALUES (7L,2L),(-7L,-2L),(NULL,0L) t(a,b)'),('numeric_float','SELECT 7D % 2D AS r'),('numeric_decimal','SELECT CAST(7 AS DECIMAL(10,2)) % CAST(2 AS DECIMAL(10,2)) AS r'),('string_divide',"SELECT '7' / 2L AS r"),('string_div',"SELECT '7' DIV 2L AS r"),('string_both',"SELECT '7' / '2' AS r"),('explicit_cast',"SELECT CAST('7' AS BIGINT) % 2L AS r"),('strict_cast',"SELECT CAST('bad' AS BIGINT) AS r"),('overflow_mod',"SELECT '-9223372036854775808' % -1L AS r")]:add('control_'+name,sql)
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
