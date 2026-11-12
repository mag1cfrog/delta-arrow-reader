"""Capture nested live scalar values, executed types, and cardinality checks."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from capture_floating_group_keys_spark import encode


def main():
    output = Path(sys.argv[1])
    assert not output.exists()
    os.environ['PYSPARK_PYTHON'] = sys.executable
    spark = (SparkSession.builder.master('local[1]').appName('dar-nested-correlations')
             .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '1')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    try:
        assert spark.version == '4.2.0'
        spark.sparkContext.setLogLevel('ERROR')
        integer = types.IntegerType()
        schema = types.StructType([types.StructField('k', integer), types.StructField('v', integer)])
        outer = types.StructType([types.StructField('id', integer, False), types.StructField('k', integer)])
        definitions = [
            ('ordinary', schema, [(1, 10), (1, 20), (2, 30)]),
            ('nullable_inner', schema, [(1, 10), (1, 20), (2, 30), (3, None), (None, 40)]),
            ('bag_inner', schema, [(1, 10), (1, 10), (1, 20), (2, 30), (3, None), (None, 40), (None, 50)]),
            ('outer_keys', outer, [(0, 0), (1, 1), (2, 1), (3, 2), (4, 3), (5, None)]),
        ]
        records = [{'spark_version': spark.version, 'timezone': 'UTC', 'inputs': []}]
        for name, fields, rows in definitions:
            spark.createDataFrame(rows, fields).createOrReplaceTempView(name)
            records[0]['inputs'].append({'name': name, 'schema': fields.jsonValue(),
                'rows': [[encode(v, f.dataType, input_value=True) for v, f in zip(row, fields)] for row in rows]})
        nested = '(SELECT j.v FROM ordinary j WHERE j.k=i.k AND j.v=i.v)'

        def query(value=nested, source='ordinary', predicate='i.k=o.k AND i.v>=20',
                  outer='outer_keys o', key='o.id'):
            return f'SELECT {key},(SELECT {value} FROM {source} i WHERE {predicate}) AS r FROM {outer} ORDER BY {key}'
        cases = [
            ('issue_two_live_levels', query(outer='VALUES (0),(1),(2) o(k)', key='o.k')),
            ('duplicate_outer_keys', query()),
            ('duplicate_outer_rows', query(outer='(SELECT k FROM outer_keys) o', key='o.k')),
            ('same_alias_separate_scopes', query('(SELECT o.v FROM ordinary o WHERE o.k=i.k AND o.v=i.v)')),
            ('aliases_through_projection', query((
                '(SELECT p.amount FROM (SELECT j.k AS key,j.v AS amount FROM ordinary j) p WHERE '
                'p.key=i.k AND p.amount=i.v)'
            ))),
            ('inner_no_match', query('COALESCE((SELECT j.v FROM ordinary j WHERE j.k=i.k AND j.v=i.v+1),-99)')),
            ('null_safe_at_both_levels', query('(SELECT j.v FROM nullable_inner j WHERE j.k<=>i.k AND j.v<=>i.v)', source='nullable_inner', predicate='i.k<=>o.k AND (i.v>=20 OR i.v IS NULL)')),
            ('null_key_no_match', query('(SELECT j.v FROM nullable_inner j WHERE j.k=i.k AND j.v<=>i.v)', source='nullable_inner', predicate='i.k<=>o.k AND (i.v>=20 OR i.v IS NULL)')),
            ('sibling_nested_scalars', query(nested+'*100+(SELECT h.k FROM ordinary h WHERE h.k=i.k AND h.v=i.v)')),
            ('three_live_levels', query((
                '(SELECT (SELECT h.v FROM ordinary h WHERE h.k=j.k AND h.v=j.v) FROM ordinary j WHERE '
                'j.k=i.k AND j.v=i.v)'
            ))),
            ('unmatched_middle_must_not_evaluate_nested', query('(SELECT j.v FROM bag_inner j WHERE j.k<=>i.k AND j.v>=20)')),
            ('computed_nearest_scope_value', query('(SELECT j.v+i.v FROM ordinary j WHERE j.k=i.k AND j.v=i.v)')),
            ('computed_outer_projection', (
                'SELECT o.id,(SELECT (SELECT p.r*100+j.v FROM ordinary j WHERE j.k=p.k AND j.v=p.v) '
                'FROM (SELECT i.k,i.v,o.r AS r FROM ordinary i WHERE i.k=o.k AND i.v>=20) p) AS r FROM '
                '(SELECT id,k,id*17+k AS r FROM outer_keys) o ORDER BY o.id'
            )),
            ('volatile_outer_projection', (
                'SELECT o.id,COALESCE((SELECT (SELECT p.r FROM ordinary j WHERE j.k=p.k AND j.v=p.v) '
                'FROM (SELECT i.k,i.v,o.r AS r FROM ordinary i WHERE i.k=o.k AND i.v>=20) p)=o.r,FALSE)'
                ' AS r FROM (SELECT id,k,RAND() AS r FROM outer_keys) o ORDER BY o.id'
            )),
            ('nested_filter', query(predicate='i.k=o.k AND i.v=(SELECT MAX(j.v) FROM ordinary j WHERE j.k=i.k)')),
            ('nested_filter_after_local_filter', query('i.v', predicate='i.k=o.k AND i.v>=20 AND i.v=(SELECT j.v FROM bag_inner j WHERE j.k=i.k AND j.v=i.v)', outer='VALUES (0),(1),(2) o(k)', key='o.k')),
            ('filter_permuted_and', (
                'SELECT o.k,(SELECT i.v FROM ordinary i WHERE i.v=(SELECT j.v FROM bag_inner j WHERE '
                'j.k=i.k AND j.v=i.v) AND i.k=o.k AND i.v>=20) AS r FROM VALUES (0),(1),(2) o(k) ORDER '
                'BY o.k'
            )),
            ('filter_scalar_free_or', (
                'SELECT o.k,(SELECT i.v FROM ordinary i WHERE i.k=o.k AND (i.v=20 OR i.v=30) AND '
                'i.v=(SELECT j.v FROM bag_inner j WHERE j.k=i.k AND j.v=i.v)) AS r FROM VALUES '
                '(0),(1),(2) o(k) ORDER BY o.k'
            )),
            ('filter_mixed_or', (
                'SELECT o.k,(SELECT i.v FROM ordinary i WHERE i.k=o.k AND i.v>=20 AND (i.v=20 OR '
                'i.v=(SELECT j.v FROM bag_inner j WHERE j.k=i.k AND j.v=i.v))) AS r FROM VALUES '
                '(0),(1),(2) o(k) ORDER BY o.k'
            )),
            ('filter_sibling_scalar_and', (
                'SELECT o.k,(SELECT i.v FROM ordinary i WHERE i.k=o.k AND i.v>=20 AND i.v=(SELECT j.v '
                'FROM bag_inner j WHERE j.k=i.k AND j.v=i.v) AND i.k=(SELECT j.k FROM bag_inner j WHERE'
                ' j.k=i.k AND j.v=i.v)) AS r FROM VALUES (0),(1),(2) o(k) ORDER BY o.k'
            )),
            ('filter_top_or_atomic', (
                'SELECT o.k,(SELECT i.v FROM ordinary i WHERE i.k=o.k AND (i.v=20 OR i.v=(SELECT j.v '
                'FROM ordinary j WHERE j.k=i.k AND j.v=i.v AND j.v=30))) AS r FROM VALUES (0),(1),(2) '
                'o(k) ORDER BY o.k'
            )),
            ('duplicate_inner_count', query('(SELECT COUNT(*) FROM bag_inner j WHERE j.k=i.k)')),
        ]
        guards = [
            ('inner_cardinality_guard', query('(SELECT j.v FROM bag_inner j WHERE j.k=i.k AND j.v=10)', predicate='i.k=o.k AND i.v=20')),
            ('outer_cardinality_guard', query(predicate='i.k=o.k')),
            ('deepest_cardinality_guard', query((
                '(SELECT (SELECT h.v FROM bag_inner h WHERE h.k=j.k AND h.v=10) FROM ordinary j WHERE '
                'j.k=i.k AND j.v=i.v)'
            ))),
        ]
        for ansi in [False, True]:
            spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
            for name, sql in cases:
                print(json.dumps({'id':name,'ansi':ansi}), flush=True)
                frame = spark.sql(sql)
                rows = [encode(row, frame.schema) for row in frame.collect()]
                records.append({'id':name,'ansi':ansi,'sql':sql,'ordered':True,'rows':rows,
                    'schema':json.loads(frame._jdf.queryExecution().executedPlan().schema().json())})
            for name, sql in guards:
                try:
                    spark.sql(sql).collect()
                except Exception as error:
                    assert error.getCondition() == 'SCALAR_SUBQUERY_TOO_MANY_ROWS', error
                    records.append({'id':name,'ansi':ansi,'sql':sql,'cardinality_error':True})
                else:
                    raise AssertionError(f'{name} did not run')
        assert len(records) == 51
        with output.open('x') as destination:
            for record in records:
                destination.write(json.dumps(record,separators=(',',':'))+'\n')
        print(json.dumps({'observations':len(records)-1}))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
