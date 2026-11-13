"""Capture valid DISTINCT approximate percentile results with Spark 4.2.0."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from capture_floating_group_keys_spark import encode


def main():
    output = Path(sys.argv[1])
    assert not output.exists()
    os.environ['PYSPARK_PYTHON'] = sys.executable
    spark = (SparkSession.builder.master('local[2]').appName('dar-approx-percentile-distinct')
             .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    try:
        assert spark.version == '4.2.0'
        spark.sparkContext.setLogLevel('ERROR')
        source = ('VALUES (0,1D,20D,false),(0,1D,20D,true),(0,1D,9D,true),'
                  '(0,9D,1D,true),(0,20D,1D,true),(0,NULL,NULL,true),(0,9D,1D,false),'
                  '(1,2D,8D,false),(1,2D,8D,true),(1,2D,4D,true),'
                  '(1,4D,2D,true),(1,8D,2D,true),'
                  '(NULL,3D,12D,false),(NULL,3D,12D,true),(NULL,3D,6D,true),'
                  '(NULL,6D,3D,true),(NULL,12D,3D,true),(2,NULL,NULL,true)')
        spark.sql(f'CREATE OR REPLACE TEMP VIEW samples AS SELECT * FROM {source} AS t(g,v,w,keep)')
        cases = [('issue_original', 'SELECT APPROX_PERCENTILE(DISTINCT v,0.5) AS d,APPROX_PERCENTILE(v,0.5) AS b FROM VALUES (1D),(1D),(1D),(9D),(20D) AS t(v)')]
        for alias in ['APPROX_PERCENTILE', 'PERCENTILE_APPROX']:
            prefix = alias.lower()
            cases.extend([
                (prefix+'_single', f'SELECT {alias}(DISTINCT v,0.5) AS d FROM samples'),
                (prefix+'_grouped', f'SELECT g,{alias}(DISTINCT v,0.5) AS d,{alias}(v,0.5) AS b,COUNT(*) AS n,SUM(v) AS s,AVG(v) AS a FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
                (prefix+'_array_accuracy', f'SELECT g,{alias}(DISTINCT v,ARRAY(1D,0.5D,0D,0.5D),100) AS d,{alias}(v,ARRAY(1D,0.5D,0D,0.5D),100) AS b FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
                (prefix+'_filtered', f'SELECT g,{alias}(DISTINCT v,ARRAY(0D,0.5D)) FILTER(WHERE keep) AS d,{alias}(DISTINCT v,ARRAY(0D,1D)) FILTER(WHERE NOT keep) AS f,{alias}(v,0.5) FILTER(WHERE keep) AS b FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
                (prefix+'_multiple', f'SELECT g,{alias}(DISTINCT v,0.5) AS v,{alias}(DISTINCT w,0.4) AS w,COUNT(DISTINCT w) AS n FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
                (prefix+'_computed', f'SELECT g%2 AS k,{alias}(DISTINCT v*2+1,1.0/2,100*100) AS d,{alias}(v*2+1,0.5) AS b FROM samples GROUP BY g%2 ORDER BY k NULLS FIRST'),
                (prefix+'_empty', f'SELECT {alias}(DISTINCT v,0.5) AS d,{alias}(v,0.5) AS b,COUNT(*) AS n FROM samples WHERE false'),
                (prefix+'_all_null', f'SELECT {alias}(DISTINCT v,0.5) AS d,{alias}(DISTINCT v,ARRAY(0D,0.5D,1D)) AS a FROM samples WHERE v IS NULL'),
                (prefix+'_having', f'SELECT g,{alias}(DISTINCT v,0.5) AS d FROM samples GROUP BY g HAVING {alias}(DISTINCT v,0.5)>4 ORDER BY g NULLS FIRST'),
                (prefix+'_sorted_mixed', f'SELECT {alias}(DISTINCT v,0.5) AS d,FIRST(v,true) AS f,LAST(v,true) AS l,COLLECT_LIST(v) AS a FROM (SELECT v FROM {source} AS t(g,v,w,keep) WHERE g=0 ORDER BY w DESC,v DESC)'),
                (prefix+'_shared_grouping_sets',
                 f'SELECT g,GROUPING_ID() AS gid,{alias}(DISTINCT v,ARRAY(0D,0.5D)) AS d,'
                 f'{alias}(DISTINCT v,0.5,1) AS low,{alias}(DISTINCT v,ARRAY(1D,0D,0.5D),1) AS low_array,'
                 f'{alias}(v,0.5) AS b,COUNT(*) AS n FROM VALUES (0,1D),(0,9D),(0,20D),'
                 '(NULL,1D),(NULL,30D),(NULL,40D) AS t(g,v) '
                 'GROUP BY GROUPING SETS ((g),(),(g),()) ORDER BY gid,g NULLS FIRST'),
            ])
            for name, grouping in [('rollup', 'ROLLUP(g)'), ('cube', 'CUBE(g,keep)'),
                                   ('grouping_sets', 'GROUPING SETS ((g),(keep))'),
                                   ('duplicate_sets', 'GROUPING SETS ((g),(),(g),())')]:
                columns = 'g,keep' if name in ['cube', 'grouping_sets'] else 'g'
                cases.append((prefix+'_'+name, f'SELECT {columns},GROUPING_ID() AS gid,{alias}(DISTINCT v,ARRAY(0D,0.5D)) AS d,{alias}(v,0.5) AS b,COUNT(*) AS n FROM samples GROUP BY {grouping} ORDER BY gid,{columns} NULLS FIRST'))
        for dtype in ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL(12,2)']:
            cases.append(('type_'+dtype, f'SELECT APPROX_PERCENTILE(DISTINCT CAST(v AS {dtype}),0.5) AS d,PERCENTILE_APPROX(DISTINCT CAST(v AS {dtype}),ARRAY(0D,0.5D,1D),10) AS a,APPROX_PERCENTILE(CAST(v AS {dtype}),0.5) AS b FROM samples WHERE g=0'))
        cases.extend([
            ('filter_all_false', 'SELECT APPROX_PERCENTILE(DISTINCT v,0.5) FILTER(WHERE false) AS d,COUNT(*) AS n FROM samples'),
            ('constant', 'SELECT APPROX_PERCENTILE(DISTINCT 7D,0.5) AS d,COUNT(*) AS n FROM samples'),
            ('empty_array', 'SELECT APPROX_PERCENTILE(DISTINCT v,ARRAY()) AS d,COUNT(*) AS n FROM samples'),
            ('masked_argument', 'SELECT APPROX_PERCENTILE(DISTINCT CASE WHEN v<>0 THEN 1D/v END,0.5) FILTER(WHERE v<>0) AS d FROM VALUES (0D),(1D),(1D),(2D),(4D) AS t(v)'),
            ('nullable_filter', 'SELECT APPROX_PERCENTILE(DISTINCT v,ARRAY(0D,0.5D)) FILTER(WHERE keep) AS d FROM VALUES (1D,CAST(NULL AS BOOLEAN)),(1D,true),(1D,true),(9D,true),(20D,true) AS t(v,keep)'),
            ('plain_controls', 'SELECT g,APPROX_PERCENTILE(v,0.5) AS d,PERCENTILE_APPROX(w,0.5) AS w,COUNT(*) AS n FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
        ])
        records = [{'spark_version': spark.version, 'timezone': 'UTC', 'inputs': []}]
        for ansi in [False, True]:
            spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
            for name, sql in cases:
                sql = sql.replace('FROM samples', f'FROM ({source}) AS samples(g,v,w,keep)')
                print(json.dumps({'id': name, 'ansi': ansi}), flush=True)
                frame = spark.sql(sql)
                rows = [encode(row, frame.schema) for row in frame.collect()]
                records.append({'id': name, 'ansi': ansi, 'sql': sql, 'ordered': True,
                                'rows': rows, 'schema': json.loads(frame._jdf.queryExecution().executedPlan().schema().json())})
        with output.open('x') as destination:
            for record in records:
                destination.write(json.dumps(record, separators=(',', ':'))+'\n')
        print(json.dumps({'observations': len(records)-1}), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
