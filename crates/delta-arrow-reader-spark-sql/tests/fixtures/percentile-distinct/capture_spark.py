"""Capture numeric DISTINCT percentile results and rejected syntax with Spark 4.2.0."""

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
    spark = (SparkSession.builder.master('local[2]').appName('dar-percentile-distinct')
             .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    try:
        assert spark.version == '4.2.0'
        spark.sparkContext.setLogLevel('ERROR')
        schema = types.StructType([
            types.StructField('id', types.IntegerType(), False),
            types.StructField('g', types.IntegerType()),
            types.StructField('v', types.DoubleType()),
            types.StructField('w', types.DoubleType()),
            types.StructField('f', types.LongType()),
            types.StructField('keep', types.BooleanType()),
        ])
        rows = [(0, 0, 1., 20., 1, False), (1, 0, 1., 20., 1, None),
                (2, 0, 1., 20., 1, True), (3, 0, 1., 9., 1, True),
                (4, 0, 1., 9., 1, True), (5, 0, 1., 9., 2, True),
                (6, 0, 9., 1., 4, True), (7, 0, 20., 1., 1, True),
                (8, 0, None, None, 2, True), (9, 0, -100., -100., 0, True),
                (10, 0, 100., 100., None, True),
                (11, 1, 2., 8., 1, False), (12, 1, 2., 8., 1, True),
                (13, 1, 2., 8., 1, True), (14, 1, 2., 4., 2, True),
                (15, 1, 4., 2., 3, True), (16, 1, 8., 2., 1, True),
                (17, None, 1., 12., 1, True), (18, None, 1., 12., 1, True),
                (19, None, 6., 3., 2, True), (20, None, 12., 3., 1, True),
                (21, 2, None, None, 1, True), (22, 2, None, None, 1, True)]
        spark.createDataFrame(rows, schema).repartition(2).createOrReplaceTempView('samples')
        probabilities = 'ARRAY(1D,0.25D,0.5D,0D,0.75D,0.5D)'
        cases = [
            ('issue_ordinary', 'SELECT PERCENTILE(DISTINCT v,0.5) AS d,PERCENTILE(v,0.5) AS b,MEDIAN(DISTINCT v) AS m,MEDIAN(v) AS mb FROM VALUES (1D),(1D),(1D),(9D),(20D) AS t(v)'),
            ('issue_frequency', 'SELECT PERCENTILE(DISTINCT v,ARRAY(0.25D,0.5D),f) AS d,PERCENTILE(v,ARRAY(0.25D,0.5D),f) AS b FROM VALUES (1D,1L),(1D,1L),(1D,1L),(1D,2L),(9D,4L),(20D,1L) AS t(v,f)'),
            ('global', f'SELECT PERCENTILE(DISTINCT v,0.25D) AS scalar,PERCENTILE(DISTINCT v,{probabilities}) AS a,PERCENTILE(v,{probabilities}) AS b,MEDIAN(DISTINCT v) AS m,MEDIAN(v) AS mb FROM samples'),
            ('global_frequency', f'SELECT PERCENTILE(DISTINCT v,0.25D,f) AS scalar,PERCENTILE(DISTINCT v,{probabilities},f) AS a,PERCENTILE(v,{probabilities},f) AS b FROM samples'),
            ('grouped', f'SELECT g,PERCENTILE(DISTINCT v,0.25D) AS scalar,PERCENTILE(DISTINCT v,{probabilities}) AS a,PERCENTILE(v,{probabilities}) AS b,MEDIAN(DISTINCT v) AS m,COUNT(*) AS n,SUM(v) AS s,AVG(v) AS mean,COUNT(DISTINCT w) AS w FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
            ('grouped_frequency', f'SELECT g,PERCENTILE(DISTINCT v,0.25D,f) AS scalar,PERCENTILE(DISTINCT v,{probabilities},f) AS a,PERCENTILE(v,{probabilities},f) AS b FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
            ('filtered', f'SELECT g,PERCENTILE(DISTINCT v,{probabilities}) FILTER(WHERE keep) AS a,MEDIAN(DISTINCT v) FILTER(WHERE keep) AS m,PERCENTILE(DISTINCT v,0.5) FILTER(WHERE NOT keep) AS f,PERCENTILE(v,0.5) FILTER(WHERE keep) AS b FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
            ('filtered_frequency', f'SELECT g,PERCENTILE(DISTINCT v,0.25D,f) FILTER(WHERE keep) AS scalar,PERCENTILE(DISTINCT v,{probabilities},f) FILTER(WHERE keep) AS a,PERCENTILE(DISTINCT v,0.5,f) FILTER(WHERE NOT keep) AS f,PERCENTILE(v,0.5,f) FILTER(WHERE keep) AS b FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
            ('different_arguments', 'SELECT g,PERCENTILE(DISTINCT v,0.25D,f) AS v,PERCENTILE(DISTINCT v,0.25D,f+1) AS f,PERCENTILE(DISTINCT w,0.25D,f) AS w FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
            ('computed', f'SELECT g%2 AS k,PERCENTILE(DISTINCT v*2+1,{probabilities},f+1) AS a,PERCENTILE(v*2+1,0.5,f+1) AS b FROM samples GROUP BY g%2 ORDER BY k NULLS FIRST'),
            ('empty', f'SELECT PERCENTILE(DISTINCT v,0.5) AS scalar,PERCENTILE(DISTINCT v,{probabilities},f) AS a,MEDIAN(DISTINCT v) AS m,COUNT(*) AS n FROM samples WHERE false'),
            ('all_null', f'SELECT PERCENTILE(DISTINCT v,0.5,f) AS scalar,PERCENTILE(DISTINCT v,{probabilities}) AS a,MEDIAN(DISTINCT v) AS m FROM samples WHERE v IS NULL'),
            ('zero_or_null_frequency', f'SELECT PERCENTILE(DISTINCT v,0.5,f) AS scalar,PERCENTILE(DISTINCT v,{probabilities},f) AS a FROM samples WHERE f=0 OR f IS NULL'),
            ('filter_false_or_null', 'SELECT PERCENTILE(DISTINCT v,0.5) FILTER(WHERE false) AS a,PERCENTILE(DISTINCT v,ARRAY(0D,0.5D),f) FILTER(WHERE CAST(NULL AS BOOLEAN)) AS f,MEDIAN(DISTINCT v) FILTER(WHERE false) AS m FROM samples'),
            ('constant', 'SELECT PERCENTILE(DISTINCT 7D,0.5) AS a,PERCENTILE(DISTINCT 7D,ARRAY(0D,0.5D),f) AS f,MEDIAN(DISTINCT 7D) AS m FROM samples'),
            ('constant_filtered', 'SELECT PERCENTILE(DISTINCT 7D,0.5) FILTER(WHERE keep) AS a,PERCENTILE(DISTINCT 7D,ARRAY(0D,0.5D),f) FILTER(WHERE keep) AS f,MEDIAN(DISTINCT 7D) FILTER(WHERE keep) AS m,APPROX_PERCENTILE(DISTINCT 7D,0.5) FILTER(WHERE keep) AS ap FROM samples'),
            ('computed_constants', 'SELECT PERCENTILE(DISTINCT 3D+4D,0.5) AS a,PERCENTILE(DISTINCT 3D+4D,ARRAY(0D,0.5D),f) AS f,PERCENTILE(DISTINCT v,0.5,CAST(1 AS BIGINT)) AS v,APPROX_PERCENTILE(DISTINCT 3D+4D,0.5) AS ap FROM samples'),
            ('having', 'SELECT g,PERCENTILE(DISTINCT v,0.5,f) AS d FROM samples GROUP BY g HAVING PERCENTILE(DISTINCT v,0.5,f)>4 ORDER BY g NULLS FIRST'),
            ('subquery', 'SELECT (SELECT PERCENTILE(DISTINCT v,0.25D,f) FROM samples) AS d'),
            ('sorted_mixed', 'SELECT PERCENTILE(DISTINCT v,0.25D,f) AS d,APPROX_PERCENTILE(DISTINCT v,0.5) AS a,FIRST(v,true) AS f,LAST(v,true) AS l,COLLECT_LIST(v) AS values FROM (SELECT v,f FROM samples WHERE g=0 ORDER BY id DESC)'),
            ('family_shared', 'SELECT g,PERCENTILE(DISTINCT v,0.5) AS e,MEDIAN(DISTINCT v) AS m,APPROX_PERCENTILE(DISTINCT v,0.5) AS a,PERCENTILE_APPROX(DISTINCT v,ARRAY(0D,0.5D,1D),100) AS aa,PERCENTILE(DISTINCT v,0.25D,f) AS f FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
            ('family_ordered_controls', 'SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY v) AS c,PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY v DESC) AS d FROM samples'),
            ('empty_array_issue', 'SELECT PERCENTILE(v,ARRAY()) AS p,PERCENTILE(v,ARRAY(),f) AS weighted,PERCENTILE(DISTINCT v,ARRAY()) AS d,PERCENTILE(DISTINCT v,ARRAY(),f) AS wd FROM VALUES (1D,1L),(1D,2L),(9D,4L) AS t(v,f)'),
            ('empty_array_global', 'SELECT PERCENTILE(v,ARRAY()) AS p,PERCENTILE(v,ARRAY(),f) AS weighted,PERCENTILE(DISTINCT v,ARRAY()) AS d,PERCENTILE(DISTINCT v,ARRAY(),f) AS wd FROM samples'),
            ('empty_array_grouped', 'SELECT g,PERCENTILE(v,ARRAY()) AS p,PERCENTILE(v,ARRAY(),f) AS weighted,PERCENTILE(DISTINCT v,ARRAY()) AS d,PERCENTILE(DISTINCT v,ARRAY(),f) AS wd,COUNT(*) AS n FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
            ('empty_array_filtered', 'SELECT g,PERCENTILE(v,ARRAY()) FILTER(WHERE keep) AS p,PERCENTILE(v,ARRAY(),f) FILTER(WHERE keep) AS weighted,PERCENTILE(DISTINCT v,ARRAY()) FILTER(WHERE keep) AS d,PERCENTILE(DISTINCT v,ARRAY(),f) FILTER(WHERE keep) AS wd FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
            ('empty_array_filter_false_or_null', 'SELECT PERCENTILE(v,ARRAY()) FILTER(WHERE false) AS p,PERCENTILE(v,ARRAY(),f) FILTER(WHERE CAST(NULL AS BOOLEAN)) AS weighted,PERCENTILE(DISTINCT v,ARRAY()) FILTER(WHERE false) AS d,PERCENTILE(DISTINCT v,ARRAY(),f) FILTER(WHERE CAST(NULL AS BOOLEAN)) AS wd FROM samples'),
            ('empty_array_empty', 'SELECT PERCENTILE(v,ARRAY()) AS p,PERCENTILE(v,ARRAY(),f) AS weighted,PERCENTILE(DISTINCT v,ARRAY()) AS d,PERCENTILE(DISTINCT v,ARRAY(),f) AS wd FROM samples WHERE false'),
            ('empty_array_all_null', 'SELECT PERCENTILE(v,ARRAY()) AS p,PERCENTILE(v,ARRAY(),f) AS weighted,PERCENTILE(DISTINCT v,ARRAY()) AS d,PERCENTILE(DISTINCT v,ARRAY(),f) AS wd FROM samples WHERE v IS NULL'),
            ('empty_array_zero_or_null_frequency', 'SELECT PERCENTILE(v,ARRAY(),f) AS weighted,PERCENTILE(DISTINCT v,ARRAY(),f) AS wd FROM samples WHERE f=0 OR f IS NULL'),
            ('empty_array_constant', 'SELECT PERCENTILE(DISTINCT 7D,ARRAY()) AS d,PERCENTILE(DISTINCT 3D+4D,ARRAY(),f) FILTER(WHERE keep) AS wd FROM samples'),
            ('empty_array_family', 'SELECT APPROX_PERCENTILE(v,ARRAY()) AS a,PERCENTILE_APPROX(DISTINCT v,ARRAY()) AS d,PERCENTILE_DISC(ARRAY()) WITHIN GROUP(ORDER BY v) AS discrete,MEDIAN(v) AS m,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY v) AS continuous FROM samples'),
        ]
        for frame, label in [('UNBOUNDED PRECEDING', 'running'), ('2 PRECEDING', 'sliding')]:
            cases.append(('empty_array_'+label, f'SELECT id,PERCENTILE(v,ARRAY()) OVER(ORDER BY id ROWS BETWEEN {frame} AND CURRENT ROW) AS p,PERCENTILE(v,ARRAY(),f) OVER(ORDER BY id ROWS BETWEEN {frame} AND CURRENT ROW) AS weighted FROM samples ORDER BY id'))
        for name, grouping, columns in [
                ('rollup', 'ROLLUP(g)', 'g'), ('cube', 'CUBE(g,keep)', 'g,keep'),
                ('grouping_sets', 'GROUPING SETS ((g),(keep))', 'g,keep'),
                ('duplicate_sets', 'GROUPING SETS ((g),(),(g),())', 'g')]:
            cases.append((name, f'SELECT {columns},GROUPING_ID() AS gid,PERCENTILE(DISTINCT v,ARRAY(0D,0.5D)) FILTER(WHERE keep) AS e,MEDIAN(DISTINCT v) FILTER(WHERE keep) AS m,APPROX_PERCENTILE(DISTINCT v,0.5) FILTER(WHERE keep) AS a,PERCENTILE(DISTINCT v,ARRAY(0.25D,0.5D),f) FILTER(WHERE keep) AS f,PERCENTILE(v,0.5,f) AS b,COUNT(*) AS n FROM samples GROUP BY {grouping} ORDER BY gid,{columns} NULLS FIRST'))
        for dtype in ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL(12,2)']:
            cases.append(('value_'+dtype, f'SELECT PERCENTILE(DISTINCT CAST(v AS {dtype}),0.25D) AS scalar,PERCENTILE(DISTINCT CAST(v AS {dtype}),{probabilities},f) AS weighted,MEDIAN(DISTINCT CAST(v AS {dtype})) AS m FROM samples'))
            cases.append(('empty_array_value_'+dtype, f'SELECT PERCENTILE(CAST(v AS {dtype}),ARRAY()) AS p,PERCENTILE(CAST(v AS {dtype}),ARRAY(),f) AS weighted,PERCENTILE(DISTINCT CAST(v AS {dtype}),ARRAY()) AS d,PERCENTILE(DISTINCT CAST(v AS {dtype}),ARRAY(),f) AS wd FROM samples'))
        for dtype in ['TINYINT', 'SMALLINT', 'INT', 'BIGINT']:
            cases.append(('frequency_'+dtype, f'SELECT PERCENTILE(DISTINCT v,0.25D,CAST(f AS {dtype})) AS scalar,PERCENTILE(DISTINCT v,{probabilities},CAST(f AS {dtype})) AS a FROM samples'))
        rejected = [
            ('continuous_distinct', 'SELECT PERCENTILE_CONT(DISTINCT 0.5) WITHIN GROUP(ORDER BY v) FROM samples'),
            ('discrete_distinct', 'SELECT PERCENTILE_DISC(DISTINCT 0.5) WITHIN GROUP(ORDER BY v) FROM samples'),
        ]
        for function in ['PERCENTILE(DISTINCT v,0.5)', 'PERCENTILE(DISTINCT v,ARRAY(0D,0.5D),f)',
                         'MEDIAN(DISTINCT v)', 'APPROX_PERCENTILE(DISTINCT v,0.5)',
                         'PERCENTILE_APPROX(DISTINCT v,0.5)']:
            rejected.append((function+'_window', f'SELECT {function} OVER(PARTITION BY g) FROM samples'))
        records = [{'spark_version': spark.version, 'timezone': 'UTC', 'source_partitions': 2,
                    'inputs': [{'name': 'samples', 'schema': schema.jsonValue(),
                                'rows': [encode(row, schema, input_value=True) for row in rows]}]}]
        for ansi in [False, True]:
            spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
            for name, sql in cases:
                print(json.dumps({'id': name, 'ansi': ansi}), flush=True)
                frame = spark.sql(sql)
                result = [encode(row, frame.schema) for row in frame.collect()]
                records.append({'id': name, 'ansi': ansi, 'sql': sql, 'ordered': True,
                                'rows': result, 'schema': json.loads(
                                    frame._jdf.queryExecution().executedPlan().schema().json())})
            for name, sql in rejected:
                try:
                    spark.sql(sql).collect()
                except Exception as error:
                    records.append({'id': name, 'ansi': ansi, 'sql': sql,
                                    'spark_rejected': True, 'error': str(error)})
                else:
                    raise AssertionError(f'Spark accepted syntax expected to be rejected: {sql}')
        with output.open('x') as destination:
            for record in records:
                destination.write(json.dumps(record, separators=(',', ':'))+'\n')
        print(json.dumps({'results': len(cases)*2, 'rejected': len(rejected)*2}), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
