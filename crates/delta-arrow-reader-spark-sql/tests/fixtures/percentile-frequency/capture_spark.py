"""Capture exact numeric percentiles with frequency using Spark 4.2.0 and Java 21."""

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
    spark = (SparkSession.builder.master('local[2]').appName('dar-percentile-frequency')
             .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    try:
        assert spark.version == '4.2.0'
        spark.sparkContext.setLogLevel('ERROR')
        schema = types.StructType([
            types.StructField('id', types.IntegerType(), False),
            types.StructField('g', types.IntegerType()),
            types.StructField('v', types.DoubleType()),
            types.StructField('f', types.LongType()),
        ])
        rows = [(0, 1, 1., 1), (1, 1, None, 2), (2, 1, 9., 8),
                (3, 1, 100., 0), (4, 1, -100., None),
                (5, 2, 2., 2), (6, 2, 10., 2), (7, 2, 10., 1),
                (8, 3, None, 3), (9, 3, 4., 0), (10, 3, 8., None),
                (11, None, -4., 3), (12, None, 12., 1),
                (13, 4, 0., 1), (14, 4, 8., 1)]
        spark.createDataFrame(rows, schema).repartition(2).createOrReplaceTempView('samples')
        records = [{'spark_version': spark.version, 'timezone': 'UTC', 'source_partitions': 2,
                    'inputs': [{'name': 'samples', 'schema': schema.jsonValue(),
                                'rows': [encode(row, schema, input_value=True) for row in rows]}]}]
        probabilities = 'ARRAY(1D,0.25D,0.5D,0D,0.75D,0.5D)'
        cases = [
            ('original', 'SELECT PERCENTILE(v,ARRAY(0D,0.5D,1D),f) AS r FROM samples WHERE id<3'),
            ('grouped', f'SELECT g,PERCENTILE(v,0.5D,f) AS scalar,PERCENTILE(v,{probabilities},f) AS weighted,PERCENTILE(v,{probabilities}) AS ordinary,MEDIAN(v) AS median FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
            ('global', f'SELECT PERCENTILE(v,0.25D,f) AS scalar,PERCENTILE(v,{probabilities},f) AS weighted,PERCENTILE(v,0.25D) AS ordinary FROM samples'),
            ('empty', f'SELECT PERCENTILE(v,0.5D,f) AS scalar,PERCENTILE(v,{probabilities},f) AS weighted FROM samples WHERE id<0'),
            ('all_null', f'SELECT PERCENTILE(v,0.5D,f) AS scalar,PERCENTILE(v,{probabilities},f) AS weighted FROM samples WHERE v IS NULL'),
            ('zero_or_null', f'SELECT PERCENTILE(v,0.5D,f) AS scalar,PERCENTILE(v,{probabilities},f) AS weighted FROM samples WHERE g=3'),
            ('zero_literal', f'SELECT PERCENTILE(v,0.5D,0) AS scalar,PERCENTILE(v,{probabilities},0) AS weighted FROM samples'),
            ('null_literal', f'SELECT PERCENTILE(v,0.5D,CAST(NULL AS BIGINT)) AS scalar,PERCENTILE(v,{probabilities},CAST(NULL AS BIGINT)) AS weighted FROM samples'),
            ('one_literal', f'SELECT PERCENTILE(v,0.25D,1) AS scalar,PERCENTILE(v,{probabilities},1) AS weighted,PERCENTILE(v,{probabilities}) AS ordinary FROM samples'),
            ('filtered', f'SELECT g,PERCENTILE(v,{probabilities},f) FILTER(WHERE id%2=0) AS r FROM samples GROUP BY g ORDER BY g NULLS FIRST'),
            ('partition_window', f'SELECT id,g,PERCENTILE(v,0.25D,f) OVER(PARTITION BY g) AS scalar,PERCENTILE(v,{probabilities},f) OVER(PARTITION BY g) AS weighted FROM samples ORDER BY id'),
        ]
        for frame, label in [('UNBOUNDED PRECEDING', 'running'), ('2 PRECEDING', 'sliding')]:
            cases.append((label, f'SELECT id,PERCENTILE(v,0.25D,f) OVER(ORDER BY id ROWS BETWEEN {frame} AND CURRENT ROW) AS scalar,PERCENTILE(v,{probabilities},f) OVER(ORDER BY id ROWS BETWEEN {frame} AND CURRENT ROW) AS weighted,PERCENTILE(v,0.25D) OVER(ORDER BY id ROWS BETWEEN {frame} AND CURRENT ROW) AS ordinary FROM samples ORDER BY id'))
        for dtype in ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL(12,2)']:
            cases.append((f'value_{dtype}', f'SELECT PERCENTILE(CAST(v AS {dtype}),0.25D,f) AS scalar,PERCENTILE(CAST(v AS {dtype}),{probabilities},f) AS weighted FROM samples'))
        for dtype in ['TINYINT', 'SMALLINT', 'INT', 'BIGINT']:
            cases.append((f'frequency_{dtype}', f'SELECT PERCENTILE(v,0.25D,CAST(f AS {dtype})) AS scalar,PERCENTILE(v,{probabilities},CAST(f AS {dtype})) AS weighted FROM samples'))
        cases.extend([
            ('large_weight', f'SELECT PERCENTILE(v,0.25D,CASE WHEN id=0 THEN 1000000000000L ELSE 1L END) AS scalar,PERCENTILE(v,{probabilities},CASE WHEN id=0 THEN 1000000000000L ELSE 1L END) AS weighted FROM samples WHERE id IN (0,2)'),
            ('merge', f'SELECT g,PERCENTILE(v,0.25D,f) AS scalar,PERCENTILE(v,{probabilities},f) AS weighted FROM (SELECT CAST(id%3 AS INT) AS g,CAST(id%11 AS DOUBLE) AS v,CAST(id%5 AS BIGINT) AS f FROM range(0,90,1,3)) GROUP BY g ORDER BY g'),
        ])
        for ansi in [False, True]:
            spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
            for name, sql in cases:
                print(json.dumps({'id': name, 'ansi': ansi}), flush=True)
                frame = spark.sql(sql)
                result = [encode(row, frame.schema) for row in frame.collect()]
                records.append({'id': name, 'ansi': ansi, 'sql': sql, 'ordered': True,
                                'rows': result, 'schema': json.loads(
                                    frame._jdf.queryExecution().executedPlan().schema().json())})
        assert len(records) == 53
        with output.open('x') as destination:
            for record in records:
                destination.write(json.dumps(record, separators=(',', ':'))+'\n')
        print(json.dumps({'observations': len(records)-1}))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
