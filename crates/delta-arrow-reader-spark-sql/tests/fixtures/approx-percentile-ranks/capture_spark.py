"""Capture approximate percentile ranks and executed numeric types with Spark 4.2."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from capture_floating_group_keys_spark import encode


def main():
    output, summary_output = map(Path, sys.argv[1:])
    assert not output.exists() and not summary_output.exists()
    os.environ['PYSPARK_PYTHON'] = sys.executable
    spark = (SparkSession.builder.master('local[1]').appName('dar-approx-percentile-ranks')
             .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '1')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    try:
        assert spark.version == '4.2.0'
        spark.sparkContext.setLogLevel('ERROR')
        schema = types.StructType([types.StructField('id', types.IntegerType(), False),
                                   types.StructField('g', types.IntegerType(), False),
                                   types.StructField('v', types.DoubleType())])
        values = [1., 2., 3., 4., None, 10., 10., 10., 70., 100., 130., None]
        rows = [(i, 0 if i < 5 else 1, v) for i, v in enumerate(values)]
        spark.createDataFrame(rows, schema).createOrReplaceTempView('samples')
        records = [{'spark_version': spark.version, 'timezone': 'UTC', 'inputs': [
            {'name': 'samples', 'schema': schema.jsonValue(),
             'rows': [encode(row, schema, input_value=True) for row in rows]}]}]
        original = "SELECT APPROX_PERCENTILE(CAST(n AS DOUBLE), 0.5) AS r FROM VALUES ('1'), ('2'), ('3'), ('4') AS t(n)"
        cases = [('issue_original', original)]
        for alias in ['APPROX_PERCENTILE', 'PERCENTILE_APPROX']:
            for p in ['0', '0.01', '0.25', '0.26', '0.5', '0.51', '0.75', '0.99', '1']:
                cases.append((f'{alias.lower()}_p{p}', f'SELECT g,{alias}(v,{p}) AS r FROM samples GROUP BY g ORDER BY g'))
            cases.extend([
                (f'{alias.lower()}_odd', f'SELECT {alias}(v,0.5) AS r FROM samples WHERE id<>3 AND g=0'),
                (f'{alias.lower()}_empty', f'SELECT {alias}(v,0.5) AS r FROM samples WHERE id<0'),
                (f'{alias.lower()}_all_null', f'SELECT {alias}(v,0.5) AS r FROM samples WHERE v IS NULL'),
                (f'{alias.lower()}_window', f'SELECT id,g,{alias}(v,0.5) OVER (PARTITION BY g) AS r FROM samples ORDER BY id'),
                (f'{alias.lower()}_array', f'SELECT g,{alias}(v,ARRAY(0.75D,0.0D,0.5D,1.0D,0.5D)) AS r FROM samples GROUP BY g ORDER BY g'),
            ])
            for accuracy in [1, 2, 10, 100, 10000]:
                cases.append((f'{alias.lower()}_accuracy{accuracy}', f'SELECT g,{alias}(v,0.5,{accuracy}) AS r FROM samples GROUP BY g ORDER BY g'))
        for dtype in ['TINYINT','SMALLINT','INT','BIGINT','FLOAT','DOUBLE','DECIMAL(12,2)']:
            cases.append((f'type_{dtype}', f'SELECT APPROX_PERCENTILE(CAST(v AS {dtype}),0.5) AS r,PERCENTILE_APPROX(CAST(v AS {dtype}),0.75) AS a FROM samples WHERE g=0'))
        cases.append(('exact_controls', 'SELECT PERCENTILE(v,0.5) AS p,MEDIAN(v) AS m,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v) AS c FROM samples WHERE g=0'))
        cases.extend([
            ('empty_percentile_array', 'SELECT APPROX_PERCENTILE(v,ARRAY()) AS r FROM samples'),
            ('singleton', 'SELECT APPROX_PERCENTILE(v,0.51) AS r FROM samples WHERE id=0'),
            ('filtered', 'SELECT APPROX_PERCENTILE(v,0.5) FILTER (WHERE id<4) AS r FROM samples'),
            ('array_accuracy', 'SELECT PERCENTILE_APPROX(v,ARRAY(0.25D,0.5D,0.75D),2) AS r FROM samples'),
            ('array_window', 'SELECT id,APPROX_PERCENTILE(v,ARRAY(0D,0.5D,1D)) OVER (PARTITION BY g) AS r FROM samples ORDER BY id'),
            ('folded_parameters', 'SELECT APPROX_PERCENTILE(v,1.0/2,100*100) AS r FROM samples WHERE g=0'),
        ])
        for partitions in [1, 2]:
            for accuracy in [10, 100, 10000]:
                cases.append((f'compressed_p{partitions}_accuracy{accuracy}',
                    f'SELECT APPROX_PERCENTILE(CAST(id AS DOUBLE),ARRAY(0D,0.01D,0.25D,0.5D,0.51D,0.9D,1D),{accuracy}) AS r FROM range(0,120001,1,{partitions})'))
        edge_cases = [
            ('nan_signed', "SELECT APPROX_PERCENTILE(v,ARRAY(0D,0.25D,0.5D,0.75D,1D)) AS r FROM VALUES (CAST('-NaN' AS DOUBLE)),(CAST('NaN' AS DOUBLE)),(-2D),(0D),(2D) AS t(v)"),
            ('nan_positive', "SELECT APPROX_PERCENTILE(v,ARRAY(0D,0.25D,0.5D,0.75D,1D)) AS r FROM VALUES (CAST('NaN' AS DOUBLE)),(-2D),(0D),(2D) AS t(v)"),
            ('signed_zero', 'SELECT APPROX_PERCENTILE(v,ARRAY(0D,0.25D,0.5D,0.75D,1D)) AS r FROM VALUES (-0.0D),(0.0D),(-1D),(1D) AS t(v)'),
            ('decimal_fraction', 'SELECT APPROX_PERCENTILE(v,ARRAY(0D,0.25D,0.5D,0.75D,1D)) AS r FROM VALUES (CAST(0.29 AS DECIMAL(12,2))),(CAST(-0.57 AS DECIMAL(12,2))),(CAST(1.16 AS DECIMAL(12,2))),(CAST(2.01 AS DECIMAL(12,2))) AS t(v)'),
            ('array_integer', 'SELECT APPROX_PERCENTILE(v,ARRAY(1,0,1,0)) AS r FROM VALUES (1D),(2D),(3D),(4D) AS t(v)'),
            ('array_null', 'SELECT APPROX_PERCENTILE(v,ARRAY(0.5D,CAST(NULL AS DOUBLE))) AS r FROM VALUES (1D),(2D),(3D),(4D) AS t(v)'),
            ('float_fraction', 'SELECT PERCENTILE_APPROX(v,ARRAY(0D,0.5D,1D)) AS r FROM VALUES (CAST(0.29 AS FLOAT)),(CAST(-0.57 AS FLOAT)),(CAST(1.16 AS FLOAT)),(CAST(2.01 AS FLOAT)) AS t(v)'),
            ('window_running', 'SELECT id,APPROX_PERCENTILE(v,0.5) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM VALUES (0,1D),(1,2D),(2,3D),(3,4D),(4,CAST(NULL AS DOUBLE)) AS t(id,v)'),
            ('window_sliding', 'SELECT id,APPROX_PERCENTILE(v,0.5) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM VALUES (0,1D),(1,2D),(2,3D),(3,4D),(4,CAST(NULL AS DOUBLE)) AS t(id,v)'),
        ]
        cases.extend(edge_cases)
        for order, expression in [('reverse', '180000-id'), ('interleaved', '(id*37)%180001')]:
            cases.append((f'compressed_{order}_p3_accuracy100',
                f'SELECT APPROX_PERCENTILE(v,ARRAY(0D,0.01D,0.25D,0.5D,0.51D,0.9D,1D),100) AS r FROM (SELECT CAST({expression} AS DOUBLE) AS v FROM range(0,180001,1,3))'))
        for ansi in [False, True]:
            spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
            for name, sql in cases:
                print(json.dumps({'id': name, 'ansi': ansi}), flush=True)
                frame = spark.sql(sql)
                rows = [encode(row, frame.schema) for row in frame.collect()]
                records.append({'id': name, 'ansi': ansi, 'sql': sql, 'ordered': True, 'rows': rows,
                    'schema': json.loads(frame._jdf.queryExecution().executedPlan().schema().json()),
                    **({'single_partition': True} if any(name == edge[0] for edge in edge_cases) else {})})
        assert len(records) == 141
        with output.open('x') as destination:
            for record in records:
                destination.write(json.dumps(record, separators=(',', ':'))+'\n')
        digest_type = getattr(spark._jvm.org.apache.spark.sql.catalyst.expressions.aggregate,
                              'ApproximatePercentile$PercentileDigest')
        probabilities = [0.0, 0.01, 0.25, 0.5, 0.51, 0.9, 1.0]
        java_probabilities = spark.sparkContext._gateway.new_array(spark._jvm.double, len(probabilities))
        for i, probability in enumerate(probabilities):
            java_probabilities[i] = probability
        summary_records = []
        for order in ['reverse', 'interleaved']:
            merged = digest_type(0.01)
            for partition in range(3):
                partial = digest_type(0.01)
                for value in range(180001*partition//3, 180001*(partition+1)//3):
                    partial.add(float(180000-value if order == 'reverse' else (value*37)%180001))
                merged.merge(partial)
                print(json.dumps({'summary': order, 'merged_partition': partition}), flush=True)
            result = merged.getPercentiles(java_probabilities)
            summary_records.append({'id': order, 'spark_version': spark.version, 'size': 180001,
                'partitions': 3, 'merge_order': [0, 1, 2], 'accuracy': 100, 'probabilities': probabilities,
                'rows': [[encode([result.apply(i) for i in range(len(probabilities))],
                    types.ArrayType(types.DoubleType(), False))]]})
        with summary_output.open('x') as destination:
            for record in summary_records:
                destination.write(json.dumps(record, separators=(',', ':'))+'\n')
        print(json.dumps({'observations': len(records)-1, 'direct_summary_observations': len(summary_records)}))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
