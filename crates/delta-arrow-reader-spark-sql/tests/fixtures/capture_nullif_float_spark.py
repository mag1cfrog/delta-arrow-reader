"""Capture NULLIF floating comparisons with independent PySpark 4.2.0.

Run with Java 21 and pyspark==4.2.0. The adjacent JSONL stores input bits and
results; Rust tests do not need Spark. Finite float results use hexadecimal
IEEE bits so negative zero and float width survive JSON; all NaNs use 'nan'.
"""
import json
import math
import os
import struct
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, FloatType, IntegerType, StructField, StructType

os.environ['PYSPARK_PYTHON'] = sys.executable
spark = (SparkSession.builder.master('local[1]').appName('dar-nullif-float-reference')
         .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '1')
         .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
inputs = []
queries = []
observations = []
try:
    assert spark.version == '4.2.0', spark.version
    spark.sparkContext.setLogLevel('ERROR')
    for name, sql_type, dtype, encoding, nan_bits in [
        ('float', 'FLOAT', FloatType(), '!f', ['7fc01234', 'ffc04321']),
        ('double', 'DOUBLE', DoubleType(), '!d', ['7ff8000000001234', 'fff8000000004321']),
    ]:
        nan_a, nan_b = [struct.unpack(encoding, bytes.fromhex(bits))[0] for bits in nan_bits]
        values = [
            (None, 0.0), (0.0, -0.0), (-0.0, 0.0), (0.0, 1.0), (-0.0, 1.0),
            (1.0, 1.0), (1.0, 2.0), (-1.0, -1.0), (float('inf'), float('inf')),
            (-float('inf'), float('inf')), (nan_a, nan_b), (nan_b, nan_a),
            (nan_a, 1.0), (1.0, nan_a), (None, None), (1.0, None),
        ]
        rows = [(i, a, b) for i, (a, b) in enumerate(values)]
        schema = StructType([StructField('id', IntegerType(), False),
                             StructField('a', dtype, True), StructField('b', dtype, True)])
        table = name + '_inputs'
        spark.createDataFrame(rows, schema).createOrReplaceTempView(table)
        inputs.append({'table': table, 'type': name, 'rows': [
            [None if v is None else struct.pack(encoding, v).hex() for v in pair]
            for pair in values
        ]})
        expressions = [
            ('columns', 'NULLIF(a,b)'),
            ('zero_rhs', f'NULLIF(a,CAST(0 AS {sql_type}))'),
            ('one_rhs', f'NULLIF(a,CAST(1 AS {sql_type}))'),
            ('one_lhs', f'NULLIF(CAST(1 AS {sql_type}),b)'),
            ('infinity_rhs', f"NULLIF(a,CAST('Infinity' AS {sql_type}))"),
            ('negative_zero_lhs', f"NULLIF(CAST('-0.0' AS {sql_type}),b)"),
            ('nan_rhs', f"NULLIF(a,CAST('NaN' AS {sql_type}))"),
            ('nan_lhs', f"NULLIF(CAST('NaN' AS {sql_type}),b)"),
            ('null_rhs', f'NULLIF(a,CAST(NULL AS {sql_type}))'),
            ('null_lhs', f'NULLIF(CAST(NULL AS {sql_type}),b)'),
            ('nullifzero_control', 'NULLIFZERO(a)'),
            ('equality_control', 'a=b'),
            ('in_control', 'a IN (b)'),
        ]
        queries += [(name+'_'+case, f'SELECT {expression} AS r FROM {table} ORDER BY id')
                    for case, expression in expressions]
        queries.append((name+'_empty', f'SELECT NULLIF(a,b) AS r FROM {table} WHERE id<0'))
        for case, a, b in [('negative_zero', "'-0.0'", '0'), ('positive_zero', '0', "'-0.0'"),
                           ('nan', "'NaN'", "'NaN'"), ('unequal_negative_zero', "'-0.0'", '1')]:
            queries.append((name+'_'+case, f'SELECT NULLIF(CAST({a} AS {sql_type}),CAST({b} AS {sql_type})) AS r'))
        queries.append((name+'_live_bad_left', f"SELECT NULLIF(CAST('bad' AS {sql_type}),CAST(1 AS {sql_type})) AS r"))
        queries.append((name+'_live_bad_right', f"SELECT NULLIF(CAST(1 AS {sql_type}),CAST('bad' AS {sql_type})) AS r"))
    queries += [
        ('null_subquery_guard_int', 'SELECT NULLIF(CAST(NULL AS INT),v) + (SELECT x FROM VALUES (1),(2) q(x)) AS r FROM VALUES (1) t(v)'),
        ('null_subquery_guard_double', 'SELECT NULLIF(CAST(NULL AS DOUBLE),v) + (SELECT x FROM VALUES (1D),(2D) q(x)) AS r FROM VALUES (1D) t(v)'),
        ('live_subquery_int', 'SELECT NULLIF(1,v) + (SELECT x FROM VALUES (1),(2) q(x)) AS r FROM VALUES (2) t(v)'),
        ('live_subquery_double', 'SELECT NULLIF(1D,v) + (SELECT x FROM VALUES (1D),(2D) q(x)) AS r FROM VALUES (2D) t(v)'),
        ('historical_zero_literal', "SELECT NULLIF(CAST(v AS DOUBLE),0D) AS r FROM VALUES (0,'-0.0'),(1,NULL),(2,'2') t(k,v) ORDER BY k"),
        ('historical_zero_cast', "SELECT NULLIF(CAST(b AS DOUBLE), CAST(0 AS DOUBLE)) AS r FROM VALUES (0,'-0.0'),(1,NULL),(2,'2') t(id,b) ORDER BY id"),
        ('integer_control', 'SELECT NULLIF(a,b) AS r FROM VALUES (0,1,1),(1,1,2),(2,NULL,2),(3,1,NULL) t(id,a,b) ORDER BY id'),
        ('string_control', "SELECT NULLIF(a,b) AS r FROM VALUES (0,'a','a'),(1,'a','b'),(2,NULL,'b'),(3,'a',NULL) t(id,a,b) ORDER BY id"),
        ('decimal_control', 'SELECT NULLIF(CAST(a AS DECIMAL(10,2)),CAST(b AS DECIMAL(10,2))) AS r FROM VALUES (0,1,1),(1,1,2),(2,NULL,2),(3,1,NULL) t(id,a,b) ORDER BY id'),
    ]
    assert len(dict(queries)) == len(queries)
    for name, sql in queries:
        for ansi in [True, False]:
            spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
            try:
                frame = spark.sql(sql)
                schema = frame.schema.jsonValue()
                dtype = frame.schema.fields[0].dataType
                def encode(value):
                    if value is None:
                        return None
                    if isinstance(dtype, (FloatType, DoubleType)):
                        return 'nan' if math.isnan(value) else struct.pack('!f' if isinstance(dtype, FloatType) else '!d', value).hex()
                    return str(value).lower() if isinstance(value, bool) else str(value)
                result = {'status': 'ok', 'schema': schema, 'rows': [[encode(row[0])] for row in frame.collect()]}
            except Exception as error:
                condition = getattr(error, 'getCondition', lambda: None)()
                cause = getattr(error, 'java_exception', None)
                for _ in range(16):
                    if condition in {'CAST_INVALID_INPUT', 'SCALAR_SUBQUERY_TOO_MANY_ROWS'} or cause is None:
                        break
                    try:
                        condition = cause.getCondition()
                    except Exception:
                        pass
                    cause = cause.getCause()
                if condition not in {'CAST_INVALID_INPUT', 'SCALAR_SUBQUERY_TOO_MANY_ROWS'}:
                    raise
                result = {'status': 'error', 'condition': condition}
            assert (result['status'] == 'error') == ((ansi and '_live_bad_' in name) or name.startswith('live_subquery_')), (name, ansi, result)
            observations.append({'id': name, 'sql': sql, 'ansi': ansi, 'expected': result})
    metadata = {'spark_version': spark.version, 'java_version': spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
                'timezone': 'UTC', 'inputs': inputs}
    Path(__file__).with_name('nullif-float-spark.jsonl').write_text(
        '\n'.join(json.dumps(record, separators=(',', ':')) for record in [metadata, *observations])+'\n')
    print(json.dumps({'observations': len(observations), 'input_rows_per_type': len(values)}))
finally:
    spark.stop()
