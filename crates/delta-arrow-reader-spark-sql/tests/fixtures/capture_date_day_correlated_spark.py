"""Observe correlated integer/string day operands and NULL-date short circuit."""
import json
import os
from pathlib import Path
import sys
from pyspark.sql import SparkSession, types
from capture_date_day_arithmetic_spark import capture_query, encode

os.environ['PYSPARK_PYTHON'] = sys.executable
spark = (SparkSession.builder.master('local[1]').appName('dar-date-day-correlated')
         .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '1')
         .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
try:
    assert spark.version == '4.2.0', spark.version
    spark.sparkContext.setLogLevel('ERROR')
    specs = [
        ('date_day_outer_text', types.StringType(), [(None, 'bad'), (0, None), (0, ' 1 '), (1, '-1')]),
        ('date_day_outer_valid', types.StringType(), [(0, '1'), (1, '-1'), (0, None)]),
        ('date_day_outer_int', types.IntegerType(), [(None, 1), (0, None), (0, 2147483647), (1, -1)]),
    ]
    inputs = []
    for name, day_type, rows in specs:
        schema = types.StructType([types.StructField('base', types.IntegerType(), True),
                                   types.StructField('days', day_type, True)])
        spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
        inputs.append({'name': name, 'schema': schema.jsonValue(), 'rows': [encode(row, schema) for row in rows]})
    records = [{'spark_version': spark.version, 'timezone': 'UTC', 'date_encoding': 'unix_date', 'inputs': inputs}]
    for ansi in [True, False]:
        spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
        for function in ['date_add', 'dateadd', 'date_sub']:
            for label, table, date in [
                ('valid_text', 'date_day_outer_valid', "DATE '1970-01-01'"),
                ('invalid_text', 'date_day_outer_text', "DATE '1970-01-01'"),
                ('nullable_date', 'date_day_outer_text', 'date_from_unix_date(o.base)'),
                ('integer', 'date_day_outer_int', 'date_from_unix_date(o.base)'),
            ]:
                sql = f'SELECT (SELECT {function}({date},o.days)) AS r FROM {table} o'
                records.append({'id': f'{function}_correlated_{label}', 'ansi': ansi, 'sql': sql,
                                'expected': capture_query(spark, sql)})
    assert len(records) == 25
    target = Path(__file__).with_name('date-day-correlated-spark.jsonl')
    with target.open('x') as out:
        out.write(''.join(json.dumps(record, separators=(',', ':')) + '\n' for record in records))
    print(json.dumps({'observations':len(records)-1, 'errors':sum('condition' in r['expected'] for r in records[1:])}))
finally:
    spark.stop()
