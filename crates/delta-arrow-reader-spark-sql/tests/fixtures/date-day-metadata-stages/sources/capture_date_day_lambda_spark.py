"""Capture lambda day operands and the empty-schema conditional diagnostic."""
import json
import os
from pathlib import Path
import sys
from py4j.protocol import Py4JError
from pyspark.sql import SparkSession
from capture_date_day_arithmetic_spark import capture_query, encode

os.environ['PYSPARK_PYTHON'] = sys.executable
spark = (SparkSession.builder.master('local[1]').appName('dar-date-day-lambda')
         .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '1')
         .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
try:
    assert spark.version == '4.2.0'
    spark.sparkContext.setLogLevel('ERROR')
    records = [{'spark_version':spark.version, 'timezone':'UTC', 'date_encoding':'unix_date_array'}]
    for ansi in [True, False]:
        spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
        for function in ['date_add', 'dateadd', 'date_sub']:
            for label, values, start in [
                ('valid_string', "array('1')", "DATE '1970-01-01'"),
                ('bad_string', "array('bad')", "DATE '1970-01-01'"),
                ('null_string', 'array(CAST(NULL AS STRING))', "DATE '1970-01-01'"),
                ('integer', 'array(1,-1,CAST(NULL AS INT))', "DATE '1970-01-01'"),
                ('null_start_bad_string', "array('bad')", 'CAST(NULL AS DATE)'),
            ]:
                sql = f'SELECT transform({values}, x -> {function}({start},x)) AS r'
                try:
                    frame = spark.sql(sql)
                    schema = frame.schema.jsonValue()
                    observed = frame.selectExpr('transform(r, d -> unix_date(d)) AS r')
                    rows = [encode(row, observed.schema) for row in observed.collect()]
                    expected = {'schema':schema, 'rows':rows}
                except Exception as error:
                    condition = getattr(error, 'getCondition', lambda: None)()
                    cause = getattr(error, 'java_exception', None)
                    while condition is None and cause is not None:
                        try:
                            condition = cause.getCondition()
                        except Py4JError:
                            pass
                        cause = cause.getCause()
                    assert condition is not None, (sql, str(error))
                    expected = {'condition':condition}
                records.append({'id':f'{function}_lambda_{label}', 'ansi':ansi, 'sql':sql, 'expected':expected})
        sql='SELECT CASE WHEN false THEN missing ELSE 7 END AS r'
        records.append({'id':'conditional_missing_column','ansi':ansi,'sql':sql,'expected':capture_query(spark, sql)})
    assert len(records) == 33
    with Path(__file__).with_name('date-day-lambda-spark.jsonl').open('x') as out:
        out.write(''.join(json.dumps(r,separators=(',',':'))+'\n' for r in records))
    print(json.dumps({'observations':len(records)-1,'errors':sum('condition' in r['expected'] for r in records[1:])}))
finally:
    spark.stop()
