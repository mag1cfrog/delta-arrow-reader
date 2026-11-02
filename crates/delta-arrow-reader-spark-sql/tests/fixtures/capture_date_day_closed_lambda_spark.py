"""Capture DATE day admission for self-contained higher-order STRING expressions."""
import json
import os
from pathlib import Path
import sys
from pyspark.sql import SparkSession
from capture_date_day_arithmetic_spark import capture_query
os.environ['PYSPARK_PYTHON'] = sys.executable
spark = (SparkSession.builder.master('local[1]').appName('dar-date-day-closed-lambda')
         .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
         .config('spark.sql.session.timeZone','UTC').getOrCreate())
try:
    assert spark.version == '4.2.0'
    spark.sparkContext.setLogLevel('ERROR')
    records=[{'spark_version':spark.version,'timezone':'UTC','date_encoding':'unix_date'}]
    for ansi in [True,False]:
        spark.conf.set('spark.sql.ansi.enabled',str(ansi).lower())
        for function in ['date_add','dateadd','date_sub']:
            for label,days in [('closed_string',"element_at(transform(array('1'), x -> x),1)"),('closed_int_cast',"CAST(element_at(transform(array(1), x -> x),1) AS STRING)")]:
                sql=f"SELECT {function}(DATE '1970-01-01',{days}) AS r"
                records.append({'id':f'{function}_{label}','ansi':ansi,'sql':sql,'expected':capture_query(spark,sql)})
    assert len(records)==13
    with Path(__file__).with_name('date-day-closed-lambda-spark.jsonl').open('x') as out:
        out.write(''.join(json.dumps(r,separators=(',',':'))+'\n' for r in records))
    print(json.dumps({'observations':len(records)-1,'errors':sum('condition' in r['expected'] for r in records[1:])}))
finally:
    spark.stop()
