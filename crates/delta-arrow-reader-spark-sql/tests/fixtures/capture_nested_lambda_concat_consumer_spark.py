"""Capture issue906's exact CONCAT consumer and two isolation controls on Spark."""
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession, types
from capture_floating_group_keys_spark import encode

output = Path(sys.argv[1])
assert not output.exists()
jar, = (Path(pyspark.__file__).parent / 'jars').glob('spark-common-utils_*.jar')
with ZipFile(jar) as archive:
    build = archive.read('spark-version-info.properties').decode()
assert 'revision=32f7299601108917fb01920a54e084595b7b3bf8' in build
schema = types.StructType([
    types.StructField('k', types.IntegerType(), False),
    types.StructField('payload', types.StringType(), False, {'probe':'kept'}),
    types.StructField('a', types.ArrayType(types.IntegerType(), False), True),
    types.StructField('starts', types.ArrayType(types.IntegerType(), True), False),
    types.StructField('bias', types.IntegerType(), False),
    types.StructField('n', types.IntegerType(), False),
])
rows = [(0,'kept',None,[0,1,None],1,1),
        (1,'kept',[1,2],[0,1,None],1,1),
        (2,'kept',[3,4],[1,2,None],0,1)]
first = "CAST(CONCAT('', CAST(x + bias AS STRING)) AS INT)"
second = "CAST(CONCAT('', CAST(ARRAY_MAX(TRANSFORM(starts, y -> y + bias)) AS STRING)) AS INT)"
plain_second = 'ARRAY_MAX(TRANSFORM(starts, y -> y + bias))'
cases = [
    ('full_original', first, second),
    ('without_first_concat', 'x + bias', second),
    ('without_second_concat', first, plain_second),
]
expected_rows = [['0','kept',[None,None,None],None],
                 ['1','kept',[['1'],['2'],None],['2']],
                 ['2','kept',[['3'],['4'],None],['4']]]
os.environ['PYSPARK_PYTHON'] = sys.executable
spark = (SparkSession.builder.master('local[1]').appName('dar-821-concat-consumer')
         .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
         .config('spark.sql.session.timeZone','UTC').config('spark.sql.caseSensitive','false')
         .getOrCreate())
try:
    assert spark.version == '4.2.0'
    spark.sparkContext.setLogLevel('ERROR')
    spark.createDataFrame(rows, schema).createOrReplaceTempView('slice_projection_input')
    records = [dict(spark_version=spark.version, spark_build=build, timezone='UTC',
                    case_sensitive=False, issue=906, purpose='fresh independent consumer capture',
                    row_order='as collected from explicit ORDER BY k; no sorting',
                    inputs=[dict(name='slice_projection_input',schema=schema.jsonValue(),
                                 rows=[encode(row,schema,input_value=True) for row in rows])])]
    for ansi in [True, False]:
        spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
        for name, start, captured in cases:
            sql = f'SELECT k,payload,TRANSFORM(starts,x -> SLICE(a,{start},n)) AS r,SLICE(a,{captured},n) AS captured_start FROM slice_projection_input ORDER BY k'
            frame = spark.sql(sql)
            public = frame.schema.jsonValue()
            actual_rows = [encode(row,frame.schema) for row in frame.collect()]
            execution = frame._jdf.queryExecution()
            stages = dict(analyzed=json.loads(execution.analyzed().schema().json()),
                          optimized=json.loads(execution.optimizedPlan().schema().json()),
                          executed=json.loads(execution.executedPlan().schema().json()))
            assert actual_rows == expected_rows, (name,ansi,actual_rows)
            assert public == stages['analyzed'] == stages['executed']
            records.append(dict(id=name,input='slice_projection_input',ansi=ansi,sql=sql,
                                phase='success',expected=dict(schema=public,rows=actual_rows),
                                schema_stages=stages))
    assert len(records)==7 and len({(x['id'],x['ansi']) for x in records[1:]})==6
    with output.open('x') as stream:
        for record in records:
            stream.write(json.dumps(record,separators=(',',':'))+'\n')
    print(json.dumps(dict(records=6,output=str(output),sha256=hashlib.sha256(output.read_bytes()).hexdigest())))
finally:
    spark.stop()
