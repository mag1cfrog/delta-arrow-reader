"""Capture independent Spark stages for constant SLICE conditional boundaries."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import traceback
from zipfile import ZipFile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
PACKAGE = Path('/home/hanbo/repo/delta-arrow-reader-spark-sql/crates/delta-arrow-reader-spark-sql/tests/fixtures/slice-coordinates')
helper_path = PACKAGE / 'capture_spark.py'
assert hashlib.sha256(helper_path.read_bytes()).hexdigest() == '7626fac85ff1e8e061dd900e084f5402554737d41e1aff837112c0f3d1f2b79a'
spec = importlib.util.spec_from_file_location('coordinate_reference', helper_path)
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
scenarios = [
    ('positive_overflow_invalid_start_then', 'SELECT CASE WHEN SIZE(SLICE(ARRAY(1,2,2,3),4,2147483647)) = 0 THEN SLICE(ARRAY(1,2),0,1) ELSE ARRAY(9) END AS r'),
    ('positive_overflow_invalid_length_then', 'SELECT CASE WHEN SIZE(SLICE(ARRAY(1,2,2,3),4,2147483647)) = 0 THEN SLICE(ARRAY(1,2),1,-1) ELSE ARRAY(9) END AS r'),
    ('negative_overflow_invalid_start_then', 'SELECT CASE WHEN SIZE(SLICE(ARRAY(1,2,2,3),-1,2147483647)) = 0 THEN SLICE(ARRAY(1,2),0,1) ELSE ARRAY(9) END AS r'),
    ('positive_overflow_invalid_start_else', 'SELECT CASE WHEN SIZE(SLICE(ARRAY(1,2,2,3),4,2147483647)) = 0 THEN ARRAY(9) ELSE SLICE(ARRAY(1,2),0,1) END AS r'),
    ('nonoverflow_invalid_start_then', 'SELECT CASE WHEN SIZE(SLICE(ARRAY(1,2,2,3),4,1)) = 0 THEN SLICE(ARRAY(1,2),0,1) ELSE ARRAY(9) END AS r'),
    ('provider_overflow_invalid_start_then', 'SELECT k, CASE WHEN SIZE(SLICE(a_nn,4,2147483647)) = 0 THEN SLICE(ARRAY(1,2),0,1) ELSE ARRAY(9) END AS r FROM slice_fields ORDER BY k'),
]
source_manifest = json.loads((PACKAGE / 'manifest.json').read_text())
inputs = [item for item in source_manifest['inputs'] if item['name'] == 'slice_fields']
manifest = {'group':'slice-coordinate-conditional-probe-894-v2', 'spark_version':helper.VERSION, 'spark_revision':helper.REVISION, 'timezone':'UTC', 'inputs':inputs, 'scenarios':[{'id':name,'sql':sql,'ordered':True} for name,sql in scenarios], 'capture_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'helper_sha256':hashlib.sha256(helper_path.read_bytes()).hexdigest()}
manifest_path = ROOT / '894-conditional-coordinate-manifest-v2.json'
raw_path = ROOT / '894-conditional-coordinate-spark-v2.jsonl'
assert not manifest_path.exists() and not raw_path.exists()
manifest_path.write_text(json.dumps(manifest,indent=2)+'\n')
assert os.environ.get('TZ') == 'UTC'
os.environ['PYSPARK_PYTHON'] = sys.executable
import pyspark
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession, types

jar, = (Path(pyspark.__file__).parent / 'jars').glob('spark-common-utils_*.jar')
with ZipFile(jar) as archive:
    build = archive.read('spark-version-info.properties').decode()
assert f'revision={helper.REVISION}\n' in build and f'version={helper.VERSION}\n' in build
spark = (SparkSession.builder.master('local[1]').appName(manifest['group'])
         .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
         .config('spark.sql.session.timeZone','UTC').getOrCreate())
try:
    spark.sparkContext.setLogLevel('ERROR')
    for item in inputs:
        schema = types.StructType.fromJson(item['schema'])
        frame = spark.createDataFrame([helper.decode_input(row,item['schema']) for row in item['rows']],schema)
        frame.createOrReplaceTempView(item['name'])
        assert frame.schema.jsonValue() == item['schema']
        assert [helper.encode_result(row) for row in frame.orderBy('k').collect()] == item['rows']
    for item in manifest['scenarios']:
        spark._jsparkSession.sessionState().sqlParser().parsePlan(item['sql'])
    records = [dict(manifest,manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),spark_build=build,provider_input_roundtrip_checked=True)]
    for ansi in [True,False]:
        spark.conf.set('spark.sql.ansi.enabled',str(ansi).lower())
        for item in manifest['scenarios']:
            record = dict(item,ansi=ansi,timezone='UTC')
            stages,plans,phase = {},{},'sql'
            try:
                frame = spark.sql(item['sql']); execution = frame._jdf.queryExecution()
                phase = 'public_schema'; stages['public'] = frame.schema.jsonValue(); stages['resolved'] = stages['public']
                for name,getter,phase in [('analyzed',execution.analyzed,'analyzed_schema'),('optimized',execution.optimizedPlan,'optimized_schema'),('physical',execution.sparkPlan,'physical_schema'),('executed_before_collect',execution.executedPlan,'executed_schema')]:
                    plan = getter(); stages[name] = json.loads(plan.schema().json()); plans[name] = plan.toString()
                phase = 'collect'; rows = [helper.encode_result(row) for row in frame.collect()]
                record['spark_collected_rows'] = rows
                phase = 'executed_schema_after_collect'; plan = execution.executedPlan(); stages['executed'] = json.loads(plan.schema().json()); plans['executed'] = plan.toString()
                record['expected'] = {'schema':stages['public'],'rows':rows}
            except Exception as error:
                if not isinstance(error,PySparkException) and getattr(error,'java_exception',None) is None:
                    raise
                details = helper.error_details(error,PySparkException)
                record.update(expected={'condition':details['condition']},spark_error=str(error),spark_error_phase=phase,spark_error_class=type(error).__name__,spark_error_details=details,spark_error_traceback=traceback.format_exc())
            record.update(schema_stages=stages,spark_plans=plans); records.append(record)
    assert len(records) == 13
    raw_path.write_text(''.join(json.dumps(record,separators=(',',':'))+'\n' for record in records))
    print(json.dumps({'observations':12,'raw_sha256':hashlib.sha256(raw_path.read_bytes()).hexdigest(),'outcomes':[{'id':r['id'],'ansi':r['ansi'],'phase':r.get('spark_error_phase'),'condition':r['expected'].get('condition'),'rows':r['expected'].get('rows')} for r in records[1:]]}))
finally:
    spark.stop()
