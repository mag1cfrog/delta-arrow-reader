"""Capture ordinary aggregate and window values and executed types with Spark 4.2.0."""

import hashlib
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from capture_floating_group_keys_spark import encode
from capture_window_name_stages_spark import decode
from recapture_string_binary_spark import runtime_identity


def main():
    output = Path(sys.argv[1])
    assert not output.exists()
    manifest_path = Path(__file__).with_name('cases.json')
    manifest = json.loads(manifest_path.read_text())
    cases = manifest['cases']
    assert len(cases) == len({case['id'] for case in cases})
    os.environ['PYSPARK_PYTHON'] = sys.executable
    from pyspark.sql import SparkSession, types

    spark = (SparkSession.builder.master('local[2]').appName('dar-aggregate-window-results')
             .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
             .config('spark.sql.adaptive.enabled', 'false')
             .config('spark.sql.session.timeZone', manifest['timezone']).getOrCreate())
    try:
        assert spark.version == manifest['spark_version']
        spark.sparkContext.setLogLevel('ERROR')
        for provider in manifest['inputs']:
            schema = types.StructType.fromJson(provider['schema'])
            rows = [decode(row, schema) for row in provider['rows']]
            spark.createDataFrame(rows, schema).repartition(2).createOrReplaceTempView(provider['name'])
        records = [{**runtime_identity(spark), 'timezone': manifest['timezone'],
                    'inputs': manifest['inputs'], 'source_partitions': 2,
                    'manifest_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest()}]
        for ansi in [False, True]:
            spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
            for case in cases:
                print(json.dumps({'id': case['id'], 'ansi': ansi}), flush=True)
                frame = spark.sql(case['sql'])
                rows = [encode(row, frame.schema) for row in frame.collect()]
                if not case['ordered']:
                    rows.sort(key=lambda row: json.dumps(row, separators=(',', ':')))
                schema = json.loads(frame._jdf.queryExecution().executedPlan().schema().json())
                records.append({**case, 'ansi': ansi, 'rows': rows, 'schema': schema})
        with output.open('x') as destination:
            for record in records:
                destination.write(json.dumps(record, separators=(',', ':'))+'\n')
        print(json.dumps({'references': len(cases)*2}), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
