"""Record local preparation versus NULL propagation in Spark comparisons."""
import json
import os
from pathlib import Path
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from capture_temporal_comparison_spark import encode_reference_value

INPUT = ['bad', None, '2026-01-02']


def queries():
    for data_type in ['DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'INT', 'BOOLEAN']:
        value = {'INT': '1', 'BOOLEAN': 'true'}.get(data_type, '2026-01-02')
        peer = f'CAST(NULL AS {data_type})'
        local = f"VALUES (0, 'bad'), (1, CAST(NULL AS STRING)), (2, '{value}')"
        for source_name, source in [('local', f'({local}) t(k,s)'), ('registered', 'inputs')]:
            for name, expression in [
                ('equal', f's={peer}'), ('reverse', f'{peer}=s'),
                ('less', f's<{peer}'), ('not_equal', f's<>{peer}'),
                ('less_equal', f's<={peer}'), ('greater', f's>{peer}'),
                ('greater_equal', f's>={peer}'), ('reverse_less', f'{peer}<s'),
                ('reverse_greater', f'{peer}>s'),
                ('nullsafe', f's<=>{peer}'),
                ('case_unused', f'CASE WHEN false THEN s={peer} ELSE true END'),
                ('case_used', f'CASE WHEN true THEN s={peer} ELSE false END'),
                ('coalesce_unused', f'COALESCE(true,s={peer})'),
                ('coalesce_used', f'COALESCE(CAST(NULL AS BOOLEAN),s={peer})'),
                ('nullif_unused', f'NULLIF(CAST(NULL AS BOOLEAN),s={peer})'),
                ('nullif_used', f'NULLIF(true,s={peer})'),
            ]:
                yield f'{data_type}_{source_name}_{name}', f'SELECT {expression} AS r FROM {source} ORDER BY k'
            for suffix_name, suffix in [('where_false', ' WHERE false'), ('limit_zero', ' LIMIT 0')]:
                yield f'{data_type}_{source_name}_{suffix_name}', f'SELECT s={peer} AS r FROM {source}{suffix}'
        for name, sql in [
            ('literal', f"SELECT 'bad'={peer} AS r"),
            ('literal_reverse', f"SELECT {peer}='bad' AS r"),
            ('inner_empty', f'SELECT s={peer} AS r FROM (SELECT * FROM ({local}) t(k,s) WHERE false)'),
            ('valid', f"SELECT s={peer} AS r FROM VALUES ('{value}') t(s)"),
            ('explicit', f'SELECT CAST(s AS {data_type})={peer} AS r FROM ({local}) t(k,s) ORDER BY k'),
            ('explicit_reverse', f'SELECT {peer}=CAST(s AS {data_type}) AS r FROM ({local}) t(k,s) ORDER BY k'),
            ('try_cast', f'SELECT TRY_CAST(s AS {data_type})={peer} AS r FROM ({local}) t(k,s) ORDER BY k'),
            ('explicit_nonnull', f"SELECT CAST(s AS {data_type})=CAST('{value}' AS {data_type}) AS r FROM ({local}) t(k,s) ORDER BY k"),
            ('volatile_sibling', f'SELECT s={peer} AS r, CAST(rand()*0 AS INT) AS v FROM ({local}) t(k,s) ORDER BY k'),
        ]:
            yield f'{data_type}_{name}', sql
    yield 'nested_boolean', "SELECT (CAST(s AS INT)=1)=CAST(NULL AS BOOLEAN) AS r FROM VALUES ('bad') t(s)"


def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    spark = (SparkSession.builder.master('local[1]').appName('dar-local-null-comparison')
             .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
             .config('spark.sql.session.timeZone','UTC').getOrCreate())
    records=[]
    try:
        assert spark.version == '4.2.0', spark.version
        spark.sparkContext.setLogLevel('ERROR')
        spark.createDataFrame(list(enumerate(INPUT)),
            StructType([StructField('k',IntegerType(),False),StructField('s',StringType())])).createOrReplaceTempView('inputs')
        for name, sql in queries():
            for ansi in [True, False]:
                spark.conf.set('spark.sql.ansi.enabled',str(ansi).lower())
                try:
                    frame=spark.sql(sql)
                    schema=frame.schema.jsonValue()
                    rows=[[encode_reference_value(value,field['type']) for value,field in zip(row,schema['fields'])] for row in frame.collect()]
                    expected={'status':'ok','schema':schema,'rows':rows}
                except Exception as error:
                    condition=getattr(error,'getCondition',lambda:None)()
                    if condition not in ['CAST_INVALID_INPUT','INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION']:
                        raise
                    expected={'status':'error','condition':condition}
                record={'id':name,'sql':sql,'ansi':ansi,'expected':expected}
                if expected['status']=='error' and name.startswith('TIMESTAMP'):
                    record['error_representation']='timestamp_parser'
                records.append(record)
        assert len({(r['id'],r['ansi']) for r in records})==len(records)
        metadata={'spark_version':spark.version,'java_version':spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),'external_input':INPUT}
        Path(__file__).with_name('local-null-comparison-spark.jsonl').write_text('\n'.join(json.dumps(r,separators=(',',':')) for r in [metadata,*records])+'\n')
        print(json.dumps({'observations':len(records),'errors':sum(r['expected']['status']=='error' for r in records)}))
    finally:
        spark.stop()


if __name__=='__main__':
    main()
