"""Capture ARRAY_CONTAINS references with PySpark 4.2.0 and Java 21.

Run this file from a Python environment with pyspark==4.2.0. The adjacent JSONL
file records Spark results and input data; Rust tests need no Python or Spark.
Known differences remain labeled separately from supported-input assertions.
"""
import json
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable

CASES = [
    ('miss_with_null', 'ARRAY(1, NULL)', '2'),
    ('hit_with_null', 'ARRAY(1, NULL)', '1'),
    ('miss_without_null', 'ARRAY(1, 3)', '2'),
    ('hit_first', 'ARRAY(1, 2, 3)', '1'),
    ('hit_last', 'ARRAY(1, 2, 3)', '3'),
    ('duplicate_hit', 'ARRAY(1, NULL, 1)', '1'),
    ('duplicate_miss', 'ARRAY(1, NULL, 1)', '2'),
    ('empty', 'CAST(ARRAY() AS ARRAY<INT>)', '2'),
    ('empty_null_lookup', 'CAST(ARRAY() AS ARRAY<INT>)', 'CAST(NULL AS INT)'),
    ('null_array', 'CAST(NULL AS ARRAY<INT>)', '1'),
    ('null_array_null_lookup', 'CAST(NULL AS ARRAY<INT>)', 'CAST(NULL AS INT)'),
    ('null_lookup', 'ARRAY(1, 2)', 'CAST(NULL AS INT)'),
    ('all_null', 'CAST(ARRAY(NULL, NULL) AS ARRAY<INT>)', '1'),
    ('all_null_lookup', 'CAST(ARRAY(NULL, NULL) AS ARRAY<INT>)', 'CAST(NULL AS INT)'),
    ('string_hit', "ARRAY('a', NULL, 'b')", "'b'"),
    ('string_miss', "ARRAY('a', NULL, 'b')", "'c'"),
    ('string_empty_value', "ARRAY('a', '')", "''"),
    ('string_null_lookup', "ARRAY('a', '')", 'CAST(NULL AS STRING)'),
    ('numeric_coercion_hit', 'ARRAY(CAST(1 AS INT), CAST(NULL AS INT))', 'CAST(1 AS BIGINT)'),
    ('numeric_coercion_miss', 'ARRAY(CAST(1 AS INT), CAST(NULL AS INT))', 'CAST(2 AS BIGINT)'),
    ('decimal_same_type_hit', 'ARRAY(CAST(1.20 AS DECIMAL(10,2)), CAST(NULL AS DECIMAL(10,2)))', 'CAST(1.20 AS DECIMAL(10,2))'),
    ('decimal_same_type_miss', 'ARRAY(CAST(1.20 AS DECIMAL(10,2)), CAST(NULL AS DECIMAL(10,2)))', 'CAST(2.10 AS DECIMAL(10,2))'),
    ('untyped_null_elements', 'ARRAY(NULL, NULL)', '1'),
    ('decimal_hit', 'ARRAY(CAST(1.20 AS DECIMAL(10,2)), CAST(NULL AS DECIMAL(10,2)))', 'CAST(1.2 AS DECIMAL(8,1))'),
    ('decimal_miss', 'ARRAY(CAST(1.20 AS DECIMAL(10,2)), CAST(NULL AS DECIMAL(10,2)))', 'CAST(2.1 AS DECIMAL(8,1))'),
    ('nested_hit', 'ARRAY(ARRAY(1, NULL), ARRAY(2, 3))', 'ARRAY(1, NULL)'),
    ('nested_miss', 'ARRAY(ARRAY(1, NULL), CAST(NULL AS ARRAY<INT>))', 'ARRAY(2, NULL)'),
    ('signed_zero', 'ARRAY(CAST(-0.0 AS DOUBLE))', 'CAST(0.0 AS DOUBLE)'),
    ('nan', "ARRAY(CAST('NaN' AS DOUBLE))", "CAST('NaN' AS DOUBLE)"),
    ('live_bad_lookup', 'ARRAY(1, 2)', "CAST('bad' AS INT)"),
    ('live_bad_array', "ARRAY(CAST('bad' AS INT), 2)", '1'),
    ('bad_array_null_lookup', "ARRAY(CAST('bad' AS INT), 2)", 'CAST(NULL AS INT)'),
    ('null_array_bad_lookup', 'CAST(NULL AS ARRAY<INT>)', "CAST('bad' AS INT)"),
]
queries = [{'id':name, 'sql':f'SELECT ARRAY_CONTAINS({array}, {needle}) AS r'} for name,array,needle in CASES]
for name, array, needle in CASES:
    queries.append({'id':name+'_column', 'sql':f'SELECT ARRAY_CONTAINS(a, n) AS r FROM (SELECT {array} AS a, {needle} AS n FROM range(2)) t'})
queries += [
 {'id':'invalid_array_type', 'sql':'SELECT ARRAY_CONTAINS(1, 1) AS r'},
 {'id':'invalid_array_type_null_lookup', 'sql':'SELECT ARRAY_CONTAINS(1, CAST(NULL AS INT)) AS r'},
 {'id':'invalid_null_scalar_array_type', 'sql':'SELECT ARRAY_CONTAINS(CAST(NULL AS INT), 1) AS r'},
 {'id':'mixed_columns', 'sql':'SELECT ARRAY_CONTAINS(a, n) AS r FROM VALUES (0, ARRAY(1, NULL), 2), (1, ARRAY(1, NULL), 1), (2, ARRAY(1, 3), 2), (3, CAST(NULL AS ARRAY<INT>), 1), (4, ARRAY(1, 2), CAST(NULL AS INT)), (5, CAST(ARRAY() AS ARRAY<INT>), 2), (6, CAST(ARRAY() AS ARRAY<INT>), CAST(NULL AS INT)) t(id, a, n) ORDER BY id'},
 {'id':'empty_batch', 'sql':'SELECT ARRAY_CONTAINS(ARRAY(CAST(id AS INT), NULL), CAST(id + 1 AS INT)) AS r FROM range(0)'},
 {'id':'size_null_control', 'sql':'SELECT SIZE(CAST(NULL AS ARRAY<INT>)) AS r'},
 {'id':'array_position_control', 'sql':'SELECT ARRAY_POSITION(ARRAY(1, NULL, 3), 2) AS r'},
]
assert len({case['id'] for case in queries}) == len(queries)
spark = SparkSession.builder.master('local[1]').appName('dar-array-null-reference').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1').config('spark.sql.session.timeZone','UTC').config('spark.sql.legacy.sizeOfNull','true').getOrCreate()
try:
    assert spark.version == '4.2.0', spark.version
    spark.sparkContext.setLogLevel('ERROR')
    integer_rows = [
        (0, [1, None], 2), (1, [1, None], 1), (2, [1, 3], 2),
        (3, None, 1), (4, [1, 2], None), (5, [], 2), (6, [], None),
        (7, [None, None], 1), (8, [1, 1], 1), (9, [3, 2, 1], 1),
    ]
    spark.createDataFrame(integer_rows, 'id INT NOT NULL, a ARRAY<INT>, n INT').createOrReplaceTempView('integer_inputs')
    string_rows = [(0, ['a', None], 'b'), (1, ['a', None], 'a'), (2, [], ''),
                   (3, ['', 'a'], ''), (4, None, 'a'), (5, ['a'], None)]
    spark.createDataFrame(string_rows, 'id INT NOT NULL, a ARRAY<STRING>, n STRING').createOrReplaceTempView('string_inputs')
    spark.sql('SELECT id, NULL AS a FROM range(3)').createOrReplaceTempView('null_inputs')
    queries.extend([
        {'id':'registered_integer_columns','sql':'SELECT ARRAY_CONTAINS(a,n) AS r FROM integer_inputs ORDER BY id'},
        {'id':'registered_integer_scalar_lookup','sql':'SELECT ARRAY_CONTAINS(a,1) AS r FROM integer_inputs ORDER BY id'},
        {'id':'registered_integer_scalar_array','sql':'SELECT ARRAY_CONTAINS(ARRAY(1,NULL),n) AS r FROM integer_inputs ORDER BY id'},
        {'id':'registered_string_columns','sql':'SELECT ARRAY_CONTAINS(a,n) AS r FROM string_inputs ORDER BY id'},
        {'id':'registered_string_scalar_lookup','sql':"SELECT ARRAY_CONTAINS(a,'a') AS r FROM string_inputs ORDER BY id"},
        {'id':'registered_null_scalar_lookup','sql':'SELECT ARRAY_CONTAINS(a,CAST(NULL AS INT)) AS r FROM integer_inputs ORDER BY id'},
        {'id':'registered_null_scalar_array','sql':'SELECT ARRAY_CONTAINS(CAST(NULL AS ARRAY<INT>),n) AS r FROM integer_inputs ORDER BY id'},
        {'id':'registered_null_type_array','sql':'SELECT ARRAY_CONTAINS(a,1) AS r FROM null_inputs ORDER BY id'},
        {'id':'registered_null_type_empty_batch','sql':'SELECT ARRAY_CONTAINS(a,1) AS r FROM null_inputs WHERE id < 0'},
        {'id':'registered_empty_batch','sql':'SELECT ARRAY_CONTAINS(a,n) AS r FROM integer_inputs WHERE id < 0'},
    ])
    observations=[]
    for ansi in [True, False]:
        spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
        for case in queries:
            stage='planning'
            try:
                frame=spark.sql(case['sql']);schema=frame.schema.jsonValue();stage='execution'
                result={'status':'ok','schema':schema,'rows':[list(row) for row in frame.collect()]}
            except Exception as error:
                condition=getattr(error,'getCondition',lambda: None)()
                if condition not in {'CAST_INVALID_INPUT', 'DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE', 'DATATYPE_MISMATCH.NULL_TYPE'}:
                    raise
                result={'status':'error','stage':stage,'condition':condition}
            observations.append({**case,'ansi':ansi,'expected':result})
    expected_errors = {
        (case['id'], ansi) for case in queries for ansi in [True, False]
        if case['id'].startswith(('invalid_', 'registered_null_type_'))
        or (ansi and case['id'] in {'live_bad_lookup', 'live_bad_array', 'live_bad_lookup_column', 'live_bad_array_column'})
    }
    assert {(r['id'], r['ansi']) for r in observations if r['expected']['status'] == 'error'} == expected_errors
    inputs = {
        "spark_version": spark.version, "timezone": spark.conf.get('spark.sql.session.timeZone'),
        "legacy_size_of_null": spark.conf.get('spark.sql.legacy.sizeOfNull'),
        "java_version": spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
        "null_row_count": 3,
        "integer_rows": integer_rows, "string_rows": string_rows,
    }
    for observation in observations:
        name, ansi = observation['id'], observation['ansi']
        if name in {'decimal_hit', 'decimal_miss', 'decimal_hit_column', 'decimal_miss_column'}:
            observation['retained_difference'] = 'mixed_decimal_coercion'
        elif ansi and name in {'bad_array_null_lookup_column', 'null_array_bad_lookup_column'}:
            observation['retained_difference'] = 'nullable_projection_evaluation'
        elif name in {'registered_null_type_array', 'registered_null_type_empty_batch'}:
            observation['retained_difference'] = 'null_type_admission'
        elif not ansi and name == 'size_null_control':
            observation['retained_difference'] = 'legacy_size_of_null'
    target = Path(__file__).with_name('array-contains-spark.jsonl')
    target.write_text('\n'.join(json.dumps(record) for record in [inputs, *observations])+'\n')
    print(json.dumps({"observations": len(observations), "known_differences": sum('retained_difference' in r for r in observations)}))

finally:
    spark.stop()
