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
queries += [
    {'id':'projection_nested_null_lookup', 'sql':"SELECT ARRAY_CONTAINS(a, n) AS r FROM (SELECT a, n FROM (SELECT a, n FROM (SELECT ARRAY(CAST('bad' AS INT), 2) AS a, CAST(NULL AS INT) AS n FROM range(2)) x) y) z"},
    {'id':'projection_nested_null_array', 'sql':"SELECT ARRAY_CONTAINS(a, n) AS r FROM (SELECT a, n FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, CAST('bad' AS INT) AS n FROM range(2)) x) y"},
    {'id':'projection_cte_null_lookup', 'sql':"WITH source AS (SELECT ARRAY(CAST('bad' AS INT), 2) AS a, CAST(NULL AS INT) AS n FROM range(2)) SELECT ARRAY_CONTAINS(a,n) AS r FROM source"},
    {'id':'projection_null_lookup_division', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT ARRAY(1 DIV 0) AS a, CAST(NULL AS BIGINT) AS n FROM range(2)) t"},
    {'id':'projection_null_array_division', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT CAST(NULL AS ARRAY<BIGINT>) AS a, 1 DIV 0 AS n FROM range(2)) t"},
    {'id':'projection_cast_needed_elsewhere', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r, ARRAY_CONTAINS(a,1) AS needed FROM (SELECT ARRAY(CAST('bad' AS INT),2) AS a, CAST(NULL AS INT) AS n FROM range(2)) t"},
    {'id':'projection_lookup_needed_elsewhere', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r, n AS needed FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, CAST('bad' AS INT) AS n FROM range(2)) t"},
    {'id':'projection_nullable_not_constant', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT ARRAY(CAST('bad' AS INT),2) AS a, CASE WHEN id=0 THEN CAST(NULL AS INT) ELSE 1 END AS n FROM range(2)) t ORDER BY r"},
    {'id':'projection_division_needed_elsewhere', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r, n AS needed FROM (SELECT CAST(NULL AS ARRAY<BIGINT>) AS a, 1 DIV 0 AS n FROM range(2)) t"},
    {'id':'projection_renamed_columns', 'sql':"SELECT ARRAY_CONTAINS(items, needle) AS r FROM (SELECT ARRAY(CAST('bad' AS INT),2), CAST(NULL AS INT) FROM range(2)) t(items, needle)"},
    {'id':'projection_cast_null_column', 'sql':"SELECT ARRAY_CONTAINS(a,CAST(n AS BIGINT)) AS r FROM (SELECT ARRAY(CAST('bad' AS INT),2) AS a, CAST(NULL AS INT) AS n FROM range(2)) t"},
    {'id':'projection_nonnull_values', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT ARRAY(CAST(id AS INT),NULL) AS a, CAST(id AS INT) AS n FROM range(2)) t"},
    {'id':'projection_null_lookup_empty', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT ARRAY(CAST('bad' AS INT),2) AS a, CAST(NULL AS INT) AS n FROM range(0)) t"},
    {'id':'projection_null_array_empty', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, CAST('bad' AS INT) AS n FROM range(0)) t"},
    {'id':'projection_values_null_lookup', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT ARRAY(CAST(v AS INT)) AS a, CAST(NULL AS INT) AS n FROM VALUES ('bad') t(v)) s"},
    {'id':'projection_values_null_array', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, CAST(v AS INT) AS n FROM VALUES ('bad') t(v)) s"},
    {'id':'projection_inline_values_error', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM VALUES (ARRAY(CAST('bad' AS INT)), CAST(NULL AS INT)) t(a,n)"},
    {'id':'projection_subquery_cast', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, (SELECT CAST('bad' AS INT)) AS n FROM range(2)) t"},
    {'id':'projection_subquery_multiple_rows', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT CAST(NULL AS ARRAY<INT>) AS a, (SELECT CAST(id AS INT) FROM range(2)) AS n FROM range(2)) t"},
    {'id':'projection_subquery_needed', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT ARRAY(1,2) AS a, (SELECT CAST(id AS INT) FROM range(2)) AS n FROM range(2)) t"},
    {'id':'projection_invalid_array_null_lookup', 'sql':"SELECT ARRAY_CONTAINS(a,n) AS r FROM (SELECT 1 AS a, CAST(NULL AS INT) AS n FROM range(2)) t"},
]
for null_input, array, lookup in [
    ('lookup', "ARRAY(CAST('bad' AS INT),2)", 'CAST(NULL AS INT)'),
    ('array', 'CAST(NULL AS ARRAY<INT>)', "CAST('bad' AS INT)"),
]:
    source = f'(SELECT id, {array} AS a, {lookup} AS n FROM range(2)) t'
    empty_source = source.replace('range(2)', 'range(0)')
    for site, sql in [
        ('filter', f'SELECT 1 AS r FROM {source} WHERE ARRAY_CONTAINS(a,n)'),
        ('aggregate', f'SELECT COUNT(ARRAY_CONTAINS(a,n)) AS r FROM {source}'),
        ('grouping', f'SELECT COUNT(*) AS r FROM {source} GROUP BY ARRAY_CONTAINS(a,n)'),
        ('sort', f'SELECT id AS r FROM {source} ORDER BY ARRAY_CONTAINS(a,n), id'),
        ('window_argument', f'SELECT COUNT(ARRAY_CONTAINS(a,n)) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM {source} ORDER BY id'),
        ('window_partition', f'SELECT COUNT(*) OVER (PARTITION BY ARRAY_CONTAINS(a,n)) AS r FROM {source}'),
        ('aggregate_empty', f'SELECT COUNT(ARRAY_CONTAINS(a,n)) AS r FROM {empty_source}'),
        ('grouping_empty', f'SELECT COUNT(*) AS r FROM {empty_source} GROUP BY ARRAY_CONTAINS(a,n)'),
        ('grouping_sets', f'SELECT COUNT(*) AS r, GROUPING(ARRAY_CONTAINS(a,n)) AS g FROM {source} GROUP BY GROUPING SETS ((ARRAY_CONTAINS(a,n)), ()) ORDER BY g'),
        ('inner_join', f'SELECT COUNT(*) AS r FROM {source} JOIN range(1) u ON ARRAY_CONTAINS(t.a,t.n)'),
        ('left_join', f'SELECT COUNT(*) AS r FROM {source} LEFT JOIN range(1) u ON ARRAY_CONTAINS(t.a,t.n)'),
    ]:
        queries.append({'id':f'projection_{site}_null_{null_input}', 'sql':sql})
queries += [
    {'id':'projection_filter_local_values', 'sql':"SELECT 1 AS r FROM (SELECT ARRAY(CAST(v AS INT)) AS a, CAST(NULL AS INT) AS n FROM VALUES ('bad') t(v)) s WHERE ARRAY_CONTAINS(a,n)"},
    {'id':'projection_filter_live_error', 'sql':"SELECT 1 AS r FROM (SELECT ARRAY(CAST('bad' AS INT)) AS a, 1 AS n FROM range(2)) t WHERE ARRAY_CONTAINS(a,n)"},
]
for null_input, array, lookup in [
    ('lookup', "ARRAY(CAST('bad' AS INT),2)", 'CAST(NULL AS INT)'),
    ('array', 'CAST(NULL AS ARRAY<INT>)', "CAST('bad' AS INT)"),
]:
    for join_name, join_sql in [
        ('inner', 'JOIN'), ('left', 'LEFT JOIN'), ('right', 'RIGHT JOIN'),
        ('full', 'FULL JOIN'), ('semi', 'LEFT SEMI JOIN'), ('anti', 'LEFT ANTI JOIN'),
    ]:
        for bad_side in ['left', 'right']:
            left = f'(SELECT {array} AS a, {lookup} AS n FROM range(2)) l' if bad_side == 'left' else 'range(2) l'
            right = f'(SELECT {array} AS a, {lookup} AS n FROM range(3)) u' if bad_side == 'right' else 'range(3) u'
            side = 'l' if bad_side == 'left' else 'u'
            queries.append({
                'id':f'join_{join_name}_{bad_side}_null_{null_input}',
                'sql':f'SELECT COUNT(*) AS r FROM {left} {join_sql} {right} ON ARRAY_CONTAINS({side}.a,{side}.n)',
            })
        queries.append({
            'id':f'join_{join_name}_cross_inputs_null_{null_input}',
            'sql':f'SELECT COUNT(*) AS r FROM (SELECT {array} AS a FROM range(2)) l {join_sql} (SELECT {lookup} AS n FROM range(3)) u ON ARRAY_CONTAINS(l.a,u.n)',
        })
queries += [
    {'id':'join_inner_local_values', 'sql':"SELECT COUNT(*) AS r FROM range(2) l JOIN (SELECT ARRAY(CAST(v AS INT)) AS a, CAST(NULL AS INT) AS n FROM VALUES ('bad') t(v)) u ON ARRAY_CONTAINS(u.a,u.n)"},
    {'id':'join_left_local_values', 'sql':"SELECT COUNT(*) AS r FROM range(2) l LEFT JOIN (SELECT ARRAY(CAST(v AS INT)) AS a, CAST(NULL AS INT) AS n FROM VALUES ('bad') t(v)) u ON ARRAY_CONTAINS(u.a,u.n)"},
    {'id':'join_inner_subquery_cast', 'sql':"SELECT COUNT(*) AS r FROM range(2) l JOIN (SELECT ARRAY((SELECT CAST('bad' AS INT))) AS a, CAST(NULL AS INT) AS n FROM range(3)) u ON ARRAY_CONTAINS(u.a,u.n)"},
    {'id':'join_left_subquery_cast', 'sql':"SELECT COUNT(*) AS r FROM (SELECT ARRAY((SELECT CAST('bad' AS INT))) AS a, CAST(NULL AS INT) AS n FROM range(2)) l LEFT JOIN range(3) u ON ARRAY_CONTAINS(l.a,l.n)"},
]
join_left_input = "(SELECT id, CAST(NULL AS ARRAY<INT>) AS a, CAST('bad' AS INT) AS n FROM range(2)) l"
for name, predicate in [
    ('coalesce', 'COALESCE(ARRAY_CONTAINS(l.a,l.n),true)'),
    ('cross_or', 'ARRAY_CONTAINS(l.a,l.n) OR l.id=u.id'),
    ('cross_and', 'ARRAY_CONTAINS(l.a,l.n) AND l.id=u.id'),
    ('local_or', 'ARRAY_CONTAINS(l.a,l.n) OR l.id>=0'),
    ('is_null', 'ARRAY_CONTAINS(l.a,l.n) IS NULL'),
]:
    queries.append({'id':f'join_compound_{name}', 'sql':f'SELECT COUNT(*) AS r FROM {join_left_input} JOIN range(3) u ON {predicate}'})
queries += [
    {'id':'join_cte_aliases', 'sql':"WITH source AS (SELECT ARRAY(CAST('bad' AS INT),2) AS a, CAST(NULL AS INT) AS n FROM range(2)) SELECT COUNT(*) AS r FROM (SELECT * FROM source) l JOIN range(3) u ON ARRAY_CONTAINS(l.a,l.n)"},
    {'id':'join_left_required_output_error', 'sql':"SELECT l.id AS r, u.n AS absent FROM range(2) l LEFT JOIN (SELECT CAST(NULL AS ARRAY<INT>) AS a, CAST('bad' AS INT) AS n FROM range(3)) u ON ARRAY_CONTAINS(u.a,u.n) ORDER BY l.id"},
    {'id':'join_right_required_output_error', 'sql':f'SELECT u.id AS r, l.n AS absent FROM {join_left_input} RIGHT JOIN range(3) u ON ARRAY_CONTAINS(l.a,l.n) ORDER BY u.id'},
    {'id':'join_left_nullable_output', 'sql':"SELECT l.id AS r, u.id AS absent FROM range(2) l LEFT JOIN (SELECT id, CAST(NULL AS ARRAY<INT>) AS a, CAST('bad' AS INT) AS n FROM range(3)) u ON ARRAY_CONTAINS(u.a,u.n) ORDER BY l.id"},
    {'id':'join_right_nullable_output', 'sql':f'SELECT u.id AS r, l.id AS absent FROM {join_left_input} RIGHT JOIN range(3) u ON ARRAY_CONTAINS(l.a,l.n) ORDER BY u.id'},
    {'id':'join_empty_inner_input', 'sql':f"SELECT COUNT(*) AS r FROM {join_left_input.replace('range(2)', 'range(0)')} JOIN range(3) u ON ARRAY_CONTAINS(l.a,l.n)"},
    {'id':'join_empty_preserved_input', 'sql':f"SELECT COUNT(*) AS r FROM {join_left_input.replace('range(2)', 'range(0)')} LEFT JOIN range(3) u ON ARRAY_CONTAINS(l.a,l.n)"},
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
                structured_error = error if hasattr(error, 'getCondition') else error.java_exception
                condition = structured_error.getCondition()
                if condition not in {'CAST_INVALID_INPUT', 'DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE', 'DATATYPE_MISMATCH.NULL_TYPE', 'DIVIDE_BY_ZERO', 'SCALAR_SUBQUERY_TOO_MANY_ROWS', 'INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION'}:
                    raise
                result={'status':'error','stage':stage,'condition':condition}
            observations.append({**case,'ansi':ansi,'expected':result})
    expected_errors = {
        (case['id'], ansi) for case in queries for ansi in [True, False]
        if case['id'].startswith(('invalid_', 'registered_null_type_'))
        or (ansi and case['id'] in {'live_bad_lookup', 'live_bad_array', 'live_bad_lookup_column', 'live_bad_array_column'})
    }
    assert {(r['id'], r['ansi']) for r in observations if r['expected']['status'] == 'error' and not r['id'].startswith(('projection_', 'join_'))} == expected_errors
    inputs = {
        "spark_version": spark.version, "timezone": spark.conf.get('spark.sql.session.timeZone'),
        "legacy_size_of_null": spark.conf.get('spark.sql.legacy.sizeOfNull'),
        "java_version": spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
        "null_row_count": 3,
        "integer_rows": integer_rows, "string_rows": string_rows,
    }
    for observation in observations:
        name, ansi = observation['id'], observation['ansi']
        if name in {'registered_null_type_array', 'registered_null_type_empty_batch'}:
            observation['retained_difference'] = 'null_type_admission'
    target = Path(__file__).with_name('array-contains-spark.jsonl')
    target.write_text('\n'.join(json.dumps(record) for record in [inputs, *observations])+'\n')
    print(json.dumps({"observations": len(observations), "known_differences": sum('retained_difference' in r for r in observations)}))

finally:
    spark.stop()
