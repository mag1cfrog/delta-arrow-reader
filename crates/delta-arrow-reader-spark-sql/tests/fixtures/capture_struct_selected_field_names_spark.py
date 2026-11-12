"""Capture STRUCT names from selected fields, map keys and array elements."""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession, types as T

from capture_floating_group_keys_spark import encode
from capture_struct_expression_names_spark import capture_struct_query


def encode_input(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, T.StructType):
        return [encode_input(item, field.dataType) for item, field in zip(value, data_type.fields)]
    if isinstance(data_type, T.ArrayType):
        return [encode_input(item, data_type.elementType) for item in value]
    if isinstance(data_type, T.MapType):
        return [[encode_input(key, data_type.keyType), encode_input(item, data_type.valueType)]
                for key, item in value.items()]
    return encode(value, data_type, input_value=True)


def main():
    output = Path(sys.argv[1])
    assert not output.exists()
    schema = T.StructType([
        T.StructField('id', T.IntegerType(), False),
        T.StructField('v', T.IntegerType(), True),
        T.StructField('k', T.StringType(), True),
        T.StructField('idx', T.IntegerType(), True),
        T.StructField('m', T.MapType(T.StringType(), T.IntegerType(), True), True),
        T.StructField('mi', T.MapType(T.IntegerType(), T.IntegerType(), True), True),
        T.StructField('mn', T.MapType(T.StringType(), T.StructType([
            T.StructField('leaf', T.IntegerType(), True),
            T.StructField('deep', T.StructType([T.StructField('name', T.StringType(), True)]), True),
        ]), True), True),
        T.StructField('mm', T.MapType(T.StringType(), T.MapType(T.StringType(), T.IntegerType(), True), True), True),
        T.StructField('n', T.NullType(), True),
        T.StructField('items', T.ArrayType(T.StructType([T.StructField('item', T.IntegerType(), True)]), True), True),
        T.StructField('arr', T.ArrayType(T.IntegerType(), True), True),
        T.StructField('nested', T.StructType([T.StructField('item', T.IntegerType(), True)]), True),
    ])
    rows = [
        (0, -2, 'item', 0, {'item': -3, 'a.b': -4, '': -5}, {3: -6}, {'outer': (-7, ('yes',))}, {'outer': {'inner': -8}}, None, [(-9,), None], [-10], (-11,)),
        (1, 2, 'a.b', 0, {'item': 3, 'a.b': 4, '': 5}, {3: 6}, {'outer': (7, ('no',))}, {'outer': {'inner': 8}}, None, [(9,)], [10], (11,)),
        (2, None, 'item', None, {'item': None}, {3: None}, {'outer': None}, {'outer': None}, None, [None], [None], (None,)),
        (3, None, None, None, None, None, None, None, None, None, None, None),
    ]
    nulls = [(0,) + (None,) * 11]
    expressions = {
        'map_dot': 'STRUCT(m.item)',
        'map_bracket': "STRUCT(m['item'])",
        'map_cast_key': "STRUCT(m[CAST('item' AS STRING)])",
        'map_dynamic_key': 'STRUCT(m[k])',
        'map_quoted_key': "STRUCT(m['a.b'])",
        'map_empty_key': "STRUCT(m[''])",
        'map_numeric_key': 'STRUCT(mi[3])',
        'map_computed_numeric_key': 'STRUCT(mi[1+2])',
        'map_dynamic_numeric_key': 'STRUCT(mi[idx])',
        'map_struct_bracket_dot': "STRUCT(mn['outer'].leaf)",
        'map_struct_dot': 'STRUCT(mn.outer.leaf)',
        'map_struct_brackets': "STRUCT(mn['outer']['leaf'])",
        'map_struct_deep': "STRUCT(mn['outer'].deep.name)",
        'nested_map_brackets': "STRUCT(mm['outer']['inner'])",
        'null_dot': 'STRUCT(n.item)',
        'null_bracket': "STRUCT(n['item'])",
        'array_literal_index': 'STRUCT(arr[0])',
        'array_dynamic_index': 'STRUCT(arr[idx])',
        'array_struct_projection': 'STRUCT(items.item)',
        'array_struct_literal_index': 'STRUCT(items[0].item)',
        'array_struct_dynamic_index': 'STRUCT(items[idx].item)',
        'array_struct_bracket': "STRUCT(items['item'])",
        'qualified_column': 'STRUCT(q.v)',
        'identifier_map_extract': "STRUCT(IDENTIFIER('m')['item'])",
        'identifier_map_path': "STRUCT(IDENTIFIER('m.item'))",
        'identifier_dynamic_key': "STRUCT(m[IDENTIFIER('k')])",
        'identifier_null_extract': "STRUCT(IDENTIFIER('n')['item'])",
        'map_alias': "STRUCT(m['item'] AS renamed)",
        'null_alias': 'STRUCT(n.item AS renamed)',
        'array_struct_alias': 'STRUCT(items[idx].item AS renamed)',
        'correlated_map_dot': '(SELECT STRUCT(q.m.item))',
        'correlated_map_bracket': "(SELECT STRUCT(q.m['item']))",
        'correlated_null_dot': '(SELECT STRUCT(q.n.item))',
        'correlated_null_bracket': "(SELECT STRUCT(q.n['item']))",
        'correlated_array_struct_projection': '(SELECT STRUCT(q.items.item))',
        'correlated_array_struct_literal_index': '(SELECT STRUCT(q.items[0].item))',
        'case_struct_field': 'STRUCT((CASE WHEN v IS NULL THEN nested ELSE nested END).item)',
        'map_json': "TO_JSON(STRUCT(m['item']))",
        'null_json': 'TO_JSON(STRUCT(n.item))',
    }
    os.environ['PYSPARK_PYTHON'] = sys.executable
    spark = (SparkSession.builder.master('local[1]').appName('dar-struct-selected-field-names')
             .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '1')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    try:
        assert spark.version == '4.2.0'
        spark.sparkContext.setLogLevel('ERROR')
        records = [{'spark_version': spark.version, 'timezone': 'UTC', 'inputs': []}]
        for table, values in [('selected_mixed', rows), ('selected_nulls', nulls), ('selected_empty', [])]:
            spark.createDataFrame(values, schema).createOrReplaceTempView(table)
            records[0]['inputs'].append({'name': table, 'schema': schema.jsonValue(),
                                        'rows': [encode_input(row, schema) for row in values]})
        for ansi in [True, False]:
            spark.conf.set('spark.sql.ansi.enabled', str(ansi).lower())
            for table in ['selected_mixed', 'selected_nulls', 'selected_empty']:
                for name, expression in expressions.items():
                    sql = f'SELECT {expression} AS s FROM {table} q'
                    expected = capture_struct_query(spark, sql)
                    records.append({'id': name, 'input': table, 'ansi': ansi, 'sql': sql, 'expected': expected})
        assert len(records) == 1 + len(expressions) * 6
        assert all('schema' in record['expected'] for record in records[1:])
        output.write_text(''.join(json.dumps(record, separators=(',', ':')) + '\n' for record in records))
        print(json.dumps({'records': len(records)-1, 'valid': sum('schema' in r['expected'] for r in records[1:]), 'output': str(output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
