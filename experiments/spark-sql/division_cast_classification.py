"""Reduce and classify retained division/CAST observations for issue 148."""
import argparse
import json
from pathlib import Path

from decimal_division import capture, load_cases, types
from float_bround import values
from string_division_coercion import cause as arithmetic_cause

CASES = Path(__file__).with_name('division-cast-classification.jsonl')


def cases():
    result = []

    def add(name, family, sql):
        result.append(dict(id=name, family=family, sql=sql, batch_size=2))

    for typ in ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE']:
        for form, sql in [
            ('literal', f"SELECT CAST('bad' AS {typ}) AS r"),
            ('column', f"SELECT CAST(v AS {typ}) AS r FROM VALUES ('bad') t(v)"),
            ('try', f"SELECT TRY_CAST('bad' AS {typ}) AS r"),
        ]:
            add(f'cast_{typ.lower()}_{form}', 'cast_mode', sql)
    for name, sql in [
        ('null_left', "SELECT CAST(NULL AS DOUBLE) / CAST('bad' AS DOUBLE) AS r"),
        ('null_right', "SELECT CAST('bad' AS DOUBLE) / CAST(NULL AS DOUBLE) AS r"),
        ('div_null', "SELECT CAST('bad' AS BIGINT) DIV CAST(NULL AS BIGINT) AS r"),
        ('mod_null', "SELECT CAST('bad' AS DOUBLE) % CAST(NULL AS DOUBLE) AS r"),
        ('dead_case', "SELECT CASE WHEN false THEN CAST('bad' AS DOUBLE) ELSE 7D END AS r"),
        ('column_case', "SELECT CASE WHEN k=0 THEN 7D ELSE CAST('bad' AS DOUBLE) END AS r FROM VALUES (0) t(k)"),
        ('live_cast', "SELECT CAST('bad' AS DOUBLE) / 2D AS r"),
        ('column_null_literal', "SELECT CAST(v AS BIGINT) DIV CAST(NULL AS BIGINT) AS r FROM VALUES ('bad') t(v)"),
        ('column_null_column', "SELECT CAST(v AS BIGINT) DIV n AS r FROM VALUES ('bad',CAST(NULL AS BIGINT)) t(v,n)"),
        ('unused_cast', "SELECT id AS r FROM (SELECT id,CAST('bad' AS INT) AS unused FROM range(3)) t ORDER BY id"),
        ('cast_limit0', "SELECT CAST('bad' AS INT) AS r FROM range(3) LIMIT 0"),
    ]:
        add(name, 'dead_expression', sql)
    for name, subquery in [
        ('literal', "SELECT CAST('bad' AS DOUBLE)"),
        ('local', "SELECT CAST(v AS DOUBLE) FROM VALUES ('bad') t(v)"),
        ('local_limit0', "SELECT CAST(v AS DOUBLE) FROM VALUES ('bad') t(v) LIMIT 0"),
        ('local_empty', "SELECT CAST('bad' AS DOUBLE) FROM VALUES (1) t(k) WHERE false"),
        ('local_dead_case', "SELECT CASE WHEN k=1 THEN CAST('bad' AS DOUBLE) ELSE 4D END FROM VALUES (0),(0) t(k)"),
        ('local_live_case', "SELECT CASE WHEN k=1 THEN CAST('bad' AS DOUBLE) ELSE 4D END FROM VALUES (0),(1) t(k)"),
        ('range_cast', "SELECT CAST('bad' AS DOUBLE) FROM range(2)"),
        ('range_case', "SELECT CASE WHEN id=1 THEN CAST('bad' AS DOUBLE) ELSE 4D END FROM range(2)"),
        ('many_valid', 'SELECT CAST(id AS DOUBLE) FROM range(2)'),
        ('two_columns', 'SELECT 1D,2D'),
    ]:
        add('subquery_' + name, 'null_subquery', f'SELECT CAST(NULL AS DOUBLE) / ({subquery}) AS r')
    add('dead_correlated', 'null_subquery',
        'SELECT CAST(NULL AS DOUBLE) / (SELECT CAST(id AS DOUBLE) FROM range(4) WHERE id >= o.k) AS r FROM VALUES (1),(2) o(k) ORDER BY k')
    add('round_outer_cast', 'null_subquery',
        "SELECT ROUND(CAST((SELECT v FROM VALUES ('bad') t(v)) AS DECIMAL(10,3)),NULL) AS r")
    for name, sql in [
        ('local_limit0', 'SELECT a DIV b AS r FROM VALUES (7,0) t(a,b) LIMIT 0'),
        ('local_live_case', 'SELECT CASE WHEN k=0 THEN 7 DIV 0 ELSE 5 END AS r FROM VALUES (0) t(k) LIMIT 0'),
        ('range_limit0', 'SELECT 7 DIV 0 AS r FROM range(3) WHERE false LIMIT 0'),
        ('range_false', 'SELECT 7 DIV 0 AS r FROM range(3) WHERE false'),
        ('range_case', 'SELECT CASE WHEN true THEN 7 DIV 0 ELSE 5 END AS r FROM range(3) WHERE false'),
        ('filter_false', 'SELECT id AS r FROM range(3) WHERE false AND id > (7 DIV 0)'),
        ('local_null_cast', "SELECT CAST(NULL AS INT) DIV CAST(v AS INT) AS r FROM VALUES ('bad') t(v)"),
        ('local_null_case', "SELECT CASE WHEN true THEN CAST(NULL AS INT) DIV CAST(v AS INT) ELSE 5 END AS r FROM VALUES ('bad') t(v)"),
        ('local_null_nullif', "SELECT CAST(NULL AS INT) DIV NULLIF(CAST(v AS INT),0) AS r FROM VALUES ('bad') t(v)"),
        ('local_null_numeric', "SELECT CASE WHEN true THEN CAST(NULL AS INT) DIV CAST(v AS INT) ELSE 5 END AS r FROM VALUES (CAST('NaN' AS DOUBLE)) t(v)"),
        ('unrelated_null_cast', "SELECT CAST(NULL AS BIGINT) DIV CAST(v AS BIGINT) AS r,CAST('bad' AS BIGINT) DIV CAST(NULL AS BIGINT) AS other FROM VALUES ('2') t(v)"),
    ]:
        add('early_' + name, 'early_evaluation', sql)
    for name, subquery in [
        ('bare_limit0', 'SELECT 7 FROM range(3) LIMIT 0'),
        ('divide_limit0', 'SELECT 7 DIV 0 FROM range(3) LIMIT 0'),
        ('range_empty', 'SELECT 7 FROM range(3) WHERE false'),
        ('values_empty', 'SELECT 7 FROM VALUES (1) t(k) WHERE false'),
        ('one_row', 'SELECT 7 FROM range(1)'),
        ('null_row', 'SELECT CAST(NULL AS INT) FROM range(1)'),
        ('many_rows', 'SELECT id FROM range(2)'),
        ('count_empty', 'SELECT COUNT(*) FROM range(0)'),
    ]:
        add('scalar_' + name, 'scalar_nullability', f'SELECT ({subquery}) AS r')
    add('boolean_empty', 'conditional_control', 'SELECT (7 DIV 0 > 0) AND false AS r FROM range(3) WHERE false')
    add('boolean_live', 'conditional_control', 'SELECT true AS r')
    add('nullif_zero', 'conditional_control', "SELECT NULLIF(CAST(v AS DOUBLE),0D) AS r FROM VALUES (0,'-0.0'),(1,NULL),(2,'2') t(k,v) ORDER BY k")
    add('nullif_positive', 'conditional_control', "SELECT NULLIF(CAST(v AS DOUBLE),0D) AS r FROM VALUES (0,'0.0'),(1,NULL),(2,'2') t(k,v) ORDER BY k")
    assert len({c['id'] for c in result}) == len(result)
    return result


def canonical_types(actual):
    return ['boolean' if typ == 'Boolean' else typ for typ in types(actual)]


def cause(actual):
    if 'Scalar subquery must return exactly one column, found ' in actual.get('error', ''):
        return 'INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN'
    return arithmetic_cause(actual)


def dimensions(expected, actual, ordered=False):
    """Keep success, value/type, cause, phase and nullability distinct."""
    successful = expected['status'] == actual['status'] == 'ok'
    differences = []
    if successful:
        a, b = canonical_types(expected), canonical_types(actual)
        if a != b:
            differences.append('types')
        # The Boolean reductions have a single field; numeric helpers retain
        # exact Decimal and IEEE comparison for every other observation.
        if a != b or a == ['boolean']:
            equal = expected['rows'] == actual['rows']
        elif ordered:
            equal = ([values(expected | {'rows': [row]}) for row in expected['rows']]
                     == [values(actual | {'rows': [row]}) for row in actual['rows']])
        else:
            equal = values(expected) == values(actual)
        if not equal:
            differences.append('rows')
    elif (expected['status'] == 'ok') != (actual['status'] == 'ok'):
        differences.append('status')
    else:
        a, b = cause(expected), cause(actual)
        if a is None or a != b:
            differences.append('error_cause')
    nullable = [f['nullable'] for f in expected.get('schema', {}).get('fields', [])]
    return dict(differences=differences,
                error_causes=[cause(expected), cause(actual)] if not successful else None,
                phase=[expected['status'], actual['status']],
                logical_nullable_difference=nullable != actual.get('logical_nullable') if successful else None,
                physical_nullable_difference=nullable != actual.get('physical_nullable') if successful else None)


def compare(reference, candidate, corpus=CASES):
    a, b = (json.loads(path.read_text()) for path in [reference, candidate])
    cases = load_cases(corpus)
    assert a['cases'] == b['cases'] == cases
    ids = [f"{c['id']}_{ansi}" for c in cases for ansi in ['true', 'false']]
    assert [r['id'] for r in a['results']] == [r['id'] for r in b['results']] == ids
    rows = [dict(id=id, **dimensions(x['actual'], y['actual'], 'ORDER BY' in case['sql'].upper()))
            for id, case, x, y in zip(ids, [c for c in cases for _ in range(2)],
                                     a['results'], b['results'], strict=True)]
    return dict(total=len(rows), agreement=sum(not r['differences'] for r in rows), cases=rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    sub.add_parser('generate')
    sub.add_parser('spark').add_argument('out', type=Path)
    check = sub.add_parser('compare')
    for name in ['reference', 'candidate', 'out']:
        check.add_argument(name, type=Path)
    args = parser.parse_args()
    if args.mode == 'generate':
        CASES.write_text(''.join(json.dumps(c) + '\n' for c in cases()))
    elif args.mode == 'spark':
        capture(args.out, CASES)
    else:
        result = compare(args.reference, args.candidate)
        args.out.write_text(json.dumps(result, indent=2) + '\n')
        print(result['agreement'], '/', result['total'])
