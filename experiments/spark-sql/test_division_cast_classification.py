import unittest

from division_cast_classification import dimensions


class ClassificationComparison(unittest.TestCase):
    def test_boolean_spelling_does_not_hide_values_types_or_errors(self):
        spark = dict(status='ok', types=['boolean'], rows=[['true']],
                     schema={'fields': [{'nullable': True}]})
        native = dict(status='ok', types=['Boolean'], rows=[['true']],
                      logical_nullable=[True], physical_nullable=[True])
        self.assertEqual(dimensions(spark, native)['differences'], [])
        for change, expected in [
            ({'rows': [['false']]}, ['rows']),
            ({'rows': [[None]]}, ['rows']),
            ({'types': ['boolean-ish']}, ['types']),
        ]:
            self.assertEqual(dimensions(spark, native | change)['differences'], expected)
        self.assertTrue(dimensions(spark, native | {'physical_nullable': [False]})
                        ['physical_nullable_difference'])
        error = dict(status='execution_error', condition='CAST_INVALID_INPUT')
        self.assertEqual(dimensions(error, native)['differences'], ['status'])
        self.assertEqual(dimensions(error, dict(status='planning_error', error='unknown'))
                         ['differences'], ['error_cause'])
        self.assertEqual(dimensions(error, dict(status='execution_error',
                         error='Arrow error: Divide by zero error'))['differences'], ['error_cause'])
        columns = dict(status='planning_error',
                       condition='INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN')
        self.assertEqual(dimensions(columns, dict(status='planning_error',
                         error='invalid argument: Scalar subquery must return exactly one column, found 2'))
                         ['differences'], [])
        self.assertEqual(dimensions(columns, dict(status='execution_error',
                         error='Execution error: Scalar subquery returned more than one row'))
                         ['differences'], ['error_cause'])
        ordered = spark | {'types': ['integer'], 'rows': [['1'], ['2']]}
        swapped = native | {'types': ['Int32'], 'rows': [['2'], ['1']]}
        self.assertEqual(dimensions(ordered, swapped)['differences'], [])
        self.assertEqual(dimensions(ordered, swapped, ordered=True)['differences'], ['rows'])


if __name__ == '__main__':
    unittest.main()
