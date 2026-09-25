"""Recognize only the owned integral-division overflow cause."""
import json
from pathlib import Path
import tempfile
import unittest

from bigint_div_overflow import compare


class BigintDivCauseTest(unittest.TestCase):
    def test_division_context_and_error_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory)
            case = {'id': 'q', 'sql': 'SELECT a DIV b', 'integer_divide': True}
            def run(actual):
                (p / 'cases.jsonl').write_text(json.dumps(case) + '\n')
                for name, value in [('spark', {'status': 'execution_error', 'condition': 'ARITHMETIC_OVERFLOW'}),
                                    ('native', actual)]:
                    (p / f'{name}.json').write_text(json.dumps({'cases': [case], 'results': [
                        {'id': 'q_' + mode, 'actual': value} for mode in ['true', 'false']]}))
                return compare(p / 'spark.json', p / 'native.json', p / 'cases.jsonl')['agreement']
            overflow = {'status': 'execution_error', 'error':
                        'Arrow error: Arithmetic overflow: Overflow happened on: -9223372036854775808 / -1'}
            self.assertEqual(run(overflow), 2)
            case['integer_divide'] = False
            self.assertEqual(run(overflow), 0)
            case['integer_divide'] = True
            for value in [{'status': 'execution_error', 'error': 'Arrow error: Divide by zero error'},
                          {'status': 'execution_error', 'error': 'unknown'},
                          dict(overflow, condition='CAST_OVERFLOW'),
                          {'status': 'ok', 'types': ['Int64'], 'rows': [[None]]}]:
                self.assertEqual(run(value), 0)


if __name__ == '__main__':
    unittest.main()
