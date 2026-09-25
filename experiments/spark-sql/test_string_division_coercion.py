"""Keep type rejection, invalid input and successful NULL distinct."""
import json
from pathlib import Path
import tempfile
import unittest

from string_division_coercion import compare


class StringDivisionCauseTest(unittest.TestCase):
    def test_specific_error_causes(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory)
            case = {'id': 'q', 'sql': "SELECT '7' DIV 2"}
            (p / 'cases.jsonl').write_text(json.dumps(case) + '\n')
            def run(condition, actual):
                for name, value in [('spark', {'status': 'planning_error', 'condition': condition}),
                                    ('native', actual)]:
                    (p / f'{name}.json').write_text(json.dumps({'cases': [case], 'results': [
                        {'id': 'q_' + mode, 'actual': value} for mode in ['true', 'false']]}))
                return compare(p / 'spark.json', p / 'native.json', p / 'cases.jsonl')['agreement']
            wrong = 'DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE'
            different = 'DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES'
            rejection = {'status': 'planning_error', 'error': f'[{wrong}] unsupported string division operands'}
            self.assertEqual(run(wrong, rejection), 2)
            self.assertEqual(run(different, rejection), 0)
            for value in [{'status': 'execution_error', 'error': "Cannot cast string 'bad' to value of Int64 type"},
                          {'status': 'planning_error', 'error': 'unknown'},
                          dict(rejection, condition='CAST_INVALID_INPUT'),
                          {'status': 'ok', 'types': ['Int64'], 'rows': [[None]]}]:
                self.assertEqual(run(wrong, value), 0)


if __name__ == '__main__':
    unittest.main()
