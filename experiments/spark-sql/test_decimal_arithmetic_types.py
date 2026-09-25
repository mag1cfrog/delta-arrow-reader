import json
from pathlib import Path
import tempfile
import unittest

from decimal_arithmetic_types import compare


class DecimalArithmeticComparison(unittest.TestCase):
    def test_overflow_does_not_match_invalid_cast_or_zero_divisor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cases = [{'id': 'overflow', 'sql': 'SELECT a * b FROM t'}]
            (root / 'cases.jsonl').write_text(json.dumps(cases[0]) + '\n')
            reference = {'cases': cases, 'results': [
                {'id': 'overflow_' + ansi, 'actual': {
                    'status': 'execution_error', 'condition': 'NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION'}}
                for ansi in ['true', 'false']]}
            (root / 'reference.json').write_text(json.dumps(reference))
            for message, agreement in [
                ('Arrow error: Cast error: Cannot cast to Decimal128(38, 0). Overflowing on 100000000000000000000000000000000000000', 2),
                ('Arrow error: Invalid argument error: 100 is too large to store in a Decimal128 of precision 2. Max is 99', 2),
                ('Arrow error: Cast error: Cannot cast string bad to value of Decimal128(38, 0)', 0),
                ('Arrow error: Divide by zero error', 0),
                ('Arrow error: Arithmetic overflow: Overflow happened on intermediate multiplication', 0),
            ]:
                candidate = {'cases': cases, 'results': [
                    {'id': row['id'], 'actual': {'status': 'planning_error', 'error': message}}
                    for row in reference['results']]}
                (root / 'candidate.json').write_text(json.dumps(candidate))
                result = compare(root / 'reference.json', root / 'candidate.json', root / 'cases.jsonl')
                self.assertEqual(result['agreement'], agreement, message)
                self.assertTrue(all(row['phase_difference'] for row in result['cases']))


if __name__ == '__main__':
    unittest.main()
