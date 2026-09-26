import json
from pathlib import Path
import tempfile
import unittest

from string_modulo_coercion import compare


class ModuloComparison(unittest.TestCase):
    def test_zero_cast_and_unknown_errors_remain_distinct(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            cases = [{'id': 'mod', 'sql': "SELECT '7' % 0L", 'batch_size': 2}]
            (root / 'cases.jsonl').write_text(json.dumps(cases[0]) + '\n')
            for error, expected in [
                ('Arrow error: [REMAINDER_BY_ZERO] Remainder by zero', 2),
                ('Arrow error: Divide by zero error', 0),
                ("Arrow error: Cannot cast string 'bad' to Int64", 0),
                ('unknown error', 0),
            ]:
                for name, actual in [
                    ('spark', {'status': 'execution_error', 'condition': 'REMAINDER_BY_ZERO'}),
                    ('native', {'status': 'execution_error', 'error': error}),
                ]:
                    capture = {'cases': cases, 'results': [
                        {'id': 'mod_' + mode, 'actual': actual} for mode in ['true', 'false']
                    ]}
                    (root / (name + '.json')).write_text(json.dumps(capture))
                result = compare(root / 'spark.json', root / 'native.json', root / 'cases.jsonl')
                self.assertEqual(result['agreement'], expected)


if __name__ == '__main__':
    unittest.main()
