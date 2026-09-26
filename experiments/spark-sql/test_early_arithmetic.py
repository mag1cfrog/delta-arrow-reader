"""Keep numeric narrowing distinct from string conversion and remainder errors."""
import unittest

from early_arithmetic import cause


class ErrorCauseTest(unittest.TestCase):
    def test_numeric_overflow_and_unrelated_errors(self):
        for value in ['NaN', '-inf', 'Infinity', '32768', '9.223372036854776e18']:
            for typ in ['Int8', 'Int16', 'Int32', 'Int64']:
                self.assertEqual(cause({'error': f"Arrow error: Cast error: Can't cast value {value} to type {typ}"}),
                                 'CAST_OVERFLOW')
        for text in ["Cast error: Can't cast value bad to type Int64",
                     "Cast error: Can't cast value NaN to type Float64", 'unknown CAST error']:
            self.assertIsNone(cause({'error': text}))
        self.assertEqual(cause({'error': "Cannot cast string 'NaN' to value of Int64 type"}), 'CAST_INVALID_INPUT')
        self.assertEqual(cause({'error': 'Arrow error: Divide by zero error'}), 'DIVIDE_BY_ZERO')
        self.assertEqual(cause({'error': '[REMAINDER_BY_ZERO] Remainder by zero'}), 'REMAINDER_BY_ZERO')


if __name__ == '__main__':
    unittest.main()
