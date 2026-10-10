"""The typed oracle must distinguish floating-point bit patterns."""
from array import array
import unittest
import pyarrow as pa
from numeric_pages import check


class NumericOracleTest(unittest.TestCase):
    def test_nan_payload_signed_zero_null_and_row_order(self):
        for dtype, bits_type, codes in [(pa.float32(), "I", [0x7fc00001, 0x80000000, 0]),
                                        (pa.float64(), "Q", [0x7ff8000000000001, 0x8000000000000000, 0])]:
            values = pa.Array.from_buffers(dtype, 3, [None, pa.py_buffer(array(bits_type, codes))])
            expected = pa.table({"row_id": [0, 1, 2], "value": values})
            check(expected.take([2, 0, 1]), expected)
            for index in (0, 1):
                changed = codes.copy()
                changed[index] = 0 if index == 1 else changed[index] + 1
                value = pa.Array.from_buffers(dtype, 3, [None, pa.py_buffer(array(bits_type, changed))])
                with self.assertRaises(AssertionError):
                    check(pa.table({"row_id": [0, 1, 2], "value": value}), expected)
            with self.assertRaises(AssertionError):
                check(pa.table({"row_id": [0, 1, 2], "value": pa.array([None, -0.0, 0.0], type=dtype)}), expected)


if __name__ == "__main__":
    unittest.main()
