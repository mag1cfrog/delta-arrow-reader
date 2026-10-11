"""The typed oracle must distinguish floating-point bit patterns."""
from array import array
import os
from pathlib import Path
import random
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import pyarrow as pa
import pyarrow.parquet as pq
from numeric_pages import DECIMAL_TYPES, check, run_reader, value_bytes


class NumericOracleTest(unittest.TestCase):
    def test_decimal_values_fit_precision_and_roundtrip_fixed_width(self):
        for dtype, width in zip(DECIMAL_TYPES.values(), (9, 16), strict=True):
            raw = value_bytes(random.Random(451), dtype, 100)
            integers = [int.from_bytes(raw[i:i + 16], "little", signed=True) for i in range(0, len(raw), 16)]
            self.assertTrue(all(abs(value) < 10**dtype.precision for value in integers))
            self.assertLess(min(integers), 0)
            self.assertGreater(max(integers), 0)
            values = pa.Array.from_buffers(dtype, 100, [pa.py_buffer(b"\x55" * 13), pa.py_buffer(raw)])
            expected = pa.table({"row_id": range(100), "value": values})
            with pa.BufferOutputStream() as sink:
                pq.write_table(expected, sink, use_dictionary=False, compression="NONE")
                data = sink.getvalue()
            column = pq.read_schema(pa.BufferReader(data)).field("value")
            self.assertEqual(column.type, dtype)
            physical = pq.ParquetFile(pa.BufferReader(data)).schema.column(1)
            self.assertEqual(physical.physical_type, "FIXED_LEN_BYTE_ARRAY")
            self.assertEqual(physical.length, width)
            check(pq.read_table(pa.BufferReader(data)), expected)
            changed = expected.set_column(1, "value", pa.nulls(100, type=dtype))
            with self.assertRaises(AssertionError):
                check(changed, expected)

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


@unittest.skipUnless(sys.platform == "linux", "benchmark process groups require Linux")
class ReaderProcessTest(unittest.TestCase):
    def test_reader_descendants_stop_on_timeout_failure_and_success(self):
        child = "import time; time.sleep(60)"
        parent = (
            "import subprocess, sys, time; from pathlib import Path; "
            "child = subprocess.Popen([sys.executable, '-c', sys.argv[2]]); "
            "Path(sys.argv[1]).write_text(str(child.pid)); "
            "time.sleep(60) if sys.argv[3] == 'timeout' else sys.exit(int(sys.argv[3]))"
        )
        for result in ("timeout", "7", "0"):
            with self.subTest(result=result), tempfile.TemporaryDirectory() as directory:
                pid_file = Path(directory) / "reader.pid"
                command = [sys.executable, "-c", parent, str(pid_file), child, result]
                try:
                    if result == "timeout":
                        with self.assertRaises(subprocess.TimeoutExpired):
                            run_reader(command, timeout=1)
                    elif result == "7":
                        with self.assertRaises(subprocess.CalledProcessError) as error:
                            run_reader(command, timeout=5)
                        self.assertEqual(error.exception.returncode, 7)
                    else:
                        run_reader(command, timeout=5)
                    self.assertTrue(pid_file.exists(), "reader must start before the deadline")
                    pid = int(pid_file.read_text())
                    deadline = time.monotonic() + 2
                    while time.monotonic() < deadline and self.running(pid):
                        time.sleep(.01)
                    self.assertFalse(self.running(pid), "reader survived the launcher")
                finally:
                    if pid_file.exists():
                        try:
                            os.kill(int(pid_file.read_text()), signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    @staticmethod
    def running(pid):
        try:
            return Path(f"/proc/{pid}/stat").read_text().split()[2] != "Z"
        except FileNotFoundError:
            return False


if __name__ == "__main__":
    unittest.main()
