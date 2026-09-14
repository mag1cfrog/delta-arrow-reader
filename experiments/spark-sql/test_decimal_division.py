"""Protect the focused probe from rounding away small Decimal differences."""

import json
import tempfile
import unittest
from pathlib import Path

from decimal_division import compare, load_cases


class DecimalDivisionComparisonTest(unittest.TestCase):
    def test_exact_values_and_capture_identity(self):
        cases = load_cases()
        capture = {"cases": cases, "results": [
            {"id": f"{case['id']}_{ansi}", "actual": {
                "status": "ok", "types": ["decimal(38,37)"],
                "rows": [["0.1234567890123456789012345678901234567"], [None]]}}
            for case in cases for ansi in ("true", "false")]}
        with tempfile.TemporaryDirectory() as directory:
            reference, candidate = [Path(directory) / name for name in ("reference", "candidate")]
            reference.write_text(json.dumps(capture))
            candidate.write_text(json.dumps(capture))
            total = len(capture["results"])
            self.assertEqual(compare(reference, candidate)["agreement"], total)
            capture["results"][0]["actual"]["rows"][0][0] = "0.1234567890123456789012345678901234568"
            candidate.write_text(json.dumps(capture))
            result = compare(reference, candidate)
            self.assertEqual(result["agreement"], total - 1)
            self.assertEqual(result["cases"][0]["differences"], ["rows"])
            capture["cases"][0]["sql"] = "SELECT 1"
            candidate.write_text(json.dumps(capture))
            with self.assertRaisesRegex(ValueError, "capture SQL"):
                compare(reference, candidate)
