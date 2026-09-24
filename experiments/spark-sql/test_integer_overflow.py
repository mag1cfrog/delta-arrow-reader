import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import integer_overflow


class OverflowComparisonTest(unittest.TestCase):
    def test_error_identity_phase_and_values(self):
        case = {"id": "overflow", "sql": "SELECT a + 1 FROM t"}
        reference = {"cases": [case], "results": [
            {"id": "overflow_true", "actual": {"status": "execution_error", "condition": "ARITHMETIC_OVERFLOW"}},
            {"id": "overflow_false", "actual": {"status": "ok", "types": ["integer"], "rows": [[-2147483648]]}},
        ]}
        candidate = {"cases": [case], "results": [
            {"id": "overflow_true", "actual": {"status": "planning_error", "error": "[ARITHMETIC_OVERFLOW] add"}},
            {"id": "overflow_false", "actual": {"status": "ok", "types": ["Int32"], "rows": [["-2147483648"]]}},
        ]}
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            corpus, oracle, actual = (root / name for name in ["cases.jsonl", "spark.json", "candidate.json"])
            corpus.write_text(json.dumps(case) + "\n")
            oracle.write_text(json.dumps(reference))
            with patch.object(integer_overflow, "CASES", corpus):
                actual.write_text(json.dumps(candidate))
                check = integer_overflow.compare(oracle, actual)
                self.assertEqual(check["agreement"], 2)
                self.assertEqual(check["cases"][0]["phase_difference"], ["execution_error", "planning_error"])
                for error in ["[DIVIDE_BY_ZERO]", "[BINARY_ARITHMETIC_OVERFLOW]", "unrelated failure"]:
                    changed = copy.deepcopy(candidate)
                    changed["results"][0]["actual"]["error"] = error
                    actual.write_text(json.dumps(changed))
                    self.assertEqual(integer_overflow.compare(oracle, actual)["agreement"], 1)
                changed = copy.deepcopy(candidate)
                changed["results"][1]["actual"]["rows"] = [["2147483648"]]
                actual.write_text(json.dumps(changed))
                self.assertEqual(integer_overflow.compare(oracle, actual)["agreement"], 1)
                changed["cases"][0]["sql"] = "SELECT 1"
                actual.write_text(json.dumps(changed))
                with self.assertRaises(AssertionError):
                    integer_overflow.compare(oracle, actual)


if __name__ == "__main__":
    unittest.main()
