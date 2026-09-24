import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import decimal_bround


class BroundComparisonTest(unittest.TestCase):
    def test_values_nullability_error_identity_and_corpus(self):
        case = {"id": "decimal", "sql": "SELECT BROUND(a, 2) FROM t"}
        reference = {"cases": [case], "results": [
            {"id": "decimal_true", "actual": {"status": "ok", "types": ["decimal(6,2)"],
                "rows": [["2.34"], [None], ["2.34"]], "schema": {"fields": [{"nullable": True}]}}},
            {"id": "decimal_false", "actual": {"status": "execution_error",
                "condition": "NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION"}},
        ]}
        candidate = {"cases": [case], "results": [
            {"id": "decimal_true", "actual": {"status": "ok", "types": ["Decimal128(6, 2)"],
                "rows": [[None], ["2.34"], ["2.34"]], "logical_nullable": [True], "physical_nullable": [True]}},
            {"id": "decimal_false", "actual": {"status": "execution_error",
                "error": "[NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION] overflow"}},
        ]}
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            corpus, oracle, actual = (root / n for n in ["cases.jsonl", "spark.json", "candidate.json"])
            corpus.write_text(json.dumps(case) + "\n")
            oracle.write_text(json.dumps(reference))
            with patch.object(decimal_bround, "CASES", corpus):
                actual.write_text(json.dumps(candidate))
                self.assertEqual(decimal_bround.compare(oracle, actual)["agreement"], 2)
                for index, key, value in [
                    (0, "rows", [[None], ["2.35"], ["2.34"]]),
                    (0, "rows", [[None], ["2.34"]]),
                    (0, "types", ["Float64"]),
                    (0, "logical_nullable", [False]),
                    (1, "error", "unrelated error"),
                ]:
                    changed = copy.deepcopy(candidate)
                    changed["results"][index]["actual"][key] = value
                    actual.write_text(json.dumps(changed))
                    self.assertEqual(decimal_bround.compare(oracle, actual)["agreement"], 1)
                changed["cases"][0]["sql"] = "SELECT 1"
                actual.write_text(json.dumps(changed))
                with self.assertRaises(AssertionError):
                    decimal_bround.compare(oracle, actual)


if __name__ == "__main__":
    unittest.main()
