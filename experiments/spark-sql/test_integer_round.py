import json
from pathlib import Path
import tempfile
import unittest

from integer_round import compare


class IntegerRoundComparison(unittest.TestCase):
    def test_overflow_cause_and_bigint_precision_are_checked(self):
        cases = [{"id": "round", "sql": "SELECT ROUND(9223372036854775807L, -1) AS r"}]
        reference = {"cases": cases, "results": [
            {"id": "round_true", "actual": {"status": "execution_error", "condition": "ARITHMETIC_OVERFLOW"}},
            {"id": "round_false", "actual": {"status": "ok", "types": ["long"], "rows": [["-9223372036854775806"]]}}]}
        candidate = {"cases": cases, "results": [
            {"id": "round_true", "actual": {"status": "planning_error", "error": "[ARITHMETIC_OVERFLOW] overflow"}},
            {"id": "round_false", "actual": {"status": "ok", "types": ["Int64"], "rows": [["-9223372036854775806"]]}}]}
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory)
            corpus, left, right = (p / n for n in ["cases.jsonl", "reference.json", "candidate.json"])
            corpus.write_text(json.dumps(cases[0]) + "\n")
            left.write_text(json.dumps(reference))
            right.write_text(json.dumps(candidate))
            check = compare(left, right, corpus)
            self.assertEqual(check["agreement"], 2)
            self.assertEqual(check["cases"][0]["phase_difference"], ["execution_error", "planning_error"])
            candidate["results"][0]["actual"]["error"] = "[CAST_OVERFLOW] unrelated cast"
            candidate["results"][1]["actual"]["rows"] = [["-9223372036854775808"]]
            right.write_text(json.dumps(candidate))
            check = compare(left, right, corpus)
            self.assertEqual(check["agreement"], 0)
            self.assertEqual(check["cases"][0]["differences"], ["error_cause"])
            self.assertEqual(check["cases"][1]["differences"], ["rows"])


if __name__ == "__main__":
    unittest.main()
