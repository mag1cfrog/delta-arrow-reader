import json
from pathlib import Path
import tempfile
import unittest

from round_arguments import compare


class RoundArgumentComparison(unittest.TestCase):
    def test_error_cause_is_required_independently_of_phase(self):
        case = {"id": "cast", "sql": "SELECT ROUND(1.2, 'bad')"}
        reference = {"cases": [case], "results": [
            {"id": f"cast_{ansi}", "actual": {"status": "planning_error",
             "condition": "CAST_INVALID_INPUT", "error": "[CAST_INVALID_INPUT] bad scale"}}
            for ansi in ("true", "false")]}
        candidate = {"cases": [case], "results": [
            {"id": f"cast_{ansi}", "actual": {"status": "execution_error",
             "error": 'Execution error: Cannot cast Some("bad") to INT'}}
            for ansi in ("true", "false")]}
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory)
            corpus, left, right = (p / name for name in ("cases.jsonl", "spark.json", "candidate.json"))
            corpus.write_text(json.dumps(case) + "\n")
            left.write_text(json.dumps(reference))
            right.write_text(json.dumps(candidate))
            result = compare(left, right, corpus)
            self.assertEqual(result["agreement"], 2)
            self.assertEqual(result["cases"][0]["phase_difference"], ["planning_error", "execution_error"])
            for error in ("[DIVIDE_BY_ZERO] zero", "an unrelated planning error"):
                candidate["results"][0]["actual"] = {"status": "planning_error", "error": error}
                right.write_text(json.dumps(candidate))
                result = compare(left, right, corpus)
                self.assertEqual(result["agreement"], 1)
                self.assertEqual(result["cases"][0]["differences"], ["error_cause"])


if __name__ == "__main__":
    unittest.main()
