import copy
import json
from pathlib import Path
import tempfile
import unittest

import float_bround


class FloatBroundComparisonTest(unittest.TestCase):
    def test_float_bits_schema_and_duplicate_rows(self):
        case = {"id": "float", "sql": "SELECT BROUND(a, 1) FROM t", "category": "target"}
        oracle = {"cases": [case], "results": [
            {"id": f"float_{ansi}", "actual": {"status": "ok", "types": ["float"],
             "rows": [["1.2000000476837158"], [None], ["1.2000000476837158"]],
             "schema": {"fields": [{"nullable": True}]}}} for ansi in ("true", "false")]}
        candidate = copy.deepcopy(oracle)
        for row in candidate["results"]:
            row["actual"].update(types=["Float32"], rows=[[None], ["1.2"], ["1.2"]],
                                 logical_nullable=[True], physical_nullable=[True])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            corpus, reference, actual = (root / n for n in ("cases.jsonl", "spark.json", "candidate.json"))
            corpus.write_text(json.dumps(case) + "\n")
            reference.write_text(json.dumps(oracle))
            actual.write_text(json.dumps(candidate))
            self.assertEqual(float_bround.compare(reference, actual, corpus)["summary"]["target"]["agreement"], 2)
            for key, value in [("types", ["Float64"]), ("rows", [[None], ["1.2"]]),
                               ("rows", [[None], ["1.2000002"], ["1.2"]]),
                               ("logical_nullable", [False]), ("status", "execution_error")]:
                changed = copy.deepcopy(candidate)
                changed["results"][0]["actual"][key] = value
                actual.write_text(json.dumps(changed))
                self.assertEqual(float_bround.compare(reference, actual, corpus)["summary"]["target"]["agreement"], 1)
            changed["cases"][0]["sql"] = "SELECT 1"
            actual.write_text(json.dumps(changed))
            with self.assertRaises(AssertionError):
                float_bround.compare(reference, actual, corpus)
        self.assertNotEqual(float_bround.values({"types": ["Float32"], "rows": [["0.0"]]}),
                            float_bround.values({"types": ["Float32"], "rows": [["-0.0"]]}))


if __name__ == "__main__":
    unittest.main()
