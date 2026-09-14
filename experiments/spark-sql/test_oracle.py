"""Checks for comparison rules that must not hide SQL regressions."""

import copy
import unittest

from oracle import checked_rows, compare, schema_dimensions, validate_capture
from reference import corpus_hash


class OracleChecks(unittest.TestCase):
    def test_order_and_duplicate_rows(self):
        rows = [["1"], ["2"], ["1"]]
        reordered = [["1"], ["1"], ["2"]]
        self.assertEqual(checked_rows({}, rows), checked_rows({}, reordered))
        self.assertNotEqual(checked_rows({"comparison": "ordered"}, rows), reordered)
        self.assertNotEqual(checked_rows({}, rows), checked_rows({}, [["1"], ["2"]]))

    def test_decimal_difference_is_not_rounded_away(self):
        self.assertNotEqual(checked_rows({}, [["0.666667"]]), checked_rows({}, [["0.666666"]]))

    def test_partition_invariants(self):
        case = {"comparison": "partition_sort"}
        self.assertEqual(checked_rows(case, [["1", "0"], ["3", "0"], ["2", "1"]]),
                         checked_rows(case, [["1", "9"], ["2", "9"], ["3", "9"]]))
        with self.assertRaises(ValueError):
            checked_rows(case, [["3", "0"], ["1", "0"]])
        with self.assertRaises(ValueError):
            checked_rows({"comparison": "monotonic_id"}, [["1", "0", "5"], ["2", "1", "5"]])
        with self.assertRaises(ValueError):
            checked_rows({"comparison": "partition_id"}, [["1", "-1"]])

    def test_nested_nullability_is_separate_from_type(self):
        schema = {"type": "struct", "fields": [{"name": "a", "nullable": True, "metadata": {},
                  "type": {"type": "array", "elementType": "integer", "containsNull": True}}]}
        other = copy.deepcopy(schema)
        other["fields"][0]["type"]["containsNull"] = False
        a, b = schema_dimensions(schema), schema_dimensions(other)
        self.assertEqual(a["types"], b["types"])
        self.assertNotEqual(a["nullability"], b["nullability"])
        other["fields"][0]["type"]["elementType"] = "long"
        self.assertNotEqual(a["types"], schema_dimensions(other)["types"])

    def test_error_cause_and_stage_are_both_checked(self):
        expected = {"status": "execution_error", "condition": "DIVIDE_BY_ZERO"}
        other = {"status": "planning_error", "condition": "UNRESOLVED_ROUTINE"}
        self.assertEqual(compare({}, expected, other)["differences"], ["status", "error_condition"])

    def test_capture_must_cover_exact_current_corpus(self):
        inputs, cases = {}, [{"id": "a"}, {"id": "b"}]
        capture = {"corpus_sha256": corpus_hash(inputs, cases), "observations": [{"id": "a"}, {"id": "b"}]}
        validate_capture(capture, inputs, cases)
        capture["observations"].pop()
        with self.assertRaises(ValueError):
            validate_capture(capture, inputs, cases)
        capture["corpus_sha256"] = "stale"
        with self.assertRaises(ValueError):
            validate_capture(capture, inputs, cases)


if __name__ == "__main__":
    unittest.main()
