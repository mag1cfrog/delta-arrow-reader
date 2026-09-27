"""A capture must expose missing evidence and independent schema/error differences."""

from copy import deepcopy
import unittest

import pyarrow as pa

from arithmetic_schema_errors import compare


class SchemaErrorChecks(unittest.TestCase):
    def test_schema_dimensions_error_causes_and_capture_identity(self):
        schema = {"type": "struct", "fields": [{"name": "r", "type": "decimal(10,2)",
                                               "nullable": True, "metadata": {}}]}
        native = pa.schema([pa.field("internal", pa.decimal128(10, 2), True)])
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, native):
            pass
        payload = list(sink.getvalue().to_pybytes())
        reference = {"cases": [{"id": "cast", "sql": "SELECT 1"}], "results": [
            {"id": "cast_true", "actual": {"status": "ok", "schema": schema}},
            {"id": "cast_false", "actual": {"status": "execution_error", "condition": "REMAINDER_BY_ZERO"}}]}
        candidate = deepcopy(reference)
        candidate["results"][0]["actual"] = {"status": "ok", "output_names": ["r"],
                                              "logical_schema_ipc": payload, "physical_schema_ipc": payload}
        candidate["results"][1]["actual"] = {"status": "execution_error", "error": "Arrow error: Divide by zero error"}
        result = compare(reference, candidate)["cases"]
        self.assertEqual(set(result[0]["schemas"]["logical"].values()), {"match"})
        self.assertEqual(result[1]["stage_check"], "match")
        self.assertEqual(result[1]["error_cause_check"], "difference")
        self.assertEqual(set(result[1]["schemas"]["physical"].values()), {"unobserved"})
        for field, value, dimension in [("name", "other", "names"), ("type", "decimal(11,2)", "types"),
                                         ("type", "decimal(10,3)", "types"), ("nullable", False, "nullability"),
                                         ("metadata", {"origin": "fixture"}, "metadata")]:
            changed = deepcopy(reference)
            changed["results"][0]["actual"]["schema"]["fields"][0][field] = value
            checks = compare(changed, candidate)["cases"][0]["schemas"]["logical"]
            self.assertEqual([key for key, state in checks.items() if state == "difference"], [dimension])
        candidate["results"][1]["actual"]["error"] = "unclassified failure"
        self.assertEqual(compare(reference, candidate)["cases"][1]["error_cause_check"], "unclassified")
        candidate["results"][0]["actual"].pop("logical_schema_ipc")
        self.assertEqual(set(compare(reference, candidate)["cases"][0]["schemas"]["logical"].values()), {"unobserved"})
        candidate["results"].pop()
        with self.assertRaises(ValueError):
            compare(reference, candidate)
        candidate = deepcopy(reference)
        candidate["cases"][0]["sql"] = "SELECT 2"
        with self.assertRaises(ValueError):
            compare(reference, candidate)


if __name__ == "__main__":
    unittest.main()
