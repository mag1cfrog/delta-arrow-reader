"""A capture must expose missing evidence and independent schema/error differences."""

from copy import deepcopy
import unittest

import pyarrow as pa

from arithmetic_schema_errors import cause, compare


class SchemaErrorChecks(unittest.TestCase):
    def test_decimal_diagnostics_preserve_ambiguity_and_round_identity(self):
        for prefix in ("", "spark_decimal_null_propagation\ncaused by\n",
                       "Optimizer rule 'simplify_expressions' failed\ncaused by\n"):
            for text, expected in [
                ('Execution error: Cannot cast Some("bad") to DECIMAL(10,2)', None),
                ('Execution error: Cannot cast Some("99999999.995") to DECIMAL(10,2)', None),
                ('Execution error: Cannot cast Some("1e40") to DECIMAL(10,2)', None),
                ('Execution error: Cannot cast Some("0e38") to DECIMAL(10,2)', None),
                ('Arrow error: Cast error: Cannot cast to Decimal128(5, 2). Overflowing on 1e40',
                 'NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION'),
                ('Arrow error: Invalid argument error: -100 is too small to store in a Decimal128 of precision 2. Min is -99',
                 'NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION'),
                ('Arrow error: Arithmetic overflow: Spark decimal division result exceeds its precision',
                 'NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION'),
                ('Arrow error: Compute error: Decimal overflow: rounded value exceeds precision 2: '
                 'Invalid argument error: 100 is too large to store in a Decimal128 of precision 2. Max is 99',
                 'NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION'),
                ('Arrow error: Cast error: Cannot cast string bad to value of Decimal128(5, 2)', 'CAST_INVALID_INPUT'),
                ('Arrow error: Divide by zero error', 'DIVIDE_BY_ZERO'),
                ('Execution error: Cannot cast Some("Spark decimal division result exceeds its precision") to DECIMAL(10,2)', None),
            ]:
                self.assertEqual(cause({'error': prefix + text}), expected, text)
        actual = {'error': 'Execution error: Cannot cast Some("bad") to DECIMAL(10,2)',
                  'condition': 'CAST_INVALID_INPUT'}
        self.assertEqual(cause(actual), 'CAST_INVALID_INPUT')
        reference = {'cases': [{'id': 'invalid', 'sql': "SELECT CAST('bad' AS DECIMAL(10,2))"}],
                     'results': [{'id': 'invalid_' + ansi, 'actual': {
                         'status': 'execution_error', 'condition': 'CAST_INVALID_INPUT'}}
                         for ansi in ('true', 'false')]}
        candidate = deepcopy(reference)
        for row in candidate['results']:
            row['actual'] = {'status': 'execution_error', 'error': actual['error']}
        result = compare(reference, candidate)
        self.assertEqual(result['summary']['error_cause'], {'ambiguous': 2})

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
