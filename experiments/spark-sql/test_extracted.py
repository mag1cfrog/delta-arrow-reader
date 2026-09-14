"""Transport checks require the same PyArrow environment as extracted.py."""

import unittest
import json
import subprocess
from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa

from extracted import fixture_table, observation, read_arrow
from reference import ROOT, load_corpus


class ArrowTransportTests(unittest.TestCase):
    def test_resolved_dependencies_do_not_include_python_bridges(self):
        metadata = json.loads(subprocess.check_output([
            "cargo", "metadata", "--locked", "--format-version=1", "--manifest-path", str(ROOT / "Cargo.toml")]))
        forbidden = [package["name"] for package in metadata["packages"]
                     if package["name"].startswith("pyo3") or package["name"] in {"sail-python-udf", "sail-pyarrow"}]
        self.assertEqual(forbidden, [])

    def test_inputs_and_nested_nulls(self):
        inputs, _ = load_corpus()
        for name, table in inputs["tables"].items():
            actual = observation(fixture_table(table))
            self.assertEqual(actual["schema"], table["schema"], name)
            self.assertEqual(len(actual["rows"]), len(table["rows"]), name)
        nested = observation(fixture_table(inputs["tables"]["nested"]))
        self.assertEqual(nested["rows"][-1], ["3", [], {"map_entries": []}, None])
        self.assertEqual(nested["rows"][1], ["2", [None], {"map_entries": [["one", None]]}, [None, None]])

    def test_stream_dictionary_replacement_and_exact_values(self):
        import tempfile
        from pathlib import Path

        schema = pa.schema([("same", pa.dictionary(pa.int16(), pa.string())),
                            ("same", pa.decimal128(10, 6)), ("ts", pa.timestamp("us", tz="UTC")),
                            ("binary", pa.binary())])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stream.arrow"
            with pa.ipc.new_stream(path, schema) as writer:
                for text in ["west", "east"]:
                    writer.write_batch(pa.RecordBatch.from_arrays([
                        pa.array([text], type=schema[0].type), pa.array([Decimal("0.666667")], type=schema[1].type),
                        pa.array([datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)], type=schema[2].type),
                        pa.array([b"\x00\xff"], type=schema[3].type)], schema=schema))
            actual = read_arrow(path)
        self.assertEqual([field["name"] for field in actual["schema"]["fields"]], ["same", "same", "ts", "binary"])
        self.assertEqual(actual["schema"]["fields"][1]["type"], "decimal(10,6)")
        self.assertEqual(actual["rows"], [[text, "0.666667", "2024-01-02 03:04:05.123456", {"binary_hex": "00ff"}]
                                          for text in ["west", "east"]])
        self.assertEqual(observation(pa.Table.from_batches([], schema=schema))["rows"], [])


if __name__ == "__main__":
    unittest.main()
