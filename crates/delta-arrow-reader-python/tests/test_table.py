import json
from pathlib import Path
import tempfile
import unittest

from delta_arrow_reader import DeltaReaderError, DeltaTable


class TableTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="secret-table-")
        self.addCleanup(temporary.cleanup)
        self.location = Path(temporary.name)
        self.log = self.location / "_delta_log"
        self.log.mkdir()
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": False, "metadata": {}}
            ],
        }
        self.write_log(
            0,
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
            {
                "metaData": {
                    "id": "python-test-table",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": json.dumps(schema),
                    "partitionColumns": [],
                    "configuration": {},
                }
            },
        )

    def write_log(self, version, *actions):
        (self.log / f"{version:020}.json").write_text(
            "".join(json.dumps(action) + "\n" for action in actions), encoding="utf-8"
        )

    def test_latest_snapshot_is_immutable(self):
        original = DeltaTable(self.location)
        self.assertEqual(original.version, 0)
        self.assertEqual(repr(original), "DeltaTable(version=0)")
        self.write_log(1, {"commitInfo": {"operation": "WRITE"}})
        for location in (self.location, str(self.location), self.location.as_uri()):
            with self.subTest(location=location):
                table = DeltaTable(location)
                self.assertEqual(table.version, 1)
                self.assertNotIn(str(self.location), repr(table))
        self.assertEqual(original.version, 0)
        with self.assertRaises(AttributeError):
            original.version = 1

    def test_location_requires_text(self):
        class BytesPath:
            def __fspath__(self):
                return b"secret-table"

        for location in (None, 42, True, object(), b"secret-table", BytesPath()):
            with self.subTest(location=location), self.assertRaises(TypeError):
                DeltaTable(location)

    def test_reader_errors_are_redacted(self):
        empty = self.location / "secret-empty-table"
        empty.mkdir()
        cases = (
            (
                self.location / "secret-missing-table",
                "table_location",
                "invalid_table_location",
            ),
            (empty, "snapshot", "snapshot_load"),
            (
                "unknown://secret-user:secret-password@host/table?token=secret-token",
                "storage",
                "storage_initialization",
            ),
        )
        for location, phase, code in cases:
            with self.subTest(location=location):
                with self.assertRaises(DeltaReaderError) as caught:
                    DeltaTable(location)
                error = caught.exception
                self.assertEqual(error.phase, phase)
                self.assertEqual(error.code, code)
                self.assertIn(f"phase={phase} code={code}", str(error))
                for text in (str(error), repr(error), repr(error.args), repr(vars(error))):
                    self.assertNotIn("secret", text)
                self.assertIsNone(error.__cause__)
                self.assertIsNone(error.__context__)
        self.assertEqual(DeltaTable(self.location).version, 0)

    def test_protocol_validation_stays_deferred(self):
        self.write_log(
            1, {"protocol": {"minReaderVersion": 4, "minWriterVersion": 2}}
        )
        self.assertEqual(DeltaTable(self.location).version, 1)


if __name__ == "__main__":
    unittest.main()
