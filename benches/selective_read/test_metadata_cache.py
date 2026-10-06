"""Changed manifests or workload rows must still reach original validation."""

import copy
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import metadata_cache
import production_workloads as production


class MetadataCache(unittest.TestCase):
    def test_reuse_changes_races_and_scope_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "manifest.json"
            manifest.write_text("original")
            row = {"case_id": "case", "canonical_sql": "SELECT original", "nested": {"value": 1}}
            value = {"cases": [row]}
            def validate(value, fixtures, case):
                if row["canonical_sql"] != "SELECT original" or row["nested"]["value"] != 1:
                    raise ValueError("workload row changed")
                if manifest.read_text() != "original":
                    raise ValueError("manifest changed")
                return row
            with patch.object(production, "binding", side_effect=validate) as original:
                with metadata_cache.bindings():
                    self.assertIs(production.binding(value, root, "case"), row)
                    equal = copy.deepcopy(value)
                    self.assertIs(production.binding(equal, root, "case"), equal["cases"][0])
                    self.assertEqual(original.call_count, 1)
                    row["nested"]["value"] = 2
                    with self.assertRaisesRegex(ValueError, "workload row changed"):
                        production.binding(value, root, "case")
                    row["nested"]["value"] = 1
                    stamp = manifest.stat()
                    manifest.write_text("tampered")
                    os.utime(manifest, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
                    with self.assertRaisesRegex(ValueError, "manifest changed"):
                        production.binding(value, root, "case")
                    replacement = root / "replacement"
                    replacement.write_text("original")
                    replacement.replace(manifest)
                    production.binding(value, root, "case")
                    self.assertEqual(original.call_count, 4)
                self.assertIs(production.binding, original)
                with metadata_cache.bindings():
                    production.binding(value, root, "case")
                    self.assertEqual(original.call_count, 5)
                def race(*args):
                    manifest.write_text("tampered")
                    return row
                original.side_effect = race
                with self.assertRaisesRegex(ValueError, "changed during metadata validation"), metadata_cache.bindings():
                    production.binding(value, root, "case")
                self.assertIs(production.binding, original)


if __name__ == "__main__":
    unittest.main()
