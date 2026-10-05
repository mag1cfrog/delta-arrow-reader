"""Check physical pairing capacity without allocating a full Q2 table."""

import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import production_pairs as pairs


class PairCapacity(unittest.TestCase):
    @patch.object(pairs.shutil, "disk_usage", return_value=SimpleNamespace(free=400 * 1024**3))
    def test_shared_inputs_copy_space_and_limits(self, _disk_usage):
        gib = 1024**3
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixtures, transfers = [], []
            for name in ("localized", "scattered"):
                fixture = root / name
                (fixture / name).mkdir(parents=True)
                data = fixture / name / "part.parquet"
                # Sparse files reproduce the 132 GiB input size without writing it.
                with data.open("wb") as stream:
                    stream.truncate(66 * gib)
                geometry = data.with_suffix(".geometry.json")
                geometry.write_text("{}")
                log = data.with_name("delta.json")
                log.write_text("{}")
                table = {"path": name, "files": [{"path": data.name, "bytes": data.stat().st_size,
                         "geometry": {"path": geometry.name, "bytes": geometry.stat().st_size}}],
                         "delta_log": {"path": log.name, "bytes": log.stat().st_size}}
                fixtures.append(fixture)
                transfers.append((fixture, table))
            os.link(transfers[0][0] / "localized/part.parquet", fixtures[1] / "shared-input")
            output = root / "pairs"
            phase = pairs.capacity(fixtures, transfers, output, 192 * gib)
            self.assertGreaterEqual(phase["retained_input_bytes"], 132 * gib)
            self.assertLess(phase["retained_input_bytes"], 133 * gib)
            self.assertLess(phase["conservative_bytes"], 192 * gib)
            self.assertEqual(phase["copied_input_bytes"], 0)
            self.assertEqual(phase["additional_disk_bytes"], phase["metadata_reserve_bytes"])
            self.assertTrue(phase["immutable_hardlinks"])
            with self.assertRaisesRegex(ValueError, "disk ceiling"):
                pairs.capacity(fixtures, transfers, output, 131 * gib)
            with patch.object(pairs.shutil, "disk_usage", return_value=SimpleNamespace(
                    free=phase["additional_disk_bytes"] - 1)), self.assertRaisesRegex(ValueError, "disk ceiling"):
                pairs.capacity(fixtures, transfers, output, 192 * gib)

            actual_stat = Path.stat
            def foreign_destination(path, *args, **kwargs):
                value = actual_stat(path, *args, **kwargs)
                if path == output.parent:
                    fields = list(value)
                    fields[2] += 1
                    return os.stat_result(fields)
                return value
            with patch.object(Path, "stat", foreign_destination), self.assertRaisesRegex(ValueError, "disk ceiling"):
                pairs.capacity(fixtures, transfers, output, 192 * gib)
            # A small cross-device transfer reserves data, sidecar and log copies.
            for fixture, table in transfers:
                data = fixture / table["path"] / "part.parquet"
                with data.open("wb") as stream:
                    stream.truncate(1024)
                table["files"][0]["bytes"] = 1024
            with patch.object(Path, "stat", foreign_destination):
                copied = pairs.capacity(fixtures, transfers, output, gib)
            self.assertEqual(copied["copied_input_bytes"], 2 * (1024 + 2 + 2))
            self.assertEqual(copied["additional_disk_bytes"], copied["copied_input_bytes"] + copied["metadata_reserve_bytes"])
            self.assertFalse(copied["immutable_hardlinks"])
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
