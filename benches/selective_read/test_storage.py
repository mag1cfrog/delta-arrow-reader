"""Check staged uploads without starting a server or reader."""

from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import run
import storage


class StagedUpload(unittest.TestCase):
    def test_inline_upload_reuses_checksums_and_rejects_changed_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixtures, state = root / "fixtures", root / "state"
            fixtures.mkdir()
            state.mkdir()
            table = fixtures / "base"
            table.mkdir()
            log, part = table / "log.json", table / "part.parquet"
            log.write_bytes(b"log")
            part.write_bytes(b"data")
            item = lambda p: {"path": p.name, "bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
            (fixtures / "manifest.json").write_text(json.dumps({"status": "complete", "protocol": "selective-read-v1",
                "tables": [{"id": "base", "path": "base", "delta_log": item(log), "files": [item(part)]}]}))
            (state / "server.json").write_text("{}")
            upload = root / "upload.json"
            scans = []
            original = run.hashlib.file_digest
            def digest(source, algorithm):
                scans.append(source.name)
                return original(source, algorithm)
            with patch.object(storage, "verify_server"), patch.object(storage, "exclusive", side_effect=lambda _: nullcontext()), \
                    patch.object(storage, "put_verified", return_value="server-sha256"), \
                    patch.object(run.hashlib, "file_digest", side_effect=digest):
                storage.upload(state, fixtures, upload, ["base"], checksum=True)
                storage.inventory(fixtures, ["base"])
            self.assertEqual(scans.count(str(part)), 1)
            receipt = json.loads(upload.read_text())
            self.assertEqual(receipt["verification"], {"requested": "server-sha256", "methods": {"server-sha256": 2}})
            stamp = part.stat()
            part.write_bytes(b"bad!")
            os.utime(part, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
            with self.assertRaises(AssertionError):
                storage.inventory(fixtures, ["base"])

    def test_selection_and_missing_remote_table(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixtures, state = root / "fixtures", root / "state"
            fixtures.mkdir()
            state.mkdir()
            tables = []
            for name in ("base", "base.dv"):
                (fixtures / name).mkdir()
                items = []
                for filename in ("log.json", "part.parquet", "sidecar.bin"):
                    path = fixtures / name / filename
                    path.write_bytes((name + filename).encode())
                    items.append({"path": filename, "bytes": path.stat().st_size, "sha256": storage.digest(path)})
                tables.append({"id": name, "path": name, "delta_log": items[0],
                               "files": [items[1] | ({"deletion_vector": items[2]} if name.endswith(".dv") else {})]})
            (fixtures / "manifest.json").write_text(json.dumps({"status": "complete", "protocol": "selective-read-v1", "tables": tables}))
            selected = storage.inventory(fixtures, ["base"])
            self.assertEqual([item["path"] for item in selected], ["base/log.json", "base/part.parquet"])
            self.assertEqual(len(storage.inventory(fixtures)), 5)
            self.assertEqual(len(storage.inventory(fixtures, ["base.dv"])), 3)
            for invalid in ([], ["base", "base"], ["unknown"], "base", [None]):
                with self.assertRaisesRegex(ValueError, "upload table selection"):
                    storage.inventory(fixtures, invalid)
            (fixtures / "base.dv/part.parquet").unlink()
            self.assertEqual(storage.inventory(fixtures, ["base"]), selected)
            with self.assertRaises(AssertionError):
                storage.inventory(fixtures)

            (fixtures / "base/part.parquet").write_bytes(b"changed")
            with self.assertRaises(AssertionError):
                storage.inventory(fixtures, ["base"])


if __name__ == "__main__":
    unittest.main()
