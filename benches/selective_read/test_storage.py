"""Check staged uploads without starting a server or reader."""

from argparse import Namespace
import base64
from contextlib import nullcontext
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import campaign
import run
import storage


class StagedUpload(unittest.TestCase):
    def test_remote_checksums_lengths_and_legacy_readback(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "part.parquet"
            source.write_bytes(b"data")
            expected = hashlib.sha256(b"data").hexdigest()
            sha = base64.b64encode(bytes.fromhex(expected)).decode()
            def process(body=b"", data=b"data"):
                return SimpleNamespace(returncode=0, communicate=lambda: (body, b""),
                                       stdout=io.BytesIO(data), stderr=io.BytesIO(), wait=lambda: 0)
            def head(fields):
                return process(("HTTP/1.1 200 OK\r\n" + "\r\n".join(fields) + "\r\n\r\n200").encode())
            correct = ["Content-Length: 4", "X-Amz-Checksum-Sha256: " + sha, "X-Amz-Checksum-Type: FULL_OBJECT"]
            for status in (b"200", b"412"):
                with patch.object(storage, "curl", side_effect=[process(status), head(correct)]) as curl:
                    self.assertEqual(storage.put_verified(None, "key", source, expected, checksum=True), "server-sha256")
                    self.assertTrue(all("--upload-file" in call.args or "--head" in call.args for call in curl.call_args_list))
            for fields in (["Content-Length: 5"] + correct[1:], correct + ["Content-Length: 4"],
                           correct + ["X-Amz-Checksum-Sha256: " + sha],
                           [correct[0], "X-Amz-Checksum-Sha256: " + "A" * 44, correct[2]]):
                with patch.object(storage, "curl", side_effect=[process(b"412"), head(fields)]), self.assertRaises(AssertionError):
                    storage.put_verified(None, "key", source, expected, checksum=True)
            for fields in (["Content-Length: 4"],
                           [correct[0], "X-Amz-Checksum-Sha256: " + sha + "-2", "X-Amz-Checksum-Type: COMPOSITE"]):
                with patch.object(storage, "curl", side_effect=[process(b"412"), head(fields), process()]):
                    self.assertEqual(storage.put_verified(None, "key", source, expected, checksum=True), "full-get")
            with patch.object(storage, "curl", side_effect=[process(b"412"), head([correct[0]]), process(data=b"bad!")]), \
                    self.assertRaises(AssertionError):
                storage.put_verified(None, "key", source, expected, checksum=True)
            with patch.object(storage, "curl", side_effect=[process(b"200"), process()]):
                self.assertEqual(storage.put_verified(None, "key", source, expected), "full-get")

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
            args = Namespace(output=root / "campaign", fixtures=fixtures, state=state, upload=root / "upload.json",
                             upload_table=["base"], binary=[], matrix=None, workload=None, case=["base.query"],
                             reference=[], no_sessions=True, session=None)
            scans = []
            original = run.hashlib.file_digest
            def digest(source, algorithm):
                scans.append(source.name)
                return original(source, algorithm)
            with patch.object(storage, "verify_server"), patch.object(storage, "exclusive", side_effect=lambda _: nullcontext()), \
                    patch.object(storage, "put_verified", return_value="server-sha256"), \
                    patch.object(storage, "state", return_value={"cpus": {"observer": [0]}}), \
                    patch.object(campaign.os, "sched_setaffinity"), patch.object(campaign.observe, "invoke") as invoke, \
                    patch.object(run.hashlib, "file_digest", side_effect=digest):
                self.assertEqual(campaign.execute(args), 1)  # No reference: no native reader may launch.
                invoke.assert_not_called()
            self.assertEqual(scans.count(str(part)), 1)
            receipt = json.loads(args.upload.read_text())
            self.assertEqual(receipt["verification"], {"requested": "server-sha256", "methods": {"server-sha256": 2}})
            args.output = root / "existing-receipt"
            with self.assertRaisesRegex(ValueError, "new receipt path"):
                campaign.execute(args)
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

            (state / "server.json").write_text("{}")
            prefix = storage.digest(fixtures / "manifest.json")
            receipt = {"status": "verified", "server_sha256": storage.digest(state / "server.json"),
                       "fixture_manifest_sha256": prefix, "table_root": f"s3://{storage.BUCKET}/{prefix}",
                       "table_ids": ["base"], "objects": selected}
            upload = root / "upload.json"
            upload.write_text(json.dumps(receipt))
            args = Namespace(output=root / "campaign", fixtures=fixtures, state=state, upload=upload,
                             binary=[], matrix=None, workload=None, case=["base.dv.query"],
                             reference=[], no_sessions=True, session=None)
            with patch.object(storage, "state", return_value={"cpus": {"observer": [0]}}), \
                    patch.object(storage, "exclusive", return_value=nullcontext()), \
                    patch.object(campaign.os, "sched_setaffinity"), \
                    patch.object(campaign.run, "request", return_value={"table_uri": (fixtures / "base.dv").as_uri()}), \
                    patch.object(campaign.observe, "invoke") as invoke:
                self.assertEqual(campaign.execute(args), 1)
                invoke.assert_not_called()
            inventory = json.loads((args.output / "inventory.json").read_text())
            entries = inventory["base.dv.query"]
            self.assertEqual(set(entries), set(campaign.READERS))
            self.assertTrue(all(row["status"] == "preparation_failed"
                                and "table is absent from verified upload" in row["failure_reason"]
                                for row in entries.values()))
            args.output = root / "forged-campaign"
            receipt["objects"].append({"path": "base.dv/sidecar.bin"})
            upload.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "objects differ from verified upload"):
                campaign.execute(args)
            (fixtures / "base/part.parquet").write_bytes(b"changed")
            with self.assertRaises(AssertionError):
                storage.inventory(fixtures, ["base"])


if __name__ == "__main__":
    unittest.main()
