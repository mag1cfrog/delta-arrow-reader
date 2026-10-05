"""Check staged uploads without starting a server or reader."""

from argparse import Namespace
from contextlib import nullcontext
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import campaign
import storage


class StagedUpload(unittest.TestCase):
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
