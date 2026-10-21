"""Python SDK reader digest caching rejects mutations and replacements."""
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent / "runners"))
import run


class ReaderDigests(unittest.TestCase):
    def test_checksums_share_hardlinks_and_detect_writes_replacement_and_races(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path, link = root / "file", root / "link"
            path.write_bytes(b"aaaa")
            link.hardlink_to(path)
            initial = path.stat()
            actual_digest = hashlib.file_digest
            with patch.object(hashlib, "file_digest", wraps=actual_digest) as calls:
                before = run.digest(path)
                self.assertEqual(run.digest(link), before)
                self.assertEqual(run.digest(path), before)
                self.assertEqual(calls.call_count, 1)
                path.write_bytes(b"bbbb")
                os.utime(path, ns=(initial.st_atime_ns, initial.st_mtime_ns))
                after = run.digest(link)
                self.assertNotEqual(after, before)
                self.assertEqual(calls.call_count, 2)
                replacement = root / "replacement"
                replacement.write_bytes(b"cccc")
                os.utime(replacement, ns=(initial.st_atime_ns, initial.st_mtime_ns))
                replacement.replace(path)
                self.assertNotEqual(run.digest(path), after)
                self.assertEqual(calls.call_count, 3)
            race = root / "race"
            race.write_bytes(b"dddd")

            def mutate(stream, algorithm):
                value = actual_digest(stream, algorithm)
                race.write_bytes(b"eeee")
                return value

            with patch.object(hashlib, "file_digest", side_effect=mutate):
                with self.assertRaisesRegex(ValueError, "changed during checksum"):
                    run.digest(race)
            self.assertEqual(run.digest(race), hashlib.sha256(b"eeee").hexdigest())
