"""Regression check for an exited transient proxy and an unsuccessful stop."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import network


class Cleanup(unittest.TestCase):
    def test_service_exit_and_stop_failure(self):
        for active, pid in (("inactive", "0"), ("active", "123")):
            with self.subTest(active=active), tempfile.TemporaryDirectory() as temp:
                directory = Path(temp)
                pointer = directory / "network.json"
                pointer.write_text("{}")
                config = dict(output=str(directory), unit="owned-proxy")
                actual = dict(ActiveState=active, MainPID=pid)
                def systemctl(command, *, check=False, **kwargs):
                    if check:
                        raise subprocess.CalledProcessError(5, command)
                    return subprocess.CompletedProcess(command, 5)
                with patch.object(network, "config", return_value=config), \
                     patch.object(network.storage, "properties", return_value=actual), \
                     patch.object(network.subprocess, "run", side_effect=systemctl):
                    # systemctl reports a nonzero status for a vanished unit.
                    if pid == "0":
                        network.stop(directory)
                        self.assertFalse(pointer.exists())
                        self.assertEqual(json.loads((directory / "stopped.json").read_text())["properties"], actual)
                        pointer.write_text("{}")
                        network.stop(directory)  # Recover after a receipt was saved but the pointer remained.
                        self.assertFalse(pointer.exists())
                    else:
                        with self.assertRaises(AssertionError):
                            network.stop(directory)
                        self.assertTrue(pointer.exists())
                        self.assertFalse((directory / "stopped.json").exists())


if __name__ == "__main__":
    unittest.main()
