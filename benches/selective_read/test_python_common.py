"""Shared reader bookkeeping preserves artifacts, rejection and CLI behavior."""

from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent / "runners"))
import python_common as common
import run


class ReaderBookkeeping(unittest.TestCase):
    def test_native_bridge_preserves_exports_and_failures(self):
        request = json.loads((Path(__file__).parent / "runners/contract-cases.json").read_text())["request"]
        common.validate(request)
        with self.assertRaises(ValueError):
            common.validate(request | {"surprise": True})
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            (output / "query-0.arrow").write_bytes(b"export")
            exported = common.export_result(output, 0, {"case_id": "case"})
            self.assertEqual(exported, {"result": "query-0.arrow", "identity": "query-0.identity.json"})
            self.assertEqual(json.loads((output / exported["identity"]).read_text())["result_sha256"], run.digest(output / exported["result"]))
            with redirect_stdout(io.StringIO()) as stdout:
                self.assertEqual(common.write_observation(output, {"status": "success"}), 0)
            self.assertEqual(json.loads(stdout.getvalue()), {"status": "success"})
            with self.assertRaises(ValueError):
                common.write_observation(output, {"status": "validation_failed"})
            self.assertEqual(json.loads((output / "record.json").read_text()), {"status": "success"})



    def test_adapter_probes_use_bundled_helpers_and_restore_controller_imports(self):
        previous_path = sys.path.copy()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "run.py").write_text("marker = 'bundled run'\n")
            (root / "python_common.py").write_text("marker = 'bundled common'\n")
            (root / "native.py").write_text("marker = 'bundled controller'\n")
            binary = root / "adapter"
            source = ("import sys\nfrom pathlib import Path\n"
                      "sys.path.insert(0, str(Path(__file__).parent))\n"
                      "from run import marker as runner\n"
                      "from python_common import marker as common\nfrom native import marker as controller\n")
            binary.write_text(source)
            adapter = run.load_adapter(binary)
            self.assertEqual((adapter["runner"], adapter["common"]), ("bundled run", "bundled common"))
            self.assertEqual(adapter["controller"], "bundled controller")
            self.assertIs(sys.modules["native"], run.native)
            self.assertIs(sys.modules["run"], run)
            self.assertIs(sys.modules["python_common"], common)
            self.assertEqual(sys.path, previous_path)
            binary.write_text(source + "raise ValueError('probe failed')\n")
            with self.assertRaisesRegex(ValueError, "probe failed"):
                run.load_adapter(binary)
            self.assertIs(sys.modules["run"], run)
            self.assertIs(sys.modules["python_common"], common)
            self.assertEqual(sys.path, previous_path)


    def test_cli_and_credentials(self):
        calls = []
        def run(request, output):
            calls.append((request, output))
            return 7
        def describe():
            print('{"build": "test"}')
        for arguments, code, message in (([], 1, "expected REQUEST.json NEW_OUTPUT_DIRECTORY"),
                                        (["request.json", "output"], 7, None)):
            with patch.object(sys, "argv", ["reader", *arguments]), redirect_stderr(io.StringIO()) as stderr:
                with self.assertRaises(SystemExit) as exited:
                    common.runner_cli(run, describe)
                self.assertEqual(exited.exception.code, code)
                self.assertEqual(json.loads(stderr.getvalue()) if message else stderr.getvalue(),
                                 {"status": "invalid_input", "failure_reason": message} if message else "")
        self.assertEqual(calls, [(Path("request.json"), Path("output"))])
        with patch.object(sys, "argv", ["reader", "--describe-build"]), redirect_stdout(io.StringIO()) as stdout:
            common.runner_cli(run, describe)
        self.assertEqual(json.loads(stdout.getvalue()), {"build": "test"})
        def failing_describe():
            describe()
            raise ValueError("cleanup failed")
        with patch.object(sys, "argv", ["reader", "--describe-build"]), redirect_stdout(io.StringIO()) as stdout, redirect_stderr(io.StringIO()) as stderr:
            with self.assertRaises(SystemExit) as exited:
                common.runner_cli(run, failing_describe)
        self.assertEqual(exited.exception.code, 1)
        self.assertEqual(json.loads(stdout.getvalue()), {"build": "test"})
        self.assertEqual(json.loads(stderr.getvalue()), {"status": "invalid_input", "failure_reason": "cleanup failed"})
        with patch.dict(os.environ, AWS_ACCESS_KEY_ID="fake-key", AWS_SECRET_ACCESS_KEY="fake-secret", AWS_SESSION_TOKEN=""):
            self.assertEqual(common.redact_credentials("fake-key/fake-secret/fake-key"), "[redacted]/[redacted]/[redacted]")


if __name__ == "__main__":
    unittest.main()
