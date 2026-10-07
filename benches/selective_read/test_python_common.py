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


class ReaderBookkeeping(unittest.TestCase):
    def test_build_checks_and_observation_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            executable = root / "selective-read-test"
            files = {executable: b"reader", root / "lock.json": b"{}", root / "shared.py": b"shared"}
            for path, content in files.items():
                path.write_bytes(content)
            build = {"reader_id": "test", "executable_sha256": common.digest(executable),
                     "lockfile_sha256": common.digest(root / "lock.json"),
                     "bundled_sha256": {"shared.py": common.digest(root / "shared.py")}}
            common.save(root / "build.json", build)
            self.assertEqual(common.checked_build(executable, "test"), (root / "build.json", build))
            with self.assertRaisesRegex(ValueError, "stale runner build"):
                common.checked_build(executable, "wrong-reader")
            for path, original in files.items():
                path.write_bytes(b"changed")
                with self.assertRaisesRegex(ValueError, "stale runner build"):
                    common.checked_build(executable, "test")
                path.write_bytes(original)
            for mode, durations, totals in (
                ("open", [0], {"open_query_ns": 0}),
                ("reuse", [3, 5], {"initialization_plus_query1_ns": 10, "initialization_plus_all_queries_ns": 15}),
                ("reuse", [2] * 10, {"initialization_plus_query1_ns": 9, "initialization_plus_all_queries_ns": 27}),
            ):
                for status, code in (("success", 0), ("validation_failed", 1)):
                    record = {"execution_mode": mode, "initialization_ns": 7, "status": status,
                              "queries": [{"completion_ns": value} for value in durations]}
                    expected = record | totals
                    common.timing_totals(record)
                    self.assertEqual(record, expected)
                    output = root / f"{mode}-{len(durations)}-{status}"
                    output.mkdir()
                    with redirect_stdout(io.StringIO()) as stdout:
                        self.assertEqual(common.write_observation(output, record), code)
                    self.assertEqual(json.loads(stdout.getvalue()), expected)
                    saved = output / "record.json"
                    self.assertEqual(json.loads(saved.read_text()), expected)
                    with self.assertRaises(FileExistsError):
                        common.write_observation(output, {"status": "success"})
                    self.assertEqual(json.loads(saved.read_text()), expected)

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
