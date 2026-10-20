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
    def test_build_audit_changes_preserve_certificates_but_artifact_changes_do_not(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            build_path, proof = root / "build.json", root / "proof.json"
            build = {"reader_id": "test", "executable_sha256": "1" * 64, "lockfile_sha256": "2" * 64,
                     "harness_git_commit": "before", "source_sha256": {"source": "before"}}
            build_path.write_text(json.dumps(build))
            request = {"purpose": "timing", "execution_mode": "open", "correctness_file": str(proof),
                       "comparison_revision": 2, "case_id": "case", "snapshot_version": 0,
                       "fixture_manifest_sha256": "3" * 64, "canonical_sql": "SELECT id FROM bench"}
            identity = common.reader_identity(request, "test", build_path, {})
            proof.write_text(json.dumps({"status": "passed", "checks": [identity | {"status": "passed", "output_rows": 3}]}))
            for change in ({"harness_git_commit": "after"}, {"source_sha256": {"source": "after"}}):
                build_path.write_text(json.dumps(build | change))
                current = common.reader_identity(request, "test", build_path, {})
                self.assertNotEqual(current["reader_build_sha256"], identity["reader_build_sha256"])
                self.assertEqual(common.correctness(request, current)["expected_output_rows"], [3])
            build_path.write_text(json.dumps(build | {"executable_sha256": "4" * 64}))
            with self.assertRaisesRegex(ValueError, "stale correctness"):
                common.correctness(request, common.reader_identity(request, "test", build_path, {}))

    def test_correctness_uses_result_identity_and_rows_not_oracle_source_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "correctness.json"
            identity = {"reader_id": "test", "reader_build_sha256": "build",
                        "reader_config_sha256": "config", "canonical_sql_sha256": "query",
                        "protocol_sha256": "current documentation"}
            check = identity | {"status": "passed", "output_rows": 3, "oracle_sha256": "older source",
                                "protocol_sha256": "older documentation"}
            request = {"purpose": "timing", "execution_mode": "open", "correctness_file": str(path)}
            path.write_text(json.dumps({"status": "passed", "checks": [check]}))
            self.assertEqual(common.correctness(request, identity)["expected_output_rows"], [3])
            for change in ({"status": "failed"}, {"output_rows": -1}, {"output_rows": True},
                           {"canonical_sql_sha256": "different query"}, {"reader_build_sha256": "different reader"}):
                with self.subTest(change=change), self.assertRaises(ValueError):
                    path.write_text(json.dumps({"status": "passed", "checks": [check | change]}))
                    common.correctness(request, identity)

    def test_adapter_probes_use_bundled_helpers_and_restore_controller_imports(self):
        previous_path = sys.path.copy()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "run.py").write_text("marker = 'bundled run'\n")
            (root / "python_common.py").write_text("marker = 'bundled common'\n")
            binary = root / "adapter"
            source = ("import sys\nfrom pathlib import Path\n"
                      "sys.path.insert(0, str(Path(__file__).parent))\n"
                      "from run import marker as runner\n"
                      "from python_common import marker as common\n")
            binary.write_text(source)
            adapter = run.load_adapter(binary)
            self.assertEqual((adapter["runner"], adapter["common"]), ("bundled run", "bundled common"))
            self.assertIs(sys.modules["run"], run)
            self.assertIs(sys.modules["python_common"], common)
            self.assertEqual(sys.path, previous_path)
            binary.write_text(source + "raise ValueError('probe failed')\n")
            with self.assertRaisesRegex(ValueError, "probe failed"):
                run.load_adapter(binary)
            self.assertIs(sys.modules["run"], run)
            self.assertIs(sys.modules["python_common"], common)
            self.assertEqual(sys.path, previous_path)

    def test_python_adapters_keep_the_comparison_identity_and_reader_roster(self):
        identity = {"comparison_revision": 6, "protocol_sha256": run.digest(run.SPARK_MATRIX),
            "base_protocol_sha256": run.digest(run.PROTOCOL), "sampling_sha256": run.digest(run.SAMPLING),
            "sampling_stage": "formal", "workload_manifest_sha256": "1" * 64}
        self.assertEqual(run.comparison_identity(identity), identity)
        for revision in range(2, 7):
            self.assertEqual(run.reader_roster({"comparison_revision": revision}),
                ("delta-arrow-reader", "delta-rs", "duckdb", "polars", "spark" if revision == 6 else "daft"))
        for change in ({"protocol_sha256": run.digest(run.PRODUCTION)},
                       {"base_protocol_sha256": "0" * 64}, {"sampling_sha256": "0" * 64}):
            self.assertEqual(run.comparison_contract(run.comparison_identity(identity | change)),
                             run.comparison_contract(identity))
        for change in ({"sampling_stage": None}, {"comparison_revision": 7},
                       {"workload_manifest_sha256": None}):
            with self.assertRaises(ValueError):
                run.comparison_identity(identity | change)
        for revision in (1, 7, True, "6"):
            with self.assertRaises(ValueError):
                run.reader_roster({"comparison_revision": revision})

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


    def test_concurrent_request_counts(self):
        payload = {"execution_mode": "reuse", "comparison_revision": 6,
                   "purpose": "timing", "concurrent_queries": 4}
        for count in (0, 1, 3, True, "4"):
            with self.subTest(count=count), self.assertRaises(ValueError):
                run.query_count(payload | {"concurrent_queries": count})
        with self.assertRaises(ValueError):
            run.query_count(payload | {"execution_mode": "open"})
        with self.assertRaises(ValueError):
            run.query_count(payload | {"comparison_revision": 5})


if __name__ == "__main__":
    unittest.main()
