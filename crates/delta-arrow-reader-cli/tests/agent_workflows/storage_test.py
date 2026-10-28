"""Exercise the workflow storage fixture through the real CLI, including failed reads."""

import importlib.util
import json
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("workflow_storage", Path(__file__).with_name("storage.py"))
storage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(storage)
WorkflowStorageServer = storage.WorkflowStorageServer
prepare_authorization_files = storage.prepare_authorization_files


class WorkflowStorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name) / "case"
        self.directory.mkdir()
        fixture = (Path(__file__).resolve().parents[3] / "delta-arrow-reader-core/tests/reader/fixtures"
                   "/external_writer/corpus/deletion_vectors/table")
        shutil.copytree(fixture, self.directory / "table")
        self.server = WorkflowStorageServer(self.directory.parent)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.references = prepare_authorization_files(self.directory, self.server.origin)
        self.identities = json.loads((self.directory / "harness/storage-identities.json").read_text())
        (self.directory / "query.sql").write_text("SELECT id FROM t ORDER BY id")

    def command(self, mode="live", identity="selected", deadline="3000"):
        return [str(BINARY), "--diagnostics=json", "query", self.references["tables"][mode],
                "--table-version", "1", "--auth-source", "json", "--storage-options-file",
                self.references[identity], "--sql-file", "query.sql", "--format", "json",
                "--output", "result.json", "--deadline-ms", deadline]

    def invoke(self, mode="live", identity="selected"):
        ambient = self.identities["ambient"]
        output = subprocess.run(self.command(mode, identity), cwd=self.directory, capture_output=True,
                                timeout=8, env={"AWS_ACCESS_KEY_ID": ambient["key"],
                                               "AWS_SECRET_ACCESS_KEY": ambient["secret"],
                                               "AWS_SESSION_TOKEN": ambient["token"]})
        self.check_no_credential_output(output.stdout + output.stderr)
        return output

    def check_no_credential_output(self, output):
        for identity in self.identities.values():
            for value in identity.values():
                self.assertFalse(value.encode() in output, "credential material exposed")

    def storage_events(self):
        path = self.directory / "harness/storage-requests.jsonl"
        events = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        requests = {event["request_id"]: event for event in events if event["event"] == "request"}
        self.assertEqual(len(requests), sum(event["event"] == "request" for event in events))
        for event in events:
            request = requests[event["request_id"]]
            self.assertLessEqual(request["time_ns"], event["time_ns"])
            for field in ("method", "path", "mode", "identity"):
                self.assertEqual(request[field], event[field])
        return events

    def assert_failed_without_artifact(self, output):
        self.assertNotEqual(output.returncode, 0)
        self.assertEqual(output.stdout, b"")
        self.assertEqual(json.loads(output.stderr)["kind"], "diagnostic")
        self.assertFalse((self.directory / "result.json").exists())
        self.assertFalse((self.directory / "result.json.manifest.json").exists())
        self.assertFalse(any(path.name.startswith((".dar-result-", ".dar-manifest-"))
                             for path in self.directory.iterdir()))
        self.assertFalse(any(event.get("identity") == "ambient" for event in self.storage_events()))

    def assert_storage_error(self, status, code):
        responses = [event for event in self.storage_events() if event["event"] == "response"]
        self.assertTrue(responses)
        self.assertEqual({(event["status"], event["error_code"]) for event in responses}, {(status, code)})

    def assert_pinned_authorization_failure(self, output, status, code):
        self.assert_failed_without_artifact(output)
        self.assert_storage_error(status, code)
        diagnostic = json.loads(output.stderr)
        self.assertEqual(diagnostic["code"], "snapshot_load")
        # The pinned snapshot loader currently hides the backend authorization cause.
        self.assertEqual(diagnostic["source_kind"], "unknown")
        self.assertEqual(diagnostic["recovery"], "unknown")

    def test_selected_identity_reads_pinned_deletion_vectors(self):
        output = self.invoke()
        self.assertEqual(output.returncode, 0, output.stderr.decode())
        result = json.loads((self.directory / "result.json").read_text())
        self.assertEqual([int(row[0]) for row in result["rows"]], [1, 3, 4, 6, 7, 8, 9, 10, 11])
        self.assertEqual(result["completion"], "complete")
        receipt = json.loads(output.stdout)
        self.assertEqual(receipt, {"format_version": 2, "kind": "result_artifact",
                                   "result": str((self.directory / "result.json").resolve())})
        self.assertFalse((self.directory / "result.json.manifest.json").exists())
        self.assertEqual({r["identity"] for r in self.storage_events() if r["event"] == "request"}, {"selected"})

    def test_env_file_selects_the_same_identity(self):
        command = self.command()
        start = command.index("--auth-source")
        command[start:start + 4] = ["--env-file", self.references["env_file"],
                                   "--storage-options-file", self.references["endpoint_file"]]
        output = subprocess.run(command, cwd=self.directory, capture_output=True, timeout=8, env={})
        self.assertEqual(output.returncode, 0, output.stderr.decode())
        self.check_no_credential_output(output.stdout + output.stderr)
        self.assertEqual({r["identity"] for r in self.storage_events() if r["event"] == "request"}, {"selected"})

    def test_unpinned_denial_exposes_request_access_recovery(self):
        command = [str(BINARY), "--diagnostics=json", "inspect", self.references["tables"]["denied"],
                   "--auth-source", "json", "--storage-options-file", self.references["selected"]]
        output = subprocess.run(command, cwd=self.directory, capture_output=True, timeout=8, env={})
        self.assert_failed_without_artifact(output)
        self.assert_storage_error(403, "AccessDenied")
        diagnostic = json.loads(output.stderr)
        self.assertEqual(diagnostic["source_kind"], "permission_denied")
        self.assertEqual(diagnostic["recovery"], "request_access")

    def test_expired_reference_requires_explicit_renewal(self):
        self.assert_pinned_authorization_failure(self.invoke(identity="expired"), 400, "ExpiredToken")
        output = self.invoke(identity="renewed")
        self.assertEqual(output.returncode, 0, output.stderr.decode())
        self.assertEqual({r["identity"] for r in self.storage_events() if r["event"] == "request"}, {"expired", "renewed"})

    def test_denied_reference_does_not_fall_back_to_ambient_identity(self):
        self.assert_pinned_authorization_failure(self.invoke(mode="denied"), 403, "AccessDenied")

    def test_broader_identity_is_a_working_negative_control(self):
        configuration = json.loads(Path(self.references["selected"]).read_text())
        ambient = self.identities["ambient"]
        configuration.update(aws_access_key_id=ambient["key"], aws_secret_access_key=ambient["secret"],
                             aws_session_token=ambient["token"])
        path = self.directory / "broader.json"
        path.write_text(json.dumps(configuration))
        self.references["ambient"] = str(path)
        output = self.invoke(mode="denied", identity="ambient")
        self.assertEqual(output.returncode, 0, output.stderr.decode())
        self.assertEqual({r["identity"] for r in self.storage_events() if r["event"] == "request"}, {"ambient"})

    def test_bad_signature_is_rejected(self):
        path = Path(self.references["selected"])
        configuration = json.loads(path.read_text())
        configuration["aws_secret_access_key"] = "invalid-fixture-secret"
        path.write_text(json.dumps(configuration))
        self.assert_failed_without_artifact(self.invoke())
        self.assertTrue(self.storage_events())
        self.assertTrue(all(r.get("identity") is None for r in self.storage_events()))

    def test_authorization_preparation_preserves_existing_files_and_rejects_remote_origins(self):
        private = self.directory / "harness/storage-identities.json"
        original = private.read_bytes()
        for origin in ("https://example.com", "http://127.0.0.1:1234/path", "http://user@127.0.0.1:1234"):
            with self.assertRaises(ValueError):
                prepare_authorization_files(self.directory, origin)
        with self.assertRaises(FileExistsError):
            prepare_authorization_files(self.directory, self.server.origin)
        self.assertTrue(private.read_bytes() == original, "existing credentials changed")

    def test_interrupted_body_cannot_publish_a_complete_result(self):
        log = self.directory / "table/_delta_log"
        added = next(json.loads(line)["add"] for line in (log / "00000000000000000001.json").read_text().splitlines()
                     if "add" in json.loads(line))
        shutil.copyfile(self.directory / "table" / added["path"], self.directory / "table/second.parquet")
        added["path"] = "second.parquet"
        (log / "00000000000000000002.json").write_text(json.dumps({"add": added}) + "\n")
        (self.directory / "execution.json").write_text(json.dumps({
            "max_concurrent_file_reads_per_scan": 1, "prefetch_files_per_partition": 0}))
        command = [str(BINARY), "--diagnostics=json", "scan", self.references["tables"]["live"],
                   "--table-version", "1", "--auth-source", "json", "--storage-options-file",
                   self.references["selected"], "--column", "id", "--target-partitions", "1",
                   "--execution-options-file", "execution.json", "--deadline-ms", "3000"]
        complete = subprocess.run(command, cwd=self.directory, capture_output=True, timeout=8, env={})
        self.assertEqual(complete.returncode, 0, complete.stderr.decode())
        completion = json.loads(complete.stderr)
        self.assertEqual(completion["rows"], "9")
        self.assertEqual(completion["batches"], "1")
        self.assertEqual(completion["completion"], "complete")
        self.assertEqual(completion["stop_reason"], "end_of_input")
        self.assertIs(completion["has_more"], False)
        self.assertEqual(completion["bindings"], [{"alias": "t", "table_version": "1"}])
        command[3] = self.references["tables"]["interrupted"]
        command[5] = "2"
        partial = subprocess.run(command, cwd=self.directory, capture_output=True, timeout=8, env={})
        self.assertNotEqual(partial.returncode, 0)
        # The first complete batch is present, but the Arrow stream has no end marker.
        self.assertEqual(complete.stdout[-8:], b"\xff\xff\xff\xff\x00\x00\x00\x00")
        self.assertEqual(partial.stdout, complete.stdout[:-8])
        self.assertEqual(json.loads(partial.stderr)["code"], "data_file_read")
        self.check_no_credential_output(partial.stdout + partial.stderr)
        output = subprocess.run(command + ["--output", "result.json"], cwd=self.directory,
                                capture_output=True, timeout=8, env={})
        self.assert_failed_without_artifact(output)
        self.assertEqual(json.loads(output.stderr)["code"], "data_file_read")
        events = self.storage_events()
        self.assertTrue(any(r.get("truncated") for r in events))
        self.assertTrue(any(r.get("error_code") == "IncompleteBody" for r in events))

    def test_stalled_storage_obeys_the_deadline(self):
        output = self.invoke(mode="stalled")
        self.assert_failed_without_artifact(output)
        self.assertEqual(json.loads(output.stderr)["code"], "deadline_exceeded")

    def test_cancellation_disconnects_stalled_storage(self):
        process = subprocess.Popen(self.command(mode="stalled", deadline="10000"),
                                   cwd=self.directory, env={}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            until = time.monotonic() + 5
            while not self.storage_events() and process.poll() is None and time.monotonic() < until:
                time.sleep(0.01)
            self.assertTrue(self.storage_events(), "cancellation must follow an actual storage request")
            cleanup_deadline = time.monotonic() + 1
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=1)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
        self.check_no_credential_output(stdout + stderr)
        self.assert_failed_without_artifact(subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr))
        self.assertEqual(json.loads(stderr)["code"], "command_cancelled")
        while not any(r["event"] == "disconnected" for r in self.storage_events()) and time.monotonic() < cleanup_deadline:
            time.sleep(0.01)
        self.assertTrue(any(r["event"] == "disconnected" for r in self.storage_events()))


if __name__ == "__main__":
    BINARY = Path(sys.argv.pop(1)).resolve()
    unittest.main()
