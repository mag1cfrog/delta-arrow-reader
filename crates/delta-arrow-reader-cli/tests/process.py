"""Linux process checks invoked by cargo xtask cli test-process."""

import functools
import importlib.util
from itertools import product
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from urllib.parse import parse_qs, quote, urlsplit

DAR = Path(sys.argv.pop(1)).resolve()
ROOT = Path(__file__).resolve().parents[3]
CORPUS = ROOT / "tests/reader/fixtures/external_writer/corpus"

spec = importlib.util.spec_from_file_location("reader_https", ROOT / "tests/reader/https.py")
http = importlib.util.module_from_spec(spec)
spec.loader.exec_module(http)


class RecordingStorageHandler(http.Storage):
    def do_PROPFIND(self):
        self.server.requests.append((self.path, None, self.client_address[1]))
        super().do_PROPFIND()


class ProcessTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="secret-dar-")
        self.addCleanup(temporary.cleanup)
        self.cwd = Path(temporary.name)
        self.env = dict(os.environ, NO_PROXY="*", no_proxy="*", RUST_LOG="trace")

    def invoke(self, *args, **kwargs):
        return subprocess.run(
            [DAR, *args], cwd=self.cwd, env=self.env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, **kwargs,
        )

    def assert_error(self, result, status, phase, code):
        self.assertEqual(result.returncode, status, result)
        self.assertEqual(result.stdout, b"")
        self.assertTrue(result.stderr.endswith(b"\n"))
        self.assertEqual(result.stderr.count(b"\n"), 1)
        diagnostic = json.loads(result.stderr)
        self.assertEqual(set(diagnostic), {"phase", "code", "message"})
        self.assertTrue(all(isinstance(value, str) for value in diagnostic.values()))
        self.assertEqual((diagnostic["phase"], diagnostic["code"]), (phase, code))
        self.assertNotIn(b"secret", result.stderr)
        return diagnostic

    def assert_inspection(self, result):
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, b"")
        self.assertTrue(result.stdout.endswith(b"\n"))
        self.assertEqual(result.stdout.count(b"\n"), 1)
        value = json.loads(result.stdout)
        self.assertEqual(set(value), {"format_version", "table_version", "schema"})
        self.assertEqual(value["format_version"], 1)
        self.assertIsInstance(value["table_version"], str)
        return value

    def metadata_table(self, fields=(), protocol=1):
        table = self.cwd / "-secret table, with spaces"
        log = table / "_delta_log"
        log.mkdir(parents=True, exist_ok=True)
        actions = [
            {"protocol": {"minReaderVersion": protocol, "minWriterVersion": 2}},
            {"metaData": {
                "id": "test", "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": fields}),
                "partitionColumns": [], "configuration": {},
            }},
        ]
        (log / "00000000000000000000.json").write_text(
            "".join(json.dumps(action) + "\n" for action in actions), encoding="utf-8"
        )
        return table

    def options(self, content=b"{}"):
        path = self.cwd / "secret options, file.json"
        path.write_bytes(content)
        return path

    def test_explicit_version_stops_before_auxiliary_listing_pages(self):
        (self.cwd / "bucket").mkdir()
        for table_name in ["table", "secret%name"]:
            table = self.metadata_table().rename(self.cwd / "bucket" / table_name)
            commit = "00000000000000000000.json"
            commit_size = (table / "_delta_log" / commit).stat().st_size
            for last_entry in ["_last_checkpoint", "_sidecars/part.parquet", "_staged_commits/commit.json"]:
                with self.subTest(table=table_name, last_entry=last_entry):
                    class PaginatedStorageHandler(RecordingStorageHandler):
                        def do_GET(self):
                            query = parse_qs(urlsplit(self.path).query)
                            if query.get("list-type") != ["2"]:
                                return super().do_GET()
                            self.server.requests.append((self.path, None, self.client_address[1]))
                            if "continuation-token" in query:
                                self.send_error(403, "unnecessary listing page")
                                return
                            # The nested entry sorts before the valid commit and must
                            # be skipped without ending the listing at its leaf name.
                            entries = [("00000000000000000000.aaa/zzz", 1),
                                       (commit, commit_size), (last_entry, 1)]
                            contents = "".join(
                                f"<Contents><Key>{table_name}/_delta_log/{name}</Key>"
                                "<LastModified>2026-01-01T00:00:00Z</LastModified>"
                                f"<Size>{size}</Size></Contents>" for name, size in entries
                            )
                            data = (
                                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                                "<IsTruncated>true</IsTruncated>"
                                "<NextContinuationToken>auxiliary-page</NextContinuationToken>"
                                + contents + "</ListBucketResult>"
                            ).encode()
                            self.send_response(200)
                            self.send_header("Content-Type", "application/xml")
                            self.send_header("Content-Length", str(len(data)))
                            self.end_headers()
                            self.wfile.write(data)

                    with http.serve(functools.partial(PaginatedStorageHandler, directory=self.cwd)) as server:
                        options = self.options(json.dumps({
                            "aws_endpoint_url_s3": f"http://127.0.0.1:{server.server_port}",
                            "aws_region": "us-east-1", "aws_skip_signature": "true", "allow_http": "true",
                        }).encode())
                        for version in ["0", "999"]:
                            server.requests.clear()
                            result = self.invoke("inspect", "--table-version", version,
                                                 "--storage-options-file", options,
                                                 f"s3://bucket/{quote(table_name)}")
                            if version == "0":
                                self.assertEqual(self.assert_inspection(result)["table_version"], "0")
                            else:
                                self.assert_error(result, 1, "snapshot", "snapshot_load")
                            listings = [path for path, *_ in server.requests
                                        if "list-type" in parse_qs(urlsplit(path).query)]
                            self.assertEqual(len(listings), 1, server.requests)

    def test_http_inspection_reads_only_metadata(self):
        with http.serve(functools.partial(RecordingStorageHandler, directory=CORPUS / "deletion_vectors/table")) as server:
            value = self.assert_inspection(self.invoke("inspect", "--storage-options-file", self.options(b'{"allow_http":"true"}'),
                                             f"http://127.0.0.1:{server.server_port}/"))
            self.assertEqual(value["table_version"], "1")
            self.assertTrue(server.requests)
            self.assertTrue(all(path == "/_delta_log" or path.startswith("/_delta_log/")
                                for path, *_ in server.requests), server.requests)

    def test_signals_stop_blocked_loading_without_child_processes(self):
        for command, signum in product(["inspect", "scan"], [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]):
            with self.subTest(command=command, signal=signum):
                started, disconnected = threading.Event(), threading.Event()

                class BlockingStorageHandler(RecordingStorageHandler):
                    def send_head(self):
                        started.set()
                        self.wait_for_disconnect(disconnected)

                    def do_PROPFIND(self):
                        self.send_head()

                with http.serve(functools.partial(BlockingStorageHandler, directory=self.cwd)) as server:
                    args = [DAR, command, "--storage-options-file", self.options(b'{"allow_http":"true"}'),
                            f"http://127.0.0.1:{server.server_port}/"]
                    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env) as process:
                        try:
                            self.assertTrue(started.wait(10), "table request never started")
                            for children in Path(f"/proc/{process.pid}/task").glob("*/children"):
                                self.assertEqual(children.read_text().strip(), "")
                            process.send_signal(signum)
                            stdout, stderr = process.communicate(timeout=10)
                            self.assertEqual(process.returncode, -signum)
                            self.assertEqual((stdout, stderr), (b"", b""))
                            self.assertTrue(disconnected.wait(5), "terminated process retained a socket")
                        finally:
                            if process.poll() is None:
                                process.kill()
                                process.communicate(timeout=10)


if __name__ == "__main__":
    unittest.main()
