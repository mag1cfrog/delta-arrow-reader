from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
import importlib.util
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import threading
import unittest

import pyarrow as pa

from delta_arrow_reader import DeltaTable


def interrupt_loading():
    release = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            # This Python thread can run only if loading releases the GIL.
            print("loading", flush=True)
            release.wait(30)
            self.send_response(403)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            DeltaTable("s3://test-bucket/table", storage_options={
                "aws_endpoint_url_s3": f"http://127.0.0.1:{server.server_port}",
                "aws_region": "us-east-1",
                "aws_skip_signature": "true",
                "aws_allow_http": "true",
            })
        except KeyboardInterrupt:
            print("interrupted", flush=True)
        else:
            raise AssertionError("loading completed without KeyboardInterrupt")
        finally:
            release.set()
            server.shutdown()
            worker.join()


def interrupt_reading():
    root = Path(__file__).resolve().parents[3]
    spec = importlib.util.spec_from_file_location("reader_https", root / "tests/reader/https.py")
    http = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(http)
    directory = root / "tests/reader/fixtures/external_writer/corpus/nested_mapping/table"
    disconnected = threading.Event()
    requests = []

    class Storage(http.Storage):
        def do_GET(self):
            if not self.path.endswith(".parquet"):
                return super().do_GET()
            requests.append(self.path)
            # This Python thread can run only if the batch read releases the GIL.
            print("reading", flush=True)
            self.connection.settimeout(10)
            try:
                if self.rfile.read(1) == b"":
                    disconnected.set()
            except ConnectionError:
                disconnected.set()
            except TimeoutError:
                self.send_error(400, "test read was not cancelled")

    with http.serve(partial(Storage, directory=str(directory))) as server:
        table = DeltaTable(
            f"http://127.0.0.1:{server.server_port}/",
            storage_options={"allow_http": "true"},
        )
        with table.to_reader() as reader:
            for _ in range(2):
                try:
                    reader.read_next_batch()
                except pa.ArrowInvalid as error:
                    assert (
                        "phase=execution code=python_signal reason=reader_wait_interrupted"
                        in str(error)
                    ), str(error)
                else:
                    raise AssertionError("batch read completed without a signal error")
            assert disconnected.wait(10), "interruption left the HTTP read pending"
            assert len(requests) == 1, requests
        print("interrupted", flush=True)


class RuntimeTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "requires POSIX subprocess SIGINT")
    def test_loading_releases_gil_and_preserves_keyboard_interrupt(self):
        self.assert_interrupted("loading")

    @unittest.skipUnless(os.name == "posix", "requires POSIX subprocess SIGINT")
    def test_batch_read_releases_gil_and_cancels_on_sigint(self):
        self.assert_interrupted("reading")

    def assert_interrupted(self, phase):
        with subprocess.Popen(
            [sys.executable, "-I", __file__, f"--interrupt-{phase}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        ) as process:
            try:
                ready, _, _ = select.select([process.stdout], [], [], 15)
                self.assertTrue(ready, f"{phase} did not reach the local server")
                self.assertEqual(process.stdout.readline(), f"{phase}\n")
                process.send_signal(signal.SIGINT)
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual(process.returncode, 0, stderr)
                self.assertEqual(stdout, "interrupted\n")
                self.assertEqual(stderr, "")
            finally:
                if process.poll() is None:
                    process.kill()


if __name__ == "__main__":
    if sys.argv[1:] == ["--interrupt-loading"]:
        interrupt_loading()
    elif sys.argv[1:] == ["--interrupt-reading"]:
        interrupt_reading()
    else:
        unittest.main()
