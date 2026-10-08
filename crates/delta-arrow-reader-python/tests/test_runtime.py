from http.server import BaseHTTPRequestHandler, HTTPServer
import os
import select
import signal
import subprocess
import sys
import threading
import unittest

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


class RuntimeTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "requires POSIX subprocess SIGINT")
    def test_loading_releases_gil_and_preserves_keyboard_interrupt(self):
        with subprocess.Popen(
            [sys.executable, "-I", __file__, "--interrupt-loading"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        ) as process:
            try:
                ready, _, _ = select.select([process.stdout], [], [], 15)
                self.assertTrue(ready, "loading did not reach the local server")
                self.assertEqual(process.stdout.readline(), "loading\n")
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
    else:
        unittest.main()
