"""HTTP fixtures run in Python threads so binding tests can detect a held GIL."""

import contextlib
import http.server
import io
from pathlib import Path
import threading
from urllib.parse import quote
from xml.sax.saxutils import escape


class Storage(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def wait_for_disconnect(self, disconnected):
        """Signal a client disconnect; fail the held request if it times out."""
        self.connection.settimeout(10)
        try:
            if self.rfile.read(1) == b"":
                disconnected.set()
        except ConnectionError:
            disconnected.set()
        except TimeoutError:
            self.send_error(400, "test read was not cancelled")

    def send_head(self):
        path = Path(self.translate_path(self.path))
        if not path.is_file():
            self.send_error(404)
            return None
        data = path.read_bytes()
        size = len(data)
        requested = self.headers.get("Range")
        self.server.requests.append((self.path, requested, self.client_address[1]))
        if requested:
            first, last = requested.removeprefix("bytes=").split("-")
            start = int(first) if first else max(0, size - int(last))
            end = min(int(last) + 1, size) if first and last else size
            if start >= end:
                self.send_error(416)
                return None
            data = data[start:end]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end - 1}/{size}")
        else:
            self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Last-Modified", self.date_time_string(path.stat().st_mtime))
        self.end_headers()
        return io.BytesIO(data)

    def do_PROPFIND(self):
        # Only listing is needed in addition to GET/HEAD for Delta snapshot reads.
        root = Path(self.directory)
        directory = Path(self.translate_path(self.path))
        responses = []
        for path in sorted(directory.rglob("*")):
            if path.is_file():
                href = escape("/" + quote(path.relative_to(root).as_posix()))
                responses.append(
                    f"<response><href>{href}</href><propstat><prop>"
                    f"<getcontentlength>{path.stat().st_size}</getcontentlength>"
                    f"<getlastmodified>{self.date_time_string(path.stat().st_mtime)}</getlastmodified>"
                    "<resourcetype/></prop><status>HTTP/1.1 200 OK</status>"
                    "</propstat></response>"
                )
        data = (
            '<multistatus xmlns="DAV:">' + "".join(responses) + "</multistatus>"
        ).encode()
        self.send_response(207)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


@contextlib.contextmanager
def serve(handler):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
