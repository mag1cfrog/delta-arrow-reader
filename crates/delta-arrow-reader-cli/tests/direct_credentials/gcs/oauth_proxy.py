"""Local OAuth fixture. CONNECT requests terminate here and never leave the host."""

import http.server
import json
from pathlib import Path
import ssl
import subprocess
import sys
import threading
from urllib.parse import parse_qs


class TokenEndpoint(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        assert self.path == "/o/oauth2/token"
        form = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
        with self.server.lock:
            self.server.calls += 1
            call = self.server.calls
            with (self.server.directory / "requests.jsonl").open("a") as output:
                output.write(json.dumps(form) + "\n")
        rejected = self.server.mode == "reject" or (
            self.server.mode == "reject_refresh" and call > 1
        )
        body = json.dumps(
            {"error": "invalid_grant", "error_description": "secret-refresh-error"}
            if rejected else {
                "access_token": f"secret-oauth-{call}",
                "expires_in": 0 if "refresh" in self.server.mode else 3600,
            }
        ).encode()
        self.send_response(400 if rejected else 200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class OAuthProxy(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_CONNECT(self):
        if self.path != "accounts.google.com:443":
            self.send_error(403)
            return
        self.send_response(200)
        self.end_headers()
        with self.server.tls.wrap_socket(self.connection, server_side=True) as connection:
            TokenEndpoint(connection, self.client_address, self.server)


def main():
    directory = Path(sys.argv[1])
    key = Path(__file__).with_name("test-key.pem")
    certificate = directory / "certificate.pem"
    subprocess.run(
        ["openssl", "req", "-new", "-x509", "-key", str(key), "-days", "2",
         "-subj", "/CN=accounts.google.com", "-out", str(certificate)],
        check=True, capture_output=True, timeout=10,
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), OAuthProxy)
    server.directory = directory
    server.mode = sys.argv[2]
    server.calls = 0
    server.lock = threading.Lock()
    server.tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.tls.load_cert_chain(certificate, key)
    (directory / "requests.jsonl").touch()
    (directory / "endpoint").write_text(f"http://127.0.0.1:{server.server_port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
