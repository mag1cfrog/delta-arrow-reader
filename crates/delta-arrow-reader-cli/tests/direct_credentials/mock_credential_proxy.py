"""Mock credential services. CONNECT requests terminate here and never leave the host."""

import http.server
import json
from pathlib import Path
import ssl
import subprocess
import sys
import threading
from urllib.parse import parse_qs, urlsplit


class CredentialEndpointHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        assert self.server.service == "container"
        url = urlsplit(self.path)
        request = {"host": self.headers["Host"], "path": url.path,
                   "parameters": parse_qs(url.query),
                   "authorization": self.headers.get("Authorization")}
        with self.server.lock:
            self.server.calls += 1
            call = self.server.calls
            with (self.server.directory / "requests.jsonl").open("a") as output:
                output.write(json.dumps(request) + "\n")
        rejected = self.server.mode == "reject" or (
            self.server.mode == "reject_refresh" and call > 1
        )
        body = json.dumps(
            {"error": "secret-container-error"} if rejected else {
                "AccessKeyId": f"secret-container-key-{call}",
                "SecretAccessKey": f"secret-container-password-{call}",
                "Token": f"secret-container-session-{call}",
                "Expiration": "2000-01-01T00:00:00Z",
            }
        ).encode()
        if self.server.mode == "malformed":
            body = b"secret-invalid-container-response"
        self.send_response(400 if rejected else 200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.server.service == "gcs":
            assert self.path == "/o/oauth2/token"
            request = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
        else:
            url = urlsplit(self.path)
            request = {"host": self.headers["Host"], "path": url.path,
                       "parameters": parse_qs(url.query)}
        with self.server.lock:
            self.server.calls += 1
            call = self.server.calls
            with (self.server.directory / "requests.jsonl").open("a") as output:
                output.write(json.dumps(request) + "\n")
        rejected = self.server.mode == "reject" or (
            self.server.mode == "reject_refresh" and call > 1
        )
        if self.server.service == "gcs":
            content_type = "application/json"
            body = json.dumps(
                {"error": "invalid_grant", "error_description": "secret-refresh-error"}
                if rejected else {
                    "access_token": f"secret-oauth-{call}",
                    "expires_in": 0 if "refresh" in self.server.mode else 3600,
                }
            ).encode()
        else:
            content_type = "application/xml"
            # An expired timestamp forces the native cache to refresh without sleeps.
            body = (
                "<ErrorResponse><Error><Code>AccessDenied</Code>"
                "<Message>secret-sts-error</Message></Error></ErrorResponse>"
                if rejected else
                "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult>"
                f"<Credentials><AccessKeyId>secret-sts-key-{call}</AccessKeyId>"
                f"<SecretAccessKey>secret-sts-password-{call}</SecretAccessKey>"
                f"<SessionToken>secret-sts-session-{call}</SessionToken>"
                "<Expiration>2000-01-01T00:00:00Z</Expiration></Credentials>"
                "</AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
            ).encode()
        self.send_response(400 if rejected else 200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class CredentialProxyHandler(CredentialEndpointHandler):
    def do_CONNECT(self):
        hosts = {
            "gcs": {"accounts.google.com:443"},
            "sts": {"sts.eu-west-1.amazonaws.com:443", "sts.secret-override.invalid:443"},
            "container": {"credentials.secret.invalid:443"},
        }[self.server.service]
        if self.path not in hosts:
            self.send_error(403)
            return
        self.send_response(200)
        self.end_headers()
        with self.server.tls.wrap_socket(self.connection, server_side=True) as connection:
            CredentialEndpointHandler(connection, self.client_address, self.server)


def main():
    directory = Path(sys.argv[1])
    key = Path(__file__).with_name("test-key.pem")
    certificate = directory / "certificate.pem"
    subprocess.run(
        ["openssl", "req", "-new", "-x509", "-key", str(key), "-days", "2",
         "-subj", "/CN=accounts.google.com", "-out", str(certificate)],
        check=True, capture_output=True, timeout=10,
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CredentialProxyHandler)
    server.directory = directory
    server.service = sys.argv[2]
    assert server.service in {"gcs", "sts", "container"}
    server.mode = sys.argv[3]
    server.calls = 0
    server.lock = threading.Lock()
    server.tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.tls.load_cert_chain(certificate, key)
    (directory / "requests.jsonl").touch()
    (directory / "endpoint").write_text(f"http://127.0.0.1:{server.server_port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
