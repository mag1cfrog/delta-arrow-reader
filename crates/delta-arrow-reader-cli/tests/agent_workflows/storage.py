"""Local S3 reads with selected, expired, renewed, and broader fixture identities.

Each consumer directory contains tables prepared by `xtask cli prepare-agent-fixtures`.
The evaluator creates authorization references with prepare_authorization_files(), then
serves their parent directory. Consumers see references, never the fixture controls.
The server verifies signatures with the existing independent S3 test verifier and
records identity labels rather than credential values.
"""

import argparse
import importlib.util
import json
import re
import secrets
import select
import signal
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape


if not __debug__:
    raise RuntimeError("The signature verifier requires Python assertions; run without -O")

verifier_path = Path(__file__).resolve().parents[1] / "direct_credentials/verify_request_signatures.py"
spec = importlib.util.spec_from_file_location("request_signatures", verifier_path)
signatures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signatures)

MODES = {"live", "denied", "expired", "stalled", "interrupted"}


def prepare_authorization_files(directory, origin):
    """Create synthetic credentials and return only their paths and table locations."""
    directory = Path(directory).resolve()
    if not re.fullmatch(r"[a-z0-9-]+", directory.name):
        raise ValueError("Consumer directory must be a lowercase S3 bucket suffix")
    endpoint = urlsplit(origin)
    if (endpoint.scheme != "http" or endpoint.hostname != "127.0.0.1" or not endpoint.port
            or endpoint.username or endpoint.password or endpoint.path not in ("", "/")
            or endpoint.query or endpoint.fragment):
        raise ValueError("The fixture endpoint must be a loopback HTTP origin")
    key, secret = "selected-" + secrets.token_hex(12), secrets.token_hex(24)
    identities = {
        name: {"key": key, "secret": secret, "token": secrets.token_hex(24)}
        for name in ("selected", "expired", "renewed")
    }
    identities["ambient"] = {
        "key": "ambient-" + secrets.token_hex(12),
        "secret": secrets.token_hex(24), "token": secrets.token_hex(24),
    }
    controls = directory / "harness"
    controls.mkdir(exist_ok=True)
    private = controls / "storage-identities.json"
    with private.open("x") as output:
        json.dump(identities, output)
    private.chmod(0o600)
    references = {"tables": {
        mode: f"s3://fixture-{directory.name}/{mode}/table" for mode in sorted(MODES)
    }}
    endpoint = {"aws_endpoint": origin, "aws_region": "us-east-1", "allow_http": "true"}
    for name in ("selected", "expired", "renewed"):
        identity = identities[name]
        path = directory / ("authorization.json" if name == "selected" else f"{name}.json")
        with path.open("x") as output:
            json.dump({**endpoint, "aws_access_key_id": identity["key"],
                       "aws_secret_access_key": identity["secret"],
                       "aws_session_token": identity["token"]}, output)
        path.chmod(0o600)
        references[name] = str(path)
    env_file = directory / "authorization.env"
    with env_file.open("x") as output:
        identity = identities["selected"]
        output.write(f"AWS_ACCESS_KEY_ID={identity['key']}\nAWS_SECRET_ACCESS_KEY={identity['secret']}\n"
                     f"AWS_SESSION_TOKEN={identity['token']}\n")
    env_file.chmod(0o600)
    endpoint_file = directory / "endpoint.json"
    with endpoint_file.open("x") as output:
        json.dump(endpoint, output)
    references.update(env_file=str(env_file), endpoint_file=str(endpoint_file))
    return references


class WorkflowStorageServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.record_lock = threading.Lock()
        self.interrupted_files = {}
        super().__init__(("127.0.0.1", 0), StorageRequestHandler)

    @property
    def origin(self):
        return f"http://127.0.0.1:{self.server_port}"

    def record_event(self, directory, **fields):
        with self.record_lock, (directory / "harness/storage-requests.jsonl").open("a") as output:
            output.write(json.dumps({"time_ns": time.time_ns(), **fields}) + "\n")


class StorageRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.respond()

    def do_HEAD(self):
        self.respond()

    def send_bytes(self, status, data, *, total=None, start=0, truncate=False):
        if hasattr(self, "case_directory"):
            code = re.search(rb"<Code>([^<]+)</Code>", data)
            self.server.record_event(self.case_directory, event="response", **self.request_log_fields,
                                     status=status, bytes=len(data),
                                     truncated=truncate, error_code=code[1].decode() if code else None)
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("ETag", '"workflow-fixture"')
        self.send_header("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{start + len(data) - 1}/{total}")
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(data[:len(data) // 2] if truncate else data)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
        if truncate:
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def respond(self):
        url = urlsplit(self.path)
        self.request_log_fields = {"request_id": secrets.token_hex(8),
                                "method": self.command, "path": url.path}
        parts = unquote(url.path).strip("/").split("/")
        name = parts[0].removeprefix("fixture-")
        if not parts[0].startswith("fixture-") or not re.fullmatch(r"[a-z0-9-]+", name):
            self.send_bytes(404, b"")
            return
        directory = self.server.directory / name
        credentials = directory / "harness/storage-identities.json"
        if not credentials.is_file():
            self.send_bytes(404, b"")
            return
        self.case_directory = directory
        identities = json.loads(credentials.read_text())
        headers = {key.lower(): " ".join(value.split()) for key, value in self.headers.items()}
        identity = next((name for name, value in identities.items()
                         if headers.get("x-amz-security-token") == value["token"]), None)
        try:
            if identity is None:
                raise ValueError("unknown identity")
            signatures.verify_s3_signature(self.command, self.path, headers,
                                           {value["key"]: value["secret"] for value in identities.values()})
            if f"Credential={identities[identity]['key']}/" not in headers["authorization"]:
                raise ValueError("mixed identity")
        except (AssertionError, KeyError, ValueError):
            identity = None
        query = parse_qs(url.query)
        prefix = query.get("prefix", [""])[0]
        mode = parts[1] if len(parts) > 1 else prefix.split("/", 1)[0]
        self.request_log_fields.update(mode=mode, identity=identity)
        self.server.record_event(directory, event="request", **self.request_log_fields)
        if identity is None:
            self.send_bytes(403, b"<Error><Code>SignatureDoesNotMatch</Code></Error>")
            return
        if identity == "expired" or (mode == "expired" and identity == "selected"):
            self.send_bytes(400, b"<Error><Code>ExpiredToken</Code></Error>")
            return
        if mode == "denied" and identity != "ambient":
            self.send_bytes(403, b"<Error><Code>AccessDenied</Code></Error>")
            return
        if mode not in MODES:
            self.send_bytes(404, b"")
            return
        if mode == "stalled":
            while True:
                readable, _, _ = select.select([self.connection], [], [], 0.1)
                try:
                    disconnected = readable and not self.connection.recv(1, socket.MSG_PEEK)
                except ConnectionResetError:
                    disconnected = True
                if disconnected:
                    self.server.record_event(directory, event="disconnected", **self.request_log_fields)
                    self.close_connection = True
                    return
        if "list-type" in query:
            keys = []
            for table in ("table", "lookup"):
                for file in sorted((directory / table / "_delta_log").glob("*.json")):
                    key = f"{mode}/{table}/_delta_log/{file.name}"
                    if key.startswith(prefix):
                        keys.append(f"<Contents><Key>{escape(key)}</Key><Size>{file.stat().st_size}</Size>"
                                    "<LastModified>2026-01-01T00:00:00Z</LastModified></Contents>")
            data = ("<ListBucketResult><IsTruncated>false</IsTruncated>" + "".join(keys)
                    + "</ListBucketResult>").encode()
            self.send_bytes(200, data)
            return
        relative = parts[2:]
        if not relative or relative[0] not in {"table", "lookup"} or any(p in {"", ".", ".."} or "\\" in p or "\0" in p for p in relative):
            self.send_bytes(404, b"")
            return
        path = directory.joinpath(*relative)
        if not path.resolve().is_relative_to(directory / relative[0]) or not path.is_file():
            self.send_bytes(404, b"")
            return
        data = path.read_bytes()
        total, start, status = len(data), 0, 200
        if span := self.headers.get("Range"):
            match = re.fullmatch(r"bytes=(\d*)-(\d*)", span)
            if not match or not any(match.groups()):
                self.send_bytes(416, b"")
                return
            first, last = match.groups()
            start = int(first) if first else max(0, total - int(last))
            end = min(int(last), total - 1) if first and last else total - 1
            if start > end:
                self.send_bytes(416, b"")
                return
            data, status = data[start:end + 1], 206
        truncate = False
        if mode == "interrupted" and path.suffix == ".parquet" and self.command == "GET":
            with self.server.record_lock:
                first, interrupted = self.server.interrupted_files.setdefault(directory, [path, False])
                if path != first:
                    self.server.interrupted_files[directory][1] = True
                    truncate = not interrupted
            if path != first and interrupted:
                self.send_bytes(400, b"<Error><Code>IncompleteBody</Code></Error>")
                return
        self.send_bytes(status, data, total=total, start=start, truncate=truncate)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Serve prepared consumer directories on loopback")
    serve.add_argument("directory", type=Path)
    prepare = commands.add_parser("prepare-authorization", help="Create synthetic authorization references")
    prepare.add_argument("directory", type=Path)
    prepare.add_argument("origin")
    args = parser.parse_args()
    if args.command == "prepare-authorization":
        print(json.dumps(prepare_authorization_files(args.directory, args.origin)))
        return
    with WorkflowStorageServer(args.directory) as server:
        print(json.dumps({"origin": server.origin}), flush=True)
        def stop(signum, frame):
            raise SystemExit(0)
        signal.signal(signal.SIGTERM, stop)
        server.serve_forever()


if __name__ == "__main__":
    main()
