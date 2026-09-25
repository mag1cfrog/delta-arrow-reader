#!/usr/bin/env python3
"""Linux HTTPS regression: python3 tests/reader/https.py [--features datafusion].

Uses Python's stdlib and the openssl CLI to create temporary test certificates.
Trust and proxy overrides affect only the test subprocess, never the system store.
"""

import contextlib
import functools
import http.server
import io
import json
import os
from pathlib import Path
import select
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
from urllib.parse import quote
from xml.sax.saxutils import escape


class Storage(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *_):
        pass

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


class Proxy(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_CONNECT(self):
        host, port = self.path.rsplit(":", 1)
        if host not in ("localhost", "127.0.0.1") or int(port) not in self.server.ports:
            self.send_error(403)
            return
        self.server.requests.append(self.path)
        with socket.create_connection(("127.0.0.1", int(port)), timeout=10) as upstream:
            self.server.local_ports.add(upstream.getsockname()[1])
            self.send_response(200)
            self.end_headers()
            while readable := select.select([self.connection, upstream], [], [], 10)[0]:
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        return
                    target = upstream if source is self.connection else self.connection
                    target.sendall(data)


def openssl(directory, *args):
    subprocess.run(
        ["openssl", *args], cwd=directory, check=True, capture_output=True, timeout=30
    )


def certificates(directory):
    for name in ("trusted", "untrusted"):
        openssl(
            directory,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            f"/CN=Delta reader {name} test CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-keyout",
            f"{name}-ca.key",
            "-out",
            f"{name}-ca.pem",
        )
        openssl(
            directory,
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-keyout",
            f"{name}.key",
            "-out",
            f"{name}.csr",
        )
        openssl(
            directory,
            "x509",
            "-req",
            "-in",
            f"{name}.csr",
            "-CA",
            f"{name}-ca.pem",
            "-CAkey",
            f"{name}-ca.key",
            "-CAcreateserial",
            "-days",
            "2",
            "-extfile",
            "extensions",
            "-out",
            f"{name}.pem",
        )


@contextlib.contextmanager
def serve(handler, tls=None):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.requests = []
    if tls:
        server.socket = tls.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def main():
    if not sys.platform.startswith("linux"):
        raise SystemExit("This harness uses Linux SSL_CERT_FILE trust overrides.")
    os.chdir(Path(__file__).resolve().parents[2])
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--locked", "--format-version", "1", *sys.argv[1:]],
            text=True,
        )
    )
    resolved = {node["id"] for node in metadata["resolve"]["nodes"]}
    forbidden = {"native-tls", "openssl", "openssl-sys"}
    assert not any(
        package["id"] in resolved and package["name"] in forbidden
        for package in metadata["packages"]
    ), "native TLS re-entered the resolved dependency graph"
    with tempfile.TemporaryDirectory(prefix="delta-reader-https-") as temporary:
        root = Path(temporary)
        (root / "extensions").write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n"
        )
        certificates(root)
        data = root / "data"
        data.mkdir()
        (root / "empty-certs").mkdir()
        handler = functools.partial(Storage, directory=str(data))
        with contextlib.ExitStack() as stack:
            servers = []
            for name in ("trusted", "untrusted"):
                tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                tls.load_cert_chain(root / f"{name}.pem", root / f"{name}.key")
                servers.append(stack.enter_context(serve(handler, tls)))
            trusted, untrusted = servers
            proxy = stack.enter_context(serve(Proxy))
            proxy.ports = [server.server_port for server in servers]
            proxy.local_ports = set()
            env = {
                key: value
                for key, value in os.environ.items()
                if key.lower()
                not in ("http_proxy", "https_proxy", "all_proxy", "no_proxy")
                and not key.startswith("DAR_TLS_")
            }
            env.update(
                SSL_CERT_FILE=str(root / "trusted-ca.pem"),
                SSL_CERT_DIR=str(root / "empty-certs"),
                DAR_TLS_DIRECTORY=str(data),
                DAR_TLS_ENDPOINT=f"https://localhost:{trusted.server_port}",
                DAR_TLS_UNTRUSTED_ENDPOINT=f"https://localhost:{untrusted.server_port}",
            )
            for proxied in (False, True):
                if proxied:
                    address = f"http://127.0.0.1:{proxy.server_port}"
                    env.update(HTTPS_PROXY=address, DAR_TLS_PROXY=address)
                trusted.requests.clear()
                print(f"HTTPS certificate checks, proxy={proxied}", flush=True)
                subprocess.run(
                    [
                        "cargo",
                        "test",
                        "--locked",
                        "--test",
                        "reader",
                        *sys.argv[1:],
                        "https::verified_storage_paths",
                        "--",
                        "--exact",
                        "--ignored",
                        "--nocapture",
                    ],
                    env=env,
                    check=True,
                    timeout=600,
                )
                # Prove the tests exercised both full presigned GETs and range reads.
                assert any(
                    "X-Amz-Signature=" in path and not rng
                    for path, rng, _ in trusted.requests
                )
                assert any(rng for _, rng, _ in trusted.requests)
                assert not untrusted.requests, (
                    "untrusted server received an HTTP request"
                )
                if proxied:
                    assert proxy.requests, "proxy was bypassed"
                    assert all(
                        port in proxy.local_ports for _, _, port in trusted.requests
                    ), "HTTPS request bypassed the proxy"


if __name__ == "__main__":
    main()
