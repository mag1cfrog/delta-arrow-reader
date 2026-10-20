"""Prepare and operate the dedicated, pinned loopback MinIO benchmark server."""

import argparse
import base64
from collections import Counter
from contextlib import contextmanager
import fcntl
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import build_opener, ProxyHandler

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import digest, save
import native

BUCKET = "selective-read"
GO_ENV = {"GOTOOLCHAIN": "go1.24.7", "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64",
          "GOAMD64": "v1", "GOMAXPROCS": "8"}
HTTP = build_opener(ProxyHandler({}))


def prepare(output, source=None):
    subprocess.run([str(native.executable()), "selective-read", "prepare-storage", "--output", str(output.resolve()),
                    *(["--source", str(source.resolve())] if source else [])], check=True, cwd=HERE.parents[1])


def topology():
    return native.call("storage", {"action": "topology"})


def placement(rows):
    return native.call("storage", {"action": "placement", "rows": rows})


def properties(unit):
    return native.call("storage", {"action": "properties", "unit": unit})


def state(path):
    return native.call("storage", {"action": "state", "directory": str(path)})


def verify_server(directory):
    native.call("storage", {"action": "verify-server", "directory": str(directory)})
    import network
    network.verify(directory)


@contextmanager
def exclusive(directory):
    with (directory / ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def credentials(directory):
    return native.call("storage", {"action": "credentials", "directory": str(directory)})


def curl(directory, path, *options, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    config = state(directory)
    secret = credentials(directory)
    # curl's native SigV4 signer avoids another SDK. Secrets travel on stdin,
    # never in argv, observation records, request URLs or a curl configuration file.
    text = f'user = "{secret["access_key"]}:{secret["secret_key"]}"\naws-sigv4 = "aws:amz:us-east-1:s3"\n'
    process = subprocess.Popen(["curl", "--disable", "--silent", "--show-error", "--noproxy", "*", "--config", "-",
        config["endpoint"] + path, *options], stdin=subprocess.PIPE, stdout=stdout, stderr=stderr)
    process.stdin.write(text.encode())
    process.stdin.close()
    process.stdin = None
    return process


def request(directory, path, *options):
    return base64.b64decode(native.call("storage", {"action": "request", "directory": str(directory),
                                                  "path": path, "options": options}))


def start(build, directory, port):
    subprocess.run([str(native.executable()), "selective-read", "start-storage", "--build", str(build.resolve()),
                    "--state", str(directory.resolve()), "--port", str(port)], check=True, cwd=HERE.parents[1])


def inventory(fixtures, table_ids=None):
    manifest = json.loads((fixtures / "manifest.json").read_text())
    from run import fixture_tables
    tables = fixture_tables(manifest)
    if table_ids is not None:
        if (not isinstance(table_ids, list) or not table_ids
                or not all(isinstance(t, str) for t in table_ids)
                or len(set(table_ids)) != len(table_ids)
                or not set(table_ids) <= {t["id"] for t in tables}):
            raise ValueError("invalid native upload table selection")
        tables = [t for t in tables if t["id"] in table_ids]
    objects = []
    for table in tables:
        logs = table.get("delta_logs", [table["delta_log"]])
        dvs = [f["deletion_vector"] for f in table["files"] if "deletion_vector" in f]
        for item in logs + table["files"] + dvs:
            name = str(Path(table["path"]) / item["path"])
            path = (fixtures / name).resolve()
            assert path.is_relative_to(fixtures.resolve()) and path.is_file(), name
            assert path.stat().st_size == item["bytes"] and digest(path) == item["sha256"], name
            objects.append({"path": name, "bytes": item["bytes"], "sha256": item["sha256"]})
    assert len({o["path"] for o in objects}) == len(objects)
    return objects


def upload(directory, fixtures, output, table_ids=None, *, checksum=False):
    started = time.monotonic()
    objects = inventory(fixtures, table_ids)
    prefix = digest(fixtures / "manifest.json")
    with exclusive(directory):
        verify_server(directory)
        methods = Counter(put_verified(directory, prefix + "/" + item["path"],
                                      fixtures / item["path"], item["sha256"], checksum=checksum)
                          for item in objects)
    save(output, {"status": "verified", "server_sha256": digest(directory / "server.json"),
                  "fixture_manifest_sha256": prefix, "prefix": prefix, "objects": objects,
                  "table_root": f"s3://{BUCKET}/{prefix}", "table_ids": table_ids, "verified_ns": time.time_ns(),
                  "verification": {"requested": "server-sha256" if checksum else "full-get", "methods": dict(methods)},
                  "source_sha256": digest(Path(__file__)), "controller_sha256": digest(native.executable()),
                  "elapsed_seconds": time.monotonic() - started})


def put_verified(directory, key, source, expected, *, checksum=False):
    return native.call("storage", {"action": "put-verified", "directory": str(directory), "key": key,
        "source": str(source), "expected": expected, "checksum": checksum})


def reader_environment(directory):
    return native.call("storage", {"action": "reader-environment", "directory": str(directory)})


def reader_prefix(directory, unit=None):
    return native.call("storage", {"action": "reader-prefix", "directory": str(directory), "unit": unit})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("prepare")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--source", type=Path)
    p = commands.add_parser("start")
    p.add_argument("--build", type=Path, required=True)
    p.add_argument("--state", type=Path, required=True)
    p.add_argument("--port", type=int, default=19000)
    p = commands.add_parser("stop")
    p.add_argument("--state", type=Path, required=True)
    p = commands.add_parser("upload")
    p.add_argument("--state", type=Path, required=True)
    p.add_argument("--fixtures", type=Path, required=True)
    p.add_argument("--table", action="append", help="repeat for exact native table IDs; default: all tables")
    p.add_argument("--verify-checksum", action="store_true",
                   help="verify stored full-object SHA-256 and length; read back objects without that checksum")
    p.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.output, args.source)
    elif args.command == "start":
        start(args.build, args.state, args.port)
    elif args.command == "upload":
        upload(args.state, args.fixtures, args.output, args.table, checksum=args.verify_checksum)
    else:
        with exclusive(args.state):
            import network
            network.stop(args.state)
            subprocess.run(["systemctl", "--user", "stop", state(args.state)["unit"]], check=True)
