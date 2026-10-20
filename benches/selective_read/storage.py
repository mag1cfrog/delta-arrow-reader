"""Stage verified fixtures on the dedicated MinIO benchmark server."""

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import digest, save
import native

BUCKET = "selective-read"


def state(path):
    return native.call("storage", {"action": "state", "directory": str(path)})


def verify_server(directory):
    native.call("storage", {"action": "verify-server", "directory": str(directory)})


@contextmanager
def exclusive(directory):
    with (directory / ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def credentials(directory):
    return native.call("storage", {"action": "credentials", "directory": str(directory)})


def inventory(fixtures, table_ids=None):
    return native.call("storage", {"action": "inventory", "fixtures": str(fixtures), "table_ids": table_ids})


def upload(directory, fixtures, output, table_ids=None, *, checksum=False):
    native.call("storage", {"action": "upload", "directory": str(directory), "fixtures": str(fixtures),
        "output": str(output), "table_ids": table_ids, "checksum": checksum})


def put_verified(directory, key, source, expected, *, checksum=False):
    return native.call("storage", {"action": "put-verified", "directory": str(directory), "key": key,
        "source": str(source), "expected": expected, "checksum": checksum})


if __name__ == "__main__":
    if sys.argv[1:2] == ["upload"]:
        sys.argv.pop(1)
    os.execv(native.executable(), [str(native.executable()), "selective-read", "upload-storage", *sys.argv[1:]])
