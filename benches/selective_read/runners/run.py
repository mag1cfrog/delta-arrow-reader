"""Shared Python adapter identities, protocol checks and immutable artifacts."""

import hashlib
import json
import os
from pathlib import Path
import runpy
import sys

import native


_DIGESTS = {}


def file_identity(path):
    stat = os.stat(path)
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def digest(path):
    """Hash an unchanged local inode once per process, including hard links."""
    with Path(path).open("rb") as source:
        identity = file_identity(source.fileno())
        previous = _DIGESTS.get(identity[:2])
        value = previous[1] if previous and previous[0] == identity else hashlib.file_digest(source, "sha256").hexdigest()
        if file_identity(source.fileno()) != identity or file_identity(path) != identity:
            raise ValueError(f"file changed during checksum: {path}")
        _DIGESTS[identity[:2]] = identity, value
        return value


def save(path, value):
    with path.open("x") as out:
        json.dump(value, out, sort_keys=True, indent=2)
        out.write("\n")


def load_adapter(binary):
    """Import the built adapter with its bundled helpers in a probe interpreter."""
    previous = {name: sys.modules.pop(name, None) for name in ("run", "python_common", "native")}
    search_path = sys.path.copy()
    try:
        return runpy.run_path(str(binary.resolve()), run_name="native_probe")
    finally:
        sys.path[:] = search_path
        for name, module in previous.items():
            sys.modules.pop(name, None)
            if module is not None:
                sys.modules[name] = module


def query_count(value):
    count = value.get("concurrent_queries")
    if count is not None:
        if (type(count) is not int or count not in (2, 4) or value["execution_mode"] != "reuse"
                or value.get("comparison_revision") != 6):
            raise ValueError("concurrent queries require revision 6, reuse mode and a count of 2 or 4")
        return count
    if value["execution_mode"] == "open":
        return 1
    return 2 if value.get("comparison_revision", 2) >= 4 else 10
