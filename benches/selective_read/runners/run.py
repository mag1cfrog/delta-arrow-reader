"""Shared Python adapter identities, protocol checks and immutable artifacts."""

import json
import runpy
import sys

import native


def digest(path):
    return native.call("reader-runtime", {"action": "digest", "path": str(path)})


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
