"""Reuse validated production bindings during one controller/report operation."""

from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

from oracle import file_identity, require


@contextmanager
def bindings():
    # ponytail: scoped replacement for synchronous scripts; pass a binding
    # resolver if a controller later validates metadata concurrently.
    import production_workloads as production
    original = production.binding
    cached = {}

    def binding(value, fixtures, case):
        row = next(r for r in value["cases"] if r["case_id"] == case)
        path = (Path(fixtures) / "manifest.json").resolve()
        identity = file_identity(path)
        previous = cached.get((path, case))
        if previous is not None and previous[0] == identity and previous[1] == row:
            require(file_identity(path) == identity, "fixture manifest changed during metadata validation")
            return row
        result = original(value, fixtures, case)
        require(file_identity(path) == identity, "fixture manifest changed during metadata validation")
        cached[path, case] = identity, deepcopy(row)
        return result

    production.binding = binding
    try:
        yield
    finally:
        production.binding = original
