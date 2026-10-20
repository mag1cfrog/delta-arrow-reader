"""Launch the Rust watchdog from the Python reader adapters."""

from pathlib import Path

from native import call


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def integer(value):
    return type(value) is int and value >= 0


def launch(command, payload, output, stdout, stderr, env=None, *, query_seconds=1800, cleanup_seconds=60):
    return call("supervise", {"command": command, "payload": payload, "output": str(output.resolve()),
                "stdout": str(Path(stdout.name).resolve()), "stderr": str(Path(stderr.name).resolve()),
                "env": env, "query_seconds": query_seconds, "cleanup_seconds": cleanup_seconds})
