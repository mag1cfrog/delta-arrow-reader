"""Call the native network controller from existing campaign entry points."""

from storage import native

BOUNDARY = "proxy client-facing HTTP ResponseWriter accepted body bytes"


def config(directory):
    return native.call("network", {"action": "config", "directory": str(directory)})


def control(directory, *, reset=False, traced=False, timeout=10):
    return native.call("network", {"action": "control", "directory": str(directory),
                                  "reset": reset, "traced": traced, "timeout": timeout})


def verify(directory):
    native.call("network", {"action": "verify", "directory": str(directory)})


def stop(directory):
    native.call("network", {"action": "stop", "directory": str(directory)})


def finish(directory, output):
    return native.call("network", {"action": "finish", "directory": str(directory), "output": str(output)})
