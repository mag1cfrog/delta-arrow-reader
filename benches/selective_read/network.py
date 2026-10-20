"""Call the native network controller from existing campaign entry points."""

from storage import native

def config(directory):
    return native.call("network", {"action": "config", "directory": str(directory)})


def control(directory, *, reset=False, traced=False, timeout=10):
    return native.call("network", {"action": "control", "directory": str(directory),
                                  "reset": reset, "traced": traced, "timeout": timeout})


def finish(directory, output):
    return native.call("network", {"action": "finish", "directory": str(directory), "output": str(output)})
