"""Call the native network controller from existing campaign entry points."""

from storage import native


def config(directory):
    return native.call("network", {"action": "config", "directory": str(directory)})
