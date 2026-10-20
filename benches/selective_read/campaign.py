"""Compatibility entry point for the native selective-read campaign controller."""

import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
import native
from run import HISTORICAL_READERS as READERS


def schedule(inventory, campaign_id, comparison=None, *, combined=None, gate_warmup=None, gate_diagnostics=None):
    return native.call("campaign", {"action": "schedule", "inventory": list(inventory.items()),
        "campaign_id": campaign_id, "comparison": comparison or {}, "combined": combined,
        "gate_warmup": gate_warmup, "gate_diagnostics": gate_diagnostics})


def summarize(inventory, slots, rows, resolution, integrity=True):
    return native.call("campaign", {"action": "summarize", "inventory": inventory, "slots": slots,
        "rows": rows, "resolution": resolution, "integrity": integrity})


if __name__ == "__main__":
    os.execv(native.executable(), [str(native.executable()), "selective-read", "run-campaign",
        "--python", sys.executable, "--harness", str(HERE), *sys.argv[1:]])
