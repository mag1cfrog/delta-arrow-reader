"""Run the original #1513 coordinator against the rebased correction."""

import hashlib
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True
STAGING = "d39ce212b3301c689063b290c2d432004f58a8d8"
FROZEN = Path(__file__).resolve().parents[1] / "time_zone_range_1513/run.py"


def load_coordinator():
    if not __debug__:
        raise RuntimeError("run the comparison with Python assertions enabled")
    assert hashlib.sha256(FROZEN.read_bytes()).hexdigest() == (
        "98ba4595f929c8cdf77e56f9907e82fb1647cc33ffcab7294098408d46b791b3"
    )
    spec = importlib.util.spec_from_file_location("frozen_1513", FROZEN)
    coordinator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(coordinator)
    coordinator.STAGING = STAGING
    return coordinator


if __name__ == "__main__":
    sys.exit(load_coordinator().main(*(Path(arg).resolve() for arg in sys.argv[1:])))
