"""Prepare the pinned Polars runner and save its offline artifacts."""

from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from prepare_python import prepare, prepare_cli

if __name__ == "__main__":
    prepare_cli(HERE, __doc__)
