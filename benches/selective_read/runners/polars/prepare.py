"""Prepare the pinned Polars runner and save its offline artifacts."""

import argparse
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from prepare_python import prepare

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, help="reuse saved artifacts without downloads")
    args = parser.parse_args()
    prepare(HERE, args.output, args.artifacts)
