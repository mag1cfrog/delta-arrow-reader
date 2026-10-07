"""Prepare the optional Spark Delta pilot from pinned, reusable artifacts."""

from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from prepare_python import prepare as prepare_python, prepare_cli


def prepare(output, java_home, artifacts):
    prepare_python(HERE, output, artifacts, java_home)


if __name__ == "__main__":
    prepare_cli(HERE, __doc__)
