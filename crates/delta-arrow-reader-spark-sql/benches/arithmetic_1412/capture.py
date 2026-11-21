#!/usr/bin/env python3
"""Capture the committed #1412 arithmetic plans and CPU profiles."""

import argparse
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent


def main():
    if not __debug__:
        raise SystemExit("Run with Python validation enabled.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("build", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    source = HERE.parent / "array_functions_1410" / "profile.py"
    spec = importlib.util.spec_from_file_location("function_profile", source)
    profile = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(profile)
    profile.MANIFEST = HERE / "manifest.json"
    settings = profile.runner.read(profile.MANIFEST)
    frozen = profile.FROZEN / "manifest.json"
    assert profile.runner.digest(frozen) == settings["frozen_manifest_sha256"]
    originals = {case["id"]: case for case in profile.runner.read(frozen)["functions"]}
    cases = settings["functions"]
    assert len(cases) == len({case["id"] for case in cases}) == 15
    assert all(case == originals[case["id"]] and case["native_sql"] for case in cases)
    profile.capture_profiles(args.build.resolve(), args.output.resolve(), settings)


if __name__ == "__main__":
    main()
