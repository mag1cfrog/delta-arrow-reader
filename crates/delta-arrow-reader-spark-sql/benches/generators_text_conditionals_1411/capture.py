#!/usr/bin/env python3
"""Run the committed #1411 statements with the existing profile schedule."""

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
    parser.add_argument("--case", action="append", default=[])
    args = parser.parse_args()

    source = HERE.parent / "array_functions_1410" / "profile.py"
    spec = importlib.util.spec_from_file_location("function_profile", source)
    profile = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(profile)
    profile.MANIFEST = HERE / "manifest.json"
    settings = profile.load_manifest()
    if args.case:
        selected = set(args.case)
        settings["functions"] = [case for case in settings["functions"] if case["id"] in selected]
        if selected != {case["id"] for case in settings["functions"]}:
            parser.error("A selected case is missing from the manifest.")
    profile.capture_profiles(args.build.resolve(), args.output.resolve(), settings)


if __name__ == "__main__":
    main()
