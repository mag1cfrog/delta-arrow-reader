"""Check the 37 paired results and profile one representative per plan group."""

import argparse
import importlib.util
from pathlib import Path
import time
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
MANIFEST = HERE / "manifest.json"

spec = importlib.util.spec_from_file_location(
    "function_profile", HERE.parent / "array_functions_1410" / "profile.py")
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)
profile.MANIFEST = MANIFEST
runner = profile.runner


def protocol():
    settings = runner.read(MANIFEST)
    source = profile.FROZEN / "manifest.json"
    assert settings["frozen_manifest_sha256"] == runner.digest(source)
    original = {case["id"]: case for case in runner.read(source)["functions"]}
    cases = settings["functions"]
    assert len(cases) == len({case["id"] for case in cases}) == 37
    assert all(case == original[case["id"]] and case["native_sql"] for case in cases)
    return settings


def preflight(binary, receipt, commit, settings, output, deadline):
    output.mkdir()
    eligible, excluded = {}, {}
    for case in settings["functions"]:
        case_id = case["id"]
        print(case_id, flush=True)
        try:
            result = profile.observe(binary, case, output / case_id, profile.schedule(), deadline, MANIFEST)
            eligible[case_id] = {key: value for key, value in result.items() if key != "plans"}
        except ValueError as error:
            excluded[case_id] = str(error)
    profile.write(output / "receipt.json", dict(
        protocol_commit=commit, manifest_sha256=runner.digest(MANIFEST),
        build=receipt, eligible=eligible, excluded=excluded))


def profiles(binary, receipt, commit, settings, checked, groups_path, output, deadline):
    groups = runner.read(groups_path)
    checked_path = checked / "receipt.json"
    before = runner.read(checked_path)
    assert before["build"] == receipt and before["manifest_sha256"] == runner.digest(MANIFEST)
    assert runner.digest(checked_path) == groups["preflight_receipt_sha256"]
    all_ids = [case["id"] for case in settings["functions"]]
    assignments = [case for group in groups["groups"] for case in group["cases"]]
    assert sorted(all_ids) == sorted(assignments)
    assert len({group["name"] for group in groups["groups"]}) == len(groups["groups"])
    output.mkdir()
    profile.write(output / "receipt.json", dict(
        protocol_commit=commit, profile_manifest_sha256=runner.digest(groups_path),
        preflight_receipt_sha256=runner.digest(checked_path), build=receipt))
    for group in groups["groups"]:
        if not set(group["cases"]) & before["eligible"].keys():
            assert group["representative"] is None
            continue
        case_id = group["representative"]
        assert case_id in group["cases"] and case_id in before["eligible"]
        name = group["name"]
        assert name.replace("-", "").isalnum()
        print(name, case_id, flush=True)
        config = dict(settings["profile"], configuration_by_case={case_id: group["configuration"]})
        assert group["configuration"] in ("spark-off", "spark-on", "native")
        profile.profile_case(binary, case_id, before["eligible"][case_id], output / name,
                             profile.schedule(), config, deadline)


def main():
    if not __debug__:
        raise SystemExit("Run with Python validation enabled.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("build", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--preflight", type=Path)
    parser.add_argument("--groups", type=Path)
    args = parser.parse_args()
    settings = protocol()
    assert bool(args.preflight) == bool(args.groups)
    assert not runner.git("status", "--porcelain"), "commit this protocol before collection"
    commit = runner.git("rev-parse", "HEAD")
    binary, receipt = profile.verify_build(args.build.resolve(), settings, commit)
    deadline = time.monotonic() + settings["overall_timeout_seconds"]
    output = args.output.resolve()
    if args.groups:
        groups_path = args.groups.resolve()
        assert runner.git("show", f"{commit}:{groups_path.relative_to(HERE.parents[3])}") == groups_path.read_text().strip()
        profiles(binary, receipt, commit, settings, args.preflight.resolve(), groups_path, output, deadline)
    else:
        preflight(binary, receipt, commit, settings, output, deadline)


if __name__ == "__main__":
    main()
