"""Prepare and report the 30 retained public predicate/projection cases."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import digest, load_adapter, save
from python_common import require
import native



def sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def translations(binary, request, output):
    """Use the actual pinned adapter in its own interpreter, without table I/O."""
    adapter = load_adapter(binary)
    build = json.loads(binary.with_name("build.json").read_text())
    require(build["reader_id"] == "polars", "new workload translations require Polars")
    require(digest(binary) == build["executable_sha256"], "stale adapter executable")
    require(adapter["runtime_metadata"](adapter["engine"]()) == build["runtime"], "stale native runtime")
    result = {"reader": build["reader_id"], "lock_sha256": build["lockfile_sha256"],
              "adapter_sha256": digest(binary), "expressions": {}}
    for key, sql in json.loads(request.read_text()).items():
        expression = adapter["expression_identity"](sql)
        result["expressions"][key] = {"native_expression": expression, "sha256": adapter["json_hash"](expression)}
    save(output, result)


def catalog():
    return native.call("matrix", {"action": "catalog", "harness": str(HERE)})


def load(path, fixtures):
    value = native.call("matrix", {"action": "load", "path": str(path), "fixtures": str(fixtures), "harness": str(HERE)})
    import oracle
    for row in value["cases"]:
        if row["status"] == "prepared":
            oracle.check_reference_build(json.loads((Path(row["reference"]) / "reference.json").read_text()))
    return value


if __name__ == "__main__":
    if sys.argv[1:2] in (["prepare"], ["report"]):
        binary = str(native.executable())
        command = [binary, "selective-read", sys.argv[1] + "-matrix", *sys.argv[2:], "--harness", str(HERE)]
        if sys.argv[1] == "prepare":
            command += ["--python", sys.executable]
        os.execv(binary, command)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("translations", help=argparse.SUPPRESS)
    for name in ("binary", "request", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    commands.add_parser("prepare")
    commands.add_parser("report")
    args = parser.parse_args()
    translations(args.binary, args.request, args.output)
