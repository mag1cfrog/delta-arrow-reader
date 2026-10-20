"""Prepare and report the 30 retained public predicate/projection cases."""

import argparse
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "runners"))
from run import digest, load_adapter, save
from python_common import require
import native



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



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("translations", help=argparse.SUPPRESS)
    for name in ("binary", "request", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    translations(args.binary, args.request, args.output)
