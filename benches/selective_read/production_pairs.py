"""Validate production inputs and ask Rust to prepare immutable Q2/Q4 DV pairs."""

import argparse
import json
from pathlib import Path

import production_shapes as shapes
from production_workloads import query_fields
from run import digest, fixture_tables
import native


def prepare(fixtures, binary, output, disk_gib, seconds, layout=None):
    validated = []
    for root in fixtures:
        path = root / "manifest.json"
        checksum = digest(path)
        manifest = json.loads(path.read_text())
        for table in fixture_tables(manifest):
            query_fields(table["id"], manifest)
        validated.append({"path": str(root.resolve()), "manifest_sha256": checksum})
    return native.call("production-pairs", {"fixtures": validated, "binary": str(binary.resolve()),
        "output": str(output.resolve()), "disk_gib": disk_gib, "seconds": seconds, "layout": layout,
        "contract_sha256": digest(shapes.CONTRACT), "driver_sha256": digest(Path(__file__))})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, action="append", required=True)
    parser.add_argument("--layout", choices=("localized", "scattered"),
                        help="prepare one layout and its DV snapshot for staged execution")
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--disk-limit-gib", type=int, required=True)
    parser.add_argument("--elapsed-limit-seconds", type=int, default=1800)
    args = parser.parse_args()
    value = prepare(args.fixtures, args.binary, args.output, args.disk_limit_gib, args.elapsed_limit_seconds, args.layout)
    print(json.dumps({"status": value["status"], "tables": len(value["writer"]["tables"])}))
