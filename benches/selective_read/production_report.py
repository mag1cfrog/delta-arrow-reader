"""Combine audited formal batches without pooling or replacing their samples."""

import argparse
import json
from pathlib import Path
import tempfile

import large_workloads
import metadata_cache
import native
from run import PROTOCOL
from supervise import require


def report(definition, campaigns, output):
    require(not output.exists(), "report output already exists")
    with metadata_cache.bindings(), tempfile.TemporaryDirectory(dir=output.parent, prefix="production-report-") as scratch:
        frozen = large_workloads.load(definition)
        identity = large_workloads.identity(definition)
        audited = [(str(path), large_workloads.report(path, Path(scratch) / str(index)))
                   for index, path in enumerate(campaigns)]
        return native.call("production-report", {"definition": str(definition), "frozen": frozen,
            "identity": identity, "campaigns": audited, "output": str(output), "protocols": str(PROTOCOL.parent)})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--definition", type=Path, required=True)
    parser.add_argument("--campaign", type=Path, action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = report(args.definition, args.campaign, args.output)
    print(json.dumps({k: result[k] for k in ("status", "complete_cases", "formal_coverage_complete", "publication_ready")}))
