"""Run one Spark feasibility invocation through the existing observer and oracle."""

import argparse
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE.parent), str(HERE.parents[1])]
import observe
import oracle
import run


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--upload", type=Path, required=True)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--case", required=True)
    parser.add_argument("--execution", choices=("open", "reuse"), required=True)
    parser.add_argument("--purpose", choices=("validation", "diagnostic", "io"), required=True)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.purpose == "validation" and args.reference is None:
        parser.error("validation requires --reference")
    if json.loads(args.binary.with_name("build.json").read_text())["reader_id"] != "spark":
        parser.error("provide the Spark pilot build")
    receipt = json.loads(args.upload.read_text())
    if not (receipt["status"] == "verified" and args.case in receipt["table_ids"]
            and receipt["server_sha256"] == run.digest(args.state / "server.json")
            and receipt["fixture_manifest_sha256"] == run.digest(args.fixtures / "manifest.json")
            and receipt["objects"] == observe.storage.inventory(args.fixtures, receipt["table_ids"])):
        parser.error("upload differs from the verified server, fixture or selected snapshot")
    # Accept an additional export identity only in this pilot process. Historical
    # workloads, formal rosters and the independent value checker stay unchanged.
    oracle.READERS.add("spark")
    payload = run.request(args.fixtures, args.case, args.execution, args.purpose, args.output.name,
                          workload=args.workload)
    row = next(r for r in json.loads(args.workload.read_text())["cases"] if r["case_id"] == args.case)
    payload["table_uri"] = f"s3://{observe.storage.BUCKET}/{row['fixture_manifest_sha256']}/{row['fixture_path']}"
    result = observe.invoke(args.state, args.binary, payload, args.output, args.fixtures, args.reference)
    run.save(args.output / "pilot-inputs.json", {"upload_sha256": run.digest(args.upload),
             "command_sha256": run.digest(Path(__file__)), "pilot_only": True, "publication_ready": False})
    print(json.dumps({"status": result["status"], "observation": str(args.output / "storage-observation.json"),
                      "pilot_only": True, "publication_ready": False}))
    sys.exit(0 if result["status"] == "success" else 1)
