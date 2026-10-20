"""Keep independent Python validation caches alive for a native campaign."""

from contextlib import redirect_stdout
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE / "runners")]
import large_workloads
import matrix
import metadata_cache
import run
import storage


def dispatch(value):
    action = value["action"]
    fixtures = Path(value["fixtures"]) if "fixtures" in value else None
    if action == "prepare":
        state, upload = Path(value["state"]), Path(value["upload"])
        if value["upload_tables"]:
            if upload.exists():
                raise ValueError("inline upload requires a new receipt path")
            storage.upload(state, fixtures, upload, value["upload_tables"], checksum=True)
        receipt = json.loads(upload.read_text())
        return {"receipt": receipt, "objects": storage.inventory(fixtures, receipt.get("table_ids"))}
    if action == "load":
        workload = Path(value["workload"]) if value["workload"] else None
        return {"matrix": matrix.load(Path(value["matrix"]), fixtures) if value["matrix"] else None,
                "workload": large_workloads.load(workload) if workload else None,
                "comparison": large_workloads.identity(workload) if workload else
                    {"comparison_revision": 2, "protocol_sha256": run.digest(run.PROTOCOL)}}
    if action == "request":
        return run.request(fixtures, value["case_id"], value["execution_mode"], "validation", "pending",
                           workload=Path(value["workload"]) if value["workload"] else None)
    if action == "check":
        record = value["record"]
        run.check_result(record, Path(value["output"]), fixtures, Path(value["reference"]), value["payload"])
        return record
    raise ValueError("unknown controller worker operation")


if __name__ == "__main__":
    with metadata_cache.bindings():
        for line in sys.stdin:
            try:
                with redirect_stdout(sys.stderr):
                    response = {"result": dispatch(json.loads(line))}
            except Exception as error:
                response = {"error": type(error).__name__ + ": " + str(error)}
            print(json.dumps(response), flush=True)
