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


def check_intra(value):
    import oracle
    import pyarrow as pa
    reference = Path(value["reference"])
    record = value["record"]
    metadata = json.loads((reference / "reference.json").read_text())
    for field in oracle.IDENTITY_FIELDS:
        oracle.require(record["identity"][field] == metadata[field], "reference identity mismatch: " + field)
    checks = []
    for query in record["queries"]:
        path = Path(value["output"]) / "reader" / query["result"]
        with path.open("rb") as source, pa.ipc.open_stream(source) as batches:
            oracle.check_schema(batches.schema, metadata["projection"])
            rows = oracle.compare(batches, reference / "reference.parquet", metadata["projection"], metadata["output_rows"])
            oracle.require(not source.read(1), "trailing Arrow bytes")
        checks.append(record["identity"] | {"status": "passed", "output_rows": rows,
            "oracle_sha256": run.digest(HERE / "oracle.py"), "reference_sha256": metadata["reference_sha256"],
            "result_sha256": run.digest(path)})
    return {"status": "passed", "checks": checks}


def dispatch(value):
    action = value["action"]
    fixtures = Path(value["fixtures"]) if "fixtures" in value else None
    if action == "hashes":
        return [run.digest(Path(path)) for path in value["paths"]]
    if action == "check-intra":
        return check_intra(value)
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
