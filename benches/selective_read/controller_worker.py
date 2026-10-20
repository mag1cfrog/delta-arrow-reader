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

EXPECTED = {}


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
    if action == "capability-export":
        from capabilities import export_corpus
        return export_corpus(value["record"], Path(value["output"]), value["deleted"], value["ids"], value.get("semantics"))
    if action == "capability-stream":
        from capabilities import stream_fixture
        return stream_fixture(Path(value["output"]))
    if action == "capability-corrupt":
        from capabilities import corrupt_export
        return corrupt_export(Path(value["input"]), Path(value["output"]))
    if action == "check-export":
        import oracle
        return oracle.check(Path(value["reference"]), fixtures, Path(value["result"]), Path(value["identity"]))
    if action == "check-intra":
        return check_intra(value)
    if action == "experiment-reference":
        import pyarrow as pa
        import pyarrow.parquet as pq
        fixture, manifest = Path(value["fixture"]), value["manifest"]
        if value["driver"] == "numeric" and value["selection"] == "dense":
            expected = pq.read_table([fixture / f["path"] for f in manifest["files"]],
                columns=["row_id"] + [f"{value['column_type']}_{i}" for i in range(8)])
        else:
            if value["driver"] == "length" and pa.__version__ != manifest["pyarrow"]:
                raise ValueError("oracle Arrow version changed")
            key = value["column_type"] if value["driver"] == "numeric" else value["selection"]
            expected = pa.ipc.open_stream(fixture / manifest["references"][key]["path"]).read_all()
        EXPECTED[value["driver"]] = expected
        return expected.num_rows
    if action == "experiment-check":
        import pyarrow as pa
        actual = pa.ipc.open_stream(value["path"]).read_all()
        expected = EXPECTED[value["driver"]]
        if value["driver"] == "numeric":
            from numeric_pages import check
            check(actual, expected)
        elif not actual.cast(expected.schema).equals(expected):
            raise ValueError("query result differs: " + value["path"])
        return None
    if action == "repacked-values":
        from file_organizations import batches, same_rows
        return same_rows(batches(Path(value["parent"]), value["original"]), batches(fixtures, value["table"]))
    if action == "prepare-reference":
        import oracle
        reference = Path(value["reference"])
        if "sql" in value:
            oracle.require(oracle.case_input(fixtures, value["case_id"])[-1] == value["sql"], "oracle SQL differs from catalog")
        metadata = (json.loads((reference / "reference.json").read_text()) if value["reuse"] else
                    oracle.prepare(fixtures, value["case_id"], reference))
        oracle.check_reference_build(metadata)
        return metadata
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
