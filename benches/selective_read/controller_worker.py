"""Keep SDK fixtures and bit-exact Arrow experiment checks in one process."""

from contextlib import redirect_stdout
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE / "runners")]

EXPECTED = {}


def dispatch(value):
    action = value["action"]
    if action == "capability-export":
        from capabilities import export_corpus
        return export_corpus(value["record"], Path(value["output"]), value["deleted"], value["ids"], value.get("semantics"))
    if action == "capability-stream":
        from capabilities import stream_fixture
        return stream_fixture(Path(value["output"]))
    if action == "capability-corrupt":
        from capabilities import corrupt_export
        return corrupt_export(Path(value["input"]), Path(value["output"]))
    if action == "experiment-reference":
        import pyarrow as pa
        import pyarrow.parquet as pq
        fixture, manifest = Path(value["fixture"]), value["manifest"]
        if value["driver"] == "numeric" and value["selection"] == "dense":
            expected = pq.read_table([fixture / f["path"] for f in manifest["files"]],
                columns=["row_id"] + [f"{value['column_type']}_{i}" for i in range(8)])
        else:
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
    raise ValueError("unknown controller worker operation")


if __name__ == "__main__":
    for line in sys.stdin:
        try:
            with redirect_stdout(sys.stderr):
                response = {"result": dispatch(json.loads(line))}
        except Exception as error:
            response = {"error": type(error).__name__ + ": " + str(error)}
        print(json.dumps(response), flush=True)
