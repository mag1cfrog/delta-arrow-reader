"""Capture #1117 ordered values and executed schemas from source-only requests.

Run with pinned PySpark 4.2.0, Java 21 and TZ=UTC:
    python capture_spark.py /path/to/fresh-spark-results.jsonl
"""

import hashlib
import json
from pathlib import Path
import sys
import tempfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from capture_spark_requests import capture_requested_queries


def capture_executed(session, case, encode):
    frame = session.sql(case["sql"])
    rows = frame.collect()
    schema = frame.schema
    executed_schema = json.loads(frame._jdf.queryExecution().executedPlan().schema().json())
    return dict(case, status="ok", rows=[encode(row, schema) for row in rows],
                physical_schema=executed_schema)


def main():
    output = Path(sys.argv[1])
    assert not output.exists(), output
    manifest_path = HERE / "inputs.json"
    manifest = json.loads(manifest_path.read_text())
    source_path = HERE.parent / manifest["input_source"]["path"]
    assert hashlib.sha256(source_path.read_bytes()).hexdigest() == manifest["input_source"]["sha256"]
    source = json.loads(source_path.read_text())
    inputs = [item for item in source["corpora"][0]["metadata"]["inputs"]
              if item["name"] in manifest["input_source"]["providers"]]
    assert len(inputs) == 3
    cases = [dict(case, corpus="zero-offset-nulls", ansi=ansi)
             for case in manifest["cases"] for ansi in manifest["ansi_modes"]]
    assert len(cases) == 18
    request = {"issue": manifest["issue"], "expected_record_count": len(cases), "corpora": [{
        "corpus": "zero-offset-nulls", "source": {"reads_expected_outputs": False},
        "metadata": {"inputs": inputs, "timezone": manifest["timezone"],
                     "spark_version": manifest["spark_version"]}, "cases": cases}]}
    with tempfile.TemporaryDirectory(prefix="dar-zero-offset-") as temporary:
        raw_output = Path(temporary) / "spark.jsonl"
        capture_requested_queries(request, raw_output,
            [Path(__file__), manifest_path, source_path], [manifest_path],
            capture_query=capture_executed)
        records = [json.loads(line) for line in raw_output.read_text().splitlines()[1:]]
        with output.open("x") as result:
            for record in records:
                del record["corpus"], record["effective_settings"]
                result.write(json.dumps(record, separators=(",", ":")) + "\n")
    print(json.dumps({"output": str(output), "observations": len(records)}), flush=True)


if __name__ == "__main__":
    main()
