"""Capture file origins from Spark Parquet and ordinary metadata from explicit inputs."""

import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from capture_spark_requests import capture_requested_queries
from capture_date_range_747_spark import capture

REQUEST = HERE / "requests.json"
request = json.loads(REQUEST.read_text())
cases = request["corpora"][0]["cases"]
assert request["expected_record_count"] == len(cases) == 28
assert len({(case["id"], case["ansi"]) for case in cases}) == 28
assert {case["ansi"] for case in cases} == {False, True}
if sys.argv[1:] == ["--self-check"]:
    print("28 unique source-only requests validated; no Spark process started")
    raise SystemExit(0)

owned = []
file_sources = {}


def capture_file_query(session, case, encode):
    session_id = session._jsparkSession.sessionUUID()
    if session_id not in file_sources:
        root = Path(tempfile.mkdtemp(prefix="dar-file-metadata-spark-"))
        owned.append(root)
        source = session.table("mem_source")
        for index, condition in [(1, "id <= 2"), (2, "id > 2")]:
            staging = root / f"write-{index}"
            source.where(condition).coalesce(1).write.parquet(str(staging))
            parquet = list(staging.glob("*.parquet"))
            assert len(parquet) == 1
            parquet[0].rename(root / f"part-{index:05}.parquet")
            shutil.rmtree(staging)
        session.read.parquet(str(root)).createOrReplaceTempView("file_source")
        file_sources[session_id] = {
            "format": "parquet",
            "root_uri": root.as_uri() + "/",
            "files": ["part-00001.parquet", "part-00002.parquet"],
            "delta_python_package_available": importlib.util.find_spec("delta") is not None,
        }
    observed = capture(session, case, encode)
    observed["file_source"] = file_sources[session_id]
    return observed


try:
    capture_requested_queries(
        request, Path(sys.argv[1]), [Path(__file__), REQUEST], [REQUEST],
        capture_query=capture_file_query,
        spark_settings={"spark.sql.adaptive.enabled": "false"},
    )
finally:
    for root in owned:
        shutil.rmtree(root)
