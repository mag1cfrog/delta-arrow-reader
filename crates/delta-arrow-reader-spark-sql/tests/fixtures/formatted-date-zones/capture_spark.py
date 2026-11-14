"""Capture formatted date zones from source-only SQL and registered inputs."""

import importlib.util
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent
sys.path.insert(0, str(FIXTURES))
from pyspark.sql import types
from py4j.protocol import Py4JError
from capture_spark_requests import capture_requested_queries

source = FIXTURES / "try-to-date/capture_try_to_date_spark.py"
spec = importlib.util.spec_from_file_location("date_capture", source)
date_capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(date_capture)
request_path = HERE / "requests.json"
request = json.loads(request_path.read_text())
checked_sessions = set()


def capture(session, case, _encode):
    session.conf.set("spark.sql.session.timeZone", case["timezone"])
    readbacks = []
    identity = session._jsparkSession.sessionUUID()
    if identity not in checked_sessions:
        corpus = next(item for item in request["corpora"] if item["corpus"] == case["corpus"])
        for item in corpus["metadata"]["inputs"]:
            frame = session.table(item["name"])
            rows = [[date_capture.encode_value(row, index, field.dataType, types)
                     for index, field in enumerate(frame.schema.fields)]
                    for row in frame._jdf.queryExecution().executedPlan().executeCollect()]
            assert rows == item["rows"], rows
            readbacks.append({"name": item["name"], "schema": frame.schema.jsonValue(), "rows": rows})
        checked_sessions.add(identity)
    return {**case, **date_capture.capture(session, case["sql"], types, Py4JError),
            "input_readbacks": readbacks}


capture_requested_queries(request, Path(sys.argv[1]), [Path(__file__), request_path, source],
                          [request_path], capture_query=capture)
