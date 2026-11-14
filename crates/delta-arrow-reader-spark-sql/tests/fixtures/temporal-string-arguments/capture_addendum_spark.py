"""Capture the bounded literal/SECOND addendum without changing the main capture."""
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

source = FIXTURES / "timestamp-ntz-conversion/capture_timestamp_ntz_conversion_spark.py"
spec = importlib.util.spec_from_file_location("timestamp_capture", source)
timestamp_capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(timestamp_capture)
original_encode = timestamp_capture.encode_value


def encode_internal(row, index, data_type, types):
    if not row.isNullAt(index) and isinstance(data_type, types.DecimalType):
        return format(row.getDecimal(index, data_type.precision, data_type.scale).toJavaBigDecimal(), "f")
    return original_encode(row, index, data_type, types)


timestamp_capture.encode_value = encode_internal
request_path = HERE / "addendum-requests.json"
request = json.loads(request_path.read_text())
checked = set()


def capture(session, case, _encode):
    session.conf.set("spark.sql.session.timeZone", case["timezone"])
    session.conf.set("spark.sql.timestampType", case["timestamp_type"])
    readbacks = []
    identity = session._jsparkSession.sessionUUID()
    if identity not in checked:
        corpus = next(item for item in request["corpora"] if item["corpus"] == case["corpus"])
        for item in corpus["metadata"]["inputs"]:
            frame = session.table(item["name"])
            rows = [[encode_internal(row, i, field.dataType, types)
                     for i, field in enumerate(frame.schema.fields)]
                    for row in frame._jdf.queryExecution().executedPlan().executeCollect()]
            assert rows == item["rows"], rows
            readbacks.append({"name": item["name"], "schema": frame.schema.jsonValue(), "rows": rows})
        checked.add(identity)
    return {**case, **timestamp_capture.capture(session, case["sql"], types, Py4JError),
            "input_readbacks": readbacks,
            "timestamp_type_observed": session.conf.get("spark.sql.timestampType")}


capture_requested_queries(request, Path(sys.argv[1]), [Path(__file__), request_path, source],
                          [request_path], capture_query=capture)
