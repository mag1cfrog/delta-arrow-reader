"""Capture date-format timezone results from source-only definitions on pinned Spark."""

from datetime import date, datetime, timedelta, timezone
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
import capture_floating_group_keys_spark as encoding
import capture_window_name_stages_spark as registration
from capture_spark_requests import capture_requested_queries

source = FIXTURES / "timestamp-ntz-conversion/capture_timestamp_ntz_conversion_spark.py"
spec = importlib.util.spec_from_file_location("timestamp_capture", source)
timestamp_capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(timestamp_capture)
original_encode = encoding.encode
original_decode = registration.decode


def encode(value, data_type, *, input_value=False):
    if value is not None and input_value:
        if isinstance(data_type, types.DateType):
            return str((value - date(1970, 1, 1)).days)
        if isinstance(data_type, (types.TimestampType, types.TimestampNTZType)):
            value = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
            delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
            return str((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
    return original_encode(value, data_type, input_value=input_value)


def decode(value, data_type):
    if value is not None:
        if isinstance(data_type, types.DateType):
            return date(1970, 1, 1) + timedelta(days=int(value))
        if isinstance(data_type, types.TimestampType):
            return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(value))
        if isinstance(data_type, types.TimestampNTZType):
            return datetime(1970, 1, 1) + timedelta(microseconds=int(value))
    return original_decode(value, data_type)


encoding.encode = encode
registration.decode = decode
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
            rows = [[timestamp_capture.encode_value(row, index, field.dataType, types)
                     for index, field in enumerate(frame.schema.fields)]
                    for row in frame._jdf.queryExecution().executedPlan().executeCollect()]
            assert rows == item["rows"], rows
            readbacks.append({"name": item["name"], "schema": frame.schema.jsonValue(), "rows": rows})
        checked_sessions.add(identity)
    return {**case, **timestamp_capture.capture(session, case["sql"], types, Py4JError),
            "input_readbacks": readbacks}


capture_requested_queries(request, Path(sys.argv[1]),
                          [Path(__file__), request_path, source,
                           FIXTURES / "capture_window_name_stages_spark.py",
                           FIXTURES / "capture_floating_group_keys_spark.py"],
                          [request_path], capture_query=capture)
