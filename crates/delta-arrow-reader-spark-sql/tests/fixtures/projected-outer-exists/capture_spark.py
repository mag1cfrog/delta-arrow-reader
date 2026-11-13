"""Capture projected outer EXISTS from portable SQL and input schemas."""

import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent
sys.path.insert(0, str(FIXTURES))
from pyspark.sql import types
import capture_window_name_stages_spark as registration
from capture_spark_requests import capture_requested_queries

original_decode = registration.decode


def decode_input(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.LongType):
        return int(value)
    if isinstance(data_type, types.ArrayType):
        return [decode_input(item, data_type.elementType) for item in value]
    return original_decode(value, data_type)


registration.decode = decode_input
request_path = HERE / "requests.json"
capture_requested_queries(
    json.loads(request_path.read_text()), Path(sys.argv[1]),
    [Path(__file__), request_path, FIXTURES / "capture_window_name_stages_spark.py"],
    [request_path],
)
