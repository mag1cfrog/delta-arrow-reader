"""Capture ordinary nested SQL from source-only requests on pinned Spark."""

import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent
sys.path.insert(0, str(FIXTURES))
from pyspark.sql import types
import capture_floating_group_keys_spark as encoding
import capture_window_name_stages_spark as registration
from capture_spark_requests import capture_requested_queries

original_encode = encoding.encode
original_decode = registration.decode


def encode(value, data_type, *, input_value=False):
    if value is not None and isinstance(data_type, types.MapType):
        return [[encode(key, data_type.keyType, input_value=input_value),
                 encode(item, data_type.valueType, input_value=input_value)]
                for key, item in value.items()]
    return original_encode(value, data_type, input_value=input_value)


def decode(value, data_type):
    if value is not None and isinstance(data_type, types.ArrayType):
        return [decode(item, data_type.elementType) for item in value]
    if value is not None and isinstance(data_type, types.MapType):
        return {decode(key, data_type.keyType): decode(item, data_type.valueType)
                for key, item in value}
    return original_decode(value, data_type)


encoding.encode = encode
registration.decode = decode
request_path = HERE / "requests.json"
capture_requested_queries(
    json.loads(request_path.read_text()), Path(sys.argv[1]),
    [Path(__file__), request_path, FIXTURES / "capture_window_name_stages_spark.py",
     FIXTURES / "capture_floating_group_keys_spark.py"],
    [request_path],
)
