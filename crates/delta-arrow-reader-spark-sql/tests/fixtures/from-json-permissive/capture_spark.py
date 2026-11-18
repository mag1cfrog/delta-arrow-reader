"""Capture default PERMISSIVE FROM_JSON values with the maintained Spark protocol."""

import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from pyspark.sql import types
import capture_floating_group_keys_spark as floating
from capture_spark_requests import capture_requested_queries

base_encode = floating.encode

def encode(value, data_type, *, input_value=False):
    if value is None:
        return None
    if isinstance(data_type, types.MapType):
        return [[encode(key, data_type.keyType, input_value=input_value),
                 encode(item, data_type.valueType, input_value=input_value)]
                for key, item in value.items()]
    return base_encode(value, data_type, input_value=input_value)

floating.encode = encode
request = HERE / "requests.json"
capture_requested_queries(json.loads(request.read_text()), Path(sys.argv[1]),
                          [Path(__file__), request], [request])
