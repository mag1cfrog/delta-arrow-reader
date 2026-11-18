"""Capture nested HASH values with the maintained Spark fixture protocol."""

import decimal
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import types

import capture_floating_group_keys_spark as floating
import capture_window_name_stages_spark as windows
from capture_spark_requests import capture_requested_queries

base_encode = floating.encode
base_decode = windows.decode


def decode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.ArrayType):
        return [decode(item, data_type.elementType) for item in value]
    if isinstance(data_type, types.DecimalType):
        return decimal.Decimal(value)
    return base_decode(value, data_type)


def encode(value, data_type, *, input_value=False):
    if value is None:
        return None
    if input_value and isinstance(data_type, types.DecimalType):
        return format(value, "f")
    return base_encode(value, data_type, input_value=input_value)


# Extend the maintained typed hooks; the capture records this driver's source hash.
floating.encode = encode
windows.decode = decode

here = Path(__file__).resolve().parent
request = here / "requests.json"
capture_requested_queries(
    json.loads(request.read_text()), Path(sys.argv[1]),
    [Path(__file__), request], [request],
)
