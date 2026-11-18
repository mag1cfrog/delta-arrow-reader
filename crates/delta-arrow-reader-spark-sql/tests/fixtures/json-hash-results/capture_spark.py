"""Capture ordinary JSON and hash results with the maintained Spark protocol."""

import datetime
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
    if isinstance(data_type, types.MapType):
        return {
            decode(key, data_type.keyType): decode(item, data_type.valueType)
            for key, item in value
        }
    if isinstance(data_type, types.LongType):
        return int(value)
    if isinstance(data_type, types.DecimalType):
        return decimal.Decimal(value)
    if isinstance(data_type, types.DateType):
        return datetime.date(1970, 1, 1) + datetime.timedelta(days=int(value))
    if isinstance(data_type, types.TimestampType):
        return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=int(value))
    return base_decode(value, data_type)


def encode(value, data_type, *, input_value=False):
    if value is None:
        return None
    if isinstance(data_type, types.MapType):
        return [
            [encode(key, data_type.keyType, input_value=input_value),
             encode(item, data_type.valueType, input_value=input_value)]
            for key, item in value.items()
        ]
    if input_value and isinstance(data_type, types.DateType):
        return str((value - datetime.date(1970, 1, 1)).days)
    if input_value and isinstance(data_type, types.TimestampType):
        delta = value - datetime.datetime(1970, 1, 1)
        return str((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds)
    return base_encode(value, data_type, input_value=input_value)


floating.encode = encode
windows.decode = decode

here = Path(__file__).resolve().parent
request = here / "requests.json"
capture_requested_queries(
    json.loads(request.read_text()),
    Path(sys.argv[1]),
    [Path(__file__), request],
    [request],
)
