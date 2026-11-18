"""Capture XXHASH64 NULL STRUCT values with the maintained Spark fixture protocol."""

import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import types

import capture_window_name_stages_spark as windows
from capture_spark_requests import capture_requested_queries

base_decode = windows.decode


def decode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.ArrayType):
        return [decode(item, data_type.elementType) for item in value]
    return base_decode(value, data_type)

# Extend the maintained typed hooks; the capture records this driver's source hash.
windows.decode = decode

here = Path(__file__).resolve().parent
request = here / "requests.json"
capture_requested_queries(
    json.loads(request.read_text()), Path(sys.argv[1]),
    [Path(__file__), request], [request],
)
