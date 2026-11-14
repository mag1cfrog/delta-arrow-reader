"""Capture numeric epoch conversions using raw Spark internal timestamp values."""

from decimal import Decimal
import importlib.util
import json
from pathlib import Path
import struct
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent
sys.path.insert(0, str(FIXTURES))
from pyspark.sql import types
from py4j.protocol import Py4JError
import capture_window_name_stages_spark as registration
from capture_spark_requests import capture_requested_queries

source = FIXTURES / "timestamp-from-epoch/capture_timestamp_from_epoch_spark.py"
spec = importlib.util.spec_from_file_location("epoch_capture", source)
epoch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(epoch)
original_decode = registration.decode


def decode(value, data_type):
    if value is not None and isinstance(data_type, types.LongType):
        return int(value)
    if value is not None and isinstance(data_type, types.DecimalType):
        return Decimal(value)
    return original_decode(value, data_type)


registration.decode = decode


def raw_value(row, index, data_type):
    if row.isNullAt(index):
        return None
    if isinstance(data_type, types.FloatType):
        return struct.pack("!f", row.getFloat(index)).hex()
    if isinstance(data_type, types.DecimalType):
        return format(row.getDecimal(index, data_type.precision, data_type.scale)
                      .toJavaBigDecimal(), "f")
    return epoch.encode_value(row, index, data_type, types)


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
            rows = [[raw_value(row, index, field.dataType)
                     for index, field in enumerate(frame.schema.fields)]
                    for row in frame._jdf.queryExecution().executedPlan().executeCollect()]
            assert rows == item["rows"], (item["name"], rows)
            readbacks.append({"name": item["name"], "schema": frame.schema.jsonValue(),
                              "rows": rows})
        checked_sessions.add(identity)
    return {**case, **epoch.capture(session, case["sql"], types, Py4JError),
            "input_readbacks": readbacks}


capture_requested_queries(
    request, Path(sys.argv[1]),
    [Path(__file__), request_path, source, FIXTURES / "capture_window_name_stages_spark.py"],
    [request_path], capture_query=capture,
)
