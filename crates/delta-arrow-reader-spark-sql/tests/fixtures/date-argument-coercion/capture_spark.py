"""Capture scalar date argument coercion from source-only SQL and input definitions."""

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

source = FIXTURES / "try-to-date/capture_try_to_date_spark.py"
spec = importlib.util.spec_from_file_location("date_capture", source)
date_capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(date_capture)
original_decode = registration.decode


def decode(value, data_type):
    if value is not None and isinstance(data_type, types.LongType):
        return int(value)
    if value is not None and isinstance(data_type, types.DecimalType):
        return Decimal(value)
    return original_decode(value, data_type)


registration.decode = decode


def input_value(row, index, data_type):
    if row.isNullAt(index):
        return None
    if isinstance(data_type, (types.FloatType, types.DoubleType)):
        code = "!f" if isinstance(data_type, types.FloatType) else "!d"
        value = row.getFloat(index) if isinstance(data_type, types.FloatType) else row.getDouble(index)
        return struct.pack(code, value).hex()
    if isinstance(data_type, types.DecimalType):
        return format(row.getDecimal(index, data_type.precision, data_type.scale)
                      .toJavaBigDecimal(), "f")
    if isinstance(data_type, types.BinaryType):
        return bytes(row.getBinary(index)).hex()
    if isinstance(data_type, types.BooleanType):
        return str(row.getBoolean(index))
    return date_capture.encode_value(row, index, data_type, types)


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
            rows = [[input_value(row, index, field.dataType)
                     for index, field in enumerate(frame.schema.fields)]
                    for row in frame._jdf.queryExecution().executedPlan().executeCollect()]
            assert rows == item["rows"], rows
            readbacks.append({"name": item["name"], "schema": frame.schema.jsonValue(), "rows": rows})
        checked_sessions.add(identity)
    return {**case, **date_capture.capture(session, case["sql"], types, Py4JError),
            "input_readbacks": readbacks}


capture_requested_queries(request, Path(sys.argv[1]),
                          [Path(__file__), request_path, source,
                           FIXTURES / "capture_window_name_stages_spark.py"],
                          [request_path], capture_query=capture)
