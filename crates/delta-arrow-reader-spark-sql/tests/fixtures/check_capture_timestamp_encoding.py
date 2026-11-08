"""Check typed UTC timestamp output text without starting a Spark session."""

from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True

import capture_floating_group_keys_spark as codec
from pyspark import SparkContext
from pyspark.sql import SparkSession, types


def main():
    assert sys.version_info[:2] == (3, 12) and os.environ.get("TZ") == "UTC"
    assert Path(codec.__file__).resolve() == Path(__file__).resolve().with_name(
        "capture_floating_group_keys_spark.py")
    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert SparkSession._instantiatedSession is None and SparkSession._activeSession is None
    encode, timestamp = codec.encode, types.TimestampType()
    for value, expected in [
        (datetime(2020, 1, 2, 3, 4, 5), "2020-01-02T03:04:05Z"),
        (datetime(2020, 1, 2, 3, 4, 5, 123000), "2020-01-02T03:04:05.123Z"),
        (datetime(2020, 1, 2, 3, 4, 5, 123456), "2020-01-02T03:04:05.123456Z"),
        (datetime(1969, 12, 31, 23, 59, 59, 999999), "1969-12-31T23:59:59.999999Z"),
        (datetime(2020, 1, 2, 5, 4, 5, 123456, timezone(timedelta(hours=2))),
         "2020-01-02T03:04:05.123456Z"),
    ]:
        assert encode(value, timestamp) == expected
    value = datetime(2020, 1, 2, 3, 4, 5, 123456)
    original = "2020-01-02 03:04:05.123456"
    assert encode(value, timestamp, input_value=True) == original
    assert encode(original, types.StringType()) == original
    assert encode(value, types.TimestampNTZType()) == original
    assert encode(date(2020, 1, 2), types.DateType()) == "2020-01-02"
    assert encode(None, timestamp) is None
    assert encode(None, timestamp, input_value=True) is None
    schema = types.StructType([
        types.StructField("time", timestamp),
        types.StructField("literal", types.StringType()),
        types.StructField("flags", types.ArrayType(types.BooleanType())),
    ])
    row = (value, original, [True, None])
    assert encode(row, schema) == ["2020-01-02T03:04:05.123456Z", original, ["True", None]]
    assert encode(row, schema, input_value=True) == [original, original, ["True", None]]
    assert encode(False, types.BooleanType()) == "False"
    assert encode(False, types.BooleanType(), input_value=True) == "False"
    assert encode(-0.0, types.FloatType()) == "80000000"
    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert SparkSession._instantiatedSession is None and SparkSession._activeSession is None
    print(json.dumps({"timestamp_codec_check": "passed", "spark_session_started": False}))


if __name__ == "__main__":
    main()
