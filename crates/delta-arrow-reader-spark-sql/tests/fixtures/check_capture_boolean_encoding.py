"""Check Boolean capture text against literals without starting a Spark session."""

import json
from pathlib import Path
import struct
import sys

sys.dont_write_bytecode = True

import capture_floating_group_keys_spark as codec
from pyspark import SparkContext
from pyspark.sql import SparkSession, types


def main():
    assert sys.version_info[:2] == (3, 12), "use Python 3.12"
    assert Path(codec.__file__).resolve() == Path(__file__).resolve().with_name(
        "capture_floating_group_keys_spark.py")
    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert SparkSession._instantiatedSession is None and SparkSession._activeSession is None
    encode = codec.encode
    boolean, string = types.BooleanType(), types.StringType()
    assert encode(True, boolean) == "true"
    assert encode(False, boolean) == "false"
    assert encode(None, boolean) is None
    assert encode(True, boolean, input_value=True) == "True"
    assert encode(False, boolean, input_value=True) == "False"
    assert encode(None, boolean, input_value=True) is None
    assert encode("True", string) == "True"
    assert encode("False", string) == "False"
    assert encode("True", string, input_value=True) == "True"
    assert encode("False", string, input_value=True) == "False"
    array = types.ArrayType(boolean)
    assert encode([True, False, None], array) == ["true", "false", None]
    assert encode([True, False, None], array, input_value=True) == ["True", "False", None]
    assert encode([], array) == [] and encode(None, array) is None
    child = types.StructType([types.StructField("flag", boolean, True)])
    schema = types.StructType([
        types.StructField("flag", boolean, False),
        types.StructField("text", string, False),
        types.StructField("children", types.ArrayType(child), True),
    ])
    value = (True, "False", [(False,), (None,), None])
    assert encode(value, schema) == ["true", "False", [["false"], [None], None]]
    assert encode(value, schema, input_value=True) == ["True", "False", [["False"], [None], None]]
    assert encode(None, schema) is None
    assert encode(-0.0, types.FloatType()) == "80000000"
    assert encode(0.0, types.FloatType()) == "00000000"
    assert encode(-0.0, types.DoubleType()) == "8000000000000000"
    assert encode(0.0, types.DoubleType()) == "0000000000000000"
    nan = struct.unpack("!d", bytes.fromhex("7ff8000000001234"))[0]
    assert encode(nan, types.DoubleType()) == "nan"
    assert encode(nan, types.DoubleType(), input_value=True) == "7ff8000000001234"
    assert encode(7, types.IntegerType()) == "7"
    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert SparkSession._instantiatedSession is None and SparkSession._activeSession is None
    print(json.dumps({"boolean_codec_self_check": "passed", "spark_session_started": False,
                      "checks": ["Boolean output/input", "literal strings", "NULL",
                                 "recursive arrays/structs", "float bits/NaN", "integer"]}))


if __name__ == "__main__":
    main()
