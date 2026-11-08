"""Capture #1098 inputs with maintained stage, codec, identity and JVM helpers."""

import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True

import capture_date_range_747_spark as protocol
import capture_spark_requests as requests

HERE = Path(__file__).resolve().parent
REQUEST = HERE / "cast-admission-1098-cases.json"


def load_request():
    request = json.loads(REQUEST.read_text())
    assert request["version"] == 1 and request["issue"] == 1098
    assert request["spark_version"] == protocol.VERSION
    assert request["spark_revision"] == protocol.REVISION
    corpus, = request["corpora"]
    cases = corpus["cases"]
    assert len(cases) == request["expected_record_count"] == 30
    assert len({case["sql"] for case in cases}) == request["sql_source_count"] == 15
    assert len({(case["id"], case["ansi"]) for case in cases}) == 30
    assert all(type(case["ansi"]) is bool for case in cases)
    for case_id in {case["id"] for case in cases}:
        assert {case["ansi"] for case in cases if case["id"] == case_id} == {True, False}
    assert request["native"]["representation_ids"] == [0, 1, 2]
    assert request["native"]["batch_sizes"] == [1, 32]
    names = [provider["name"] for provider in corpus["metadata"]["inputs"]]
    assert names == ["substring_text", "substring_text_empty",
                     "floating_integer_required", "floating_integer_all_null_sliced"]
    originals = [case for case in cases if case["ownership"] == "original_cast_admission"]
    assert len(originals) == request["original_record_count"] == 8
    assert all(case["providers"] == names for case in originals)
    assert sum(len(case["providers"]) for case in cases) == request["expected_original_provider_receipts"] == 32
    assert not any("expected" in case for case in cases)
    return request


def capture_with_providers(spark, case, encode):
    receipts = []
    for name in case["providers"]:
        provider = dict(case, id=case["id"] + ":provider:" + name,
                        sql="SELECT * FROM " + name + " ORDER BY id",
                        ownership="provider_readback", providers=[])
        receipts.append(dict(name=name, observation=protocol.capture(spark, provider, encode)))
    observed = protocol.capture(spark, case, encode)
    observed["provider_readbacks"] = receipts
    failures = [item["name"] for item in receipts if item["observation"]["status"] != "ok"]
    if failures:
        observed["capture_failure"] = {"provider_readbacks_failed": failures}
    return observed


def check_provider_wrapper(request):
    from unittest.mock import patch
    case = request["corpora"][0]["cases"][0]
    for failed in [False, True]:
        statuses = [{"status": "ok"} for _ in case["providers"]]
        if failed:
            statuses[0] = {"status": "capture_failure"}
        statuses.append({"status": "error"})
        with patch.object(protocol, "capture", side_effect=statuses) as capture_mock:
            observed = capture_with_providers(None, case, None)
        assert capture_mock.call_count == 5
        assert [call.args[1]["sql"] for call in capture_mock.call_args_list] == [
            *["SELECT * FROM " + name + " ORDER BY id" for name in case["providers"]], case["sql"],
        ]
        assert len(observed["provider_readbacks"]) == 4
        assert ("capture_failure" in observed) == failed
        assert observed["status"] == "error"


def check_codecs(request):
    from copy import deepcopy
    import math
    import struct

    from capture_floating_group_keys_spark import encode
    from capture_window_name_stages_spark import decode
    from pyspark import SparkContext
    from pyspark.sql import SparkSession, types

    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert SparkSession._instantiatedSession is None and SparkSession._activeSession is None
    string, binary = types.StringType(), types.BinaryType()
    for literal in ["", "a\x00b", "00ff", "False", "NaN", "7ff8000000001234"]:
        assert decode(literal, string) == literal
        assert encode(literal, string) == literal
        assert encode(literal, string, input_value=True) == literal
    for text, literal in [("", b""), ("0001ff", b"\x00\x01\xff"), ("f09f92a9", b"\xf0\x9f\x92\xa9")]:
        assert decode(text, binary) == literal
        for value in [literal, bytearray(literal), memoryview(literal)]:
            assert encode(value, binary) == text
            assert encode(value, binary, input_value=True) == text
    for kind, zeroes, finite, nans in [
        (types.FloatType(), ["00000000", "80000000"], "3fa00000", ["7fc01234", "ffc04321"]),
        (types.DoubleType(), ["0000000000000000", "8000000000000000"],
         "3ff4000000000000", ["7ff8000000001234", "fff8000000004321"]),
    ]:
        for bits, sign in zip(zeroes, [1.0, -1.0]):
            value = decode(bits, kind)
            assert value == 0.0 and math.copysign(1.0, value) == sign
            assert encode(value, kind) == bits
            assert encode(value, kind, input_value=True) == bits
        assert decode(finite, kind) == 1.25
        assert encode(1.25, kind) == finite
        for bits in nans:
            value = decode(bits, kind)
            assert math.isnan(value)
            assert encode(value, kind) == "nan"
            assert encode(value, kind, input_value=True) == bits
    for kind in [string, binary, types.FloatType(), types.DoubleType()]:
        assert decode(None, kind) is None
        assert encode(None, kind) is None and encode(None, kind, input_value=True) is None
    for value, kind, error_type in [
        (1, string, AssertionError), ("0", binary, ValueError),
        ("00", types.FloatType(), struct.error),
    ]:
        try:
            decode(value, kind)
        except error_type:
            pass
        else:
            raise AssertionError(("invalid codec input accepted", value, kind))
    providers = request["corpora"][0]["metadata"]["inputs"]
    for provider in providers:
        schema = types.StructType.fromJson(provider["schema"])
        assert schema.jsonValue() == provider["schema"]
        decoded = [decode(row, schema) for row in provider["rows"]]
        assert [encode(row, schema, input_value=True) for row in decoded] == provider["rows"]
        expected = deepcopy(provider["rows"])
        if provider["name"] == "floating_integer_required":
            assert expected[6] == ["6", "7fc00000", "7ff8000000000000"]
            expected[6][1:] = ["nan", "nan"]
        assert [encode(row, schema) for row in decoded] == expected
    schema = types.StructType([
        types.StructField("literal", string, False, {"role": "literal"}),
        types.StructField("bytes", binary, False),
        types.StructField("f", types.FloatType(), False),
        types.StructField("d", types.DoubleType(), False),
    ])
    input_rows = [
        ["NaN", "0001ff", "80000000", "7ff8000000001234"],
        ["80000000", "", "ffc04321", "8000000000000000"],
    ]
    output_rows = [
        ["NaN", "0001ff", "80000000", "nan"],
        ["80000000", "", "nan", "8000000000000000"],
    ]
    values = [decode(row, schema) for row in input_rows]
    assert [encode(row, schema, input_value=True) for row in values] == input_rows
    assert [encode(row, schema) for row in values] == output_rows
    assert SparkContext._gateway is None and SparkContext._active_spark_context is None
    assert SparkSession._instantiatedSession is None and SparkSession._activeSession is None
    print(json.dumps({"cast_codec_self_check": "passed", "spark_session_started": False,
                      "checks": ["STRING literals", "BINARY literals/input/output",
                                 "signed zero", "noncanonical quiet NaN input payloads",
                                 "all original provider rows", "mixed provider round trips",
                                 "malformed input rejection"]}))


def check_capture_arguments(request):
    from unittest.mock import patch

    settings_before = dict(requests.SETTINGS)
    callback_before = requests.capture
    for settings, expected_error in [
        (None, "provide a fresh output path"),
        (request["settings"], "provide a fresh output path"),
        ([], "Spark settings overrides must be a dict"),
        ({"not.spark": "x"}, "Spark settings overrides must map spark.* keys to strings"),
        ({"spark.setting": True}, "Spark settings overrides must map spark.* keys to strings"),
    ]:
        try:
            requests.capture_requested_queries({}, None, [], [], spark_settings=settings)
        except AssertionError as error:
            assert str(error) == expected_error
        else:
            raise AssertionError("capture accepted invalid settings or absent output")
    assert "pyspark" not in sys.modules
    assert requests.SETTINGS == settings_before and requests.capture is callback_before
    with patch.object(sys, "argv", [__file__, str(HERE / "capture-not-run.jsonl")]), \
            patch.object(requests, "capture_requested_queries") as capture_mock:
        main()
    args, kwargs = capture_mock.call_args
    assert args[0] == request and args[1] == HERE / "capture-not-run.jsonl"
    assert kwargs == {"capture_query": capture_with_providers, "spark_settings": request["settings"]}
    assert requests.SETTINGS == settings_before and requests.capture is callback_before


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    assert sys.version_info[:2] == (3, 12), "use Python 3.12"
    request = load_request()
    if args.self_check:
        assert args.output is None and "pyspark" not in sys.modules
        check_provider_wrapper(request)
        check_capture_arguments(request)
        protocol.self_check(request)
        check_codecs(request)
        return
    requests.capture_requested_queries(
        request, args.output, [Path(__file__).resolve(), REQUEST, Path(protocol.__file__)], [REQUEST],
        capture_query=capture_with_providers, spark_settings=request["settings"],
    )


if __name__ == "__main__":
    main()
