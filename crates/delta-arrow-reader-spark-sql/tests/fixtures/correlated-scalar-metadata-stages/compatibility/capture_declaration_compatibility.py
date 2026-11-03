"""Capture valid nested nullability and MAP controls without normalizing metadata."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh JSONL output path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    manifest_path = ROOT / "inputs-and-cases.json"
    manifest = json.loads(manifest_path.read_text())
    assert len(manifest["cases"]) == manifest["observations"] == 8
    assert len({(case["id"], case["input"], case["ansi"])
                for case in manifest["cases"]}) == 8
    for source in manifest["source_files"]:
        data = (ROOT / source["path"]).read_bytes()
        assert len(data) == source["bytes"], source["path"]
        assert hashlib.sha256(data).hexdigest() == source["sha256"], source["path"]

    import pyspark
    from pyspark.sql import SparkSession, types

    sys.path.insert(0, str(ROOT / "sources"))
    from capture_correlated_scalar_metadata_stages import capture_case, decode
    from capture_floating_group_keys_spark import encode as encode_leaf

    def encode(value, data_type, *, input_value=False):
        if value is None:
            return None
        if isinstance(data_type, types.MapType):
            return [[encode(key, data_type.keyType, input_value=input_value),
                     encode(item, data_type.valueType, input_value=input_value)]
                    for key, item in value.items()]
        if isinstance(data_type, types.StructType):
            return [encode(item, field.dataType, input_value=input_value)
                    for item, field in zip(value, data_type)]
        if isinstance(data_type, types.ArrayType):
            return [encode(item, data_type.elementType, input_value=input_value)
                    for item in value]
        return encode_leaf(value, data_type, input_value=input_value)

    map_type = types.MapType(types.StringType(), types.StructType([
        types.StructField("renamed", types.IntegerType(), True)]), False)
    assert encode({"k": (-2,)}, map_type) == [["k", ["-2"]]]
    assert encode({"k": (None,)}, map_type) == [["k", [None]]]
    assert encode(None, map_type) is None

    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert f"revision={manifest['spark_revision']}" in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-818-declaration-compatibility")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == manifest["spark_version"]
        assert spark.conf.get("spark.sql.session.timeZone") == manifest["timezone"]
        spark.sparkContext.setLogLevel("ERROR")
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            values = [decode(row, schema, types) for row in provider["rows"]]
            assert [encode(row, schema, input_value=True) for row in values] == provider["rows"]
            spark.createDataFrame(values, schema).createOrReplaceTempView(provider["name"])
        calls = spark.sparkContext.accumulator(0)
        records = [{key: value for key, value in manifest.items() if key != "cases"}]
        records[0].update({"spark_build": build,
                           "capture_script_sha256": hashlib.sha256(
                               Path(__file__).read_bytes()).hexdigest(),
                           "capture_manifest_sha256": hashlib.sha256(
                               manifest_path.read_bytes()).hexdigest()})
        for case in manifest["cases"]:
            spark.conf.set("spark.sql.ansi.enabled", str(case["ansi"]).lower())
            records.append(capture_case(spark, case, calls, encode))
        assert len(records) == 9
        assert all(("expected" in record) != ("error" in record) for record in records[1:])
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": 8, "output": str(args.output),
                          "errors": sum("error" in record for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
