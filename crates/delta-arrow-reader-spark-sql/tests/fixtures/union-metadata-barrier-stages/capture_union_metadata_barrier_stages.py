"""Independent Spark stage capture for one reviewed UNION metadata barrier."""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession, types


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.StructType):
        return [encode(item, field.dataType) for item, field in zip(value, data_type)]
    return str(value).lower() if isinstance(value, bool) else str(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    manifest_path = Path(__file__).with_name("manifest.json")
    manifest = json.loads(manifest_path.read_text())
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    assert "revision=" + manifest["spark_revision"] in build, build
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-union-metadata-barrier-review")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", manifest["timezone"])
             .getOrCreate())
    try:
        assert spark.version == manifest["spark_version"], spark.version
        spark.sparkContext.setLogLevel("ERROR")
        header = {key: manifest[key] for key in
                  ["spark_version", "spark_revision", "timezone", "inputs"]}
        header.update(spark_build=build,
                      manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                      capture_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        header.update(capture_settings=manifest["capture_settings"],
                      replay_settings=manifest["replay_settings"])
        records = [header]
        for provider in manifest["inputs"]:
            schema = types.StructType.fromJson(provider["schema"])
            values = [tuple(None if value is None else int(value) for value in row)
                      for row in provider["rows"]]
            assert [encode(row, schema) for row in values] == provider["rows"]
            spark.createDataFrame(values, schema).createOrReplaceTempView(provider["name"])
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for case in manifest["queries"]:
                frame = spark.sql(case["sql"])
                execution = frame._jdf.queryExecution()
                public = frame.schema.jsonValue()
                stages = {"analyzed": json.loads(execution.analyzed().schema().json()),
                          "optimized": json.loads(execution.optimizedPlan().schema().json()),
                          "executed_before_collect": json.loads(execution.executedPlan().schema().json())}
                rows = [encode(row, frame.schema) for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                stages["executed"] = json.loads(execution.executedPlan().schema().json())
                assert public == stages["analyzed"], case
                records.append({**case, "ansi": ansi,
                                "expected": {"schema": public, "rows": rows},
                                "schema_stages": stages,
                                "analyzed_plan": execution.analyzed().toString(),
                                "optimized_plan": execution.optimizedPlan().toString(),
                                "executed_plan": execution.executedPlan().toString()})
        assert len(records) == 3
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == 2
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records)-1, "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
