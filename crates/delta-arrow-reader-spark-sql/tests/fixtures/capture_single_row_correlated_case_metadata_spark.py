"""Capture metadata after a scalar CASE simplifies to a source column."""

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types

from capture_single_row_correlated_spark import capture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="fresh output JSONL path")
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    metadata = json.loads(Path(__file__).with_name(
        "single-row-correlated-metadata-spark.jsonl").read_text().splitlines()[0])
    spark = (SparkSession.builder.master("local[1]").appName("dar-scalar-case-metadata")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for source in metadata["inputs"]:
            schema = types.StructType.fromJson(source["schema"])
            rows = [tuple(None if value is None else int(value) for value in row)
                    for row in source["rows"]]
            spark.createDataFrame(rows, schema).createOrReplaceTempView(source["name"])
        records = [metadata]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for source in metadata["inputs"]:
                table = source["name"]
                for cast in [False, True]:
                    suffix = ",CAST(7 AS INT) AS incidental" if cast else ""
                    sql = ("SELECT p.id AS direct_id,"
                           "(SELECT CASE WHEN true THEN p.id ELSE 0 END) AS scalar_id"
                           f"{suffix} FROM {table} p")
                    records.append({"id": f"{table}_case_{'cast' if cast else 'bare'}",
                                    "ansi": ansi, "sql": sql,
                                    "expected": capture(spark, sql)})
        assert len(records) == 13
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == 12
        with args.output.open("x") as output:
            output.write("".join(json.dumps(record, separators=(",", ":")) + "\n"
                                 for record in records))
        print(json.dumps({"observations": len(records)-1}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
