# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pyspark==4.0.1",
#   "delta-spark==4.0.0",
#   "pyarrow==20.0.0",
#   "pandas==2.2.3",
# ]
# ///
"""Generate the small Spark/Delta corpus into a new directory, outside normal CI."""

import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import pyarrow as pa
from pyspark.sql import SparkSession


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: generate.py NEW_OUTPUT_DIRECTORY")
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("delta-arrow-reader-external-fixtures")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.parquet.block.size", "128")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    manifest = {
        "tools": {
            name: importlib.metadata.version(name)
            for name in ["pyspark", "delta-spark", "pyarrow", "pandas"]
        },
        "python": platform.python_version(),
        "java": spark.sparkContext._jvm.java.lang.System.getProperty(
            "java.runtime.version"
        ),
        "fixtures": [],
    }

    def save_oracle(name):
        directory = output / name
        table = directory / "table"
        frame = spark.read.format("delta").load(str(table))
        expected = frame.orderBy("id").toArrow()
        oracle = directory / "expected.arrow"
        with pa.OSFile(str(oracle), "wb") as sink:
            with pa.ipc.new_file(sink, expected.schema) as writer:
                writer.write_table(expected)
        manifest["fixtures"].append({
            "name": name,
            "snapshot_version": int(DeltaTable.forPath(spark, str(table)).history(1).select("version").first()[0]),
            "schema": frame.schema.jsonValue(),
        })

    try:
        rows = [
            (
                i,
                None if i % 4 == 0 else i * 3,
                None if i % 5 == 0 else f"value-{i}",
                [None, "east", "west"][i % 3],
            )
            for i in range(1, 361)
        ]
        frame = spark.createDataFrame(
            rows, "id int, value int, label string, region string"
        )
        (
            frame.coalesce(1)
            .sortWithinPartitions("id")
            .write.format("delta")
            .partitionBy("region")
            .save(str(output / "partitioned/table"))
        )
        save_oracle("partitioned")

        rows = [
            (1, 10, (3, "Paris")),
            (2, None, (None, "")),
            (3, 30, None),
            (4, 40, (7, None)),
            (5, None, (None, None)),
            (6, 60, (9, "Tokyo")),
        ]
        frame = spark.createDataFrame(
            rows, "id int, value int, profile struct<score:int,old_city:string>"
        )
        path = output / "nested_mapping/table"
        (
            frame.coalesce(1)
            .write.format("delta")
            .option("delta.columnMapping.mode", "name")
            .save(str(path))
        )
        quoted_path = str(path).replace("`", "``")
        spark.sql(
            f"ALTER TABLE delta.`{quoted_path}` RENAME COLUMN profile.old_city TO city"
        )
        save_oracle("nested_mapping")

        rows = [
            (i, None if i % 4 == 0 else i * 3, None if i % 5 == 0 else f"value-{i}")
            for i in range(1, 13)
        ]
        frame = spark.createDataFrame(rows, "id int, value int, label string")
        path = output / "deletion_vectors/table"
        (
            frame.coalesce(1)
            .sortWithinPartitions("id")
            .write.format("delta")
            .option("delta.enableDeletionVectors", "true")
            .save(str(path))
        )
        DeltaTable.forPath(spark, str(path)).delete("id IN (2, 5, 12)")
        save_oracle("deletion_vectors")

        (output / "writer.json").write_text(json.dumps(manifest) + "\n", encoding="utf-8")
    finally:
        spark.stop()
    subprocess.run(
        [os.environ.get("CARGO", "cargo"), "run", "--locked", "-p", "xtask", "--",
         "finalize-external-fixtures", str(output)],
        cwd=Path(__file__).resolve().parents[6], check=True,
    )


if __name__ == "__main__":
    main()
