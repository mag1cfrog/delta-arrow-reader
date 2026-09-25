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

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import sys

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import pyarrow as pa
import pyarrow.parquet as pq
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
        "provenance": "Original synthetic data written and read by Apache Spark / Delta Lake; no reader fixture helper is used.",
        "license": "Apache-2.0 (same as this repository)",
        "generator": "../generate.py",
        "generation_command": "JAVA_HOME=/path/to/java-21 SPARK_LOCAL_IP=127.0.0.1 uv run --python 3.13.12 --script tests/reader/fixtures/external_writer/generate.py NEW_OUTPUT_DIRECTORY",
        "tools": {
            name: importlib.metadata.version(name)
            for name in ["pyspark", "delta-spark", "pyarrow", "pandas"]
        },
        "python": platform.python_version(),
        "java": spark.sparkContext._jvm.java.lang.System.getProperty(
            "java.runtime.version"
        ),
        "size_budget_bytes": 262144,
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
        actions = [
            json.loads(line)
            for log in sorted((table / "_delta_log").glob("*.json"))
            for line in log.read_text().splitlines()
        ]
        protocol = [action["protocol"] for action in actions if "protocol" in action][
            -1
        ]
        metadata = [action["metaData"] for action in actions if "metaData" in action][
            -1
        ]
        parquet_files = sorted(table.rglob("*.parquet"))
        groups = {
            str(path.relative_to(table)): pq.ParquetFile(path).metadata.num_row_groups
            for path in parquet_files
        }
        physical_rows = sum(
            pq.ParquetFile(path).metadata.num_rows for path in parquet_files
        )
        descriptors = [
            action["add"]["deletionVector"]
            for action in actions
            if "deletionVector" in action.get("add", {})
        ]
        if name == "partitioned":
            assert len(groups) >= 3 and max(groups.values()) > 1, groups
            assert expected.num_rows == 360
        elif name == "nested_mapping":
            assert metadata["configuration"]["delta.columnMapping.mode"] == "name"
            assert frame.schema["profile"].dataType.fieldNames() == ["score", "city"]
            assert expected.num_rows == 6
        else:
            assert descriptors and sum(d["cardinality"] for d in descriptors) == 3
            assert physical_rows == 12 and expected.num_rows == 9
            assert [row["id"] for row in expected.to_pylist()] == [
                1,
                3,
                4,
                6,
                7,
                8,
                9,
                10,
                11,
            ]
        # Hadoop checksum sidecars are not part of the Delta table format.
        for path in table.rglob("*.crc"):
            path.unlink()
        files = {
            str(path.relative_to(directory)): {
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in sorted(directory.rglob("*"))
            if path.is_file()
        }
        manifest["fixtures"].append(
            {
                "name": name,
                "snapshot_version": int(
                    DeltaTable.forPath(spark, str(table))
                    .history(1)
                    .select("version")
                    .first()[0]
                ),
                "protocol": protocol,
                "partition_columns": metadata["partitionColumns"],
                "schema": frame.schema.jsonValue(),
                "expected_schema_and_rows": "expected.arrow",
                "expected_row_count": expected.num_rows,
                "physical_row_count": physical_rows,
                "row_groups_per_file": groups,
                "deletion_vectors": descriptors,
                "files": files,
            }
        )

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

        manifest["data_bytes"] = sum(
            info["bytes"]
            for fixture in manifest["fixtures"]
            for info in fixture["files"].values()
        )
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        actual = sum(
            path.stat().st_size for path in output.rglob("*") if path.is_file()
        )
        assert actual <= manifest["size_budget_bytes"], actual
        print(
            f"Generated {len(manifest['fixtures'])} fixtures, {actual} bytes including manifest."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
