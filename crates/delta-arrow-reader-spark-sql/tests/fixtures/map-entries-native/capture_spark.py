"""Capture MAP_ENTRIES values and executed types from Spark 4.2.0."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession


SQL = "SELECT id, map_entries(m) AS value FROM bench ORDER BY id"
INPUTS = [
    [0, None],
    [1, [["a", 4], ["b", None]]],
    [2, []],
    [3, [["c", 9]]],
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("map-entries-reference")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        assert spark.version == "4.2.0", spark.version
        rows = []
        for id_, entries in INPUTS:
            if entries is None:
                value = "NULL"
            else:
                arguments = [f"'{key}', {'NULL' if number is None else number}"
                             for key, number in entries]
                value = f"map({', '.join(arguments)})"
            rows.append(f"(CAST({id_} AS BIGINT), CAST({value} AS MAP<STRING,BIGINT>))")
        spark.sql(f"CREATE OR REPLACE TEMP VIEW bench AS VALUES {', '.join(rows)} AS t(id, m)")
        build = spark.sql("SELECT version()").first()[0]
        observations = []
        for ansi in (False, True):
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            frame = spark.sql(SQL)
            result = [
                [str(row.id), None if row.value is None else [
                    [entry.key, None if entry.value is None else str(entry.value)]
                    for entry in row.value
                ]]
                for row in frame.collect()
            ]
            executed = json.loads(frame._jdf.queryExecution().executedPlan().schema().json())
            observations.append({"ansi": ansi, "executed_schema": executed, "rows": result})
        capture = {
            "spark_version": spark.version, "spark_build": build, "sql": SQL,
            "inputs": INPUTS, "observations": observations,
        }
        with args.output.open("x", encoding="utf-8") as out:
            json.dump(capture, out, indent=2)
            out.write("\n")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
