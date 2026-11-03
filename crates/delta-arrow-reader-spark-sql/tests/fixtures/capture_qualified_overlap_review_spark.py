"""Independent Spark references for overlapping database/table/column names."""

import argparse
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession, types


def capture(spark, sql):
    try:
        frame = spark.sql(sql)
        rows = [[None if value is None else str(value) for value in row]
                for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        return {"schema": frame.schema.jsonValue(), "rows": rows}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        if condition is None:
            raise
        return {"condition": condition}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="Fresh JSONL path; existing files are refused")
    args = parser.parse_args()
    if args.output.exists():
        parser.error(f"output already exists: {args.output}")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    with TemporaryDirectory(prefix="dar-qualified-overlap-review-") as owned:
        warehouse = Path(owned) / "warehouse"
        spark = (SparkSession.builder.master("local[1]")
                 .appName("dar-qualified-overlap-review")
                 .config("spark.ui.enabled", "false")
                 .config("spark.sql.catalogImplementation", "in-memory")
                 .config("spark.sql.warehouse.dir", warehouse.as_uri())
                 .config("spark.sql.shuffle.partitions", "1")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.caseSensitive", "false").getOrCreate())
        try:
            assert spark.version == "4.2.0", spark.version
            spark.sparkContext.setLogLevel("ERROR")
            schema = types.StructType([types.StructField(name, types.IntegerType(), True)
                                       for name in ["p", "id"]])
            rows = [(99, 7), (None, 8), (100, None)]
            spark.sql(f"CREATE DATABASE p LOCATION '{(warehouse / 'p').as_uri()}'")
            spark.createDataFrame(rows, schema).write.saveAsTable("p.p")
            records = [{"spark_version": spark.version, "timezone": "UTC",
                        "case_sensitive": False,
                        "inputs": [{"name": "p.p", "schema": schema.jsonValue(),
                                    "rows": [[None if v is None else str(v) for v in row]
                                             for row in rows]}]}]
            queries = [
                ("database_table_id", "SELECT p.p.id AS selected_id FROM p.p"),
                ("table_id", "SELECT p.id AS selected_id FROM p.p"),
                ("database_table_same_name", "SELECT p.p.p AS selected_id FROM p.p"),
                ("catalog_database_table_id", "SELECT spark_catalog.p.p.id AS selected_id FROM spark_catalog.p.p"),
                ("catalog_read_suffix", "SELECT p.p.id AS selected_id FROM spark_catalog.p.p"),
                ("alias_id", "SELECT q.id AS selected_id FROM p.p q"),
                ("alias_same_name", "SELECT p.p AS selected_id FROM p.p p"),
                ("alias_invalid_nested", "SELECT p.p.id AS selected_id FROM p.p p"),
                ("case", "SELECT P.P.ID AS selected_id FROM p.p"),
            ]
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for name, sql in queries:
                    records.append({"id": name, "input": "p.p", "ansi": ansi,
                                    "sql": sql, "expected": capture(spark, sql)})
            assert len(records) == 19
            assert len({(record["id"], record["ansi"]) for record in records[1:]}) == 18
            with args.output.open("x") as output:
                for record in records:
                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
            print(json.dumps({"observations": len(records) - 1,
                              "errors": sum("condition" in record["expected"]
                                            for record in records[1:])}))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
