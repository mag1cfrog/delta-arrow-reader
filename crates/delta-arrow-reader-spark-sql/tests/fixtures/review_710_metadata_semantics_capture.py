"""Independent Spark capture for issue 710 metadata and NULL/error semantics."""
import argparse
import hashlib
import importlib.util
import json
import os
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), f"Refusing to overwrite {args.output}"
    helper_path = Path(__file__).with_name("capture_generated_alias_metadata_spark_v2.py")
    helper_spec = importlib.util.spec_from_file_location("alias_capture", helper_path)
    helper = importlib.util.module_from_spec(helper_spec)
    helper_spec.loader.exec_module(helper)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (helper.SparkSession.builder.master("local[1]")
             .appName("dar-review-710-metadata-semantics")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    expressions = [
        ("null_scalar_add", "NULL + (SELECT 1)"),
        ("scalar_null_divide", "(SELECT 1)/CAST(NULL AS INT)"),
        ("round_scalar_null_scale", "ROUND((SELECT 1),NULL)"),
        ("bad_cast_add_null_cast", "CAST('bad' AS INT)+CAST(NULL AS INT)"),
        ("bad_cast_divide_null_cast", "CAST('bad' AS INT)/CAST(NULL AS INT)"),
        ("null_cast_add_bad_cast", "CAST(NULL AS INT)+CAST('bad' AS INT)"),
        ("null_cast_div_bad_cast", "CAST(NULL AS INT) DIV CAST('bad' AS INT)"),
        ("bad_cast_div_null_cast", "CAST('bad' AS INT) DIV CAST(NULL AS INT)"),
        ("null_cast_mod_bad_cast", "CAST(NULL AS INT)%CAST('bad' AS INT)"),
        ("case_dead_cast", "CASE WHEN false THEN CAST('bad' AS INT) ELSE 1 END"),
        ("coalesce_dead_cast", "COALESCE(CAST(1 AS INT),CAST('bad' AS INT))"),
        ("cast_round_null_scale", "ROUND(CAST('bad' AS DOUBLE),CAST(NULL AS INT))"),
    ]
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": [],
                    "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "helper_script_sha256": hashlib.sha256(helper_path.read_bytes()).hexdigest()}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for label, expression in expressions:
                for suffix, alias in [("generated", ""), ("explicit", " AS value")]:
                    sql = f"SELECT {expression}{alias}"
                    records.append({"id": f"{label}_{suffix}", "ansi": ansi, "sql": sql,
                                    **helper.capture(spark, sql)})
        assert len(records) == 49
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records)-1, "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
