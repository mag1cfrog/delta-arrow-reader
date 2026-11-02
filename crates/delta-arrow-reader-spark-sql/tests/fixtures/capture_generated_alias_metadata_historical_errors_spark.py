"""Capture exact remaining historical error changes without replacing the originals."""
import argparse
import hashlib
import importlib.util
import json
import os
import sys
from pathlib import Path

QUERIES = [('replay/cast-scalar-cast-cases/194/later_live_error_true', 'SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN (SELECT 3L) WHEN true THEN (SELECT 7L DIV 0L) ELSE 7L END AS r'), ('replay/existing-div-null-cast-cases/49/tinyint_parent_null_true', "SELECT (CAST(NULL AS TINYINT) DIV CAST(v AS TINYINT)) * CAST(NULL AS BIGINT) AS q FROM VALUES ('bad') t(v)"), ('replay/existing-div-null-cast-cases/116/smallint_parent_null_true', "SELECT (CAST(NULL AS SMALLINT) DIV CAST(v AS SMALLINT)) * CAST(NULL AS BIGINT) AS q FROM VALUES ('bad') t(v)"), ('replay/existing-div-null-cast-cases/183/int_parent_null_true', "SELECT (CAST(NULL AS INT) DIV CAST(v AS INT)) * CAST(NULL AS BIGINT) AS q FROM VALUES ('bad') t(v)"), ('replay/existing-div-null-cast-cases/250/bigint_parent_null_true', "SELECT (CAST(NULL AS BIGINT) DIV CAST(v AS BIGINT)) * CAST(NULL AS BIGINT) AS q FROM VALUES ('bad') t(v)"), ('replay/existing-div-null-numeric-cases/63/tinyint_parent_null_true', "SELECT (CAST(NULL AS TINYINT) DIV CAST(v AS TINYINT)) * CAST(NULL AS BIGINT) AS q FROM VALUES (CAST('NaN' AS DOUBLE)) t(v)"), ('replay/existing-div-null-numeric-cases/137/smallint_parent_null_true', "SELECT (CAST(NULL AS SMALLINT) DIV CAST(v AS SMALLINT)) * CAST(NULL AS BIGINT) AS q FROM VALUES (CAST('NaN' AS DOUBLE)) t(v)"), ('replay/existing-div-null-numeric-cases/211/int_parent_null_true', "SELECT (CAST(NULL AS INT) DIV CAST(v AS INT)) * CAST(NULL AS BIGINT) AS q FROM VALUES (CAST('NaN' AS DOUBLE)) t(v)"), ('replay/existing-div-null-numeric-cases/283/bigint_parent_null_true', "SELECT (CAST(NULL AS BIGINT) DIV CAST(v AS BIGINT)) * CAST(NULL AS BIGINT) AS q FROM VALUES (CAST('NaN' AS DOUBLE)) t(v)"), ('replay/integer-cases/264/add_nested_error_null_true', 'SELECT (a + 1) + CAST(NULL AS INT) AS r FROM VALUES (2147483647,false),(0,true) t(a,keep)'), ('replay/integer-cases/281/sub_nested_error_null_true', 'SELECT (a - 1) - CAST(NULL AS INT) AS r FROM VALUES (-2147483648,false),(0,true) t(a,keep)'), ('replay/integer-cases/298/mul_nested_error_null_true', 'SELECT (a * 2) * CAST(NULL AS INT) AS r FROM VALUES (1073741824,false),(0,true) t(a,keep)')]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), args.output
    helper_path = Path(__file__).with_name("capture_generated_alias_metadata_spark_v2.py")
    spec = importlib.util.spec_from_file_location("alias_capture", helper_path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (helper.SparkSession.builder.master("local[1]")
             .appName("dar-generated-alias-historical-errors")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        records = [{"spark_version": spark.version, "timezone": "UTC", "inputs": [],
                    "capture_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "helper_script_sha256": hashlib.sha256(helper_path.read_bytes()).hexdigest()}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for label, sql in QUERIES:
                records.append({"id": label, "ansi": ansi, "sql": sql,
                                **helper.capture(spark, sql)})
        assert len(records) == 25
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1, "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
