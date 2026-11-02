"""Independent admission and NULL/error controls supplementary to the frozen corpus."""
import json
import os
from pathlib import Path
import sys

from capture_date_day_arithmetic_spark import capture_query, encode
from pyspark.sql import SparkSession, types


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-date-day-admission")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([types.StructField("base", types.IntegerType(), True),
                                   types.StructField("days", types.StringType(), True)])
        rows = [(None, "bad"), (0, None), (0, " 1 "), (1, "-1")]
        spark.createDataFrame(rows, schema).createOrReplaceTempView("date_day_text")
        records = [{"spark_version": spark.version, "timezone": "UTC", "date_encoding": "unix_date",
                    "inputs": [{"name": "date_day_text", "schema": schema.jsonValue(),
                                "rows": [encode(row, schema) for row in rows]}]}]
        definitions = []
        for function in ["date_add", "dateadd", "date_sub"]:
            for label, first, second in [
                ("null_start_bad_days", "CAST(NULL AS DATE)", "'bad'"),
                ("bad_start_null_days", "'bad'", "CAST(NULL AS INT)"),
                ("typed_null_text_days", "DATE '1970-01-01'", "CAST(NULL AS STRING)"),
                ("typed_null_text_start", "CAST(NULL AS STRING)", "1"),
                ("computed_text_days", "DATE '1970-01-01'", "CAST(1 AS STRING)"),
                ("signed_text_days", "DATE '1970-01-01'", "'+1'"),
                ("padded_text_days", "DATE '1970-01-01'", "' 1 '"),
                ("decimal_text_days", "DATE '1970-01-01'", "'1.0'"),
            ]:
                definitions.append((f"{function}_{label}", f"SELECT {function}({first},{second}) AS r"))
            definitions.append((f"{function}_arity_4_literals",
                                f"SELECT {function}(DATE '1970-01-01',1,DATE '1970-01-01',1) AS r"))
            definitions.append((f"{function}_text_provider",
                                f"SELECT {function}(date_from_unix_date(base),days) AS r FROM date_day_text"))
            definitions.append((f"{function}_invalid_text_provider",
                                f"SELECT {function}(DATE '1970-01-01',days) AS r FROM date_day_text"))
        definitions.append(("unresolved_column", "SELECT missing AS r"))
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for name, sql in definitions:
                records.append({"id": name, "ansi": ansi, "sql": sql,
                                "expected": capture_query(spark, sql)})
        assert len({(r["id"], r["ansi"]) for r in records[1:]}) == len(records) - 1
        Path(__file__).with_name("date-day-admission-probes-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1,
                          "errors": sum("condition" in record["expected"] for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
