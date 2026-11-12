"""Capture clock-independent current-time expressions with Spark 4.2.0."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile


QUERIES = {
    "timestamp_interval_filter": "SELECT k FROM t WHERE ts > current_timestamp() - INTERVAL 7 DAYS ORDER BY k",
    "timestamp_date_filter": "SELECT k FROM t WHERE ts < current_date() ORDER BY k",
    "timestamp_filter": "SELECT k FROM t WHERE ts < current_timestamp() ORDER BY k",
    "date_filter": "SELECT k FROM t WHERE d < current_date() ORDER BY k",
    "reversed_interval_filter": "SELECT k FROM t WHERE current_timestamp() - INTERVAL 7 DAYS < ts ORDER BY k",
    "literal_timestamp_interval": "SELECT current_timestamp() - INTERVAL 7 DAYS > TIMESTAMP '2000-01-01 00:00:00' AS r",
    "literal_future_timestamp": "SELECT current_timestamp() + INTERVAL 7 DAYS < TIMESTAMP '2100-01-01 00:00:00' AS r",
    "literal_date": "SELECT current_date() > DATE '2000-01-01' AS r",
    "literal_date_interval": "SELECT current_date() + INTERVAL 7 DAYS < DATE '2100-01-01' AS r",
    "repeated_date": "SELECT current_date() = current_date() AS r, datediff(current_date(), current_date()) AS days FROM t",
    "repeated_timestamp": "SELECT current_timestamp() = current_timestamp() AS r, unix_micros(current_timestamp()) - unix_micros(current_timestamp()) AS micros FROM t",
    "repeated_unix_date": "SELECT unix_timestamp(current_date(), 'yyyy') - unix_timestamp(current_date(), 'yyyy') AS r FROM t",
    "repeated_to_unix_date": "SELECT to_unix_timestamp(current_date(), 'yyyy') - to_unix_timestamp(current_date(), 'yyyy') AS r FROM t",
    "repeated_unix_date_null_format": "SELECT unix_timestamp(current_date(), CAST(NULL AS STRING)) - unix_timestamp(current_date(), CAST(NULL AS STRING)) AS r FROM t",
    "repeated_unix_date_column_format": "SELECT k, unix_timestamp(current_date(), f) - unix_timestamp(current_date(), f) AS r FROM t ORDER BY k",
    "repeated_unix_timestamp": "SELECT unix_timestamp(current_timestamp(), 'yyyy') - unix_timestamp(current_timestamp(), 'yyyy') AS r FROM t",
    "repeated_to_unix_timestamp": "SELECT to_unix_timestamp(current_timestamp(), f) - to_unix_timestamp(current_timestamp(), f) AS r FROM t",
    "nested_current_date": "SELECT unix_timestamp(date_add(current_date(), 1), 'yyyy') - unix_timestamp(date_add(current_date(), 1), 'yyyy') AS r FROM t",
    "repeated_interval_timestamp": "SELECT current_timestamp() - INTERVAL 7 DAYS = current_timestamp() - INTERVAL 7 DAYS AS r FROM t",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert not args.output.exists()
    os.environ["PYSPARK_PYTHON"] = sys.executable

    import pyspark
    from pyspark.sql import SparkSession, types

    spark = (SparkSession.builder.master("local[1]")
             .appName("dar-current-time-expressions")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
        with ZipFile(jar) as archive:
            build = archive.read("spark-version-info.properties").decode()
        rows = [
            (1, datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc),
             datetime.date(2000, 1, 1), "yyyy"),
            (2, datetime.datetime(2100, 1, 1, tzinfo=datetime.timezone.utc),
             datetime.date(2100, 1, 1), "yyyy-MM-dd"),
            (3, None, None, None),
        ]
        schema = types.StructType([
            types.StructField("k", types.IntegerType(), False),
            types.StructField("ts", types.TimestampType()),
            types.StructField("d", types.DateType()),
            types.StructField("f", types.StringType()),
        ])
        spark.createDataFrame(rows, schema).createOrReplaceTempView("t")
        records = [{"spark_version": spark.version, "spark_build": build,
                    "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
                    "capture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "clock_independence": "Run between 2000-01-08 and 2099-12-24. Repeated calls use one query clock."}]
        for zone in ["UTC", "America/Los_Angeles"]:
            spark.conf.set("spark.sql.session.timeZone", zone)
            assert spark.sql("SELECT current_date() BETWEEN DATE '2000-01-08' AND DATE '2099-12-24'").first()[0]
            for ansi in [False, True]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for name, sql in QUERIES.items():
                    frame = spark.sql(sql)
                    values = [[None if v is None else str(v).lower() if isinstance(v, bool)
                               else str(v) for v in row] for row in frame.collect()]
                    executed_schema = json.loads(frame._jdf.queryExecution().executedPlan().schema().json())
                    records.append({"id": name, "sql": sql, "timezone": zone, "ansi": ansi,
                                    "types": [f["type"] for f in executed_schema["fields"]],
                                    "rows": values})
                for clock in ["2026-01-01T00:30:00Z", "2026-07-01T00:30:00Z"]:
                    reference_sql = (f"SELECT CAST(TIMESTAMP '{clock}' AS DATE) AS d, TIMESTAMP '{clock}' AS ts, "
                                     f"TIMESTAMP '2025-12-31T20:00:00Z' < CAST(TIMESTAMP '{clock}' AS DATE) AS before_date")
                    frame = spark.sql(reference_sql)
                    row, = frame.collect()
                    instant = datetime.datetime.fromisoformat(clock.replace("Z", "+00:00"))
                    epoch_day = (row[0] - datetime.date(1970, 1, 1)).days
                    assert row[1].replace(tzinfo=datetime.timezone.utc) == instant
                    executed_schema = json.loads(frame._jdf.queryExecution().executedPlan().schema().json())
                    records.append({"id": "fixed_query_clock", "clock": clock,
                                    "sql": "SELECT current_date() AS d, current_timestamp() AS ts, TIMESTAMP '2025-12-31T20:00:00Z' < current_date() AS before_date",
                                    "reference_sql": reference_sql,
                                    "timezone": zone, "ansi": ansi,
                                    "types": [f["type"] for f in executed_schema["fields"]],
                                    "rows": [[str(epoch_day), str(int(instant.timestamp() * 1_000_000)), str(row[2]).lower()]]})
        with args.output.open("x") as output:
            for record in records:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps({"observations": len(records) - 1, "output": str(args.output)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
