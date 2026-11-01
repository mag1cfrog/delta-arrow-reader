"""Capture mixed DATE/LTZ/NTZ comparisons and original NULLIF results.

Use the Python 3.12 / Java 21 environment in README.md. Registered-table rows
also cover an Arrow UTC annotation that differs from the session timezone.
"""
import datetime
import json
import os
from pathlib import Path
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, TimestampType, TimestampNTZType

SCENARIOS = [
    ("UTC", "midnight", "2026-01-02 00:00:00"),
    ("UTC", "pre_epoch", "1969-12-31 23:59:59.999999"),
    ("America/Phoenix", "midnight", "2026-01-02 00:00:00"),
    ("America/Phoenix", "fraction", "2026-01-02 03:04:05.123456"),
    ("+05:45", "offset_positive", "2026-01-02 00:00:00"),
    ("-07:00", "offset_negative", "2026-01-02 00:00:00"),
    ("America/New_York", "spring_gap", "2026-03-08 02:30:00.123456"),
    ("America/New_York", "fall_overlap", "2026-11-01 01:30:00.123456"),
    ("Pacific/Apia", "skipped_date", "2011-12-30 00:00:00"),
]

def temporal_comparison_queries(text):
    for pair in [("DATE", "TIMESTAMP"), ("DATE", "TIMESTAMP_NTZ"), ("TIMESTAMP_NTZ", "TIMESTAMP")]:
        for left_type, right_type in [pair, pair[::-1]]:
            def literal(data_type):
                return f"CAST('{text[:10] if data_type == 'DATE' else text}' AS {data_type})"
            left, right = literal(left_type), literal(right_type)
            left_null, right_null = f"CAST(NULL AS {left_type})", f"CAST(NULL AS {right_type})"
            name = left_type.lower() + "_" + right_type.lower()
            for label, expression in [
                ("equal", f"{left}={right}"),
                ("nullif", f"NULLIF({left},{right})"),
                ("null_left", f"NULLIF({left_null},{right})"),
                ("null_right", f"NULLIF({left},{right_null})"),
            ]:
                yield name + "_" + label, f"SELECT {expression} AS r"
            source = (f" FROM VALUES (0,{left},{right}),(1,{left_null},{right}),"
                      f"(2,{left},{right_null}),(3,{left_null},{right_null}) t(k,a,b)")
            for label, expression in [("equal", "a=b"), ("less", "a<b"),
                                      ("nullsafe", "a<=>b"), ("nullif", "NULLIF(a,b)")]:
                yield name + "_" + label + "_columns", f"SELECT {expression} AS r" + source + " ORDER BY k"
            yield name + "_nullif_empty", "SELECT NULLIF(a,b) AS r" + source + " WHERE false"
            yield name + "_between", "SELECT k AS r" + source + " WHERE a BETWEEN b AND b ORDER BY k"
            yield name + "_join_on", f"SELECT s.k AS r FROM VALUES (0,{left}),(1,{left_null}) s(k,a) JOIN VALUES ({right}) u(b) ON s.a=u.b ORDER BY s.k"
            yield name + "_join_using", f"SELECT s.k AS r FROM VALUES (0,{left}),(1,{left_null}) s(k,v) JOIN VALUES ({right}) u(v) USING(v) ORDER BY s.k"
    for label, sql in [
        ("bad_right_unused", "SELECT NULLIF(CAST(NULL AS DATE),CAST('bad' AS TIMESTAMP)) AS r"),
        ("bad_left_required", "SELECT NULLIF(CAST('bad' AS TIMESTAMP),CAST(NULL AS DATE)) AS r"),
        ("incomparable", "SELECT NULLIF(DATE '2026-01-02',ARRAY(1)) AS r"),
    ]:
        yield label, sql

def encode_reference_value(value, data_type):
    if isinstance(value, datetime.datetime):
        if data_type == "timestamp":
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        elapsed = value - datetime.datetime(1970,1,1)
        return str((elapsed.days * 86400 + elapsed.seconds) * 1000000 + elapsed.microseconds)
    if isinstance(value,bool):
        return str(value).lower()
    return None if value is None else str(value)

def registered_table_queries():
    for name, left, right in [("date", "d", "ltz"), ("ntz", "ntz", "ltz")]:
        for reverse in [False, True]:
            first, second = (right, left) if reverse else (left, right)
            for operation in ["equal", "nullif"]:
                expression = f"{first}={second}" if operation == "equal" else f"NULLIF({first},{second})"
                yield f"external_{name}_{reverse}_{operation}", f"SELECT {expression} AS r FROM external_temporal ORDER BY k", operation == "nullif" and first == "ltz"


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    spark = (SparkSession.builder.master("local[1]").appName("dar-temporal-comparisons")
             .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","1").getOrCreate())
    records = []
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for timezone, scenario, text in SCENARIOS:
            spark.conf.set("spark.sql.session.timeZone",timezone)
            for ansi in [True,False]:
                spark.conf.set("spark.sql.ansi.enabled",str(ansi).lower())
                for name,sql in temporal_comparison_queries(text):
                    try:
                        frame=spark.sql(sql)
                        schema=frame.schema.jsonValue()
                        rows=[[encode_reference_value(row[0],schema["fields"][0]["type"])] for row in frame.collect()]
                        expected={"status":"ok","schema":schema,"rows":rows}
                    except Exception as error:
                        condition=getattr(error,"getCondition",lambda:None)()
                        if condition not in ["CAST_INVALID_INPUT","DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES"]:
                            raise
                        expected={"status":"error","condition":condition}
                    records.append({"id":scenario+"_"+name,"sql":sql,"ansi":ansi,"timezone":timezone,"expected":expected})
        epoch = datetime.datetime(1970, 1, 1)
        wall = datetime.datetime(2026, 1, 2)
        wall_us = int((wall - epoch).total_seconds()) * 1000000
        date_days = (wall.date() - epoch.date()).days
        external_input = {"date_days": [date_days, date_days, None, date_days],
                          "ntz_us": [wall_us, wall_us, None, wall_us],
                          "ltz_utc_us": [wall_us + 7 * 3600 * 1000000, wall_us, None, wall_us + 5 * 3600 * 1000000]}
        rows = [(k, None if days is None else epoch.date() + datetime.timedelta(days=days),
                 None if ntz is None else epoch + datetime.timedelta(microseconds=ntz),
                 None if ltz is None else (epoch + datetime.timedelta(microseconds=ltz)).replace(tzinfo=datetime.timezone.utc))
                for k, (days, ntz, ltz) in enumerate(zip(external_input["date_days"], external_input["ntz_us"], external_input["ltz_utc_us"]))]
        schema = StructType([StructField("k", IntegerType(), False), StructField("d", DateType(), True),
                             StructField("ntz", TimestampNTZType(), True), StructField("ltz", TimestampType(), True)])
        spark.createDataFrame(rows, schema).createOrReplaceTempView("external_temporal")
        for timezone in ["UTC", "America/Phoenix", "+05:45", "America/New_York"]:
            spark.conf.set("spark.sql.session.timeZone", timezone)
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                for name, sql, original_ltz in registered_table_queries():
                    frame = spark.sql(sql)
                    schema = frame.schema.jsonValue()
                    values = [[encode_reference_value(row[0], schema["fields"][0]["type"])] for row in frame.collect()]
                    record = {"id": name, "sql": sql, "ansi": ansi, "timezone": timezone,
                              "expected": {"status": "ok", "schema": schema, "rows": values}}
                    if original_ltz:
                        record["result_timezone"] = "UTC"
                    records.append(record)
        assert len({(r["id"], r["ansi"], r["timezone"]) for r in records}) == len(records)
        metadata={"external_input": external_input, "spark_version":spark.version,"java_version":spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),"timestamp_encoding":"microseconds since 1970-01-01; TIMESTAMP in UTC, TIMESTAMP_NTZ in wall time"}
        Path(__file__).with_name('temporal-comparison-spark.jsonl').write_text('\n'.join(json.dumps(r,separators=(',',':')) for r in [metadata,*records])+'\n')
        print(json.dumps({"observations":len(records),"errors":sum(r['expected']['status']=='error' for r in records)}))
    finally:
        spark.stop()

if __name__=='__main__':
    main()
