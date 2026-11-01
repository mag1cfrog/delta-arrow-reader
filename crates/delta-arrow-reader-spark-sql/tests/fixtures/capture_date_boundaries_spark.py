"""Capture Spark DATE string boundaries; run in the README reference environment."""
import json
import os
from pathlib import Path
import sys
from pyspark.sql import SparkSession

VALUES = [
    ("day", "2026-01-02"), ("year", "2026"), ("month", "2026-01"),
    ("iso", "2026-01-02T00:00:00-07:00"), ("iso_suffix", "2026-01-02Tgarbage"),
    ("iso_empty", "2026-01-02T"), ("lowercase", "2026-01-02t00:00:00"),
    ("short_day", "2026-1-2Tgarbage"), ("other_separator", "2026-01-02X00:00:00"),
    ("year_suffix", "2026T00:00:00"), ("month_suffix", "2026-01T00:00:00"),
    ("year_space", "2026 garbage"), ("month_space", "2026-01 garbage"),
    ("tab_suffix", "2026-01-02\tgarbage"), ("newline_suffix", "2026-01-02\ngarbage"),
    ("space_suffix", "2026-01-02 \tgarbage"),
    ("leading_space", " 2026-01-02"), ("trailing_space", "2026-01-02 "),
    ("outer_controls", "\x00\t2026-01-02\n\x1f"), ("del", "\x7f2026-01-02\x7f"),
    ("unicode_space", "\u00a02026-01-02\u00a0"), ("signed_year", "+2026-1-2T"),
    ("negative_year", "-0001-1-2T"), ("bad_date", "2026-02-30Tignored"),
    ("empty", ""), ("bad", "bad"),
]


def queries():
    for name, value in VALUES:
        encoded = value.encode().hex()
        text = f"decode(unhex('{encoded}'),'UTF-8')"
        for operator in ["CAST", "TRY_CAST", "TO_DATE"]:
            def expression(value):
                return f"TO_DATE({value})" if operator == "TO_DATE" else f"{operator}({value} AS DATE)"
            yield f"{name}_{operator}_literal", "SELECT " + expression(text) + " AS r"
            source = (f" FROM (SELECT k,decode(unhex(h),'UTF-8') AS s "
                      f"FROM VALUES (0,'{encoded}'),(1,CAST(NULL AS STRING)) t(k,h)) v")
            yield f"{name}_{operator}_columns", "SELECT " + expression("s") + " AS r" + source + " ORDER BY k"
            yield f"{name}_{operator}_empty", "SELECT " + expression("s") + " AS r" + source + " WHERE false"
        if value.isprintable():
            yield f"{name}_date_literal", f"SELECT DATE '{value}' AS r"
    for name, value in [("timestamp_upper", "2026-01-02T01:02:03.123456Z"),
                        ("timestamp_lower", "2026-01-02t01:02:03.123456Z"),
                        ("timestamp_partial", "2026-01"), ("timestamp_space", "2026-01-02 01:02:03")]:
        yield name, f"SELECT CAST(TIMESTAMP '{value}' AS STRING) AS r"


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-date-boundaries")
             .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate())
    records = []
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for name, sql in queries():
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                try:
                    frame = spark.sql(sql)
                    rows = [[row[0]] for row in frame.selectExpr("CAST(r AS STRING) AS r").collect()]
                    expected = {"status": "ok", "type": frame.schema.fields[0].dataType.typeName(), "rows": rows}
                except Exception as error:
                    condition = getattr(error, "getCondition", lambda: None)()
                    if condition not in ["CAST_INVALID_INPUT", "INVALID_TYPED_LITERAL", "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION"]:
                        raise
                    expected = {"status": "error", "condition": condition}
                record = {"id": name, "sql": sql, "ansi": ansi, "expected": expected}
                if name == "timestamp_lower":
                    record["retained_difference"] = "timestamp_separator_632"
                records.append(record)
        assert len({(r["id"], r["ansi"]) for r in records}) == len(records)
        metadata = {"spark_version": spark.version, "timezone": "UTC"}
        Path(__file__).with_name('date-boundaries-spark.jsonl').write_text(
            '\n'.join(json.dumps(record, separators=(',', ':')) for record in [metadata, *records])+'\n')
        print(json.dumps({"observations": len(records), "errors": sum(r["expected"]["status"] == "error" for r in records)}))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
