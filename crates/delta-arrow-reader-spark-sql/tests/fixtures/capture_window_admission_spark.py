"""Capture Spark 4.2.0 window ordering, frame and DISTINCT admission."""
import json
import math
import os
from pathlib import Path
import struct
import sys

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, types


def window_expressions():
    row_prefix = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
    range_prefix = "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
    for name, function in [("row_number", "ROW_NUMBER()"), ("rank", "RANK()"),
                           ("dense_rank", "DENSE_RANK()"), ("percent_rank", "PERCENT_RANK()"),
                           ("ntile", "NTILE(2)"), ("cume_dist", "CUME_DIST()")]:
        for label, spec in [
            ("no_order", ""), ("partition_only", "PARTITION BY v"),
            ("default", "ORDER BY id"), ("rows", f"ORDER BY id {row_prefix}"),
            ("range", f"ORDER BY id {range_prefix}"),
            ("bounded", "ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"),
            ("all_rows", "ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),
            ("zero_preceding", "ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING"),
        ]:
            yield f"{name}_{label}", f"{function} OVER ({spec})", ""
        for label, frame in [("matching", range_prefix if name == "cume_dist" else row_prefix),
                             ("wrong", "ROWS BETWEEN CURRENT ROW AND CURRENT ROW")]:
            yield f"{name}_named_{label}", f"{function} OVER w", f"WINDOW w AS (ORDER BY id {frame})"
    for name in ["LAG", "LEAD"]:
        yield f"{name}_no_order", f"{name}(v) OVER ()", ""
        for offset in ["1", "2", "0", "-1", "1+1", "CAST(2 AS INT)"]:
            value = 2 if offset in ["1+1", "CAST(2 AS INT)"] else int(offset)
            direction = "PRECEDING" if (name == "LAG") == (value >= 0) else "FOLLOWING"
            bound = f"{abs(value)} {direction}"
            for label, frame in [("default", ""), ("exact", f"ROWS BETWEEN {bound} AND {bound}"),
                                 ("wrong", row_prefix)]:
                yield f"{name}_{offset}_{label}", f"{name}(v,{offset},-99) OVER (ORDER BY id {frame})", ""
        yield f"{name}_current", f"{name}(v,0) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW)", ""
        yield f"{name}_ignore_nulls", f"{name}(v) IGNORE NULLS OVER (ORDER BY id)", ""
        direction = "PRECEDING" if name == "LAG" else "FOLLOWING"
        yield f"{name}_named", f"{name}(v) OVER w", f"WINDOW w AS (ORDER BY id ROWS BETWEEN 1 {direction} AND 1 {direction})"
    for label, function, bound in [
        ("lead_folded", "LEAD(v,1+1)", "(1+1) FOLLOWING"),
        ("lead_cast", "LEAD(v,CAST(2 AS INT))", "CAST(2 AS INT) FOLLOWING"),
        ("lag_literal_negative", "LAG(v,1)", "-1 FOLLOWING"),
        ("lag_folded_negative", "LAG(v,1+1)", "-2 FOLLOWING"),
    ]:
        yield label, f"{function} OVER (ORDER BY id ROWS BETWEEN {bound} AND {bound})", ""
    for name, expression in [("nth_no_order", "NTH_VALUE(v,2) OVER ()"),
                              ("nth_default", "NTH_VALUE(v,2) OVER (ORDER BY id)"),
                              ("nth_bounded", "NTH_VALUE(v,2) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"),
                              ("range_no_order", "SUM(v) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW)"),
                              ("range_unbounded", "SUM(v) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"),
                              ("range_multiple_keys", "SUM(v) OVER (ORDER BY v,id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)"),
                              ("reverse_frame", "SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING)")]:
        yield name, expression, ""
    for name, function in [("sum", "SUM(v)"), ("count", "COUNT(v)"),
                           ("first", "FIRST_VALUE(v)"), ("last", "LAST_VALUE(v,true)"),
                           ("collect_set", "SORT_ARRAY(COLLECT_SET(v) OVER ())")]:
        if name == "collect_set":
            yield name, function, ""
            continue
        for label, spec in [("no_order", ""), ("default", "ORDER BY id"),
                            ("bounded", "ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING")]:
            yield f"aggregate_{name}_{label}", f"{function} OVER ({spec})", ""
    for function, argument in [("COUNT", "v"), ("SUM", "v"), ("AVG", "v"),
                               ("MIN", "v"), ("COUNT_IF", "v>0")]:
        for filtered in [False, True]:
            suffix = " FILTER (WHERE id>1)" if filtered else ""
            yield f"distinct_{function}_{filtered}", f"{function}(DISTINCT {argument}){suffix} OVER ()", ""
    for name, bounds in [("zero_start", "0 FOLLOWING AND CURRENT ROW"),
                         ("zero_end", "CURRENT ROW AND 0 PRECEDING"),
                         ("zero_pair", "0 FOLLOWING AND 0 PRECEDING"),
                         ("negative_following", "-1 FOLLOWING AND CURRENT ROW"),
                         ("negative_preceding", "CURRENT ROW AND -1 PRECEDING")]:
        yield name, f"SUM(v) OVER (ORDER BY id ROWS BETWEEN {bounds})", ""


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, types.DoubleType):
        return "nan" if math.isnan(value) else struct.pack("!d", value).hex()
    if isinstance(data_type, types.ArrayType):
        return [encode(item, data_type.elementType) for item in value]
    return str(value)


def capture_query(spark, sql):
    try:
        frame = spark.sql(sql)
        rows = [[encode(value, field.dataType) for value, field in zip(row, frame.schema)]
                for row in frame.collect()]
        rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
        return {"schema": frame.schema.jsonValue(), "rows": rows}
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        cause = getattr(error, "java_exception", None)
        while condition is None and cause is not None:
            try:
                condition = cause.getCondition()
            except Py4JError:
                pass
            cause = cause.getCause()
        assert condition is not None, (sql, str(error))
        return {"condition": condition}


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-window-admission")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        records = [{"spark_version": spark.version, "timezone": "UTC"}]
        for ansi in [True, False]:
            spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
            for empty in [False, True]:
                source = "(SELECT * FROM VALUES(1,2),(2,NULL),(3,4),(4,4) t(id,v)"
                source += " WHERE false) inputs" if empty else ") inputs"
                for name, expression, named_window in window_expressions():
                    sql = f"SELECT id,{expression} AS result FROM {source} {named_window}"
                    records.append({"id": name, "ansi": ansi, "empty": empty, "sql": sql,
                                    "expected": capture_query(spark, sql)})
        assert len({(r["id"], r["ansi"], r["empty"]) for r in records[1:]}) == len(records)-1
        Path(__file__).with_name("window-admission-spark.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
        print(json.dumps({"observations": len(records)-1,
                          "errors": sum("condition" in record["expected"] for record in records[1:])}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
