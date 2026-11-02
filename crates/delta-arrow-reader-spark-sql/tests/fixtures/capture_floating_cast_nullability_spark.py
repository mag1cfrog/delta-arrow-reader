"""Capture string-to-floating schemas and NULL predicate evaluation with Spark 4.2.0."""
import json
import os
import struct
import sys
from pathlib import Path

from pyspark.sql import SparkSession, types


os.environ["PYSPARK_PYTHON"] = sys.executable
queries = []
ansi_only = set()
for kind in ["FLOAT", "DOUBLE"]:
    valid = "CASE WHEN id=0 THEN '-0.0' ELSE '1.5' END"
    invalid = "CASE WHEN id=0 THEN 'bad' ELSE '1.5' END"
    nullable = "CASE WHEN id=0 THEN NULL ELSE '1.5' END"
    for source_name, source in [("valid", valid), ("invalid", invalid), ("nullable", nullable)]:
        for cast in ["CAST", "TRY_CAST"]:
            expression = f"{cast}({source} AS {kind})"
            grouped = (f"SELECT {expression} AS r FROM range(3) GROUP BY {expression} "
                       f"HAVING {expression} IS NULL")
            cases = [
                ("projection", f"SELECT {expression} AS r FROM range(3)"),
                ("where", f"SELECT id AS r FROM range(3) WHERE {expression} IS NULL"),
                ("having", grouped),
                ("exists", f"SELECT EXISTS({grouped} LIMIT 1) AS r"),
                ("count", f"SELECT count(*) AS r FROM ({grouped} LIMIT 1) t"),
            ]
            for name, sql in cases:
                queries.append((f"{kind.lower()}_{cast.lower()}_{source_name}_{name}", sql))
    expression = f"CAST({invalid} AS {kind})"
    for name, sql in [
        ("literal", f"SELECT CAST('1.5' AS {kind}) AS r"),
        ("null_literal", f"SELECT CAST(NULL AS {kind}) AS r"),
        ("empty", f"SELECT {expression} AS r FROM range(0)"),
        ("limit_zero", f"SELECT {expression} AS r FROM range(3) LIMIT 0"),
        ("unreachable", f"SELECT CASE WHEN false THEN {expression} ELSE CAST(2 AS {kind}) END AS r FROM range(3)"),
    ]:
        queries.append((f"{kind.lower()}_{name}", sql))
    for source_name, source in [("valid", valid), ("invalid", invalid)]:
        # Legacy COALESCE selects STRING and has no string-to-floating conversion.
        ansi_only.add(f"{kind.lower()}_implicit_coalesce_{source_name}")
        queries.extend([
            (f"{kind.lower()}_implicit_comparison_{source_name}",
             f"SELECT ({source}) = CAST(2 AS {kind}) AS r FROM range(3)"),
            (f"{kind.lower()}_implicit_coalesce_{source_name}",
             f"SELECT COALESCE({source},CAST(2 AS {kind})) AS r FROM range(3)"),
        ])
    for name, expression in [
        ("null_safe_decimal", f"CAST(CAST({valid} AS {kind}) <=> CAST(0 AS DECIMAL(10,7)) AS INT)"),
        ("null_safe_invalid", f"CAST({invalid} AS {kind}) <=> 0D"),
        ("null_safe_is_null", f"(CAST({invalid} AS {kind}) <=> 0D) IS NULL"),
        ("equal_is_null", f"(CAST({invalid} AS {kind}) = 0D) IS NULL"),
        ("null_safe_null", f"NOT(CAST({nullable} AS {kind}) <=> NULL)"),
    ]:
        queries.append((f"{kind.lower()}_{name}", f"SELECT {expression} AS r FROM range(3)"))
assert len({name for name, _ in queries}) == len(queries)


def encode(value, data_type):
    if value is None:
        return None
    if isinstance(data_type, (types.FloatType, types.DoubleType)):
        return struct.pack("!f" if isinstance(data_type, types.FloatType) else "!d", value).hex()
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


spark = (SparkSession.builder.master("local[1]").appName("dar-floating-cast-nullability")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    assert spark.version == "4.2.0", spark.version
    spark.sparkContext.setLogLevel("ERROR")
    records = [{"spark_version": spark.version, "timezone": "UTC"}]
    for ansi in [True, False]:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
        for name, sql in queries:
            if not ansi and name in ansi_only:
                continue
            try:
                frame = spark.sql(sql)
                rows = [[encode(value, field.dataType) for value, field in zip(row, frame.schema)]
                        for row in frame.collect()]
                rows.sort(key=lambda row: json.dumps(row, separators=(",", ":")))
                expected = {"schema": frame.schema.jsonValue(), "rows": rows}
            except Exception as error:
                message = str(error)
                # Distributed Spark failures may wrap the structured JVM exception.
                assert "[CAST_INVALID_INPUT]" in message, message
                expected = {"condition": "CAST_INVALID_INPUT"}
            records.append({"id": name, "ansi": ansi, "sql": sql, "expected": expected})
    Path(__file__).with_name("floating-cast-nullability-spark.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records))
    print(json.dumps({"observations": len(records) - 1}))
finally:
    spark.stop()
