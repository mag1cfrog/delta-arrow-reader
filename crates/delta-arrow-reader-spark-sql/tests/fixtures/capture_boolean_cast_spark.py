"""Capture Spark STRING-to-BOOLEAN CAST and TRY_CAST results.

Use the Python/Java environment described in README.md. Hex-encoded literals
keep control bytes out of SQL source files while preserving the actual input.
"""

import json
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession


def boolean_cast_queries():
    tokens = ["true", "false", "t", "f", "yes", "no", "y", "n", "1", "0",
              "TRUE", "False", "tr", "tru", "ye", "fa", "fal", "fals", "of",
              "off", "on", "maybe", "", " true ", "\x7ftrue\x7f",
              "\x00true\x1f", "\u00a0true\u00a0"]
    for token in tokens:
        encoded = token.encode().hex()
        literal = f"decode(unhex('{encoded}'),'UTF-8')"
        source = (f" FROM (SELECT k,decode(unhex(h),'UTF-8') AS v FROM VALUES "
                  f"(0,'{encoded}'),(1,CAST(NULL AS STRING)),(2,'66616c7365') t(k,h)) s")
        for operation in ["CAST", "TRY_CAST"]:
            name = f"{encoded or 'empty'}_{operation.lower()}"
            yield name + "_scalar", f"SELECT {operation}({literal} AS BOOLEAN) AS r"
            yield name + "_columns", f"SELECT {operation}(v AS BOOLEAN) AS r" + source + " ORDER BY k"
            yield name + "_empty", f"SELECT {operation}(v AS BOOLEAN) AS r" + source + " WHERE false"
    for name, sql in [
        ("typed_null", "SELECT CAST(CAST(NULL AS STRING) AS BOOLEAN) AS r"),
        ("untyped_null", "SELECT CAST(NULL AS BOOLEAN) AS r"),
        ("literal_empty", "SELECT CAST('bad' AS BOOLEAN) AS r WHERE false"),
        ("case_unused", "SELECT CASE WHEN false THEN CAST('bad' AS BOOLEAN) ELSE true END AS r"),
        ("case_live", "SELECT CASE WHEN true THEN CAST('bad' AS BOOLEAN) ELSE true END AS r"),
        ("coalesce_unused", "SELECT COALESCE(true,CAST('bad' AS BOOLEAN)) AS r"),
        ("nullif_unused", "SELECT NULLIF(CAST(NULL AS BOOLEAN),CAST('bad' AS BOOLEAN)) AS r"),
        ("nullif_live", "SELECT NULLIF(false,CAST('bad' AS BOOLEAN)) AS r"),
        ("integer_controls", "SELECT CAST(v AS BOOLEAN) AS r FROM VALUES (0,0),(1,2),(2,CAST(NULL AS INT)) t(k,v) ORDER BY k"),
        ("boolean_controls", "SELECT CAST(v AS BOOLEAN) AS r FROM VALUES (0,true),(1,false),(2,CAST(NULL AS BOOLEAN)) t(k,v) ORDER BY k"),
    ]:
        yield name, sql


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[1]").appName("dar-boolean-casts")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    observations = []
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        queries = list(boolean_cast_queries())
        assert len(dict(queries)) == len(queries)
        for name, sql in queries:
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                try:
                    frame = spark.sql(sql)
                    expected = {"status": "ok", "schema": frame.schema.jsonValue(),
                                "rows": [[None if row[0] is None else row[0]] for row in frame.collect()]}
                    assert expected["schema"]["fields"][0]["type"] == "boolean"
                except Exception as error:
                    condition = getattr(error, "getCondition", lambda: None)()
                    if condition != "CAST_INVALID_INPUT":
                        raise
                    expected = {"status": "error", "condition": condition}
                observations.append({"id": name, "sql": sql, "ansi": ansi, "expected": expected})
        metadata = {"spark_version": spark.version, "timezone": "UTC",
                    "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version")}
        Path(__file__).with_name("boolean-cast-spark.jsonl").write_text(
            "\n".join(json.dumps(record, separators=(",", ":")) for record in [metadata, *observations]) + "\n")
        print(json.dumps({"observations": len(observations), "errors": sum(x["expected"]["status"] == "error" for x in observations)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
