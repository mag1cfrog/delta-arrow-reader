"""Capture empty grouping-set values and executed types with Spark 4.2.0."""

import json
import os
from pathlib import Path
import sys
from zipfile import ZipFile

import pyspark
from pyspark.sql import SparkSession, types

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from capture_floating_group_keys_spark import encode


def queries():
    for source in ["empty_samples", "samples", "null_samples"]:
        for label, grouping in [
            ("sets", "GROUPING SETS((g,h),(g),())"),
            ("rollup", "ROLLUP(g,h)"),
            ("cube", "CUBE(g,h)"),
            ("duplicates", "GROUPING SETS((g,h),(g,h),(),())"),
            ("no_empty_set", "GROUPING SETS((g,h),(g))"),
        ]:
            yield f"{source}_{label}", (
                "SELECT g,h,GROUPING(g) AS gg,GROUPING(h) AS gh,GROUPING_ID(g,h) AS gid,"
                "COUNT(*) AS n,COUNT(g) AS present,SUM(h) AS s,"
                "COUNT(*) FILTER(WHERE false) AS masked "
                f"FROM {source} GROUP BY {grouping} ORDER BY gid,g,h")
        for label, grouping in [
            ("constant_sets", "GROUPING SETS(('key'),())"),
            ("constant_rollup", "ROLLUP('key')"),
            ("constant_cube", "CUBE('key')"),
        ]:
            yield f"{source}_{label}", (
                f"SELECT 'key' AS k,GROUPING('key') AS total,COUNT(*) AS n "
                f"FROM {source} GROUP BY {grouping} ORDER BY total,k")
        yield f"{source}_group_only", (
            f"SELECT g,GROUPING(g) AS total FROM {source} "
            "GROUP BY GROUPING SETS((g),()) ORDER BY total,g")
        yield f"{source}_repeated_empty", (
            f"SELECT GROUPING_ID() AS gid,COUNT(*) AS n FROM {source} "
            "GROUP BY GROUPING SETS((),(),()) ORDER BY gid,n")
        yield f"{source}_single_empty_gid", (
            f"SELECT GROUPING_ID() AS gid,COUNT(*) AS n FROM {source} GROUP BY GROUPING SETS(())")
        for label, grouping in [
            ("global", ""),
            ("single_empty", " GROUP BY GROUPING SETS(())"),
        ]:
            yield f"{source}_{label}", f"SELECT COUNT(*) AS n,SUM(h) AS s FROM {source}{grouping}"
        yield f"{source}_distinct_percentile", (
            "SELECT g,GROUPING(g) AS total,APPROX_PERCENTILE(DISTINCT h,0.5) AS p,"
            f"COUNT(*) FILTER(WHERE false) AS masked FROM {source} "
            "GROUP BY GROUPING SETS((g),(),()) ORDER BY total,g,p")
        if source != "null_samples":
            for label, predicate in [("true", "true"), ("false", "false"),
                                     ("null", "CAST(NULL AS BOOLEAN)")]:
                yield f"{source}_having_{label}", (
                    f"SELECT g,GROUPING(g) AS total,COUNT(*) AS n FROM {source} "
                    f"GROUP BY ROLLUP(g) HAVING {predicate} ORDER BY total,g")
            for label, grouping in [("rollup", "ROLLUP(g)"),
                                    ("sets", "GROUPING SETS((g),())")]:
                yield f"{source}_having_divide_{label}", (
                    f"SELECT g,GROUPING(g) AS total,COUNT(*) AS n FROM {source} "
                    f"GROUP BY {grouping} HAVING 1/COUNT(*)>0 ORDER BY total,g")
    yield "filtered_empty", ("SELECT g,COUNT(*) AS n FROM samples WHERE false "
                             "GROUP BY ROLLUP(g) ORDER BY g")
    yield "having_total", ("SELECT g,GROUPING(g) AS total,COUNT(*) AS n FROM samples "
                           "GROUP BY ROLLUP(g) HAVING GROUPING(g)=1 ORDER BY total,g")
    yield "empty_having", ("SELECT g,COUNT(*) AS n FROM empty_samples "
                           "GROUP BY ROLLUP(g) HAVING COUNT(*)=0 ORDER BY g")
    yield "nested_global", ("SELECT COUNT(*) AS n,SUM(n) AS s FROM "
                            "(SELECT g,COUNT(*) AS n FROM empty_samples GROUP BY ROLLUP(g))")
    yield "permuted_grouping_bits", (
        "SELECT g,h,GROUPING_ID(h,g) AS gid,COUNT(*) AS n FROM samples "
        "GROUP BY GROUPING SETS((h),(g),()) ORDER BY gid,g,h")
    yield "issue_sets", ("SELECT g,GROUPING(g) AS total,COUNT(*) AS n FROM empty_samples "
                         "GROUP BY GROUPING SETS((g),()) ORDER BY total,g")
    yield "issue_rollup", ("SELECT g,COUNT(*) AS n FROM empty_samples "
                           "GROUP BY ROLLUP(g) ORDER BY g")
    yield "empty_global_pivot_sum", (
        "SELECT * FROM empty_samples PIVOT(SUM(h) FOR g IN(1 AS one,2 AS two))")
    yield "empty_global_pivot_count", (
        "SELECT * FROM (SELECT g FROM empty_samples) "
        "PIVOT(COUNT(*) FOR g IN(1 AS one,2 AS two))")


def main():
    output = Path(sys.argv[1])
    assert not output.exists(), output
    os.environ["PYSPARK_PYTHON"] = sys.executable
    jar, = (Path(pyspark.__file__).parent / "jars").glob("spark-common-utils_*.jar")
    with ZipFile(jar) as archive:
        build = archive.read("spark-version-info.properties").decode()
    revision = "32f7299601108917fb01920a54e084595b7b3bf8"
    assert f"revision={revision}" in build, build
    spark = (SparkSession.builder.master("local[2]").appName("dar-empty-grouping-sets")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        schema = types.StructType([types.StructField(name, types.IntegerType(), True)
                                   for name in ["g", "h"]])
        inputs = []
        for name, rows in [("empty_samples", []), ("samples", [[1, 10], [1, 10], [1, 20],
                            [2, 30], [None, 40], [2, None], [None, None]]),
                           ("null_samples", [[None, None], [None, None], [None, None]])]:
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
            inputs.append({"name": name, "schema": schema.jsonValue(),
                           "rows": [encode(row, schema, input_value=True) for row in rows]})
        with output.open("x") as destination:
            destination.write(json.dumps({"spark_version": spark.version, "spark_revision": revision,
                                          "timezone": "UTC",
                                          "inputs": inputs}, separators=(",", ":"))+"\n")
            for ansi in [False, True]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                historical = json.loads(Path(__file__).with_name("historical-cases.json").read_text())
                definitions = list(queries()) + [(case["id"], case["sql"]) for case in historical
                                                 if case["ansi"] == ansi]
                for name, sql in definitions:
                    frame = spark.sql(sql)
                    rows = [encode(row, frame.schema) for row in frame.collect()]
                    record = {"id": name, "ansi": ansi, "sql": sql, "ordered": True,
                              "rows": rows, "schema": json.loads(
                                  frame._jdf.queryExecution().executedPlan().schema().json())}
                    destination.write(json.dumps(record, separators=(",", ":"))+"\n")
                    destination.flush()
        print(json.dumps({"observations": 2*len(list(queries()))+len(historical)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
